/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 *
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 *
 *   - 1   commit processor main thread, which watches the request queues and
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned
 *         to the same thread (and hence are guaranteed to run in order).
 *   - 0-N worker threads, which run the rest of the request processor pipeline
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 *
 * Multi-threading constraints:
 *   - Each session's requests must be processed in order.
 *   - Write requests must be processed in zxid order
 *   - Must ensure no race condition between writes in one session that would
 *     trigger a watch being set by a read request in another session
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS =
            "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT =
            "zookeeper.commitProcessor.shutdownTimeout";
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);
    /**
     * Requests that we are holding until the commit comes in.
     * 提交到来之前持有的请求
     */
    protected final LinkedBlockingQueue<Request> queuedRequests =
            new LinkedBlockingQueue<Request>();

    /**
     * Requests that have been committed.
     * 已提交的请求
     */
    protected final LinkedBlockingQueue<Request> committedRequests =
            new LinkedBlockingQueue<Request>();

    /**
     *  Request for which we are currently awaiting a commit
     *  正在等待提交的请求
     *  */
    protected final AtomicReference<Request> nextPending =
            new AtomicReference<Request>();
    /**
     *  Request currently being committed (ie, sent off to next processor)
     * 当前正在提交的请求（即，发送到下一个处理器
     * */
    private final AtomicReference<Request> currentlyCommitting =
            new AtomicReference<Request>();

    /**
     * The number of requests currently being processed
     * <p>
     * 当前正在处理的请求数
     */
    protected AtomicInteger numRequestsProcessing = new AtomicInteger(0);
    /**
     * 是否暂停
     */
    protected volatile boolean stopped = true;
    /**
     * 工作服务
     */
    protected WorkerService workerPool;
    /**
     * 下一个请求处理器
     */
    RequestProcessor nextProcessor;
    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     * <p>
     * 是否需要等待leader服务返回，即同步请求
     */
    boolean matchSyncs;
    /**
     * work服务超时时间
     */
    private long workerShutdownTimeoutMS;

    /**
     * 提交处理器
     * @param nextProcessor
     * @param id
     * @param matchSyncs
     * @param listener
     */
    public CommitProcessor(RequestProcessor nextProcessor, String id,
                           boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    private boolean isWaitingForCommit() {
        return nextPending.get() != null;
    }

    private boolean isProcessingCommit() {
        return currentlyCommitting.get() != null;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setData:
            case OpCode.reconfig:
            case OpCode.multi:
            case OpCode.setACL:
                return true;
            case OpCode.sync:
                return matchSyncs;
            case OpCode.createSession:
            case OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    @Override
    public void run() {
        Request request;
        try {
            // 没有停止的情况下死循环
            while (!stopped) {
                // 锁
                synchronized (this) {
                    // 需要等待的条件：
                    // 1. 未停止
                    // 2.1 queuedRequests集合为空
                    // 2.2 nextPending 中存在数据
                    // 2.3 currentlyCommitting 中存在数据
                    // 3.1 committedRequests为空
                    // 3.2 numRequestsProcessing数量不为0
                    while (
                            !stopped &&
                                    ((queuedRequests.isEmpty() || isWaitingForCommit()
                                            || isProcessingCommit()) &&
                                            (committedRequests.isEmpty()
                                                    || isProcessingRequest()))) {

                        wait();
                    }
                }

                /*
                 * Processing queuedRequests: Process the next requests until we
                 * find one for which we need to wait for a commit. We cannot
                 * process a read request while we are processing write request.
                 */
                // 处理queuedRequests中的请求
                // 死循环条件
                // 1. 未停止工作
                // 2. nextPending 中存在数据
                // 3. currentlyCommitting 中存在数据
                // 4. queuedRequests 中获取一个请求对象，该请求对象不为空
                while (!stopped && !isWaitingForCommit() &&
                        !isProcessingCommit() &&
                        (request = queuedRequests.poll()) != null) {
                    // 判断是否需要提交
                    if (needCommit(request)) {
                        nextPending.set(request);
                    }
                    else {
                        // 发送给下一个请求处理器处理
                        sendToNextProcessor(request);
                    }
                }

                /*
                 * Processing committedRequests: check and see if the commit
                 * came in for the pending request. We can only commit a
                 * request when there is no other request being processed.
                 */
                // 处理已提交的请求
                processCommitted();
            }
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    /*
     * Separated this method from the main run loop
     * for test purposes (ZOOKEEPER-1863)
     */
    protected void processCommitted() {
        // 请求对象
        Request request;

        // 满足条件执行
        // 1. 没有停止工作
        // 2. numRequestsProcessing数量为0
        // 3. 从committedRequests中取出一个数据不为空
        if (!stopped && !isProcessingRequest() &&
                (committedRequests.peek() != null)) {

            /*
             * ZOOKEEPER-1863: continue only if there is no new request
             * waiting in queuedRequests or it is waiting for a
             * commit.
             */
            // 1. nextPending中存在数据
            // 2. queuedRequests不为空
            if (!isWaitingForCommit() && !queuedRequests.isEmpty()) {
                return;
            }
            // 取出请求处理
            request = committedRequests.poll();

            /*
             * We match with nextPending so that we can move to the
             * next request when it is committed. We also want to
             * use nextPending because it has the cnxn member set
             * properly.
             */
            // 从 nextPending 中取出下一个请求
            Request pending = nextPending.get();
            // 1. 下一个请求不为空
            // 2. 下一个请求与当前请求的sessionId相同
            // 3. 下一个请求与当前请求的cxid相同
            if (pending != null &&
                    pending.sessionId == request.sessionId &&
                    pending.cxid == request.cxid) {
                // we want to send our version of the request.
                // the pointer to the connection in the request
                pending.setHdr(request.getHdr());
                pending.setTxn(request.getTxn());
                pending.zxid = request.zxid;
                // Set currentlyCommitting so we will block until this
                // completes. Cleared by CommitWorkRequest after
                // nextProcessor returns.
                currentlyCommitting.set(pending);
                nextPending.set(null);
                sendToNextProcessor(pending);
            }
            else {
                // this request came from someone else so just
                // send the commit packet
                currentlyCommitting.set(request);
                sendToNextProcessor(request);
            }
        }
    }

    @Override
    public void start() {
        // 核心数量
        int numCores = Runtime.getRuntime().availableProcessors();
        // 工作线程数量
        int numWorkerThreads = Integer.getInteger(
                ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(
                ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring CommitProcessor with "
                + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                + " worker threads.");
        if (workerPool == null) {
            workerPool = new WorkerService(
                    "CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        // 启动
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    synchronized private void wakeup() {
        notifyAll();
    }

    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing request:: " + request);
        }
        committedRequests.add(request);
        if (!isProcessingCommit()) {
            wakeup();
        }
    }

    @Override
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        queuedRequests.add(request);
        if (!isWaitingForCommit()) {
            wakeup();
        }
    }

    private void halt() {
        stopped = true;
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }


    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor,"
                        + " unable to continue.");
                CommitProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            try {
                nextProcessor.processRequest(request);
            } finally {
                // If this request is the commit request that was blocking
                // the processor, clear.
                currentlyCommitting.compareAndSet(request, null);

                /*
                 * Decrement outstanding request count. The processor may be
                 * blocked at the moment because it is waiting for the pipeline
                 * to drain. In that case, wake it up if there are pending
                 * requests.
                 */
                if (numRequestsProcessing.decrementAndGet() == 0) {
                    if (!queuedRequests.isEmpty() ||
                            !committedRequests.isEmpty()) {
                        wakeup();
                    }
                }
            }
        }
    }

}
