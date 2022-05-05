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

package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 *
 *             同步请求处理器
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    /**
     * The number of log entries to log before starting a snapshot
     * 快照数量
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests =
            new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;
    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     *
     * 等待写入的请求
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random();
    private final Request requestOfDeath = Request.requestOfDeath;

    private Thread snapInProcess = null;
    /**
     * 是否运行中
     */
    volatile private boolean running;

    /**
     * 同步请求处理器
     * @param zks
     * @param nextProcessor
     */
    public SyncRequestProcessor(ZooKeeperServer zks,
                                RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    @Override
    public void run() {
        try {
            // 日志计数器
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time

            // 随机的意义是避免集群服务中同一时刻进行快照操作

            // 随机快照数量，避免全量快照日志
            int randRoll = r.nextInt(snapCount / 2);
            while (true) {
                Request si = null;
                // 从queuedRequests集合中取出请求对象
                if (toFlush.isEmpty()) {
                    si = queuedRequests.take();
                } else {
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                // 如果请求对象和死亡请求对象相同则跳出死循环
                if (si == requestOfDeath) {
                    break;
                }
                //
                if (si != null) {
                    // track the number of records written to the log
                    // 向zk数据库追加当前请求
                    if (zks.getZKDatabase().append(si)) {
                        // 日志计数器累加一
                        logCount++;
                        // 日志计数器
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount / 2);
                            // roll the log
                            // 滚动日志
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            // 确认是否需要生成快照
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            }
                            else {
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                    public void run() {
                                        try {
                                            // 生成快照
                                            zks.takeSnapshot();
                                        } catch (Exception e) {
                                            LOG.warn("Unexpected exception", e);
                                        }
                                    }
                                };
                                // 启动
                                snapInProcess.start();
                            }
                            // 计数器归零
                            logCount = 0;
                        }
                    }
                    // 等待写入的请求为空
                    else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable) nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    // 等待写入的请求
                    toFlush.add(si);
                    // 超过1000个进行写入操作
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally {
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush)
            throws IOException, RequestProcessorException {
        if (toFlush.isEmpty()) {
            return;
        }

        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable) nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if (running) {
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
