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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This RequestProcessor forwards any requests that modify the state of the
 * system to the Leader.
 */
public class ObserverRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ObserverRequestProcessor.class);

    ObserverZooKeeperServer zks;

    RequestProcessor nextProcessor;

    // We keep a queue of requests. As requests get submitted they are
    // stored here. The queue is drained in the run() method.
    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    boolean finished = false;

    /**
     * Constructor - takes an ObserverZooKeeperServer to associate with
     * and the next processor to pass requests to after we're finished.
     * @param zks
     * @param nextProcessor
     */
    public ObserverRequestProcessor(ObserverZooKeeperServer zks,
                                    RequestProcessor nextProcessor) {
        super("ObserverRequestProcessor:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            // 是否已处理
            while (!finished) {
                // 从需要处理的请求中获取一个请求
                Request request = queuedRequests.take();
                // 日志
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, ZooTrace.CLIENT_REQUEST_TRACE_MASK,
                            'F', request, "");
                }
                // 当前处理请求等于死亡请求结束处理
                if (request == Request.requestOfDeath) {
                    break;
                }
                // We want to queue the request to be processed before we submit
                // the request to the leader so that we are ready to receive
                // the response
                // 交给下一个请求处理器处理
                nextProcessor.processRequest(request);

                // We now ship the request to the leader. As with all
                // other quorum operations, sync also follows this code
                // path, but different from others, we need to keep track
                // of the sync operations this Observer has pending, so we
                // add it to pendingSyncs.
                switch (request.type) {
                    case OpCode.sync:
                        // 等待同步请求中加入当前请求
                        zks.pendingSyncs.add(request);
                        // 发送数据包
                        zks.getObserver().request(request);
                        break;
                    case OpCode.create:
                    case OpCode.create2:
                    case OpCode.createTTL:
                    case OpCode.createContainer:
                    case OpCode.delete:
                    case OpCode.deleteContainer:
                    case OpCode.setData:
                    case OpCode.reconfig:
                    case OpCode.setACL:
                    case OpCode.multi:
                    case OpCode.check:
                        zks.getObserver().request(request);
                        break;
                    case OpCode.createSession:
                    case OpCode.closeSession:
                        // Don't forward local sessions to the leader.
                        if (!request.isLocalSession()) {
                            zks.getObserver().request(request);
                        }
                        break;
                }
            }
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("ObserverRequestProcessor exited loop!");
    }

    /**
     * Simply queue the request, which will be processed in FIFO order.
     */
    public void processRequest(Request request) {
        // 是否已经完成处理
        if (!finished) {
            // 创建升级请求后的请求对象
            Request upgradeRequest = null;
            try {
                // 通过 ObserverZooKeeperServer 进行请求升级操作
                upgradeRequest = zks.checkUpgradeSession(request);
            } catch (KeeperException ke) {
                if (request.getHdr() != null) {
                    request.getHdr().setType(OpCode.error);
                    request.setTxn(new ErrorTxn(ke.code().intValue()));
                }
                request.setException(ke);
                LOG.info("Error creating upgrade request", ke);
            } catch (IOException ie) {
                LOG.error("Unexpected error in upgrade", ie);
            }
            // 升级后的请求对象不为空
            if (upgradeRequest != null) {
                queuedRequests.add(upgradeRequest);
            }
            queuedRequests.add(request);
        }
    }

    /**
     * Shutdown the processor.
     */
    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}
