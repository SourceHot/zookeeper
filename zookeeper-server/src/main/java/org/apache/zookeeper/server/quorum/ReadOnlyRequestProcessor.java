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

import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This processor is at the beginning of the ReadOnlyZooKeeperServer's
 * processors chain. All it does is, it passes read-only operations (e.g.
 * OpCode.getData, OpCode.exists) through to the next processor, but drops
 * state-changing operations (e.g. OpCode.create, OpCode.setData).
 */
public class ReadOnlyRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyRequestProcessor.class);

    private final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;
    private final ZooKeeperServer zks;
    /**
     * 是否已经处理完成
     */
    private boolean finished = false;

    public ReadOnlyRequestProcessor(ZooKeeperServer zks,
                                    RequestProcessor nextProcessor) {
        super("ReadOnlyRequestProcessor:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    public void run() {
        try {
            while (!finished) {
                // 从请求列表中获取一个请求
                Request request = queuedRequests.take();

                // 日志记录
                // log request
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'R', request, "");
                }

                // 如果请求和死亡请求相同则结束处理
                if (Request.requestOfDeath == request) {
                    break;
                }

                // filter read requests
                // 过滤请求
                switch (request.type) {
                    case OpCode.sync:
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
                        // 构造新的头对象
                        ReplyHeader hdr = new ReplyHeader(request.cxid, zks.getZKDatabase()
                                .getDataTreeLastProcessedZxid(), Code.NOTREADONLY.intValue());
                        try {
                            // 发送响应
                            request.cnxn.sendResponse(hdr, null, null);
                        } catch (IOException e) {
                            LOG.error("IO exception while sending response", e);
                        }
                        // 进行下一个处理
                        continue;
                }

                // proceed to the next processor
                // 下一个请求处理器处理器
                if (nextProcessor != null) {
                    nextProcessor.processRequest(request);
                }
            }
        } catch (RequestProcessorException e) {
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("ReadOnlyRequestProcessor exited loop!");
    }

    @Override
    public void processRequest(Request request) {
        if (!finished) {
            queuedRequests.add(request);
        }
    }

    @Override
    public void shutdown() {
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}
