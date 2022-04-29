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
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Responsible for performing local session upgrade. Only request submitted
 * directly to the leader should go through this processor.
 */
public class LeaderRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory
            .getLogger(LeaderRequestProcessor.class);

    /**
     * Zookeeper的领导者服务
     */
    private final LeaderZooKeeperServer lzks;

    /**
     * 下一个请求处理器
     */
    private final RequestProcessor nextProcessor;

    public LeaderRequestProcessor(LeaderZooKeeperServer zks,
                                  RequestProcessor nextProcessor) {
        this.lzks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void processRequest(Request request)
            throws RequestProcessorException {
        // Check if this is a local session and we are trying to create
        // an ephemeral node, in which case we upgrade the session
        // 升级后的请求对象
        Request upgradeRequest = null;
        try {
            // 验证session确认是否需要升级，如果需要则进行升级操作
            upgradeRequest = lzks.checkUpgradeSession(request);
        } catch (KeeperException ke) {
            if (request.getHdr() != null) {
                LOG.debug("Updating header");
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(ke.code().intValue()));
            }
            request.setException(ke);
            LOG.info("Error creating upgrade request " + ke.getMessage());
        } catch (IOException ie) {
            LOG.error("Unexpected error in upgrade", ie);
        }
        // 如果升级后的请求对象不为空
        if (upgradeRequest != null) {
            nextProcessor.processRequest(upgradeRequest);
        }

        nextProcessor.processRequest(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
    }

}
