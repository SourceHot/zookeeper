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

import org.apache.jute.Record;
import org.apache.zookeeper.server.ObserverBean;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Observers are peers that do not take part in the atomic broadcast protocol.
 * Instead, they are informed of successful proposals by the Leader. Observers
 * therefore naturally act as a relay point for publishing the proposal stream
 * and can relieve Followers of some of the connection load. Observers may
 * submit proposals, but do not vote in their acceptance.
 *
 * See ZOOKEEPER-368 for a discussion of this feature.
 */
public class Observer extends Learner {

    Observer(QuorumPeer self, ObserverZooKeeperServer observerZooKeeperServer) {
        this.self = self;
        this.zk = observerZooKeeperServer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Observer ").append(sock);
        sb.append(" pendingRevalidationCount:")
                .append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the observer to observe the leader
     * @throws Exception
     */
    void observeLeader() throws Exception {
        zk.registerJMX(new ObserverBean(this, zk), self.jmxLocalPeerBean);

        try {
            // 寻找leader服务
            QuorumServer leaderServer = findLeader();
            LOG.info("Observing " + leaderServer.addr);
            try {
                // 与leader服务建立连接
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                // 将当前节点注册到leader，得到新选举周期对应的zxid
                long newLeaderZxid = registerWithLeader(Leader.OBSERVERINFO);
                // 如果当前节点发生了状态改变抛出异常
                if (self.isReconfigStateChange()) {
                    throw new Exception("learned about role change");
                }

                // 从leader同步
                syncWithLeader(newLeaderZxid);
                // 创建数据包
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning()) {
                    // 读取数据包
                    readPacket(qp);
                    // 处理数据包
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.warn("Exception when observing the leader", e);
                try {
                    sock.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    /**
     * Controls the response of an observer to the receipt of a quorumpacket
     * @param qp
     * @throws Exception
     */
    protected void processPacket(QuorumPacket qp) throws Exception {
        switch (qp.getType()) {
            case Leader.PING:
                ping(qp);
                break;
            case Leader.PROPOSAL:
                LOG.warn("Ignoring proposal");
                break;
            case Leader.COMMIT:
                LOG.warn("Ignoring commit");
                break;
            case Leader.UPTODATE:
                LOG.error("Received an UPTODATE message after Observer started");
                break;
            case Leader.REVALIDATE:
                revalidate(qp);
                break;
            case Leader.SYNC:
                ((ObserverZooKeeperServer) zk).sync();
                break;
            case Leader.INFORM:
                TxnHeader hdr = new TxnHeader();
                Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);
                Request request =
                        new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, 0);
                ObserverZooKeeperServer obs = (ObserverZooKeeperServer) zk;
                obs.commitRequest(request);
                break;
            case Leader.INFORMANDACTIVATE:
                hdr = new TxnHeader();

                // get new designated leader from (current) leader's message
                ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                long suggestedLeaderId = buffer.getLong();

                byte[] remainingdata = new byte[buffer.remaining()];
                buffer.get(remainingdata);
                txn = SerializeUtils.deserializeTxn(remainingdata, hdr);
                QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) txn).getData()));

                request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, 0);
                obs = (ObserverZooKeeperServer) zk;

                boolean majorChange =
                        self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);

                obs.commitRequest(request);

                if (majorChange) {
                    throw new Exception("changes proposed in reconfig");
                }
                break;
            default:
                LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
                break;
        }
    }

    /**
     * Shutdown the Observer.
     */
    public void shutdown() {
        LOG.info("shutdown called", new Exception("shutdown Observer"));
        super.shutdown();
    }
}

