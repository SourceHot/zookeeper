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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ReconfigDuringLeaderSyncTest extends QuorumPeerTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(ReconfigDuringLeaderSyncTest.class);
    private static int SERVER_COUNT = 3;
    private MainThread[] mt;

    private static CustomQuorumPeer getCustomQuorumPeer(MainThread mt) {
        while (true) {
            QuorumPeer quorumPeer = mt.getQuorumPeer();
            if (null != quorumPeer) {
                return (CustomQuorumPeer) quorumPeer;
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Before
    public void setup() {
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "super:D/InIHSb7yEEbrWz8b9l71RjZJU="/* password is 'test'*/);
        QuorumPeerConfig.setReconfigEnabled(true);
    }

    /**
     * <pre>
     * Test case for https://issues.apache.org/jira/browse/ZOOKEEPER-2172.
     * Cluster crashes when reconfig a new node as a participant.
     * </pre>
     *
     * This issue occurs when reconfig's PROPOSAL and COMMITANDACTIVATE come in
     * between the snapshot and the UPTODATE. In this case processReconfig was
     * not invoked on the newly added node, and zoo.cfg.dynamic.next wasn't
     * deleted.
     */

    @Test
    public void testDuringLeaderSync() throws Exception {
        final int clientPorts[] = new int[SERVER_COUNT + 1];
        StringBuilder sb = new StringBuilder();
        String[] serverConfig = new String[SERVER_COUNT + 1];

        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            serverConfig[i] = "server." + i + "=127.0.0.1:" + PortAssignment.unique() + ":"
                    + PortAssignment.unique()
                    + ":participant;127.0.0.1:" + clientPorts[i];
            sb.append(serverConfig[i] + "\n");
        }
        String currentQuorumCfgSection = sb.toString();
        mt = new MainThread[SERVER_COUNT + 1];

        // start 3 servers
        for (int i = 0; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection, false);
            mt[i].start();
        }

        // ensure all servers started
        for (int i = 0; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i], CONNECTION_TIMEOUT));
        }
        CountdownWatcher watch = new CountdownWatcher();
        ZooKeeperAdmin preReconfigClient = new ZooKeeperAdmin("127.0.0.1:" + clientPorts[0],
                ClientBase.CONNECTION_TIMEOUT, watch);
        preReconfigClient.addAuthInfo("digest", "super:test".getBytes());
        watch.waitForConnected(ClientBase.CONNECTION_TIMEOUT);

        // new server joining
        int joinerId = SERVER_COUNT;
        clientPorts[joinerId] = PortAssignment.unique();
        serverConfig[joinerId] =
                "server." + joinerId + "=127.0.0.1:" + PortAssignment.unique() + ":"
                        + PortAssignment.unique() + ":participant;127.0.0.1:"
                        + clientPorts[joinerId];

        // Find leader id.
        int leaderId = -1;
        for (int i = 0; i < SERVER_COUNT; i++) {
            if (mt[i].main.quorumPeer.leader != null) {
                leaderId = i;
                break;
            }
        }
        assertFalse(leaderId == -1);

        // Joiner initial config consists of itself and the leader.
        sb = new StringBuilder();
        sb.append(serverConfig[leaderId] + "\n").append(serverConfig[joinerId] + "\n");

        /**
         * This server will delay the response to a NEWLEADER message, and run
         * reconfig command so that message at this processed in bellow order
         *
         * <pre>
         * NEWLEADER
         * reconfig's PROPOSAL
         * reconfig's COMMITANDACTIVATE
         * UPTODATE
         * </pre>
         */
        mt[joinerId] = new MainThread(joinerId, clientPorts[joinerId], sb.toString(), false) {
            @Override
            public TestQPMain getTestQPMain() {
                return new MockTestQPMain();
            }
        };
        mt[joinerId].start();
        CustomQuorumPeer qp = getCustomQuorumPeer(mt[joinerId]);

        // delete any already existing .next file
        String nextDynamicConfigFilename = qp.getNextDynamicConfigFilename();
        File nextDynaFile = new File(nextDynamicConfigFilename);
        nextDynaFile.delete();

        // call reconfig API when the new server has received
        // Leader.NEWLEADER
        while (true) {
            if (qp.isNewLeaderMessage()) {
                preReconfigClient.reconfigure(serverConfig[joinerId], null, null, -1, null, null);
                break;
            } else {
                // sleep for 10 millisecond and then again check
                Thread.sleep(10);
            }
        }
        watch = new CountdownWatcher();
        ZooKeeper postReconfigClient = new ZooKeeper("127.0.0.1:" + clientPorts[joinerId],
                ClientBase.CONNECTION_TIMEOUT, watch);
        watch.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        // do one successful operation on the newly added node
        postReconfigClient.create("/reconfigIssue", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertFalse("zoo.cfg.dynamic.next is not deleted.", nextDynaFile.exists());

        // verify that joiner has up-to-date config, including all four servers.
        for (long j = 0; j <= SERVER_COUNT; j++) {
            assertNotNull("server " + j + " is not present in the new quorum",
                    qp.getQuorumVerifier().getVotingMembers().get(j));
        }

        // close clients
        preReconfigClient.close();
        postReconfigClient.close();
    }

    @After
    public void tearDown() {
        // stop all severs
        if (null != mt) {
            for (int i = 0; i < mt.length; i++) {
                try {
                    mt[i].shutdown();
                } catch (InterruptedException e) {
                    LOG.warn("Quorum Peer interrupted while shutting it down", e);
                }
            }
        }
    }

    private static class CustomQuorumPeer extends QuorumPeer {
        private boolean newLeaderMessage = false;

        public CustomQuorumPeer(Map<Long, QuorumServer> quorumPeers,
                                File snapDir,
                                File logDir,
                                int clientPort,
                                int electionAlg,
                                long myid,
                                int tickTime,
                                int initLimit,
                                int syncLimit)
                throws IOException {
            super(quorumPeers, snapDir, logDir, electionAlg, myid, tickTime, initLimit, syncLimit,
                    false,
                    ServerCnxnFactory.createFactory(new InetSocketAddress(clientPort), -1),
                    new QuorumMaj(quorumPeers));
        }

        /**
         * If true, after 100 millisecond NEWLEADER response is send to leader
         *
         * @return
         */
        public boolean isNewLeaderMessage() {
            return newLeaderMessage;
        }

        @Override
        protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {

            return new Follower(this,
                    new FollowerZooKeeperServer(logFactory, this, this.getZkDb())) {

                @Override
                void writePacket(QuorumPacket pp, boolean flush) throws IOException {
                    if (pp != null && pp.getType() == Leader.ACK) {
                        newLeaderMessage = true;
                        try {
                            /**
                             * Delaying the ACK message, a follower sends as
                             * response to a NEWLEADER message, so that the
                             * leader has a chance to send the reconfig and only
                             * then the UPTODATE message.
                             */
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    super.writePacket(pp, flush);
                }
            };
        }
    }


    private static class MockTestQPMain extends TestQPMain {
        @Override
        public void runFromConfig(QuorumPeerConfig config)
                throws IOException, AdminServerException {
            quorumPeer = new CustomQuorumPeer(config.getQuorumVerifier().getAllMembers(),
                    config.getDataDir(),
                    config.getDataLogDir(), config.getClientPortAddress().getPort(),
                    config.getElectionAlg(),
                    config.getServerId(), config.getTickTime(), config.getInitLimit(),
                    config.getSyncLimit());
            quorumPeer.setConfigFileName(config.getConfigFilename());
            quorumPeer.start();
            try {
                quorumPeer.join();
            } catch (InterruptedException e) {
                LOG.warn("Quorum Peer interrupted", e);
            }
        }
    }
}
