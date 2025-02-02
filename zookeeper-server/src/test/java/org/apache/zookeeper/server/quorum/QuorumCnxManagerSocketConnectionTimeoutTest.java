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

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.QuorumRestartTest;
import org.apache.zookeeper.test.QuorumUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;

import static org.junit.Assert.assertTrue;

public class QuorumCnxManagerSocketConnectionTimeoutTest extends ZKTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumRestartTest.class);
    private QuorumUtil qu;

    @Before
    public void setUp() throws Exception {
        // starting a 3 node ensemble without observers
        qu = new QuorumUtil(1, 2);
        qu.startAll();
    }

    /**
     * Testing an error case reported in ZOOKEEPER-3756:
     * <p>
     * When a new leader election happens after a ZooKeeper server restarted, in Kubernetes
     * the rest of the servers can not initiate connection to the restarted one. But they
     * get SocketTimeoutException instead of immediate IOException. The Leader Election was
     * time-outing quicker than the socket.connect call, so we ended up with cycles of broken
     * leader elections.
     * <p>
     * The fix was to make the connection initiation asynchronous, so one 'broken' connection
     * doesn't make the whole leader election to be blocked, even in case of SocketTimeoutException.
     *
     * @throws Exception
     */
    @Test
    public void testSocketConnectionTimeoutDuringConnectingToElectionAddress() throws Exception {

        int leaderId = qu.getLeaderServer();

        // use a custom socket factory that will cause timeout instead of connecting to the
        // leader election port of the current leader
        final InetSocketAddress leaderElectionAddress =
                qu.getLeaderQuorumPeer().getElectionAddress();
        QuorumCnxManager.setSocketFactory(() -> new SocketStub(leaderElectionAddress));

        qu.shutdown(leaderId);

        assertTrue("Timeout during waiting for current leader to go down",
                ClientBase.waitForServerDown("127.0.0.1:" + qu.getPeer(leaderId).clientPort,
                        ClientBase.CONNECTION_TIMEOUT));

        String errorMessage = "No new leader was elected";
        waitFor(errorMessage, () -> qu.leaderExists() && qu.getLeaderServer() != leaderId, 15);
    }

    @After
    public void tearDown() throws Exception {
        qu.shutdownAll();
        QuorumCnxManager.setSocketFactory(QuorumCnxManager.DEFAULT_SOCKET_FACTORY);
    }


    final class SocketStub extends Socket {

        private final InetSocketAddress addressToTimeout;

        SocketStub(InetSocketAddress addressToTimeout) {
            this.addressToTimeout = addressToTimeout;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            if (addressToTimeout.equals(endpoint)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    LOG.warn("interrupted SocketStub.connect", e);
                }
                throw new SocketTimeoutException("timeout reached in SocketStub.connect()");
            }

            super.connect(endpoint, timeout);
        }
    }

}