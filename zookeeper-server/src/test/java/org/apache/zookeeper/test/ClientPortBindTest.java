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

package org.apache.zookeeper.test;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

public class ClientPortBindTest extends ZKTestCase {
    protected static final Logger LOG =
            LoggerFactory.getLogger(ClientPortBindTest.class);

    /**
     * Verify that the server binds to the specified address
     */
    @Test
    public void testBindByAddress() throws Exception {
        String bindAddress = null;
        Enumeration<NetworkInterface> intfs =
                NetworkInterface.getNetworkInterfaces();
        // if we have a loopback and it has an address use it
        while (intfs.hasMoreElements()) {
            NetworkInterface i = intfs.nextElement();
            try {
                if (i.isLoopback()) {
                    Enumeration<InetAddress> addrs = i.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        InetAddress a = addrs.nextElement();
                        if (a.isLoopbackAddress()) {
                            bindAddress = a.getHostAddress();
                            break;
                        }
                    }
                }
            } catch (SocketException se) {
                LOG.warn("Couldn't find  loopback interface: " + se.getMessage());
            }
        }
        if (bindAddress == null) {
            LOG.warn("Unable to determine loop back address, skipping test");
            return;
        }
        final int PORT = PortAssignment.unique();

        LOG.info("Using " + bindAddress + " as the bind address");
        final String HOSTPORT = bindAddress + ":" + PORT;
        LOG.info("Using " + HOSTPORT + " as the host/port");


        File tmpDir = ClientBase.createTmpDir();

        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);

        ServerCnxnFactory f = ServerCnxnFactory.createFactory(
                new InetSocketAddress(bindAddress, PORT), -1);
        f.startup(zks);
        LOG.info("starting up the the server, waiting");

        Assert.assertTrue("waiting for server up",
                ClientBase.waitForServerUp(HOSTPORT,
                        CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        try {
            zk.close();
        } finally {
            f.shutdown();
            zks.shutdown();

            Assert.assertTrue("waiting for server down",
                    ClientBase.waitForServerDown(HOSTPORT,
                            CONNECTION_TIMEOUT));
        }
    }
}
