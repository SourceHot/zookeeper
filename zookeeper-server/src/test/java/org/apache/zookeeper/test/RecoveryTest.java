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

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

public class RecoveryTest extends ZKTestCase implements Watcher {
    protected static final Logger LOG = LoggerFactory.getLogger(RecoveryTest.class);

    private static final String HOSTPORT =
            "127.0.0.1:" + PortAssignment.unique();

    private volatile CountDownLatch startSignal;

    /**
     * Verify that if a server goes down that clients will reconnect
     * automatically after the server is restarted. Note that this requires the
     * server to restart within the connection timeout period.
     *
     * Also note that the client latches are used to eliminate any chance
     * of spurrious connectionloss exceptions on the read ops. Specifically
     * a sync operation will throw this exception if the server goes down
     * (as recognized by the client) during the operation. If the operation
     * occurs after the server is down, but before the client recognizes
     * that the server is down (ping) then the op will throw connectionloss.
     */
    @Test
    public void testRecovery() throws Exception {
        File tmpDir = ClientBase.createTmpDir();

        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);

        int oldSnapCount = SyncRequestProcessor.getSnapCount();
        SyncRequestProcessor.setSnapCount(1000);
        try {
            final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
            ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
            f.startup(zks);
            LOG.info("starting up the the server, waiting");

            Assert.assertTrue("waiting for server up",
                    ClientBase.waitForServerUp(HOSTPORT,
                            CONNECTION_TIMEOUT));

            startSignal = new CountDownLatch(1);
            ZooKeeper zk = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, this);
            startSignal.await(CONNECTION_TIMEOUT,
                    TimeUnit.MILLISECONDS);
            Assert.assertTrue("count == 0", startSignal.getCount() == 0);
            String path;
            LOG.info("starting creating nodes");
            for (int i = 0; i < 10; i++) {
                path = "/" + i;
                zk.create(path,
                        (path + "!").getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                for (int j = 0; j < 10; j++) {
                    String subpath = path + "/" + j;
                    zk.create(subpath, (subpath + "!").getBytes(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    for (int k = 0; k < 20; k++) {
                        String subsubpath = subpath + "/" + k;
                        zk.create(subsubpath, (subsubpath + "!").getBytes(),
                                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                }
            }

            f.shutdown();
            zks.shutdown();
            Assert.assertTrue("waiting for server down",
                    ClientBase.waitForServerDown(HOSTPORT,
                            CONNECTION_TIMEOUT));

            zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
            f = ServerCnxnFactory.createFactory(PORT, -1);

            startSignal = new CountDownLatch(1);

            f.startup(zks);

            Assert.assertTrue("waiting for server up",
                    ClientBase.waitForServerUp(HOSTPORT,
                            CONNECTION_TIMEOUT));

            startSignal.await(CONNECTION_TIMEOUT,
                    TimeUnit.MILLISECONDS);
            Assert.assertTrue("count == 0", startSignal.getCount() == 0);

            Stat stat = new Stat();
            for (int i = 0; i < 10; i++) {
                path = "/" + i;
                LOG.info("Checking " + path);
                Assert.assertEquals(new String(zk.getData(path, false, stat)), path
                        + "!");
                for (int j = 0; j < 10; j++) {
                    String subpath = path + "/" + j;
                    Assert.assertEquals(new String(zk.getData(subpath, false, stat)),
                            subpath + "!");
                    for (int k = 0; k < 20; k++) {
                        String subsubpath = subpath + "/" + k;
                        Assert.assertEquals(new String(zk.getData(subsubpath, false,
                                stat)), subsubpath + "!");
                    }
                }
            }
            f.shutdown();
            zks.shutdown();

            Assert.assertTrue("waiting for server down",
                    ClientBase.waitForServerDown(HOSTPORT,
                            ClientBase.CONNECTION_TIMEOUT));

            zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
            f = ServerCnxnFactory.createFactory(PORT, -1);

            startSignal = new CountDownLatch(1);

            f.startup(zks);

            Assert.assertTrue("waiting for server up",
                    ClientBase.waitForServerUp(HOSTPORT,
                            CONNECTION_TIMEOUT));

            startSignal.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
            Assert.assertTrue("count == 0", startSignal.getCount() == 0);

            stat = new Stat();
            LOG.info("Check 2");
            for (int i = 0; i < 10; i++) {
                path = "/" + i;
                Assert.assertEquals(new String(zk.getData(path, false, stat)),
                        path + "!");
                for (int j = 0; j < 10; j++) {
                    String subpath = path + "/" + j;
                    Assert.assertEquals(new String(zk.getData(subpath, false, stat)),
                            subpath + "!");
                    for (int k = 0; k < 20; k++) {
                        String subsubpath = subpath + "/" + k;
                        Assert.assertEquals(new String(zk.getData(subsubpath, false,
                                stat)), subsubpath + "!");
                    }
                }
            }
            zk.close();

            f.shutdown();
            zks.shutdown();

            Assert.assertTrue("waiting for server down",
                    ClientBase.waitForServerDown(HOSTPORT,
                            CONNECTION_TIMEOUT));
        } finally {
            SyncRequestProcessor.setSnapCount(oldSnapCount);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatcherEvent)
     */
    public void process(WatchedEvent event) {
        LOG.info("Event:" + event.getState() + " " + event.getType() + " " + event.getPath());
        if (event.getState() == KeeperState.SyncConnected
                && startSignal != null && startSignal.getCount() > 0) {
            startSignal.countDown();
        }
    }
}
