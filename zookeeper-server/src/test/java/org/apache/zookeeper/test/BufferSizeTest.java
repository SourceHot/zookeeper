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

import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BufferSizeTest extends ClientBase {
    public static final int TEST_MAXBUFFER = 100;
    private static final File TEST_DATA = new File(
            System.getProperty("test.data.dir", "src/test/resources/data"),
            "buffersize");

    private ZooKeeper zk;

    @Before
    public void setMaxBuffer() throws IOException, InterruptedException {
        System.setProperty("jute.maxbuffer", "" + TEST_MAXBUFFER);
        assertEquals("Can't set jute.maxbuffer!", TEST_MAXBUFFER, BinaryInputArchive.maxBuffer);
        zk = createClient();
    }

    @Test
    public void testCreatesReqs() throws Exception {
        testRequests(new ClientOp() {
            @Override
            public void execute(byte[] data) throws Exception {
                zk.create("/create_test", data, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
            }
        });
    }

    @Test
    public void testSetReqs() throws Exception {
        final String path = "/set_test";
        zk.create(path, new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        testRequests(new ClientOp() {
            @Override
            public void execute(byte[] data) throws Exception {
                zk.setData(path, data, -1);
            }
        });
    }

    /** Issues requests containing data smaller, equal, and greater than TEST_MAXBUFFER. */
    private void testRequests(ClientOp clientOp) throws Exception {
        clientOp.execute(new byte[TEST_MAXBUFFER - 60]);
        try {
            // This should fail since the buffer size > the data size due to extra fields
            clientOp.execute(new byte[TEST_MAXBUFFER]);
            fail("Request exceeding jute.maxbuffer succeeded!");
        } catch (KeeperException.ConnectionLossException e) {
        }
        try {
            clientOp.execute(new byte[TEST_MAXBUFFER + 10]);
            fail("Request exceeding jute.maxbuffer succeeded!");
        } catch (KeeperException.ConnectionLossException e) {
        }
    }

    @Test
    public void testStartup() throws Exception {
        final String path = "/test_node";
        zk.create(path, new byte[TEST_MAXBUFFER - 60], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.setData(path, new byte[TEST_MAXBUFFER - 50], -1);

        stopServer();
        startServer();
    }

    @Test
    public void testStartupFailureCreate() throws Exception {
        // Empty snapshot and logfile containing a 5000-byte create
        testStartupFailure(new File(TEST_DATA, "create"),
                "Server started despite create exceeding jute.maxbuffer!");
    }

    @Test
    public void testStartupFailureSet() throws Exception {
        // Empty snapshot and logfile containing a 1-byte create and 5000-byte set
        testStartupFailure(new File(TEST_DATA, "set"),
                "Server started despite set exceeding jute.maxbuffer!");
    }

    @Test
    public void testStartupFailureSnapshot() throws Exception {
        // Snapshot containing 5000-byte znode and logfile containing create txn
        testStartupFailure(new File(TEST_DATA, "snapshot"),
                "Server started despite znode exceeding jute.maxbuffer!");
    }

    private void testStartupFailure(File testDir, String failureMsg) throws Exception {
        stopServer();
        // Point server at testDir
        File oldTmpDir = tmpDir;
        tmpDir = testDir;
        try {
            startServer();
            fail(failureMsg);
        } catch (IOException e) {
            LOG.info("Successfully caught IOException: " + e);
        } finally {
            tmpDir = oldTmpDir;
        }
    }

    private interface ClientOp {
        void execute(byte[] data) throws Exception;
    }
}
