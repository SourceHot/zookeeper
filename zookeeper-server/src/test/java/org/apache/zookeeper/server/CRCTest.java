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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

public class CRCTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(CRCTest.class);

    private static final String HOSTPORT =
            "127.0.0.1:" + PortAssignment.unique();

    /**
     * corrupt a file by writing m at 500 b
     * offset
     * @param file the file to be corrupted
     * @throws IOException
     */
    private void corruptFile(File file) throws IOException {
        // corrupt the logfile
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        byte[] b = "mahadev".getBytes();
        long writeLen = 500L;
        raf.seek(writeLen);
        //corrupting the data
        raf.write(b);
        raf.close();
    }

    /** return if checksum matches for a snapshot **/
    private boolean getCheckSum(FileSnap snap, File snapFile) throws IOException {
        DataTree dt = new DataTree();
        Map<Long, Integer> sessions = new ConcurrentHashMap<Long, Integer>();
        InputStream snapIS = new BufferedInputStream(new FileInputStream(
                snapFile));
        CheckedInputStream crcIn = new CheckedInputStream(snapIS, new Adler32());
        InputArchive ia = BinaryInputArchive.getArchive(crcIn);
        try {
            snap.deserialize(dt, sessions, ia);
        } catch (IOException ie) {
            // we failed on the most recent snapshot
            // must be incomplete
            // try reading the next one
            // after corrupting
            snapIS.close();
            crcIn.close();
            throw ie;
        }

        long checksum = crcIn.getChecksum().getValue();
        long val = ia.readLong("val");
        snapIS.close();
        crcIn.close();
        return (val != checksum);
    }

    /** test checksums for the logs and snapshots.
     * the reader should fail on reading
     * a corrupt snapshot and a corrupt log
     * file
     * @throws Exception
     */
    @Test
    public void testChecksums() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(150);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        LOG.info("starting up the zookeeper server .. waiting");
        Assert.assertTrue("waiting for server being up",
                ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        try {
            for (int i = 0; i < 2000; i++) {
                zk.create("/crctest- " + i, ("/crctest- " + i).getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }
        f.shutdown();
        zks.shutdown();
        Assert.assertTrue("waiting for server down",
                ClientBase.waitForServerDown(HOSTPORT,
                        ClientBase.CONNECTION_TIMEOUT));

        File versionDir = new File(tmpDir, "version-2");
        File[] list = versionDir.listFiles();
        //there should be only two files
        // one the snapshot and the other logFile
        File snapFile = null;
        File logFile = null;
        for (File file : list) {
            LOG.info("file is " + file);
            if (file.getName().startsWith("log")) {
                logFile = file;
                corruptFile(logFile);
            }
        }
        FileTxnLog flog = new FileTxnLog(versionDir);
        TxnIterator itr = flog.read(1);
        //we will get a checksum failure
        try {
            while (itr.next()) {
            }
            Assert.assertTrue(false);
        } catch (IOException ie) {
            LOG.info("crc corruption", ie);
        }
        itr.close();
        // find the last snapshot
        FileSnap snap = new FileSnap(versionDir);
        List<File> snapFiles = snap.findNRecentSnapshots(2);
        snapFile = snapFiles.get(0);
        corruptFile(snapFile);
        boolean cfile = false;
        try {
            cfile = getCheckSum(snap, snapFile);
        } catch (IOException ie) {
            //the last snapshot seems incompelte
            // corrupt the last but one
            // and use that
            snapFile = snapFiles.get(1);
            corruptFile(snapFile);
            cfile = getCheckSum(snap, snapFile);
        }
        Assert.assertTrue(cfile);
    }
}
