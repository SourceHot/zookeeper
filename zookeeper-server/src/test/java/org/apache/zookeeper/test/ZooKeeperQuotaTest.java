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
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ZooKeeperQuotaTest extends ClientBase {

    @Test
    public void testQuota() throws IOException,
            InterruptedException, KeeperException, Exception {
        final ZooKeeper zk = createClient();
        final String path = "/a/b/v";
        // making sure setdata works on /
        zk.setData("/", "some".getBytes(), -1);
        zk.create("/a", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zk.create("/a/b", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zk.create("/a/b/v", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zk.create("/a/b/v/d", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        ZooKeeperMain.createQuota(zk, path, 5L, 10);

        // see if its set
        String absolutePath = Quotas.quotaZookeeper + path + "/" + Quotas.limitNode;
        byte[] data = zk.getData(absolutePath, false, new Stat());
        StatsTrack st = new StatsTrack(new String(data));
        Assert.assertTrue("bytes are set", st.getBytes() == 5L);
        Assert.assertTrue("num count is set", st.getCount() == 10);

        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode;
        byte[] qdata = zk.getData(statPath, false, new Stat());
        StatsTrack qst = new StatsTrack(new String(qdata));
        Assert.assertTrue("bytes are set", qst.getBytes() == 8L);
        Assert.assertTrue("count is set", qst.getCount() == 2);

        //force server to restart and load from snapshot, not txn log
        stopServer();
        startServer();
        stopServer();
        startServer();
        ZooKeeperServer server = getServer(serverFactory);
        Assert.assertNotNull("Quota is still set",
                server.getZKDatabase().getDataTree().getMaxPrefixWithQuota(path) != null);
    }
}
