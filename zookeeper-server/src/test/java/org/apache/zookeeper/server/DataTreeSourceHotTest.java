package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class DataTreeSourceHotTest {
    DataTree dt = null;

    @Before
    public void init() {
        dt = new DataTree();
    }

    /**
     * 普通节点
     */
    @Test
    public void testCreateNormalNode() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
//        dt.createNode("/a","hello".getBytes(),new ArrayList<>(),Long.MIN_VALUE,1,1,1,null);
//        dt.createNode("/a/b","hello".getBytes(),new ArrayList<>(),Long.MIN_VALUE,0,1,1,null);
        dt.createNode("/zookeeper/quota/a","hello".getBytes(),new ArrayList<>(),Long.MIN_VALUE,0,1,1,null);
        dt.createNode("/zookeeper/quota/a/zookeeper_limits","hello".getBytes(),new ArrayList<>(),Long.MIN_VALUE,0,1,1,null);
        dt.createNode("/zookeeper/quota/a/zookeeper_stats","hello".getBytes(),new ArrayList<>(),Long.MIN_VALUE,0,1,1,null);
        dt.createNode("/zookeeper/quota/b/zookeeper_stats","hello".getBytes(),new ArrayList<>(),Long.MIN_VALUE,0,1,1,null);



        System.out.println();
    }
}