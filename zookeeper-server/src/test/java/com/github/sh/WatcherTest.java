package com.github.sh;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WatcherTest {
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = getZooKeeper();

        clear(zooKeeper);
        syncCreateNode(zooKeeper);
        getChildren(zooKeeper);
        delete(zooKeeper);

    }


    public static void delete(ZooKeeper zk) throws Exception {
        zk.delete("/test2", -1);
        zk.delete("/test1", -1);
    }


    public static void getChildren(ZooKeeper zk) throws Exception {
        List<String> childList = zk.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("getChildren " + watchedEvent);
            }
        });
    }


    public static void clear(ZooKeeper zk) throws Exception {
        List<String> childList = zk.getChildren("/", false);
        for(String s : childList) {
            if(s.equals("zookeeper"))
                continue;
            zk.delete("/" + s, -1);
        }
    }

    public static void syncCreateNode(ZooKeeper zk) {
        try {
            String path1 = zk.create("/test1", "znode1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            String path2 = zk.create("/test2", "znode2".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ZooKeeper getZooKeeper() throws IOException, InterruptedException {
        ZooKeeper s = new ZooKeeper("127.0.0.1:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);

                if (event.getState().equals(Event.KeeperState.SyncConnected)) {

                    countDownLatch.countDown();
                }
            }
        });

        countDownLatch.await();
        return s;
    }

}
