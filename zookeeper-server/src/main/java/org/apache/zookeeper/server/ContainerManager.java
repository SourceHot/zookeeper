/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages cleanup of container ZNodes. This class is meant to only
 * be run from the leader. There's no harm in running from followers/observers
 * but that will be extra work that's not needed. Once started, it periodically
 * checks container nodes that have a cversion > 0 and have no children. A
 * delete is attempted on the node. The result of the delete is unimportant.
 * If the proposal fails or the container node is not empty there's no harm.
 * 容器管理器
 */
public class ContainerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
    private final ZKDatabase zkDb;
    private final RequestProcessor requestProcessor;
    private final int checkIntervalMs;
    private final int maxPerMinute;
    private final Timer timer;
    private final AtomicReference<TimerTask> task = new AtomicReference<TimerTask>(null);

    /**
     * @param zkDb             the ZK database
     * @param requestProcessor request processer - used to inject delete
     *                         container requests
     * @param checkIntervalMs  how often to check containers in milliseconds
     * @param maxPerMinute     the max containers to delete per second - avoids
     *                         herding of container deletions
     */
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor,
                            int checkIntervalMs, int maxPerMinute) {
        this.zkDb = zkDb;
        this.requestProcessor = requestProcessor;
        this.checkIntervalMs = checkIntervalMs;
        this.maxPerMinute = maxPerMinute;
        timer = new Timer("ContainerManagerTask", true);

        LOG.info(String.format("Using checkIntervalMs=%d maxPerMinute=%d",
                checkIntervalMs, maxPerMinute));
    }

    /**
     * start/restart the timer the runs the check. Can safely be called
     * multiple times.
     */
    public void start() {
        if (task.get() == null) {
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        checkContainers();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("interrupted");
                        cancel();
                    } catch (Throwable e) {
                        LOG.error("Error checking containers", e);
                    }
                }
            };
            if (task.compareAndSet(null, timerTask)) {
                timer.scheduleAtFixedRate(timerTask, checkIntervalMs,
                        checkIntervalMs);
            }
        }
    }

    /**
     * stop the timer if necessary. Can safely be called multiple times.
     */
    public void stop() {
        TimerTask timerTask = task.getAndSet(null);
        if (timerTask != null) {
            timerTask.cancel();
        }
        timer.cancel();
    }

    /**
     * Manually check the containers. Not normally used directly
     */
    public void checkContainers()
            throws InterruptedException {
        // 间隔时间
        long minIntervalMs = getMinIntervalMs();
        // 循环容器路径
        for (String containerPath : getCandidates()) {
            // 获取当前时间
            long startMs = Time.currentElapsedTime();

            // 将容器路径字符串转换为请求
            ByteBuffer path = ByteBuffer.wrap(containerPath.getBytes());
            Request request = new Request(null, 0, 0,
                    ZooDefs.OpCode.deleteContainer, path, null);
            try {
                LOG.info("Attempting to delete candidate container: {}",
                        containerPath);
                // 处理请求
                requestProcessor.processRequest(request);
            } catch (Exception e) {
                LOG.error("Could not delete container: {}",
                        containerPath, e);
            }

            // 计算时间差，当前时间-开始时间
            long elapsedMs = Time.currentElapsedTime() - startMs;
            // 间隔时间-时间差，如果还有剩余需要进行线程等待
            long waitMs = minIntervalMs - elapsedMs;
            if (waitMs > 0) {
                Thread.sleep(waitMs);
            }
        }
    }

    // VisibleForTesting
    protected long getMinIntervalMs() {
        return TimeUnit.MINUTES.toMillis(1) / maxPerMinute;
    }

    // VisibleForTesting
    protected Collection<String> getCandidates() {
        Set<String> candidates = new HashSet<String>();
        // 从zk数据库中获取容器路径集合
        for (String containerPath : zkDb.getDataTree().getContainers()) {
            // 获取容器路径对应的数据节点
            DataNode node = zkDb.getDataTree().getNode(containerPath);
            /*
                cversion > 0: keep newly created containers from being deleted
                before any children have been added. If you were to create the
                container just before a container cleaning period the container
                would be immediately be deleted.
             */
            // 数据节点不为空
            // 数据节点中的 cversion 大于0
            // 子节点数量为0
            if ((node != null) && (node.stat.getCversion() > 0) &&
                    (node.getChildren().size() == 0)) {
                candidates.add(containerPath);
            }
        }

        // 从zk数据库中获取过期路径集合
        for (String ttlPath : zkDb.getDataTree().getTtls()) {
            // 获取过期路径对应的数据节点
            DataNode node = zkDb.getDataTree().getNode(ttlPath);
            // 数据节点不为空
            if (node != null) {
                // 获取子节点路径集合
                Set<String> children = node.getChildren();
                // 子节点路径集合为空
                // 子节点路径数量为0
                if ((children == null) || (children.size() == 0)) {
                    // EphemeralType类型是TTL
                    if (EphemeralType.get(node.stat.getEphemeralOwner()) == EphemeralType.TTL) {
                        // 获取过期时间
                        long ttl = EphemeralType.TTL.getValue(node.stat.getEphemeralOwner());
                        // 过期时间不为0
                        // 节点过期时间大于ttl时间
                        if ((ttl != 0) && (getElapsed(node) > ttl)) {
                            candidates.add(ttlPath);
                        }
                    }
                }
            }
        }
        return candidates;
    }

    // VisibleForTesting
    protected long getElapsed(DataNode node) {
        return Time.currentWallTime() - node.stat.getMtime();
    }
}
