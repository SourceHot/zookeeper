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

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathTrie;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.txn.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class maintains the tree data structure. It doesn't have any networking
 * or client connection code in it so that it can be tested in a stand alone
 * way.
 * <p>
 * The tree maintains two parallel data structures: a hashtable that maps from
 * full paths to DataNodes and a tree of DataNodes. All accesses to a path is
 * through the hashtable. The tree is traversed only when serializing to disk.
 *
 * 数据树
 */
public class DataTree {
    private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);
    /**
     *  the root of zookeeper tree
     * 根节点路径
     * */
    private static final String rootZookeeper = "/";
    /**
     *  the zookeeper nodes that acts as the management and status node
     * /zookeeper 节点路径
     * **/
    private static final String procZookeeper = Quotas.procZookeeper;
    /**
     *  this will be the string thats stored as a child of root
     * 根节点字符串
     * */
    private static final String procChildZookeeper = procZookeeper.substring(1);
    /**
     * the zookeeper quota node that acts as the quota management node for
     * zookeeper
     *
     * /zookeeper/quota 节点字符串
     */
    private static final String quotaZookeeper = Quotas.quotaZookeeper;
    /** this will be the string thats stored as a child of /zookeeper */
    private static final String quotaChildZookeeper = quotaZookeeper
            .substring(procZookeeper.length() + 1);
    /**
     * the zookeeper config node that acts as the config management node for
     * zookeeper
     */
    private static final String configZookeeper = ZooDefs.CONFIG_NODE;
    /** this will be the string thats stored as a child of /zookeeper */
    private static final String configChildZookeeper = configZookeeper
            .substring(procZookeeper.length() + 1);
    /**
     * This hashtable provides a fast lookup to the datanodes. The tree is the
     * source of truth and is where all the locking occurs
     *
     * 节点容器
     */
    private final ConcurrentHashMap<String, DataNode> nodes =
            new ConcurrentHashMap<String, DataNode>();
    /**
     * 数据监控管理器（观察管理器）
     */
    private final WatchManager dataWatches = new WatchManager();
    /**
     * 子节点监控管理器（观察管理器）
     */
    private final WatchManager childWatches = new WatchManager();
    /**
     * the path trie that keeps track fo the quota nodes in this datatree
     *
     * 前缀匹配树
     */
    private final PathTrie pTrie = new PathTrie();

    /**
     * This hashtable lists the paths of the ephemeral nodes of a session.
     *
     * 会话中临时节点集合
     * key: session id
     * value: 临时节点集合
     */
    private final Map<Long, HashSet<String>> ephemerals =
            new ConcurrentHashMap<Long, HashSet<String>>();

    /**
     * This set contains the paths of all container nodes
     *
     * 所有的节点路径
     */
    private final Set<String> containers =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * This set contains the paths of all ttl nodes
     * 具备过期时间的节点集合
     */
    private final Set<String> ttls =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * acl 缓存
     */
    private final ReferenceCountedACLCache aclCache = new ReferenceCountedACLCache();
    /**
     * create a /zookeeper filesystem that is the proc filesystem of zookeeper
     *
     * /zookeeper 节点
     */
    private final DataNode procDataNode = new DataNode(new byte[0], -1L, new StatPersisted());
    /**
     * create a /zookeeper/quota node for maintaining quota properties for
     * zookeeper
     *
     * /zookeeper/quota 节点
     */
    private final DataNode quotaDataNode = new DataNode(new byte[0], -1L, new StatPersisted());
    /**
     * 最后处理的zxid
     */
    public volatile long lastProcessedZxid = 0;
    /**
     * This is a pointer to the root of the DataTree. It is the source of truth,
     * but we usually use the nodes hashmap to find nodes in the tree.
     *
     * 根节点
     */
    private DataNode root = new DataNode(new byte[0], -1L, new StatPersisted());

    public DataTree() {
        /* Rather than fight it, let root have an alias */

        // 设置空字符串对应root节点
        nodes.put("", root);
        // 设置/对应root节点
        nodes.put(rootZookeeper, root);

        /** add the proc node and quota node */
        // 向root节点添加zookeeper子节点字符串
        root.addChild(procChildZookeeper);
        // 设置/zookeeper对应procDataNode节点
        nodes.put(procZookeeper, procDataNode);

        // 向procDataNode节点添加quota子节点字符串
        procDataNode.addChild(quotaChildZookeeper);
        // 设置/zookeeper/quota对应的quotaDataNode节点
        nodes.put(quotaZookeeper, quotaDataNode);
        // 添加配置节点
        addConfigNode();
    }

    static public void copyStatPersisted(StatPersisted from, StatPersisted to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
    }

    static public void copyStat(Stat from, Stat to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
        to.setDataLength(from.getDataLength());
        to.setNumChildren(from.getNumChildren());
    }

    @SuppressWarnings("unchecked")
    public Set<String> getEphemerals(long sessionId) {
        HashSet<String> retv = ephemerals.get(sessionId);
        if (retv == null) {
            return new HashSet<String>();
        }
        HashSet<String> cloned = null;
        synchronized (retv) {
            cloned = (HashSet<String>) retv.clone();
        }
        return cloned;
    }

    public Set<String> getContainers() {
        return new HashSet<String>(containers);
    }

    public Set<String> getTtls() {
        return new HashSet<String>(ttls);
    }

    public Collection<Long> getSessions() {
        return ephemerals.keySet();
    }

    public DataNode getNode(String path) {
        return nodes.get(path);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public int getWatchCount() {
        return dataWatches.size() + childWatches.size();
    }

    public int getEphemeralsCount() {
        int result = 0;
        for (HashSet<String> set : ephemerals.values()) {
            result += set.size();
        }
        return result;
    }

    /**
     * Get the size of the nodes based on path and data length.
     *
     * @return size of the data
     */
    public long approximateDataSize() {
        long result = 0;
        for (Map.Entry<String, DataNode> entry : nodes.entrySet()) {
            DataNode value = entry.getValue();
            synchronized (value) {
                result += entry.getKey().length();
                result += value.getApproximateDataSize();
            }
        }
        return result;
    }

    /**
     * create a /zookeeper/config node for maintaining the configuration (membership and quorum system) info for
     * zookeeper
     */
    public void addConfigNode() {
        // 获取/zookeeper节点
        DataNode zookeeperZnode = nodes.get(procZookeeper);
        // 如果/zookeeper节点不为空则向其添加config子节点字符串
        if (zookeeperZnode != null) { // should always be the case
            zookeeperZnode.addChild(configChildZookeeper);
        } else {
            assert false : "There's no /zookeeper znode - this should never happen.";
        }

        // 向nodes容器添加/zookeeper/config路径对应的节点数据
        nodes.put(configZookeeper, new DataNode(new byte[0], -1L, new StatPersisted()));
        try {
            // Reconfig node is access controlled by default (ZOOKEEPER-2014).
            // 设置acl
            setACL(configZookeeper, ZooDefs.Ids.READ_ACL_UNSAFE, -1);
        } catch (KeeperException.NoNodeException e) {
            assert false : "There's no " + configZookeeper +
                    " znode - this should never happen.";
        }
    }

    /**
     * is the path one of the special paths owned by zookeeper.
     *
     * @param path
     *            the path to be checked
     * @return true if a special path. false if not.
     */
    boolean isSpecialPath(String path) {
        if (rootZookeeper.equals(path) || procZookeeper.equals(path)
                || quotaZookeeper.equals(path) || configZookeeper.equals(path)) {
            return true;
        }
        return false;
    }

    /**
     * update the count of this stat datanode
     *
     * @param lastPrefix the path of the node that is quotaed.
     * @param diff       the diff to be added to the count
     */
    public void updateCount(String lastPrefix, int diff) {
        // 组装路径，/zookeeper/quota + lastPrefix + /zookeeper_stats
        String statNode = Quotas.statPath(lastPrefix);
        // 获取数据节点
        DataNode node = nodes.get(statNode);
        StatsTrack updatedStat = null;
        // 数据节点为空
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for stat " + statNode);
            return;
        }
        synchronized (node) {
            // 从数据节点中获取data信息将其装换为StatsTrack对象
            updatedStat = new StatsTrack(new String(node.data));
            // 设置新的count信息
            updatedStat.setCount(updatedStat.getCount() + diff);
            // 覆盖数据节点的data数据
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the counts match the quota
        // 组装路径 /zookeeper/quota + lastPrefix + /zookeeper_limits
        String quotaNode = Quotas.quotaPath(lastPrefix);
        // 获取数据节点
        node = nodes.get(quotaNode);
        StatsTrack thisStats = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for quota " + quotaNode);
            return;
        }
        synchronized (node) {
            // 从数据节点中获取data信息将其装换为StatsTrack对象
            thisStats = new StatsTrack(new String(node.data));
        }
        // 警告日志
        if (thisStats.getCount() > -1 && (thisStats.getCount() < updatedStat.getCount())) {
            LOG
                    .warn("Quota exceeded: " + lastPrefix + " count="
                            + updatedStat.getCount() + " limit="
                            + thisStats.getCount());
        }
    }

    /**
     * update the count of bytes of this stat datanode
     *
     * @param lastPrefix the path of the node that is quotaed
     * @param diff       the diff to added to number of bytes
     * @throws IOException if path is not found
     */
    public void updateBytes(String lastPrefix, long diff) {
        // 组装路径，/zookeeper/quota + lastPrefix + /zookeeper_stats
        String statNode = Quotas.statPath(lastPrefix);
        DataNode node = nodes.get(statNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing stat node for bytes " + statNode);
            return;
        }
        StatsTrack updatedStat = null;
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data));
            updatedStat.setBytes(updatedStat.getBytes() + diff);
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the bytes match the quota
        // 组装路径 /zookeeper/quota + lastPrefix + /zookeeper_limits
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing quota node for bytes " + quotaNode);
            return;
        }
        StatsTrack thisStats = null;
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getBytes() > -1 && (thisStats.getBytes() < updatedStat.getBytes())) {
            LOG
                    .warn("Quota exceeded: " + lastPrefix + " bytes="
                            + updatedStat.getBytes() + " limit="
                            + thisStats.getBytes());
        }
    }

    /**
     * Add a new node to the DataTree.
     * @param path
     * 			  Path for the new node.
     * @param data
     *            Data to store in the node.
     * @param acl
     *            Node acls
     * @param ephemeralOwner
     *            the session id that owns this node. -1 indicates this is not
     *            an ephemeral node.
     * @param zxid
     *            Transaction ID
     * @param time
     * @throws NodeExistsException
     * @throws NoNodeException
     * @throws KeeperException
     */
    public void createNode(final String path, byte data[], List<ACL> acl,
                           long ephemeralOwner, int parentCVersion, long zxid, long time)
            throws NoNodeException, NodeExistsException {
        createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, null);
    }

    /**
     * Add a new node to the DataTree.
     *
     * @param path           Path for the new node.
     * @param data           Data to store in the node.
     * @param acl            Node acls
     * @param ephemeralOwner the session id that owns this node. -1 indicates this is not
     *                       an ephemeral node.
     * @param zxid           Transaction ID
     * @param time
     * @param outputStat     A Stat object to store Stat output results into.
     * @throws NodeExistsException
     * @throws NoNodeException
     * @throws KeeperException
     */
    public void createNode(final String path,
                           byte data[],
                           List<ACL> acl,
                           long ephemeralOwner,
                           int parentCVersion,
                           long zxid,
                           long time,
                           Stat outputStat)
            throws KeeperException.NoNodeException,
            KeeperException.NodeExistsException {

        // 最后一个斜杠的索引
        int lastSlash = path.lastIndexOf('/');
        // 确认父节点路径
        String parentName = path.substring(0, lastSlash);
        // 确认当前操作节点的路径
        String childName = path.substring(lastSlash + 1);
        // 创建持久化统计对象设置各项数据
        StatPersisted stat = new StatPersisted();
        stat.setCtime(time);
        stat.setMtime(time);
        stat.setCzxid(zxid);
        stat.setMzxid(zxid);
        stat.setPzxid(zxid);
        stat.setVersion(0);
        stat.setAversion(0);
        stat.setEphemeralOwner(ephemeralOwner);
        // 获取父节点
        DataNode parent = nodes.get(parentName);
        // 如果父节点不存在抛出异常
        if (parent == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            // 获取父节点的子节点字符串集合
            Set<String> children = parent.getChildren();
            // 判断当前子节点是否在子节点字符串集合中
            if (children.contains(childName)) {
                throw new KeeperException.NodeExistsException();
            }

            // 如果参数父节点的创建版本号为-1，需要重新从父节点中获取并进行累加1操作
            if (parentCVersion == -1) {
                parentCVersion = parent.stat.getCversion();
                parentCVersion++;
            }
            // 对父节点的统计信息进行重新设置
            parent.stat.setCversion(parentCVersion);
            parent.stat.setPzxid(zxid);
            // 转换acl数据
            Long longval = aclCache.convertAcls(acl);
            // 创建子节点
            DataNode child = new DataNode(data, longval, stat);
            // 向父节点中添加子节点路径
            parent.addChild(childName);
            // 将数据放入到节点容器中
            nodes.put(path, child);
            // 获取临时拥有者的类型
            EphemeralType ephemeralType = EphemeralType.get(ephemeralOwner);
            // 类型如果是CONTAINER
            if (ephemeralType == EphemeralType.CONTAINER) {
                containers.add(path);
            }
            // 类型如果是TTL
            else if (ephemeralType == EphemeralType.TTL) {
                ttls.add(path);
            }
            // 如果节点拥有者数据不为0
            else if (ephemeralOwner != 0) {
                // 从ephemerals容器中获取对应的临时节点路径集合
                HashSet<String> list = ephemerals.get(ephemeralOwner);
                // 如果临时节点集合为null则创建set集合并加入到容器，反之则将当前操作节点放入到临时节点路径容器中
                if (list == null) {
                    list = new HashSet<String>();
                    ephemerals.put(ephemeralOwner, list);
                }
                synchronized (list) {
                    list.add(path);
                }
            }
            // 如果输出统计对象不为空则进行拷贝操作
            if (outputStat != null) {
                child.copyStat(outputStat);
            }
        }
        // now check if its one of the zookeeper node child
        // 如果父节点的路径是以/zookeeper/quota开头
        if (parentName.startsWith(quotaZookeeper)) {
            // now check if its the limit node
            // 如果当前路径是zookeeper_limits
            if (Quotas.limitNode.equals(childName)) {
                // this is the limit node
                // get the parent and add it to the trie
                // 添加到路径树中
                pTrie.addPath(parentName.substring(quotaZookeeper.length()));
            }
            // 如果当前路径是zookeeper_stats
            if (Quotas.statNode.equals(childName)) {
                // 更新quota数据
                updateQuotaForPath(parentName
                        .substring(quotaZookeeper.length()));
            }
        }
        // also check to update the quotas for this node
        // 最大配额匹配路径
        String lastPrefix = getMaxPrefixWithQuota(path);
        // 最大配额匹配路径不为空
        if (lastPrefix != null) {
            // ok we have some match and need to update
            // 更新计数器
            updateCount(lastPrefix, 1);
            // 更新字节数
            updateBytes(lastPrefix, data == null ? 0 : data.length);
        }
        // 触发数据观察事件
        dataWatches.triggerWatch(path, Event.EventType.NodeCreated);
        // 触发子节点观察事件
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                Event.EventType.NodeChildrenChanged);
    }

    /**
     * remove the path from the datatree
     *
     * @param path
     *            the path to of the node to be deleted
     * @param zxid
     *            the current zxid
     * @throws KeeperException.NoNodeException
     */
    public void deleteNode(String path, long zxid)
            throws KeeperException.NoNodeException {
        // 最后一个斜杠的索引
        int lastSlash = path.lastIndexOf('/');
        // 确认父节点路径
        String parentName = path.substring(0, lastSlash);
        // 确认当前操作节点的路径
        String childName = path.substring(lastSlash + 1);

        // The child might already be deleted during taking fuzzy snapshot,
        // but we still need to update the pzxid here before throw exception
        // for no such child
        // 获取父节点路径对应的数据节点
        DataNode parent = nodes.get(parentName);
        // 父节点不存在
        if (parent == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            // 父节点的child属性中移除当前操作节点路径
            parent.removeChild(childName);
            // Only update pzxid when the zxid is larger than the current pzxid,
            // otherwise we might override higher pzxid set by a following create
            // Txn, which could cause the cversion and pzxid inconsistent
            // 如果zxid大于父节点中的pzxid需要重写父节点中的pzxid
            if (zxid > parent.stat.getPzxid()) {
                parent.stat.setPzxid(zxid);
            }
        }

        // 获取当前操作路径的数据节点
        DataNode node = nodes.get(path);
        // 数据节点为空
        if (node == null) {
            throw new KeeperException.NoNodeException();
        }
        // 从nodes容器中移除当前操作路径对应的数据
        nodes.remove(path);
        synchronized (node) {
            // 删除acl缓存中相关数据
            aclCache.removeUsage(node.acl);
        }

        // Synchronized to sync the containers and ttls change, probably
        // only need to sync on containers and ttls, will update it in a
        // separate patch.
        synchronized (parent) {
            // 获取拥有者
            long eowner = node.stat.getEphemeralOwner();
            // 将拥有者转换为EphemeralType
            EphemeralType ephemeralType = EphemeralType.get(eowner);
            // 如果是CONTAINER
            if (ephemeralType == EphemeralType.CONTAINER) {
                containers.remove(path);
            }
            // 如果是TTL
            else if (ephemeralType == EphemeralType.TTL) {
                ttls.remove(path);
            }
            // 如果拥有者表示为非0
            else if (eowner != 0) {
                // 从ephemerals容器中根据拥有者信息获取路基信息将当前路径从中移除
                HashSet<String> nodes = ephemerals.get(eowner);
                if (nodes != null) {
                    synchronized (nodes) {
                        nodes.remove(path);
                    }
                }
            }
        }

        // 如果父节点路径是以/zookeeper开头并且子节点路径是zookeeper_limits
        if (parentName.startsWith(procZookeeper) && Quotas.limitNode.equals(childName)) {
            // delete the node in the trie.
            // we need to update the trie as well
            // 从pTrie容器中删除
            pTrie.deletePath(parentName.substring(quotaZookeeper.length()));
        }

        // also check to update the quotas for this node
        // 最大配额匹配路径
        String lastPrefix = getMaxPrefixWithQuota(path);
        // 最大配额匹配路径不为空
        if (lastPrefix != null) {
            // ok we have some match and need to update
            // 更新count和bytes
            updateCount(lastPrefix, -1);
            int bytes = 0;
            synchronized (node) {
                bytes = (node.data == null ? 0 : -(node.data.length));
            }
            updateBytes(lastPrefix, bytes);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "dataWatches.triggerWatch " + path);
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "childWatches.triggerWatch " + parentName);
        }
        // 触发观察者
        Set<Watcher> processed = dataWatches.triggerWatch(path,
                EventType.NodeDeleted);
        childWatches.triggerWatch(path, EventType.NodeDeleted, processed);
        childWatches.triggerWatch("".equals(parentName) ? "/" : parentName,
                EventType.NodeChildrenChanged);
    }

    public Stat setData(String path, byte data[], int version, long zxid,
                        long time) throws KeeperException.NoNodeException {
        // 创建统计对象
        Stat s = new Stat();
        // 获取数据节点
        DataNode n = nodes.get(path);
        // 数据节点为空
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        byte lastdata[] = null;
        synchronized (n) {
            // 从数据节点中获取历史数据
            lastdata = n.data;
            // 赋值新数据
            n.data = data;
            n.stat.setMtime(time);
            n.stat.setMzxid(zxid);
            n.stat.setVersion(version);
            // 拷贝统计对象
            n.copyStat(s);
        }
        // now update if the path is in a quota subtree.
        // 最大配额匹配路径
        String lastPrefix = getMaxPrefixWithQuota(path);
        // 最大配额匹配路径不为空
        if (lastPrefix != null) {
            // 更新bytes
            this.updateBytes(lastPrefix, (data == null ? 0 : data.length)
                    - (lastdata == null ? 0 : lastdata.length));
        }
        // 触发观察者
        dataWatches.triggerWatch(path, EventType.NodeDataChanged);
        return s;
    }

    /**
     * If there is a quota set, return the appropriate prefix for that quota
     * Else return null
     *
     * 最大配额匹配路径
     * @param path The ZK path to check for quota
     * @return Max quota prefix, or null if none
     */
    public String getMaxPrefixWithQuota(String path) {
        // do nothing for the root.
        // we are not keeping a quota on the zookeeper
        // root node for now.

        String lastPrefix = pTrie.findMaxPrefix(path);

        if (rootZookeeper.equals(lastPrefix) || "".equals(lastPrefix)) {
            return null;
        } else {
            return lastPrefix;
        }
    }

    /**
     * 获取数据
     * @param path
     * @param stat
     * @param watcher
     * @return
     * @throws KeeperException.NoNodeException
     */
    public byte[] getData(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            if (watcher != null) {
                dataWatches.addWatch(path, watcher);
            }
            return n.data;
        }
    }

    /**
     * 统计节点
     * @param path
     * @param watcher
     * @return
     * @throws KeeperException.NoNodeException
     */
    public Stat statNode(String path, Watcher watcher)
            throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (watcher != null) {
            dataWatches.addWatch(path, watcher);
        }
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return stat;
        }
    }

    /**
     * 获取子节点
     * @param path
     * @param stat
     * @param watcher
     * @return
     * @throws KeeperException.NoNodeException
     */
    public List<String> getChildren(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            if (stat != null) {
                n.copyStat(stat);
            }
            List<String> children = new ArrayList<String>(n.getChildren());

            if (watcher != null) {
                childWatches.addWatch(path, watcher);
            }
            return children;
        }
    }

    public Stat setACL(String path, List<ACL> acl, int version)
            throws KeeperException.NoNodeException {
        // 创建统计对象
        Stat stat = new Stat();
        // 从nodes容器中根据路径获取节点数据
        DataNode n = nodes.get(path);
        // 如果节点数据为空抛出异常
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            // 从acl缓存中移除部分数据
            aclCache.removeUsage(n.acl);
            // 向操作节点设置acl版本号
            n.stat.setAversion(version);
            // 通过acl缓存转换acl对象将其设置到数据节点的acl变量中
            n.acl = aclCache.convertAcls(acl);
            // 将数据节点中的统计信息拷贝到新建的统计对象中然后返回
            n.copyStat(stat);
            return stat;
        }
    }

    public List<ACL> getACL(String path, Stat stat)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return new ArrayList<ACL>(aclCache.convertLong(n.acl));
        }
    }

    public List<ACL> getACL(DataNode node) {
        synchronized (node) {
            return aclCache.convertLong(node.acl);
        }
    }

    public int aclCacheSize() {
        return aclCache.size();
    }

    public ProcessTxnResult processTxn(TxnHeader header, Record txn) {
        return this.processTxn(header, txn, false);
    }

    /**
     * 处理事务：
     * @param header
     * @param txn
     * @param isSubTxn
     * @return
     */
    public ProcessTxnResult processTxn(TxnHeader header, Record txn, boolean isSubTxn) {
        ProcessTxnResult rc = new ProcessTxnResult();

        try {
            rc.clientId = header.getClientId();
            rc.cxid = header.getCxid();
            rc.zxid = header.getZxid();
            rc.type = header.getType();
            rc.err = 0;
            rc.multiResult = null;
            switch (header.getType()) {
                case OpCode.create:
                    CreateTxn createTxn = (CreateTxn) txn;
                    rc.path = createTxn.getPath();
                    createNode(
                            createTxn.getPath(),
                            createTxn.getData(),
                            createTxn.getAcl(),
                            createTxn.getEphemeral() ? header.getClientId() : 0,
                            createTxn.getParentCVersion(),
                            header.getZxid(), header.getTime(), null);
                    break;
                case OpCode.create2:
                    CreateTxn create2Txn = (CreateTxn) txn;
                    rc.path = create2Txn.getPath();
                    Stat stat = new Stat();
                    createNode(
                            create2Txn.getPath(),
                            create2Txn.getData(),
                            create2Txn.getAcl(),
                            create2Txn.getEphemeral() ? header.getClientId() : 0,
                            create2Txn.getParentCVersion(),
                            header.getZxid(), header.getTime(), stat);
                    rc.stat = stat;
                    break;
                case OpCode.createTTL:
                    CreateTTLTxn createTtlTxn = (CreateTTLTxn) txn;
                    rc.path = createTtlTxn.getPath();
                    stat = new Stat();
                    createNode(
                            createTtlTxn.getPath(),
                            createTtlTxn.getData(),
                            createTtlTxn.getAcl(),
                            EphemeralType.TTL.toEphemeralOwner(createTtlTxn.getTtl()),
                            createTtlTxn.getParentCVersion(),
                            header.getZxid(), header.getTime(), stat);
                    rc.stat = stat;
                    break;
                case OpCode.createContainer:
                    CreateContainerTxn createContainerTxn = (CreateContainerTxn) txn;
                    rc.path = createContainerTxn.getPath();
                    stat = new Stat();
                    createNode(
                            createContainerTxn.getPath(),
                            createContainerTxn.getData(),
                            createContainerTxn.getAcl(),
                            EphemeralType.CONTAINER_EPHEMERAL_OWNER,
                            createContainerTxn.getParentCVersion(),
                            header.getZxid(), header.getTime(), stat);
                    rc.stat = stat;
                    break;
                case OpCode.delete:
                case OpCode.deleteContainer:
                    DeleteTxn deleteTxn = (DeleteTxn) txn;
                    rc.path = deleteTxn.getPath();
                    deleteNode(deleteTxn.getPath(), header.getZxid());
                    break;
                case OpCode.reconfig:
                case OpCode.setData:
                    SetDataTxn setDataTxn = (SetDataTxn) txn;
                    rc.path = setDataTxn.getPath();
                    rc.stat = setData(setDataTxn.getPath(), setDataTxn
                            .getData(), setDataTxn.getVersion(), header
                            .getZxid(), header.getTime());
                    break;
                case OpCode.setACL:
                    SetACLTxn setACLTxn = (SetACLTxn) txn;
                    rc.path = setACLTxn.getPath();
                    rc.stat = setACL(setACLTxn.getPath(), setACLTxn.getAcl(),
                            setACLTxn.getVersion());
                    break;
                case OpCode.closeSession:
                    killSession(header.getClientId(), header.getZxid());
                    break;
                case OpCode.error:
                    ErrorTxn errTxn = (ErrorTxn) txn;
                    rc.err = errTxn.getErr();
                    break;
                case OpCode.check:
                    CheckVersionTxn checkTxn = (CheckVersionTxn) txn;
                    rc.path = checkTxn.getPath();
                    break;
                case OpCode.multi:
                    MultiTxn multiTxn = (MultiTxn) txn;
                    List<Txn> txns = multiTxn.getTxns();
                    rc.multiResult = new ArrayList<ProcessTxnResult>();
                    boolean failed = false;
                    for (Txn subtxn : txns) {
                        if (subtxn.getType() == OpCode.error) {
                            failed = true;
                            break;
                        }
                    }

                    boolean post_failed = false;
                    for (Txn subtxn : txns) {
                        ByteBuffer bb = ByteBuffer.wrap(subtxn.getData());
                        Record record = null;
                        switch (subtxn.getType()) {
                            case OpCode.create:
                                record = new CreateTxn();
                                break;
                            case OpCode.createTTL:
                                record = new CreateTTLTxn();
                                break;
                            case OpCode.createContainer:
                                record = new CreateContainerTxn();
                                break;
                            case OpCode.delete:
                            case OpCode.deleteContainer:
                                record = new DeleteTxn();
                                break;
                            case OpCode.setData:
                                record = new SetDataTxn();
                                break;
                            case OpCode.error:
                                record = new ErrorTxn();
                                post_failed = true;
                                break;
                            case OpCode.check:
                                record = new CheckVersionTxn();
                                break;
                            default:
                                throw new IOException("Invalid type of op: " + subtxn.getType());
                        }
                        assert (record != null);

                        ByteBufferInputStream.byteBuffer2Record(bb, record);

                        if (failed && subtxn.getType() != OpCode.error) {
                            int ec = post_failed ? Code.RUNTIMEINCONSISTENCY.intValue()
                                    : Code.OK.intValue();

                            subtxn.setType(OpCode.error);
                            record = new ErrorTxn(ec);
                        }

                        if (failed) {
                            assert (subtxn.getType() == OpCode.error);
                        }

                        TxnHeader subHdr = new TxnHeader(header.getClientId(), header.getCxid(),
                                header.getZxid(), header.getTime(),
                                subtxn.getType());
                        ProcessTxnResult subRc = processTxn(subHdr, record, true);
                        rc.multiResult.add(subRc);
                        if (subRc.err != 0 && rc.err == 0) {
                            rc.err = subRc.err;
                        }
                    }
                    break;
            }
        } catch (KeeperException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed: " + header + ":" + txn, e);
            }
            rc.err = e.code().intValue();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed: " + header + ":" + txn, e);
            }
        }


        /*
         * Things we can only update after the whole txn is applied to data
         * tree.
         *
         * If we update the lastProcessedZxid with the first sub txn in multi
         * and there is a snapshot in progress, it's possible that the zxid
         * associated with the snapshot only include partial of the multi op.
         *
         * When loading snapshot, it will only load the txns after the zxid
         * associated with snapshot file, which could cause data inconsistency
         * due to missing sub txns.
         *
         * To avoid this, we only update the lastProcessedZxid when the whole
         * multi-op txn is applied to DataTree.
         */
        if (!isSubTxn) {
            /*
             * A snapshot might be in progress while we are modifying the data
             * tree. If we set lastProcessedZxid prior to making corresponding
             * change to the tree, then the zxid associated with the snapshot
             * file will be ahead of its contents. Thus, while restoring from
             * the snapshot, the restore method will not apply the transaction
             * for zxid associated with the snapshot file, since the restore
             * method assumes that transaction to be present in the snapshot.
             *
             * To avoid this, we first apply the transaction and then modify
             * lastProcessedZxid.  During restore, we correctly handle the
             * case where the snapshot contains data ahead of the zxid associated
             * with the file.
             */
            if (rc.zxid > lastProcessedZxid) {
                lastProcessedZxid = rc.zxid;
            }
        }

        /*
         * Snapshots are taken lazily. It can happen that the child
         * znodes of a parent are created after the parent
         * is serialized. Therefore, while replaying logs during restore, a
         * create might fail because the node was already
         * created.
         *
         * After seeing this failure, we should increment
         * the cversion of the parent znode since the parent was serialized
         * before its children.
         *
         * Note, such failures on DT should be seen only during
         * restore.
         */
        if (header.getType() == OpCode.create &&
                rc.err == Code.NODEEXISTS.intValue()) {
            LOG.debug("Adjusting parent cversion for Txn: " + header.getType() +
                    " path:" + rc.path + " err: " + rc.err);
            int lastSlash = rc.path.lastIndexOf('/');
            String parentName = rc.path.substring(0, lastSlash);
            CreateTxn cTxn = (CreateTxn) txn;
            try {
                setCversionPzxid(parentName, cTxn.getParentCVersion(),
                        header.getZxid());
            } catch (KeeperException.NoNodeException e) {
                LOG.error("Failed to set parent cversion for: " +
                        parentName, e);
                rc.err = e.code().intValue();
            }
        } else if (rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr: " + header.getType() +
                    " : error: " + rc.err);
        }
        return rc;
    }

    void killSession(long session, long zxid) {
        // the list is already removed from the ephemerals
        // so we do not have to worry about synchronizing on
        // the list. This is only called from FinalRequestProcessor
        // so there is no need for synchronization. The list is not
        // changed here. Only create and delete change the list which
        // are again called from FinalRequestProcessor in sequence.
        // 从ephemerals容器中获取对应的临时节点路径
        HashSet<String> list = ephemerals.remove(session);
        // 如果临时节点路径不为空
        if (list != null) {
            // 循环删除节点
            for (String path : list) {
                try {
                    deleteNode(path, zxid);
                    if (LOG.isDebugEnabled()) {
                        LOG
                                .debug("Deleting ephemeral node " + path
                                        + " for session 0x"
                                        + Long.toHexString(session));
                    }
                } catch (NoNodeException e) {
                    LOG.warn("Ignoring NoNodeException for path " + path
                            + " while removing ephemeral for dead session 0x"
                            + Long.toHexString(session));
                }
            }
        }
    }

    /**
     * this method gets the count of nodes and the bytes under a subtree
     *
     * @param path
     *            the path to be used
     * @param counts
     *            the int count
     */
    private void getCounts(String path, Counts counts) {
        // 根据路径获取节点对象
        DataNode node = getNode(path);
        // 节点对象为空不做操作
        if (node == null) {
            return;
        }
        String[] children = null;
        int len = 0;
        synchronized (node) {
            // 获取当前节点对象的子节点路径
            Set<String> childs = node.getChildren();
            children = childs.toArray(new String[childs.size()]);
            // 计算字节长度
            len = (node.data == null ? 0 : node.data.length);
        }
        // add itself
        // 累加数据
        counts.count += 1;
        counts.bytes += len;
        // 循环子节点重复处理路径
        for (String child : children) {
            getCounts(path + "/" + child, counts);
        }
    }

    /**
     * update the quota for the given path
     *
     * @param path the path to be used
     */
    private void updateQuotaForPath(String path) {
        // 创建计数器
        Counts c = new Counts();
        // 获取path的计数器信息
        getCounts(path, c);
        // 创建StatsTrack对象
        StatsTrack strack = new StatsTrack();
        // 设置字节数
        strack.setBytes(c.bytes);
        // 设置总量
        strack.setCount(c.count);
        // 组合 zookeeper_stats 地址
        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode;
        // 获取 zookeeper_stats 节点
        DataNode node = getNode(statPath);
        // it should exist
        // 如果节点为空不做处理
        if (node == null) {
            LOG.warn("Missing quota stat node " + statPath);
            return;
        }
        synchronized (node) {
            // 将数据进行覆写
            node.data = strack.toString().getBytes();
        }
    }

    /**
     * this method traverses the quota path and update the path trie and sets
     * <p>
     * 该方法遍历配额路径并更新路径trie和集合
     *
     * @param path
     */
    private void traverseNode(String path) {
        // 获取数据节点
        DataNode node = getNode(path);
        // 获取子节点路径集合
        String children[] = null;
        synchronized (node) {
            Set<String> childs = node.getChildren();
            children = childs.toArray(new String[childs.size()]);
        }
        // 子节点路径集合长度为0的情况下
        if (children.length == 0) {
            // this node does not have a child
            // is the leaf node
            // check if its the leaf node
            // 组合结尾路径 /zookeeper_limits
            String endString = "/" + Quotas.limitNode;
            // 判断路径是否是/zookeeper_limits结尾
            if (path.endsWith(endString)) {
                // ok this is the limit node
                // get the real node and update
                // the count and the bytes
                // 得到 /zookeeper/quota/和/zookeeper_limits之间的路径
                String realPath = path.substring(Quotas.quotaZookeeper
                        .length(), path.indexOf(endString));
                // 更新配额数据
                updateQuotaForPath(realPath);
                // 添加路径
                this.pTrie.addPath(realPath);
            }
            return;
        }
        // 循环子路径处理
        for (String child : children) {
            traverseNode(path + "/" + child);
        }
    }

    /**
     * this method sets up the path trie and sets up stats for quota nodes
     * <p>
     * 此方法设置路径特里树并设置配额节点的统计信息
     */
    private void setupQuota() {
        // /zookeeper/quota
        String quotaPath = Quotas.quotaZookeeper;
        DataNode node = getNode(quotaPath);
        if (node == null) {
            return;
        }
        traverseNode(quotaPath);
    }

    /**
     * this method uses a stringbuilder to create a new path for children. This
     * is faster than string appends ( str1 + str2).
     * <p>
     * 序列化节点
     *
     * @param oa   OutputArchive to write to.
     * @param path a string builder.
     * @throws IOException
     * @throws InterruptedException
     */
    void serializeNode(OutputArchive oa, StringBuilder path) throws IOException {
        // 获取路径字符串表示
        String pathString = path.toString();
        // 获取数据节点
        DataNode node = getNode(pathString);
        // 数据节点为空
        if (node == null) {
            return;
        }
        String children[] = null;
        DataNode nodeCopy;
        synchronized (node) {
            // 创建统计对象
            StatPersisted statCopy = new StatPersisted();
            // 将数据节点中的统计信息拷贝到统计对象中
            copyStatPersisted(node.stat, statCopy);
            //we do not need to make a copy of node.data because the contents
            //are never changed
            // 根据原有数据节点创建一个拷贝的数据节点
            nodeCopy = new DataNode(node.data, node.acl, statCopy);
            // 获取数据节点的子节点集合
            Set<String> childs = node.getChildren();
            children = childs.toArray(new String[childs.size()]);
        }
        // 序列化数据 path 和 node
        serializeNodeData(oa, pathString, nodeCopy);
        // 路径地址后加上斜杠
        path.append('/');
        // 地址长度
        int off = path.length();
        // 循环子节点集合序列化子节点数据
        for (String child : children) {
            // since this is single buffer being resused
            // we need
            // to truncate the previous bytes of string.
            path.delete(off, Integer.MAX_VALUE);
            path.append(child);
            serializeNode(oa, path);
        }
    }

    // visiable for test
    public void serializeNodeData(OutputArchive oa, String path, DataNode node) throws IOException {
        oa.writeString(path, "path");
        oa.writeRecord(node, "node");
    }

    public void serialize(OutputArchive oa, String tag) throws IOException {
        aclCache.serialize(oa);
        serializeNode(oa, new StringBuilder(""));
        // / marks end of stream
        // we need to check if clear had been called in between the snapshot.
        if (root != null) {
            oa.writeString("/", "path");
        }
    }

    /**
     * 反序列化节点
     *
     * @param ia
     * @param tag
     * @throws IOException
     */
    public void deserialize(InputArchive ia, String tag) throws IOException {
        // 将ia中的数据反序列化到acl缓存中
        aclCache.deserialize(ia);
        // 将nodes和pTrie清空
        nodes.clear();
        pTrie.clear();
        // 从ia中获取path数据
        String path = ia.readString("path");
        // 循环处理节点，当节点为/时结束处理
        while (!"/".equals(path)) {
            // 创建数据节点对象
            DataNode node = new DataNode();
            // 从ia容器中获取node数据
            ia.readRecord(node, "node");
            // 向nodes容器添加数据
            nodes.put(path, node);
            // 处理acl缓存
            synchronized (node) {
                aclCache.addUsage(node.acl);
            }
            // 获取最后一个斜杠的索引
            int lastSlash = path.lastIndexOf('/');
            // 如果不存在则将当前节点设置为root节点
            if (lastSlash == -1) {
                root = node;
            }
            else {
                // 提取父节点路径
                String parentPath = path.substring(0, lastSlash);
                // 从nodes容器中获取父节点
                DataNode parent = nodes.get(parentPath);
                // 父节点为空
                if (parent == null) {
                    throw new IOException("Invalid Datatree, unable to find " +
                            "parent " + parentPath + " of path " + path);
                }
                // 添加子节点路径
                parent.addChild(path.substring(lastSlash + 1));
                // 获取拥有者标识
                long eowner = node.stat.getEphemeralOwner();
                // 拥有者类型
                EphemeralType ephemeralType = EphemeralType.get(eowner);
                // CONTAINER类型
                if (ephemeralType == EphemeralType.CONTAINER) {
                    containers.add(path);
                }
                // TTL类型
                else if (ephemeralType == EphemeralType.TTL) {
                    ttls.add(path);
                }
                // 拥有者非0
                else if (eowner != 0) {
                    // 获取ephemerals容器中对应的路径集合
                    HashSet<String> list = ephemerals.get(eowner);
                    // 路径集合为空创建新的路径容器加入到ephemerals容器中
                    if (list == null) {
                        list = new HashSet<String>();
                        ephemerals.put(eowner, list);
                    }
                    // 向路径集合中放入到路径中
                    list.add(path);
                }
            }
            // 覆盖path
            path = ia.readString("path");
        }
        // 向nodes容器添加根数据
        nodes.put("/", root);
        // we are done with deserializing the
        // the datatree
        // update the quotas - create path trie
        // and also update the stat nodes
        // 设置配额节点
        setupQuota();

        // 清除acl中的未使用数据
        aclCache.purgeUnused();
    }

    /**
     * Summary of the watches on the datatree.
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatchesSummary(PrintWriter pwriter) {
        pwriter.print(dataWatches.toString());
    }

    /**
     * Write a text dump of all the watches on the datatree.
     * Warning, this is expensive, use sparingly!
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        dataWatches.dumpWatches(pwriter, byPath);
    }

    /**
     * Returns a watch report.
     *
     * @return watch report
     * @see WatchesReport
     */
    public synchronized WatchesReport getWatches() {
        return dataWatches.getWatches();
    }

    /**
     * Returns a watch report by path.
     *
     * @return watch report
     * @see WatchesPathReport
     */
    public synchronized WatchesPathReport getWatchesByPath() {
        return dataWatches.getWatchesByPath();
    }

    /**
     * Returns a watch summary.
     *
     * @return watch summary
     * @see WatchesSummary
     */
    public synchronized WatchesSummary getWatchesSummary() {
        return dataWatches.getWatchesSummary();
    }

    /**
     * Write a text dump of all the ephemerals in the datatree.
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        pwriter.println("Sessions with Ephemerals ("
                + ephemerals.keySet().size() + "):");
        for (Entry<Long, HashSet<String>> entry : ephemerals.entrySet()) {
            pwriter.print("0x" + Long.toHexString(entry.getKey()));
            pwriter.println(":");
            HashSet<String> tmp = entry.getValue();
            if (tmp != null) {
                synchronized (tmp) {
                    for (String path : tmp) {
                        pwriter.println("\t" + path);
                    }
                }
            }
        }
    }

    /**
     * Returns a mapping of session ID to ephemeral znodes.
     *
     * @return map of session ID to sets of ephemeral znodes
     */
    public Map<Long, Set<String>> getEphemerals() {
        HashMap<Long, Set<String>> ephemeralsCopy = new HashMap<Long, Set<String>>();
        for (Entry<Long, HashSet<String>> e : ephemerals.entrySet()) {
            synchronized (e.getValue()) {
                ephemeralsCopy.put(e.getKey(), new HashSet<String>(e.getValue()));
            }
        }
        return ephemeralsCopy;
    }

    public void removeCnxn(Watcher watcher) {
        dataWatches.removeWatcher(watcher);
        childWatches.removeWatcher(watcher);
    }

    /**
     *
     * @param relativeZxid 客户端可见的zxid
     * @param dataWatches 数据观察节点路径集合
     * @param existWatches 存在观察节点路径集合
     * @param childWatches 子节点观察集合
     * @param watcher
     */
    public void setWatches(long relativeZxid, List<String> dataWatches,
                           List<String> existWatches, List<String> childWatches,
                           Watcher watcher) {
        // 循环数据观察路径
        for (String path : dataWatches) {
            DataNode node = getNode(path);
            WatchedEvent e = null;
            if (node == null) {
                watcher.process(new WatchedEvent(EventType.NodeDeleted,
                        KeeperState.SyncConnected, path));
            }
            else if (node.stat.getMzxid() > relativeZxid) {
                watcher.process(new WatchedEvent(EventType.NodeDataChanged,
                        KeeperState.SyncConnected, path));
            }
            else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        // 循环观察节点是否存在路径
        for (String path : existWatches) {
            DataNode node = getNode(path);
            if (node != null) {
                watcher.process(new WatchedEvent(EventType.NodeCreated,
                        KeeperState.SyncConnected, path));
            }
            else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        // 循环观察子节点路径
        for (String path : childWatches) {
            DataNode node = getNode(path);
            if (node == null) {
                watcher.process(new WatchedEvent(EventType.NodeDeleted,
                        KeeperState.SyncConnected, path));
            }
            else if (node.stat.getPzxid() > relativeZxid) {
                watcher.process(new WatchedEvent(EventType.NodeChildrenChanged,
                        KeeperState.SyncConnected, path));
            }
            else {
                this.childWatches.addWatch(path, watcher);
            }
        }
    }

    /**
     * This method sets the Cversion and Pzxid for the specified node to the
     * values passed as arguments. The values are modified only if newCversion
     * is greater than the current Cversion. A NoNodeException is thrown if
     * a znode for the specified path is not found.
     *
     * @param path
     *     Full path to the znode whose Cversion needs to be modified.
     *     A "/" at the end of the path is ignored.
     * @param newCversion
     *     Value to be assigned to Cversion
     * @param zxid
     *     Value to be assigned to Pzxid
     * @throws KeeperException.NoNodeException
     *     If znode not found.
     **/
    public void setCversionPzxid(String path, int newCversion, long zxid)
            throws KeeperException.NoNodeException {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        DataNode node = nodes.get(path);
        if (node == null) {
            throw new KeeperException.NoNodeException(path);
        }
        synchronized (node) {
            if (newCversion == -1) {
                newCversion = node.stat.getCversion() + 1;
            }
            if (newCversion > node.stat.getCversion()) {
                node.stat.setCversion(newCversion);
                node.stat.setPzxid(zxid);
            }
        }
    }

    public boolean containsWatcher(String path, WatcherType type, Watcher watcher) {
        boolean containsWatcher = false;
        switch (type) {
            case Children:
                containsWatcher = this.childWatches.containsWatcher(path, watcher);
                break;
            case Data:
                containsWatcher = this.dataWatches.containsWatcher(path, watcher);
                break;
            case Any:
                if (this.childWatches.containsWatcher(path, watcher)) {
                    containsWatcher = true;
                }
                if (this.dataWatches.containsWatcher(path, watcher)) {
                    containsWatcher = true;
                }
                break;
        }
        return containsWatcher;
    }

    public boolean removeWatch(String path, WatcherType type, Watcher watcher) {
        boolean removed = false;
        switch (type) {
            case Children:
                removed = this.childWatches.removeWatcher(path, watcher);
                break;
            case Data:
                removed = this.dataWatches.removeWatcher(path, watcher);
                break;
            case Any:
                if (this.childWatches.removeWatcher(path, watcher)) {
                    removed = true;
                }
                if (this.dataWatches.removeWatcher(path, watcher)) {
                    removed = true;
                }
                break;
        }
        return removed;
    }

    // visible for testing
    public ReferenceCountedACLCache getReferenceCountedAclCache() {
        return aclCache;
    }


    /**
     * 事务处理结果存储对象
     *
     * 事务处理结果
     */
    static public class ProcessTxnResult {
        public long clientId;

        public int cxid;

        public long zxid;

        public int err;

        public int type;

        public String path;

        public Stat stat;

        public List<ProcessTxnResult> multiResult;

        /**
         * Equality is defined as the clientId and the cxid being the same. This
         * allows us to use hash tables to track completion of transactions.
         *
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o) {
            if (o instanceof ProcessTxnResult) {
                ProcessTxnResult other = (ProcessTxnResult) o;
                return other.clientId == clientId && other.cxid == cxid;
            }
            return false;
        }

        /**
         * See equals() to find the rational for how this hashcode is generated.
         *
         * @see ProcessTxnResult#equals(Object)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return (int) ((clientId ^ cxid) % Integer.MAX_VALUE);
        }

    }


    /**
     * a encapsultaing class for return value
     */
    private static class Counts {
        long bytes;
        int count;
    }
}
