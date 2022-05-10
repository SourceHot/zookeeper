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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.PlayBackListener;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * This class maintains the in memory database of zookeeper
 * server states that includes the sessions, datatree and the
 * committed logs. It is booted up  after reading the logs
 * and snapshots from the disk.
 */
public class ZKDatabase {

    /**
     * Default value is to use snapshot if txnlog size exceeds 1/3 the size of snapshot
     */
    public static final String SNAPSHOT_SIZE_FACTOR = "zookeeper.snapshotSizeFactor";
    public static final double DEFAULT_SNAPSHOT_SIZE_FACTOR = 0.33;
    public static final int commitLogCount = 500;
    private static final Logger LOG = LoggerFactory.getLogger(ZKDatabase.class);
    protected static int commitLogBuffer = 700;
    /**
     * make sure on a clear you take care of
     * all these members.
     *
     * 数据树
     */
    protected DataTree dataTree;
    /**
     * session 过期映射表
     * key: session id
     * value: 过期时间
     */
    protected ConcurrentHashMap<Long, Integer> sessionsWithTimeouts;
    /**
     * 快照日志
     */
    protected FileTxnSnapLog snapLog;
    /**
     * 最小提交日志的zxid
     * 最大提交日志的zxid
     */
    protected long minCommittedLog, maxCommittedLog;
    /**
     * 提交日志，提案日志
     */
    protected LinkedList<Proposal> committedLog = new LinkedList<Proposal>();
    /**
     * 读写锁
     */
    protected ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();

    /**
     * 提案提交事件
     */
    private final PlayBackListener commitProposalPlaybackListener = new PlayBackListener() {
        public void onTxnLoaded(TxnHeader hdr, Record txn) {
            addCommittedProposal(hdr, txn);
        }
    };
    /**
     * 快照大小因子
     */
    private double snapshotSizeFactor;
    /**
     * 是否实例化
     */
    volatile private boolean initialized = false;

    /**
     * the filetxnsnaplog that this zk database
     * maps to. There is a one to one relationship
     * between a filetxnsnaplog and zkdatabase.
     * @param snapLog the FileTxnSnapLog mapping this zkdatabase
     */
    public ZKDatabase(FileTxnSnapLog snapLog) {
        dataTree = createDataTree();
        sessionsWithTimeouts = new ConcurrentHashMap<Long, Integer>();
        this.snapLog = snapLog;

        try {
            snapshotSizeFactor = Double.parseDouble(
                    System.getProperty(SNAPSHOT_SIZE_FACTOR,
                            Double.toString(DEFAULT_SNAPSHOT_SIZE_FACTOR)));
            if (snapshotSizeFactor > 1) {
                snapshotSizeFactor = DEFAULT_SNAPSHOT_SIZE_FACTOR;
                LOG.warn("The configured {} is invalid, going to use " +
                                "the default {}", SNAPSHOT_SIZE_FACTOR,
                        DEFAULT_SNAPSHOT_SIZE_FACTOR);
            }
        } catch (NumberFormatException e) {
            LOG.error("Error parsing {}, using default value {}",
                    SNAPSHOT_SIZE_FACTOR, DEFAULT_SNAPSHOT_SIZE_FACTOR);
            snapshotSizeFactor = DEFAULT_SNAPSHOT_SIZE_FACTOR;
        }
        LOG.info("{} = {}", SNAPSHOT_SIZE_FACTOR, snapshotSizeFactor);
    }

    /**
     * checks to see if the zk database has been
     * initialized or not.
     * @return true if zk database is initialized and false if not
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * clear the zkdatabase.
     * Note to developers - be careful to see that
     * the clear method does clear out all the
     * data structures in zkdatabase.
     */
    public void clear() {
        minCommittedLog = 0;
        maxCommittedLog = 0;
        /* to be safe we just create a new
         * datatree.
         */
        dataTree = createDataTree();
        sessionsWithTimeouts.clear();
        WriteLock lock = logLock.writeLock();
        try {
            lock.lock();
            committedLog.clear();
        } finally {
            lock.unlock();
        }
        initialized = false;
    }

    /**
     * the datatree for this zkdatabase
     * @return the datatree for this zkdatabase
     */
    public DataTree getDataTree() {
        return this.dataTree;
    }

    /**
     * the committed log for this zk database
     * @return the committed log for this zkdatabase
     */
    public long getmaxCommittedLog() {
        return maxCommittedLog;
    }

    /**
     * the minimum committed transaction log
     * available in memory
     * @return the minimum committed transaction
     * log available in memory
     */
    public long getminCommittedLog() {
        return minCommittedLog;
    }

    /**
     * Get the lock that controls the committedLog. If you want to get the pointer to the committedLog, you need
     * to use this lock to acquire a read lock before calling getCommittedLog()
     * @return the lock that controls the committed log
     */
    public ReentrantReadWriteLock getLogLock() {
        return logLock;
    }

    public synchronized List<Proposal> getCommittedLog() {
        ReadLock rl = logLock.readLock();
        // only make a copy if this thread isn't already holding a lock
        if (logLock.getReadHoldCount() <= 0) {
            try {
                rl.lock();
                return new LinkedList<Proposal>(this.committedLog);
            } finally {
                rl.unlock();
            }
        }
        return this.committedLog;
    }

    /**
     * get the last processed zxid from a datatree
     * @return the last processed zxid of a datatree
     */
    public long getDataTreeLastProcessedZxid() {
        return dataTree.lastProcessedZxid;
    }

    /**
     * return the sessions in the datatree
     * @return the data tree sessions
     */
    public Collection<Long> getSessions() {
        return dataTree.getSessions();
    }

    /**
     * get sessions with timeouts
     * @return the hashmap of sessions with timeouts
     */
    public ConcurrentHashMap<Long, Integer> getSessionWithTimeOuts() {
        return sessionsWithTimeouts;
    }

    /**
     * load the database from the disk onto memory and also add
     * the transactions to the committedlog in memory.
     * @return the last valid zxid on disk
     * @throws IOException
     */
    public long loadDataBase() throws IOException {
        long zxid = snapLog.restore(dataTree, sessionsWithTimeouts, commitProposalPlaybackListener);
        initialized = true;
        return zxid;
    }

    /**
     * Fast forward the database adding transactions from the committed log into memory.
     * @return the last valid zxid.
     * @throws IOException
     */
    public long fastForwardDataBase() throws IOException {
        long zxid = snapLog.fastForwardFromEdits(dataTree, sessionsWithTimeouts,
                commitProposalPlaybackListener);
        initialized = true;
        return zxid;
    }

    private void addCommittedProposal(TxnHeader hdr, Record txn) {
        Request r = new Request(0, hdr.getCxid(), hdr.getType(), hdr, txn, hdr.getZxid());
        addCommittedProposal(r);
    }

    /**
     * maintains a list of last <i>committedLog</i>
     * or so committed requests. This is used for
     * fast follower synchronization.
     *
     * @param request committed request
     */
    public void addCommittedProposal(Request request) {
        // 写锁
        WriteLock wl = logLock.writeLock();
        try {
            // 上锁
            wl.lock();
            // 提交日志总量大于默认最大提交数量
            if (committedLog.size() > commitLogCount) {
                // 移除第一个元素
                committedLog.removeFirst();
                // 最小提交日志的zxid设置为事务日志中的第一个
                minCommittedLog = committedLog.getFirst().packet.getZxid();
            }
            // 如果提交日志为空
            if (committedLog.isEmpty()) {
                // 最小提交日志的zxid和最大提交日志的zxid都设置为请求中的zxid
                minCommittedLog = request.zxid;
                maxCommittedLog = request.zxid;
            }

            // 解析请求中的数据
            byte[] data = SerializeUtils.serializeRequest(request);
            // 构造数据信息
            QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);
            // 构造提案
            Proposal p = new Proposal();
            p.packet = pp;
            p.request = request;
            // 向提交日志中加入提案信息
            committedLog.add(p);
            // 最大提交日志的zxid修改为提案中的zxid
            maxCommittedLog = p.packet.getZxid();
        } finally {
            // 解锁
            wl.unlock();
        }
    }

    public boolean isTxnLogSyncEnabled() {
        boolean enabled = snapshotSizeFactor >= 0;
        if (enabled) {
            LOG.info("On disk txn sync enabled with snapshotSizeFactor "
                    + snapshotSizeFactor);
        } else {
            LOG.info("On disk txn sync disabled");
        }
        return enabled;
    }

    /**
     * 计算事务日志大小限制
     */
    public long calculateTxnLogSizeLimit() {
        long snapSize = 0;
        try {
            // 寻找最新的快照文件
            File snapFile = snapLog.findMostRecentSnapshot();
            if (snapFile != null) {
                // 快照文件的大小
                snapSize = snapFile.length();
            }
        } catch (IOException e) {
            LOG.error("Unable to get size of most recent snapshot");
        }
        // 快照文件大小乘快照大小因子
        return (long) (snapSize * snapshotSizeFactor);
    }

    /**
     * Get proposals from txnlog. Only packet part of proposal is populated.
     *
     * 获取提案
     * @param startZxid the starting zxid of the proposal
     * @param sizeLimit maximum on-disk size of txnlog to fetch
     *                  0 is unlimited, negative value means disable.
     * @return list of proposal (request part of each proposal is null)
     */
    public Iterator<Proposal> getProposalsFromTxnLog(long startZxid,
                                                     long sizeLimit) {
        // 如果sizeLimit值为负数则返回空迭代器
        if (sizeLimit < 0) {
            LOG.debug("Negative size limit - retrieving proposal via txnlog is disabled");
            return TxnLogProposalIterator.EMPTY_ITERATOR;
        }

        // 创建事务迭代器
        TxnIterator itr = null;
        try {

            // 通过snapLog读取startZxid开始的事务迭代器
            itr = snapLog.readTxnLog(startZxid, false);

            // If we cannot guarantee that this is strictly the starting txn
            // after a given zxid, we should fail.
            // 头信息不为空并且头信息中的zxid大于参数startZxid，关闭迭代器，返回空迭代器
            if ((itr.getHeader() != null)
                    && (itr.getHeader().getZxid() > startZxid)) {
                LOG.warn("Unable to find proposals from txnlog for zxid: "
                        + startZxid);
                itr.close();
                return TxnLogProposalIterator.EMPTY_ITERATOR;
            }

            // 参数sizeLimit大于0
            if (sizeLimit > 0) {
                // 获取快照文件大小
                long txnSize = itr.getStorageSize();
                // 文件大小大于限制大小，关闭迭代器，返回空迭代器
                if (txnSize > sizeLimit) {
                    LOG.info("Txnlog size: " + txnSize + " exceeds sizeLimit: "
                            + sizeLimit);
                    itr.close();
                    return TxnLogProposalIterator.EMPTY_ITERATOR;
                }
            }
        } catch (IOException e) {
            LOG.error("Unable to read txnlog from disk", e);
            try {
                if (itr != null) {
                    itr.close();
                }
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
            return TxnLogProposalIterator.EMPTY_ITERATOR;
        }
        // 将事务迭代器分装到TxnLogProposalIterator类中返回
        return new TxnLogProposalIterator(itr);
    }

    public List<ACL> aclForNode(DataNode n) {
        return dataTree.getACL(n);
    }

    /**
     * remove a cnxn from the datatree
     * @param cnxn the cnxn to remove from the datatree
     */
    public void removeCnxn(ServerCnxn cnxn) {
        dataTree.removeCnxn(cnxn);
    }

    /**
     * kill a given session in the datatree
     * @param sessionId the session id to be killed
     * @param zxid the zxid of kill session transaction
     */
    public void killSession(long sessionId, long zxid) {
        dataTree.killSession(sessionId, zxid);
    }

    /**
     * write a text dump of all the ephemerals in the datatree
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        dataTree.dumpEphemerals(pwriter);
    }

    public Map<Long, Set<String>> getEphemerals() {
        return dataTree.getEphemerals();
    }

    /**
     * the node count of the datatree
     * @return the node count of datatree
     */
    public int getNodeCount() {
        return dataTree.getNodeCount();
    }

    /**
     * the paths for  ephemeral session id
     * @param sessionId the session id for which paths match to
     * @return the paths for a session id
     */
    public Set<String> getEphemerals(long sessionId) {
        return dataTree.getEphemerals(sessionId);
    }

    /**
     * the last processed zxid in the datatree
     * @param zxid the last processed zxid in the datatree
     */
    public void setlastProcessedZxid(long zxid) {
        dataTree.lastProcessedZxid = zxid;
    }

    /**
     * the process txn on the data
     * @param hdr the txnheader for the txn
     * @param txn the transaction that needs to be processed
     * @return the result of processing the transaction on this
     * datatree/zkdatabase
     */
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        return dataTree.processTxn(hdr, txn);
    }

    /**
     * stat the path
     * @param path the path for which stat is to be done
     * @param serverCnxn the servercnxn attached to this request
     * @return the stat of this node
     * @throws KeeperException.NoNodeException
     */
    public Stat statNode(String path, ServerCnxn serverCnxn)
            throws KeeperException.NoNodeException {
        return dataTree.statNode(path, serverCnxn);
    }

    /**
     * get the datanode for this path
     * @param path the path to lookup
     * @return the datanode for getting the path
     */
    public DataNode getNode(String path) {
        return dataTree.getNode(path);
    }

    /**
     * get data and stat for a path
     * @param path the path being queried
     * @param stat the stat for this path
     * @param watcher the watcher function
     * @return
     * @throws KeeperException.NoNodeException
     */
    public byte[] getData(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        return dataTree.getData(path, stat, watcher);
    }

    /**
     * set watches on the datatree
     * @param relativeZxid the relative zxid that client has seen
     * @param dataWatches the data watches the client wants to reset
     * @param existWatches the exists watches the client wants to reset
     * @param childWatches the child watches the client wants to reset
     * @param watcher the watcher function
     */
    public void setWatches(long relativeZxid, List<String> dataWatches,
                           List<String> existWatches, List<String> childWatches, Watcher watcher) {
        dataTree.setWatches(relativeZxid, dataWatches, existWatches, childWatches, watcher);
    }

    /**
     * get acl for a path
     * @param path the path to query for acl
     * @param stat the stat for the node
     * @return the acl list for this path
     * @throws NoNodeException
     */
    public List<ACL> getACL(String path, Stat stat) throws NoNodeException {
        return dataTree.getACL(path, stat);
    }

    /**
     * get children list for this path
     * @param path the path of the node
     * @param stat the stat of the node
     * @param watcher the watcher function for this path
     * @return the list of children for this path
     * @throws KeeperException.NoNodeException
     */
    public List<String> getChildren(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        return dataTree.getChildren(path, stat, watcher);
    }

    /**
     * check if the path is special or not
     * @param path the input path
     * @return true if path is special and false if not
     */
    public boolean isSpecialPath(String path) {
        return dataTree.isSpecialPath(path);
    }

    /**
     * get the acl size of the datatree
     * @return the acl size of the datatree
     */
    public int getAclSize() {
        return dataTree.aclCacheSize();
    }

    /**
     * Truncate the ZKDatabase to the specified zxid
     * 截断日志
     * @param zxid the zxid to truncate zk database to
     * @return true if the truncate is successful and false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        clear();

        // truncate the log
        boolean truncated = snapLog.truncateLog(zxid);

        if (!truncated) {
            return false;
        }

        loadDataBase();
        return true;
    }

    /**
     * deserialize a snapshot from an input archive
     *
     * 反序列化快照
     * @param ia the input archive you want to deserialize from
     * @throws IOException
     */
    public void deserializeSnapshot(InputArchive ia) throws IOException {
        clear();
        SerializeUtils.deserializeSnapshot(getDataTree(), ia, getSessionWithTimeOuts());
        initialized = true;
    }

    /**
     * serialize the snapshot
     * 序列化到快照文件
     * @param oa the output archive to which the snapshot needs to be serialized
     * @throws IOException
     * @throws InterruptedException
     */
    public void serializeSnapshot(OutputArchive oa) throws IOException,
            InterruptedException {
        SerializeUtils.serializeSnapshot(getDataTree(), oa, getSessionWithTimeOuts());
    }

    /**
     * append to the underlying transaction log
     * @param si the request to append
     * @return true if the append was succesfull and false if not
     */
    public boolean append(Request si) throws IOException {
        return this.snapLog.append(si);
    }

    /**
     * roll the underlying log
     */
    public void rollLog() throws IOException {
        this.snapLog.rollLog();
    }

    /**
     * commit to the underlying transaction log
     * @throws IOException
     */
    public void commit() throws IOException {
        this.snapLog.commit();
    }

    /**
     * close this database. free the resources
     * @throws IOException
     */
    public void close() throws IOException {
        this.snapLog.close();
    }

    /**
     * 初始化配置节点数据
     *
     * @param qv
     */
    public synchronized void initConfigInZKDatabase(QuorumVerifier qv) {
        //
        if (qv == null)
            return; // only happens during tests
        try {
            // 如果数据树中 /zookeeper/config 节点不存在
            if (this.dataTree.getNode(ZooDefs.CONFIG_NODE) == null) {
                // should only happen during upgrade
                LOG.warn(
                        "configuration znode missing (should only happen during upgrade), creating the node");
                // 执行 /zookeeper/config 节点添加操作
                this.dataTree.addConfigNode();
            }
            // 设置 /zookeeper/config 节点数据
            this.dataTree.setData(ZooDefs.CONFIG_NODE, qv.toString().getBytes(), -1,
                    qv.getVersion(), Time.currentWallTime());
        } catch (NoNodeException e) {
            System.out.println("configuration node missing - should not happen");
        }
    }

    /**
     * Use for unit testing, so we can turn this feature on/off
     * @param snapshotSizeFactor Set to minus value to turn this off.
     */
    public void setSnapshotSizeFactor(double snapshotSizeFactor) {
        this.snapshotSizeFactor = snapshotSizeFactor;
    }

    /**
     * Check whether the given watcher exists in datatree
     *
     * @param path
     *            node to check watcher existence
     * @param type
     *            type of watcher
     * @param watcher
     *            watcher function
     */
    public boolean containsWatcher(String path, WatcherType type, Watcher watcher) {
        return dataTree.containsWatcher(path, type, watcher);
    }

    /**
     * Remove watch from the datatree
     *
     * @param path
     *            node to remove watches from
     * @param type
     *            type of watcher to remove
     * @param watcher
     *            watcher function to remove
     */
    public boolean removeWatch(String path, WatcherType type, Watcher watcher) {
        return dataTree.removeWatch(path, type, watcher);
    }

    // visible for testing
    public DataTree createDataTree() {
        return new DataTree();
    }
}
