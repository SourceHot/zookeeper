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

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.txn.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 * 前置处理器
 */
public class PrepRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;
    /**
     * this is only for testing purposes.
     * should never be used otherwise
     */
    private static boolean failCreate = false;

    static {
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    private final RequestProcessor nextProcessor;
    /**
     * 已提交的请求
     */
    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();
    ZooKeeperServer zks;

    public PrepRequestProcessor(ZooKeeperServer zks,
                                RequestProcessor nextProcessor) {
        super("ProcessThread(sid:" + zks.getServerId() + " cport:"
                + zks.getClientPort() + "):", zks.getZooKeeperServerListener());
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    /**
     * method for tests to set failCreate
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }

    /**
     * Grant or deny authorization to an operation on a node as a function of:
     *
     * @param zks:  not used.
     * @param acl:  set of ACLs for the node
     * @param perm: the permission that the client is requesting
     * @param ids:  the credentials supplied by the client
     */
    static void checkACL(ZooKeeperServer zks, List<ACL> acl, int perm,
                         List<Id> ids) throws KeeperException.NoAuthException {
        // 跳过acl认证
        if (skipACL) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Permission requested: {} ", perm);
            LOG.debug("ACLs for node: {}", acl);
            LOG.debug("Client credentials: {}", ids);
        }
        // acl集合为空或acl数量为空
        if (acl == null || acl.size() == 0) {
            return;
        }
        // 处理id集合
        for (Id authId : ids) {
            // 如果id集合中的元素中有一个方案是super返回
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        // 处理acl集合
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                // 方案是world并且id数据为anyone就放行
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                // 根据安全方案获取安全验证器
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                // 安全验证器不为空
                if (ap != null) {
                    // 处理参数ids
                    for (Id authId : ids) {
                        // 当前id中的方案是否和acl中id的方案相同
                        // 安全认证器对当前id和acl中的id进行匹配判断
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        // 抛出异常
        throw new KeeperException.NoAuthException();
    }

    /**
     *
     * @param currentVersion 当前版本号
     * @param expectedVersion 预期版本
     * @param path
     * @return
     * @throws KeeperException.BadVersionException
     */
    private static int checkAndIncVersion(int currentVersion, int expectedVersion, String path)
            throws KeeperException.BadVersionException {
        if (expectedVersion != -1 && expectedVersion != currentVersion) {
            throw new KeeperException.BadVersionException(path);
        }
        return currentVersion + 1;
    }

    @Override
    public void run() {
        LOG.info(String.format("PrepRequestProcessor (sid:%d) started, reconfigEnabled=%s",
                zks.getServerId(), zks.reconfigEnabled));
        try {
            // 死循环
            while (true) {
                // 从成员变量submittedRequests中获取一个请求
                Request request = submittedRequests.take();
                // 设置跟踪类型为CLIENT_REQUEST_TRACE_MASK
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                // 如果请求类型是ping，将跟踪类型设置为CLIENT_PING_TRACE_MASK
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                // 如果当前请求和死亡请求相同结束死循环停止工作
                if (Request.requestOfDeath == request) {
                    break;
                }
                // 处理请求
                pRequest(request);
            }
        } catch (RequestProcessorException e) {
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            // 处理异常
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    private ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        // 锁住未完成的变化记录
        synchronized (zks.outstandingChanges) {
            // 从outstandingChangesForPath对象中获取变化档案
            lastChange = zks.outstandingChangesForPath.get(path);
            if (lastChange == null) {
                // 从zk数据库中获取path对应的数据节点
                DataNode n = zks.getZKDatabase().getNode(path);
                // 如果数据节点不为空
                if (n != null) {
                    Set<String> children;
                    synchronized (n) {
                        // 获取当前节点的子节点
                        children = n.getChildren();
                    }
                    // 重写变化档案
                    lastChange = new ChangeRecord(-1, path, n.stat, children.size(),
                            zks.getZKDatabase().aclForNode(n));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        // 返回
        return lastChange;
    }

    private ChangeRecord getOutstandingChange(String path) {
        synchronized (zks.outstandingChanges) {
            return zks.outstandingChangesForPath.get(path);
        }
    }

    /**
     * 添加变化档案
     *
     * @param c
     */
    private void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     *
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     *
     * @param multiRequest
     * @return a map that contains previously existed records that probably need to be
     *         rolled back in any failure.
     */
    private Map<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();

        for (Op op : multiRequest) {
            String path = op.getPath();
            ChangeRecord cr = getOutstandingChange(path);
            // only previously existing records need to be rolled back.
            if (cr != null) {
                pendingChangeRecords.put(path, cr);
            }

            /*
             * ZOOKEEPER-1624 - We need to store for parent's ChangeRecord
             * of the parent node of a request. So that if this is a
             * sequential node creation request, rollbackPendingChanges()
             * can restore previous parent's ChangeRecord correctly.
             *
             * Otherwise, sequential node name generation will be incorrect
             * for a subsequent request.
             */
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1 || path.indexOf('\0') != -1) {
                continue;
            }
            String parentPath = path.substring(0, lastSlash);
            ChangeRecord parentCr = getOutstandingChange(parentPath);
            if (parentCr != null) {
                pendingChangeRecords.put(parentPath, parentCr);
            }
        }

        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords
     */
    void rollbackPendingChanges(long zxid, Map<String, ChangeRecord> pendingChangeRecords) {
        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            Iterator<ChangeRecord> iter = zks.outstandingChanges.descendingIterator();
            while (iter.hasNext()) {
                ChangeRecord c = iter.next();
                if (c.zxid == zxid) {
                    iter.remove();
                    // Remove all outstanding changes for paths of this multi.
                    // Previous records will be added back later.
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }

            // we don't need to roll back any records because there is nothing left.
            if (zks.outstandingChanges.isEmpty()) {
                return;
            }

            long firstZxid = zks.outstandingChanges.peek().zxid;

            for (ChangeRecord c : pendingChangeRecords.values()) {
                // Don't apply any prior change records less than firstZxid.
                // Note that previous outstanding requests might have been removed
                // once they are completed.
                if (c.zxid < firstZxid) {
                    continue;
                }

                // add previously existing records back.
                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    /**
     * Performs basic validation of a path for a create request.
     * Throws if the path is not valid and returns the parent path.
     * @throws BadArgumentsException
     */
    private String validatePathForCreate(String path, long sessionId)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {
            LOG.info("Invalid path %s with session 0x%s",
                    path, Long.toHexString(sessionId));
            throw new KeeperException.BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type 操作类型
     * @param zxid zxid
     * @param request 请求
     * @param record 档案（用于记录的对象)
     */
    protected void pRequest2Txn(int type, long zxid, Request request,
                                Record record, boolean deserialize)
            throws KeeperException, IOException, RequestProcessorException {
        request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                Time.currentWallTime(), type));

        switch (type) {
            // 创建操作
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer: {
                // 转换创建相关请求
                pRequest2TxnCreate(type, request, record, deserialize);
                break;
            }
            // 删除容器操作
            case OpCode.deleteContainer: {
                // 提取请求中的路径
                String path = new String(request.request.array());
                // 提取父路径
                String parentPath = getParentPathAndValidate(path);
                // 父节点档案信息
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                // 当前节点档案信息
                ChangeRecord nodeRecord = getRecordForPath(path);
                // 当前节点存在子节点抛出异常
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                if (EphemeralType.get(nodeRecord.stat.getEphemeralOwner())
                        == EphemeralType.NORMAL) {
                    throw new KeeperException.BadVersionException(path);
                }
                // 设置 txn
                request.setTxn(new DeleteTxn(path));
                // 拷贝父节点数据
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                // 父节点的子节点数量减一
                parentRecord.childCount--;
                // 添加变化档案
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            }
            // 删除节点操作
            case OpCode.delete:
                // 通过sessionTracker验证session
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 转换为删除请求
                DeleteRequest deleteRequest = (DeleteRequest) record;
                // 如果需要反序列化请求对象中的信息反序列化到删除请求中
                if (deserialize) {
                    ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
                }
                // 获取需要删除的路径
                String path = deleteRequest.getPath();
                // 获取删除路径的父路径
                String parentPath = getParentPathAndValidate(path);
                // 获取父路径的档案
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                // 获取当前路径的档案
                ChangeRecord nodeRecord = getRecordForPath(path);
                // 验证ACL
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.DELETE, request.authInfo);
                // 验证版本号
                checkAndIncVersion(nodeRecord.stat.getVersion(), deleteRequest.getVersion(), path);
                // 当前节点的子节点数量大于0抛出异常
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                // 设置txn
                request.setTxn(new DeleteTxn(path));
                // 拷贝父节点
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                // 父节点的子节点数量减一
                parentRecord.childCount--;
                // 加入变化档案
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            //设置数据操作
            case OpCode.setData:
                // 通过sessionTracker验证session
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 转换为设置数据请求
                SetDataRequest setDataRequest = (SetDataRequest) record;
                // 如果需要反序列化请求对象中的信息反序列化到设置数据请求中
                if (deserialize) {
                    ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
                }
                // 获取当前操作路径
                path = setDataRequest.getPath();
                // 验证当前操作路径
                validatePath(path, request.sessionId);
                // 获取当前操作路径对应的档案
                nodeRecord = getRecordForPath(path);
                // 验证权限
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);
                // 计算新的版本
                int newVersion = checkAndIncVersion(nodeRecord.stat.getVersion(),
                        setDataRequest.getVersion(), path);

                // 设置txn
                request.setTxn(new SetDataTxn(path, setDataRequest.getData(), newVersion));
                // 拷贝当前节点的档案
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                // 设置版本号
                nodeRecord.stat.setVersion(newVersion);
                // 添加变化档案
                addChangeRecord(nodeRecord);
                break;
            // 重载配置操作
            case OpCode.reconfig:
                // 如果zk服务没有启动配置重载将抛出异常
                if (!zks.isReconfigEnabled()) {
                    LOG.error("Reconfig operation requested but reconfig feature is disabled.");
                    throw new KeeperException.ReconfigDisabledException();
                }

                // 如果跳过acl会输出警告日志
                if (skipACL) {
                    LOG.warn("skipACL is set, reconfig operation will skip ACL checks!");
                }

                // 通过sessionTracker验证session
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 转换为重载配置请求
                ReconfigRequest reconfigRequest = (ReconfigRequest) record;
                // 确认leader服务
                LeaderZooKeeperServer lzks;
                try {
                    // 如果当前zk服务无法转换为LeaderZooKeeperServer将抛出异常
                    lzks = (LeaderZooKeeperServer) zks;
                } catch (ClassCastException e) {
                    // standalone mode - reconfiguration currently not supported
                    throw new KeeperException.UnimplementedException();
                }
                // 获取最后一个验证器 QuorumVerifier
                QuorumVerifier lastSeenQV = lzks.self.getLastSeenQuorumVerifier();
                // check that there's no reconfig in progress
                // 最后一个验证器的版本和leader服务中的验证器版本不相等将抛出异常
                if (lastSeenQV.getVersion() != lzks.self.getQuorumVerifier().getVersion()) {
                    throw new KeeperException.ReconfigInProgress();
                }
                // 获取配置id
                long configId = reconfigRequest.getCurConfigId();

                // 配置id为-1
                // 配置id不等于最后一个验证器的版本
                if (configId != -1 && configId != lzks.self.getLastSeenQuorumVerifier()
                        .getVersion()) {
                    String msg = "Reconfiguration from version " + configId
                            + " failed -- last seen version is " +
                            lzks.self.getLastSeenQuorumVerifier().getVersion();
                    throw new KeeperException.BadVersionException(msg);
                }

                // 获取新成员(配置数据)
                String newMembers = reconfigRequest.getNewMembers();

                // 新成员不为空
                if (newMembers != null) { //non-incremental membership change
                    LOG.info("Non-incremental reconfig");

                    // Input may be delimited by either commas or newlines so convert to common newline separated format
                    newMembers = newMembers.replaceAll(",", "\n");

                    try {
                        // 读取新成员数据
                        Properties props = new Properties();
                        props.load(new StringReader(newMembers));
                        // 解析配置
                        request.qv = QuorumPeerConfig.parseDynamicConfig(props,
                                lzks.self.getElectionType(), true, false);
                        request.qv.setVersion(request.getHdr().getZxid());
                    } catch (IOException | ConfigException e) {
                        throw new KeeperException.BadArgumentsException(e.getMessage());
                    }
                } else { //incremental change - must be a majority quorum system
                    LOG.info("Incremental reconfig");

                    // 求需要进入集群的服务
                    List<String> joiningServers = null;
                    String joiningServersString = reconfigRequest.getJoiningServers();
                    if (joiningServersString != null) {
                        joiningServers = StringUtils.split(joiningServersString, ",");
                    }

                    // 求需要离开的服务
                    List<String> leavingServers = null;
                    String leavingServersString = reconfigRequest.getLeavingServers();
                    if (leavingServersString != null) {
                        leavingServers = StringUtils.split(leavingServersString, ",");
                    }

                    // 类型不同抛出异常
                    if (!(lastSeenQV instanceof QuorumMaj)) {
                        String msg =
                                "Incremental reconfiguration requested but last configuration seen has a non-majority quorum system";
                        LOG.warn(msg);
                        throw new KeeperException.BadArgumentsException(msg);
                    }
                    // 服务集合
                    Map<Long, QuorumServer> nextServers =
                            new HashMap<Long, QuorumServer>(lastSeenQV.getAllMembers());
                    try {
                        // 如果需要离开的服务变量存在则将nextServers中对应的数据删除
                        if (leavingServers != null) {
                            for (String leaving : leavingServers) {
                                long sid = Long.parseLong(leaving);
                                nextServers.remove(sid);
                            }
                        }
                        // 如果需要进入的服务存在则将数据加入到nextServers中
                        if (joiningServers != null) {
                            for (String joiner : joiningServers) {
                                // joiner should have the following format: server.x = server_spec;client_spec
                                String[] parts =
                                        StringUtils.split(joiner, "=").toArray(new String[0]);
                                if (parts.length != 2) {
                                    throw new KeeperException.BadArgumentsException(
                                            "Wrong format of server string");
                                }
                                // extract server id x from first part of joiner: server.x
                                Long sid = Long.parseLong(
                                        parts[0].substring(parts[0].lastIndexOf('.') + 1));
                                QuorumServer qs = new QuorumServer(sid, parts[1]);
                                if (qs.clientAddr == null || qs.electionAddr == null
                                        || qs.addr == null) {
                                    throw new KeeperException.BadArgumentsException(
                                            "Wrong format of server string - each server should have 3 ports specified");
                                }

                                // check duplication of addresses and ports
                                for (QuorumServer nqs : nextServers.values()) {
                                    if (qs.id == nqs.id) {
                                        continue;
                                    }
                                    qs.checkAddressDuplicate(nqs);
                                }

                                nextServers.remove(qs.id);
                                nextServers.put(qs.id, qs);
                            }
                        }
                    } catch (ConfigException e) {
                        throw new KeeperException.BadArgumentsException("Reconfiguration failed");
                    }
                    // 设置请求参数qv
                    request.qv = new QuorumMaj(nextServers);
                    request.qv.setVersion(request.getHdr().getZxid());
                }
                // 如果仲裁配置启用并且投票成员数量小于2，抛出异常
                if (QuorumPeerConfig.isStandaloneEnabled()
                        && request.qv.getVotingMembers().size() < 2) {
                    String msg =
                            "Reconfig failed - new configuration must include at least 2 followers";
                    LOG.warn(msg);
                    throw new KeeperException.BadArgumentsException(msg);
                }
                // 如果成员数量小于1抛出异常
                else if (request.qv.getVotingMembers().size() < 1) {
                    String msg =
                            "Reconfig failed - new configuration must include at least 1 follower";
                    LOG.warn(msg);
                    throw new KeeperException.BadArgumentsException(msg);
                }

                // 仲裁数据是否同步，如果没有同步抛出异常
                if (!lzks.getLeader().isQuorumSynced(request.qv)) {
                    String msg2 =
                            "Reconfig failed - there must be a connected and synced quorum in new configuration";
                    LOG.warn(msg2);
                    throw new KeeperException.NewConfigNoQuorum();
                }

                // 获取配置节点的档案
                nodeRecord = getRecordForPath(ZooDefs.CONFIG_NODE);
                // 验证权限
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);
                // 设置txn
                request.setTxn(
                        new SetDataTxn(ZooDefs.CONFIG_NODE, request.qv.toString().getBytes(), -1));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setVersion(-1);
                addChangeRecord(nodeRecord);
                break;
            // 设置权限操作
            case OpCode.setACL:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetACLRequest setAclRequest = (SetACLRequest) record;
                if (deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
                path = setAclRequest.getPath();
                validatePath(path, request.sessionId);
                List<ACL> listACL = fixupACL(path, request.authInfo, setAclRequest.getAcl());
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.ADMIN, request.authInfo);
                newVersion = checkAndIncVersion(nodeRecord.stat.getAversion(),
                        setAclRequest.getVersion(), path);
                request.setTxn(new SetACLTxn(path, listACL, newVersion));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setAversion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            // 创建session操作
            case OpCode.createSession:
                request.request.rewind();
                int to = request.request.getInt();
                request.setTxn(new CreateSessionTxn(to));
                request.request.rewind();
                if (request.isLocalSession()) {
                    // This will add to local session tracker if it is enabled
                    zks.sessionTracker.addSession(request.sessionId, to);
                } else {
                    // Explicitly add to global session if the flag is not set
                    zks.sessionTracker.addGlobalSession(request.sessionId, to);
                }
                zks.setOwner(request.sessionId, request.getOwner());
                break;
            // 关闭session操作
            case OpCode.closeSession:
                // We don't want to do this check since the session expiration thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                //zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                Set<String> es = zks.getZKDatabase()
                        .getEphemerals(request.sessionId);
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(
                                new ChangeRecord(request.getHdr().getZxid(), path2Delete, null, 0,
                                        null));
                    }

                    zks.sessionTracker.setSessionClosing(request.sessionId);
                }
                break;
            // 检查
            case OpCode.check:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest) record;
                if (deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
                path = checkVersionRequest.getPath();
                validatePath(path, request.sessionId);
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.READ, request.authInfo);
                request.setTxn(
                        new CheckVersionTxn(path, checkAndIncVersion(nodeRecord.stat.getVersion(),
                                checkVersionRequest.getVersion(), path)));
                break;
            default:
                LOG.warn("unknown type " + type);
                break;
        }
    }

    private void pRequest2TxnCreate(int type, Request request, Record record, boolean deserialize)
            throws IOException, KeeperException {
        // 是否需要反序列化，如果需要则将请求转换到record中
        if (deserialize) {
            ByteBufferInputStream.byteBuffer2Record(request.request, record);
        }

        // 标记，该标记表示了创建类型
        int flags;
        // 节点地址
        String path;
        // 权限信息
        List<ACL> acl;
        // 数据信息
        byte[] data;
        // 过期时间
        long ttl;
        // 类型为创建具备过期时间的节点
        if (type == OpCode.createTTL) {
            CreateTTLRequest createTtlRequest = (CreateTTLRequest) record;
            flags = createTtlRequest.getFlags();
            path = createTtlRequest.getPath();
            acl = createTtlRequest.getAcl();
            data = createTtlRequest.getData();
            ttl = createTtlRequest.getTtl();
        }
        else {
            CreateRequest createRequest = (CreateRequest) record;
            flags = createRequest.getFlags();
            path = createRequest.getPath();
            acl = createRequest.getAcl();
            data = createRequest.getData();
            ttl = -1;
        }
        // 将 flags 转换为创建类型
        CreateMode createMode = CreateMode.fromFlag(flags);
        // 验证创建请求是否合法
        validateCreateRequest(path, createMode, request, ttl);
        // 验证创建地址
        String parentPath = validatePathForCreate(path, request.sessionId);

        // 处理ACL
        List<ACL> listACL = fixupACL(path, request.authInfo, acl);
        // 获取父节点的数据记录
        ChangeRecord parentRecord = getRecordForPath(parentPath);

        // 验证ACL
        checkACL(zks, parentRecord.acl, ZooDefs.Perms.CREATE, request.authInfo);
        // 获取父节点的cversion(创建版本号)
        int parentCVersion = parentRecord.stat.getCversion();
        // 是否是顺序创建
        if (createMode.isSequential()) {
            path = path + String.format(Locale.ENGLISH, "%010d", parentCVersion);
        }
        // 验证路径是否合法
        validatePath(path, request.sessionId);
        try {
            // 如果当前路径已经存在相关档案抛出异常
            if (getRecordForPath(path) != null) {
                throw new KeeperException.NodeExistsException(path);
            }
        } catch (KeeperException.NoNodeException e) {
            // ignore this one
        }
        // 是否是临时节点
        boolean ephemeralParent =
                EphemeralType.get(parentRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL;
        if (ephemeralParent) {
            throw new KeeperException.NoChildrenForEphemeralsException(path);
        }
        // 计算新的创建版本号
        int newCversion = parentRecord.stat.getCversion() + 1;
        // 如果操作类型是创建容器则将txn设置为CreateContainerTxn
        if (type == OpCode.createContainer) {
            request.setTxn(new CreateContainerTxn(path, data, listACL, newCversion));
        }
        // 如果操作类型是创建过期节点则将txn设置为CreateTTLTxn
        else if (type == OpCode.createTTL) {
            request.setTxn(new CreateTTLTxn(path, data, listACL, newCversion, ttl));
        }
        // 其他创建相关的操作类型将txn设置为CreateTxn
        else {
            request.setTxn(new CreateTxn(path, data, listACL, createMode.isEphemeral(),
                    newCversion));
        }
        StatPersisted s = new StatPersisted();
        if (createMode.isEphemeral()) {
            s.setEphemeralOwner(request.sessionId);
        }
        // 拷贝父节点数据
        parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
        // 父节点中子节点数量加一
        parentRecord.childCount++;
        // 设置创建版本号
        parentRecord.stat.setCversion(newCversion);
        // 添加变化档案
        addChangeRecord(parentRecord);
        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, s, 0, listACL));
    }

    private void validatePath(String path, long sessionId) throws BadArgumentsException {
        try {
            PathUtils.validatePath(path);
        } catch (IllegalArgumentException ie) {
            LOG.info("Invalid path {} with session 0x{}, reason: {}",
                    path, Long.toHexString(sessionId), ie.getMessage());
            throw new BadArgumentsException(path);
        }
    }

    private String getParentPathAndValidate(String path)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1
                || zks.getZKDatabase().isSpecialPath(path)) {
            throw new BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    protected void pRequest(Request request) throws RequestProcessorException {
        // LOG.info("Prep>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        request.setHdr(null);
        request.setTxn(null);

        try {
            switch (request.type) {
                case OpCode.createContainer:
                case OpCode.create:
                case OpCode.create2:
                    CreateRequest create2Request = new CreateRequest();
                    pRequest2Txn(request.type, zks.getNextZxid(), request, create2Request, true);
                    break;
                case OpCode.createTTL:
                    CreateTTLRequest createTtlRequest = new CreateTTLRequest();
                    pRequest2Txn(request.type, zks.getNextZxid(), request, createTtlRequest, true);
                    break;
                case OpCode.deleteContainer:
                case OpCode.delete:
                    DeleteRequest deleteRequest = new DeleteRequest();
                    pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest, true);
                    break;
                case OpCode.setData:
                    SetDataRequest setDataRequest = new SetDataRequest();
                    pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest, true);
                    break;
                case OpCode.reconfig:
                    ReconfigRequest reconfigRequest = new ReconfigRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request, reconfigRequest);
                    pRequest2Txn(request.type, zks.getNextZxid(), request, reconfigRequest, true);
                    break;
                case OpCode.setACL:
                    SetACLRequest setAclRequest = new SetACLRequest();
                    pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest, true);
                    break;
                case OpCode.check:
                    CheckVersionRequest checkRequest = new CheckVersionRequest();
                    pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest, true);
                    break;
                case OpCode.multi:
                    // 创建多事务请求对象
                    MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                    try {
                        // 请求对象转换为多事务请求对象
                        ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);
                    } catch (IOException e) {
                        // 异常设置头信息
                        request.setHdr(
                                new TxnHeader(request.sessionId, request.cxid, zks.getNextZxid(),
                                        Time.currentWallTime(), OpCode.multi));
                        throw e;
                    }
                    List<Txn> txns = new ArrayList<Txn>();
                    //Each op in a multi-op must have the same zxid!
                    // 获取下一个zxid
                    long zxid = zks.getNextZxid();
                    // 异常记录变量
                    KeeperException ke = null;

                    //Store off current pending change records in case we need to rollback
                    // 将多事务请求对象转换为map结构，用于后续回滚操作
                    Map<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                    // 循环处理多事务请求对象
                    for (Op op : multiRequest) {
                        // 从单个事务中获取请求信息
                        Record subrequest = op.toRequestRecord();
                        // 操作类型
                        int type;
                        // 档案信息
                        Record txn;

                        /* If we've already failed one of the ops, don't bother
                         * trying the rest as we know it's going to fail and it
                         * would be confusing in the logfiles.
                         */
                        // 如果异常记录变量不为空设置异常相关信息
                        if (ke != null) {
                            type = OpCode.error;
                            txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                        }

                        /* Prep the request and convert to a Txn */
                        else {
                            try {
                                // 执行核心请求转事务逻辑
                                pRequest2Txn(op.getType(), zxid, request, subrequest, false);
                                // 记录操作类型
                                type = request.getHdr().getType();
                                // 记录txn
                                txn = request.getTxn();
                            }
                            // 异常相关信息记录
                            catch (KeeperException e) {

                                ke = e;
                                type = OpCode.error;
                                txn = new ErrorTxn(e.code().intValue());

                                if (e.code().intValue() > Code.APIERROR.intValue()) {
                                    LOG.info(
                                            "Got user-level KeeperException when processing {} aborting"
                                                    +
                                                    " remaining multi ops. Error Path:{} Error:{}",
                                            request.toString(), e.getPath(), e.getMessage());
                                }

                                request.setException(e);

                                /* Rollback change records from failed multi-op */
                                rollbackPendingChanges(zxid, pendingChanges);
                            }
                        }

                        //FIXME: I don't want to have to serialize it here and then
                        //       immediately deserialize in next processor. But I'm
                        //       not sure how else to get the txn stored into our list.

                        // 输出档案处理
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                        txn.serialize(boa, "request");
                        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                        txns.add(new Txn(type, bb.array()));
                    }

                    request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                            Time.currentWallTime(), request.type));
                    request.setTxn(new MultiTxn(txns));

                    break;

                //create/close session don't require request record
                case OpCode.createSession:
                case OpCode.closeSession:
                    // 不是本地session
                    if (!request.isLocalSession()) {
                        pRequest2Txn(request.type, zks.getNextZxid(), request,
                                null, true);
                    }
                    break;

                //All the rest don't need to create a Txn - just verify session
                case OpCode.sync:
                case OpCode.exists:
                case OpCode.getData:
                case OpCode.getACL:
                case OpCode.getChildren:
                case OpCode.getChildren2:
                case OpCode.ping:
                case OpCode.setWatches:
                case OpCode.checkWatches:
                case OpCode.removeWatches:
                    // 检查session是否合法
                    zks.sessionTracker.checkSession(request.sessionId,
                            request.getOwner());
                    break;
                default:
                    LOG.warn("unknown type " + request.type);
                    break;
            }
        } catch (KeeperException e) {
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(e.code().intValue()));
            }

            if (e.code().intValue() > Code.APIERROR.intValue()) {
                LOG.info("Got user-level KeeperException when processing {} Error Path:{} Error:{}",
                        request.toString(), e.getPath(), e.getMessage());
            }
            request.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if (bb != null) {
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(Code.MARSHALLINGERROR.intValue()));
            }
        }
        request.zxid = zks.getZxid();
        nextProcessor.processRequest(request);
    }

    private List<ACL> removeDuplicates(List<ACL> acl) {

        LinkedList<ACL> retval = new LinkedList<ACL>();
        for (ACL a : acl) {
            if (!retval.contains(a)) {
                retval.add(a);
            }
        }
        return retval;
    }

    private void validateCreateRequest(String path,
                                       CreateMode createMode,
                                       Request request,
                                       long ttl)
            throws KeeperException {
        // 创建模型是否是TTL的
        // 是否启用临时节点的拓展属性
        if (createMode.isTTL() && !EphemeralType.extendedEphemeralTypesEnabled()) {
            throw new KeeperException.UnimplementedException();
        }
        try {
            // 验证TTL是否合法
            EphemeralType.validateTTL(createMode, ttl);
        } catch (IllegalArgumentException e) {
            throw new BadArgumentsException(path);
        }
        // 创建模型属于临时的
        if (createMode.isEphemeral()) {
            // Exception is set when local session failed to upgrade
            // so we just need to report the error
            // 请求中是否携带异常
            if (request.getException() != null) {
                throw request.getException();
            }
            // 会话跟踪器验证会话
            zks.sessionTracker.checkGlobalSession(request.sessionId,
                    request.getOwner());
        }
        else {
            // 会话跟踪器验证会话
            zks.sessionTracker.checkSession(request.sessionId,
                    request.getOwner());
        }
    }

    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection
     * @param acls list of ACLs being assigned to the node (create or setACL operation)
     * @return verified and expanded ACLs
     * @throws KeeperException.InvalidACLException
     */
    private List<ACL> fixupACL(String path, List<Id> authInfo, List<ACL> acls)
            throws KeeperException.InvalidACLException {
        // check for well formed ACLs
        // This resolves https://issues.apache.org/jira/browse/ZOOKEEPER-1877
        // 将参数 acls 中重复的数据进行过滤
        List<ACL> uniqacls = removeDuplicates(acls);
        // 返回结果值存储容器
        LinkedList<ACL> rv = new LinkedList<ACL>();
        // 如果过滤后的ACL数量为空将抛出异常
        if (uniqacls == null || uniqacls.size() == 0) {
            throw new KeeperException.InvalidACLException(path);
        }
        // 遍历过滤后的acl集合
        for (ACL a : uniqacls) {
            LOG.debug("Processing ACL: {}", a);
            // 如果当前元素为空抛出异常
            if (a == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            // 提取id
            Id id = a.getId();
            // id 为空或者id中存储的方案为空抛出异常
            if (id == null || id.getScheme() == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            // 如果id中存储的方案是world并且id中的id变量为anyone加入到结果集合中
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                rv.add(a);
            }
            // 如果id中存储的方案是auth则进行验证逻辑
            else if (id.getScheme().equals("auth")) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor
                // 身份认证通过标记
                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    // 根据安全方案获取安全验证器
                    AuthenticationProvider ap =
                            ProviderRegistry.getProvider(cid.getScheme());

                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for "
                                + cid.getScheme());
                    }
                    // 通过身份验证
                    else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        rv.add(new ACL(a.getPerms(), cid));
                    }
                }
                // 是否认证通过标记为false抛出异常
                if (!authIdValid) {
                    throw new KeeperException.InvalidACLException(path);
                }
            }
            else {
                // 根据安全方案获取安全验证器
                AuthenticationProvider ap = ProviderRegistry.getProvider(id.getScheme());
                // 安全验证器为空或者id验证不通过抛出异常
                if (ap == null || !ap.isValid(id.getId())) {
                    throw new KeeperException.InvalidACLException(path);
                }
                rv.add(a);
            }
        }
        return rv;
    }

    public void processRequest(Request request) {
        submittedRequests.add(request);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}
