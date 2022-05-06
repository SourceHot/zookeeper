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

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.*;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.quorum.QuorumZooKeeperServer;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * This Request processor actually applies any transaction associated with a
 * request and services any queries. It is always at the end of a
 * RequestProcessor chain (hence the name), so it does not have a nextProcessor
 * member.
 *
 * This RequestProcessor counts on ZooKeeperServer to populate the
 * outstandingRequests member of ZooKeeperServer.
 *
 * 最终处理器
 */
public class FinalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FinalRequestProcessor.class);

    ZooKeeperServer zks;

    public FinalRequestProcessor(ZooKeeperServer zks) {
        this.zks = zks;
    }

    public void processRequest(Request request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        // 日志记录
        // request.addRQRec(">final");
        long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
        if (request.type == OpCode.ping) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logRequest(LOG, traceMask, 'E', request, "");
        }
        // 事务处理结果
        ProcessTxnResult rc = null;
        synchronized (zks.outstandingChanges) {
            // Need to process local session requests
            // Zookeeper进行请求处理
            rc = zks.processTxn(request);

            // request.hdr is set for write requests, which are the only ones
            // that add to outstandingChanges.
            // 请求中头信息不为空
            if (request.getHdr() != null) {
                // 获取头信息
                TxnHeader hdr = request.getHdr();
                // 获取请求中的档案
                Record txn = request.getTxn();
                // 从头信息中获取zxid
                long zxid = hdr.getZxid();
                // 1. 没有完成的变化档案集合不为空
                // 2. 从没有完成的变化档案集合获取一个档案并将其zxid小于当前zxid
                while (!zks.outstandingChanges.isEmpty()
                        && zks.outstandingChanges.peek().zxid <= zxid) {
                    // 没有完成的变化档案集合中移除一个数据
                    ChangeRecord cr = zks.outstandingChanges.remove();
                    // 警告日志
                    if (cr.zxid < zxid) {
                        LOG.warn("Zxid outstanding " + cr.zxid
                                + " is less than current " + zxid);
                    }
                    // 从未完成的变换档案中根据移除的数据地址获取档案，若档案相同则从未完成的变换档案中移除数据
                    if (zks.outstandingChangesForPath.get(cr.path) == cr) {
                        zks.outstandingChangesForPath.remove(cr.path);
                    }
                }
            }

            // do not add non quorum packets to the queue.
            // 如果是投票数据，添加到提案数据中
            if (request.isQuorum()) {
                zks.getZKDatabase().addCommittedProposal(request);
            }
        }

        // ZOOKEEPER-558:
        // In some cases the server does not close the connection (e.g., closeconn buffer
        // was not being queued — ZOOKEEPER-558) properly. This happens, for example,
        // when the client closes the connection. The server should still close the session, though.
        // Calling closeSession() after losing the cnxn, results in the client close session response being dropped.

        // 1. 请求类型是关闭session
        // 2. 连接对象为空
        if (request.type == OpCode.closeSession && connClosedByClient(request)) {
            // We need to check if we can close the session id.
            // Sometimes the corresponding ServerCnxnFactory could be null because
            // we are just playing diffs from the leader.
            // 执行关闭session操作
            if (closeSession(zks.serverCnxnFactory, request.sessionId) ||
                    closeSession(zks.secureServerCnxnFactory, request.sessionId)) {
                return;
            }
        }

        // 连接对象为空结束处理
        if (request.cnxn == null) {
            return;
        }
        // 获取连接对象
        ServerCnxn cnxn = request.cnxn;

        // 最后一个操作标记，初始化为NA
        String lastOp = "NA";
        // 请求处理数量减一
        zks.decInProcess();
        // 状态码
        Code err = Code.OK;
        // 处理结果
        Record rsp = null;
        try {
            // 1. 头信息不为空
            // 2. 头信息类型是error
            if (request.getHdr() != null && request.getHdr().getType() == OpCode.error) {
                /*
                 * When local session upgrading is disabled, leader will
                 * reject the ephemeral node creation due to session expire.
                 * However, if this is the follower that issue the request,
                 * it will have the correct error code, so we should use that
                 * and report to user
                 */
                // 请求中的异常不为空将其抛出，反之则新建一个异常抛出
                if (request.getException() != null) {
                    throw request.getException();
                } else {
                    throw KeeperException.create(KeeperException.Code
                            .get(((ErrorTxn) request.getTxn()).getErr()));
                }
            }

            // 获取请求对象中的异常
            KeeperException ke = request.getException();
            // 1. 异常不为空
            // 2. 请求类型是multi
            if (ke != null && request.type != OpCode.multi) {
                throw ke;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}", request);
            }
            // 根据不同请求类型做出不同操作
            switch (request.type) {
                case OpCode.ping: {

                    // 根据请求创建时间计算服务统计中的一些变量
                    zks.serverStats().updateLatency(request.createTime);

                    // 操作标记设置为 PING
                    lastOp = "PING";
                    // 更新响应统计
                    cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp,
                            request.createTime, Time.currentElapsedTime());

                    // 发送响应
                    cnxn.sendResponse(new ReplyHeader(-2,
                                    zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null,
                            "response");
                    return;
                }
                case OpCode.createSession: {
                    // 根据请求创建时间计算服务统计中的一些变量
                    zks.serverStats().updateLatency(request.createTime);

                    // 操作标记设置为 SESS
                    lastOp = "SESS";
                    // 更新响应统计
                    cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp,
                            request.createTime, Time.currentElapsedTime());

                    // 完成会话创建
                    zks.finishSessionInit(request.cnxn, true);
                    return;
                }
                case OpCode.multi: {
                    // 操作标记设置为 MULT
                    lastOp = "MULT";
                    // 创建响应请求
                    rsp = new MultiResponse();

                    // 循环处理结果
                    for (ProcessTxnResult subTxnResult : rc.multiResult) {

                        OpResult subResult;

                        // 根据不同处理类型做创建不同的响应对象并将其放入到响应结果中
                        switch (subTxnResult.type) {
                            case OpCode.check:
                                subResult = new CheckResult();
                                break;
                            case OpCode.create:
                                subResult = new CreateResult(subTxnResult.path);
                                break;
                            case OpCode.create2:
                            case OpCode.createTTL:
                            case OpCode.createContainer:
                                subResult = new CreateResult(subTxnResult.path, subTxnResult.stat);
                                break;
                            case OpCode.delete:
                            case OpCode.deleteContainer:
                                subResult = new DeleteResult();
                                break;
                            case OpCode.setData:
                                subResult = new SetDataResult(subTxnResult.stat);
                                break;
                            case OpCode.error:
                                subResult = new ErrorResult(subTxnResult.err);
                                break;
                            default:
                                throw new IOException("Invalid type of op");
                        }

                        ((MultiResponse) rsp).add(subResult);
                    }

                    break;
                }
                case OpCode.create: {
                    // 操作标记设置为 CREA
                    lastOp = "CREA";
                    rsp = new CreateResponse(rc.path);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.create2:
                case OpCode.createTTL:
                case OpCode.createContainer: {
                    lastOp = "CREA";
                    rsp = new Create2Response(rc.path, rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.delete:
                case OpCode.deleteContainer: {
                    lastOp = "DELE";
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.setData: {
                    lastOp = "SETD";
                    rsp = new SetDataResponse(rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.reconfig: {
                    lastOp = "RECO";
                    rsp = new GetDataResponse(
                            ((QuorumZooKeeperServer) zks).self.getQuorumVerifier().toString()
                                    .getBytes(), rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.setACL: {
                    lastOp = "SETA";
                    rsp = new SetACLResponse(rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.closeSession: {
                    lastOp = "CLOS";
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.sync: {
                    lastOp = "SYNC";
                    // 创建同步请求
                    SyncRequest syncRequest = new SyncRequest();
                    // 请求对象中的数据反序列化到同步请求中
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            syncRequest);
                    // 响应结果
                    rsp = new SyncResponse(syncRequest.getPath());
                    break;
                }
                case OpCode.check: {
                    lastOp = "CHEC";
                    rsp = new SetDataResponse(rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.exists: {
                    lastOp = "EXIS";
                    // TODO we need to figure out the security requirement for this!
                    ExistsRequest existsRequest = new ExistsRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            existsRequest);
                    String path = existsRequest.getPath();
                    if (path.indexOf('\0') != -1) {
                        throw new KeeperException.BadArgumentsException();
                    }
                    // 获取节点统计信息
                    Stat stat = zks.getZKDatabase().statNode(path, existsRequest
                            .getWatch() ? cnxn : null);
                    rsp = new ExistsResponse(stat);
                    break;
                }
                case OpCode.getData: {
                    lastOp = "GETD";
                    GetDataRequest getDataRequest = new GetDataRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getDataRequest);
                    // 获取地址对应的数据节点
                    DataNode n = zks.getZKDatabase().getNode(getDataRequest.getPath());
                    // 数据节点为空抛出异常
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    // 验证ACL
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ,
                            request.authInfo);
                    // 统计信息处理
                    Stat stat = new Stat();
                    byte b[] = zks.getZKDatabase().getData(getDataRequest.getPath(), stat,
                            getDataRequest.getWatch() ? cnxn : null);
                    rsp = new GetDataResponse(b, stat);
                    break;
                }
                case OpCode.setWatches: {
                    lastOp = "SETW";
                    SetWatches setWatches = new SetWatches();
                    // XXX We really should NOT need this!!!!
                    request.request.rewind();
                    ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
                    long relativeZxid = setWatches.getRelativeZxid();
                    // 设置观察信息
                    zks.getZKDatabase().setWatches(relativeZxid,
                            setWatches.getDataWatches(),
                            setWatches.getExistWatches(),
                            setWatches.getChildWatches(), cnxn);
                    break;
                }
                case OpCode.getACL: {
                    lastOp = "GETA";
                    GetACLRequest getACLRequest = new GetACLRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getACLRequest);
                    // 获取当前请求中地址对应的数据节点
                    DataNode n = zks.getZKDatabase().getNode(getACLRequest.getPath());
                    // 数据节点为空抛出异常
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    // 验证ACL
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ | ZooDefs.Perms.ADMIN,
                            request.authInfo);

                    // 处理统计信息
                    Stat stat = new Stat();
                    // 获取当前请求地址对应的ACL数据集合
                    List<ACL> acl =
                            zks.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                    try {
                        // 验证ACL
                        PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                                ZooDefs.Perms.ADMIN,
                                request.authInfo);
                        // 组装响应
                        rsp = new GetACLResponse(acl, stat);
                    }
                    // 异常情况下组装响应
                    catch (KeeperException.NoAuthException e) {
                        List<ACL> acl1 = new ArrayList<ACL>(acl.size());
                        for (ACL a : acl) {
                            if ("digest".equals(a.getId().getScheme())) {
                                Id id = a.getId();
                                Id id1 = new Id(id.getScheme(), id.getId().replaceAll(":.*", ":x"));
                                acl1.add(new ACL(a.getPerms(), id1));
                            } else {
                                acl1.add(a);
                            }
                        }
                        rsp = new GetACLResponse(acl1, stat);
                    }
                    break;
                }
                case OpCode.getChildren: {
                    lastOp = "GETC";
                    GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getChildrenRequest);
                    // 获取当前请求中地址对应的数据节点
                    DataNode n = zks.getZKDatabase().getNode(getChildrenRequest.getPath());
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    // 验证ACL
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ,
                            request.authInfo);
                    // 获取子节点地址
                    List<String> children = zks.getZKDatabase().getChildren(
                            getChildrenRequest.getPath(), null, getChildrenRequest
                                    .getWatch() ? cnxn : null);
                    rsp = new GetChildrenResponse(children);
                    break;
                }
                case OpCode.getChildren2: {
                    lastOp = "GETC";
                    GetChildren2Request getChildren2Request = new GetChildren2Request();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getChildren2Request);
                    Stat stat = new Stat();
                    // 获取当前请求中地址对应的数据节点
                    DataNode n = zks.getZKDatabase().getNode(getChildren2Request.getPath());
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    // 验证ACL
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ,
                            request.authInfo);
                    // 获取子节点地址
                    List<String> children = zks.getZKDatabase().getChildren(
                            getChildren2Request.getPath(), stat, getChildren2Request
                                    .getWatch() ? cnxn : null);
                    rsp = new GetChildren2Response(children, stat);
                    break;
                }
                // 检查观察者
                case OpCode.checkWatches: {
                    lastOp = "CHKW";
                    CheckWatchesRequest checkWatches = new CheckWatchesRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            checkWatches);
                    // 将请求中的观察类型转换为WatcherType类
                    WatcherType type = WatcherType.fromInt(checkWatches.getType());
                    // 判断当前节点是否具备观察者
                    boolean containsWatcher = zks.getZKDatabase().containsWatcher(
                            checkWatches.getPath(), type, cnxn);
                    // 不存在观察者抛出异常
                    if (!containsWatcher) {
                        String msg = String.format(Locale.ENGLISH, "%s (type: %s)",
                                checkWatches.getPath(), type);
                        throw new KeeperException.NoWatcherException(msg);
                    }
                    break;
                }
                // 移除观察者
                case OpCode.removeWatches: {
                    lastOp = "REMW";
                    RemoveWatchesRequest removeWatches = new RemoveWatchesRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            removeWatches);
                    // 将请求中的观察类型转换为WatcherType类
                    WatcherType type = WatcherType.fromInt(removeWatches.getType());
                    // 移除当前节点中的观察者
                    boolean removed = zks.getZKDatabase().removeWatch(
                            removeWatches.getPath(), type, cnxn);
                    if (!removed) {
                        String msg = String.format(Locale.ENGLISH, "%s (type: %s)",
                                removeWatches.getPath(), type);
                        throw new KeeperException.NoWatcherException(msg);
                    }
                    break;
                }
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        // 获取最后的zxid
        long lastZxid = zks.getZKDatabase().getDataTreeLastProcessedZxid();
        // 创建头信息
        ReplyHeader hdr =
                new ReplyHeader(request.cxid, lastZxid, err.intValue());

        // 根据请求创建时间计算服务统计中的一些变量
        zks.serverStats().updateLatency(request.createTime);
        // 更新统计信息
        cnxn.updateStatsForResponse(request.cxid, lastZxid, lastOp,
                request.createTime, Time.currentElapsedTime());

        try {
            // 发送响应
            cnxn.sendResponse(hdr, rsp, "response");
            // 如果请求类型是关闭session执行关闭操作
            if (request.type == OpCode.closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG", e);
        }
    }

    private boolean closeSession(ServerCnxnFactory serverCnxnFactory, long sessionId) {
        if (serverCnxnFactory == null) {
            return false;
        }
        return serverCnxnFactory.closeSession(sessionId);
    }

    private boolean connClosedByClient(Request request) {
        return request.cnxn == null;
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

}
