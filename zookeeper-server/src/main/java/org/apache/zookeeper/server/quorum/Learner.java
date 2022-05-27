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
package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.apache.jute.*;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication. 
 */
public class Learner {
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);
    static final private boolean nodelay =
            System.getProperty("follower.nodelay", "true").equals("true");

    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations =
            new ConcurrentHashMap<Long, ServerCnxn>();
    protected BufferedOutputStream bufferedOutput;
    protected Socket sock;
    protected InputArchive leaderIs;
    protected OutputArchive leaderOs;
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;
    QuorumPeer self;
    LearnerZooKeeperServer zk;

    /**
     * Socket getter
     * @return
     */
    public Socket getSocket() {
        return sock;
    }

    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }

    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout)
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos
                .toByteArray(), null);
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "To validate session 0x"
                            + Long.toHexString(clientId));
        }
        writePacket(qp, true);
    }

    /**
     * write a packet to the leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet");
        }
        if (LOG.isTraceEnabled()) {
            final long traceMask =
                    (pp.getType() == Leader.PING) ? ZooTrace.SERVER_PING_TRACE_MASK
                            : ZooTrace.SERVER_PACKET_TRACE_MASK;

            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }

    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos
                .toByteArray(), request.authInfo);
        writePacket(qp, true);
    }

    /**
     * Returns the address of the node we think is the leader.
     */
    protected QuorumServer findLeader() {
        QuorumServer leaderServer = null;
        // Find the leader by id
        // 获取选票
        Vote current = self.getCurrentVote();
        // 循环参选节点
        for (QuorumServer s : self.getView().values()) {
            // 如果参选节点的id和选票中的id对应，那么他就是leader
            if (s.id == current.getId()) {
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return leaderServer;
    }

    /**
     * Overridable helper method to return the System.nanoTime().
     * This method behaves identical to System.nanoTime().
     */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Overridable helper method to simply call sock.connect(). This can be
     * overriden in tests to fake connection success/failure for connectToLeader.
     */
    protected void sockConnect(Socket sock, InetSocketAddress addr, int timeout)
            throws IOException {
        sock.connect(addr, timeout);
    }

    /**
     * Establish a connection with the Leader found by findLeader. Retries
     * until either initLimit time has elapsed or 5 tries have happened.
     * @param addr - the address of the Leader to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * <li>if there is an authentication failure while connecting to leader</li>
     * @throws ConnectException
     * @throws InterruptedException
     */
    protected void connectToLeader(InetSocketAddress addr, String hostname)
            throws IOException, InterruptedException, X509Exception {
        this.sock = createSocket();

        int initLimitTime = self.tickTime * self.initLimit;
        int remainingInitLimitTime = initLimitTime;
        long startNanoTime = nanoTime();

        for (int tries = 0; tries < 5; tries++) {
            try {
                // recalculate the init limit time because retries sleep for 1000 milliseconds
                remainingInitLimitTime =
                        initLimitTime - (int) ((nanoTime() - startNanoTime) / 1000000);
                if (remainingInitLimitTime <= 0) {
                    LOG.error("initLimit exceeded on retries.");
                    throw new IOException("initLimit exceeded on retries.");
                }

                sockConnect(sock, addr,
                        Math.min(self.tickTime * self.syncLimit, remainingInitLimitTime));
                if (self.isSslQuorum()) {
                    ((SSLSocket) sock).startHandshake();
                }
                sock.setTcpNoDelay(nodelay);
                break;
            } catch (IOException e) {
                remainingInitLimitTime =
                        initLimitTime - (int) ((nanoTime() - startNanoTime) / 1000000);

                if (remainingInitLimitTime <= 1000) {
                    LOG.error("Unexpected exception, initLimit exceeded. tries=" + tries +
                            ", remaining init limit=" + remainingInitLimitTime +
                            ", connecting to " + addr, e);
                    throw e;
                } else if (tries >= 4) {
                    LOG.error("Unexpected exception, retries exceeded. tries=" + tries +
                            ", remaining init limit=" + remainingInitLimitTime +
                            ", connecting to " + addr, e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries=" + tries +
                            ", remaining init limit=" + remainingInitLimitTime +
                            ", connecting to " + addr, e);
                    this.sock = createSocket();
                }
            }
            Thread.sleep(1000);
        }

        self.authLearner.authenticate(sock, hostname);

        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(
                sock.getInputStream()));
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
    }

    private Socket createSocket() throws X509Exception, IOException {
        Socket sock;
        if (self.isSslQuorum()) {
            sock = self.getX509Util().createSSLSocket();
        } else {
            sock = new Socket();
        }
        sock.setSoTimeout(self.tickTime * self.initLimit);
        return sock;
    }

    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection.
     *
     * 将自己注册到领导者上
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException {
        /*
         * Send follower info, including last zxid and sid
         */
        // 获取最后操作的zxid
        long lastLoggedZxid = self.getLastLoggedZxid();
        // 创建数据包
        QuorumPacket qp = new QuorumPacket();
        // 设置数据包类型
        qp.setType(pktType);
        // 设置zxid
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));

        /*
         * Add sid to payload
         */
        // 创建LearnerInfo对象
        LearnerInfo li =
                new LearnerInfo(self.getId(), 0x10000, self.getQuorumVerifier().getVersion());
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo");
        qp.setData(bsid.toByteArray());

        // 写出数据包，FOLLOWERINFO或者OBSERVERINFO
        writePacket(qp, true);
        // 读取数据包，读取内容LeaderInfo
        readPacket(qp);
        // 从数据包中获取新的选举周期
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
        // 如果数据包类型是LEADERINFO
        if (qp.getType() == Leader.LEADERINFO) {
            // we are connected to a 1.0 server so accept the new epoch and read the next packet
            // 读取leader中的协议版本
            leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
            byte epochBytes[] = new byte[4];
            final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
            // 如果leader中的周期大于当前节点认可的周期
            if (newEpoch > self.getAcceptedEpoch()) {
                wrappedEpochBytes.putInt((int) self.getCurrentEpoch());
                // 设置当前节点认可的周期为leader中的中期
                self.setAcceptedEpoch(newEpoch);
            }
            // 如果相同
            else if (newEpoch == self.getAcceptedEpoch()) {
                // since we have already acked an epoch equal to the leaders, we cannot ack
                // again, but we still need to send our lastZxid to the leader so that we can
                // sync with it if it does assume leadership of the epoch.
                // the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1);
            }
            // 如果大于抛出异常
            else {
                throw new IOException(
                        "Leaders epoch, " + newEpoch + " is less than accepted epoch, "
                                + self.getAcceptedEpoch());
            }
            // 发送ACKEPOCH数据包
            QuorumPacket ackNewEpoch =
                    new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
            // 写出数据包
            writePacket(ackNewEpoch, true);
            // 将leader中的周期计算为zxid
            return ZxidUtils.makeZxid(newEpoch, 0);
        }
        // 其他类型的数据包
        else {
            // leader中的周期大于当前节点认可的周期覆盖当前节点的任选周期
            if (newEpoch > self.getAcceptedEpoch()) {
                self.setAcceptedEpoch(newEpoch);
            }
            // 如果数据包类型不是NEWLEADER抛出异常
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            // 返回数据包中的zxid
            return qp.getZxid();
        }
    }

    /**
     * Finally, synchronize our history with the Leader.
     * <p>
     * 从leader同步数据
     *
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    protected void syncWithLeader(long newLeaderZxid) throws Exception {
        // 创建ACK数据包
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        // 创建数据包
        QuorumPacket qp = new QuorumPacket();
        // 从参数zxid中获取新的选举周期
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);

        QuorumVerifier newLeaderQV = null;

        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        // For SNAP and TRUNC the snapshot is needed to save that history
        // 是否全量同步标志
        boolean snapshotNeeded = true;
        // 读取数据包
        readPacket(qp);
        // 已经提交的数据包，存储的是zxid
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        // 未提交的数据包
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>();
        synchronized (zk) {
            // 数据包中表示差异化同步
            if (qp.getType() == Leader.DIFF) {
                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                snapshotNeeded = false;
            }
            // 数据包中表示全量同步
            else if (qp.getType() == Leader.SNAP) {
                LOG.info("Getting a snapshot from leader 0x" + Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // db is clear as part of deserializeSnapshot()
                // 从输入流将档案反序列化到zk数据库
                zk.getZKDatabase().deserializeSnapshot(leaderIs);
                // ZOOKEEPER-2819: overwrite config node content extracted
                // from leader snapshot with local config, to avoid potential
                // inconsistency of config node content during rolling restart.
                // 重载配置未启用的情况下需要初始化配置节点
                if (!self.isReconfigEnabled()) {
                    LOG.debug(
                            "Reset config node content from local config after deserialization of snapshot.");
                    zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
                }
                // 读取signature信息
                String signature = leaderIs.readString("signature");
                // 如果signature信息不是BenWasHere抛出异常
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");
                }
                // 设置最后处理的zxid
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());
            }
            // 数据包中表示回滚同步
            else if (qp.getType() == Leader.TRUNC) {
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x"
                        + Long.toHexString(qp.getZxid()));
                // 截断日志
                boolean truncated = zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(13);
                }
                // 设置最后处理的zxid
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

            }
            // 其他情况进行退出
            else {
                LOG.error("Got unexpected packet from leader: {}, exiting ... ",
                        LearnerHandler.packetToString(qp));
                System.exit(13);

            }
            // 初始化配置节点
            zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
            // 创建会话跟踪器
            zk.createSessionTracker();

            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            // 是否是zab1.0之前的标记
            boolean isPreZAB1_0 = true;
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            // 是否写入事务日志
            boolean writeToTxnLog = !snapshotNeeded;
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            // 循环
            while (self.isRunning()) {
                // 读取数据包
                readPacket(qp);
                // 根据数据包做不同操作
                switch (qp.getType()) {

                    case Leader.PROPOSAL:
                        // 创建飞行数据包
                        // 将数据包转换为PacketInFlight对象加入到packetsNotCommitted集合
                        PacketInFlight pif = new PacketInFlight();
                        pif.hdr = new TxnHeader();
                        pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr);
                        if (pif.hdr.getZxid() != lastQueued + 1) {
                            LOG.warn("Got zxid 0x"
                                    + Long.toHexString(pif.hdr.getZxid())
                                    + " expected 0x"
                                    + Long.toHexString(lastQueued + 1));
                        }
                        lastQueued = pif.hdr.getZxid();

                        if (pif.hdr.getType() == OpCode.reconfig) {
                            SetDataTxn setDataTxn = (SetDataTxn) pif.rec;
                            QuorumVerifier qv =
                                    self.configFromString(new String(setDataTxn.getData()));
                            self.setLastSeenQuorumVerifier(qv, true);
                        }

                        packetsNotCommitted.add(pif);
                        break;
                    case Leader.COMMIT:
                    case Leader.COMMITANDACTIVATE:
                        // 从packetsNotCommitted中获取第一个数据
                        pif = packetsNotCommitted.peekFirst();
                        if (pif.hdr.getZxid() == qp.getZxid()
                                && qp.getType() == Leader.COMMITANDACTIVATE) {
                            QuorumVerifier qv = self.configFromString(
                                    new String(((SetDataTxn) pif.rec).getData()));
                            //重载配置，如果重载成功抛出异常
                            boolean majorChange = self.processReconfig(qv,
                                    ByteBuffer.wrap(qp.getData()).getLong(),
                                    qp.getZxid(), true);
                            if (majorChange) {
                                throw new Exception("changes proposed in reconfig");
                            }
                        }
                        // 如果不需要写入事务日志
                        if (!writeToTxnLog) {
                            // 飞行数据包中的zxid和数据包中的zxid不相同记录日志，反之则需要进行事务处理，并将飞行数据包从packetsNotCommitted集合中移除
                            if (pif.hdr.getZxid() != qp.getZxid()) {
                                LOG.warn("Committing " + qp.getZxid() + ", but next proposal is "
                                        + pif.hdr.getZxid());
                            }
                            else {
                                zk.processTxn(pif.hdr, pif.rec);
                                packetsNotCommitted.remove();
                            }
                        }
                        else {
                            // 将数据包中的zxid添加到packetsCommitted集合
                            packetsCommitted.add(qp.getZxid());
                        }
                        break;
                    case Leader.INFORM:
                    case Leader.INFORMANDACTIVATE:
                        // 创建飞行数据包
                        PacketInFlight packet = new PacketInFlight();
                        // 赋值头信息
                        packet.hdr = new TxnHeader();

                        // 数据包类型是INFORMANDACTIVATE
                        if (qp.getType() == Leader.INFORMANDACTIVATE) {
                            ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                            long suggestedLeaderId = buffer.getLong();
                            byte[] remainingdata = new byte[buffer.remaining()];
                            buffer.get(remainingdata);
                            packet.rec = SerializeUtils.deserializeTxn(remainingdata, packet.hdr);
                            QuorumVerifier qv = self.configFromString(
                                    new String(((SetDataTxn) packet.rec).getData()));
                            boolean majorChange =
                                    self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
                            //重载配置，如果重载成功抛出异常
                            if (majorChange) {
                                throw new Exception("changes proposed in reconfig");
                            }
                        }
                        else {
                            packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                            // Log warning message if txn comes out-of-order
                            if (packet.hdr.getZxid() != lastQueued + 1) {
                                LOG.warn("Got zxid 0x"
                                        + Long.toHexString(packet.hdr.getZxid())
                                        + " expected 0x"
                                        + Long.toHexString(lastQueued + 1));
                            }
                            lastQueued = packet.hdr.getZxid();
                        }

                        if (!writeToTxnLog) {
                            // Apply to db directly if we haven't taken the snapshot
                            zk.processTxn(packet.hdr, packet.rec);
                        }
                        else {
                            packetsNotCommitted.add(packet);
                            packetsCommitted.add(qp.getZxid());
                        }

                        break;
                    case Leader.UPTODATE:
                        LOG.info("Learner received UPTODATE message");
                        if (newLeaderQV != null) {
                            //重载配置，如果重载成功抛出异常
                            boolean majorChange =
                                    self.processReconfig(newLeaderQV, null, null, true);
                            if (majorChange) {
                                throw new Exception("changes proposed in reconfig");
                            }
                        }
                        // 如果是zab1.0之前的协议
                        if (isPreZAB1_0) {
                            // 创建快照
                            zk.takeSnapshot();
                            // 设置当前选举周期为新的选举周期
                            self.setCurrentEpoch(newEpoch);
                        }
                        self.setZooKeeperServer(zk);
                        self.adminServer.setZooKeeperServer(zk);
                        // 回到循环开始
                        break outerLoop;
                    case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery
                        // means this is Zab 1.0
                        LOG.info("Learner received NEWLEADER message");
                        // 数据包中的数据不为空并且数据长度大于0 重新设置QuorumVerifier对象
                        if (qp.getData() != null && qp.getData().length > 1) {
                            try {
                                QuorumVerifier qv = self.configFromString(new String(qp.getData()));
                                self.setLastSeenQuorumVerifier(qv, true);
                                newLeaderQV = qv;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        // 如果需要全量同步
                        if (snapshotNeeded) {
                            // 创建快照
                            zk.takeSnapshot();
                        }

                        // 设置当前选举周期为新的选举周期
                        self.setCurrentEpoch(newEpoch);
                        // 设置是否需要写入事务日志为真
                        writeToTxnLog =
                                true; //Anything after this needs to go to the transaction log, not applied directly in memory
                        // 设置是否为ZAB1.0之前的版本为假
                        isPreZAB1_0 = false;
                        // 写出ACK数据包
                        writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                        break;
                }
            }
        }
        // 在ACK数据包中设置zxid
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        // 写出ACK数据包
        writePacket(ack, true);
        // 设置超时时间
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        // zk服务启动
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         *
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch);

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer) zk;
            // 未提交的数据包进行处理
            for (PacketInFlight p : packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            // 对已提交的数据包集合进行提交操作
            for (Long zxid : packetsCommitted) {
                fzk.commit(zxid);
            }
        }
        else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            // 对未提交的数据包进行处理
            for (PacketInFlight p : packetsNotCommitted) {
                // 获取已提交数据包中的第一个zxid
                Long zxid = packetsCommitted.peekFirst();
                // 未处理数据包中的zxid不等于第一个zxid则跳过处理
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                // 从packetsCommitted中移除第一个数据
                packetsCommitted.remove();
                // 创建请求对象
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.setTxn(p.rec);
                request.setHdr(p.hdr);
                // 对请求对象进行处理
                ozk.commitRequest(request);
            }
        }
        else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }

    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations.remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                            + " is valid: " + valid);
        }
    }

    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Map<Long, Integer> touchTable = zk.getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());
        writePacket(qp, true);
    }

    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        self.setZooKeeperServer(null);
        self.closeAllConnections();
        self.adminServer.setZooKeeperServer(null);
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }


    static class PacketInFlight {
        TxnHeader hdr;
        Record rec;
    }
}
