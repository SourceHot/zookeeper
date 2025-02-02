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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
public class LearnerHandler extends ZooKeeperThread {
    /**
     * For testing purpose, force leader to use snapshot to sync with followers
     */
    public static final String FORCE_SNAP_SYNC = "zookeeper.forceSnapshotSync";
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);
    protected final Socket sock;
    final Leader leader;
    /**
     * The packets to be sent to the learner
     * 待发送给学习者的数据包，Follower和Observer称之为学习者
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets =
            new LinkedBlockingQueue<QuorumPacket>();
    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();
    private final BufferedInputStream bufferedInput;
    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;
    protected int version = 0x1;
    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;
    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();


    ;
    private BinaryInputArchive ia;
    private BinaryOutputArchive oa;
    private BufferedOutputStream bufferedOutput;
    /**
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;
    private boolean forceSnapSync = false;
    /**
     * Keep track of whether we need to queue TRUNC or DIFF into packet queue
     * that we are going to blast it to the learner
     */
    private boolean needOpPacket = true;
    /**
     * Last zxid sent to the learner as part of synchronization
     */
    private long leaderLastZxid;
    private LearnerType learnerType = LearnerType.PARTICIPANT;

    LearnerHandler(Socket sock, BufferedInputStream bufferedInput, Leader leader)
            throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        this.bufferedInput = bufferedInput;

        if (Boolean.getBoolean(FORCE_SNAP_SYNC)) {
            forceSnapSync = true;
            LOG.info("Forcing snapshot sync is enabled");
        }

        try {
            if (leader.self != null) {
                leader.self.authServer.authenticate(sock,
                        new DataInputStream(bufferedInput));
            }
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection",
                    sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }
    }

    static public String packetToString(QuorumPacket p) {
        String type;
        String mess = null;

        switch (p.getType()) {
            case Leader.ACK:
                type = "ACK";
                break;
            case Leader.COMMIT:
                type = "COMMIT";
                break;
            case Leader.FOLLOWERINFO:
                type = "FOLLOWERINFO";
                break;
            case Leader.NEWLEADER:
                type = "NEWLEADER";
                break;
            case Leader.PING:
                type = "PING";
                break;
            case Leader.PROPOSAL:
                type = "PROPOSAL";
                TxnHeader hdr = new TxnHeader();
                try {
                    SerializeUtils.deserializeTxn(p.getData(), hdr);
                    // mess = "transaction: " + txn.toString();
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
                ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
                DataInputStream dis = new DataInputStream(bis);
                try {
                    long id = dis.readLong();
                    mess = " sessionid = " + id;
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }

                break;
            case Leader.UPTODATE:
                type = "UPTODATE";
                break;
            case Leader.DIFF:
                type = "DIFF";
                break;
            case Leader.TRUNC:
                type = "TRUNC";
                break;
            case Leader.SNAP:
                type = "SNAP";
                break;
            case Leader.ACKEPOCH:
                type = "ACKEPOCH";
                break;
            case Leader.SYNC:
                type = "SYNC";
                break;
            case Leader.INFORM:
                type = "INFORM";
                break;
            case Leader.COMMITANDACTIVATE:
                type = "COMMITANDACTIVATE";
                break;
            case Leader.INFORMANDACTIVATE:
                type = "INFORMANDACTIVATE";
                break;
            default:
                type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    public Socket getSocket() {
        return sock;
    }

    long getSid() {
        return sid;
    }

    int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch (IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            // 第一部分确认基础数据
            // 向leader对象中添加学习处理程序
            leader.addLearnerHandler(this);
            // 计算截止时间
            tickOfNextAckDeadline = leader.self.tick.get()
                    + leader.self.initLimit + leader.self.syncLimit;

            // 将bufferedInput对象做出类型转换
            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            // 创建数据包
            QuorumPacket qp = new QuorumPacket();
            // 从变量ia中将数据包相关信息读取，反序列化到qp变量
            ia.readRecord(qp, "packet");
            // 如果数据包类型不是FOLLOWERINFO也不是OBSERVERINFO结束处理
            if (qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO) {
                LOG.error("First packet " + qp.toString()
                        + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }

            // 读取数据，数据对象是LearnerInfo，数据是在org.apache.zookeeper.server.quorum.Learner.registerWithLeader中创建的
            byte learnerInfoData[] = qp.getData();
            // 从learnerInfoData中读取sid、version和configVersion
            if (learnerInfoData != null) {
                ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                if (learnerInfoData.length >= 8) {
                    this.sid = bbsid.getLong();
                }
                if (learnerInfoData.length >= 12) {
                    this.version = bbsid.getInt(); // protocolVersion
                }
                if (learnerInfoData.length >= 20) {
                    long configVersion = bbsid.getLong();
                    if (configVersion > leader.self.getQuorumVerifier().getVersion()) {
                        throw new IOException(
                                "Follower is ahead of the leader (has a later activated configuration)");
                    }
                }
            } else {
                // sid等于Follower类型的节点数量减一
                this.sid = leader.followerCounter.getAndDecrement();
            }

            // 日志记录
            if (leader.self.getView().containsKey(this.sid)) {
                LOG.info("Follower sid: " + this.sid + " : info : "
                        + leader.self.getView().get(this.sid).toString());
            } else {
                LOG.info("Follower sid: " + this.sid + " not in the current config "
                        + Long.toHexString(leader.self.getQuorumVerifier().getVersion()));
            }

            // 如果数据包类型是OBSERVERINFO，修改成员变量learnerType
            if (qp.getType() == Leader.OBSERVERINFO) {
                learnerType = LearnerType.OBSERVER;
            }


            // 第二部分不同协议版本号之间的操作
            // 从数据包中根据zxid获取最后接受的epoch
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

            long peerLastZxid;
            StateSummary ss = null;
            // 从数据包中获取zxid
            long zxid = qp.getZxid();
            // 构建新的选举周期
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            // 构建新的zxid
            long newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);


            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information
                // 从zxid
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                // 等待epoch数据包的ack数量过半，ACK数据包从org.apache.zookeeper.server.quorum.Learner.registerWithLeader方法中来
                leader.waitForEpochAck(this.getSid(), ss);
            }
            // 主流版本都将执行这个代码
            else {
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                // 构建 LEADERINFO 数据包并将其发送给其他节点
                QuorumPacket newEpochPacket =
                        new QuorumPacket(Leader.LEADERINFO, newLeaderZxid, ver, null);
                oa.writeRecord(newEpochPacket, "packet");
                bufferedOutput.flush();
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
                }
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                leader.waitForEpochAck(this.getSid(), ss);
            }
            peerLastZxid = ss.getLastZxid();

            // Take any necessary action if we need to send TRUNC or DIFF
            // startForwarding() will be called in all cases
            // 第三部分：确认是否需要全量同步
            // 确认是否需要全量
            boolean needSnap = syncFollower(peerLastZxid, leader.zk.getZKDatabase(), leader);

            /* if we are not truncating or sending a diff just send a snapshot */
            // 需要全量同步的操作
            if (needSnap) {
                boolean exemptFromThrottle = getLearnerType() != LearnerType.OBSERVER;
                LearnerSnapshot snapshot =
                        leader.getLearnerSnapshotThrottler().beginSnapshot(exemptFromThrottle);
                try {
                    // 最后处理的zxid
                    long zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
                    // 发送全量同步数据包
                    oa.writeRecord(new QuorumPacket(Leader.SNAP, zxidToSend, null, null), "packet");
                    bufferedOutput.flush();

                    LOG.info("Sending snapshot last zxid of peer is 0x{}, zxid of leader is 0x{}, "
                                    + "send zxid of db as 0x{}, {} concurrent snapshots, "
                                    + "snapshot was {} from throttle",
                            Long.toHexString(peerLastZxid),
                            Long.toHexString(leaderLastZxid),
                            Long.toHexString(zxidToSend),
                            snapshot.getConcurrentSnapshotNumber(),
                            snapshot.isEssential() ? "exempt" : "not exempt");
                    // Dump data to peer
                    // 将zk数据库的数据序列化到oa变量写出
                    leader.zk.getZKDatabase().serializeSnapshot(oa);
                    oa.writeString("BenWasHere", "signature");
                    bufferedOutput.flush();
                } finally {
                    snapshot.close();
                }
            }

            LOG.debug("Sending NEWLEADER message to " + sid);
            // the version of this quorumVerifier will be set by leader.lead() in case
            // the leader is just being established. waitForEpochAck makes sure that readyToStart is true if
            // we got here, so the version was set

            // 第四部分：NEWLEADER数据包传输
            // 创建NEWLEADER数据包，根据不同版本按照不同方式写出
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                        newLeaderZxid, null, null);
                oa.writeRecord(newLeaderQP, "packet");
            }
            else {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                        newLeaderZxid, leader.self.getLastSeenQuorumVerifier()
                        .toString().getBytes(), null);
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();

            // Start thread that blast packets in the queue to learner
            // 启动数据包发送线程
            startSendingPackets();

            /*
             * Have to wait for the first ACK, wait until
             * the leader is ready, and only then we can
             * start processing messages.
             */

            // 创建一个空的数据包
            qp = new QuorumPacket();
            // 从变量ia中将数据包相关信息读取，反序列化到qp变量
            ia.readRecord(qp, "packet");
            // 数据包类型不是ACK结束处理
            if (qp.getType() != Leader.ACK) {
                LOG.error("Next packet was supposed to be an ACK,"
                        + " but received packet: {}", packetToString(qp));
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Received NEWLEADER-ACK message from " + sid);
            }
            leader.waitForNewLeaderAck(getSid(), qp.getZxid());

            syncLimitCheck.start();

            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit);

            /*
             * Wait until leader starts up
             */
            synchronized (leader.zk) {
                while (!leader.zk.isRunning() && !this.isInterrupted()) {
                    leader.zk.wait(20);
                }
            }
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            //
            LOG.debug("Sending UPTODATE message to " + sid);
            // 添加UPTODATE数据包
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));

            // 第五部分，根据不同数据包进行处理
            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = leader.self.tick.get() + leader.self.syncLimit;


                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                // 不同的数据包类型做出不同的行为操作
                switch (qp.getType()) {
                    // 类型是ACK
                    case Leader.ACK:
                        if (this.learnerType == LearnerType.OBSERVER) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Received ACK from Observer  " + this.sid);
                            }
                        }
                        syncLimitCheck.updateAck(qp.getZxid());
                        // 处理ack数据包
                        leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                        break;
                    // 类型是PING
                    case Leader.PING:
                        // Process the touches
                        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                                .getData());
                        DataInputStream dis = new DataInputStream(bis);
                        while (dis.available() > 0) {
                            long sess = dis.readLong();
                            int to = dis.readInt();
                            leader.zk.touch(sess, to);
                        }
                        break;
                    // 类型是REVALIDATE
                    case Leader.REVALIDATE:
                        bis = new ByteArrayInputStream(qp.getData());
                        dis = new DataInputStream(bis);
                        long id = dis.readLong();
                        int to = dis.readInt();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        DataOutputStream dos = new DataOutputStream(bos);
                        dos.writeLong(id);
                        boolean valid = leader.zk.checkIfValidGlobalSession(id, to);
                        if (valid) {
                            try {
                                //set the session owner
                                // as the follower that
                                // owns the session
                                // 设置所有者，标记leader是谁
                                leader.zk.setOwner(id, this);
                            } catch (SessionExpiredException e) {
                                LOG.error("Somehow session " + Long.toHexString(id) +
                                        " expired right after being renewed! (impossible)", e);
                            }
                        }
                        if (LOG.isTraceEnabled()) {
                            ZooTrace.logTraceMessage(LOG,
                                    ZooTrace.SESSION_TRACE_MASK,
                                    "Session 0x" + Long.toHexString(id)
                                            + " is valid: " + valid);
                        }
                        dos.writeBoolean(valid);
                        qp.setData(bos.toByteArray());
                        // 加入到queuedPackets集合
                        queuedPackets.add(qp);
                        break;
                    // 类型是REQUEST
                    case Leader.REQUEST:
                        bb = ByteBuffer.wrap(qp.getData());
                        sessionId = bb.getLong();
                        cxid = bb.getInt();
                        type = bb.getInt();
                        bb = bb.slice();
                        Request si;
                        if (type == OpCode.sync) {
                            si = new LearnerSyncRequest(this, sessionId, cxid, type, bb,
                                    qp.getAuthinfo());
                        } else {
                            si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                        }
                        si.setOwner(this);
                        // 提交请求
                        leader.zk.submitLearnerRequest(si);
                        break;
                    default:
                        LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                        break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
                //close the socket to make sure the
                //other side can see it being close
                try {
                    sock.close();
                } catch (IOException ie) {
                    // do nothing
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } catch (SnapshotThrottleException e) {
            LOG.error("too many concurrent snapshots: " + e);
        } finally {
            LOG.warn("******* GOODBYE "
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the follower
     */
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName(
                            "Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption " + e.getMessage());
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * Determine if we need to sync with follower using DIFF/TRUNC/SNAP
     * and setup follower to receive packets from commit processor
     *
     * @param peerLastZxid 参选节点操作后的zxid
     * @param db
     * @param leader
     * @return true if snapshot transfer is needed.
     */
    public boolean syncFollower(long peerLastZxid, ZKDatabase db, Leader leader) {
        /*
         * When leader election is completed, the leader will set its
         * lastProcessedZxid to be (epoch < 32). There will be no txn associated
         * with this zxid.
         *
         * The learner will set its lastProcessedZxid to the same value if
         * it get DIFF or SNAP from the leader. If the same learner come
         * back to sync with leader using this zxid, we will never find this
         * zxid in our history. In this case, we will ignore TRUNC logic and
         * always send DIFF if we have old enough history
         */
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // Keep track of the latest zxid which already queued
        long currentZxid = peerLastZxid;
        boolean needSnap = true;
        // 是否开启事务同步日志
        boolean txnLogSyncEnabled = db.isTxnLogSyncEnabled();
        ReentrantReadWriteLock lock = db.getLogLock();
        ReadLock rl = lock.readLock();
        try {
            rl.lock();
            // 最大事务id
            long maxCommittedLog = db.getmaxCommittedLog();
            // 最小事务id
            long minCommittedLog = db.getminCommittedLog();
            // 选举周期 << 32
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();

            LOG.info("Synchronizing with Follower sid: {} maxCommittedLog=0x{}"
                            + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                            + " peerLastZxid=0x{}", getSid(),
                    Long.toHexString(maxCommittedLog),
                    Long.toHexString(minCommittedLog),
                    Long.toHexString(lastProcessedZxid),
                    Long.toHexString(peerLastZxid));

            // 数据库提交日志为空
            if (db.getCommittedLog().isEmpty()) {
                /*
                 * It is possible that committedLog is empty. In that case
                 * setting these value to the latest txn in leader db
                 * will reduce the case that we need to handle
                 *
                 * Here is how each case handle by the if block below
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 */
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }

            /*
             * Here are the cases that we want to handle
             *
             * 1. Force sending snapshot (for testing purpose)
             * 2. Peer and leader is already sync, send empty diff
             * 3. Follower has txn that we haven't seen. This may be old leader
             *    so we need to send TRUNC. However, if peer has newEpochZxid,
             *    we cannot send TRUNC since the follower has no txnlog
             * 4. Follower is within committedLog range or already in-sync.
             *    We may need to send DIFF or TRUNC depending on follower's zxid
             *    We always send empty DIFF if follower is already in-sync
             * 5. Follower missed the committedLog. We will try to use on-disk
             *    txnlog + committedLog to sync with follower. If that fail,
             *    we will send snapshot
             */

            if (forceSnapSync) {
                // Force leader to use snapshot to sync with follower
                LOG.warn("Forcing snapshot sync - should not see this in production");
            }
            // 相等表示Follower没有差异量发送空的DIFF数据
            else if (lastProcessedZxid == peerLastZxid) {
                // Follower is already sync with us, send empty diff
                LOG.info("Sending DIFF zxid=0x" + Long.toHexString(peerLastZxid) +
                        " for peer sid: " + getSid());
                queueOpPacket(Leader.DIFF, peerLastZxid);
                needOpPacket = false;
                // 不需要全量同步
                needSnap = false;
            }
            // Follower 数据比Leader新发送TRUNC
            else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {

                // Newer than committedLog, send trunc and done
                LOG.debug("Sending TRUNC to follower zxidToSend=0x" +
                        Long.toHexString(maxCommittedLog) +
                        " for peer sid:" + getSid());
                queueOpPacket(Leader.TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            }
            // Follower 数据在Leader数据范围内
            else if ((maxCommittedLog >= peerLastZxid)
                    && (minCommittedLog <= peerLastZxid)) {
                // Follower is within commitLog range
                LOG.info("Using committedLog for peer sid: " + getSid());
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                currentZxid = queueCommittedProposals(itr, peerLastZxid,
                        null, maxCommittedLog);
                needSnap = false;
            }
            //
            else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {
                // Use txnlog and committedLog to sync

                // Calculate sizeLimit that we allow to retrieve txnlog from disk
                // 计算事务日志的大小限制
                long sizeLimit = db.calculateTxnLogSizeLimit();
                // This method can return empty iterator if the requested zxid
                // is older than on-disk txnlog
                // 读取一定量的数据
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(
                        peerLastZxid, sizeLimit);
                //
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: " + getSid());
                    // 将数据放入queuedPackets队列
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid,
                            minCommittedLog, maxCommittedLog);

                    LOG.debug("Queueing committedLog 0x" + Long.toHexString(currentZxid));
                    Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                    currentZxid = queueCommittedProposals(committedLogItr, currentZxid,
                            null, maxCommittedLog);
                    needSnap = false;
                }
                // closing the resources
                // 关闭 txnLogItr 资源
                if (txnLogItr instanceof TxnLogProposalIterator) {
                    TxnLogProposalIterator txnProposalItr = (TxnLogProposalIterator) txnLogItr;
                    txnProposalItr.close();
                }
            } else {
                LOG.warn("Unhandled scenario for peer sid: " + getSid());
            }
            LOG.debug("Start forwarding 0x" + Long.toHexString(currentZxid) +
                    " for peer sid: " + getSid());
            // 通知领导者同步完成
            leaderLastZxid = leader.startForwarding(this, currentZxid);
        } finally {
            rl.unlock();
        }

        if (needOpPacket && !needSnap) {
            // This should never happen, but we should fall back to sending
            // snapshot just in case.
            LOG.error("Unhandled scenario for peer sid: " + getSid() +
                    " fall back to use snapshot");
            needSnap = true;
        }

        return needSnap;
    }

    /**
     * Queue committed proposals into packet queue. The range of packets which
     * is going to be queued are (peerLaxtZxid, maxZxid]
     *
     * @param itr  iterator point to the proposals
     * @param peerLastZxid  last zxid seen by the follower
     * @param maxZxid  max zxid of the proposal to queue, null if no limit
     * @param lastCommittedZxid when sending diff, we need to send lastCommittedZxid
     *        on the leader to follow Zab 1.0 protocol.
     * @return last zxid of the queued proposal
     */
    protected long queueCommittedProposals(Iterator<Proposal> itr,
                                           long peerLastZxid,
                                           Long maxZxid,
                                           Long lastCommittedZxid) {
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        long queuedZxid = peerLastZxid;
        // as we look through proposals, this variable keeps track of previous
        // proposal Id.
        long prevProposalZxid = -1;
        while (itr.hasNext()) {
            Proposal propose = itr.next();

            long packetZxid = propose.packet.getZxid();
            // abort if we hit the limit
            if ((maxZxid != null) && (packetZxid > maxZxid)) {
                break;
            }

            // skip the proposals the peer already has
            if (packetZxid < peerLastZxid) {
                prevProposalZxid = packetZxid;
                continue;
            }

            // If we are sending the first packet, figure out whether to trunc
            // or diff
            if (needOpPacket) {

                // Send diff when we see the follower's zxid in our history
                if (packetZxid == peerLastZxid) {
                    LOG.info("Sending DIFF zxid=0x" +
                            Long.toHexString(lastCommittedZxid) +
                            " for peer sid: " + getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                    continue;
                }

                if (isPeerNewEpochZxid) {
                    // Send diff and fall through if zxid is of a new-epoch
                    LOG.info("Sending DIFF zxid=0x" +
                            Long.toHexString(lastCommittedZxid) +
                            " for peer sid: " + getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                } else if (packetZxid > peerLastZxid) {
                    // Peer have some proposals that the leader hasn't seen yet
                    // it may used to be a leader
                    if (ZxidUtils.getEpochFromZxid(packetZxid) !=
                            ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                        // We cannot send TRUNC that cross epoch boundary.
                        // The learner will crash if it is asked to do so.
                        // We will send snapshot this those cases.
                        LOG.warn("Cannot send TRUNC to peer sid: " + getSid() +
                                " peer zxid is from different epoch");
                        return queuedZxid;
                    }

                    LOG.info("Sending TRUNC zxid=0x" +
                            Long.toHexString(prevProposalZxid) +
                            " for peer sid: " + getSid());
                    queueOpPacket(Leader.TRUNC, prevProposalZxid);
                    needOpPacket = false;
                }
            }

            if (packetZxid <= queuedZxid) {
                // We can get here, if we don't have op packet to queue
                // or there is a duplicate txn in a given iterator
                continue;
            }

            // Since this is already a committed proposal, we need to follow
            // it by a commit packet
            queuePacket(propose.packet);
            queueOpPacket(Leader.COMMIT, packetZxid);
            queuedZxid = packetZxid;

        }

        if (needOpPacket && isPeerNewEpochZxid) {
            // We will send DIFF for this kind of zxid in any case. This if-block
            // is the catch when our history older than learner and there is
            // no new txn since then. So we need an empty diff
            LOG.info("Sending DIFF zxid=0x" +
                    Long.toHexString(lastCommittedZxid) +
                    " for peer sid: " + getSid());
            queueOpPacket(Leader.DIFF, lastCommittedZxid);
            needOpPacket = false;
        }

        return queuedZxid;
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */
    public void ping() {
        // If learner hasn't sync properly yet, don't send ping packet
        // otherwise, the learner will crash
        if (!sendingThreadStarted) {
            return;
        }
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            synchronized (leader) {
                id = leader.lastProposed;
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * Queue leader packet of a given type
     * @param type
     * @param zxid
     */
    private void queueOpPacket(int type, long zxid) {
        QuorumPacket packet = new QuorumPacket(type, zxid, null, null);
        queuePacket(packet);
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    public boolean synced() {
        return isAlive()
                && leader.self.tick.get() <= tickOfNextAckDeadline;
    }

    /**
     * For testing, return packet queue
     * @return
     */
    public Queue<QuorumPacket> getQueuedPackets() {
        return queuedPackets;
    }

    /**
     * For testing, we need to reset this value
     */
    public void setFirstPacket(boolean value) {
        needOpPacket = value;
    }


    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
            if (currentZxid == zxid) {
                currentTime = nextTime;
                currentZxid = nextZxid;
                nextTime = 0;
                nextZxid = 0;
            } else if (nextZxid == zxid) {
                LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                nextTime = 0;
                nextZxid = 0;
            }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    }
}
