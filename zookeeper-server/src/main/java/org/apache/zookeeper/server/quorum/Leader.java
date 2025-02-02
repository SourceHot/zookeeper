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

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class has the control logic for the Leader.
 */
public class Leader {
    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;
    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;
    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;
    /**
     * This message is for follower to expect diff
     */
    final static int DIFF = 13;
    /**
     * This is for follower to truncate its logs
     */
    final static int TRUNC = 14;
    /**
     * This is for follower to download the snapshots
     */
    final static int SNAP = 15;
    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;
    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;
    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;
    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    final static int UPTODATE = 12;
    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;
    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;
    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;
    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;
    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;
    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;
    /**
     * This message type informs observers of a committed proposal.
     */
    final static int INFORM = 8;
    /**
     * Similar to COMMIT, only for a reconfig operation.
     */
    final static int COMMITANDACTIVATE = 9;
    /**
     * Similar to INFORM, only for a reconfig operation.
     */
    final static int INFORMANDACTIVATE = 19;
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
    static final private boolean nodelay =
            System.getProperty("leader.nodelay", "true").equals("true");
    // Throttle when there are too many concurrent snapshots being sent to observers
    private static final String MAX_CONCURRENT_SNAPSHOTS =
            "zookeeper.leader.maxConcurrentSnapshots";
    private static final int maxConcurrentSnapshots;
    private static final String MAX_CONCURRENT_SNAPSHOT_TIMEOUT =
            "zookeeper.leader.maxConcurrentSnapshotTimeout";
    private static final long maxConcurrentSnapshotTimeout;

    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static {
        maxConcurrentSnapshots = Integer.getInteger(MAX_CONCURRENT_SNAPSHOTS, 10);
        LOG.info(MAX_CONCURRENT_SNAPSHOTS + " = " + maxConcurrentSnapshots);
        maxConcurrentSnapshotTimeout = Long.getLong(MAX_CONCURRENT_SNAPSHOT_TIMEOUT, 5);
        LOG.info(MAX_CONCURRENT_SNAPSHOT_TIMEOUT + " = " + maxConcurrentSnapshotTimeout);
    }

    // VisibleForTesting
    protected final Proposal newLeaderProposal = new Proposal();
    // VisibleForTesting
    protected final Set<Long> connectingFollowers = new HashSet<Long>();
    // VisibleForTesting
    /**
     * Follower集合
     */
    protected final Set<Long> electingFollowers = new HashSet<Long>();
    final LeaderZooKeeperServer zk;
    final QuorumPeer self;
    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1);
    final ConcurrentMap<Long, Proposal> outstandingProposals =
            new ConcurrentHashMap<Long, Proposal>();
    private final LearnerSnapshotThrottler learnerSnapshotThrottler;
    // list of all the followers
    private final HashSet<LearnerHandler> learners =
            new HashSet<LearnerHandler>();
    private final BufferStats proposalStats;
    // list of followers that are ready to follow (i.e synced with the leader)
    private final HashSet<LearnerHandler> forwardingFollowers =
            new HashSet<LearnerHandler>();
    private final HashSet<LearnerHandler> observingLearners =
            new HashSet<LearnerHandler>();
    // Pending sync requests. Must access under 'this' lock.
    private final HashMap<Long, List<LearnerSyncRequest>> pendingSyncs =
            new HashMap<Long, List<LearnerSyncRequest>>();
    private final ServerSocket ss;
    private final ConcurrentLinkedQueue<Proposal> toBeApplied =
            new ConcurrentLinkedQueue<Proposal>();
    // VisibleForTesting
    protected boolean quorumFormed = false;
    // VisibleForTesting
    protected boolean electionFinished = false;
    // the follower acceptor thread
    volatile LearnerCnxAcceptor cnxAcceptor = null;
    StateSummary leaderStateSummary;
    long epoch = -1;
    boolean waitingForNewEpoch = true;
    // when a reconfig occurs where the leader is removed or becomes an observer,
    // it does not commit ops after committing the reconfig
    boolean allowedToCommit = true;
    boolean isShutdown;
    long lastCommitted = -1;
    long lastProposed;

    Leader(QuorumPeer self, LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new BufferStats();
        try {
            if (self.shouldUsePortUnification() || self.isSslQuorum()) {
                boolean allowInsecureConnection = self.shouldUsePortUnification();
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection,
                            self.getQuorumAddress().getPort());
                } else {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection);
                }
            } else {
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new ServerSocket(self.getQuorumAddress().getPort());
                } else {
                    ss = new ServerSocket();
                }
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk = zk;
        this.learnerSnapshotThrottler = createLearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
            case DIFF:
                return "DIFF";
            case TRUNC:
                return "TRUNC";
            case SNAP:
                return "SNAP";
            case OBSERVERINFO:
                return "OBSERVERINFO";
            case NEWLEADER:
                return "NEWLEADER";
            case FOLLOWERINFO:
                return "FOLLOWERINFO";
            case UPTODATE:
                return "UPTODATE";
            case LEADERINFO:
                return "LEADERINFO";
            case ACKEPOCH:
                return "ACKEPOCH";
            case REQUEST:
                return "REQUEST";
            case PROPOSAL:
                return "PROPOSAL";
            case ACK:
                return "ACK";
            case COMMIT:
                return "COMMIT";
            case COMMITANDACTIVATE:
                return "COMMITANDACTIVATE";
            case PING:
                return "PING";
            case REVALIDATE:
                return "REVALIDATE";
            case SYNC:
                return "SYNC";
            case INFORM:
                return "INFORM";
            case INFORMANDACTIVATE:
                return "INFORMANDACTIVATE";
            default:
                return "UNKNOWN";
        }
    }

    public BufferStats getProposalStats() {
        return proposalStats;
    }

    public LearnerSnapshotThrottler createLearnerSnapshotThrottler(
            int maxConcurrentSnapshots, long maxConcurrentSnapshotTimeout) {
        return new LearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    /**
     * Adds peer to the leader.
     *
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     *
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);
        }
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }
    }

    /**
     * Returns true if a quorum in qv is connected and synced with the leader
     * and false otherwise
     *
     * @param qv, a QuorumVerifier
     */
    public boolean isQuorumSynced(QuorumVerifier qv) {
        HashSet<Long> ids = new HashSet<Long>();
        if (qv.getVotingMembers().containsKey(self.getId()))
            ids.add(self.getId());
        synchronized (forwardingFollowers) {
            for (LearnerHandler learnerHandler : forwardingFollowers) {
                if (learnerHandler.synced() && qv.getVotingMembers()
                        .containsKey(learnerHandler.getSid())) {
                    ids.add(learnerHandler.getSid());
                }
            }
        }
        return qv.containsQuorum(ids);
    }

    /**
     * This method is main function that is called to lead
     *
     * @throws IOException
     * @throws InterruptedException
     */
    void lead() throws IOException, InterruptedException {
        // 获取当前时间
        self.end_fle = Time.currentElapsedTime();
        // 计算选举消耗的时间：当前时间-选举开始时间
        long electionTimeTaken = self.end_fle - self.start_fle;
        // 设置选举消耗的时间
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {} {}", electionTimeTaken,
                QuorumPeer.FLE_TIME_UNIT);
        // 时间计数器归零
        self.start_fle = 0;
        self.end_fle = 0;

        // 注册JMX
        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            self.tick.set(0);
            // 执行数据加载操作
            zk.loadData();

            // 创建状态信息，主要存储参选周期和最后处理的zxid
            leaderStateSummary =
                    new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

            // Start thread that waits for connection requests from
            // new followers.
            // 创建LearnerCnxAcceptor类用于等待Follower发送同步请求
            cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();

            // 计算最新选举周期
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());

            // 将任选周期转换为zxid并将其设置到zk中
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));

            synchronized (this) {
                lastProposed = zk.getZxid();
            }

            // 创建NEWLEADER数据包
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),
                    null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }

            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            QuorumVerifier curQV = self.getQuorumVerifier();
            if (curQV.getVersion() == 0 && curQV.getVersion() == lastSeenQV.getVersion()) {
                // This was added in ZOOKEEPER-1783. The initial config has version 0 (not explicitly
                // specified by the user; the lack of version in a config file is interpreted as version=0).
                // As soon as a config is established we would like to increase its version so that it
                // takes presedence over other initial configs that were not established (such as a config
                // of a server trying to join the ensemble, which may be a partial view of the system, not the full config).
                // We chose to set the new version to the one of the NEWLEADER message. However, before we can do that
                // there must be agreement on the new version, so we can only change the version when sending/receiving UPTODATE,
                // not when sending/receiving NEWLEADER. In other words, we can't change curQV here since its the committed quorum verifier,
                // and there's still no agreement on the new version that we'd like to use. Instead, we use
                // lastSeenQuorumVerifier which is being sent with NEWLEADER message
                // so its a good way to let followers know about the new version. (The original reason for sending
                // lastSeenQuorumVerifier with NEWLEADER is so that the leader completes any potentially uncommitted reconfigs
                // that it finds before starting to propose operations. Here we're reusing the same code path for
                // reaching consensus on the new version number.)

                // It is important that this is done before the leader executes waitForEpochAck,
                // so before LearnerHandlers return from their waitForEpochAck
                // hence before they construct the NEWLEADER message containing
                // the last-seen-quorumverifier of the leader, which we change below
                try {
                    LOG.debug(String.format(
                            "set lastSeenQuorumVerifier to currentQuorumVerifier (%s)",
                            curQV.toString()));
                    QuorumVerifier newQV = self.configFromString(curQV.toString());
                    newQV.setVersion(zk.getZxid());
                    self.setLastSeenQuorumVerifier(newQV, true);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            newLeaderProposal.addQuorumVerifier(self.getQuorumVerifier());
            if (self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier()
                    .getVersion()) {
                newLeaderProposal.addQuorumVerifier(self.getLastSeenQuorumVerifier());
            }

            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged

            // 等待 epoch 的ACK数据包
            waitForEpochAck(self.getId(), leaderStateSummary);
            // 设置当前选举周期
            self.setCurrentEpoch(epoch);

            try {
                // 等待NEWLEADER的ACK数据包
                waitForNewLeaderAck(self.getId(), zk.getZxid());
            } catch (InterruptedException e) {
                shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                        + newLeaderProposal.ackSetsToString() + " ]");
                HashSet<Long> followerSet = new HashSet<Long>();


                for (LearnerHandler f : getLearners()) {
                    if (self.getQuorumVerifier().getVotingMembers().containsKey(f.getSid())) {
                        followerSet.add(f.getSid());
                    }
                }
                boolean initTicksShouldBeIncreased = true;
                for (Proposal.QuorumVerifierAcksetPair qvAckset : newLeaderProposal.qvAcksetPairs) {
                    if (!qvAckset.getQuorumVerifier().containsQuorum(followerSet)) {
                        initTicksShouldBeIncreased = false;
                        break;
                    }
                }
                if (initTicksShouldBeIncreased) {
                    LOG.warn("Enough followers present. " +
                            "Perhaps the initTicks need to be increased.");
                }
                return;
            }

            // 启动zk服务
            startZkServer();

            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             *
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }

            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.setZooKeeperServer(zk);
            }

            self.adminServer.setZooKeeperServer(zk);

            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration
            boolean tickSkip = true;
            // If not null then shutdown this leader
            String shutdownMessage = null;

            while (true) {
                synchronized (this) {
                    long start = Time.currentElapsedTime();
                    long cur = start;
                    long end = start + self.tickTime / 2;
                    while (cur < end) {
                        wait(end - cur);
                        cur = Time.currentElapsedTime();
                    }

                    if (!tickSkip) {
                        self.tick.incrementAndGet();
                    }

                    // We use an instance of SyncedLearnerTracker to
                    // track synced learners to make sure we still have a
                    // quorum of current (and potentially next pending) view.
                    // 创建SyncedLearnerTracker对象，该对象用于确认是否有大多数同意
                    SyncedLearnerTracker syncedAckSet = new SyncedLearnerTracker();
                    syncedAckSet.addQuorumVerifier(self.getQuorumVerifier());
                    if (self.getLastSeenQuorumVerifier() != null
                            && self.getLastSeenQuorumVerifier().getVersion() > self
                            .getQuorumVerifier().getVersion()) {
                        syncedAckSet.addQuorumVerifier(self
                                .getLastSeenQuorumVerifier());
                    }

                    syncedAckSet.addAck(self.getId());

                    for (LearnerHandler f : getLearners()) {
                        if (f.synced()) {
                            syncedAckSet.addAck(f.getSid());
                        }
                    }

                    // check leader running status
                    if (!this.isRunning()) {
                        // set shutdown flag
                        shutdownMessage = "Unexpected internal error";
                        break;
                    }

                    // 没有大多数认可，结束处理
                    if (!tickSkip && !syncedAckSet.hasAllQuorums()) {
                        // Lost quorum of last committed and/or last proposed
                        // config, set shutdown flag
                        shutdownMessage =
                                "Not sufficient followers synced, only synced with sids: [ "
                                        + syncedAckSet.ackSetsToString() + " ]";
                        break;
                    }
                    tickSkip = !tickSkip;
                }
                // 获取LearnerHandler然后发送ping
                for (LearnerHandler f : getLearners()) {
                    f.ping();
                }
            }
            if (shutdownMessage != null) {
                shutdown(shutdownMessage);
                // leader goes in looking state
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    /**
     * Close down all the LearnerHandlers
     * 关闭各类对象
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }

        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }

        // NIO should not accept conenctions
        self.setZooKeeperServer(null);
        self.adminServer.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close", e);
        }
        self.closeAllConnections();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext(); ) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /** In a reconfig operation, this method attempts to find the best leader for next configuration.
     *  If the current leader is a voter in the next configuartion, then it remains the leader.
     *  Otherwise, choose one of the new voters that acked the reconfiguartion, such that it is as
     * up-to-date as possible, i.e., acked as many outstanding proposals as possible.
     *
     * 确认leader
     * @param reconfigProposal
     * @param zxid of the reconfigProposal
     * @return server if of the designated leader
     */

    private long getDesignatedLeader(Proposal reconfigProposal, long zxid) {
        //new configuration
        // 获取最后一个QuorumVerifierAcksetPair对象

        Proposal.QuorumVerifierAcksetPair newQVAcksetPair =
                reconfigProposal.qvAcksetPairs.get(reconfigProposal.qvAcksetPairs.size() - 1);

        //check if I'm in the new configuration with the same quorum address -
        // if so, I'll remain the leader

        // 判断自己是否在newQVAcksetPair中，如果在我自己就是leader
        if (newQVAcksetPair.getQuorumVerifier().getVotingMembers().containsKey(self.getId()) &&
                newQVAcksetPair.getQuorumVerifier().getVotingMembers()
                        .get(self.getId()).addr.equals(self.getQuorumAddress())) {
            return self.getId();
        }
        // start with an initial set of candidates that are voters from new config that
        // acknowledged the reconfig op (there must be a quorum). Choose one of them as
        // current leader candidate
        // 候选人集合
        HashSet<Long> candidates = new HashSet<Long>(newQVAcksetPair.getAckset());
        // 从候选人集合中移除自己
        candidates.remove(self.getId()); // if we're here, I shouldn't be the leader
        // 当前需要处理的候选人
        long curCandidate = candidates.iterator().next();

        //go over outstanding ops in order, and try to find a candidate that acked the most ops.
        //this way it will be the most up-to-date and we'll minimize the number of ops that get dropped


        long curZxid = zxid + 1;
        Proposal p = outstandingProposals.get(curZxid);

        // 循环处理，处理目标是获取最多投票数量的候选人
        while (p != null && !candidates.isEmpty()) {
            for (Proposal.QuorumVerifierAcksetPair qvAckset : p.qvAcksetPairs) {
                //reduce the set of candidates to those that acknowledged p
                // 保留交集
                candidates.retainAll(qvAckset.getAckset());
                //no candidate acked p, return the best candidate found so far
                // 如果交集为空
                if (candidates.isEmpty()) {
                    return curCandidate;
                }
                //update the current candidate, and if it is the only one remaining, return it
                curCandidate = candidates.iterator().next();
                if (candidates.size() == 1) {
                    return curCandidate;
                }
            }
            curZxid++;
            p = outstandingProposals.get(curZxid);
        }

        return curCandidate;
    }

    /**
     * 尝试提交
     * @return True if committed, otherwise false.
     **/
    synchronized public boolean tryToCommit(Proposal p, long zxid, SocketAddress followerAddr) {
        // make sure that ops are committed in order. With reconfigurations it is now possible
        // that different operations wait for different sets of acks, and we still want to enforce
        // that they are committed in order. Currently we only permit one outstanding reconfiguration
        // such that the reconfiguration and subsequent outstanding ops proposed while the reconfig is
        // pending all wait for a quorum of old and new config, so it's not possible to get enough acks
        // for an operation without getting enough acks for preceding ops. But in the future if multiple
        // concurrent reconfigs are allowed, this can happen.


        if (outstandingProposals.containsKey(zxid - 1)) {
            return false;
        }

        // in order to be committed, a proposal must be accepted by a quorum.
        //
        // getting a quorum from all necessary configurations.
        if (!p.hasAllQuorums()) {
            return false;
        }

        // commit proposals in order
        if (zxid != lastCommitted + 1) {
            LOG.warn("Commiting zxid 0x" + Long.toHexString(zxid)
                    + " from " + followerAddr + " not first!");
            LOG.warn("First is "
                    + (lastCommitted + 1));
        }

        outstandingProposals.remove(zxid);

        if (p.request != null) {
            // 待处理提案
            toBeApplied.add(p);
        }

        if (p.request == null) {
            LOG.warn("Going to commmit null: " + p);
        } else if (p.request.getHdr().getType() == OpCode.reconfig) {
            LOG.debug("Committing a reconfiguration! " + outstandingProposals.size());

            //if this server is voter in new config with the same quorum address,
            //then it will remain the leader
            //otherwise an up-to-date follower will be designated as leader. This saves
            //leader election time, unless the designated leader fails
            Long designatedLeader = getDesignatedLeader(p, zxid);
            //LOG.warn("designated leader is: " + designatedLeader);

            QuorumVerifier newQV =
                    p.qvAcksetPairs.get(p.qvAcksetPairs.size() - 1).getQuorumVerifier();

            self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);

            if (designatedLeader != self.getId()) {
                LOG.info(String.format(
                        "Committing a reconfiguration (reconfigEnabled=%s); this leader is not the designated "
                                + "leader anymore, setting allowedToCommit=false",
                        self.isReconfigEnabled()));
                allowedToCommit = false;
            }

            // we're sending the designated leader, and if the leader is changing the followers are
            // responsible for closing the connection - this way we are sure that at least a majority of them
            // receive the commit message.
            commitAndActivate(zxid, designatedLeader);
            informAndActivate(p, designatedLeader);
            //turnOffFollowers();
        } else {
            commit(zxid);
            inform(p);
        }
        zk.commitProcessor.commit(p.request);
        if (pendingSyncs.containsKey(zxid)) {
            for (LearnerSyncRequest r : pendingSyncs.remove(zxid)) {
                sendSync(r);
            }
        }

        return true;
    }

    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     *
     * 处理ACK
     * @param zxid, the zxid of the proposal sent out
     * @param sid, the id of the server that sent the ack
     * @param followerAddr
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        if (!allowedToCommit)
            return; // last op committed was a leader change - from now on
        // the new leader should commit
        // 记录日志
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }

        // 跳过处理
        if ((zxid & 0xffffffffL) == 0) {
            /*
             * We no longer process NEWLEADER ack with this method. However,
             * the learner sends an ack back to the leader after it gets
             * UPTODATE, so we just ignore the message.
             */
            return;
        }


        // 提案容器数量为0跳过处理
        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        // 如果最后提交的zxid大于等于参数zxid将跳过处理
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }

        // 获取提案
        Proposal p = outstandingProposals.get(zxid);
        // 提案为空跳过处理
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }

        // 向提案中添加已经ACK的服务id
        p.addAck(sid);
        /*if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }*/

        // 尝试提交提案
        boolean hasCommitted = tryToCommit(p, zxid, followerAddr);

        // If p is a reconfiguration, multiple other operations may be ready to be committed,
        // since operations wait for different sets of acks.
        // Currently we only permit one outstanding reconfiguration at a time
        // such that the reconfiguration and subsequent outstanding ops proposed while the reconfig is
        // pending all wait for a quorum of old and new config, so its not possible to get enough acks
        // for an operation without getting enough acks for preceding ops. But in the future if multiple
        // concurrent reconfigs are allowed, this can happen and then we need to check whether some pending
        // ops may already have enough acks and can be committed, which is what this code does.

        // 1. 提案提交失败
        // 2. 提案中的请求对象不为空并且类型为重载配置
        if (hasCommitted && p.request != null && p.request.getHdr().getType() == OpCode.reconfig) {
            long curZxid = zxid;
            while (allowedToCommit && hasCommitted && p != null) {
                curZxid++;
                p = outstandingProposals.get(curZxid);
                if (p != null) {
                    hasCommitted = tryToCommit(p, curZxid, null);
                }
            }
        }
    }

    /**
     * send a packet to all the followers ready to follow
     *
     * @param qp
     *                the packet to be sent
     */
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {
                f.queuePacket(qp);
            }
        }
    }

    /**
     * send a packet to all observers
     */
    void sendObserverPacket(QuorumPacket qp) {
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    /**
     * Create a commit packet and send it to all the members of the quorum
     *
     * @param zxid
     */
    public void commit(long zxid) {
        synchronized (this) {
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }

    //commit and send some info
    public void commitAndActivate(long zxid, long designatedLeader) {
        synchronized (this) {
            lastCommitted = zxid;
        }

        byte data[] = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putLong(designatedLeader);

        QuorumPacket qp = new QuorumPacket(Leader.COMMITANDACTIVATE, zxid, data, null);
        sendPacket(qp);
    }

    /**
     * Create an inform packet and send it to all observers.
     */
    public void inform(Proposal proposal) {
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid,
                proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }

    /**
     * Create an inform&activate packet and send it to all observers.
     */
    public void informAndActivate(Proposal proposal, long designatedLeader) {
        byte[] proposalData = proposal.packet.getData();
        byte[] data = new byte[proposalData.length + 8];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putLong(designatedLeader);
        buffer.put(proposalData);

        QuorumPacket qp =
                new QuorumPacket(Leader.INFORMANDACTIVATE, proposal.request.zxid, data, null);
        sendObserverPacket(qp);
    }

    /**
     * Returns the current epoch of the leader.
     *
     * @return
     */
    public long getEpoch() {
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }

    /**
     * create a proposal and send it out to all the members
     *
     * 创建提案发送给其他服务
     *
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }

        byte[] data = SerializeUtils.serializeRequest(request);
        proposalStats.setLastBufferSize(data.length);
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);

        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;

        synchronized (this) {
            p.addQuorumVerifier(self.getQuorumVerifier());

            if (request.getHdr().getType() == OpCode.reconfig) {
                self.setLastSeenQuorumVerifier(request.qv, true);
            }

            if (self.getQuorumVerifier().getVersion() < self.getLastSeenQuorumVerifier()
                    .getVersion()) {
                p.addQuorumVerifier(self.getLastSeenQuorumVerifier());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            lastProposed = p.packet.getZxid();

            outstandingProposals.put(lastProposed, p);
            sendPacket(pp);
        }
        return p;
    }

    public LearnerSnapshotThrottler getLearnerSnapshotThrottler() {
        return learnerSnapshotThrottler;
    }

    /**
     * Process sync requests
     *
     * @param r the request
     */

    synchronized public void processSync(LearnerSyncRequest r) {
        if (outstandingProposals.isEmpty()) {
            sendSync(r);
        } else {
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }

    /**
     * Sends a sync message to the appropriate server
     */
    public void sendSync(LearnerSyncRequest r) {
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }

    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     *
     * 一般用于Follower节点来通知领导者同步完成
     * @param handler handler of the follower
     * @return last proposed zxid
     * @throws InterruptedException
     */
    synchronized public long startForwarding(LearnerHandler handler,
                                             long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        if (lastProposed > lastSeenZxid) {
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null);
                handler.queuePacket(qp);
            }
            // Only participant need to get outstanding proposals
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                List<Long> zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid : zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }

        return lastProposed;
    }

    public long getEpochToPropose(long sid, long lastAcceptedEpoch)
            throws InterruptedException, IOException {
        synchronized (connectingFollowers) {
            if (!waitingForNewEpoch) {
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch + 1;
            }
            if (isParticipant(sid)) {
                connectingFollowers.add(sid);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) &&
                    verifier.containsQuorum(connectingFollowers)) {
                waitingForNewEpoch = false;
                self.setAcceptedEpoch(epoch);
                connectingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");
                }
            }
            return epoch;
        }
    }

    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized (electingFollowers) {
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new IOException("Follower is ahead of the leader, leader summary: "
                            + leaderStateSummary.getCurrentEpoch()
                            + " (current epoch), "
                            + leaderStateSummary.getLastZxid()
                            + " (last zxid)");
                }
                // 判断当前id是否在选举节点中
                if (isParticipant(id)) {
                    electingFollowers.add(id);
                }
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            // 方法containsQuorum确认是否一半以上收到
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(
                    electingFollowers)) {
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!electionFinished && cur < end) {
                    // 等待一定时间
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!electionFinished) {
                    throw new InterruptedException(
                            "Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Return a list of sid in set as string
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * Start up Leader ZooKeeper server and initialize zxid to the new epoch
     */
    private synchronized void startZkServer() {
        // Update lastCommitted and Db's zxid to a value representing the new epoch
        lastCommitted = zk.getZxid();
        LOG.info(
                "Have quorum of supporters, sids: [{}]; starting up and setting last processed zxid: 0x{}",
                newLeaderProposal.ackSetsToString(),
                Long.toHexString(zk.getZxid()));

        // 是否允许重载配置
        if (self.isReconfigEnabled()) {
            /*
             * ZOOKEEPER-1324. the leader sends the new config it must complete
             *  to others inside a NEWLEADER message (see LearnerHandler where
             *  the NEWLEADER message is constructed), and once it has enough
             *  acks we must execute the following code so that it applies the
             *  config to itself.
             */
            QuorumVerifier newQV = self.getLastSeenQuorumVerifier();

            // 从newLeaderProposal中确认leader
            Long designatedLeader = getDesignatedLeader(newLeaderProposal, zk.getZxid());

            // 重载配置
            self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);
            if (designatedLeader != self.getId()) {
                LOG.warn(
                        "This leader is not the designated leader, it will be initialized with allowedToCommit = false");
                allowedToCommit = false;
            }
        } else {
            LOG.info(
                    "Dynamic reconfig feature is disabled, skip designatedLeader calculation and reconfig processing.");
        }

        // zk服务类正式启动
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         *
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        // 更新选票周期
        self.updateElectionVote(getEpoch());

        //
        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * Process NEWLEADER ack of a given sid and wait until the leader receives
     * sufficient acks.
     *
     * @param sid
     * @throws InterruptedException
     */
    public void waitForNewLeaderAck(long sid, long zxid)
            throws InterruptedException {

        synchronized (newLeaderProposal.qvAcksetPairs) {

            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            /*
             * Note that addAck already checks that the learner
             * is a PARTICIPANT.
             */
            newLeaderProposal.addAck(sid);

            if (newLeaderProposal.hasAllQuorums()) {
                quorumFormed = true;
                newLeaderProposal.qvAcksetPairs.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.qvAcksetPairs.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getQuorumVerifier().getVotingMembers().containsKey(sid);
    }


    /**
     * 提案
     */
    static public class Proposal extends SyncedLearnerTracker {
        public QuorumPacket packet;
        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }


    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private final RequestProcessor next;

        private final Leader leader;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         *
         *
         * 待应用处理器
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next, Leader leader) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.leader = leader;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            next.processRequest(request);

            // The only requests that should be on toBeApplied are write
            // requests, for which we will have a hdr. We can't simply use
            // request.zxid here because that is set on read requests to equal
            // the zxid of the last write op.

            // 如果请求中的hdr(事务头)属性不为空
            if (request.getHdr() != null) {
                // 从头信息中获取zxid
                long zxid = request.getHdr().getZxid();
                // 从leader中获取提案集合
                Iterator<Proposal> iter = leader.toBeApplied.iterator();
                // 遍历提案集合
                if (iter.hasNext()) {
                    Proposal p = iter.next();
                    // 提案中的请求对象不为空，zxid和当前zxid相同则将当前提案移除并结束处理
                    if (p.request != null && p.request.zxid == zxid) {
                        iter.remove();
                        return;
                    }
                }
                LOG.error("Committed request not found on toBeApplied: "
                        + request);
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }


    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }


    class LearnerCnxAcceptor extends ZooKeeperCriticalThread {
        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress(), zk
                    .getZooKeeperServerListener());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    Socket s = null;
                    boolean error = false;
                    try {
                        // 接收socket连接
                        s = ss.accept();

                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(
                                s.getInputStream());
                        // 创建LearnerHandler对象并启动
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        fh.start();
                    } catch (SocketException e) {
                        error = true;
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        }
                        else {
                            throw e;
                        }
                    } catch (SaslException e) {
                        LOG.error("Exception while connecting to quorum learner", e);
                        error = true;
                    } catch (Exception e) {
                        error = true;
                        throw e;
                    } finally {
                        // Don't leak sockets on errors
                        if (error && s != null && !s.isClosed()) {
                            try {
                                s.close();
                            } catch (IOException e) {
                                LOG.warn("Error closing socket", e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e.getMessage());
                handleException(this.getName(), e);
            }
        }

        public void halt() {
            stop = true;
        }
    }
}
