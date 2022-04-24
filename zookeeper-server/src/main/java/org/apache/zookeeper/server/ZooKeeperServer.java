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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.ServerCnxn.CloseRequestException;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class implements a simple standalone ZooKeeperServer. It sets up the
 * following chain of RequestProcessors to process requests:
 * PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
 */
public class ZooKeeperServer implements SessionExpirer, ServerStats.Provider {
    public static final String ALLOW_SASL_FAILED_CLIENTS = "zookeeper.allowSaslFailedClients";
    public static final String SESSION_REQUIRE_CLIENT_SASL_AUTH =
            "zookeeper.sessionRequireClientSASLAuth";
    public static final String SASL_AUTH_SCHEME = "sasl";
    public static final int DEFAULT_TICK_TIME = 3000;
    public final static Exception ok = new Exception("No prob");
    protected static final Logger LOG;
    /**
     * This is the secret that we use to generate passwords. For the moment,
     * it's more of a checksum that's used in reconnection, which carries no
     * security weight, and is treated internally as if it carries no
     * security weight.
     */
    static final private long superSecret = 0XB3415C00L;

    static {
        LOG = LoggerFactory.getLogger(ZooKeeperServer.class);

        Environment.logEnv("Server environment:", LOG);
    }

    final Deque<ChangeRecord> outstandingChanges = new ArrayDeque<>();
    // this data structure must be accessed under the outstandingChanges lock
    final HashMap<String, ChangeRecord> outstandingChangesForPath =
            new HashMap<String, ChangeRecord>();
    private final AtomicLong hzxid = new AtomicLong(0);
    /**
     * 请求处理数量
     */
    private final AtomicInteger requestsInProcess = new AtomicInteger(0);
    private final ServerStats serverStats;
    private final ZooKeeperServerListener listener;
    protected ZooKeeperServerBean jmxServerBean;
    protected DataTreeBean jmxDataTreeBean;
    protected int tickTime = DEFAULT_TICK_TIME;
    /**
     * value of -1 indicates unset, use default
     */
    protected int minSessionTimeout = -1;
    /**
     * value of -1 indicates unset, use default
     */
    protected int maxSessionTimeout = -1;
    protected SessionTracker sessionTracker;
    protected RequestProcessor firstProcessor;
    protected volatile State state = State.INITIAL;
    protected boolean reconfigEnabled;
    protected ServerCnxnFactory serverCnxnFactory;
    protected ServerCnxnFactory secureServerCnxnFactory;
    private FileTxnSnapLog txnLogFactory = null;
    private ZKDatabase zkDb;
    private ZooKeeperServerShutdownHandler zkShutdownHandler;
    private volatile int createSessionTrackerServerId = 1;
    /**
     * Creates a ZooKeeperServer instance. Nothing is setup, use the setX
     * methods to prepare the instance (eg datadir, datalogdir, ticktime,
     * builder, etc...)
     *
     * @throws IOException
     */
    public ZooKeeperServer() {
        serverStats = new ServerStats(this);
        listener = new ZooKeeperServerListenerImpl(this);
    }

    /**
     * Keeping this constructor for backward compatibility
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory,
                           int tickTime,
                           int minSessionTimeout,
                           int maxSessionTimeout,
                           ZKDatabase zkDb) {
        this(txnLogFactory, tickTime, minSessionTimeout, maxSessionTimeout, zkDb,
                QuorumPeerConfig.isReconfigEnabled());
    }

    /**
     *  * Creates a ZooKeeperServer instance. It sets everything up, but doesn't
     * actually start listening for clients until run() is invoked.
     *
     * @param dataDir the directory to put the data
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory,
                           int tickTime,
                           int minSessionTimeout,
                           int maxSessionTimeout,
                           ZKDatabase zkDb,
                           boolean reconfigEnabled) {
        serverStats = new ServerStats(this);
        this.txnLogFactory = txnLogFactory;
        this.txnLogFactory.setServerStats(this.serverStats);
        this.zkDb = zkDb;
        this.tickTime = tickTime;
        setMinSessionTimeout(minSessionTimeout);
        setMaxSessionTimeout(maxSessionTimeout);
        this.reconfigEnabled = reconfigEnabled;
        listener = new ZooKeeperServerListenerImpl(this);
        LOG.info("Created server with tickTime " + tickTime
                + " minSessionTimeout " + getMinSessionTimeout()
                + " maxSessionTimeout " + getMaxSessionTimeout()
                + " datadir " + txnLogFactory.getDataDir()
                + " snapdir " + txnLogFactory.getSnapDir());
    }

    /**
     * creates a zookeeperserver instance.
     * @param txnLogFactory the file transaction snapshot logging class
     * @param tickTime the ticktime for the server
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime)
            throws IOException {
        this(txnLogFactory, tickTime, -1, -1, new ZKDatabase(txnLogFactory),
                QuorumPeerConfig.isReconfigEnabled());
    }

    /**
     * This constructor is for backward compatibility with the existing unit
     * test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public ZooKeeperServer(File snapDir, File logDir, int tickTime)
            throws IOException {
        this(new FileTxnSnapLog(snapDir, logDir),
                tickTime);
    }

    /**
     * Default constructor, relies on the config for its argument values
     *
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory)
            throws IOException {
        this(txnLogFactory, DEFAULT_TICK_TIME, -1, -1, new ZKDatabase(txnLogFactory),
                QuorumPeerConfig.isReconfigEnabled());
    }

    public static int getSnapCount() {
        String sc = System.getProperty("zookeeper.snapCount");
        try {
            int snapCount = Integer.parseInt(sc);

            // snapCount must be 2 or more. See org.apache.zookeeper.server.SyncRequestProcessor
            if (snapCount < 2) {
                LOG.warn("SnapCount should be 2 or more. Now, snapCount is reset to 2");
                snapCount = 2;
            }
            return snapCount;
        } catch (Exception e) {
            return 100000;
        }
    }

    private static boolean shouldAllowSaslFailedClientsConnect() {
        return Boolean.getBoolean(ALLOW_SASL_FAILED_CLIENTS);
    }

    /**
     * 客户端是否需要sasl验证
     */
    private static boolean shouldRequireClientSaslAuth() {
        return Boolean.getBoolean(SESSION_REQUIRE_CLIENT_SASL_AUTH);
    }

    void removeCnxn(ServerCnxn cnxn) {
        zkDb.removeCnxn(cnxn);
    }

    public ServerStats serverStats() {
        return serverStats;
    }

    public void dumpConf(PrintWriter pwriter) {
        pwriter.print("clientPort=");
        pwriter.println(getClientPort());
        pwriter.print("secureClientPort=");
        pwriter.println(getSecureClientPort());
        pwriter.print("dataDir=");
        pwriter.println(zkDb.snapLog.getSnapDir().getAbsolutePath());
        pwriter.print("dataDirSize=");
        pwriter.println(getDataDirSize());
        pwriter.print("dataLogDir=");
        pwriter.println(zkDb.snapLog.getDataDir().getAbsolutePath());
        pwriter.print("dataLogSize=");
        pwriter.println(getLogDirSize());
        pwriter.print("tickTime=");
        pwriter.println(getTickTime());
        pwriter.print("maxClientCnxns=");
        pwriter.println(getMaxClientCnxnsPerHost());
        pwriter.print("minSessionTimeout=");
        pwriter.println(getMinSessionTimeout());
        pwriter.print("maxSessionTimeout=");
        pwriter.println(getMaxSessionTimeout());

        pwriter.print("serverId=");
        pwriter.println(getServerId());
    }

    public ZooKeeperServerConf getConf() {
        return new ZooKeeperServerConf
                (getClientPort(),
                        zkDb.snapLog.getSnapDir().getAbsolutePath(),
                        zkDb.snapLog.getDataDir().getAbsolutePath(),
                        getTickTime(),
                        getMaxClientCnxnsPerHost(),
                        getMinSessionTimeout(),
                        getMaxSessionTimeout(),
                        getServerId());
    }

    /**
     * get the zookeeper database for this server
     * @return the zookeeper database for this server
     */
    public ZKDatabase getZKDatabase() {
        return this.zkDb;
    }

    /**
     * set the zkdatabase for this zookeeper server
     * @param zkDb
     */
    public void setZKDatabase(ZKDatabase zkDb) {
        this.zkDb = zkDb;
    }

    /**
     *  Restore sessions and data
     */
    public void loadData() throws IOException, InterruptedException {
        /*
         * When a new leader starts executing Leader#lead, it
         * invokes this method. The database, however, has been
         * initialized before running leader election so that
         * the server could pick its zxid for its initial vote.
         * It does it by invoking QuorumPeer#getLastLoggedZxid.
         * Consequently, we don't need to initialize it once more
         * and avoid the penalty of loading it a second time. Not
         * reloading it is particularly important for applications
         * that host a large database.
         *
         * The following if block checks whether the database has
         * been initialized or not. Note that this method is
         * invoked by at least one other method:
         * ZooKeeperServer#startdata.
         *
         * See ZOOKEEPER-1642 for more detail.
         */
        if (zkDb.isInitialized()) {
            setZxid(zkDb.getDataTreeLastProcessedZxid());
        } else {
            setZxid(zkDb.loadDataBase());
        }

        // Clean up dead sessions
        LinkedList<Long> deadSessions = new LinkedList<Long>();
        for (Long session : zkDb.getSessions()) {
            if (zkDb.getSessionWithTimeOuts().get(session) == null) {
                deadSessions.add(session);
            }
        }

        for (long session : deadSessions) {
            // XXX: Is lastProcessedZxid really the best thing to use?
            killSession(session, zkDb.getDataTreeLastProcessedZxid());
        }

        // Make a clean snapshot
        takeSnapshot();
    }

    public void takeSnapshot() {
        try {
            txnLogFactory.save(zkDb.getDataTree(), zkDb.getSessionWithTimeOuts());
        } catch (IOException e) {
            LOG.error("Severe unrecoverable error, exiting", e);
            // This is a severe error that we cannot recover from,
            // so we need to exit
            System.exit(10);
        }
    }

    @Override
    public long getDataDirSize() {
        if (zkDb == null) {
            return 0L;
        }
        File path = zkDb.snapLog.getDataDir();
        return getDirSize(path);
    }

    @Override
    public long getLogDirSize() {
        if (zkDb == null) {
            return 0L;
        }
        File path = zkDb.snapLog.getSnapDir();
        return getDirSize(path);
    }

    private long getDirSize(File file) {
        long size = 0L;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    size += getDirSize(f);
                }
            }
        } else {
            size = file.length();
        }
        return size;
    }

    public long getZxid() {
        return hzxid.get();
    }

    public void setZxid(long zxid) {
        hzxid.set(zxid);
    }

    public SessionTracker getSessionTracker() {
        return sessionTracker;
    }

    long getNextZxid() {
        return hzxid.incrementAndGet();
    }

    private void close(long sessionId) {
        Request si = new Request(null, sessionId, 0, OpCode.closeSession, null, null);
        setLocalSessionFlag(si);
        submitRequest(si);
    }

    public void closeSession(long sessionId) {
        LOG.info("Closing session 0x" + Long.toHexString(sessionId));

        // we do not want to wait for a session close. send it as soon as we
        // detect it!
        close(sessionId);
    }

    protected void killSession(long sessionId, long zxid) {
        zkDb.killSession(sessionId, zxid);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "ZooKeeperServer --- killSession: 0x"
                            + Long.toHexString(sessionId));
        }
        if (sessionTracker != null) {
            sessionTracker.removeSession(sessionId);
        }
    }

    public void expire(Session session) {
        long sessionId = session.getSessionId();
        LOG.info("Expiring session 0x" + Long.toHexString(sessionId) + ", timeout of "
                + session.getTimeout() + "ms exceeded");
        close(sessionId);
    }

    /**
     * 如果会话id不存在或者会话过期，则抛出异常
     *
     * @param cnxn
     * @throws MissingSessionException
     */
    void touch(ServerCnxn cnxn) throws MissingSessionException {
        // 如果服务连接类为空返回
        if (cnxn == null) {
            return;
        }
        // 获取 session id
        long id = cnxn.getSessionId();
        // 获取 session 超时时间
        int to = cnxn.getSessionTimeout();
        // 确认会话id是否存在，或者过期，如果满足则抛出异常
        if (!sessionTracker.touchSession(id, to)) {
            throw new MissingSessionException(
                    "No session with sessionid 0x" + Long.toHexString(id) + " exists, probably expired and removed");
        }
    }

    protected void registerJMX() {
        // register with JMX
        try {
            jmxServerBean = new ZooKeeperServerBean(this);
            MBeanRegistry.getInstance().register(jmxServerBean, null);

            try {
                jmxDataTreeBean = new DataTreeBean(zkDb.getDataTree());
                MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
                jmxDataTreeBean = null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    public void startdata()
            throws IOException, InterruptedException {
        //check to see if zkDb is not null
        if (zkDb == null) {
            zkDb = new ZKDatabase(this.txnLogFactory);
        }
        if (!zkDb.isInitialized()) {
            loadData();
        }
    }

    public synchronized void startup() {
        if (sessionTracker == null) {
            createSessionTracker();
        }
        startSessionTracker();
        setupRequestProcessors();

        registerJMX();

        setState(State.RUNNING);
        notifyAll();
    }

    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor syncProcessor = new SyncRequestProcessor(this,
                finalProcessor);
        ((SyncRequestProcessor) syncProcessor).start();
        firstProcessor = new PrepRequestProcessor(this, syncProcessor);
        ((PrepRequestProcessor) firstProcessor).start();
    }

    public ZooKeeperServerListener getZooKeeperServerListener() {
        return listener;
    }

    /**
     * Change the server ID used by {@link #createSessionTracker()}. Must be called prior to
     * {@link #startup()} being called
     *
     * @param newId ID to use
     */
    public void setCreateSessionTrackerServerId(int newId) {
        createSessionTrackerServerId = newId;
    }

    protected void createSessionTracker() {
        sessionTracker = new SessionTrackerImpl(this, zkDb.getSessionWithTimeOuts(),
                tickTime, createSessionTrackerServerId, getZooKeeperServerListener());
    }

    protected void startSessionTracker() {
        ((SessionTrackerImpl) sessionTracker).start();
    }

    /**
     * This can be used while shutting down the server to see whether the server
     * is already shutdown or not.
     *
     * @return true if the server is running or server hits an error, false
     *         otherwise.
     */
    protected boolean canShutdown() {
        return state == State.RUNNING || state == State.ERROR;
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    public boolean isRunning() {
        return state == State.RUNNING;
    }

    public void shutdown() {
        shutdown(false);
    }

    /**
     * Shut down the server instance
     * @param fullyShutDown true if another server using the same database will not replace this one in the same process
     */
    public synchronized void shutdown(boolean fullyShutDown) {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        LOG.info("shutting down");

        // new RuntimeException("Calling shutdown").printStackTrace();
        setState(State.SHUTDOWN);
        // Since sessionTracker and syncThreads poll we just have to
        // set running to false and they will detect it during the poll
        // interval.
        if (sessionTracker != null) {
            sessionTracker.shutdown();
        }
        if (firstProcessor != null) {
            firstProcessor.shutdown();
        }

        if (zkDb != null) {
            if (fullyShutDown) {
                zkDb.clear();
            } else {
                // else there is no need to clear the database
                //  * When a new quorum is established we can still apply the diff
                //    on top of the same zkDb data
                //  * If we fetch a new snapshot from leader, the zkDb will be
                //    cleared anyway before loading the snapshot
                try {
                    //This will fast forward the database to the latest recorded transactions
                    zkDb.fastForwardDataBase();
                } catch (IOException e) {
                    LOG.error("Error updating DB", e);
                    zkDb.clear();
                }
            }
        }

        unregisterJMX();
    }

    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
        jmxDataTreeBean = null;
    }

    public void incInProcess() {
        requestsInProcess.incrementAndGet();
    }

    public void decInProcess() {
        requestsInProcess.decrementAndGet();
    }

    public int getInProcess() {
        return requestsInProcess.get();
    }

    byte[] generatePasswd(long id) {
        Random r = new Random(id ^ superSecret);
        byte p[] = new byte[16];
        r.nextBytes(p);
        return p;
    }

    protected boolean checkPasswd(long sessionId, byte[] passwd) {
        return sessionId != 0
                && Arrays.equals(passwd, generatePasswd(sessionId));
    }

    long createSession(ServerCnxn cnxn, byte passwd[], int timeout) {
        if (passwd == null) {
            // Possible since it's just deserialized from a packet on the wire.
            passwd = new byte[0];
        }
        long sessionId = sessionTracker.createSession(timeout);
        Random r = new Random(sessionId ^ superSecret);
        r.nextBytes(passwd);
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(timeout);
        cnxn.setSessionId(sessionId);
        Request si = new Request(cnxn, sessionId, 0, OpCode.createSession, to, null);
        setLocalSessionFlag(si);
        submitRequest(si);
        return sessionId;
    }

    /**
     * set the owner of this session as owner
     * @param id the session id
     * @param owner the owner of the session
     * @throws SessionExpiredException
     */
    public void setOwner(long id, Object owner) throws SessionExpiredException {
        sessionTracker.setOwner(id, owner);
    }

    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
                                     int sessionTimeout) throws IOException {
        boolean rc = sessionTracker.touchSession(sessionId, sessionTimeout);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId) +
                            " is valid: " + rc);
        }
        finishSessionInit(cnxn, rc);
    }

    public void reopenSession(ServerCnxn cnxn, long sessionId, byte[] passwd,
                              int sessionTimeout) throws IOException {
        if (checkPasswd(sessionId, passwd)) {
            revalidateSession(cnxn, sessionId, sessionTimeout);
        } else {
            LOG.warn("Incorrect password from " + cnxn.getRemoteSocketAddress()
                    + " for session 0x" + Long.toHexString(sessionId));
            finishSessionInit(cnxn, false);
        }
    }

    public void finishSessionInit(ServerCnxn cnxn, boolean valid) {
        // register with JMX
        try {
            if (valid) {
                if (serverCnxnFactory != null && serverCnxnFactory.cnxns.contains(cnxn)) {
                    serverCnxnFactory.registerConnection(cnxn);
                } else if (secureServerCnxnFactory != null
                        && secureServerCnxnFactory.cnxns.contains(cnxn)) {
                    secureServerCnxnFactory.registerConnection(cnxn);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
        }

        try {
            ConnectResponse rsp = new ConnectResponse(0, valid ? cnxn.getSessionTimeout()
                    : 0, valid ? cnxn.getSessionId() : 0, // send 0 if session is no
                    // longer valid
                    valid ? generatePasswd(cnxn.getSessionId()) : new byte[16]);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            bos.writeInt(-1, "len");
            rsp.serialize(bos, "connect");
            if (!cnxn.isOldClient) {
                bos.writeBool(
                        this instanceof ReadOnlyZooKeeperServer, "readOnly");
            }
            baos.close();
            ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
            bb.putInt(bb.remaining() - 4).rewind();
            cnxn.sendBuffer(bb);

            if (valid) {
                LOG.debug("Established session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " with negotiated timeout " + cnxn.getSessionTimeout()
                        + " for client "
                        + cnxn.getRemoteSocketAddress());
                cnxn.enableRecv();
            } else {

                LOG.info("Invalid session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " for client "
                        + cnxn.getRemoteSocketAddress()
                        + ", probably expired");
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
            }

        } catch (Exception e) {
            LOG.warn("Exception while establishing session, closing", e);
            cnxn.close();
        }
    }

    public void closeSession(ServerCnxn cnxn, RequestHeader requestHeader) {
        closeSession(cnxn.getSessionId());
    }

    public long getServerId() {
        return 0;
    }

    /**
     * If the underlying Zookeeper server support local session, this method
     * will set a isLocalSession to true if a request is associated with
     * a local session.
     *
     * @param si
     */
    protected void setLocalSessionFlag(Request si) {
    }

    public void submitRequest(Request si) {
        // 如果首个请求处理器为空
        if (firstProcessor == null) {
            synchronized (this) {
                try {
                    // Since all requests are passed to the request
                    // processor it should wait for setting up the request
                    // processor chain. The state will be updated to RUNNING
                    // after the setup.

                    // 由于所有请求都传递给请求处理器，因此它应该等待设置请求处理器链。设置后状态将更新为 RUNNING。
                    while (state == State.INITIAL) {
                        wait(1000);
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected interruption", e);
                }
                if (firstProcessor == null || state != State.RUNNING) {
                    throw new RuntimeException("Not started");
                }
            }
        }
        try {
            // 尝试处理 session 相关信息
            touch(si.cnxn);
            // 验证请求类型
            boolean validpacket = Request.isValid(si.type);
            // 请求类型验证通过
            if (validpacket) {
                // 处理请求
                firstProcessor.processRequest(si);
                // 连接对象存在的情况下
                if (si.cnxn != null) {
                    // 请求处理数量+1
                    incInProcess();
                }
            }
            // 请求类型验证不通过
            else {
                LOG.warn("Received packet at server of unknown type " + si.type);
                // 用于发送错误码响应的请求处理器
                new UnimplementedRequestProcessor().processRequest(si);
            }
        } catch (MissingSessionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping request: " + e.getMessage());
            }
        } catch (RequestProcessorException e) {
            LOG.error("Unable to process request:" + e.getMessage(), e);
        }
    }

    public int getGlobalOutstandingLimit() {
        String sc = System.getProperty("zookeeper.globalOutstandingLimit");
        int limit;
        try {
            limit = Integer.parseInt(sc);
        } catch (Exception e) {
            limit = 1000;
        }
        return limit;
    }

    public ServerCnxnFactory getServerCnxnFactory() {
        return serverCnxnFactory;
    }

    public void setServerCnxnFactory(ServerCnxnFactory factory) {
        serverCnxnFactory = factory;
    }

    public ServerCnxnFactory getSecureServerCnxnFactory() {
        return secureServerCnxnFactory;
    }

    public void setSecureServerCnxnFactory(ServerCnxnFactory factory) {
        secureServerCnxnFactory = factory;
    }

    /**
     * return the last proceesed id from the
     * datatree
     */
    public long getLastProcessedZxid() {
        return zkDb.getDataTreeLastProcessedZxid();
    }

    /**
     * return the outstanding requests
     * in the queue, which havent been
     * processed yet
     */
    public long getOutstandingRequests() {
        return getInProcess();
    }

    /**
     * return the total number of client connections that are alive
     * to this server
     */
    public int getNumAliveConnections() {
        int numAliveConnections = 0;

        if (serverCnxnFactory != null) {
            numAliveConnections += serverCnxnFactory.getNumAliveConnections();
        }

        if (secureServerCnxnFactory != null) {
            numAliveConnections += secureServerCnxnFactory.getNumAliveConnections();
        }

        return numAliveConnections;
    }

    /**
     * trunccate the log to get in sync with others
     * if in a quorum
     * @param zxid the zxid that it needs to get in sync
     * with others
     * @throws IOException
     */
    public void truncateLog(long zxid) throws IOException {
        this.zkDb.truncateLog(zxid);
    }

    public int getTickTime() {
        return tickTime;
    }

    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    public void setMinSessionTimeout(int min) {
        this.minSessionTimeout = min == -1 ? tickTime * 2 : min;
        LOG.info("minSessionTimeout set to {}", this.minSessionTimeout);
    }

    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    public void setMaxSessionTimeout(int max) {
        this.maxSessionTimeout = max == -1 ? tickTime * 20 : max;
        LOG.info("maxSessionTimeout set to {}", this.maxSessionTimeout);
    }

    public int getClientPort() {
        return serverCnxnFactory != null ? serverCnxnFactory.getLocalPort() : -1;
    }

    public int getSecureClientPort() {
        return secureServerCnxnFactory != null ? secureServerCnxnFactory.getLocalPort() : -1;
    }

    /** Maximum number of connections allowed from particular host (ip) */
    public int getMaxClientCnxnsPerHost() {
        if (serverCnxnFactory != null) {
            return serverCnxnFactory.getMaxClientCnxnsPerHost();
        }
        if (secureServerCnxnFactory != null) {
            return secureServerCnxnFactory.getMaxClientCnxnsPerHost();
        }
        return -1;
    }

    public FileTxnSnapLog getTxnLogFactory() {
        return this.txnLogFactory;
    }

    public void setTxnLogFactory(FileTxnSnapLog txnLog) {
        this.txnLogFactory = txnLog;
    }

    /**
     * Returns the elapsed sync of time of transaction log in milliseconds.
     */
    public long getTxnLogElapsedSyncTime() {
        return txnLogFactory.getTxnLogElapsedSyncTime();
    }

    public String getState() {
        return "standalone";
    }

    /**
     * Sets the state of ZooKeeper server. After changing the state, it notifies
     * the server state change to a registered shutdown handler, if any.
     * <p>
     * The following are the server state transitions:
     * <li>During startup the server will be in the INITIAL state.</li>
     * <li>After successfully starting, the server sets the state to RUNNING.
     * </li>
     * <li>The server transitions to the ERROR state if it hits an internal
     * error. {@link ZooKeeperServerListenerImpl} notifies any critical resource
     * error events, e.g., SyncRequestProcessor not being able to write a txn to
     * disk.</li>
     * <li>During shutdown the server sets the state to SHUTDOWN, which
     * corresponds to the server not running.</li>
     *
     * @param state new server state.
     */
    protected void setState(State state) {
        this.state = state;
        // Notify server state changes to the registered shutdown handler, if any.
        if (zkShutdownHandler != null) {
            zkShutdownHandler.handle(state);
        } else {
            LOG.debug("ZKShutdownHandler is not registered, so ZooKeeper server "
                    + "won't take any action on ERROR or SHUTDOWN server state changes");
        }
    }

    public void dumpEphemerals(PrintWriter pwriter) {
        zkDb.dumpEphemerals(pwriter);
    }

    public Map<Long, Set<String>> getEphemerals() {
        return zkDb.getEphemerals();
    }

    public void processConnectRequest(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        // 读取数据相关对象构造
        BinaryInputArchive bia =
                BinaryInputArchive.getArchive(new ByteBufferInputStream(incomingBuffer));
        ConnectRequest connReq = new ConnectRequest();
        // 反序列化 connect 数据
        connReq.deserialize(bia, "connect");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session establishment request from client "
                    + cnxn.getRemoteSocketAddress()
                    + " client's lastZxid is 0x"
                    + Long.toHexString(connReq.getLastZxidSeen()));
        }
        // 判断是否只读
        boolean readOnly = false;
        try {
            readOnly = bia.readBool("readOnly");
            cnxn.isOldClient = false;
        } catch (IOException e) {
            // this is ok -- just a packet from an old client which
            // doesn't contain readOnly field
            LOG.warn("Connection request from old client " + cnxn.getRemoteSocketAddress()
                    + "; will be dropped if server is in r-o mode");
        }
        // 非只读并且当前zk服务是只读服务抛出异常
        if (!readOnly && this instanceof ReadOnlyZooKeeperServer) {
            String msg = "Refusing session request for not-read-only client " + cnxn.getRemoteSocketAddress();
            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        // 判断连接器请求中的zxid是否大于数据树中的最后处理过的zxid，如果是则抛出异常
        if (connReq.getLastZxidSeen() > zkDb.dataTree.lastProcessedZxid) {
            String msg = "Refusing session request for client "
                    + cnxn.getRemoteSocketAddress()
                    + " as it has seen zxid 0x"
                    + Long.toHexString(connReq.getLastZxidSeen())
                    + " our last zxid is 0x"
                    + Long.toHexString(getZKDatabase().getDataTreeLastProcessedZxid())
                    + " client must try another server";

            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        // 读取会话超时时间
        int sessionTimeout = connReq.getTimeOut();
        // 读取密码
        byte passwd[] = connReq.getPasswd();
        // 超时时间相关处理
        int minSessionTimeout = getMinSessionTimeout();
        if (sessionTimeout < minSessionTimeout) {
            sessionTimeout = minSessionTimeout;
        }
        int maxSessionTimeout = getMaxSessionTimeout();
        if (sessionTimeout > maxSessionTimeout) {
            sessionTimeout = maxSessionTimeout;
        }
        // 设置超时时间
        cnxn.setSessionTimeout(sessionTimeout);
        // We don't want to receive any packets until we are sure that the
        // session is setup
        // 在会话建立完成之前禁止接收数据
        cnxn.disableRecv();
        // 获取session id
        long sessionId = connReq.getSessionId();
        // 如果session id 等于0
        if (sessionId == 0) {
            // 重算session id
            long id = createSession(cnxn, passwd, sessionTimeout);
            LOG.debug("Client attempting to establish new session:"
                            + " session = 0x{}, zxid = 0x{}, timeout = {}, address = {}",
                    Long.toHexString(id), Long.toHexString(connReq.getLastZxidSeen()),
                    connReq.getTimeOut(), cnxn.getRemoteSocketAddress());
        }
        // session id 非零的情况
        else {
            // 从连接请求中获取session id
            long clientSessionId = connReq.getSessionId();
            LOG.debug("Client attempting to renew session:"
                            + " session = 0x{}, zxid = 0x{}, timeout = {}, address = {}",
                    Long.toHexString(clientSessionId), Long.toHexString(connReq.getLastZxidSeen()),
                    connReq.getTimeOut(), cnxn.getRemoteSocketAddress());
            // 如果serverCnxnFactory不为空关闭session
            if (serverCnxnFactory != null) {
                serverCnxnFactory.closeSession(sessionId);
            }
            // 如果secureServerCnxnFactory不为空关闭session
            if (secureServerCnxnFactory != null) {
                secureServerCnxnFactory.closeSession(sessionId);
            }
            // 设置session id
            cnxn.setSessionId(sessionId);
            // 重新打开session
            reopenSession(cnxn, sessionId, passwd, sessionTimeout);
        }
    }

    public boolean shouldThrottle(long outStandingCount) {
        if (getGlobalOutstandingLimit() < getInProcess()) {
            return outStandingCount > 0;
        }
        return false;
    }

    public void processPacket(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        // We have the request, now process and setup for next
        // 创建读取请求相关的对象
        InputStream bais = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
        RequestHeader h = new RequestHeader();
        h.deserialize(bia, "header");
        // Through the magic of byte buffers, txn will not be
        // pointing
        // to the start of the txn
        incomingBuffer = incomingBuffer.slice();
        // 判断头类型是否是安全相关的
        if (h.getType() == OpCode.auth) {
            LOG.info("got auth packet " + cnxn.getRemoteSocketAddress());
            // 创建安全数据包
            AuthPacket authPacket = new AuthPacket();
            // 将字节缓存中的数据转换为安全数据包对象
            ByteBufferInputStream.byteBuffer2Record(incomingBuffer, authPacket);
            // 获取安全方案
            String scheme = authPacket.getScheme();
            // 根据安全方案获取安全验证器
            AuthenticationProvider ap = ProviderRegistry.getProvider(scheme);
            // 设置安全状态码，第一次设置为安全认证失败
            Code authReturn = KeeperException.Code.AUTHFAILED;
            if (ap != null) {
                try {
                    // 从安全验证器中获取状态码
                    authReturn = ap.handleAuthentication(cnxn, authPacket.getAuth());
                } catch (RuntimeException e) {
                    LOG.warn("Caught runtime exception from AuthenticationProvider: " + scheme
                            + " due to " + e);
                    // 安全状态码设置为验证失败
                    authReturn = KeeperException.Code.AUTHFAILED;
                }
            }
            // 如果是安全状态码是OK则写出响应
            if (authReturn == KeeperException.Code.OK) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Authentication succeeded for scheme: " + scheme);
                }
                LOG.info("auth success " + cnxn.getRemoteSocketAddress());
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0, KeeperException.Code.OK.intValue());
                cnxn.sendResponse(rh, null, null);
            }
            // 安全状态吗非OK的情况下下
            else {
                if (ap == null) {
                    LOG.warn("No authentication provider for scheme: " + scheme + " has "
                            + ProviderRegistry.listProviders());
                } else {
                    LOG.warn("Authentication failed for scheme: " + scheme);
                }
                // send a response...
                // 组装响应信息将响应信息发送
                ReplyHeader rh =
                        new ReplyHeader(h.getXid(), 0, KeeperException.Code.AUTHFAILED.intValue());
                cnxn.sendResponse(rh, null, null);
                // ... and close connection
                // 发送关闭连接
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
                // 不再接收数据
                cnxn.disableRecv();
            }
            // 结束处理
            return;
        }
        // 如果是 sasl 交给 processSasl 方法处理
        else if (h.getType() == OpCode.sasl) {
            // sasl请求处理
            processSasl(incomingBuffer, cnxn, h);
        } else {
            // 需要sasl验证并且未通过sasl验证
            if (shouldRequireClientSaslAuth() && !hasCnxSASLAuthenticated(cnxn)) {
                // 组装响应信息将响应信息发送
                ReplyHeader replyHeader = new ReplyHeader(h.getXid(), 0,
                        Code.SESSIONCLOSEDREQUIRESASLAUTH.intValue());
                cnxn.sendResponse(replyHeader, null, "response");
                // 发送session关闭信息
                cnxn.sendCloseSession();
                // 不再接收数据
                cnxn.disableRecv();
            }
            // 不需要sasl验证或者sasl验证通过
            else {
                Request si = new Request(cnxn, cnxn.getSessionId(), h.getXid(), h.getType(),
                        incomingBuffer, cnxn.getAuthInfo());
                si.setOwner(ServerCnxn.me);
                // Always treat packet from the client as a possible
                // local request.
                // 确认是否是本地session如果是将isLocalSession设置为true
                setLocalSessionFlag(si);
                // 提交请求，进行处理
                submitRequest(si);
            }
        }
        cnxn.incrOutstandingRequests(h);
    }

    // 已通过 Cnx SASL 身份验证
    private boolean hasCnxSASLAuthenticated(ServerCnxn cnxn) {
        for (Id id : cnxn.getAuthInfo()) {
            if (id.getScheme().equals(SASL_AUTH_SCHEME)) {
                return true;
            }
        }
        return false;
    }

    private void processSasl(ByteBuffer incomingBuffer, ServerCnxn cnxn,
                             RequestHeader requestHeader) throws IOException {
        LOG.debug("Responding to client SASL token.");
        // 创建 SASL 请求对象
        GetSASLRequest clientTokenRecord = new GetSASLRequest();
        // 将字节缓存中的数据转换为 SASL 请求对象
        ByteBufferInputStream.byteBuffer2Record(incomingBuffer, clientTokenRecord);
        // 从 SASL 请求对象中获取客户端 token
        byte[] clientToken = clientTokenRecord.getToken();
        LOG.debug("Size of client SASL token: " + clientToken.length);
        // 响应token
        byte[] responseToken = null;
        try {
            // 从服务连接对象中获取 SASL 服务
            ZooKeeperSaslServer saslServer = cnxn.zooKeeperSaslServer;
            try {
                // note that clientToken might be empty (clientToken.length == 0):
                // if using the DIGEST-MD5 mechanism, clientToken will be empty at the beginning of the
                // SASL negotiation process.
                // 通过 SASL 服务创建响应token
                responseToken = saslServer.evaluateResponse(clientToken);
                // 判断 SASL 服务是否处理完成
                if (saslServer.isComplete()) {
                    // 通过 SASL 获取授权id
                    String authorizationID = saslServer.getAuthorizationID();
                    LOG.info("adding SASL authorization for authorizationID: " + authorizationID);
                    // 向服务连接对象中添加安全信息
                    cnxn.addAuthInfo(new Id("sasl", authorizationID));
                    // 如果系统环境中zookeeper.superUser属性不为空并且授权id数据和系统环境中zookeeper.superUser的属性相同增加安全信息
                    if (System.getProperty("zookeeper.superUser") != null &&
                            authorizationID.equals(System.getProperty("zookeeper.superUser"))) {
                        cnxn.addAuthInfo(new Id("super", ""));
                    }
                }
            }
            // 处理过程中出现异常
            catch (SaslException e) {
                LOG.warn("Client {} failed to SASL authenticate: {}",
                        cnxn.getRemoteSocketAddress(), e);
                // 允许 SASL 认证失败，客户端不需要 SASL 验证，输出警告日志
                if (shouldAllowSaslFailedClientsConnect() && !shouldRequireClientSaslAuth()) {
                    LOG.warn("Maintaining client connection despite SASL authentication failure.");
                } else {
                    //  确认异常值
                    int error;
                    // 需要 SASL 验证
                    if (shouldRequireClientSaslAuth()) {
                        LOG.warn(
                                "Closing client connection due to server requires client SASL authenticaiton,"
                                        +
                                        "but client SASL authentication has failed, or client is not configured with SASL "
                                        +
                                        "authentication.");
                        // 会话关闭 ， SASL 认证失败
                        error = Code.SESSIONCLOSEDREQUIRESASLAUTH.intValue();
                    } else {
                        LOG.warn("Closing client connection due to SASL authentication failure.");
                        // 认证失败
                        error = Code.AUTHFAILED.intValue();
                    }

                    // 组装响应
                    ReplyHeader replyHeader = new ReplyHeader(requestHeader.getXid(), 0, error);
                    // 发送响应
                    cnxn.sendResponse(replyHeader, new SetSASLResponse(null), "response");
                    // 发送关闭session消息
                    cnxn.sendCloseSession();
                    // 不再接收数据
                    cnxn.disableRecv();
                    return;
                }
            }
        } catch (NullPointerException e) {
            LOG.error(
                    "cnxn.saslServer is null: cnxn object did not initialize its saslServer properly.");
        }
        if (responseToken != null) {
            LOG.debug("Size of server SASL response: " + responseToken.length);
        }

        // SASL 验证通过，发送通过信息
        ReplyHeader replyHeader = new ReplyHeader(requestHeader.getXid(), 0, Code.OK.intValue());
        Record record = new SetSASLResponse(responseToken);
        cnxn.sendResponse(replyHeader, record, "response");
    }

    // entry point for quorum/Learner.java
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        return processTxn(null, hdr, txn);
    }

    // entry point for FinalRequestProcessor.java
    public ProcessTxnResult processTxn(Request request) {
        return processTxn(request, request.getHdr(), request.getTxn());
    }

    private ProcessTxnResult processTxn(Request request, TxnHeader hdr,
                                        Record txn) {
        ProcessTxnResult rc;
        int opCode = request != null ? request.type : hdr.getType();
        long sessionId = request != null ? request.sessionId : hdr.getClientId();
        if (hdr != null) {
            rc = getZKDatabase().processTxn(hdr, txn);
        } else {
            rc = new ProcessTxnResult();
        }
        if (opCode == OpCode.createSession) {
            if (hdr != null && txn instanceof CreateSessionTxn) {
                CreateSessionTxn cst = (CreateSessionTxn) txn;
                sessionTracker.addGlobalSession(sessionId, cst.getTimeOut());
            } else if (request != null && request.isLocalSession()) {
                request.request.rewind();
                int timeout = request.request.getInt();
                request.request.rewind();
                sessionTracker.addSession(request.sessionId, timeout);
            } else {
                LOG.warn("*****>>>>> Got "
                        + txn.getClass() + " "
                        + txn.toString());
            }
        } else if (opCode == OpCode.closeSession) {
            sessionTracker.removeSession(sessionId);
        }
        return rc;
    }

    public Map<Long, Set<Long>> getSessionExpiryMap() {
        return sessionTracker.getSessionExpiryMap();
    }

    /**
     * This method is used to register the ZooKeeperServerShutdownHandler to get
     * server's error or shutdown state change notifications.
     * {@link ZooKeeperServerShutdownHandler#handle(State)} will be called for
     * every server state changes {@link #setState(State)}.
     *
     * @param zkShutdownHandler shutdown handler
     */
    void registerServerShutdownHandler(ZooKeeperServerShutdownHandler zkShutdownHandler) {
        this.zkShutdownHandler = zkShutdownHandler;
    }

    public boolean isReconfigEnabled() {
        return this.reconfigEnabled;
    }

    protected enum State {
        /**
         * 初始化
         */
        INITIAL,
        /**
         * 运行中
         */
        RUNNING,
        /**
         * 关闭
         */
        SHUTDOWN,
        /**
         * 异常
         */
        ERROR
    }


    public static class MissingSessionException extends IOException {
        private static final long serialVersionUID = 7467414635467261007L;

        public MissingSessionException(String msg) {
            super(msg);
        }
    }


    /**
     * This structure is used to facilitate information sharing between PrepRP
     * and FinalRP.
     */
    static class ChangeRecord {
        long zxid;
        String path;
        StatPersisted stat; /* Make sure to create a new object when changing */
        int childCount;
        List<ACL> acl; /* Make sure to create a new object when changing */

        ChangeRecord(long zxid, String path, StatPersisted stat, int childCount,
                     List<ACL> acl) {
            this.zxid = zxid;
            this.path = path;
            this.stat = stat;
            this.childCount = childCount;
            this.acl = acl;
        }

        ChangeRecord duplicate(long zxid) {
            StatPersisted stat = new StatPersisted();
            if (this.stat != null) {
                DataTree.copyStatPersisted(this.stat, stat);
            }
            return new ChangeRecord(zxid, path, stat, childCount,
                    acl == null ? new ArrayList<ACL>() : new ArrayList<ACL>(acl));
        }
    }
}
