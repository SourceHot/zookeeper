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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl extends ZooKeeperCriticalThread implements
        SessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    /**
     * session id 和 SessionImpl的映射关系
     */
    protected final ConcurrentHashMap<Long, SessionImpl> sessionsById =
            new ConcurrentHashMap<Long, SessionImpl>();

    /**
     * 过期队列
     */
    private final ExpiryQueue<SessionImpl> sessionExpiryQueue;

    /**
     * session id 和 session 过期时间映射关系
     */
    private final ConcurrentMap<Long, Integer> sessionsWithTimeout;
    /**
     * 下一个session id
     */
    private final AtomicLong nextSessionId = new AtomicLong();
    /**
     * session 过期接口
     */
    private final SessionExpirer expirer;
    /**
     * 是否处于运行状态
     */
    volatile boolean running = true;

    public SessionTrackerImpl(SessionExpirer expirer,
                              ConcurrentMap<Long, Integer> sessionsWithTimeout, int tickTime,
                              long serverId, ZooKeeperServerListener listener) {
        super("SessionTracker", listener);
        this.expirer = expirer;
        this.sessionExpiryQueue = new ExpiryQueue<SessionImpl>(tickTime);
        this.sessionsWithTimeout = sessionsWithTimeout;
        // 计算session id
        this.nextSessionId.set(initializeNextSession(serverId));
        // 添加session
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            addSession(e.getKey(), e.getValue());
        }

        // 验证服务id有效性
        EphemeralType.validateServerId(serverId);
    }

    /**
     * Generates an initial sessionId. High order byte is serverId, next 5
     * 5 bytes are from timestamp, and low order 2 bytes are 0s.
     *
     * id 数据从Zookeeper配置中的myId属性来
     */
    public static long initializeNextSession(long id) {

        long nextSid;
        // 当前时间(毫秒)左移24位，无符号右移8位
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        // sid 与id左移56位后或运算
        nextSid = nextSid | (id << 56);
        // 判断计算的sid是否和最小long值相同，如果相同则进行加一操作
        if (nextSid == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
            ++nextSid;  // this is an unlikely edge case, but check it just in case
        }
        return nextSid;
    }

    public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session ");
        sessionExpiryQueue.dump(pwriter);
    }

    /**
     * Returns a mapping from time to session IDs of sessions expiring at that time.
     */
    synchronized public Map<Long, Set<Long>> getSessionExpiryMap() {
        // Convert time -> sessions map to time -> session IDs map
        Map<Long, Set<SessionImpl>> expiryMap = sessionExpiryQueue.getExpiryMap();
        Map<Long, Set<Long>> sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        for (Entry<Long, Set<SessionImpl>> e : expiryMap.entrySet()) {
            Set<Long> ids = new HashSet<Long>();
            sessionExpiryMap.put(e.getKey(), ids);
            for (SessionImpl s : e.getValue()) {
                ids.add(s.sessionId);
            }
        }
        return sessionExpiryMap;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    @Override
    public void run() {
        try {
            while (running) {
                // 获取等待时间
                long waitTime = sessionExpiryQueue.getWaitTime();
                if (waitTime > 0) {
                    Thread.sleep(waitTime);
                    continue;
                }

                // 从session过期队列中获取session
                for (SessionImpl s : sessionExpiryQueue.poll()) {
                    // 设置关闭状态
                    setSessionClosing(s.sessionId);
                    // 进行过期接口操作
                    expirer.expire(s);
                }
            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    synchronized public boolean touchSession(long sessionId, int timeout) {
        // 根据session id 获取session
        SessionImpl s = sessionsById.get(sessionId);

        // session 为空返回false
        if (s == null) {
            logTraceTouchInvalidSession(sessionId, timeout);
            return false;
        }

        // session 处于关闭状态 返回false
        if (s.isClosing()) {
            logTraceTouchClosingSession(sessionId, timeout);
            return false;
        }

        // 更新过期时间
        updateSessionExpiry(s, timeout);
        return true;
    }

    private void updateSessionExpiry(SessionImpl s, int timeout) {
        logTraceTouchSession(s.sessionId, timeout, "");
        sessionExpiryQueue.update(s, timeout);
    }

    private void logTraceTouchSession(long sessionId, int timeout, String sessionStatus) {
        if (!LOG.isTraceEnabled())
            return;

        String msg = MessageFormat.format(
                "SessionTrackerImpl --- Touch {0}session: 0x{1} with timeout {2}",
                sessionStatus, Long.toHexString(sessionId), Integer.toString(timeout));

        ZooTrace.logTraceMessage(LOG, ZooTrace.CLIENT_PING_TRACE_MASK, msg);
    }

    private void logTraceTouchInvalidSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "invalid ");
    }

    private void logTraceTouchClosingSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "closing ");
    }

    public int getSessionTimeout(long sessionId) {
        return sessionsWithTimeout.get(sessionId);
    }

    synchronized public void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Session closing: 0x" + Long.toHexString(sessionId));
        }
        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        s.isClosing = true;
    }

    synchronized public void removeSession(long sessionId) {
        LOG.debug("Removing session 0x" + Long.toHexString(sessionId));
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Removing session 0x"
                            + Long.toHexString(sessionId));
        }
        if (s != null) {
            sessionExpiryQueue.remove(s);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                    "Shutdown SessionTrackerImpl!");
        }
    }

    public long createSession(int sessionTimeout) {
        long sessionId = nextSessionId.getAndIncrement();
        addSession(sessionId, sessionTimeout);
        return sessionId;
    }

    public boolean addGlobalSession(long id, int sessionTimeout) {
        return addSession(id, sessionTimeout);
    }

    public synchronized boolean addSession(long id, int sessionTimeout) {
        // 向sessionsWithTimeout容器中加入数据
        sessionsWithTimeout.put(id, sessionTimeout);

        boolean added = false;

        // 从sessionsById容器中根据session id 获取session
        SessionImpl session = sessionsById.get(id);
        // 不存在创建
        if (session == null) {
            session = new SessionImpl(id, sessionTimeout);
        }

        // findbugs2.0.3 complains about get after put.
        // long term strategy would be use computeIfAbsent after JDK 1.8
        // 向sessionsById容器中加入session
        SessionImpl existedSession = sessionsById.putIfAbsent(id, session);

        if (existedSession != null) {
            session = existedSession;
        }
        else {
            added = true;
            LOG.debug("Adding session 0x" + Long.toHexString(id));
        }

        if (LOG.isTraceEnabled()) {
            String actionStr = added ? "Adding" : "Existing";
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- " + actionStr + " session 0x"
                            + Long.toHexString(id) + " " + sessionTimeout);
        }

        // 更新session过期信息
        updateSessionExpiry(session, sessionTimeout);
        return added;
    }

    public boolean isTrackingSession(long sessionId) {
        return sessionsById.containsKey(sessionId);
    }

    public synchronized void checkSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException,
            KeeperException.UnknownSessionException {
        LOG.debug("Checking session 0x" + Long.toHexString(sessionId));
        // 获取 session
        SessionImpl session = sessionsById.get(sessionId);

        // session 为空抛出异常
        if (session == null) {
            throw new KeeperException.UnknownSessionException();
        }

        // session 已关闭抛出异常
        if (session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }

        // 所有者为空赋值所有者
        if (session.owner == null) {
            session.owner = owner;
        }
        // session 所有者和参数所有者不匹配抛出异常
        else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    synchronized public void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }

    public void checkGlobalSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException {
        try {
            checkSession(sessionId, owner);
        } catch (KeeperException.UnknownSessionException e) {
            throw new KeeperException.SessionExpiredException();
        }
    }


    public static class SessionImpl implements Session {
        final long sessionId;
        final int timeout;
        boolean isClosing;
        Object owner;

        SessionImpl(long sessionId, int timeout) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            isClosing = false;
        }

        public long getSessionId() {
            return sessionId;
        }

        public int getTimeout() {
            return timeout;
        }

        public boolean isClosing() {
            return isClosing;
        }

        public String toString() {
            return "0x" + Long.toHexString(sessionId);
        }
    }
}
