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

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * This is the basic interface that ZooKeeperServer uses to track sessions. The
 * standalone and leader ZooKeeperServer use the same SessionTracker. The
 * FollowerZooKeeperServer uses a SessionTracker which is basically a simple
 * shell to track information to be forwarded to the leader.
 * <p>
 * 会话跟踪器
 */
public interface SessionTracker {
    /**
     * 创建session id
     * @param sessionTimeout
     * @return
     */
    long createSession(int sessionTimeout);

    /**
     * Add a global session to those being tracked.
     * 添加全局session
     *
     * @param id sessionId
     * @param to sessionTimeout
     * @return whether the session was newly added (if false, already existed)
     */
    boolean addGlobalSession(long id, int to);

    /**
     * Add a session to those being tracked. The session is added as a local
     * session if they are enabled, otherwise as global.
     *
     * 添加session
     * @param id sessionId
     * @param to sessionTimeout
     * @return whether the session was newly added (if false, already existed)
     */
    boolean addSession(long id, int to);

    /**
     * 会话是否处于活跃状态
     * @param sessionId
     * @param sessionTimeout
     * @return false if session is no longer active
     */
    boolean touchSession(long sessionId, int sessionTimeout);

    /**
     * Mark that the session is in the process of closing.
     *
     * 设置session正在关闭
     *
     * @param sessionId
     */
    void setSessionClosing(long sessionId);

    /**
     * 关闭会话跟踪
     */
    void shutdown();

    /**
     * 移除session
     * @param sessionId
     */
    void removeSession(long sessionId);

    /**
     * 判断是否跟踪会话id
     * @param sessionId
     * @return whether or not the SessionTracker is aware of this session
     */
    boolean isTrackingSession(long sessionId);

    /**
     * Checks whether the SessionTracker is aware of this session, the session
     * is still active, and the owner matches. If the owner wasn't previously
     * set, this sets the owner of the session.
     * <p>
     * UnknownSessionException should never been thrown to the client. It is
     * only used internally to deal with possible local session from other
     * machine
     *
     * 检查session
     * @param sessionId
     * @param owner
     */
    void checkSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException,
            KeeperException.UnknownSessionException;

    /**
     * Strictly check that a given session is a global session or not
     *
     * 检查全局session
     * @param sessionId
     * @param owner
     * @throws KeeperException.SessionExpiredException
     * @throws KeeperException.SessionMovedException
     */
    void checkGlobalSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException;

    /**
     * 设置会话所有者
     * @param id
     * @param owner
     * @throws SessionExpiredException
     */
    void setOwner(long id, Object owner) throws SessionExpiredException;

    /**
     * Text dump of session information, suitable for debugging.
     * 转储session
     *
     * @param pwriter the output writer
     */
    void dumpSessions(PrintWriter pwriter);

    /**
     * Returns a mapping of time to session IDs that expire at that time.
     *
     * 返回过期的session
     */
    Map<Long, Set<Long>> getSessionExpiryMap();

    /**
     * session接口
     */
    interface Session {
        long getSessionId();

        int getTimeout();

        boolean isClosing();
    }


    /**
     * session过期接口
     */
    interface SessionExpirer {
        void expire(Session session);

        long getServerId();
    }
}
