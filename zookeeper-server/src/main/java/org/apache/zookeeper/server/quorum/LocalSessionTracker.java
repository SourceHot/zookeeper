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

import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZooKeeperServerListener;

import java.util.concurrent.ConcurrentMap;

/**
 * Local session tracker.
 */
public class LocalSessionTracker extends SessionTrackerImpl {
    public LocalSessionTracker(SessionExpirer expirer,
                               ConcurrentMap<Long, Integer> sessionsWithTimeouts,
                               int tickTime, long id, ZooKeeperServerListener listener) {
        super(expirer, sessionsWithTimeouts, tickTime, id, listener);
    }

    /**
     * 检查session id 是否位于sessionsById容器中
     */
    public boolean isLocalSession(long sessionId) {
        return isTrackingSession(sessionId);
    }

    public boolean isGlobalSession(long sessionId) {
        return false;
    }

    /**
     * 恒定抛出异常
     */
    public boolean addGlobalSession(long sessionId, int sessionTimeout) {
        throw new UnsupportedOperationException();
    }
}
