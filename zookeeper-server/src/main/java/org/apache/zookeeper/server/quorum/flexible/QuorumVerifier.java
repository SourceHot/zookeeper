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

package org.apache.zookeeper.server.quorum.flexible;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

import java.util.Map;
import java.util.Set;

/**
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server 
 * identifiers constitutes a quorum.
 *
 *
 *仲裁验证器（投票验证器）
 */

public interface QuorumVerifier {
    long getWeight(long id);

    boolean containsQuorum(Set<Long> set);

    long getVersion();

    void setVersion(long ver);

    /**
     * 获取参与选举的所有成员
     * @return
     */
    Map<Long, QuorumServer> getAllMembers();

    /**
     * 获取投票成员
     * @return
     */
    Map<Long, QuorumServer> getVotingMembers();

    Map<Long, QuorumServer> getObservingMembers();

    boolean equals(Object o);

    String toString();
}
