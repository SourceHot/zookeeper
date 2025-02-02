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

/**
 * An MBean representing a zookeeper cluster nodes (aka quorum peers)
 */
public interface QuorumMXBean {
    /**
     * @return the name of the quorum
     */
    public String getName();

    /**
     * @return configured number of peers in the quorum
     */
    public int getQuorumSize();

    /**
     * @return SSL communication between quorum members required
     */
    public boolean isSslQuorum();

    /**
     * @return SSL communication between quorum members enabled
     */
    public boolean isPortUnification();
}
