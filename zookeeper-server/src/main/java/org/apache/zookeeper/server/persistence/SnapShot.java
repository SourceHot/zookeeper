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

package org.apache.zookeeper.server.persistence;

import org.apache.zookeeper.server.DataTree;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * snapshot interface for the persistence layer.
 * implement this interface for implementing 
 * snapshots.
 */
public interface SnapShot {

    /**
     * deserialize a data tree from the last valid snapshot and 
     * return the last zxid that was deserialized
     * 从最后一个有效快照中反序列化一个数据树并返回最后一个被反序列化的 zxid
     * @param dt the datatree to be deserialized into
     * @param sessions the sessions to be deserialized into
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    long deserialize(DataTree dt, Map<Long, Integer> sessions)
            throws IOException;

    /**
     * persist the datatree and the sessions into a persistence storage
     *
     * 将数据树和会话持久化到持久存储中
     * @param dt the datatree to be serialized
     * @param sessions
     * @throws IOException
     */
    void serialize(DataTree dt, Map<Long, Integer> sessions,
                   File name)
            throws IOException;

    /**
     * find the most recent snapshot file
     * 查找最新的快照文件
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;

    /**
     * free resources from this snapshot immediately
     * 关闭快照
     * @throws IOException
     */
    void close() throws IOException;
} 
