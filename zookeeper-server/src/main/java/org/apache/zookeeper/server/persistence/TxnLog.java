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

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Interface for reading transaction logs.
 *
 * 事务日志接口
 */
public interface TxnLog {

    /**
     * Setter for ServerStats to monitor fsync threshold exceed
     *
     * 设置ServerStats
     * @param serverStats used to update fsyncThresholdExceedCount
     */
    void setServerStats(ServerStats serverStats);

    /**
     * roll the current
     * log being appended to
     *
     * 日志滚动
     * @throws IOException
     */
    void rollLog() throws IOException;

    /**
     * Append a request to the transaction log
     * 追加日志
     * @param hdr the transaction header
     * @param r the transaction itself
     * returns true iff something appended, otw false 
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * Start reading the transaction logs
     * from a given zxid
     *
     * 根据zxid读取TxnIterator
     * @param zxid
     * @return returns an iterator to read the 
     * next transaction in the logs.
     * @throws IOException
     */
    TxnIterator read(long zxid) throws IOException;

    /**
     * the last zxid of the logged transactions.
     * 获取最后一个zxid
     * @return the last zxid of the logged transactions.
     * @throws IOException
     */
    long getLastLoggedZxid() throws IOException;

    /**
     * truncate the log to get in sync with the 
     * leader.
     *
     * 截断日志
     * @param zxid the zxid to truncate at.
     * @throws IOException
     */
    boolean truncate(long zxid) throws IOException;

    /**
     * the dbid for this transaction log.
     * 获取当前事务日志所在的数据库id
     * @return the dbid for this transaction log.
     * @throws IOException
     */
    long getDbId() throws IOException;

    /**
     * commit the transaction and make sure
     * they are persisted
     * 提交事务日志
     * @throws IOException
     */
    void commit() throws IOException;

    /**
     * 获取事务同步经过的时间,单位毫秒
     * @return transaction log's elapsed sync time in milliseconds
     */
    long getTxnLogSyncElapsedTime();

    /**
     * close the transactions logs
     * 关闭事务日志
     */
    void close() throws IOException;

    /**
     * an iterating interface for reading 
     * transaction logs. 
     */
    interface TxnIterator {
        /**
         * return the transaction header.
         * 获取TxnHeader
         * @return return the transaction header.
         */
        TxnHeader getHeader();

        /**
         * return the transaction record.
         *
         * 获取Record
         * @return return the transaction record.
         */
        Record getTxn();

        /**
         * go to the next transaction record.
         * 判断是否存在下一个Record
         * @throws IOException
         */
        boolean next() throws IOException;

        /**
         * close files and release the 
         * resources
         * 关闭
         * @throws IOException
         */
        void close() throws IOException;

        /**
         * Get an estimated storage space used to store transaction records
         * that will return by this iterator
         *
         * 获取存储空间大小
         * @throws IOException
         */
        long getStorageSize() throws IOException;
    }
}

