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
import org.apache.zookeeper.Version;
import org.apache.zookeeper.jmx.ZKMBeanInfo;

import java.util.Date;

/**
 * This class implements the ZooKeeper server MBean interface.
 */
public class ZooKeeperServerBean implements ZooKeeperServerMXBean, ZKMBeanInfo {
    protected final ZooKeeperServer zks;
    private final Date startTime;
    private final String name;

    public ZooKeeperServerBean(ZooKeeperServer zks) {
        startTime = new Date();
        this.zks = zks;
        name = "StandaloneServer_port" + zks.getClientPort();
    }

    public String getClientPort() {
        return Integer.toString(zks.getClientPort());
    }

    public String getName() {
        return name;
    }

    public boolean isHidden() {
        return false;
    }

    public String getStartTime() {
        return startTime.toString();
    }

    public String getVersion() {
        return Version.getFullVersion();
    }

    public long getAvgRequestLatency() {
        return zks.serverStats().getAvgLatency();
    }

    public long getMaxRequestLatency() {
        return zks.serverStats().getMaxLatency();
    }

    public long getMinRequestLatency() {
        return zks.serverStats().getMinLatency();
    }

    public long getOutstandingRequests() {
        return zks.serverStats().getOutstandingRequests();
    }

    public int getTickTime() {
        return zks.getTickTime();
    }

    public void setTickTime(int tickTime) {
        zks.setTickTime(tickTime);
    }

    public int getMaxClientCnxnsPerHost() {
        return zks.getMaxClientCnxnsPerHost();
    }

    public void setMaxClientCnxnsPerHost(int max) {
        if (zks.serverCnxnFactory != null) {
            zks.serverCnxnFactory.setMaxClientCnxnsPerHost(max);
        }
        if (zks.secureServerCnxnFactory != null) {
            zks.secureServerCnxnFactory.setMaxClientCnxnsPerHost(max);
        }
    }

    public int getMinSessionTimeout() {
        return zks.getMinSessionTimeout();
    }

    public void setMinSessionTimeout(int min) {
        zks.setMinSessionTimeout(min);
    }

    public int getMaxSessionTimeout() {
        return zks.getMaxSessionTimeout();
    }

    public void setMaxSessionTimeout(int max) {
        zks.setMaxSessionTimeout(max);
    }

    public long getDataDirSize() {
        return zks.getDataDirSize();
    }

    public long getLogDirSize() {
        return zks.getLogDirSize();
    }

    public long getPacketsReceived() {
        return zks.serverStats().getPacketsReceived();
    }

    public long getPacketsSent() {
        return zks.serverStats().getPacketsSent();
    }

    public long getFsyncThresholdExceedCount() {
        return zks.serverStats().getFsyncThresholdExceedCount();
    }

    public void resetLatency() {
        zks.serverStats().resetLatency();
    }

    public void resetMaxLatency() {
        zks.serverStats().resetMaxLatency();
    }

    public void resetFsyncThresholdExceedCount() {
        zks.serverStats().resetFsyncThresholdExceedCount();
    }

    public void resetStatistics() {
        ServerStats serverStats = zks.serverStats();
        serverStats.resetRequestCounters();
        serverStats.resetLatency();
        serverStats.resetFsyncThresholdExceedCount();
    }

    public long getNumAliveConnections() {
        return zks.getNumAliveConnections();
    }

    @Override
    public String getSecureClientPort() {
        if (zks.secureServerCnxnFactory != null) {
            return Integer.toString(zks.secureServerCnxnFactory.getLocalPort());
        }
        return "";
    }

    @Override
    public String getSecureClientAddress() {
        if (zks.secureServerCnxnFactory != null) {
            return String.format("%s:%d", zks.secureServerCnxnFactory
                            .getLocalAddress().getHostString(),
                    zks.secureServerCnxnFactory.getLocalPort());
        }
        return "";
    }

    @Override
    public long getTxnLogElapsedSyncTime() {
        return zks.getTxnLogElapsedSyncTime();
    }

    @Override
    public int getJuteMaxBufferSize() {
        return BinaryInputArchive.maxBuffer;
    }

    @Override
    public int getLastClientResponseSize() {
        return zks.serverStats().getClientResponseStats().getLastBufferSize();
    }

    @Override
    public int getMinClientResponseSize() {
        return zks.serverStats().getClientResponseStats().getMinBufferSize();
    }

    @Override
    public int getMaxClientResponseSize() {
        return zks.serverStats().getClientResponseStats().getMaxBufferSize();
    }
}
