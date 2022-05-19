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

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to a Server connection - represents a connection from a client
 * to the server.
 *
 * 服务器链接类
 */
public abstract class ServerCnxn implements Stats, Watcher {
    // This is just an arbitrary object to represent requests issued by
    // (aka owned by) this class
    //
    final public static Object me = new Object();
    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxn.class);
    /**
     * 四字节变量
     */
    private static final byte[] fourBytes = new byte[4];
    /**
     * 连接创建时间
     */
    protected final Date established = new Date();
    /**
     * 收到的数据包数量
     */
    protected final AtomicLong packetsReceived = new AtomicLong();
    /**
     * 发送的数据包数量
     */
    protected final AtomicLong packetsSent = new AtomicLong();
    /**
     * id集合，认证信息集合
     */
    protected ArrayList<Id> authInfo = new ArrayList<Id>();
    /**
     * sasl服务
     */
    protected ZooKeeperSaslServer zooKeeperSaslServer = null;
    /**
     * 最小延迟
     */
    protected long minLatency;
    /**
     * 最大延迟
     */
    protected long maxLatency;
    /**
     * 最后的操作类型
     */
    protected String lastOp;
    /**
     * 最后的cxid (客户端操作序号)
     */
    protected long lastCxid;
    /**
     * 最后的zxid
     */
    protected long lastZxid;
    /**
     * 最后响应时间
     */
    protected long lastResponseTime;
    /**
     * 最后延迟时间
     */
    protected long lastLatency;
    /**
     * 请求处理次数
     */
    protected long count;
    /**
     * 总计延迟时间
     */
    protected long totalLatency;
    /**
     * If the client is of old version, we don't send r-o mode info to it.
     * The reason is that if we would, old C client doesn't read it, which
     * results in TCP RST packet, i.e. "connection reset by peer".
     * 是否是旧的客户端
     */
    boolean isOldClient = true;

    /**
     * 获取session超时时间
     *
     * @return
     */
    abstract int getSessionTimeout();

    /**
     * 设置session超时时间
     *
     * @param sessionTimeout
     */
    abstract void setSessionTimeout(int sessionTimeout);

    /**
     * 关闭服务
     */
    abstract void close();

    /**
     * 发送响应
     *
     * @param h
     * @param r
     * @param tag
     * @throws IOException
     */
    public void sendResponse(ReplyHeader h, Record r, String tag) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // Make space for length
        BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
        try {
            baos.write(fourBytes);
            bos.writeRecord(h, "header");
            if (r != null) {
                bos.writeRecord(r, tag);
            }
            baos.close();
        } catch (IOException e) {
            LOG.error("Error serializing response");
        }
        byte b[] = baos.toByteArray();
        serverStats().updateClientResponseSize(b.length - 4);
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putInt(b.length - 4).rewind();
        sendBuffer(bb);
    }

    /*
     notify the client the session is closing and close/cleanup socket
     发送关闭session信息
    * */
    abstract void sendCloseSession();

    /**
     * 处理观察者事件
     *
     * @param event
     */
    public abstract void process(WatchedEvent event);

    /**
     * 获取session id
     * @return
     */
    public abstract long getSessionId();

    /**
     * 设置session id
     * @param sessionId
     */
    abstract void setSessionId(long sessionId);

    /**
     * auth info for the cnxn, returns an unmodifyable list
     * 获取Id信息
     */
    public List<Id> getAuthInfo() {
        return Collections.unmodifiableList(authInfo);
    }

    /**
     * 添加 Id 信息
     * @param id
     */
    public void addAuthInfo(Id id) {
        if (authInfo.contains(id) == false) {
            authInfo.add(id);
        }
    }

    /**
     * 移除 Id 信息
     * @param id
     * @return
     */
    public boolean removeAuthInfo(Id id) {
        return authInfo.remove(id);
    }

    /**
     * 发送字节数据
     * @param closeConn
     */
    abstract void sendBuffer(ByteBuffer closeConn);

    /**
     * 恢复数据接收
     */
    abstract void enableRecv();

    /**
     * 禁止接收数据
     */
    abstract void disableRecv();

    /**
     * 接收数据包
     */
    protected void packetReceived() {
        // 接收数据包数量加1
        incrPacketsReceived();

        // 获取服务统计
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            // 数据包接收数量加1
            serverStats().incrementPacketsReceived();
        }
    }

    /**
     * 发送数据包
     */
    protected void packetSent() {
        // 发送数据包数量加1
        incrPacketsSent();
        // 获取服务统计
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            // 发送数据包数量加1
            serverStats.incrementPacketsSent();
        }
    }

    /**
     * 获取服务统计
     */
    protected abstract ServerStats serverStats();

    /**
     * 重置服务统计信息
     */
    public synchronized void resetStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        minLatency = Long.MAX_VALUE;
        maxLatency = 0;
        lastOp = "NA";
        lastCxid = -1;
        lastZxid = -1;
        lastResponseTime = 0;
        lastLatency = 0;

        count = 0;
        totalLatency = 0;
    }

    /**
     * 接收数据包数量加1
     * @return
     */
    protected long incrPacketsReceived() {
        return packetsReceived.incrementAndGet();
    }

    /**
     * 增加未完成的请求数量
     */
    protected void incrOutstandingRequests(RequestHeader h) {
    }

    /**
     * 发送数据包数量加1
     * @return
     */
    protected long incrPacketsSent() {
        return packetsSent.incrementAndGet();
    }

    /**
     * 在响应阶段更新统计信息
     */
    protected synchronized void updateStatsForResponse(long cxid, long zxid,
                                                       String op, long start, long end) {
        // don't overwrite with "special" xids - we're interested
        // in the clients last real operation
        if (cxid >= 0) {
            lastCxid = cxid;
        }
        lastZxid = zxid;
        lastOp = op;
        lastResponseTime = end;
        long elapsed = end - start;
        lastLatency = elapsed;
        if (elapsed < minLatency) {
            minLatency = elapsed;
        }
        if (elapsed > maxLatency) {
            maxLatency = elapsed;
        }
        count++;
        totalLatency += elapsed;
    }

    /**
     * 获取连接创建时间
     * @return
     */
    public Date getEstablished() {
        return (Date) established.clone();
    }

    /**
     * 获取未完成的请求数量
     */
    public abstract long getOutstandingRequests();

    /**
     * 获取已经处理的数据包数量
     */
    public long getPacketsReceived() {
        return packetsReceived.longValue();
    }

    /**
     * 获取已经发送的数据包数量
     * @return
     */
    public long getPacketsSent() {
        return packetsSent.longValue();
    }

    /**
     * 获取最小延迟
     * @return
     */
    public synchronized long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }


    /**
     * 获取平均延迟
     * @return
     */
    public synchronized long getAvgLatency() {
        return count == 0 ? 0 : totalLatency / count;
    }

    /**
     * 获取最大延迟
     * @return
     */
    public synchronized long getMaxLatency() {
        return maxLatency;
    }

    /**
     * 获取最后一个操作类型
     * @return
     */
    public synchronized String getLastOperation() {
        return lastOp;
    }

    /**
     * 获取最后的cxid (客户端操作序号)
     * @return
     */
    public synchronized long getLastCxid() {
        return lastCxid;
    }

    /**
     * 获取最后的zxid
     * @return
     */
    public synchronized long getLastZxid() {
        return lastZxid;
    }

    /**
     * 获取最后响应时间
     * @return
     */
    public synchronized long getLastResponseTime() {
        return lastResponseTime;
    }

    /**
     * 获取最后延迟时间
     * @return
     */
    public synchronized long getLastLatency() {
        return lastLatency;
    }

    /**
     * Prints detailed stats information for the connection.
     *
     * @see dumpConnectionInfo(PrintWriter, boolean) for brief stats
     */
    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpConnectionInfo(pwriter, false);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    /**
     * 获取远端套接字地址
     * @return
     */
    public abstract InetSocketAddress getRemoteSocketAddress();

    /**
     * 获取可行的操作
     * @return
     */
    public abstract int getInterestOps();

    /**
     * 是否启用sasl
     * @return
     */
    public abstract boolean isSecure();

    /**
     * 获取证书
     * @return
     */
    public abstract Certificate[] getClientCertificateChain();

    /**
     * 设置证书
     * @param chain
     */
    public abstract void setClientCertificateChain(Certificate[] chain);

    /**
     * 输出连接信息
     * Print information about the connection.
     * @param brief iff true prints brief details, otw full detail
     * @return information about this connection
     */
    public synchronized void
    dumpConnectionInfo(PrintWriter pwriter, boolean brief) {
        pwriter.print(" ");
        pwriter.print(getRemoteSocketAddress());
        pwriter.print("[");
        int interestOps = getInterestOps();
        pwriter.print(interestOps == 0 ? "0" : Integer.toHexString(interestOps));
        pwriter.print("](queued=");
        pwriter.print(getOutstandingRequests());
        pwriter.print(",recved=");
        pwriter.print(getPacketsReceived());
        pwriter.print(",sent=");
        pwriter.print(getPacketsSent());

        if (!brief) {
            long sessionId = getSessionId();
            if (sessionId != 0) {
                pwriter.print(",sid=0x");
                pwriter.print(Long.toHexString(sessionId));
                pwriter.print(",lop=");
                pwriter.print(getLastOperation());
                pwriter.print(",est=");
                pwriter.print(getEstablished().getTime());
                pwriter.print(",to=");
                pwriter.print(getSessionTimeout());
                long lastCxid = getLastCxid();
                if (lastCxid >= 0) {
                    pwriter.print(",lcxid=0x");
                    pwriter.print(Long.toHexString(lastCxid));
                }
                pwriter.print(",lzxid=0x");
                pwriter.print(Long.toHexString(getLastZxid()));
                pwriter.print(",lresp=");
                pwriter.print(getLastResponseTime());
                pwriter.print(",llat=");
                pwriter.print(getLastLatency());
                pwriter.print(",minlat=");
                pwriter.print(getMinLatency());
                pwriter.print(",avglat=");
                pwriter.print(getAvgLatency());
                pwriter.print(",maxlat=");
                pwriter.print(getMaxLatency());
            }
        }
        pwriter.print(")");
    }

    /**
     * 获取连接信息
     * @param brief
     * @return
     */
    public synchronized Map<String, Object> getConnectionInfo(boolean brief) {
        Map<String, Object> info = new LinkedHashMap<String, Object>();
        info.put("remote_socket_address", getRemoteSocketAddress());
        info.put("interest_ops", getInterestOps());
        info.put("outstanding_requests", getOutstandingRequests());
        info.put("packets_received", getPacketsReceived());
        info.put("packets_sent", getPacketsSent());
        if (!brief) {
            info.put("session_id", getSessionId());
            info.put("last_operation", getLastOperation());
            info.put("established", getEstablished());
            info.put("session_timeout", getSessionTimeout());
            info.put("last_cxid", getLastCxid());
            info.put("last_zxid", getLastZxid());
            info.put("last_response_time", getLastResponseTime());
            info.put("last_latency", getLastLatency());
            info.put("min_latency", getMinLatency());
            info.put("avg_latency", getAvgLatency());
            info.put("max_latency", getMaxLatency());
        }
        return info;
    }

    /**
     * clean up the socket related to a command and also make sure we flush the
     * data before we do that
     *
     * 清理输出对象
     * @param pwriter
     *            the pwriter for a command socket
     */
    public void cleanupWriterSocket(PrintWriter pwriter) {
        try {
            if (pwriter != null) {
                pwriter.flush();
                pwriter.close();
            }
        } catch (Exception e) {
            LOG.info("Error closing PrintWriter ", e);
        } finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error closing a command socket ", e);
            }
        }
    }


    protected static class CloseRequestException extends IOException {
        private static final long serialVersionUID = -7854505709816442681L;

        public CloseRequestException(String msg) {
            super(msg);
        }
    }


    protected static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -8255690282104294178L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }
}
