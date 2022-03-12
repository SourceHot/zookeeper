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

package org.apache.zookeeper;

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClientCnxnSocketNIO.class);

    /**
     * 选择器
     */
    private final Selector selector = Selector.open();

    /**
     * 选择键
     */
    private SelectionKey sockKey;

    /**
     * 本地套接字地址
     */
    private SocketAddress localSocketAddress;

    /**
     * 远端套接字地址
     */
    private SocketAddress remoteSocketAddress;

    ClientCnxnSocketNIO(ZKClientConfig clientConfig) throws IOException {
        this.clientConfig = clientConfig;
        initProperties();
    }

    @Override
    boolean isConnected() {
        return sockKey != null;
    }

    /**
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    void doIO(List<Packet> pendingQueue, ClientCnxn cnxn)
            throws InterruptedException, IOException {
        // 从成员变量sockKey中获取socket通道
        SocketChannel sock = (SocketChannel) sockKey.channel();
        // 如果socket通道为空抛出异常
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        // sockKey可读
        if (sockKey.isReadable()) {
            // 从socket中读取incomingBuffer量的数据
            int rc = sock.read(incomingBuffer);
            // 读取结果小于0表示从服务端读取失败
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            // 指定区间内没有内容的情况下
            if (!incomingBuffer.hasRemaining()) {
                incomingBuffer.flip();
                // incomingBuffer和lenBuffer相同
                if (incomingBuffer == lenBuffer) {
                    // 接收数量累加1
                    recvCount.getAndIncrement();
                    // 读取长度信息
                    readLength();
                }
                // 未初始化的情况
                else if (!initialized) {
                    // 读取连接结果
                    readConnectResult();
                    enableRead();
                    if (findSendablePacket(outgoingQueue,
                            sendThread.tunnelAuthInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite();
                    }
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;
                }
                // 其他情况
                else {
                    // 交给sendThread读取响应
                    sendThread.readResponse(incomingBuffer);
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }
        // sockKey可写
        if (sockKey.isWritable()) {
            // 寻找需要发送的Packet
            Packet p = findSendablePacket(outgoingQueue,
                    sendThread.tunnelAuthInProgress());


            // Packet不为空的情况
            if (p != null) {
                // 更新lastSend
                updateLastSend();
                // If we already started writing p, p.bb will already exist
                // packet中的bb变量为空将会创建bb变量
                if (p.bb == null) {
                    if ((p.requestHeader != null) &&
                            (p.requestHeader.getType() != OpCode.ping) &&
                            (p.requestHeader.getType() != OpCode.auth)) {
                        p.requestHeader.setXid(cnxn.getXid());
                    }
                    p.createBB();
                }
                // 写出bb
                sock.write(p.bb);
                // 指定区间内没有内容的情况下
                if (!p.bb.hasRemaining()) {
                    // 发送数量累加1
                    sentCount.getAndIncrement();
                    // 发送队列中移除p
                    outgoingQueue.removeFirstOccurrence(p);

                    // 如果请求类型不是ping和auth将packet写入参数pendingQueue中
                    if (p.requestHeader != null
                            && p.requestHeader.getType() != OpCode.ping
                            && p.requestHeader.getType() != OpCode.auth) {
                        synchronized (pendingQueue) {
                            pendingQueue.add(p);
                        }
                    }
                }
            }
            // 如果需要写出的包集合为空将关闭写入
            if (outgoingQueue.isEmpty()) {
                // No more packets to send: turn off write interest flag.
                // Will be turned on later by a later call to enableWrite(),
                // from within ZooKeeperSaslClient (if client is configured
                // to attempt SASL authentication), or in either doIO() or
                // in doTransport() if not.
                disableWrite();
            }
            // 如果未初始化并且数据包不为空并且数据包的bb变量在指定范围内没有数据将关闭写入操作
            else if (!initialized && p != null && !p.bb.hasRemaining()) {
                // On initial connection, write the complete connect request
                // packet, but then disable further writes until after
                // receiving a successful connection response.  If the
                // session is expired, then the server sends the expiration
                // response and immediately closes its end of the socket.  If
                // the client is simultaneously writing on its end, then the
                // TCP stack may choose to abort with RST, in which case the
                // client would never receive the session expired event.  See
                // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                disableWrite();
            }
            // 启用写入操作
            else {
                // Just in case
                enableWrite();
            }
        }
    }

    private Packet findSendablePacket(LinkedBlockingDeque<Packet> outgoingQueue,
                                      boolean tunneledAuthInProgres) {
        if (outgoingQueue.isEmpty()) {
            return null;
        }
        // If we've already starting sending the first packet, we better finish
        if (outgoingQueue.getFirst().bb != null || !tunneledAuthInProgres) {
            return outgoingQueue.getFirst();
        }
        // Since client's authentication with server is in progress,
        // send only the null-header packet queued by primeConnection().
        // This packet must be sent so that the SASL authentication process
        // can proceed, but all other packets should wait until
        // SASL authentication completes.
        Iterator<Packet> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            Packet p = iter.next();
            if (p.requestHeader == null) {
                // We've found the priming-packet. Move it to the beginning of the queue.
                iter.remove();
                outgoingQueue.addFirst(p);
                return p;
            } else {
                // Non-priming packet: defer it until later, leaving it in the queue
                // until authentication completes.
                LOG.debug("deferring non-priming packet {} until SASL authentation completes.", p);
            }
        }
        return null;
    }

    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                // 先关闭输入
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                // 再关闭输出
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                // socket关闭
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                // 通道关闭
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }

    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }

    /**
     * create a socket channel.
     *
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     *
     * @param sock the {@link SocketChannel}
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr)
            throws IOException {
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        boolean immediateConnect = sock.connect(addr);
        if (immediateConnect) {
            sendThread.primeConnection();
        }
    }

    @Override
    void connect(InetSocketAddress addr) throws IOException {
        // 打开socket通道
        SocketChannel sock = createSock();
        try {
            // 注册套接字地址并连接
            registerAndConnect(sock, addr);
        } catch (UnresolvedAddressException | UnsupportedAddressTypeException | SecurityException | IOException e) {
            LOG.error("Unable to open socket to {}" + addr);
            sock.close();
            throw e;
        }
        //
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     *
     * @return ip address of the remote side of the connection or null if not
     * connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        return remoteSocketAddress;
    }

    /**
     * Returns the local address to which the socket is bound.
     *
     * @return ip address of the remote side of the connection or null if not
     * connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        return localSocketAddress;
    }

    private void updateSocketAddresses() {
        Socket socket = ((SocketChannel) sockKey.channel()).socket();
        localSocketAddress = socket.getLocalSocketAddress();
        remoteSocketAddress = socket.getRemoteSocketAddress();
    }

    @Override
    void packetAdded() {
        wakeupCnxn();
    }

    @Override
    void onClosing() {
        wakeupCnxn();
    }

    private synchronized void wakeupCnxn() {
        selector.wakeup();
    }

    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, ClientCnxn cnxn)
            throws IOException, InterruptedException {
        // 设置等待时间
        selector.select(waitTimeOut);
        // SelectionKey集合
        Set<SelectionKey> selected;
        // 初始化SelectionKey集合
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here

        // 计算成员变量now
        updateNow();
        for (SelectionKey k : selected) {
            // 从SelectionKey中获取socket通道
            SocketChannel sc = ((SocketChannel) k.channel());
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                // 完成连接
                if (sc.finishConnect()) {
                    // 更新lastSend和lastHeard
                    updateLastSendAndHeard();
                    // 更新localSocketAddress和remoteSocketAddress
                    updateSocketAddresses();
                    sendThread.primeConnection();
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                doIO(pendingQueue, cnxn);
            }
        }
        // 发送线程中的zk状态是已连接
        if (sendThread.getZkState().isConnected()) {
            // 查询是否有需要发送的Packet
            if (findSendablePacket(outgoingQueue,
                    sendThread.tunnelAuthInProgress()) != null) {
                enableWrite();
            }
        }
        selected.clear();
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        // sockKey may be concurrently accessed by multiple
        // threads. We use tmp here to avoid a race condition
        SelectionKey tmp = sockKey;
        if (tmp != null) {
            ((SocketChannel) tmp.channel()).socket().close();
        }
    }

    @Override
    void saslCompleted() {
        enableWrite();
    }

    // 开启写入
    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    // 关闭写入
    private synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    // 开启读取
    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    void connectionPrimed() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }
}
