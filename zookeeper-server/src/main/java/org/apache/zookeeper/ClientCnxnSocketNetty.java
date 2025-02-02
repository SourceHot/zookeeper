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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.NettyUtils;
import org.apache.zookeeper.common.X509Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.zookeeper.common.X509Exception.SSLContextException;

/**
 * ClientCnxnSocketNetty implements ClientCnxnSocket abstract methods.
 * It's responsible for connecting to server, reading/writing network traffic and
 * being a layer between network data and higher level packets.
 */
public class ClientCnxnSocketNetty extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocketNetty.class);
    private static final AtomicReference<ByteBufAllocator> TEST_ALLOCATOR =
            new AtomicReference<>(null);
    /**
     * 事件组
     */
    private final EventLoopGroup eventLoopGroup;
    /**
     * 连接锁
     */
    private final Lock connectLock = new ReentrantLock();
    /**
     * 是否断开连接
     */
    private final AtomicBoolean disconnected = new AtomicBoolean();
    /**
     * 是否需要SASL
     */
    private final AtomicBoolean needSasl = new AtomicBoolean();
    /**
     * 型号量
     */
    private final Semaphore waitSasl = new Semaphore(0);
    // Use a single listener instance to reduce GC
    /**
     * 发送成功监听器
     */
    private final GenericFutureListener<Future<Void>> onSendPktDoneListener = f -> {
        if (f.isSuccess()) {
            sentCount.getAndIncrement();
        }
    };
    /**
     * 通道
     */
    private Channel channel;
    /**
     * 同步器
     */
    private CountDownLatch firstConnect;
    private ChannelFuture connectFuture;

    ClientCnxnSocketNetty(ZKClientConfig clientConfig) throws IOException {
        this.clientConfig = clientConfig;
        // Client only has 1 outgoing socket, so the event loop group only needs
        // a single thread.
        eventLoopGroup = NettyUtils.newNioOrEpollEventLoopGroup(1 /* nThreads */);
        initProperties();
    }

    /**
     * Sets the test ByteBufAllocator. This allocator will be used by all
     * future instances of this class.
     * It is not recommended to use this method outside of testing.
     *
     * @param allocator the ByteBufAllocator to use for all netty buffer
     *                  allocations.
     */
    static void setTestAllocator(ByteBufAllocator allocator) {
        TEST_ALLOCATOR.set(allocator);
    }

    /**
     * Clears the test ByteBufAllocator. The default allocator will be used
     * by all future instances of this class.
     * It is not recommended to use this method outside of testing.
     */
    static void clearTestAllocator() {
        TEST_ALLOCATOR.set(null);
    }

    /**
     * lifecycles diagram:
     * <p/>
     * loop:
     * - try:
     * - - !isConnected()
     * - - - connect()
     * - - doTransport()
     * - catch:
     * - - cleanup()
     * close()
     * <p/>
     * Other non-lifecycle methods are in jeopardy getting a null channel
     * when calling in concurrency. We must handle it.
     */

    @Override
    boolean isConnected() {
        // Assuming that isConnected() is only used to initiate connection,
        // not used by some other connection status judgement.
        connectLock.lock();
        try {
            return channel != null || connectFuture != null;
        } finally {
            connectLock.unlock();
        }
    }

    private Bootstrap configureBootstrapAllocator(Bootstrap bootstrap) {
        ByteBufAllocator testAllocator = TEST_ALLOCATOR.get();
        if (testAllocator != null) {
            return bootstrap.option(ChannelOption.ALLOCATOR, testAllocator);
        } else {
            return bootstrap;
        }
    }

    @Override
    void connect(InetSocketAddress addr) throws IOException {
        // 同步器初始化
        firstConnect = new CountDownLatch(1);

        // 创建Netty引导程序
        Bootstrap bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NettyUtils.nioOrEpollSocketChannel())
                .option(ChannelOption.SO_LINGER, -1)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ZKClientPipelineFactory(addr.getHostString(), addr.getPort()));
        // 配置引导程序
        bootstrap = configureBootstrapAllocator(bootstrap);
        // 验证引导程序
        bootstrap.validate();

        // 上锁
        connectLock.lock();
        try {
            // 建立连接
            connectFuture = bootstrap.connect(addr);
            // 在连接结果中增加监听器
            connectFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    // this lock guarantees that channel won't be assigned after cleanup().
                    // 初始化连接成功标记为false
                    boolean connected = false;
                    connectLock.lock();
                    try {
                        // 建立连接失败
                        if (!channelFuture.isSuccess()) {
                            LOG.info("future isn't success, cause:", channelFuture.cause());
                            return;
                        }
                        // connectFuture为空关闭通道
                        else if (connectFuture == null) {
                            LOG.info("connect attempt cancelled");
                            // If the connect attempt was cancelled but succeeded
                            // anyway, make sure to close the channel, otherwise
                            // we may leak a file descriptor.
                            channelFuture.channel().close();
                            return;
                        }
                        // setup channel, variables, connection, etc.
                        // 打开通道赋值给成员变量channel
                        channel = channelFuture.channel();
                        // 设置是否断开为未断开
                        disconnected.set(false);
                        // 设置是否初始化为未初始化
                        initialized = false;
                        // 清理lenBuffer变量和incomingBuffer变量
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;

                        sendThread.primeConnection();
                        // 更新now、lastSend和lastHeard
                        updateNow();
                        updateLastSendAndHeard();

                        // 是否处于身份验证中
                        if (sendThread.tunnelAuthInProgress()) {
                            waitSasl.drainPermits();
                            needSasl.set(true);
                            sendPrimePacket();
                        } else {
                            needSasl.set(false);
                        }
                        connected = true;
                    } finally {
                        connectFuture = null;
                        connectLock.unlock();
                        if (connected) {
                            LOG.info("channel is connected: {}", channelFuture.channel());
                        }
                        // need to wake on connect success or failure to avoid
                        // timing out ClientCnxn.SendThread which may be
                        // blocked waiting for first connect in doTransport().
                        wakeupCnxn();
                        firstConnect.countDown();
                    }
                }
            });
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    void cleanup() {
        connectLock.lock();
        try {
            if (connectFuture != null) {
                connectFuture.cancel(false);
                connectFuture = null;
            }
            if (channel != null) {
                channel.close().syncUninterruptibly();
                channel = null;
            }
        } finally {
            connectLock.unlock();
        }
        Iterator<Packet> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            Packet p = iter.next();
            if (p == WakeupPacket.getInstance()) {
                iter.remove();
            }
        }
    }

    @Override
    void close() {
        eventLoopGroup.shutdownGracefully();
    }

    @Override
    void saslCompleted() {
        needSasl.set(false);
        waitSasl.release();
    }

    @Override
    void connectionPrimed() {
    }

    @Override
    void packetAdded() {
        // NO-OP. Adding a packet will already wake up a netty connection
        // so we don't need to add a dummy packet to the queue to trigger
        // a wake-up.
    }

    @Override
    void onClosing() {
        firstConnect.countDown();
        wakeupCnxn();
        LOG.info("channel is told closing");
    }

    private void wakeupCnxn() {
        if (needSasl.get()) {
            waitSasl.release();
        }
        outgoingQueue.add(WakeupPacket.getInstance());
    }

    @Override
    void doTransport(int waitTimeOut,
                     List<Packet> pendingQueue,
                     ClientCnxn cnxn)
            throws IOException, InterruptedException {
        try {
            // 在一定时间内计数器未清零结束处理
            if (!firstConnect.await(waitTimeOut, TimeUnit.MILLISECONDS)) {
                return;
            }
            Packet head = null;
            if (needSasl.get()) {
                // 信号量获取失败结束处理
                if (!waitSasl.tryAcquire(waitTimeOut, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } else {
                // 从outgoingQueue中获取数据包
                head = outgoingQueue.poll(waitTimeOut, TimeUnit.MILLISECONDS);
            }
            // check if being waken up on closing.
            // 如果发送线程中的zk状态为不存活
            if (!sendThread.getZkState().isAlive()) {
                // adding back the packet to notify of failure in conLossPacket().
                // 将数据包写入到outgoingQueue第一位
                addBack(head);
                return;
            }
            // channel disconnection happenedx
            // 如果连接断开
            if (disconnected.get()) {
                // 将数据包写入到outgoingQueue第一位
                addBack(head);
                throw new EndOfStreamException("channel for sessionid 0x"
                        + Long.toHexString(sessionId)
                        + " is lost");
            }
            // 数据包不为空的情况下写出
            if (head != null) {
                doWrite(pendingQueue, head, cnxn);
            }
        } finally {
            updateNow();
        }
    }

    private void addBack(Packet head) {
        if (head != null && head != WakeupPacket.getInstance()) {
            outgoingQueue.addFirst(head);
        }
    }

    /**
     * Sends a packet to the remote peer and flushes the channel.
     *
     * @param p packet to send.
     * @return a ChannelFuture that will complete when the write operation
     * succeeds or fails.
     */
    private ChannelFuture sendPktAndFlush(Packet p) {
        return sendPkt(p, true);
    }

    /**
     * Sends a packet to the remote peer but does not flush() the channel.
     *
     * @param p packet to send.
     * @return a ChannelFuture that will complete when the write operation
     * succeeds or fails.
     */
    private ChannelFuture sendPktOnly(Packet p) {
        return sendPkt(p, false);
    }

    private ChannelFuture sendPkt(Packet p, boolean doFlush) {
        // Assuming the packet will be sent out successfully. Because if it fails,
        // the channel will close and clean up queues.
        // 创建数据包中的bb变量
        p.createBB();
        updateLastSend();
        // 包装bb变量
        final ByteBuf writeBuffer = Unpooled.wrappedBuffer(p.bb);
        // 写出包装后的字节
        final ChannelFuture result = doFlush
                ? channel.writeAndFlush(writeBuffer)
                : channel.write(writeBuffer);
        // 添加监听器
        result.addListener(onSendPktDoneListener);
        // 返回写出结果
        return result;
    }

    private void sendPrimePacket() {
        // assuming the first packet is the priming packet.
        sendPktAndFlush(outgoingQueue.remove());
    }

    /**
     * doWrite handles writing the packets from outgoingQueue via network to server.
     */
    private void doWrite(List<Packet> pendingQueue, Packet p, ClientCnxn cnxn) {
        updateNow();
        // 是否全部发送
        boolean anyPacketsSent = false;
        while (true) {
            // 数据包和空数据包不相同
            if (p != WakeupPacket.getInstance()) {
                // 数据包头信息不为空
                // 数据包头信息中的类型不为ping和auth
                if ((p.requestHeader != null) &&
                        (p.requestHeader.getType() != ZooDefs.OpCode.ping) &&
                        (p.requestHeader.getType() != ZooDefs.OpCode.auth)) {
                    // 设置头信息的xid
                    p.requestHeader.setXid(cnxn.getXid());
                    synchronized (pendingQueue) {
                        pendingQueue.add(p);
                    }
                }
                // 发送包
                sendPktOnly(p);
                anyPacketsSent = true;
            }
            // 如果outgoingQueue为空跳出循环
            if (outgoingQueue.isEmpty()) {
                break;
            }
            // 从outgoingQueue中移除一个数据包用于后续发送
            p = outgoingQueue.remove();
        }
        // TODO: maybe we should flush in the loop above every N packets/bytes?
        // But, how do we determine the right value for N ...
        // 如果全部发送则进行flush操作
        if (anyPacketsSent) {
            channel.flush();
        }
    }

    @Override
    void sendPacket(ClientCnxn.Packet p) throws IOException {
        if (channel == null) {
            throw new IOException("channel has been closed");
        }
        sendPktAndFlush(p);
    }

    @Override
    SocketAddress getRemoteSocketAddress() {
        Channel copiedChanRef = channel;
        return (copiedChanRef == null) ? null : copiedChanRef.remoteAddress();
    }

    @Override
    SocketAddress getLocalSocketAddress() {
        Channel copiedChanRef = channel;
        return (copiedChanRef == null) ? null : copiedChanRef.localAddress();
    }

    @Override
    void testableCloseSocket() throws IOException {
        Channel copiedChanRef = channel;
        if (copiedChanRef != null) {
            copiedChanRef.disconnect().awaitUninterruptibly();
        }
    }


    // *************** <END> CientCnxnSocketNetty </END> ******************
    private static class WakeupPacket {
        private static final Packet instance = new Packet(null, null, null, null, null);

        protected WakeupPacket() {
            // Exists only to defeat instantiation.
        }

        public static Packet getInstance() {
            return instance;
        }
    }


    /**
     * ZKClientPipelineFactory is the netty pipeline factory for this netty
     * connection implementation.
     */
    private class ZKClientPipelineFactory extends ChannelInitializer<SocketChannel> {
        private SSLContext sslContext = null;
        private SSLEngine sslEngine = null;
        private final String host;
        private final int port;

        public ZKClientPipelineFactory(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            if (clientConfig.getBoolean(ZKClientConfig.SECURE_CLIENT)) {
                initSSL(pipeline);
            }
            pipeline.addLast("handler", new ZKClientHandler());
        }

        // The synchronized is to prevent the race on shared variable "sslEngine".
        // Basically we only need to create it once.
        private synchronized void initSSL(ChannelPipeline pipeline) throws SSLContextException {
            if (sslContext == null || sslEngine == null) {
                try (X509Util x509Util = new ClientX509Util()) {
                    sslContext = x509Util.createSSLContext(clientConfig);
                    sslEngine = sslContext.createSSLEngine(host, port);
                    sslEngine.setUseClientMode(true);
                }
            }
            pipeline.addLast("ssl", new SslHandler(sslEngine));
            LOG.info("SSL handler added for channel: {}", pipeline.channel());
        }
    }


    /**
     * ZKClientHandler is the netty handler that sits in netty upstream last
     * place. It mainly handles read traffic and helps synchronize connection state.
     */
    private class ZKClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
        AtomicBoolean channelClosed = new AtomicBoolean(false);

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("channel is disconnected: {}", ctx.channel());
            cleanup();
        }

        /**
         * netty handler has encountered problems. We are cleaning it up and tell outside to close
         * the channel/connection.
         */
        private void cleanup() {
            if (!channelClosed.compareAndSet(false, true)) {
                return;
            }
            disconnected.set(true);
            onClosing();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
            updateNow();
            while (buf.isReadable()) {
                if (incomingBuffer.remaining() > buf.readableBytes()) {
                    int newLimit = incomingBuffer.position()
                            + buf.readableBytes();
                    incomingBuffer.limit(newLimit);
                }
                buf.readBytes(incomingBuffer);
                incomingBuffer.limit(incomingBuffer.capacity());

                if (!incomingBuffer.hasRemaining()) {
                    incomingBuffer.flip();
                    if (incomingBuffer == lenBuffer) {
                        recvCount.getAndIncrement();
                        readLength();
                    } else if (!initialized) {
                        readConnectResult();
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;
                        initialized = true;
                        updateLastHeard();
                    } else {
                        sendThread.readResponse(incomingBuffer);
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;
                        updateLastHeard();
                    }
                }
            }
            wakeupCnxn();
            // Note: SimpleChannelInboundHandler releases the ByteBuf for us
            // so we don't need to do it.
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("Exception caught", cause);
            cleanup();
        }
    }
}
