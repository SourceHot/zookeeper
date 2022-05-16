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

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Login;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.auth.SaslServerCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * ServerCnxn工厂
 */
public abstract class ServerCnxnFactory {

    public static final String ZOOKEEPER_SERVER_CNXN_FACTORY = "zookeeper.serverCnxnFactory";
    /**
     * The buffer will cause the connection to be close when we do a send.
     */
    static final ByteBuffer closeConn = ByteBuffer.allocate(0);
    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxnFactory.class);
    // Connection set is relied on heavily by four letter commands
    // Construct a ConcurrentHashSet using a ConcurrentHashMap

    /**
     * 服务器链接集合
     */
    protected final Set<ServerCnxn> cnxns = Collections.newSetFromMap(
            new ConcurrentHashMap<ServerCnxn, Boolean>());
    /**
     * 服务器链接和客户端连接之间的映射关系
     */
    private final ConcurrentHashMap<ServerCnxn, ConnectionBean> connectionBeans =
            new ConcurrentHashMap<ServerCnxn, ConnectionBean>();
    /**
     * 登陆信息
     */
    public Login login;
    // Tells whether SSL is enabled on this ServerCnxnFactory
    /**
     * 是否启用ssl
     */
    protected boolean secure;
    /**
     * Sasl 服务器回调处理程序
     */
    protected SaslServerCallbackHandler saslServerCallbackHandler;
    /**
     * zk服务
     */
    protected ZooKeeperServer zkServer;

    static public ServerCnxnFactory createFactory() throws IOException {
        // zookeeper 服务连接工厂类名
        String serverCnxnFactoryName =
                System.getProperty(ZOOKEEPER_SERVER_CNXN_FACTORY);
        // 类名为空采用NIOServerCnxnFactory类名
        if (serverCnxnFactoryName == null) {
            serverCnxnFactoryName = NIOServerCnxnFactory.class.getName();
        }
        try {
            // 反射创建类
            ServerCnxnFactory serverCnxnFactory =
                    (ServerCnxnFactory) Class.forName(serverCnxnFactoryName)
                            .getDeclaredConstructor().newInstance();
            LOG.info("Using {} as server connection factory", serverCnxnFactoryName);
            return serverCnxnFactory;
        } catch (Exception e) {
            IOException ioe = new IOException("Couldn't instantiate "
                    + serverCnxnFactoryName);
            ioe.initCause(e);
            throw ioe;
        }
    }

    static public ServerCnxnFactory createFactory(int clientPort,
                                                  int maxClientCnxns) throws IOException {
        return createFactory(new InetSocketAddress(clientPort), maxClientCnxns);
    }

    static public ServerCnxnFactory createFactory(InetSocketAddress addr,
                                                  int maxClientCnxns) throws IOException {
        ServerCnxnFactory factory = createFactory();
        factory.configure(addr, maxClientCnxns);
        return factory;
    }

    /**
     * 获取本地端口
     *
     * @return
     */
    public abstract int getLocalPort();

    /**
     * 获取服务连接集合
     *
     * @return
     */
    public abstract Iterable<ServerCnxn> getConnections();

    /**
     * 获取活跃的（存活的）连接数量
     *
     * @return
     */
    public int getNumAliveConnections() {
        return cnxns.size();
    }

    public ZooKeeperServer getZooKeeperServer() {
        return zkServer;
    }

    final public void setZooKeeperServer(ZooKeeperServer zks) {
        this.zkServer = zks;
        if (zks != null) {
            if (secure) {
                zks.setSecureServerCnxnFactory(this);
            } else {
                zks.setServerCnxnFactory(this);
            }
        }
    }

    /**
     * 关闭会话
     *
     * @return true if the cnxn that contains the sessionId exists in this ServerCnxnFactory
     * and it's closed. Otherwise false.
     */
    public abstract boolean closeSession(long sessionId);

    /**
     * 配置
     */
    public void configure(InetSocketAddress addr, int maxcc) throws IOException {
        configure(addr, maxcc, false);
    }

    /**
     * 配置
     */
    public abstract void configure(InetSocketAddress addr, int maxcc, boolean secure)
            throws IOException;

    /**
     * 重载配置
     *
     * @param addr
     */
    public abstract void reconfigure(InetSocketAddress addr);

    /**
     * Maximum number of connections allowed from particular host (ip)
     * 获取特定主机 (ip) 允许的最大连接数
     */
    public abstract int getMaxClientCnxnsPerHost();

    /**
     * Maximum number of connections allowed from particular host (ip)
     * 设置特定主机 (ip) 允许的最大连接数
     */
    public abstract void setMaxClientCnxnsPerHost(int max);

    public boolean isSecure() {
        return secure;
    }

    /**
     * 启动
     */
    public void startup(ZooKeeperServer zkServer) throws IOException, InterruptedException {
        startup(zkServer, true);
    }

    // This method is to maintain compatiblity of startup(zks) and enable sharing of zks
    // when we add secureCnxnFactory.

    /**
     * 启动
     */
    public abstract void startup(ZooKeeperServer zkServer, boolean startServer)
            throws IOException, InterruptedException;

    /**
     * 各类线程进行join
     */
    public abstract void join() throws InterruptedException;

    /**
     * 关闭
     */
    public abstract void shutdown();

    /**
     * 启动
     */
    public abstract void start();

    /**
     * 关闭所有服务连接
     */
    public abstract void closeAll();

    /**
     * 获取本地网络地址
     */
    public abstract InetSocketAddress getLocalAddress();

    /**
     * 重置所有服务连接统计信息
     */
    public abstract void resetAllConnectionStats();


    /**
     * 获取所有服务连接信息
     * @param brief
     * @return
     */
    public abstract Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief);

    /**
     * 取消服务连接注册
     * @param serverCnxn
     */
    public void unregisterConnection(ServerCnxn serverCnxn) {
        ConnectionBean jmxConnectionBean = connectionBeans.remove(serverCnxn);
        if (jmxConnectionBean != null) {
            MBeanRegistry.getInstance().unregister(jmxConnectionBean);
        }
    }

    /**
     * 注册服务连接
     *
     * @param serverCnxn
     */
    public void registerConnection(ServerCnxn serverCnxn) {
        // zk服务不为空
        if (zkServer != null) {
            // 创建连接存储对象
            ConnectionBean jmxConnectionBean = new ConnectionBean(serverCnxn, zkServer);
            try {
                // 注册实例
                MBeanRegistry.getInstance().register(jmxConnectionBean, zkServer.jmxServerBean);
                // 添加到容器
                connectionBeans.put(serverCnxn, jmxConnectionBean);
            } catch (JMException e) {
                LOG.warn("Could not register connection", e);
            }
        }

    }

    /**
     * Initialize the server SASL if specified.
     *
     * If the user has specified a "ZooKeeperServer.LOGIN_CONTEXT_NAME_KEY"
     * or a jaas.conf using "java.security.auth.login.config"
     * the authentication is required and an exception is raised.
     * Otherwise no authentication is configured and no exception is raised.
     *
     * 配置SASL登陆信息
     * @throws IOException if jaas.conf is missing or there's an error in it.
     */
    protected void configureSaslLogin() throws IOException {
        // 从系统环境中获取zookeeper.sasl.serverconfig对应的数据，默认Server
        String serverSection = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
                ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);

        // Note that 'Configuration' here refers to javax.security.auth.login.Configuration.

        // 应用程序配置条目
        AppConfigurationEntry entries[] = null;
        SecurityException securityException = null;
        try {
            // 获取AppConfigurationEntries属性
            entries = Configuration.getConfiguration().getAppConfigurationEntry(serverSection);
        } catch (SecurityException e) {
            // handle below: might be harmless if the user doesn't intend to use JAAS authentication.
            securityException = e;
        }

        // No entries in jaas.conf
        // If there's a configuration exception fetching the jaas section and
        // the user has required sasl by specifying a LOGIN_CONTEXT_NAME_KEY or a jaas file
        // we throw an exception otherwise we continue without authentication.
        // 应用程序配置条目为空
        if (entries == null) {
            // 读取系统变量java.security.auth.login.config
            String jaasFile = System.getProperty(Environment.JAAS_CONF_KEY);
            // 读取系统变量zookeeper.sasl.serverconfig
            String loginContextName =
                    System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY);
            // 异常对象不为空
            // 系统变量zookeeper.sasl.serverconfig不为空或者java.security.auth.login.config连接不为空
            if (securityException != null && (loginContextName != null || jaasFile != null)) {
                String errorMessage =
                        "No JAAS configuration section named '" + serverSection + "' was found";
                if (jaasFile != null) {
                    errorMessage += " in '" + jaasFile + "'.";
                }
                if (loginContextName != null) {
                    errorMessage +=
                            " But " + ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY + " was set.";
                }
                LOG.error(errorMessage);
                throw new IOException(errorMessage);
            }
            // 结束处理
            return;
        }

        // jaas.conf entry available
        try {
            // 创建成员变量 saslServerCallbackHandler和login并开启login变量中的线程
            saslServerCallbackHandler =
                    new SaslServerCallbackHandler(Configuration.getConfiguration());
            login = new Login(serverSection, saslServerCallbackHandler, new ZKConfig());
            login.startThreadIfNeeded();
        } catch (LoginException e) {
            throw new IOException(
                    "Could not configure server because SASL configuration did not allow the "
                            + " ZooKeeper server to authenticate itself properly: " + e);
        }
    }
}
