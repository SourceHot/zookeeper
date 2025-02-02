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

package org.apache.zookeeper.server.auth;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class ProviderRegistry {
    public static final String AUTHPROVIDER_PROPERTY_PREFIX = "zookeeper.authProvider.";
    private static final Logger LOG = LoggerFactory.getLogger(ProviderRegistry.class);
    private static final Map<String, AuthenticationProvider> authenticationProviders =
            new HashMap<>();
    private static boolean initialized = false;

    public static void initialize() {

        synchronized (ProviderRegistry.class) {
            // 如果已经实例化则跳过
            if (initialized) {
                return;
            }
            // 创建 ip 和 digest 的认证模式加入到容器中
            IPAuthenticationProvider ipp = new IPAuthenticationProvider();
            DigestAuthenticationProvider digp = new DigestAuthenticationProvider();
            authenticationProviders.put(ipp.getScheme(), ipp);
            authenticationProviders.put(digp.getScheme(), digp);
            // 获取系统变量
            Enumeration<Object> en = System.getProperties().keys();
            while (en.hasMoreElements()) {

                String k = (String) en.nextElement();
                // 如果系统变量是以zookeeper.authProvider.开头
                if (k.startsWith(AUTHPROVIDER_PROPERTY_PREFIX)) {
                    // 获取属性值
                    String className = System.getProperty(k);
                    try {
                        // 反射创建类对象
                        Class<?> c = ZooKeeperServer.class.getClassLoader()
                                .loadClass(className);
                        AuthenticationProvider ap =
                                (AuthenticationProvider) c.getDeclaredConstructor()
                                        .newInstance();
                        // 加入到容器
                        authenticationProviders.put(ap.getScheme(), ap);
                    } catch (Exception e) {
                        LOG.warn("Problems loading " + className, e);
                    }
                }
            }
            // 序列化完毕
            initialized = true;
        }
    }

    public static AuthenticationProvider getProvider(String scheme) {
        if (!initialized) {
            initialize();
        }
        return authenticationProviders.get(scheme);
    }

    public static String listProviders() {
        StringBuilder sb = new StringBuilder();
        for (String s : authenticationProviders.keySet()) {
            sb.append(s + " ");
        }
        return sb.toString();
    }
}
