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

import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SaslServerCallbackHandler implements CallbackHandler {
    private static final String USER_PREFIX = "user_";
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerCallbackHandler.class);
    private static final String SYSPROP_SUPER_PASSWORD =
            "zookeeper.SASLAuthenticationProvider.superPassword";
    private static final String SYSPROP_REMOVE_HOST = "zookeeper.kerberos.removeHostFromPrincipal";
    private static final String SYSPROP_REMOVE_REALM =
            "zookeeper.kerberos.removeRealmFromPrincipal";
    /**
     * 证书容器
     * key:用户名
     * value:证书
     */
    private final Map<String, String> credentials = new HashMap<String, String>();
    private String userName;

    public SaslServerCallbackHandler(Configuration configuration)
            throws IOException {
        // 读取系统变量zookeeper.sasl.serverconfig，默认值Server
        String serverSection = System.getProperty(
                ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
                ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);
        // 获取serverSection对应的AppConfigurationEntry集合
        AppConfigurationEntry[] configurationEntries =
                configuration.getAppConfigurationEntry(serverSection);

        // 如果AppConfigurationEntry集合为空抛出异常
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + serverSection
                    + "' entry in this configuration: Server cannot start.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
        // 证书容器进行清理操作
        credentials.clear();
        // 循环AppConfigurationEntry集合
        for (AppConfigurationEntry entry : configurationEntries) {
            Map<String, ?> options = entry.getOptions();
            // Populate DIGEST-MD5 user -> password map with JAAS configuration entries from the "Server" section.
            // Usernames are distinguished from other options by prefixing the username with a "user_" prefix.
            for (Map.Entry<String, ?> pair : options.entrySet()) {
                String key = pair.getKey();
                // 数据信息是以user_开头将信息放入到证书容器中
                if (key.startsWith(USER_PREFIX)) {
                    String userName = key.substring(USER_PREFIX.length());
                    credentials.put(userName, (String) pair.getValue());
                }
            }
        }
    }

    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                handleNameCallback((NameCallback) callback);
            } else if (callback instanceof PasswordCallback) {
                handlePasswordCallback((PasswordCallback) callback);
            } else if (callback instanceof RealmCallback) {
                handleRealmCallback((RealmCallback) callback);
            } else if (callback instanceof AuthorizeCallback) {
                handleAuthorizeCallback((AuthorizeCallback) callback);
            }
        }
    }

    private void handleNameCallback(NameCallback nc) {
        // check to see if this user is in the user password database.
        if (credentials.get(nc.getDefaultName()) == null) {
            LOG.warn("User '" + nc.getDefaultName()
                    + "' not found in list of DIGEST-MD5 authenticateable users.");
            return;
        }
        nc.setName(nc.getDefaultName());
        userName = nc.getDefaultName();
    }

    private void handlePasswordCallback(PasswordCallback pc) {
        if ("super".equals(this.userName) && System.getProperty(SYSPROP_SUPER_PASSWORD) != null) {
            // superuser: use Java system property for password, if available.
            pc.setPassword(System.getProperty(SYSPROP_SUPER_PASSWORD).toCharArray());
        } else if (credentials.containsKey(userName)) {
            pc.setPassword(credentials.get(userName).toCharArray());
        } else {
            LOG.warn("No password found for user: " + userName);
        }
    }

    private void handleRealmCallback(RealmCallback rc) {
        LOG.debug("client supplied realm: " + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        // 获取身份验证id
        String authenticationID = ac.getAuthenticationID();
        // 获取授权id
        String authorizationID = ac.getAuthorizationID();

        LOG.info("Successfully authenticated client: authenticationID=" + authenticationID
                + ";  authorizationID=" + authorizationID + ".");
        // 是否允许授权，设置为允许
        ac.setAuthorized(true);

        // canonicalize authorization id according to system properties:
        // zookeeper.kerberos.removeRealmFromPrincipal(={true,false})
        // zookeeper.kerberos.removeHostFromPrincipal(={true,false})
        // 创建KerberosName对象
        KerberosName kerberosName = new KerberosName(authenticationID);
        try {
            // 用户名构造，初始化值：短名字
            StringBuilder userNameBuilder = new StringBuilder(kerberosName.getShortName());
            // 是否需要追加host，如果需要则追加
            if (shouldAppendHost(kerberosName)) {
                userNameBuilder.append("/").append(kerberosName.getHostName());
            }
            // 是否需要追加域（realm）
            if (shouldAppendRealm(kerberosName)) {
                userNameBuilder.append("@").append(kerberosName.getRealm());
            }
            LOG.info("Setting authorizedID: " + userNameBuilder);
            // 设置授权id
            ac.setAuthorizedID(userNameBuilder.toString());
        } catch (IOException e) {
            LOG.error("Failed to set name based on Kerberos authentication rules.", e);
        }
    }

    // 是否需要追加域
    private boolean shouldAppendRealm(KerberosName kerberosName) {
        return !isSystemPropertyTrue(SYSPROP_REMOVE_REALM) && kerberosName.getRealm() != null;
    }

    // 是否需要追加host
    private boolean shouldAppendHost(KerberosName kerberosName) {
        return !isSystemPropertyTrue(SYSPROP_REMOVE_HOST) && kerberosName.getHostName() != null;
    }

    private boolean isSystemPropertyTrue(String propertyName) {
        return "true".equals(System.getProperty(propertyName));
    }
}
