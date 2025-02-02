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

import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class QuorumPeerConfigTest {

    /**
     * test case for https://issues.apache.org/jira/browse/ZOOKEEPER-2264
     */
    @Test
    public void testErrorMessageWhensecureClientPortNotSetButsecureClientPortAddressSet()
            throws IOException, ConfigException {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        try {
            Properties zkProp = getDefaultZKProperties();
            zkProp.setProperty("secureClientPortAddress", "localhost");
            quorumPeerConfig.parseProperties(zkProp);
            fail("IllegalArgumentException is expected");
        } catch (IllegalArgumentException e) {
            String expectedMessage =
                    "secureClientPortAddress is set but secureClientPort is not set";
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    /**
     *
     * Test case for https://issues.apache.org/jira/browse/ZOOKEEPER-2264
     */
    @Test
    public void testErrorMessageWhenclientPortNotSetButclientPortAddressSet()
            throws IOException, ConfigException {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        try {
            Properties zkProp = getDefaultZKProperties();
            zkProp.setProperty("clientPortAddress", "localhost");
            quorumPeerConfig.parseProperties(zkProp);
            fail("IllegalArgumentException is expected");
        } catch (IllegalArgumentException e) {
            String expectedMessage = "clientPortAddress is set but clientPort is not set";
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    /**
     * https://issues.apache.org/jira/browse/ZOOKEEPER-2297
     */
    @Test
    public void testConfigureSSLAuthGetsConfiguredIfSecurePortConfigured()
            throws IOException, ConfigException {
        String sslAuthProp = "zookeeper.authProvider.x509";
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        Properties zkProp = getDefaultZKProperties();
        zkProp.setProperty("secureClientPort", "12345");
        quorumPeerConfig.parseProperties(zkProp);
        String expected = "org.apache.zookeeper.server.auth.X509AuthenticationProvider";
        String result = System.getProperty(sslAuthProp);
        assertEquals(expected, result);
    }

    /**
     * https://issues.apache.org/jira/browse/ZOOKEEPER-2297
     */
    @Test
    public void testCustomSSLAuth() throws IOException {
        try (ClientX509Util x509Util = new ClientX509Util()) {
            System.setProperty(x509Util.getSslAuthProviderProperty(), "y509");
            QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
            try {
                Properties zkProp = getDefaultZKProperties();
                zkProp.setProperty("secureClientPort", "12345");
                quorumPeerConfig.parseProperties(zkProp);
                fail("ConfigException is expected");
            } catch (ConfigException e) {
                assertNotNull(e.getMessage());
            }
        }
    }

    /**
     * Test case for https://issues.apache.org/jira/browse/ZOOKEEPER-2873
     */
    @Test(expected = ConfigException.class)
    public void testSamePortConfiguredForClientAndElection() throws IOException, ConfigException {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        Properties zkProp = getDefaultZKProperties();
        zkProp.setProperty("server.1", "localhost:2888:2888");
        quorumPeerConfig.parseProperties(zkProp);
    }

    private Properties getDefaultZKProperties() {
        Properties zkProp = new Properties();
        zkProp.setProperty("dataDir", new File("myDataDir").getAbsolutePath());
        return zkProp;
    }

}
