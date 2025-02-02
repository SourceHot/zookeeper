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

package org.apache.zookeeper.common;

import org.apache.zookeeper.ZKTestCase;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

public class NetUtilsTest extends ZKTestCase {

    private Integer port = 1234;
    private String v4addr = "127.0.0.1";
    private String v6addr = "[0:0:0:0:0:0:0:1]";
    private String v6addr2 = "[2600:0:0:0:0:0:0:0]";
    private String v4local = v4addr + ":" + port.toString();
    private String v6local = v6addr + ":" + port.toString();
    private String v6ext = v6addr2 + ":" + port.toString();

    @Test
    public void testFormatInetAddrGoodIpv4() {
        InetSocketAddress isa = new InetSocketAddress(v4addr, port);
        assertEquals("127.0.0.1:1234", NetUtils.formatInetAddr(isa));
    }

    @Test
    public void testFormatInetAddrGoodIpv6Local() {
        // Have to use the expanded address here, hence not using v6addr in instantiation
        InetSocketAddress isa = new InetSocketAddress("::1", port);
        assertEquals(v6local, NetUtils.formatInetAddr(isa));
    }

    @Test
    public void testFormatInetAddrGoodIpv6Ext() {
        // Have to use the expanded address here, hence not using v6addr in instantiation
        InetSocketAddress isa = new InetSocketAddress("2600::", port);
        assertEquals(v6ext, NetUtils.formatInetAddr(isa));
    }

    @Test
    public void testFormatInetAddrGoodHostname() {
        InetSocketAddress isa = new InetSocketAddress("localhost", 1234);

        Assert.assertThat(NetUtils.formatInetAddr(isa),
                AnyOf.anyOf(IsEqual.equalTo(v4local), IsEqual.equalTo(v6local)
                ));
    }

    @Test
    public void testFormatAddrUnresolved() {
        InetSocketAddress isa = InetSocketAddress.createUnresolved("doesnt.exist.com", 1234);
        assertEquals("doesnt.exist.com:1234", NetUtils.formatInetAddr(isa));
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetGetIPV6HostAndPort_WhenHostDoesNotEndWithBracket() {
        NetUtils.getIPV6HostAndPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334:443");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetGetIPV6HostAndPort_WhenNoPortAfterColon() {
        NetUtils.getIPV6HostAndPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetGetIPV6HostAndPort_WhenPortIsNotSeparatedProperly() {
        NetUtils.getIPV6HostAndPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]2181");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetGetIPV6HostAndPort_WhenHostIsEmpty() {
        NetUtils.getIPV6HostAndPort("[]:2181");
    }

    @Test
    public void tetGetIPV6HostAndPort_EmptyStringArrayIfDoesNotStartWithBracket() {
        String[] ipv6HostAndPort =
                NetUtils.getIPV6HostAndPort("2001:0db8:85a3:0000:0000:8a2e:0370:7334]");
        assertEquals(0, ipv6HostAndPort.length);
    }

    @Test
    public void tetGetIPV6HostAndPort_ReturnHostPort() {
        String[] ipv6HostAndPort =
                NetUtils.getIPV6HostAndPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:2181");
        assertEquals(2, ipv6HostAndPort.length);
        assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", ipv6HostAndPort[0]);
        assertEquals("2181", ipv6HostAndPort[1]);
    }

    @Test
    public void tetGetIPV6HostAndPort_ReturnHostPortPort() {
        String[] ipv6HostAndPort =
                NetUtils.getIPV6HostAndPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:2181:3181");
        assertEquals(2, ipv6HostAndPort.length);
        assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", ipv6HostAndPort[0]);
        assertEquals("2181:3181", ipv6HostAndPort[1]);
    }
}