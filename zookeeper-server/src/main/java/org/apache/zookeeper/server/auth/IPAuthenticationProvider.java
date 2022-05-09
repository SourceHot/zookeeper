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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ServerCnxn;

public class IPAuthenticationProvider implements AuthenticationProvider {

    public String getScheme() {
        return "ip";
    }

    public KeeperException.Code
    handleAuthentication(ServerCnxn cnxn, byte[] authData) {
        String id = cnxn.getRemoteSocketAddress().getAddress().getHostAddress();
        cnxn.addAuthInfo(new Id(getScheme(), id));
        return KeeperException.Code.OK;
    }

    // This is a bit weird but we need to return the address and the number of
    // bytes (to distinguish between IPv4 and IPv6
    private byte[] addr2Bytes(String addr) {
        byte b[] = v4addr2Bytes(addr);
        // TODO Write the v6addr2Bytes
        return b;
    }

    private byte[] v4addr2Bytes(String addr) {
        String parts[] = addr.split("\\.", -1);
        if (parts.length != 4) {
            return null;
        }
        byte b[] = new byte[4];
        for (int i = 0; i < 4; i++) {
            try {
                int v = Integer.parseInt(parts[i]);
                if (v >= 0 && v <= 255) {
                    b[i] = (byte) v;
                } else {
                    return null;
                }
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return b;
    }

    private void mask(byte b[], int bits) {
        int start = bits / 8;
        int startMask = (1 << (8 - (bits % 8))) - 1;
        // 进行非运算
        startMask = ~startMask;
        while (start < b.length) {
            b[start] &= startMask;
            startMask = 0;
            start++;
        }
    }

    public boolean matches(String id, String aclExpr) {
        String parts[] = aclExpr.split("/", 2);
        byte aclAddr[] = addr2Bytes(parts[0]);
        if (aclAddr == null) {
            return false;
        }
        int bits = aclAddr.length * 8;
        if (parts.length == 2) {
            try {
                bits = Integer.parseInt(parts[1]);
                if (bits < 0 || bits > aclAddr.length * 8) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        // 对字节数组进行加工
        mask(aclAddr, bits);
        // 将id转换为字节数组
        byte remoteAddr[] = addr2Bytes(id);
        if (remoteAddr == null) {
            return false;
        }
        // 对字节数组进行加工
        mask(remoteAddr, bits);
        // 遍历字节判断是否和acl中的数据相同，如果不相同则返回false
        for (int i = 0; i < remoteAddr.length; i++) {
            if (remoteAddr[i] != aclAddr[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean isAuthenticated() {
        return false;
    }

    public boolean isValid(String id) {
        // 按照斜杠拆分
        String parts[] = id.split("/", 2);
        // 将拆分结果的第一个元素转换为字节，第一个元素是ip
        byte aclAddr[] = addr2Bytes(parts[0]);
        // ip字节为空返回false
        if (aclAddr == null) {
            return false;
        }
        // 拆分结果长度为2
        if (parts.length == 2) {
            try {
                // 将第二个元素转换为int，第二个元素是bits
                int bits = Integer.parseInt(parts[1]);
                // port小于0或者 aclAddr.length * 8值
                if (bits < 0 || bits > aclAddr.length * 8) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }
}
