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

package org.apache.zookeeper.client;

import org.apache.zookeeper.common.NetUtils;
import org.apache.zookeeper.common.PathUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.common.StringUtils.split;

/**
 * A parser for ZooKeeper Client connect strings.
 *
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 *
 * The chrootPath member should be replaced by a Path object in issue
 * ZOOKEEPER-849.
 *
 * @see org.apache.zookeeper.ZooKeeper
 */
public final class ConnectStringParser {
    private static final int DEFAULT_PORT = 2181;

    private final String chrootPath;

    private final ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

    /**
     * 连接字符串解析器
     * @throws IllegalArgumentException
     *             for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {
        // parse out chroot, if any
        // 确认/的索引位置
        int off = connectString.indexOf('/');
        // 如果斜杠索引存在就将其解析成为成员变量chrootPath的数据，不存在将成员变量chrootPath设置为null
        if (off >= 0) {
            String chrootPath = connectString.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            connectString = connectString.substring(0, off);
        }
        else {
            this.chrootPath = null;
        }

        // 将连接字符串按照逗号拆分
        List<String> hostsList = split(connectString, ",");
        for (String host : hostsList) {
            // 设置端口为默认端口
            int port = DEFAULT_PORT;
            // 拆分host，拆分后结果：host,port
            String[] hostAndPort = NetUtils.getIPV6HostAndPort(host);
            if (hostAndPort.length != 0) {
                host = hostAndPort[0];
                if (hostAndPort.length == 2) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            }
            // 拆分后长度为0
            else {
                // 按照冒号拆分
                int pidx = host.lastIndexOf(':');
                if (pidx >= 0) {
                    // otherwise : is at the end of the string, ignore
                    if (pidx < host.length() - 1) {
                        port = Integer.parseInt(host.substring(pidx + 1));
                    }
                    host = host.substring(0, pidx);
                }
            }
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
    }

    public String getChrootPath() {
        return chrootPath;
    }

    public ArrayList<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }
}
