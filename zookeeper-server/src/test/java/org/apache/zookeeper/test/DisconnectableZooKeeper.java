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

package org.apache.zookeeper.test;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class DisconnectableZooKeeper extends ZooKeeper {
    public DisconnectableZooKeeper(String host, int sessionTimeout, Watcher watcher)
            throws IOException {
        super(host, sessionTimeout, watcher);
    }

    public DisconnectableZooKeeper(String host, int sessionTimeout, Watcher watcher,
                                   long sessionId, byte[] sessionPasswd)
            throws IOException {
        super(host, sessionTimeout, watcher, sessionId, sessionPasswd);
    }

    /** Testing only!!! Really!!!! This is only here to test when the client
     * disconnects from the server w/o sending a session disconnect (ie
     * ending the session cleanly). The server will eventually notice the
     * client is no longer pinging and will timeout the session.
     */
    public void disconnect() throws IOException {
        cnxn.disconnect();
    }

    /**
     * Prevent the client from automatically reconnecting if the connection to the
     * server is lost
     */
    public void dontReconnect() throws Exception {
        java.lang.reflect.Field f = cnxn.getClass().getDeclaredField("closing");
        f.setAccessible(true);
        f.setBoolean(cnxn, true);
    }
}
