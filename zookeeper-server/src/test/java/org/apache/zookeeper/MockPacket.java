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

import org.apache.jute.Record;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

import java.nio.ByteBuffer;

public class MockPacket extends ClientCnxn.Packet {

    public MockPacket(RequestHeader requestHeader, ReplyHeader replyHeader,
                      Record request, Record response,
                      WatchRegistration watchRegistration) {
        super(requestHeader, replyHeader, request, response, watchRegistration);
    }

    public MockPacket(RequestHeader requestHeader, ReplyHeader replyHeader,
                      Record request, Record response,
                      WatchRegistration watchRegistration, boolean readOnly) {
        super(requestHeader, replyHeader, request, response, watchRegistration, readOnly);
    }

    public ByteBuffer createAndReturnBB() {
        createBB();
        return this.bb;
    }

}
