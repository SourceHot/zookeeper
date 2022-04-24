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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ReplyHeader;

import java.io.IOException;

/**
 * Manages the unknown requests (i.e. unknown OpCode), by:
 * - sending back the KeeperException.UnimplementedException() error code to the client
 * - closing the connection.
 */
public class UnimplementedRequestProcessor implements RequestProcessor {

    public void processRequest(Request request) throws RequestProcessorException {
        // 创建异常对象
        KeeperException ke = new KeeperException.UnimplementedException();
        // 向请求中写入异常信息
        request.setException(ke);
        // 组装响应
        ReplyHeader rh = new ReplyHeader(request.cxid, request.zxid,
                ke.code().intValue());
        try {
            // 发送响应
            request.cnxn.sendResponse(rh, null, "response");
        }
        catch (IOException e) {
            throw new RequestProcessorException("Can't send the response", e);
        }
        // 发送关闭session请求
        request.cnxn.sendCloseSession();
    }

    public void shutdown() {
    }
}
