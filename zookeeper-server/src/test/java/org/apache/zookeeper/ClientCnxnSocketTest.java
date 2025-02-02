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

import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.test.TestByteBufAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientCnxnSocketTest {
    @Before
    public void setUp() {
        ClientCnxnSocketNetty.setTestAllocator(TestByteBufAllocator.getInstance());
    }

    @After
    public void tearDown() {
        ClientCnxnSocketNetty.clearTestAllocator();
        TestByteBufAllocator.checkForLeaks();
    }

    @Test
    public void testWhenInvalidJuteMaxBufferIsConfiguredIOExceptionIsThrown() {
        ZKClientConfig clientConfig = new ZKClientConfig();
        String value = "SomeInvalidInt";
        clientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER, value);
        // verify ClientCnxnSocketNIO creation
        try {
            new ClientCnxnSocketNIO(clientConfig);
            fail("IOException is expected.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains(value));
        }
        // verify ClientCnxnSocketNetty creation
        try {
            new ClientCnxnSocketNetty(clientConfig);
            fail("IOException is expected.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains(value));
        }

    }

}
