/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;


import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;



public class ZKConfigTest {

    @Rule
    public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);
    X509Util x509Util = new ClientX509Util();

    @After
    public void tearDown() throws Exception {
        System.clearProperty(x509Util.getSslProtocolProperty());
    }

    // property is not set we should get the default value
    @Test
    public void testBooleanRetrievalFromPropertyDefault() {
        ZKConfig conf = new ZKConfig();
        String prop = "UnSetProperty" + System.currentTimeMillis();
        boolean defaultValue = false;
        boolean result = conf.getBoolean(prop, defaultValue);
        assertEquals(defaultValue, result);
    }

    // property is set to an valid boolean, we should get the set value
    @Test
    public void testBooleanRetrievalFromProperty() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), Boolean.toString(value));
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }

    // property is set but with white spaces in the beginning
    @Test
    public void testBooleanRetrievalFromPropertyWithWhitespacesInBeginning() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), " " + value);
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }

    // property is set but with white spaces at the end
    @Test
    public void testBooleanRetrievalFromPropertyWithWhitespacesAtEnd() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), value + " ");
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }

    // property is set but with white spaces at the beginning and the end
    @Test
    public void testBooleanRetrievalFromPropertyWithWhitespacesAtBeginningAndEnd() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), " " + value + " ");
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }
}
