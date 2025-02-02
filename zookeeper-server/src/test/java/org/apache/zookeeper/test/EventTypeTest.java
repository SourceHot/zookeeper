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

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;

public class EventTypeTest extends ZKTestCase {

    @Test
    public void testIntConversion() {
        // Ensure that we can convert all valid integers to EventTypes
        EnumSet<EventType> allTypes = EnumSet.allOf(EventType.class);

        for (EventType et : allTypes) {
            Assert.assertEquals(et, EventType.fromInt(et.getIntValue()));
        }
    }

    @Test
    public void testInvalidIntConversion() {
        try {
            EventType.fromInt(324242);
            Assert.fail("Was able to create an invalid EventType via an integer");
        } catch (RuntimeException re) {
            // we're good.
        }

    }
}
