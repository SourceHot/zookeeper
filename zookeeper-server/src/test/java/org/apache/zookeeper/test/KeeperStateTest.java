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

import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;

public class KeeperStateTest extends ZKTestCase {

    @Test
    public void testIntConversion() {
        // Ensure that we can convert all valid integers to KeeperStates
        EnumSet<KeeperState> allStates = EnumSet.allOf(KeeperState.class);

        for (KeeperState as : allStates) {
            Assert.assertEquals(as, KeeperState.fromInt(as.getIntValue()));
        }
    }

    @Test
    public void testInvalidIntConversion() {
        try {
            KeeperState.fromInt(324142);
            Assert.fail("Was able to create an invalid KeeperState via an integer");
        } catch (RuntimeException re) {
            // we're good.
        }

    }

    /** Validate that the deprecated constant still works. There were issues
     * found with switch statements - which need compile time constants.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedCodeOkInSwitch() {
        int test = 1;
        switch (test) {
            case Code.Ok:
                Assert.assertTrue(true);
                break;
        }
    }

    /** Verify the enum works (paranoid) */
    @Test
    public void testCodeOKInSwitch() {
        Code test = Code.OK;
        switch (test) {
            case OK:
                Assert.assertTrue(true);
                break;
        }
    }
}
