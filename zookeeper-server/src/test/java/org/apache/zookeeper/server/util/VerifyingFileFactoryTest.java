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

package org.apache.zookeeper.server.util;

import org.apache.zookeeper.ZKTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class VerifyingFileFactoryTest extends ZKTestCase {

    private Logger log;

    @Before
    public void setUp() {
        log = LoggerFactory.getLogger("TODO: Mock Logging");
    }

    @Test
    public void testForWarningOnRelativePath() {
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log)
                .warnForRelativePath().build();
        vff.create("a/relative/path");
        // assertTrue(log.hasWarned);
    }

    @Test
    public void testForNoWarningOnIntendedRelativePath() {
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log)
                .warnForRelativePath().build();
        vff.create("./an/intended/relative/path");
        // assertFalse(log.hasWarned);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testForFailForNonExistingPath() {
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log)
                .failForNonExistingPath().build();
        vff.create("/I/H0p3/this/path/d035/n0t/ex15t");
    }

    @Test
    public void testFileHasCorrectPath() {
        File file = new File("/some/path");
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log).build();
        assertEquals(file, vff.create(file.getPath()));
    }
}
