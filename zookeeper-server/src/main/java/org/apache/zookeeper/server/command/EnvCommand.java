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

package org.apache.zookeeper.server.command;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.server.ServerCnxn;

import java.io.PrintWriter;
import java.util.List;

public class EnvCommand extends AbstractFourLetterCommand {
    EnvCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        List<Environment.Entry> env = Environment.list();

        pw.println("Environment:");
        for (Environment.Entry e : env) {
            pw.print(e.getKey());
            pw.print("=");
            pw.println(e.getValue());
        }
    }
}
