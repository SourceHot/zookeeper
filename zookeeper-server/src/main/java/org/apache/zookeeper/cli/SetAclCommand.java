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
package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * setAcl command for cli.
 * Available options are s for printing znode's stats, v for set version of znode(s), R for
 * recursive setting. User can combine v and R options together, but not s and R considering the
 * number of znodes could be large.
 */
public class SetAclCommand extends CliCommand {

    private static Options options = new Options();

    static {
        options.addOption("s", false, "stats");
        options.addOption("v", true, "version");
        options.addOption("R", false, "recursive");
    }

    private String[] args;
    private CommandLine cl;

    public SetAclCommand() {
        super("setAcl", "[-s] [-v version] [-R] path acl");
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        Parser parser = new PosixParser();
        try {
            cl = parser.parse(options, cmdArgs);
        } catch (ParseException ex) {
            throw new CliParseException(ex);
        }
        args = cl.getArgs();
        if (args.length < 3) {
            throw new CliParseException(getUsageStr());
        }

        return this;
    }

    @Override
    public boolean exec() throws CliException {
        String path = args[1];
        String aclStr = args[2];
        List<ACL> acl = AclParser.parse(aclStr);
        int version;
        if (cl.hasOption("v")) {
            version = Integer.parseInt(cl.getOptionValue("v"));
        } else {
            version = -1;
        }
        try {
            if (cl.hasOption("R")) {
                ZKUtil.visitSubTreeDFS(zk, path, false, new StringCallback() {
                    @Override
                    public void processResult(int rc, String p, Object ctx, String name) {
                        try {
                            zk.setACL(p, acl, version);
                        } catch (KeeperException | InterruptedException e) {
                            out.print(e.getMessage());
                        }
                    }
                });
            } else {
                Stat stat = zk.setACL(path, acl, version);
                if (cl.hasOption("s")) {
                    new StatPrinter(out).print(stat);
                }
            }
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException | InterruptedException ex) {
            throw new CliWrapperException(ex);
        }

        return false;

    }
}
