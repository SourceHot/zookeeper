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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.*;

/**
 * this class is used to clean up the 
 * snapshot and data log dir's. This is usually
 * run as a cronjob on the zookeeper server machine.
 * Invocation of this class will clean up the datalogdir
 * files and snapdir files keeping the last "-n" snapshot files
 * and the corresponding logs.
 */
@InterfaceAudience.Public
public class PurgeTxnLog {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeTxnLog.class);

    private static final String COUNT_ERR_MSG = "count should be greater than or equal to 3";
    private static final String PREFIX_SNAPSHOT = "snapshot";
    private static final String PREFIX_LOG = "log";

    static void printUsage() {
        System.out.println("Usage:");
        System.out.println("PurgeTxnLog dataLogDir [snapDir] -n count");
        System.out.println("\tdataLogDir -- path to the txn log directory");
        System.out.println("\tsnapDir -- path to the snapshot directory");
        System.out.println("\tcount -- the number of old snaps/logs you want " +
                "to keep, value should be greater than or equal to 3");
    }

    /**
     * Purges the snapshot and logs keeping the last num snapshots and the
     * corresponding logs. If logs are rolling or a new snapshot is created
     * during this process, these newest N snapshots or any data logs will be
     * excluded from current purging cycle.
     *
     * @param dataDir the dir that has the logs
     *                数据目录
     * @param snapDir the dir that has the snapshots
     *                快照目录
     * @param num the number of snapshots to keep
     *            需要保留的快照数量
     *
     * @throws IOException
     */
    public static void purge(File dataDir, File snapDir, int num) throws IOException {
        // 快照保留数量小于3抛出异常
        if (num < 3) {
            throw new IllegalArgumentException(COUNT_ERR_MSG);
        }

        // 创建FileTxnSnapLog对象
        FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir, snapDir);

        // 从FileTxnSnapLog对象中获取一定数量的文件
        List<File> snaps = txnLog.findNRecentSnapshots(num);
        // 获取寻找的文件数量
        int numSnaps = snaps.size();
        // 如果文件数量大于0
        if (numSnaps > 0) {
            // 清理快照
            purgeOlderSnapshots(txnLog, snaps.get(numSnaps - 1));
        }
    }

    // VisibleForTesting
    static void purgeOlderSnapshots(FileTxnSnapLog txnLog, File snapShot) {
        // 从快照文件的名字中提取zxid
        final long leastZxidToBeRetain = Util.getZxidFromName(
                snapShot.getName(), PREFIX_SNAPSHOT);

        /**
         * We delete all files with a zxid in their name that is less than leastZxidToBeRetain.
         * This rule applies to both snapshot files as well as log files, with the following
         * exception for log files.
         *
         * A log file with zxid less than X may contain transactions with zxid larger than X.  More
         * precisely, a log file named log.(X-a) may contain transactions newer than snapshot.X if
         * there are no other log files with starting zxid in the interval (X-a, X].  Assuming the
         * latter condition is true, log.(X-a) must be retained to ensure that snapshot.X is
         * recoverable.  In fact, this log file may very well extend beyond snapshot.X to newer
         * snapshot files if these newer snapshots were not accompanied by log rollover (possible in
         * the learner state machine at the time of this writing).  We can make more precise
         * determination of whether log.(leastZxidToBeRetain-a) for the smallest 'a' is actually
         * needed or not (e.g. not needed if there's a log file named log.(leastZxidToBeRetain+1)),
         * but the complexity quickly adds up with gains only in uncommon scenarios.  It's safe and
         * simple to just preserve log.(leastZxidToBeRetain-a) for the smallest 'a' to ensure
         * recoverability of all snapshots being retained.  We determine that log file here by
         * calling txnLog.getSnapshotLogs().
         */
        // 在参数txnLog提取zxid在leastZxidToBeRetain之前的数据
        final Set<File> retainedTxnLogs = new HashSet<File>();
        retainedTxnLogs.addAll(Arrays.asList(txnLog.getSnapshotLogs(leastZxidToBeRetain)));

        /**
         * Finds all candidates for deletion, which are files with a zxid in their name that is less
         * than leastZxidToBeRetain.  There's an exception to this rule, as noted above.
         */
        // 文件过滤器
        class MyFileFilter implements FileFilter {
            private final String prefix;

            MyFileFilter(String prefix) {
                this.prefix = prefix;
            }

            public boolean accept(File f) {
                if (!f.getName().startsWith(prefix + ".")) {
                    return false;
                }
                if (retainedTxnLogs.contains(f)) {
                    return false;
                }
                // 当前文件的zxid
                long fZxid = Util.getZxidFromName(f.getName(), prefix);
                // 文件的zxid大于等于最小值不需要删除
                if (fZxid >= leastZxidToBeRetain) {
                    return false;
                }
                return true;
            }
        }
        // add all non-excluded log files
        // 过滤数据文件
        File[] logs = txnLog.getDataDir().listFiles(new MyFileFilter(PREFIX_LOG));
        List<File> files = new ArrayList<>();
        if (logs != null) {
            files.addAll(Arrays.asList(logs));
        }

        // add all non-excluded snapshot files to the deletion list
        // 过滤快照文件
        File[] snapshots = txnLog.getSnapDir().listFiles(new MyFileFilter(PREFIX_SNAPSHOT));
        if (snapshots != null) {
            files.addAll(Arrays.asList(snapshots));
        }

        // remove the old files
        // 删除历史文件
        for (File f : files) {
            final String msg = "Removing file: " +
                    DateFormat.getDateTimeInstance().format(f.lastModified()) +
                    "\t" + f.getPath();
            LOG.info(msg);
            System.out.println(msg);
            if (!f.delete()) {
                System.err.println("Failed to remove " + f.getPath());
            }
        }

    }

    /**
     * @param args dataLogDir [snapDir] -n count
     * dataLogDir -- path to the txn log directory
     * snapDir -- path to the snapshot directory
     * count -- the number of old snaps/logs you want to keep, value should be greater than or equal to 3<br>
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            printUsageThenExit();
        }
        File dataDir = validateAndGetFile(args[0]);
        File snapDir = dataDir;
        int num = -1;
        String countOption = "";
        if (args.length == 3) {
            countOption = args[1];
            num = validateAndGetCount(args[2]);
        } else {
            snapDir = validateAndGetFile(args[1]);
            countOption = args[2];
            num = validateAndGetCount(args[3]);
        }
        if (!"-n".equals(countOption)) {
            printUsageThenExit();
        }
        purge(dataDir, snapDir, num);
    }

    /**
     * validates file existence and returns the file
     *
     * @param path
     * @return File
     */
    private static File validateAndGetFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            System.err.println("Path '" + file.getAbsolutePath()
                    + "' does not exist. ");
            printUsageThenExit();
        }
        return file;
    }

    /**
     * Returns integer if parsed successfully and it is valid otherwise prints
     * error and usage and then exits
     *
     * @param number
     * @return count
     */
    private static int validateAndGetCount(String number) {
        int result = 0;
        try {
            result = Integer.parseInt(number);
            if (result < 3) {
                System.err.println(COUNT_ERR_MSG);
                printUsageThenExit();
            }
        } catch (NumberFormatException e) {
            System.err
                    .println("'" + number + "' can not be parsed to integer.");
            printUsageThenExit();
        }
        return result;
    }

    private static void printUsageThenExit() {
        printUsage();
        System.exit(1);
    }
}
