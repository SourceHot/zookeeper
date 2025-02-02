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

import org.apache.zookeeper.server.quorum.LearnerHandler;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates and centralizes tracing for the ZooKeeper server.
 * Trace messages go to the log with TRACE level.
 * <p>
 * Log4j must be correctly configured to capture the TRACE messages.
 *
 * 跟踪类型
 */
public class ZooTrace {
    final static public long CLIENT_REQUEST_TRACE_MASK = 1 << 1;

    final static public long CLIENT_DATA_PACKET_TRACE_MASK = 1 << 2;

    final static public long CLIENT_PING_TRACE_MASK = 1 << 3;

    final static public long SERVER_PACKET_TRACE_MASK = 1 << 4;

    final static public long SESSION_TRACE_MASK = 1 << 5;

    final static public long EVENT_DELIVERY_TRACE_MASK = 1 << 6;

    final static public long SERVER_PING_TRACE_MASK = 1 << 7;

    final static public long WARNING_TRACE_MASK = 1 << 8;

    final static public long JMX_TRACE_MASK = 1 << 9;

    private static long traceMask = CLIENT_REQUEST_TRACE_MASK
            | SERVER_PACKET_TRACE_MASK | SESSION_TRACE_MASK
            | WARNING_TRACE_MASK;

    public static synchronized long getTextTraceLevel() {
        return traceMask;
    }

    public static synchronized void setTextTraceLevel(long mask) {
        traceMask = mask;
        final Logger LOG = LoggerFactory.getLogger(ZooTrace.class);
        LOG.info("Set text trace mask to 0x" + Long.toHexString(mask));
    }

    public static synchronized boolean isTraceEnabled(Logger log, long mask) {
        return log.isTraceEnabled() && (mask & traceMask) != 0;
    }

    public static void logTraceMessage(Logger log, long mask, String msg) {
        if (isTraceEnabled(log, mask)) {
            log.trace(msg);
        }
    }

    static public void logQuorumPacket(Logger log, long mask,
                                       char direction, QuorumPacket qp) {
        if (isTraceEnabled(log, mask)) {
            logTraceMessage(log, mask, direction +
                    " " + LearnerHandler.packetToString(qp));
        }
    }

    static public void logRequest(Logger log, long mask,
                                  char rp, Request request, String header) {
        if (isTraceEnabled(log, mask)) {
            log.trace(header + ":" + rp + request.toString());
        }
    }
}
