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

package org.apache.zookeeper.server.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FilePadding {
    private static final Logger LOG;
    private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);
    private static long preAllocSize = 65536 * 1024;

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        String size = System.getProperty("zookeeper.preAllocSize");
        if (size != null) {
            try {
                preAllocSize = Long.parseLong(size) * 1024;
            } catch (NumberFormatException e) {
                LOG.warn(size + " is not a valid value for preAllocSize");
            }
        }
    }

    private long currentSize;

    /**
     * Getter of preAllocSize has been added for testing
     */
    public static long getPreAllocSize() {
        return preAllocSize;
    }

    /**
     * method to allow setting preallocate size
     * of log file to pad the file.
     *
     * @param size the size to set to in bytes
     */
    public static void setPreallocSize(long size) {
        preAllocSize = size;
    }

    /**
     * Calculates a new file size with padding. We only return a new size if
     * the current file position is sufficiently close (less than 4K) to end of
     * file and preAllocSize is > 0.
     *
     * @param position     the point in the file we have written to
     *                     当前位置
     * @param fileSize     application keeps track of the current file size
     *                     当前文件大小
     * @param preAllocSize how many bytes to pad
     *                     需要填充的大小
     * @return the new file size. It can be the same as fileSize if no
     * padding was done.
     * @throws IOException
     */
    // VisibleForTesting
    public static long calculateFileSizeWithPadding(long position,
                                                    long fileSize,
                                                    long preAllocSize) {
        // If preAllocSize is positive and we are within 4KB of the known end of the file calculate a new file size
        // 1. 需要填充的数据大小大于0
        // 2. 当前位置+4096后大于等于当前文件大小
        if (preAllocSize > 0 && position + 4096 >= fileSize) {
            // If we have written more than we have previously preallocated we need to make sure the new
            // file size is larger than what we already have

            // 如果当前位置大于文件大小
            if (position > fileSize) {
                // 文件大小重新计算：当前位置+需要填充的大小
                fileSize = position + preAllocSize;
                // 文件大小重新计算： 文件大小-（文件大小取模需要填充的大小)
                fileSize = fileSize - (fileSize % preAllocSize);
            } else {
                // 文件大小重新计算： 文件大小+需要填充的大小
                fileSize = fileSize + preAllocSize;
            }
        }

        return fileSize;
    }

    public void setCurrentSize(long currentSize) {
        this.currentSize = currentSize;
    }

    /**
     * pad the current file to increase its size to the next multiple of preAllocSize greater than the current size and position
     *
     * @param fileChannel the fileChannel of the file to be padded
     * @throws IOException
     */
    long padFile(FileChannel fileChannel) throws IOException {
        // 计算新的文件大小
        long newFileSize =
                calculateFileSizeWithPadding(fileChannel.position(), currentSize, preAllocSize);
        // 当前文件大小不等于新的文件大小需要填充0
        if (currentSize != newFileSize) {
            fileChannel.write((ByteBuffer) fill.position(0), newFileSize - fill.remaining());
            // 将当前文件大小设置为新的文件大小
            currentSize = newFileSize;
        }
        return currentSize;
    }
}
