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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {
    public final static int SNAP_MAGIC = ByteBuffer.wrap("ZKSN".getBytes()).getInt();
    public static final String SNAPSHOT_FILE_PREFIX = "snapshot";
    private static final int VERSION = 2;
    private static final long dbId = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    File snapDir;
    private volatile boolean close = false;

    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     *
     * @return the zxid of the snapshot
     */
    public long deserialize(DataTree dt, Map<Long, Integer> sessions)
            throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        // 查询n个快照文件
        List<File> snapList = findNValidSnapshots(100);
        // 如果快照文件数量为0放回-1
        if (snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        // 是否寻找成功
        boolean foundValid = false;
        // 循环快照文件集合
        for (int i = 0, snapListSize = snapList.size(); i < snapListSize; i++) {
            // 获取当前快照
            snap = snapList.get(i);
            LOG.info("Reading snapshot " + snap);
            // 打开快照输入流和校验流
            try (InputStream snapIS = new BufferedInputStream(new FileInputStream(snap));
                    CheckedInputStream crcIn = new CheckedInputStream(snapIS, new Adler32())) {
                // 将校验流转换为输入档案
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                // 从输入存档反序列化数据树
                deserialize(dt, sessions, ia);
                // 提取校验流中的校验和
                long checkSum = crcIn.getChecksum().getValue();
                // 从输入档案中获取val数据
                long val = ia.readLong("val");
                // 如果val数据值和校验和不相同将会抛出异常
                if (val != checkSum) {
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                // 是否寻找成功设置为真
                foundValid = true;
                // 结束循环
                break;
            } catch (IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            }
        }
        // 如果是否寻找成功标记为假抛出异常
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        // 通过快照文件计算zxid将其赋值给数据树的lastProcessedZxid变量
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), SNAPSHOT_FILE_PREFIX);
        // 返回zxid
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     * <p>
     * 从输入存档反序列化数据树
     *
     * @param dt       the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia       the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions,
                            InputArchive ia) throws IOException {
        // 创建文件头信息
        FileHeader header = new FileHeader();
        // 将输入档案中的信息反序列化到文件头信息中
        header.deserialize(ia, "fileheader");
        // 如果头信息中的magic属性不是SNAP_MAGIC变量将抛出异常
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers "
                    + header.getMagic() +
                    " !=  " + FileSnap.SNAP_MAGIC);
        }
        // 将输入档案中的信息反序列化到数据树和会话信息中
        SerializeUtils.deserializeSnapshot(dt, ia, sessions);
    }

    /**
     * find the most recent snapshot in the database.
     *
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }

    /**
     * find the last (maybe) valid n snapshots. this does some
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent
     * will be first on the list.
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    private List<File> findNValidSnapshots(int n) throws IOException {
        // 在快照目录下搜索所有的快照文件并将其根据zxid排序
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        //
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                // 校验是否是有效快照
                if (Util.isValidSnapshot(f)) {
                    // 加入到文件集合中
                    list.add(f);
                    // 计数器累加1
                    count++;
                    // 如果计数器累加后和需要的文件数量相同则跳出循环
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            if (count == n) {
                break;
            }
            if (Util.getZxidFromName(f.getName(), SNAPSHOT_FILE_PREFIX) != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa       the output archive to serialize into
     * @param header   the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt, Map<Long, Integer> sessions,
                             OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if (header == null) {
            throw new IllegalStateException(
                    "Snapshot's not open for writing: uninitialized header");
        }
        header.serialize(oa, "fileheader");
        SerializeUtils.serializeSnapshot(dt, oa, sessions);
    }

    /**
     * serialize the datatree and session into the file snapshot
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot)
            throws IOException {
        // 判断是否关闭
        if (!close) {
            // 打开输出流和校验输出流
            try (OutputStream sessOS = new BufferedOutputStream(new FileOutputStream(snapShot));
                    CheckedOutputStream crcOut = new CheckedOutputStream(sessOS, new Adler32())) {
                // 获取输出文档
                OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);
                // 创建文件头
                FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
                // 序列化数据
                serialize(dt, sessions, oa, header);
                // 获取校验和
                long val = crcOut.getChecksum().getValue();
                // 写出
                oa.writeLong(val, "val");
                oa.writeString("/", "path");
                sessOS.flush();
            }
        } else {
            throw new IOException("FileSnap has already been closed");
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

}
