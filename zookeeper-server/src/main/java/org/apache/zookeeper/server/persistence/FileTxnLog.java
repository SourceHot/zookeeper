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

import org.apache.jute.Record;
import org.apache.jute.*;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * This class implements the TxnLog interface. It provides api's
 * to access the txnlogs and add entries to it.
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * LogFile:
 *     FileHeader TxnList ZeroPad
 *
 * FileHeader: {
 *     magic 4bytes (ZKLG)
 *     version 4bytes
 *     dbid 8bytes
 *   }
 *
 * TxnList:
 *     Txn || Txn TxnList
 *
 * Txn:
 *     checksum Txnlen TxnHeader Record 0x42
 *
 * checksum: 8bytes Adler32 is currently used
 *   calculated across payload -- Txnlen, TxnHeader, Record and 0x42
 *
 * Txnlen:
 *     len 4bytes
 *
 * TxnHeader: {
 *     sessionid 8bytes
 *     cxid 4bytes
 *     zxid 8bytes
 *     time 8bytes
 *     type 4bytes
 *   }
 *
 * Record:
 *     See Jute definition file for details on the various record types
 *
 * ZeroPad:
 *     0 padded to EOF (filled during preallocation stage)
 * </pre></blockquote>
 */
public class FileTxnLog implements TxnLog, Closeable {

    public final static int TXNLOG_MAGIC =
            ByteBuffer.wrap("ZKLG".getBytes()).getInt();
    public final static int VERSION = 2;
    public static final String LOG_FILE_PREFIX = "log";
    static final String FSYNC_WARNING_THRESHOLD_MS_PROPERTY = "fsync.warningthresholdms";
    static final String ZOOKEEPER_FSYNC_WARNING_THRESHOLD_MS_PROPERTY =
            "zookeeper." + FSYNC_WARNING_THRESHOLD_MS_PROPERTY;
    private static final Logger LOG;
    /** Maximum time we allow for elapsed fsync before WARNing */
    private final static long fsyncWarningThresholdMS;

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        /** Local variable to read fsync.warningthresholdms into */
        Long fsyncWarningThreshold;
        if ((fsyncWarningThreshold = Long.getLong(ZOOKEEPER_FSYNC_WARNING_THRESHOLD_MS_PROPERTY))
                == null)
            fsyncWarningThreshold = Long.getLong(FSYNC_WARNING_THRESHOLD_MS_PROPERTY, 1000);
        fsyncWarningThresholdMS = fsyncWarningThreshold;
    }

    private final boolean forceSync =
            !System.getProperty("zookeeper.forceSync", "yes").equals("no");
    /**
     * 最大的zxid，最新的zxid
     */
    long lastZxidSeen;
    volatile BufferedOutputStream logStream = null;
    volatile OutputArchive oa;
    volatile FileOutputStream fos = null;
    File logDir;
    long dbId;
    File logFileWrite = null;
    private LinkedList<FileOutputStream> streamsToFlush =
            new LinkedList<FileOutputStream>();
    private FilePadding filePadding = new FilePadding();

    private ServerStats serverStats;

    private volatile long syncElapsedMS = -1L;

    /**
     * constructor for FileTxnLog. Take the directory
     * where the txnlogs are stored
     * @param logDir the directory where the txnlogs are stored
     */
    public FileTxnLog(File logDir) {
        this.logDir = logDir;
    }

    /**
     * method to allow setting preallocate size
     * of log file to pad the file.
     * @param size the size to set to in bytes
     */
    public static void setPreallocSize(long size) {
        FilePadding.setPreallocSize(size);
    }

    /**
     * Find the log file that starts at, or just before, the snapshot. Return
     * this and all subsequent logs. Results are ordered by zxid of file,
     * ascending order.
     *
     * @param logDirList   array of files
     * @param snapshotZxid return files at, or before this zxid
     * @return
     */
    public static File[] getLogFiles(File[] logDirList, long snapshotZxid) {
        // 通过Util.sortDataDir对日志文件排序
        List<File> files = Util.sortDataDir(logDirList, LOG_FILE_PREFIX, true);
        // 日志zxid标记
        long logZxid = 0;
        // Find the log file that starts before or at the same time as the
        // zxid of the snapshot
        // 循环日志文件集合
        for (File f : files) {
            // 从文件名称中获取zxid
            long fzxid = Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
            // 如果文件名的zxid大于快照zxid跳过处理
            if (fzxid > snapshotZxid) {
                continue;
            }
            // the files
            // are sorted with zxid's
            // 如果文件名的zxid大于日志zxid将文件名的zxid赋值给日志zxid
            if (fzxid > logZxid) {
                logZxid = fzxid;
            }
        }
        // 创建结果集
        List<File> v = new ArrayList<File>(5);
        // 循环日志文件集合
        for (File f : files) {
            // 从文件名称中获取zxid
            long fzxid = Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
            // 如果文件名称的zxid小于日志zxid不将其加入到结果集中
            if (fzxid < logZxid) {
                continue;
            }
            v.add(f);
        }
        return v.toArray(new File[0]);

    }

    /**
     * read the header of the transaction file
     * @param file the transaction file to read
     * @return header that was read from the file
     * @throws IOException
     */
    private static FileHeader readHeader(File file) throws IOException {
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            InputArchive ia = BinaryInputArchive.getArchive(is);
            FileHeader hdr = new FileHeader();
            hdr.deserialize(ia, "fileheader");
            return hdr;
        } finally {
            try {
                if (is != null)
                    is.close();
            } catch (IOException e) {
                LOG.warn("Ignoring exception during close", e);
            }
        }
    }

    /**
     * Setter for ServerStats to monitor fsync threshold exceed
     * @param serverStats used to update fsyncThresholdExceedCount
     */
    @Override
    public synchronized void setServerStats(ServerStats serverStats) {
        this.serverStats = serverStats;
    }

    /**
     * creates a checksum algorithm to be used
     * @return the checksum used for this txnlog
     */
    protected Checksum makeChecksumAlgorithm() {
        return new Adler32();
    }

    /**
     * rollover the current log file to a new one.
     * @throws IOException
     */
    public synchronized void rollLog() throws IOException {
        if (logStream != null) {
            this.logStream.flush();
            this.logStream = null;
            oa = null;
        }
    }

    /**
     * close all the open file handles
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if (logStream != null) {
            logStream.close();
        }
        for (FileOutputStream log : streamsToFlush) {
            log.close();
        }
    }

    /**
     * append an entry to the transaction log
     *
     * @param hdr the header of the transaction
     * @param txn the transaction part of the entry
     *            returns true iff something appended, otw false
     */
    public synchronized boolean append(TxnHeader hdr, Record txn)
            throws IOException {
        // 判断事务头是否为空，如果为空则返回false结束处理
        if (hdr == null) {
            return false;
        }
        // 判断事务头信息中的zxid是否小于等于最大的zxid，如果是则输出warn日志，反之则会将最大zxid设置数据进行重设，重设内容是事务头信息中的zxid
        if (hdr.getZxid() <= lastZxidSeen) {
            LOG.warn("Current zxid " + hdr.getZxid()
                    + " is <= " + lastZxidSeen + " for "
                    + hdr.getType());
        } else {
            lastZxidSeen = hdr.getZxid();
        }
        // 日志流为空
        if (logStream == null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Creating new log file: " + Util.makeLogName(hdr.getZxid()));
            }

            // 创建需要写出的文件对象
            logFileWrite = new File(logDir, Util.makeLogName(hdr.getZxid()));
            // 创建文件输出流
            fos = new FileOutputStream(logFileWrite);
            // 初始化日志流对象
            logStream = new BufferedOutputStream(fos);
            // 初始化输出档案对象
            oa = BinaryOutputArchive.getArchive(logStream);
            // 构造事务头信息
            FileHeader fhdr = new FileHeader(TXNLOG_MAGIC, VERSION, dbId);
            // 序列化头信息
            fhdr.serialize(oa, "fileheader");
            // Make sure that the magic number is written before padding.
            // 写出到文件
            logStream.flush();
            // 计算当前通道的大小并设置到filePadding对象中
            filePadding.setCurrentSize(fos.getChannel().position());
            // 将fos放入到集合中
            streamsToFlush.add(fos);
        }
        // 填充文件
        filePadding.padFile(fos.getChannel());
        // 对事务头信息和事务对象进行序列化
        byte[] buf = Util.marshallTxnEntry(hdr, txn);
        // 序列化结果为空或者长度为0抛出异常
        if (buf == null || buf.length == 0) {
            throw new IOException("Faulty serialization for header " +
                    "and txn");
        }
        // 获取校验和计算接口
        Checksum crc = makeChecksumAlgorithm();
        // 更新校验和
        crc.update(buf, 0, buf.length);
        // 将校验和信息写入到输出档案
        oa.writeLong(crc.getValue(), "txnEntryCRC");
        // 写入到输出档案中
        Util.writeTxnBytes(oa, buf);

        return true;
    }

    /**
     * get the last zxid that was logged in the transaction logs
     * @return the last zxid logged in the transaction logs
     */
    public long getLastLoggedZxid() {
        // 获取日志目录下的所有文件
        File[] files = getLogFiles(logDir.listFiles(), 0);
        // 确定最大日志编号
        // 日志文件数量大于零的情况下通过getZxidFromName方法获取，反之则直接取值-1
        long maxLog = files.length > 0 ?
                Util.getZxidFromName(files[files.length - 1].getName(), LOG_FILE_PREFIX) : -1;

        // if a log file is more recent we must scan it to find
        // the highest zxid
        // 设置zxid，先将最大日志编号设置给zxid
        long zxid = maxLog;
        TxnIterator itr = null;
        try {
            // 将日志目录转换为事务日志
            FileTxnLog txn = new FileTxnLog(logDir);
            // 从事务日志中读取TxnIterator
            itr = txn.read(maxLog);
            // 循环处理TxnIterator中的数据，跳出条件是没有下一个TxnIterator对象
            while (true) {
                if (!itr.next()) {
                    break;
                }
                // 获取头信息，将
                TxnHeader hdr = itr.getHeader();
                zxid = hdr.getZxid();
            }
        } catch (IOException e) {
            LOG.warn("Unexpected exception", e);
        } finally {
            close(itr);
        }
        return zxid;
    }

    private void close(TxnIterator itr) {
        if (itr != null) {
            try {
                itr.close();
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
        }
    }

    /**
     * commit the logs. make sure that everything hits the
     * disk
     */
    public synchronized void commit() throws IOException {
        // 日志流不为空的情况下进行输出
        if (logStream != null) {
            logStream.flush();
        }
        // 循环文件输出流集合
        for (FileOutputStream log : streamsToFlush) {
            // 输出流
            log.flush();
            // 如果需要强制同步
            if (forceSync) {
                // 获取当前纳秒时间
                long startSyncNS = System.nanoTime();
                // 获取通道
                FileChannel channel = log.getChannel();
                // 强制将通道中的数据写出
                channel.force(false);
                // 计算时间差
                syncElapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startSyncNS);
                // 如果时间差大于最大允许同步时间
                if (syncElapsedMS > fsyncWarningThresholdMS) {
                    // 服务状态不为空
                    if (serverStats != null) {
                        // 同步超时次数累加1
                        serverStats.incrementFsyncThresholdExceedCount();
                    }
                    LOG.warn("fsync-ing the write ahead log in "
                            + Thread.currentThread().getName()
                            + " took " + syncElapsedMS
                            + "ms which will adversely effect operation latency. "
                            + "File size is " + channel.size() + " bytes. "
                            + "See the ZooKeeper troubleshooting guide");
                }
            }
        }
        // 如果文件输出流集合数量大于1则会将第一个移除并且关闭
        while (streamsToFlush.size() > 1) {
            streamsToFlush.removeFirst().close();
        }
    }

    /**
     *
     * @return elapsed sync time of transaction log in milliseconds
     */
    public long getTxnLogSyncElapsedTime() {
        return syncElapsedMS;
    }

    /**
     * start reading all the transactions from the given zxid
     * @param zxid the zxid to start reading transactions from
     * @return returns an iterator to iterate through the transaction
     * logs
     */
    public TxnIterator read(long zxid) throws IOException {
        return read(zxid, true);
    }

    /**
     * start reading all the transactions from the given zxid.
     *
     * @param zxid the zxid to start reading transactions from
     * @param fastForward true if the iterator should be fast forwarded to point
     *        to the txn of a given zxid, else the iterator will point to the
     *        starting txn of a txnlog that may contain txn of a given zxid
     * @return returns an iterator to iterate through the transaction logs
     */
    public TxnIterator read(long zxid, boolean fastForward) throws IOException {
        return new FileTxnIterator(logDir, zxid, fastForward);
    }

    /**
     * truncate the current transaction logs
     *
     * @param zxid the zxid to truncate the logs to
     * @return true if successful false if not
     */
    public boolean truncate(long zxid) throws IOException {
        FileTxnIterator itr = null;
        try {
            // 创建FileTxnIterator对象
            itr = new FileTxnIterator(this.logDir, zxid);
            // 从FileTxnIterator对象中获取输入流
            PositionInputStream input = itr.inputStream;
            // 如果输入流为空抛出异常
            if (input == null) {
                throw new IOException("No log files found to truncate! This could " +
                        "happen if you still have snapshots from an old setup or " +
                        "log files were deleted accidentally or dataLogDir was changed in zoo.cfg.");
            }
            // 从输入流中获取位置索引
            long pos = input.getPosition();
            // now, truncate at the current position
            // 截断日志文件内容
            RandomAccessFile raf = new RandomAccessFile(itr.logFile, "rw");
            // 阶段到pos的位置
            raf.setLength(pos);
            raf.close();
            // 如果存在下一个日志文件
            while (itr.goToNextLog()) {
                // 删除失败记录warn级别日志
                if (!itr.logFile.delete()) {
                    LOG.warn("Unable to truncate {}", itr.logFile);
                }
            }
        } finally {
            close(itr);
        }
        return true;
    }

    /**
     * the dbid of this transaction database
     * @return the dbid of this database
     */
    public long getDbId() throws IOException {
        FileTxnIterator itr = new FileTxnIterator(logDir, 0);
        FileHeader fh = readHeader(itr.logFile);
        itr.close();
        if (fh == null) {
            throw new IOException("Unsupported Format.");
        }
        return fh.getDbid();
    }

    /**
     * the forceSync value. true if forceSync is enabled, false otherwise.
     * @return the forceSync value
     */
    public boolean isForceSync() {
        return forceSync;
    }

    /**
     * a class that keeps track of the position
     * in the input stream. The position points to offset
     * that has been consumed by the applications. It can
     * wrap buffered input streams to provide the right offset
     * for the application.
     */
    static class PositionInputStream extends FilterInputStream {
        long position;

        protected PositionInputStream(InputStream in) {
            super(in);
            position = 0;
        }

        @Override
        public int read() throws IOException {
            int rc = super.read();
            if (rc > -1) {
                position++;
            }
            return rc;
        }

        public int read(byte[] b) throws IOException {
            int rc = super.read(b);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rc = super.read(b, off, len);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }

        @Override
        public long skip(long n) throws IOException {
            long rc = super.skip(n);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset");
        }
    }


    /**
     * this class implements the txnlog iterator interface
     * which is used for reading the transaction logs
     */
    public static class FileTxnIterator implements TxnLog.TxnIterator {
        static final String CRC_ERROR = "CRC check failed";
        /**
         * 日志文件夹
         */
        File logDir;
        /**
         * zxid
         */
        long zxid;
        /**
         * 交易头信息
         */
        TxnHeader hdr;
        /**
         * 数据信息
         */
        Record record;
        /**
         * 日志文件
         */
        File logFile;
        /**
         * 输入档案
         */
        InputArchive ia;
        PositionInputStream inputStream = null;
        //stored files is the list of files greater than
        //the zxid we are looking for.
        private ArrayList<File> storedFiles;

        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @param fastForward   true if the iterator should be fast forwarded to
         *        point to the txn of a given zxid, else the iterator will
         *        point to the starting txn of a txnlog that may contain txn of
         *        a given zxid
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid, boolean fastForward)
                throws IOException {
            this.logDir = logDir;
            this.zxid = zxid;
            init();

            if (fastForward && hdr != null) {
                while (hdr.getZxid() < zxid) {
                    if (!next())
                        break;
                }
            }
        }

        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid) throws IOException {
            this(logDir, zxid, true);
        }

        /**
         * initialize to the zxid specified
         * this is inclusive of the zxid
         * @throws IOException
         */
        void init() throws IOException {
            storedFiles = new ArrayList<File>();
            // 读取日志目录下的所有日志文件
            List<File> files =
                    Util.sortDataDir(FileTxnLog.getLogFiles(logDir.listFiles(), 0), LOG_FILE_PREFIX,
                            false);
            // 循环日志文件
            for (File f : files) {
                // 获取日志文件名称中的zxid，如果大于等于则加入到storedFiles集合中
                if (Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX) >= zxid) {
                    storedFiles.add(f);
                }
                // add the last logfile that is less than the zxid
                // 如果日志文件名称中的zxid小于zxid则加入到storedFiles集合中，并且结束循环
                else if (Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX) < zxid) {
                    storedFiles.add(f);
                    break;
                }
            }

            // 将storedFiles集合中的最后一个文件转换为InputArchive类型，并赋值给成员变量ia
            goToNextLog();
            // 移动到下一个FileTxnIterator
            next();
        }

        /**
         * Return total storage size of txnlog that will return by this iterator.
         */
        public long getStorageSize() {
            long sum = 0;
            for (File f : storedFiles) {
                sum += f.length();
            }
            return sum;
        }

        /**
         * go to the next logfile
         * @return true if there is one and false if there is no
         * new file to be read
         * @throws IOException
         */
        private boolean goToNextLog() throws IOException {
            if (storedFiles.size() > 0) {
                this.logFile = storedFiles.remove(storedFiles.size() - 1);
                ia = createInputArchive(this.logFile);
                return true;
            }
            return false;
        }

        /**
         * read the header from the inputarchive
         * @param ia the inputarchive to be read from
         * @param is the inputstream
         * @throws IOException
         */
        protected void inStreamCreated(InputArchive ia, InputStream is)
                throws IOException {
            FileHeader header = new FileHeader();
            header.deserialize(ia, "fileheader");
            if (header.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
                throw new IOException(
                        "Transaction log: " + this.logFile + " has invalid magic number "
                                + header.getMagic()
                                + " != " + FileTxnLog.TXNLOG_MAGIC);
            }
        }

        /**
         * Invoked to indicate that the input stream has been created.
         * @param ia input archive
         * @param is file input stream associated with the input archive.
         * @throws IOException
         **/
        protected InputArchive createInputArchive(File logFile) throws IOException {
            if (inputStream == null) {
                inputStream = new PositionInputStream(
                        new BufferedInputStream(new FileInputStream(logFile)));
                LOG.debug("Created new input stream " + logFile);
                ia = BinaryInputArchive.getArchive(inputStream);
                inStreamCreated(ia, inputStream);
                LOG.debug("Created new input archive " + logFile);
            }
            return ia;
        }

        /**
         * create a checksum algorithm
         * @return the checksum algorithm
         */
        protected Checksum makeChecksumAlgorithm() {
            return new Adler32();
        }

        /**
         * the iterator that moves to the next transaction
         * @return true if there is more transactions to be read
         * false if not.
         */
        public boolean next() throws IOException {
            if (ia == null) {
                return false;
            }
            try {
                // 从ia(档案)中读取crcvalue数据
                long crcValue = ia.readLong("crcvalue");
                // 读取ia中的txn数据
                byte[] bytes = Util.readTxnBytes(ia);
                // Since we preallocate, we define EOF to be an
                if (bytes == null || bytes.length == 0) {
                    throw new EOFException("Failed to read " + logFile);
                }
                // EOF or corrupted record
                // validate CRC
                // 生成校验和接口实现类
                Checksum crc = makeChecksumAlgorithm();
                // 更新校验和数据
                crc.update(bytes, 0, bytes.length);
                // 如果发现前后两个校验和数据不同抛出异常
                if (crcValue != crc.getValue()) {
                    throw new IOException(CRC_ERROR);
                }
                // 创建事务头
                hdr = new TxnHeader();
                // 将txn反序列化到record
                record = SerializeUtils.deserializeTxn(bytes, hdr);
            } catch (EOFException e) {
                LOG.debug("EOF exception " + e);
                inputStream.close();
                inputStream = null;
                ia = null;
                hdr = null;
                // this means that the file has ended
                // we should go to the next file
                if (!goToNextLog()) {
                    return false;
                }
                // if we went to the next log file, we should call next() again
                return next();
            } catch (IOException e) {
                inputStream.close();
                throw e;
            }
            return true;
        }

        /**
         * return the current header
         * @return the current header that
         * is read
         */
        public TxnHeader getHeader() {
            return hdr;
        }

        /**
         * return the current transaction
         * @return the current transaction
         * that is read
         */
        public Record getTxn() {
            return record;
        }

        /**
         * close the iterator
         * and release the resources.
         */
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

}
