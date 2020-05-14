/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 该类为一个存储文件的直接内存映射业务抽象类，通过操作该类，可以把消息字节写入pagecache缓存区(commit),或者原子性的消息刷盘(flush)
 *
 * 对于commitlog、consumequeue、index三类大文件进行磁盘读写操作，均是通过MappedFile类来完成
 *
 * Message 添加到 MappedFile是顺序写的，消费的时候是随机读的
 *
 * MappedFile是MappedFileQueue（相当于一个存放commitlog文件的文件夹）下的一个个文件
 * 文件实体 和 物理文件是一对一关系
 */
public class MappedFile extends ReferenceResource {

    // 操作系统  每页大小，默认4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前JVM实例中MappedFile虚拟内存（占用内存大小）
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前JVM实例中MappedFile对象个数（文件数）
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 只针对当前MappedFile文件的写指针，从0开始(内存映射文件中的写指针)，如果要获取全局的写指针则需要加上当前MappedfFile文件的起始物理偏移量
     *
     * 写到的位置，在该值==fileSize时代表文件已经写满
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前文件的提交指针,刷盘之后的位置。当writeBuffer为空时，就不会有真正的提交。
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 刷盘刷到的位置(指针)，该指针之前的数据持久化到磁盘中
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * mappedFile文件大小，参照MessageStoreConfig.mapedFileSizeCommitLog，
     * commitLog，consumerQueue，IndexFile每个类型的MappedFile文件的大小不一样。
     */
    protected int fileSize;

    // nio阻塞  文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆外内存：其能不能使用取决于transientStorePool变量
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 堆外内存池，transientStorePoolEnable为true时启用
     *
     * transientStorePool Enable为true表示内容先存储在堆外内存，然后通过Commit线程将数据提交到内存映射Buffer中，
     * 再通过Flush线程将内存映射Buffer中的数据持久化到磁盘中
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 该文件的文件名（20位的）：不够20位前面补0 + fileFromOffset
     * 如：00000000000000000000
     */
    private String fileName;

    /**
     * 该文件的物理偏移量：就是MappedFile文件的名字
     */
    private long fileFromOffset;

    // 文件对象(物理文件)
    private File file;

    /**
     * 物理文件 对应 的内存映射(文件)Buffer
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 文件最后一次内容写入时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是否是MappedFileQueue队列中第一个文件
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 初始化MappedFile的方法 1
     *
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 初始化MappedFile的方法 2
     * transientStorePoolEnable为true表示内容先存储在堆外内存，然后通过Commit线程将数据提交到内存映射Buffer，再通过Flush线程将内存映射
     * Buffer中数据持久化到磁盘
     */
    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    /**
     * 嵌套调用，获取最深层的attachment 或者 viewedBuffer方法转化为ByteBuffer对象
     */
    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);

        // 从复用池中获取ByteBuffer,可能为null
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 初始化文件名，大小，以及writeBuffer
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);

        // 设置fileFromOffset代表文件对应的偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            // 创建一个随机访问文件流 (从File参数指定的文件中读取，并可选地写入文件),然后通过getChannel()方法获取用于可读取，写入，映射和操作文件的通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

            // 将fileSize大小的文件直接全部映射到内存中   内存映射文件MappedByteBuffer对象
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            // 将给定的值原子地添加到当前值
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);

            // 原子上增加一个当前值
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 将消息追加到MappedFile（即commitLog文件）中
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 将消息追加到MappedFile中,追加MessageExt消息, 更新写的位置以及存储时间, 返回AppendMessageResult
     *
     * 开始往mappedFile对象（commitLog文件）中写消息数据量了。
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 首先获取MappedFile当前写指针
        int currentPos = this.wrotePosition.get();

        //文件还有剩余空间
        if (currentPos < this.fileSize) {

            /*
             * 写入临时缓冲区（此处的ByteBuffer byteBuffer是申请的堆外缓冲区）：通过slice()方法创建一个与MappedFile的共享内存区，
             * 并设置 position 为当前指针
             *
             * 知识点介绍：
             * slice()方法会返回一个新的buffer，但是新的bf2和源对象bf引用的是同一个，也就是bf2的改变会改变bf。
             * 调用该方法得到的新缓冲区所操作的数组 还是原始缓冲区中的 那个数组，不过，通过slice创建的新缓冲区只能操作原始缓冲区中数组剩余的数据（即空间），
             * 即索引为调用slice方法时原始缓冲区的position到limit索引之间的数据（即空间），超出这个范围的数据通过slice创建的新缓冲区无法操作到
             */
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);//标记当前消息的position（只能操作slice出来的缓冲区种的 原始缓冲区的position到limit索引之间的数据），即剩余的空间
            AppendMessageResult result = null;
            if (messageExt instanceof MessageExtBrokerInner) {

                // 开始追加单条消息到MappedFile对象（即commitlog文件）
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {

                // 开始追加批量消息到MappedFile对象（即commitlog文件）
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            // 更新写的位置：表示将当前的写指针，向前推进 result.getWroteBytes(本次写入的字节)个字节
            this.wrotePosition.addAndGet(result.getWroteBytes());

            // 更新存储时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        // 如果currentPos大于或等于文件大小则表明文件已写满
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }


    /**
     * 将消息追加到MappedFile中,追加byte[]内容
     * 空间足够的话,将data[]直接写入fileChannel更新wrotePosition
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file. 将消息追加到MappedFile中,追加byte[]内容
     *
     * 空间足够的话,将data[]直接写入fileChannel更新wrotePosition
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 将内存中的数据刷写到磁盘
     *
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {// 是否可以刷盘
            if (this.hold()) {
                int value = getReadPosition();// 获得可以读到的最大位置

                try {
                    // We only append data to fileChannel or mappedByteBuffer, never both.
                    // 从缓存刷到磁盘
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);// 更新刷到的位置
                this.release();// 释放引用
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * commitLeastPages:为本次提交最小的页数。如果待提交数据不满commitLeastPages，则不执行本次提交操作。 如果writerBuffer为空
     *     直接返回writerPosition指针，无需执行commit操作。
     *
     * 提交: writeBuffer写入FileChannel
     * 1.当writeBuffer为null，直接返回
     * 2.否则将writeBuffer中(lastCommittedPosition,writePos)的部分 写入fileChannel
     * 3.然后对writeBuffer归还给transientStorePool，返回committedPosition
     */
    public int commit(final int commitLeastPages) {

        // 如果写缓冲区为空
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            // 直接返回当前该文件的写指针
            return this.wrotePosition.get();
        }

        // 判断是否执行commit操作
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // 如果没有被占用, 则将内存刷到磁盘上(具体的提交实现)
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);// 还给复用池
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 具体的提交实现:commit实现,持久化到日志
     *
     * commit的作用就是将MappedFile(内存映射文件)的writeBuffer中的数据提交到文件通道FileChannel中。
     *
     * @Param commitLeastPages:为本次提交最小的页数
     */
    protected void commit0(final int commitLeastPages) {

        // 当前该文件的写指针
        int writePos = this.wrotePosition.get();

        // 获取上一次提交的位置(committedPosition)
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                /*
                 * 利用slice()方法创建一个共享缓存区，与原先的ByteBuffer共享内存但维护一套独立的指针(position、mark、limit)
                 *
                 * 创建一个共享缓存区
                 */
                ByteBuffer byteBuffer = writeBuffer.slice();

                // 将新创建的position（指针）回退到上一次提交的位置(committedPosition)
                byteBuffer.position(lastCommittedPosition);

                // 设置limit为wrotePosition(当前最大有效数据指针)
                byteBuffer.limit(writePos);

                // 然后把commitedPosition到wrotePosition的数据复制 (写入)到FileChannel中
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);// 持久化

                // 然后更新committedPosition指针为wrotePosition
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否能够flush。满足如下条件任意条件：
     * 1. 文件已经写满
     * 2. flushLeastPages > 0 && 未flush部分超过flushLeastPages
     * 3. flushLeastPages = 0 && 有新写入部分
     *
     * @param flushLeastPages flush最小分页
     * @return 是否能够写入
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();// 最后有效的写到的位置

        if (this.isFull()) {
            return true;
        }

        // 文件写入的内容超过了4个操作系统默认页大小（每个4K）
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 是否能够commit。满足如下条件任意条件：
     * 1. 映射文件已经写满
     * 2. commitLeastPages > 0 && 未commit部分超过commitLeastPages
     * 3. commitLeastPages = 0 && 有新写入部分
     *
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {

        // 以刷盘指针
        int flush = this.committedPosition.get();

        // 文件写指针
        int write = this.wrotePosition.get();

        // 如果文件已满，返回TRUE
        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            // 文件内容达到commitLeastPages页数，则刷盘
            // 当前 writeBuffe 的写指针与上一次提交的指针(committedPosition) 的差值，除以 OS_PAGE_SIZE得到当前脏页的数量
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    /**
     * 如果写的位置和要求文件大小一样，则写满了
     */
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 返回从pos到 pos + size的内存映射
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();// 获取当前有效数据的最大位置
        if ((pos + size) <= readPosition) {// 返回的数据 必须是有效的

            if (this.hold()) {// 引用+1

                // 复制一个byteBuffer(与原byteBuffer共享数据, 只是指针位置独立)
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);

                // 绝对offset
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 返回从pos到最大有效位置的所有数据
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 实现父类函数，用于处理directByteBuffer相关的资源回收
     */
    @Override
    public boolean cleanup(final long currentRef) {
        // 如果available为true，表示MappedFile当前可用，无须清理
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        // 如果资源已经被清除，返回true
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        // 如果是堆外内存，调用堆外内存的 cleanup方法清除
        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));// 占用内存减少
        TOTAL_MAPPED_FILES.decrementAndGet();// 文件数减少
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 关闭FileChannel，删除文件
     */
    public boolean destroy(final long intervalForcibly) {

        // 销毁MappedFile过程中，首先需要释放资源，释放资源的前提条件是该MappedFile的引用小于等于0
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {

                // 关闭文件通道，删除物理文件
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 返回的是可以读到的最大位置(就是实际写到的位置)
     *
     * RocketMQ文件的一个组织方式是内存映射文件，预先申请一块连续的固定大小的内存，需要一套指针标识当前最大有效数据的位置，
     * 获取最大有效数据偏移量的方法由MappedFile的getReadPosition方法实现
     *
     * 获取前文件(MappedFile)最大读指针
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 预热mappedFile,
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        // 本地调用锁页操作
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));

            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            // 当用户态应用使用MADV_WILLNEED命令执行madvise()系统调用时，它会通知内核，某个文件内存映射区域中的给定范围的文件页不久将要被访问。
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
