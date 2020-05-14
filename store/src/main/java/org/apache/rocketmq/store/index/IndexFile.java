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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 在实际的物理存储上，文件名则是以创建时的时间戳命名的，固定的单个IndexFile文件大小约为400M，一个IndexFile可以保存 2000W个索引；
 *
 * 针对指定消息的索引文件，主要存储消息Key 与 Offset的对应关系
 *
 * 索引文件的对应实体类
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 每一个槽信息含4字节
    private static int hashSlotSize = 4;

    // 每一个索引信息占据20字节
    private static int indexSize = 20;

    // invalidIndex是0表示无效索引下标
    private static int invalidIndex = 0;

    /**
     * SlotTable槽表数量(4字节500w记录--默认有500w个槽)
     *
     * 每个槽中记录的值的是什么:这一个slot存放的最新的索引,下标是几(从1开始,到默认的2000w)
     * 比如说第5个槽，之前放了第20个记录，slotTable中第5个槽记录的值就是20
     * 之后第22条记录也hash到了第5个槽，那么slotTable第5个槽记录的值就变成22
     */
    private final int hashSlotNum;

    /**
     * 索引数量：
     * Index Linked List索引链表,默认2000w条记录，每一条记录占位20字节构成如下:
     *
     * keyHash: 4位，int值，key的hash值
     * phyOffset：8位，long值，索引消息commitLog偏移量
     * timeDiff: 4位，int值，索引存储的时间与IndexHeader的beginTimestamp时间差(这是为了节省空间)
     * slotValue:4位，int值，索引对应slot的上一条索引的下标(据此完成一条向老记录的链表)
     */
    private final int indexNum;

    // 没有用到
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;

    // 其缓存的是整个indexFile文件
    private final MappedByteBuffer mappedByteBuffer;

    // IndexHeader 索引头(40字节)
    private final IndexHeader indexHeader;

    /**
     * 创建一个IndexFile
     * @param fileName 文件名
     * @param hashSlotNum hash槽的个数，默认5million
     * @param indexNum 最多记录的索引条数,默认2千万
     * @param endPhyOffset 开始物理偏移
     * @param endTimestamp 开始的时间戳
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {

            // 设置物理偏移
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {

            // 设置时间
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    // 加载IndexHeader，读取取byteBuffer的内容到内存中
    public void load() {
        this.indexHeader.load();
    }

    // 将内存中的数据更新ByteBuffer，持久化IndexHeader
    public void flush() {
        long beginTime = System.currentTimeMillis();

        // 引用+1
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();

            // 刷到磁盘
            this.mappedByteBuffer.force();

            // 释放引用
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    // 是否写满了，就是已经写的索引数是否超过了指定的索引数（2千万）
    public boolean isWriteFull() {

        // IndexFile文件的索引总个数 大于 指定的索引个数
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 写入索引消息:存放一条索引消息,更新IndexHeader，slotTable，Index Linked List三个记录
     * @param key 是 topic-key值
     * @param phyOffset 消息的物理偏移量
     * @param storeTimestamp 消息的存储时间
     * @return
     **/
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) { // 说明这个indexFile文件 所容纳的索引容量 还没写满

            // 获取message Key的hash值
            int keyHash = indexKeyHashMethod(key);

            // 取模决定放在哪个槽
            int slotPos = keyHash % this.hashSlotNum;

            // 计算得到该hash slot 在该indexFile文件中的 的实际文件位置Position（即在第多少个字节）

            // 槽在Index中的绝对位置 = 索引头的大小（40） + 第几个槽 * 单个槽的站位大小
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,false);

                // 根据该hash slot的实际文件位置absSlotPos得到slot里的值 这里有两种情况：
                // 1). slot=0, 当前message的key是该hash值第一个消息索引
                // 2). slot>0, 该key hash值上一个消息索引的位置

                // ByteBuffer  getInt（int value）API 介绍：读取给定索引处的四个字节，根据当前字节顺序将它们组成一个int值。

                // 这里的意思就是获取当前indexFile文件中的 该位置的slot槽中存储的是 第几条index索引值

                /*
                 * 读取给定索引处(往后取4个字节)的四个字节，根据当前字节顺序将它们组成一个int值。
                 *
                 * 操中的值: 看此solt中是否有值。 如果有，则是存在hash冲突（下面会把value设置成当前index的前一个index，
                 * 同时将slot中的value更新成当前消息的序号。这样整个索引的生成就结束了），获取这个slot存的前一个index的计数，如果没有则值为0
                 */
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {

                    // 表示当前indexFile文件中没有一条索引数据
                    slotValue = invalidIndex;
                }

                //
                //
                // storeTimestamp（CommitLog消息的存储时间），timeDiff是相对header的beginTime(Index第一个条目的时间)的偏移
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 目前该indexFile文件最后一条索引文件的存放位置： 索引该放在文件的哪个字节  计算当前消息索引具体的存储位置(Append模式)
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize // 前半部分值是固定的（索引头+slotTable占据的空间）
                        + this.indexHeader.getIndexCount() * indexSize; // 每增加一条消息的位置增量也是固定的。

                /*
                 * 存入该消息索引
                 * 先是4字节，将此条消息的keyHash值（决定了其放在那个槽），放入索引绝对位置的角标上（占个单条索引数据的头部）。
                 * 再4字节，是commitLog的物理偏移
                 * 再8字节，是时间戳
                 * 再4字节，槽位信息，代表该槽位上一个key在文件中的位置(即向前的链表)
                 *
                 * putInt(int index, int value) 将包含给定int值的四个字节（按当前字节顺序）写入给定索引处的此缓冲区。
                 */
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);

                // 同一个Hash槽上 该条索引的 上一个Index索引的位置（0标识没有上一个）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 记录该slot当前index，如果hash冲突（即absSlotPos一致）作为下一次该slot新增的前置index
                // 更新slot中的值为本条消息的顺序号
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 如果是本indexFile文件的第一条索引，记录开始的物理偏移，以及时间
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 槽的数量+1,这个slotCount并没有实际作用，随便写
                this.indexHeader.incHashSlotCount();

                // 索引文件中的索引个数+1
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    // 获取key的hash值(int)，转成绝对值
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    // 判断时间是否有重叠：[begin,end]时间与 [indexHeader.getBeginTimestamp(), indexHeader.getEndTimestamp()]时间有重叠
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * @description: 根据key找到slot对应的整个列表，找到keyHash一样且存储时间在给定的[begin,end]范围内的索引列表
     *              （最多maxNum条，从新到旧），返回他们的物理偏移
     * @param phyOffsets 物理偏移列表,用于返回
     * @param key 关键字
     * @param maxNum phyOffsets最多保存这么多条记录就返回
     * @param begin 开始时间戳
     * @param end 结束时间戳
     * @param lock:没有用
     * @return: 
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);

            // 定位到第几个槽
            int slotPos = keyHash % this.hashSlotNum;

            // 该槽在indexFile文件中的 那个字节上
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取对应槽的最新的对应的 索引（即indexFile数据结构 indexList 部分的角标号）的下标
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) { // 索引列表对应的角标 对应的槽就没有合理记录
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {// 遍历链表一样的结构

                        // 若果phyOffsets集合大小 超过了 最多纪录的maxNum个，则结束循环
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        // 根据 索引的下脚标 找到 此索引 在文件中的位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);

                        /**
                         * hash冲突处理的关键之处, 相同hash值的  上一个消息索引的index脚标  (如果当前消息索引是该hash值的第一个索引，则prevIndex=0,
                         * 也是消息索引查找时的停止条件)，每个slot位置的第一个消息的prevIndex就是0的。
                         */

                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        // 不断地找更老的记录，timeDiff会越来越小
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        // 根据基准时间戳以及 timeDiff，得到索引存储的时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;

                        // 存储时间戳在[begin,end]内
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        // 原有hash值匹配：MessageKey的hash值相同 && 在给定的时间范围内的 消息的物理偏移量 都放到phyOffsets集合中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 链表循环,已经找到了最老的索引了，可以break了
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        // 遍历到同slot的前一个key中
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
