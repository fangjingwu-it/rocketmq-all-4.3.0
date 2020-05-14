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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 这个类就是索引服务，提供查询key，根据key构建索引，加载IndexFile到内存，destroy过期的IndexFile等功能
 *
 * 后台非实时执行，如果发送消息的propety字段里面有keys字段，那么会将他以空格为分隔符，生成key和对应的index信息；
 */
public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     * 尝试创建IndexFile，最多3次
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    private final int hashSlotNum;// 每个索引文件的槽数，默认500w
    private final int indexNum;// 每个索引文件最多记录的索引个数,默认2千万
    private final String storePath;// 索引文件存储路径，默认 user.home/store/index
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>(); // 索引文件（IndexFile）列表
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 将持久化文件的相关信息，读到内存中加载所有IndexFile,根据lastExitOK判断是否要destroy不合理的记录，更新indexFileList
     */
    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);

        // 获取IndexFile所有的文件名
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);

                    // 读取IndexFile头信息，加载到IndexHeader的内存中
                    f.load();

                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {

                            // 上次不ok，且时间比store记录的检查点要新的话，就无效
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    /**
     *  destroy所有"过期"的IndexFile
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 根据MessageKey查询消息的接口：根据topic和key作为关键字，在所有IndexFile中找到匹配的记录，且存储时间在[begin,end]时间内，最多记录maxNum条
     *
     * @param topic topic of the message. ：只能按topic维度来查询消息，因为索引生成的时候key是用的topic+MessageKey
     * @param key message key：消息键 messageKey
     * @param maxNum es pmaximum number of the messagossible：
     *               最多返回的消息数，因为key是由用户设置的，并不保证唯一，所以可能取到多个消息；同时index中只存储了hash，所以hash相同的消息也会取出来
     * @param begin begin timestamp 起始时间，只会查询指定时间段的消息
     * @param end end timestamp  结束时间，只会查询指定时间段的消息
     */
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {

                // 遍历indexFile文件的列表
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);

                    // 判断，只有遍历最后一个indexFile文件，才会更新indexLastUpdateTimestamp 和 indexLastUpdatePhyoffset两个参数
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {

                        // 将符合条件的 消息的物理偏移量放到 phyOffsets集合中
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    // 遍历到的indexFile文件的第一个索引消息的存储时间 小于开始时间 则之前的消息都不在此时间范围内了，结束遍历
                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    /**
     * 这个是个辅助的，就是把topic和key拼起来
     */
    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    /**
     * 建立 索引信息 到 IndexFile
     */
    public void buildIndex(DispatchRequest req) {

        // 1、获取或者新建当前可写入的index file
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {

            //2、获取当前indexFile文件中的最后一条索引消息的物理偏移量offset
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();

            //3、新请求的消息对应的 已提交物理偏移量 小于 indexFile中的最后一条消息的物理偏移量 应当前是之前的，不应该出现
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            // 单条索引数据的写入
            if (req.getUniqKey() != null) {

                // 往索引文件（indexFile）中写入一条索引 及对应 一条消息
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // 多个msg，循环逐个存入
            if (keys != null && keys.length() > 0) {
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * @description: 往索引文件（indexFile）中写入一条索引 对应 一条消息
     * @param:indexFile 要写入的索引文件
     * @param:msg 往索引文件要写入的消息
     * @param:idxKey 索引文件中 消息对应的索引Key (topic + "#" + key)
     * @return:
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp())
                     /*indexFile写满就返回false*/; !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            // indexFile文件写满需要重新创建一个
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            //
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        // 未写满的情况下，直接返回这个indexFile文件
        return indexFile;
    }

    /**
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取或创建一个IndexFile
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;


        IndexFile prevIndexFile = null;

        // 最后一次更新，最后一条消息的物理偏移量
        long lastUpdateEndPhyOffset = 0;

        // 最后一次更新更新索引文件的时间
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();

            if (!this.indexFileList.isEmpty()) {// 索引文件（IndexFile）列表不为空

                // 获取索引文件（IndexFile）列表中的最后一个 索引文件（IndexFile）
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {// 该索引文件IndexFile-没有写满

                    // 就将这个indexFile文件返回
                    indexFile = tmp;
                } else {// 该索引文件IndexFile-写满了
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();

                    // 将写满的索引文件IndexFile存起来
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        // 获取索引文件（IndexFile）列表中的最后一个 索引文件（IndexFile）写满的情况下才会走到这里
        if (indexFile == null) {
            try {
                // Linux中的File.separator 是『/』
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {


                final IndexFile flushThisFile = prevIndexFile;

                // 启动一个线程，将新创建的前一个IndexFile文件刷写落盘
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
