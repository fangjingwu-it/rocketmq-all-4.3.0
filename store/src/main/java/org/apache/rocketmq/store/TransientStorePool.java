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
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 短暂的存储池：用于池化管理多个ByteBuffer对象,进行借与还的操作
 * Transient：瞬间
 * RocketMQ单独创建一个MappedByteBuffer内存缓存池，用来临时存储数据，数据先写人该内存映射中，然后由commit线程定时将数据从该内存复制到与目的物理文件对应的内存映射中。
 * RokcetMQ引入该机制主要的原因是：提供一种内存锁定，将当前堆外内存一直锁定在内存中，避免被进程将内存交换到磁盘
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * availableBuffers个数
     * 池的大小有多少，可通过在broker中配置文件中设置transientStorePoolSize，默认为5
     */
    private final int poolSize;

    /**
     * 每个ByteBuffer大小，默认1G
     */
    private final int fileSize;

    /**
     * 用于存放创建好的堆外ByteBuffer缓冲区，双端队列
     */
    private final Deque<ByteBuffer> availableBuffers;

    /**
     * 存储配置
     */
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMapedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     *
     * 初始化函数，分配poolSize个fileSize的堆外空间
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {

            // 创建每个堆外内存，并利用com.sun.jna.Library类库将该批内存锁定，避免被置换到交换区，提高存储性能
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            // 将指定的元素插入由此deque（换句话说，在该deque的尾部）表示的队列中
            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁availableBuffers中所有buffer数据
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 用完了之后,返还一个buffer,对buffer数据进行清理
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 借一个buffer出去
     */
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * 剩余可用的buffers数量
     */
    public int remainBufferNumbs() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
