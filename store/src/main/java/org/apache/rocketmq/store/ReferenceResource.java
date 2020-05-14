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

import java.util.concurrent.atomic.AtomicLong;

/**
 * MappedFile父类,作用是记录MappedFile中的引用次数, 为正表示资源可用，刷盘前加一，然后将wrotePosotion的值赋给committedPosition，再减一。
 */
public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;// 是否可用

    /**
     * cleanupOver为true的触发条件是release成功将MappedByteBuffer资源释放
     */
    protected volatile boolean cleanupOver = false;// 是否清理干净
    private volatile long firstShutdownTimestamp = 0;// 第一次shutdown时间

    /**
     * 引用，使得引用次数 +1
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {// 该分支是不会出现的
                // 原子减1当前值（先获取当前值，然后减一）
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * @description: MappedFile文件销毁的实现方法
     * @param: intervalForcibly 表示两次清理之间至少要隔的时间
     * @return: 
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;

            // 设置初次关闭的时间戳
            this.firstShutdownTimestamp = System.currentTimeMillis();

            // 然后调用release()方法尝试释放资源
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放引用，引用次数-1
     */
    public void release() {
        // 原子减1当前值（先减一,然后获取减一后的值）
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            // 如果引用数小于等于0，则执行cleanup方法
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 判断是否清理完成
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
