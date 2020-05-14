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

package org.apache.rocketmq.client.common;

import java.util.Random;

/**
 * 作用：生产客户端在发送消息时 每选择一次该主题的任意消息队列，该值会自增l，如果 Integer.MAX_VALUE,则重置为 0，用于选择消息队列。
 */
public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal();
    private final Random random = new Random();

    public int getAndIncrement() {

        // 首先获取当前的整数值
        Integer index = this.threadLocalIndex.get();
        if (null == index) {

            // 获取下一个整数值
            index = Math.abs(random.nextInt());
            if (index < 0)
                index = 0;
            this.threadLocalIndex.set(index);
        }

        index = Math.abs(index + 1);
        if (index < 0)
            index = 0;

        this.threadLocalIndex.set(index);
        return index;
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
            "threadLocalIndex=" + threadLocalIndex.get() +
            '}';
    }
}
