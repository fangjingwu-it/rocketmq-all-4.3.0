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

import java.nio.ByteBuffer;
import org.apache.rocketmq.common.message.MessageExtBatch;

/**
 * 吐槽：
 * 这个完全和回调没关系啊，为什么叫callback.就是被调用时，同步写到mappedFile返回AppendMessageResult，不涉及回调逻辑
 *
 * Write messages callback interface
 *
 * 追加消息到MappedFile后的回调接口
 */
public interface AppendMessageCallback {

    /**
     * After message serialization, write MapedByteBuffer： 处理一条消息，写入commitLog，返回结果
     * @param fileFromOffset, 也是mappedFile得文件名，20位数字
     * @param maxBlank, 文件剩余可用空间
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBrokerInner msg);

    /**
     * After batched message serialization, write MapedByteBuffer：处理一批消息，写入commitLog，返回结果
     *
     * @param messageExtBatch, backed up by a byte array
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBatch messageExtBatch);
}
