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
package org.apache.rocketmq.common.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

/**
 * 消息拓展类
 */
public class MessageExt extends Message {
    private static final long serialVersionUID = 5720810158625748049L;

    // 队列ID
    private int queueId;

    // 整个存储空间，包含TOTALSIZE,MAGICCODE,BODYCRC等
    private int storeSize;

    // 逻辑队列长度
    private long queueOffset;
    private int sysFlag;

    // 消息产生的时间
    private long bornTimestamp;

    // 消息产生的来源host
    private SocketAddress bornHost;

    // 存放消息的时间(不是写入mappedFile的时间)
    private long storeTimestamp;

    // 存放消息的host
    private SocketAddress storeHost;
/**/
    //
    private String msgId;

    // 物理偏移
    private long commitLogOffset;

    // CRC验证
    private int bodyCRC;

    // 消费次数(又称为重试的次数)
    private int reconsumeTimes;

    // 这个不知道有什么用，似乎不是0就是commitLogOffset,事务?
    private long preparedTransactionOffset;

    public MessageExt() {
    }

    public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp,
        SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static TopicFilterType parseTopicFilterType(final int sysFlag) {
        if ((sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG) {
            return TopicFilterType.MULTI_TAG;
        }

        return TopicFilterType.SINGLE_TAG;
    }

    public static ByteBuffer socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;

        // 将IP放到缓存中的前四位
        byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);

        // 将PORT紧跟着IP放置
        byteBuffer.putInt(inetSocketAddress.getPort());

        // limit设置成position，将缓冲区的position设置为0，使该缓冲区的访问从起始位置开始
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer socketAddress2ByteBuffer(SocketAddress socketAddress) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        return socketAddress2ByteBuffer(socketAddress, byteBuffer);
    }

    public ByteBuffer getBornHostBytes() {
        return socketAddress2ByteBuffer(this.bornHost);
    }

    public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.bornHost, byteBuffer);
    }

    public ByteBuffer getStoreHostBytes() {
        return socketAddress2ByteBuffer(this.storeHost);
    }

    public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public SocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(SocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public String getBornHostString() {
        if (this.bornHost != null) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) this.bornHost;
            return inetSocketAddress.getAddress().getHostAddress();
        }

        return null;
    }

    public String getBornHostNameString() {
        if (this.bornHost != null) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) this.bornHost;
            return inetSocketAddress.getAddress().getHostName();
        }

        return null;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(SocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public int getBodyCRC() {
        return bodyCRC;
    }

    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long physicOffset) {
        this.commitLogOffset = physicOffset;
    }

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public void setPreparedTransactionOffset(long preparedTransactionOffset) {
        this.preparedTransactionOffset = preparedTransactionOffset;
    }

    @Override
    public String toString() {
        return "MessageExt [queueId=" + queueId + ", storeSize=" + storeSize + ", queueOffset=" + queueOffset
            + ", sysFlag=" + sysFlag + ", bornTimestamp=" + bornTimestamp + ", bornHost=" + bornHost
            + ", storeTimestamp=" + storeTimestamp + ", storeHost=" + storeHost + ", msgId=" + msgId
            + ", commitLogOffset=" + commitLogOffset + ", bodyCRC=" + bodyCRC + ", reconsumeTimes="
            + reconsumeTimes + ", preparedTransactionOffset=" + preparedTransactionOffset
            + ", toString()=" + super.toString() + "]";
    }
}
