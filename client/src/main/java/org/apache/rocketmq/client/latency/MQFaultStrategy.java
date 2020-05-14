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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 * Broker故障延迟机制：用来规划消息发送时的延迟策略
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * 延迟容错对象，维护延迟Brokers的信息
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 延迟容错开关
     * 选择消息队列有两种方式: 1. 默认不启用 Broker故障延迟机制; 2. 启用Broker故障延迟机制
     */
    private boolean sendLatencyFaultEnable = false;

    /**
     * 延迟级别数组
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    /**
     * 不可用持续时辰，在这个时间内，Broker将被规避 。
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 选择消息队列（发送到那个队列中去）
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {

        // 如果打开了失败延迟机制
        if (this.sendLatencyFaultEnable) {
            try {

                // 选择发往那个队列之前，先获取一个自增的整数值
                int index = tpInfo.getSendWhichQueue().getAndIncrement();

                // 循环了所有消息队列对应的Broker节点
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {

                    // 将该整数值 模于 该主题下消息队列的小大
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;

                    // 拿到要发往的消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);

                    // 判断broketName对应的Broker节点是否可用，不可用就会将其过滤掉
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                // notBestBroker是一个brokerName，所有的Broker节点均不可用时，选择一个相对比较好的Broker选择一个队列进行发送
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {

                    // 从路由信息下选择下一个消息队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();

                    /**
                     * 维护发送失败Broker信息的集合中 若有Broker节点信息，则会一直选择里面的节点使用（所以默认不开启是合理的）；
                     *                           若没有，则走常规方式通过MessageQueue，向对应的Broker节点发送消息
                     */
                    if (notBestBroker != null) {

                        // 重置这个消息队列 的brokerName 和 queueId，进行消息的发送
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }

                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * @description: 容错机制 实质就是规避调不可用的broker节点
     * @param: currentLatency 本次消息发送延迟时间（消息通过Netty发送消息后与消息发送前的 时间差）
     * @param: isolation 是否隔离：该参数的含义如果为 true，则使用默认时长30s来计算Broker故障规避时长，
     *                           如果为false，则使用本次消息发送延迟时间来计算Broker故障规避时长。
     * @return:
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {

            // 如果为 true，则使用默认时长30s来计算Broker故障规避时长
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);

            // 更新异常的Broker节点信息
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 多久的时间内该 Broker将不参与消息发送队列负载。
     * 具体算法:
     * 从latencyMax数组尾部开始寻找，找到第一个比currentLatency小的下标，
     * 然后从notAvailableDuration数组中获取需要规避的时长，该方法最终调用LatencyFaultTolerance的updateFaultltem
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
