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
package org.apache.rocketmq.example.simple;

        import java.util.List;
        import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
        import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
        import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
        import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
        import org.apache.rocketmq.client.exception.MQClientException;
        import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
        import org.apache.rocketmq.common.message.MessageExt;

public class PushConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1");
        consumer.subscribe("Jodie_topic_1023", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 设置开始消费的时间点
        consumer.setConsumeTimestamp("20170422221800");

        // 一次消息消费的数量 默认是1条
        consumer.getConsumeMessageBatchMaxSize();

        // 设置下次去拉取消息的时间间隔，默认是立刻去拉取
//        consumer.setPullInterval();

        /*
         * 执行流程描述：
         * 1. 将对象new MessageListenerConcurrently()注册到DefaultMQPushConsumer对象中；
         *
         * 注意：下面这两步实在 consumer.start();以后才会运行到的。
         * 2. 在DefaultMQPushConsumer等对象在拿到消费消息以后，会调用（回调）这个匿名对象的方法consumeMessage，
         *    将将要消费的消息和消费消息上下文传给该（回调）方法；
         * 3. 消费端在拿到消息进行业务处理完毕后，根据成功或失败，返回给调用者（Broker），供其做下一步的处理。。
         */
        // 消费端注册一个监听，随时监听新消息的产生
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            /*
             * 参数msgs, context是new MessageListenerConcurrently()匿名对象
             * 在DefaultMQPushConsumer对象执行过程中被赋予的两个参数值
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                // 这里是对获取到的消息进行逻辑处理的地方
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
