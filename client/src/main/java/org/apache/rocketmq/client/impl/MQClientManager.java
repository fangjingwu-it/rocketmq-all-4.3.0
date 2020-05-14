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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * 客户端实例工厂
 * 单例模式，负责生产/消费端的MQClientInstance对象管理：整个JVM实例中只存在一个MQClientManager实例
 */
public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    private static MQClientManager instance = new MQClientManager();

    /**
     * 用来生成MQClientInstance的id，每个递增
     */
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * 维护一个MQClientlnstance缓存表, 同一个clientld只会创建一个MQClientinstance
     *
     * MQClientManager通过id将多个MQClientInstance对象放在一个列表中。MQClientManager的id命名规范是ip@instanceName@unitName
     *
     * MQClientInstance通过组名来管理多个生产者与消费者，即一个分组只能用一个生产者与消费者
     */
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    /**
     * @description: 创建MQClientInstance实例
     * @param: 就是传进来的DefaultMQProducer对象
     * @return:
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {

       /*
        * clientld为客户端IP + （ClientConfig.instanceName:取值为System.getProperty("rocketmq.client.name", "DEFAULT")） + (unitname可选)
        *
        * 如果在同一台物理服务器部署两个应用程序，应用程序 岂不是 clientld相同，会造成混乱，解决：
        *     为了避免这个问题，如果instance为默认值DEFAULT的话，RocketMQ会自动将instance设置为进程ID这样避免了不同进程的相互影响，
        *     但同一个NM中的不同消费者和不同生产者在启动时获取到的MQClientlnstane实例都是同一个。根据后面的介绍，MQClientlnstance
        *     封装了RocketMQ网络处理API 是消息生产者( Producer)、消息消费者 (Consumer)与 NameServer、 Broker打交道的网络通道。
        */
        String clientId = clientConfig.buildMQClientId();

        // 判断缓存表中clientId有没有对应的MQClientInstance实例，没有就创建一个
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
