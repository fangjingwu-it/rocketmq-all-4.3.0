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

package org.apache.rocketmq.remoting;

/**
 * 各个服务之间的通讯则使用的 remoting 模块
 */
public interface RemotingService {

    /**
     * 开启服务
     */
    void start();

    /**
     * 关闭服务
     */
    void shutdown();

    /**
     * 注册 hook（可以在远程通信之前和通信之后，执行用户自定的一些处理。类似前置处理器和后置处理器）
     */
    void registerRPCHook(RPCHook rpcHook);
}
