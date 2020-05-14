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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于拉取消息的请求：一个topic@queueId对应一个ManyPullRequest实体，一个ManyPullRequest实体中又封装了很多个PullRequest请求
 */
public class ManyPullRequest {
    private final ArrayList<PullRequest> pullRequestList = new ArrayList<>();

    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }

    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }

    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {

            // ArrayList.clone() API学习：其是浅复制，即复制前后的两个arrayList变量指示内存中的地址是不一样的，但是变量中的元素指向同一个元素
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();

            // 克隆完以后。清除之前的那个arrayList变量中的内容
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }
}
