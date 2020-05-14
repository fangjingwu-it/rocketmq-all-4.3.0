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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

/**
 * namesrv启动类入口
 *
 * NameServer是一个简单的Topic路由注册中心，支持Broker的动态注册与发现。主要包括两个功能：
 * Broker管理：NameServer接受Broker集群的注册信息并且保存下来作为路由信息的基本数据。然后提供心跳检测机制，检查Broker是否还存活；
 * topic路由信息管理：每个NameServer将保存关于Broker集群的整个路由信息和用于客户端查询的队列信息。
 *                 然后Producer和Conumser通过NameServer就可以知道整个Broker集群的路由信息，从而进行消息的投递和消费
 */
public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    /**
     * 启动入口main
     */
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {
            // 加载nameServer配置文件，里面会创建NamesrvConfig，NettyServerConfig，NamesrvController对象
            NamesrvController controller = createNamesrvController(args);

            // 启动，初始化nameServer
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {

        // 设置Rcoker当前的版本到系统属性中
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        /*
         * Options对象表示命令行中可选参数的集合，其中描述命令行可能的选项。
         *
         * 构造命令行解析Options对象, 这个过程已经设置了h(help)、n(namesrvAddr)两个参数
         */
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        /*
         * parseCmdLine()方法会对mqbroker启动命令进行解析(*nix系统), 比如: nohup ./mqbroker -n 192.168.2.1:9876
         *
         * buildCommandlineOptions方法配置c(configFile), p(printConfigItem), m(printImportantConfig)参数，如果解析的commandLine==null, 则退出
         */
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        // 初始化NameSrvConfig,主要包含kvConfig路径，mq路径等
        final NamesrvConfig namesrvConfig = new NamesrvConfig();

        // 实例化NettyServerConfig并设置该对象的参数
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);

        // 如果启动NameServer的命令行中有参数 -c （比如: nohup ./mqbroker -n 192.168.2.1:9876  这里没有）
        // -c 指定属性配置文件的位置
        if (commandLine.hasOption('c')) {
            // 获取此项的参数 -c 后面跟的值
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();

                // 将broker.conf配置文件的内容按行读到properties对象中去
                properties.load(in);

                // 将properties转换成namesrvConfig对象
                MixAll.properties2Object(properties, namesrvConfig);
                // 将properties转换成nettyServerConfig对象
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        // 如果commandLine中有p选项，则打印所有配置信息
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(null, namesrvConfig);
            MixAll.printObjectProperties(null, nettyServerConfig);
            System.exit(0);
        }

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 初始化LogBack配置文件，生成Log对象并注入导对应的对象中去
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        // 创建NamesrvController对象
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // 将配置文件信息，记录到Configuration对象（和broker使用的是同一个配置对象）中，NamesrvStartup.properties中记录了所有配置文件的数据
        controller.getConfiguration().registerConfig(NamesrvStartup.properties);

        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 初始化NamesrvController对象
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        // 退出时执行该方法
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));

        // 启动nettyServer
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
