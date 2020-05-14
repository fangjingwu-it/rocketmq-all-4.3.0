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

package org.apache.rocketmq.remoting.netty;


import io.netty.util.internal.logging.InternalLogLevel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class NettyLogger {

    private static AtomicBoolean nettyLoggerSeted = new AtomicBoolean(false);
    
    private static InternalLogLevel nettyLogLevel = InternalLogLevel.ERROR;

    public static void initNettyLogger() {
        if (!nettyLoggerSeted.get()) {
            try {
                io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(new NettyBridgeLoggerFactory());
            } catch (Throwable e) {
                //ignore
            }
            nettyLoggerSeted.set(true);
        }
    }

    private static class NettyBridgeLoggerFactory extends io.netty.util.internal.logging.InternalLoggerFactory {
        @Override
        protected io.netty.util.internal.logging.InternalLogger newInstance(String s) {
            return new NettyBridgeLogger(s);
        }
    }

    private static class NettyBridgeLogger implements io.netty.util.internal.logging.InternalLogger {

        private InternalLogger logger = null;

        public NettyBridgeLogger(String name) {
            logger = InternalLoggerFactory.getLogger(name);
        }

        @Override
        public String name() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return false;
        }

        @Override
        public void trace(String s) {

        }

        @Override
        public void trace(String s, Object o) {

        }

        @Override
        public void trace(String s, Object o, Object o1) {

        }

        @Override
        public void trace(String s, Object... objects) {

        }

        @Override
        public void trace(String s, Throwable throwable) {

        }

        @Override
        public void trace(Throwable throwable) {

        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public void debug(String s) {

        }

        @Override
        public void debug(String s, Object o) {

        }

        @Override
        public void debug(String s, Object o, Object o1) {

        }

        @Override
        public void debug(String s, Object... objects) {

        }

        @Override
        public void debug(String s, Throwable throwable) {

        }

        @Override
        public void debug(Throwable throwable) {

        }

        @Override
        public boolean isInfoEnabled() {
            return false;
        }

        @Override
        public void info(String s) {

        }

        @Override
        public void info(String s, Object o) {

        }

        @Override
        public void info(String s, Object o, Object o1) {

        }

        @Override
        public void info(String s, Object... objects) {

        }

        @Override
        public void info(String s, Throwable throwable) {

        }

        @Override
        public void info(Throwable throwable) {

        }

        @Override
        public boolean isWarnEnabled() {
            return false;
        }

        @Override
        public void warn(String s) {

        }

        @Override
        public void warn(String s, Object o) {

        }

        @Override
        public void warn(String s, Object... objects) {

        }

        @Override
        public void warn(String s, Object o, Object o1) {

        }

        @Override
        public void warn(String s, Throwable throwable) {

        }

        @Override
        public void warn(Throwable throwable) {

        }

        @Override
        public boolean isErrorEnabled() {
            return false;
        }

        @Override
        public void error(String s) {

        }

        @Override
        public void error(String s, Object o) {

        }

        @Override
        public void error(String s, Object o, Object o1) {

        }

        @Override
        public void error(String s, Object... objects) {

        }

        @Override
        public void error(String s, Throwable throwable) {

        }

        @Override
        public void error(Throwable throwable) {

        }

        @Override
        public boolean isEnabled(InternalLogLevel internalLogLevel) {
            return nettyLogLevel.ordinal() <= internalLogLevel.ordinal();
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, o);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o, Object o1) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, o, o1);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object... objects) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, objects);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Throwable throwable) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, throwable);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, Throwable throwable) {

        }

    }

}
