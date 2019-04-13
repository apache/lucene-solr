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

package org.apache.lucene.luke.util;

import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.lucene.luke.app.desktop.util.TextAreaAppender;

/**
 * Logger factory. This programmatically configurates logger context (Appenders etc.)
 */
public class LoggerFactory {

  public static void initGuiLogging(String logFile) {
    ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
    builder.add(builder.newRootLogger(Level.INFO));
    LoggerContext context = Configurator.initialize(builder.build());

    PatternLayout layout = PatternLayout.newBuilder()
        .withPattern("[%d{ISO8601}] %5p (%F:%L) - %m%n")
        .withCharset(StandardCharsets.UTF_8)
        .build();

    Appender fileAppender = FileAppender.newBuilder()
        .setName("File")
        .setLayout(layout)
        .withFileName(logFile)
        .withAppend(false)
          .build();
    fileAppender.start();

    Appender textAreaAppender = TextAreaAppender.newBuilder()
        .setName("TextArea")
        .setLayout(layout)
        .build();
    textAreaAppender.start();

    context.getRootLogger().addAppender(fileAppender);
    context.getRootLogger().addAppender(textAreaAppender);
    context.updateLoggers();
  }

  public static Logger getLogger(Class<?> clazz) {
    return LogManager.getLogger(clazz);
  }

}
