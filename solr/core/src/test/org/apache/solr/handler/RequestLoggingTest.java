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
package org.apache.solr.handler;

import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.SolrCore;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressForbidden(reason = "test is specific to log4j2")
public class RequestLoggingTest extends SolrTestCaseJ4 {
  private StringWriter writer;
  private Appender appender;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void setupAppender() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    LoggerConfig config = ctx.getConfiguration().getLoggerConfig("RequestLoggingTest");

    writer = new StringWriter();
    appender = WriterAppender.createAppender(
      PatternLayout
        .newBuilder()
        .withPattern("%-5p [%t]: %m%n")
        .build(),
        null, writer, "RequestLoggingTest", false, true);
    appender.start();

  }

  @Test
  public void testLogBeforeExecuteWithCoreLogger() {
    Logger logger = LogManager.getLogger(SolrCore.class);
    testLogBeforeExecute(logger);
  }

  @Test
  public void testLogBeforeExecuteWithRequestLogger() {
    Logger logger = LogManager.getLogger("org.apache.solr.core.SolrCore.Request");
    testLogBeforeExecute(logger);
  }

  public void testLogBeforeExecute(Logger logger) {
    Level level = logger.getLevel();

    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    LoggerConfig config = ctx.getConfiguration().getLoggerConfig(logger.getName());
    config.setLevel(Level.DEBUG);
    config.addAppender(appender, Level.DEBUG, null);
    ctx.updateLoggers();

    try {
      assertQ(req("q", "*:*"));

      String output = writer.toString();
      Matcher matcher = Pattern.compile("DEBUG.*q=\\*:\\*.*").matcher(output);
      assertTrue(matcher.find());
      final String group = matcher.group();
      final String msg = "Should not have post query information";
      assertFalse(msg, group.contains("hits"));
      assertFalse(msg, group.contains("status"));
      assertFalse(msg, group.contains("QTime"));
    } finally {
      config.setLevel(level);
      config.removeAppender(appender.getName());
      ctx.updateLoggers();
    }
  }
}
