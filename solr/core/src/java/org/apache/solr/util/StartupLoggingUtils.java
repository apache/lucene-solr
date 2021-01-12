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

package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * Handles programmatic modification of logging during startup
 * <p>
 *   WARNING: This class should only be used during startup. For modifying log levels etc
 *   during runtime, SLF4J and LogWatcher must be used.
 * </p>
 */
public final class StartupLoggingUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final static StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();

  /**
   * Checks whether mandatory log dir is given
   */
  public static void checkLogDir() {
    if (System.getProperty("solr.log.dir") == null) {
      log.error("Missing Java Option solr.log.dir. Logging may be missing or incomplete.");
    }
  }

  public static String getLoggerImplStr() { //nowarn
    return binder.getLoggerFactoryClassStr();
  }

  /**
   * Disables all log4j2 ConsoleAppender's by modifying log4j configuration dynamically.
   * Must only be used during early startup
   * @return true if ok or else false if something happened, e.g. log4j2 classes were not in classpath
   */
  @SuppressForbidden(reason = "Legitimate log4j2 access")
  public static boolean muteConsole() {
    try {
      if (!isLog4jActive()) {
        logNotSupported("Could not mute logging to console.");
        return false;
      }
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      Configuration config = ctx.getConfiguration();
      LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
      Map<String, Appender> appenders = loggerConfig.getAppenders();
      appenders.forEach((name, appender) -> {
        if (appender instanceof ConsoleAppender) {
          loggerConfig.removeAppender(name);
          ctx.updateLoggers();
        }
      });
      return true;
    } catch (Exception e) {
      logNotSupported("Could not mute logging to console.");
      return false;
    }
  }

  /**
   * Dynamically change log4j log level through property solr.log.level
   * @param logLevel String with level, should be one of the supported, e.g. TRACE, DEBUG, INFO, WARN, ERROR...
   * @return true if ok or else false if something happened, e.g. log4j classes were not in classpath
   */
  @SuppressForbidden(reason = "Legitimate log4j2 access")
  public static boolean changeLogLevel(String logLevel) {
    try {
      if (!isLog4jActive()) {
        logNotSupported("Could not change log level.");
        return false;
      }

      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      Configuration config = ctx.getConfiguration();
      LoggerConfig loggerConfig = config.getRootLogger();
      loggerConfig.setLevel(Level.toLevel(logLevel, Level.INFO));
      ctx.updateLoggers();
      return true;
    } catch (Exception e) {
      logNotSupported("Could not change log level.");
      return false;
    }
  }

  private static boolean isLog4jActive() {
    try {
      // Make sure we have log4j LogManager in classpath
      Class.forName("org.apache.log4j.LogManager");
      // Make sure that log4j is really selected as logger in slf4j - we could have LogManager in the bridge class :)
      return binder.getLoggerFactoryClassStr().contains("Log4jLoggerFactory");
    } catch (Exception e) {
      return false;
    }
  }

  private static void logNotSupported(String msg) {
    log.warn("{} Dynamic log manipulation currently only supported for Log4j. Please consult your logging framework of choice on how to configure the appropriate logging.", msg);
  }

  /**
   * Perhaps odd to put in startup utils, but this is where the logging-init code is so it seems logical to put the
   * shutdown here too.
   *
   * Tests are particularly sensitive to this call or the object release tracker will report "lmax.disruptor" not
   * terminating when asynch logging (new default as of 8.1) is enabled.
   *
   * Expert, there are rarely good reasons for this to be called outside of the test framework. If you are tempted to
   * call this for running Solr, you should probably be using synchronous logging.
   */
  @SuppressForbidden(reason = "Legitimate log4j2 access")
  public static void shutdown() {
    if (!isLog4jActive()) {
      logNotSupported("Not running log4j2, could not call shutdown for async logging.");
      return;
    }
    flushAllLoggers();
    LogManager.shutdown(true);
  }

  /**
   * This is primarily for tests to insure that log messages don't bleed from one test case to another, see:
   * SOLR-13268.
   *
   * However, if there are situations where we want to insure that all log messages for all loggers are flushed,
   * this method can be called by anyone. It should _not_ affect Solr in any way except, perhaps, a slight delay
   * while messages are being flushed.
   *
   * Expert, there are rarely good reasons for this to be called outside of the test framework. If you are tempted to
   * call this for running Solr, you should probably be using synchronous logging.
   */
  @SuppressForbidden(reason = "Legitimate log4j2 access")
  public static void flushAllLoggers() {
    if (!isLog4jActive()) {
      logNotSupported("Not running log4j2, could not call shutdown for async logging.");
      return;
    }

    final LoggerContext logCtx = ((LoggerContext) LogManager.getContext(false));
    for (final org.apache.logging.log4j.core.Logger logger : logCtx.getLoggers()) {
      for (final Appender appender : logger.getAppenders().values()) {
        if (appender instanceof AbstractOutputStreamAppender) {
          ((AbstractOutputStreamAppender) appender).getManager().flush();
        }
      }
    }
  }

  /**
   * Return a string representing the current static ROOT logging level
   * @return a string TRACE, DEBUG, WARN, ERROR or INFO representing current log level. Default is INFO
   */
  public static String getLogLevelString() {
    final Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (rootLogger.isTraceEnabled()) return "TRACE";
    else if (rootLogger.isDebugEnabled()) return "DEBUG";
    else if (rootLogger.isInfoEnabled()) return "INFO";
    else if (rootLogger.isWarnEnabled()) return "WARN";
    else if (rootLogger.isErrorEnabled()) return "ERROR";
    else return "INFO";
  }
}
