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

package org.apache.solr.servlet;

import java.lang.invoke.MethodHandles;
import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * Handles dynamic modification of during startup, before CoreContainer is created
 * <p>
 *   WARNING: This class should only be used during startup. For modifying log levels etc
 *   during runtime, SLF4J and LogWatcher must be used.
 * </p>
 */
final class StartupLoggingUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final static StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();

  /**
   * Checks whether mandatory log dir is given
   */
  static void checkLogDir() {
    if (System.getProperty("solr.log.dir") == null) {
      log.error("Missing Java Option solr.log.dir. Logging may be missing or incomplete.");
    }
  }

  /**
   * Disables all log4j ConsoleAppender's by modifying log4j configuration dynamically.
   * Must only be used during early startup
   * @return true if ok or else false if something happened, e.g. log4j classes were not in classpath
   */
  @SuppressForbidden(reason = "Legitimate log4j access")
  static boolean muteConsole() {
    try {
      if (!isLog4jActive()) {
        logNotSupported("Could not mute logging to console.");
        return false;
      }
      org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
      Enumeration appenders = rootLogger.getAllAppenders();
      while (appenders.hasMoreElements()) {
        Appender appender = (Appender) appenders.nextElement();
        if (appender instanceof ConsoleAppender) {
          log.info("Property solr.log.muteconsole given. Muting ConsoleAppender named " + appender.getName());
          rootLogger.removeAppender(appender);
        }
      }
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
  @SuppressForbidden(reason = "Legitimate log4j access")
  static boolean changeLogLevel(String logLevel) {
    try {
      if (!isLog4jActive()) {
        logNotSupported("Could not mute logging to console.");
        return false;
      }
      log.info("Log level override, property solr.log.level=" + logLevel);
      LogManager.getRootLogger().setLevel(Level.toLevel(logLevel, Level.INFO));
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
    log.warn("{} Dynamic log manipulation currently only supported for Log4j. "
        + "Please consult your logging framework of choice on how to configure the appropriate logging.", msg);
  }
}
