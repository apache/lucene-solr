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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Annotation specifying the log level for a particular test case or method
 *
 * Log levels are set for different classes by passing a configuration string
 * to the annotation, like this:
 * <code>
 *   {@literal @}LogLevel("org.apache.solr=DEBUG;org.apache.solr.core=INFO")
 * </code>
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface LogLevel {

  /**
   * A log-level definition string
   */
  public String value();

  @SuppressForbidden(reason="Specific to Log4J2")
  public static class Configurer {

    private static Map<String, Level> parseFrom(String input) {
      Map<String, Level> testlevels = new HashMap<>();
      for (String levelSetting : input.split(";")) {
        String[] parts = levelSetting.split("=");
        testlevels.put(parts[0], parseLevel(parts[1]));
      }
      return testlevels;
    }

    private static Level parseLevel(String level) {
      return level == null ? null : Level.toLevel(level);
    }

    public static void restoreLogLevels(Map<String, Level> savedLogLevels) {
      setLogLevels(savedLogLevels);
    }

    public static Map<String, Level> setLevels(String value) {
      return setLogLevels(parseFrom(value));
    }

    private static Map<String, Level> setLogLevels(Map<String, Level> logLevels) {
      LoggerContext ctx = LoggerContext.getContext(false);
      Configuration config = ctx.getConfiguration();

      Map<String, Level> oldLevels = new HashMap<>();
      logLevels.forEach((loggerName, level) -> {
        LoggerConfig logConfig = config.getLoggerConfig(loggerName);
        // what the initial logger level was. It will use the root value if logger is being defined for the first time
        oldLevels.put(loggerName, logConfig.getLevel());
        if (loggerName.equals(logConfig.getName())) {
          logConfig.setLevel(level);
        } else {
          LoggerConfig loggerConfig = new LoggerConfig(loggerName, level, true);
          loggerConfig.setLevel(level);
          config.addLogger(loggerName, loggerConfig);
        }
      });
      ctx.updateLoggers();
      return oldLevels;
    }

  }

}
