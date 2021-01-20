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
      final LoggerContext ctx = LoggerContext.getContext(false);
      final Configuration config = ctx.getConfiguration();

      final Map<String, Level> oldLevels = new HashMap<>();
      logLevels.forEach((loggerName, newLevel) -> {
        final LoggerConfig logConfig = config.getLoggerConfig(loggerName);
        if (loggerName.equals(logConfig.getName())) {
          // we have an existing LoggerConfig for this specific loggerName
          // record the existing 'old' level...
          oldLevels.put(loggerName, logConfig.getLevel());
          // ...and set the new one (or remove if null) ...
          if (null == newLevel) {
            config.removeLogger(loggerName);
          } else {
            logConfig.setLevel(newLevel);
          }
        } else {
          // there is no existing configuration for the exact loggerName, logConfig is some ancestor
          // record an 'old' level of 'null' to track the lack of any configured level...
          oldLevels.put(loggerName, null);
          // ...and now create a new logger config wih our new level
          final LoggerConfig newLoggerConfig = new LoggerConfig(loggerName, newLevel, true);
          config.addLogger(loggerName, newLoggerConfig);
        }

        assert oldLevels.containsKey(loggerName);
      });
      ctx.updateLoggers();
      return oldLevels;
    }

  }

}
