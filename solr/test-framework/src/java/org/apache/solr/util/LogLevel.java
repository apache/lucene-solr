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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

  @SuppressForbidden(reason="Specific to Log4J")
  public static class Configurer {

    private static Map<String, Level> parseFrom(String input) {
      Map<String, Level> testlevels = new HashMap<>();
      for (String levelSetting : input.split(";")) {
        String[] parts = levelSetting.split("=");
        testlevels.put(parts[0], Level.toLevel(parts[1]));
      }
      return testlevels;
    }

    private static String levelAsString(Level level) {
      return level == null ? null : level.toString();
    }

    private static Level parseLevel(String level) {
      return level == null ? null : Level.toLevel(level);
    }

    public static void restoreLogLevels(Map<String, String> savedLogLevels) {
      savedLogLevels.forEach((name, level) -> {
        Logger logger = Logger.getLogger(name);
        logger.setLevel(parseLevel(level));
      });
    }

    public static Map<String, String> setLevels(String value) {
      Map<String, String> oldLevels = new HashMap<>();
      parseFrom(value).forEach((name, level) -> {
        Logger logger = Logger.getLogger(name);
        oldLevels.put(name, levelAsString(logger.getLevel()));
        logger.setLevel(level);
      });
      return oldLevels;
    }
  }

}
