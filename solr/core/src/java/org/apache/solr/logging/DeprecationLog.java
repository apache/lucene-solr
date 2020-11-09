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

package org.apache.solr.logging;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to log a deprecation.
 */
public class DeprecationLog {
  // not used but needed to satisfy validate-source-patterns.gradle
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String LOG_PREFIX = "org.apache.solr.DEPRECATED.";

  // featureId -> message.  Don't really need the message
  private static final Map<String, String> alreadyLogged = new ConcurrentHashMap<>();

  /**
   * Logs a deprecation warning for the provided feature, but only the first time.
   * The logger name used is {@value #LOG_PREFIX} + {@code featureId}.
   * Remember that logger names are disable-able via configuration if needed.
   * @return true if logged
   */
  public static boolean log(String featureId, String message) {
    if (alreadyLogged.putIfAbsent(featureId, message) != null) {
      return false;
    }
    Logger log = LoggerFactory.getLogger(LOG_PREFIX + featureId);
    log.warn(message);
    return true;
  }
}
