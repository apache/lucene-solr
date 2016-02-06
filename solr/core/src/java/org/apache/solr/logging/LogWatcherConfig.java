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

/**
 * Defines the configuration of a {@link LogWatcher}
 */
public class LogWatcherConfig {

  private final boolean enabled;
  private final String loggingClass;
  private final int watcherSize;
  private final String watcherThreshold;

  public LogWatcherConfig(boolean enabled, String loggingClass, String watcherThreshold, int watcherSize) {
    this.enabled = enabled;
    this.loggingClass = loggingClass;
    this.watcherThreshold = watcherThreshold;
    this.watcherSize = watcherSize;
  }

  /**
   * @return true if the LogWatcher is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Get the implementation of the LogWatcher to use.  May be "JUL" or "log4j" for the default
   * java.util.logging or log4j implementations, or the fully-qualified name of a class extending
   * {@link LogWatcher}.
   * @return the LogWatcher class to use
   */
  public String getLoggingClass() {
    return loggingClass;
  }

  /**
   * @return the size of the LogWatcher queue
   */
  public int getWatcherSize() {
    return watcherSize;
  }

  /**
   * @return the threshold above which logging events will be recorded
   */
  public String getWatcherThreshold() {
    return watcherThreshold;
  }

  /**
   * @return a {@link ListenerConfig} object using this config's settings.
   */
  public ListenerConfig asListenerConfig() {
    return new ListenerConfig(watcherSize, watcherThreshold);
  }
}
