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
package org.apache.solr.update;

public class UpdateShardHandlerConfig {

  public static final int DEFAULT_DISTRIBUPDATECONNTIMEOUT = 60000;
  public static final int DEFAULT_DISTRIBUPDATESOTIMEOUT = 600000;
  public static final int DEFAULT_MAXUPDATECONNECTIONS = 100000;
  public static final int DEFAULT_MAXUPDATECONNECTIONSPERHOST = 100000;
  public static final int DEFAULT_UPDATECONNECTIONSEVICTORSLEEPDELAY = 5000;
  public static final int DEFAULT_MAXUPDATECONNECTIONIDLETIME = 40000;
  public static final String DEFAULT_METRICNAMESTRATEGY = "queryLessURLAndMethod";
  public static final int DEFAULT_MAXRECOVERYTHREADS = -1;

  public static final UpdateShardHandlerConfig DEFAULT
      = new UpdateShardHandlerConfig(DEFAULT_MAXUPDATECONNECTIONS, DEFAULT_MAXUPDATECONNECTIONSPERHOST,
                                     DEFAULT_DISTRIBUPDATESOTIMEOUT, DEFAULT_DISTRIBUPDATECONNTIMEOUT,
                                      DEFAULT_UPDATECONNECTIONSEVICTORSLEEPDELAY, DEFAULT_MAXUPDATECONNECTIONIDLETIME,
                                      DEFAULT_METRICNAMESTRATEGY, DEFAULT_MAXRECOVERYTHREADS);

  private final int maxUpdateConnections;

  private final int maxUpdateConnectionsPerHost;

  private final int distributedSocketTimeout;

  private final int distributedConnectionTimeout;

  private final String metricNameStrategy;

  private final int updateConnectionsEvictorSleepDelay;

  private final int maxUpdateConnectionIdleTime;

  private final int maxRecoveryThreads;
  
  public UpdateShardHandlerConfig(int maxUpdateConnections, int maxUpdateConnectionsPerHost,
      int distributedSocketTimeout, int distributedConnectionTimeout, int updateConnectionsEvictorSleepDelay,
      int maxUpdateConnectionIdleTime, String metricNameStrategy, int maxRecoveryThreads) {
    this.maxUpdateConnections = maxUpdateConnections;
    this.maxUpdateConnectionsPerHost = maxUpdateConnectionsPerHost;
    this.distributedSocketTimeout = distributedSocketTimeout;
    this.distributedConnectionTimeout = distributedConnectionTimeout;
    this.metricNameStrategy = metricNameStrategy;
    this.updateConnectionsEvictorSleepDelay = updateConnectionsEvictorSleepDelay;
    this.maxUpdateConnectionIdleTime = maxUpdateConnectionIdleTime;
    this.maxRecoveryThreads = maxRecoveryThreads;
  }

  public int getMaxUpdateConnectionsPerHost() {
    return maxUpdateConnectionsPerHost;
  }

  public int getMaxUpdateConnections() {
    return maxUpdateConnections;
  }

  public int getDistributedSocketTimeout() {
    return distributedSocketTimeout;
  }

  public int getDistributedConnectionTimeout() {
    return distributedConnectionTimeout;
  }

  public String getMetricNameStrategy() {
    return metricNameStrategy;
  }

  public int getUpdateConnectionsEvictorSleepDelay() {
    return updateConnectionsEvictorSleepDelay;
  }

  public int getMaxUpdateConnectionIdleTime() {
    return maxUpdateConnectionIdleTime;
  }
  
  public int getMaxRecoveryThreads() {
    return maxRecoveryThreads;
  }
}
