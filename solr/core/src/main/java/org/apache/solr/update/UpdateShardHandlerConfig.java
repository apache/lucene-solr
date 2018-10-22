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

import org.apache.solr.client.solrj.impl.HttpClientUtil;

public class UpdateShardHandlerConfig {

  public static final String DEFAULT_METRICNAMESTRATEGY = "queryLessURLAndMethod";
  public static final int DEFAULT_MAXRECOVERYTHREADS = -1;

  public static final UpdateShardHandlerConfig DEFAULT
      = new UpdateShardHandlerConfig(HttpClientUtil.DEFAULT_MAXCONNECTIONS, HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST,
                                     HttpClientUtil.DEFAULT_SO_TIMEOUT, HttpClientUtil.DEFAULT_CONNECT_TIMEOUT,
                                      DEFAULT_METRICNAMESTRATEGY, DEFAULT_MAXRECOVERYTHREADS);

  private final int maxUpdateConnections;

  private final int maxUpdateConnectionsPerHost;

  private final int distributedSocketTimeout;

  private final int distributedConnectionTimeout;

  private final String metricNameStrategy;

  private final int maxRecoveryThreads;

  public UpdateShardHandlerConfig(int maxUpdateConnections, int maxUpdateConnectionsPerHost, int distributedSocketTimeout, int distributedConnectionTimeout,
                                  String metricNameStrategy, int maxRecoveryThreads) {
    this.maxUpdateConnections = maxUpdateConnections;
    this.maxUpdateConnectionsPerHost = maxUpdateConnectionsPerHost;
    this.distributedSocketTimeout = distributedSocketTimeout;
    this.distributedConnectionTimeout = distributedConnectionTimeout;
    this.metricNameStrategy = metricNameStrategy;
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

  public int getMaxRecoveryThreads() {
    return maxRecoveryThreads;
  }
}
