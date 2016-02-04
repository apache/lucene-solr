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
package org.apache.solr.core;

import org.apache.solr.common.SolrException;

public class CloudConfig {

  private final String zkHost;

  private final int zkClientTimeout;

  private final int hostPort;

  private final String hostName;

  private final String hostContext;

  private final boolean useGenericCoreNames;

  private final int leaderVoteWait;

  private final int leaderConflictResolveWait;

  private final int autoReplicaFailoverWaitAfterExpiration;

  private final int autoReplicaFailoverWorkLoopDelay;

  private final int autoReplicaFailoverBadNodeExpiration;

  private final String zkCredentialsProviderClass;

  private final String zkACLProviderClass;

  CloudConfig(String zkHost, int zkClientTimeout, int hostPort, String hostName, String hostContext, boolean useGenericCoreNames, int leaderVoteWait, int leaderConflictResolveWait, int autoReplicaFailoverWaitAfterExpiration, int autoReplicaFailoverWorkLoopDelay, int autoReplicaFailoverBadNodeExpiration, String zkCredentialsProviderClass, String zkACLProviderClass) {
    this.zkHost = zkHost;
    this.zkClientTimeout = zkClientTimeout;
    this.hostPort = hostPort;
    this.hostName = hostName;
    this.hostContext = hostContext;
    this.useGenericCoreNames = useGenericCoreNames;
    this.leaderVoteWait = leaderVoteWait;
    this.leaderConflictResolveWait = leaderConflictResolveWait;
    this.autoReplicaFailoverWaitAfterExpiration = autoReplicaFailoverWaitAfterExpiration;
    this.autoReplicaFailoverWorkLoopDelay = autoReplicaFailoverWorkLoopDelay;
    this.autoReplicaFailoverBadNodeExpiration = autoReplicaFailoverBadNodeExpiration;
    this.zkCredentialsProviderClass = zkCredentialsProviderClass;
    this.zkACLProviderClass = zkACLProviderClass;

    if (this.hostPort == -1)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'hostPort' must be configured to run SolrCloud");
    if (this.hostContext == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'hostContext' must be configured to run SolrCloud");
  }

  public String getZkHost() {
    return zkHost;
  }

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  public int getSolrHostPort() {
    return hostPort;
  }

  public String getSolrHostContext() {
    return hostContext;
  }

  public String getHost() {
    return hostName;
  }

  public String getZkCredentialsProviderClass() {
    return zkCredentialsProviderClass;
  }

  public String getZkACLProviderClass() {
    return zkACLProviderClass;
  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public int getLeaderConflictResolveWait() {
    return leaderConflictResolveWait;
  }

  public int getAutoReplicaFailoverWaitAfterExpiration() {
    return autoReplicaFailoverWaitAfterExpiration;
  }

  public int getAutoReplicaFailoverWorkLoopDelay() {
    return autoReplicaFailoverWorkLoopDelay;
  }

  public int getAutoReplicaFailoverBadNodeExpiration() {
    return autoReplicaFailoverBadNodeExpiration;
  }

  public boolean getGenericCoreNodeNames() {
    return useGenericCoreNames;
  }

  public static class CloudConfigBuilder {

    private static final int DEFAULT_ZK_CLIENT_TIMEOUT = 15000;
    private static final int DEFAULT_LEADER_VOTE_WAIT = 180000;  // 3 minutes
    private static final int DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT = 180000;

    // TODO: tune defaults
    private static final int DEFAULT_AUTO_REPLICA_FAILOVER_WAIT_AFTER_EXPIRATION = 30000;
    private static final int DEFAULT_AUTO_REPLICA_FAILOVER_WORKLOOP_DELAY = 10000;
    private static final int DEFAULT_AUTO_REPLICA_FAILOVER_BAD_NODE_EXPIRATION = 60000;

    private String zkHost = System.getProperty("zkHost");
    private int zkClientTimeout = Integer.getInteger("zkClientTimeout", DEFAULT_ZK_CLIENT_TIMEOUT);
    private final int hostPort;
    private final String hostName;
    private final String hostContext;
    private boolean useGenericCoreNames;
    private int leaderVoteWait = DEFAULT_LEADER_VOTE_WAIT;
    private int leaderConflictResolveWait = DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT;
    private int autoReplicaFailoverWaitAfterExpiration = DEFAULT_AUTO_REPLICA_FAILOVER_WAIT_AFTER_EXPIRATION;
    private int autoReplicaFailoverWorkLoopDelay = DEFAULT_AUTO_REPLICA_FAILOVER_WORKLOOP_DELAY;
    private int autoReplicaFailoverBadNodeExpiration = DEFAULT_AUTO_REPLICA_FAILOVER_BAD_NODE_EXPIRATION;
    private String zkCredentialsProviderClass;
    private String zkACLProviderClass;

    public CloudConfigBuilder(String hostName, int hostPort) {
      this(hostName, hostPort, null);
    }

    public CloudConfigBuilder(String hostName, int hostPort, String hostContext) {
      this.hostName = hostName;
      this.hostPort = hostPort;
      this.hostContext = hostContext;
    }

    public CloudConfigBuilder setZkHost(String zkHost) {
      this.zkHost = zkHost;
      return this;
    }

    public CloudConfigBuilder setZkClientTimeout(int zkClientTimeout) {
      this.zkClientTimeout = zkClientTimeout;
      return this;
    }

    public CloudConfigBuilder setUseGenericCoreNames(boolean useGenericCoreNames) {
      this.useGenericCoreNames = useGenericCoreNames;
      return this;
    }

    public CloudConfigBuilder setLeaderVoteWait(int leaderVoteWait) {
      this.leaderVoteWait = leaderVoteWait;
      return this;
    }

    public CloudConfigBuilder setLeaderConflictResolveWait(int leaderConflictResolveWait) {
      this.leaderConflictResolveWait = leaderConflictResolveWait;
      return this;
    }

    public CloudConfigBuilder setAutoReplicaFailoverWaitAfterExpiration(int autoReplicaFailoverWaitAfterExpiration) {
      this.autoReplicaFailoverWaitAfterExpiration = autoReplicaFailoverWaitAfterExpiration;
      return this;
    }

    public CloudConfigBuilder setAutoReplicaFailoverWorkLoopDelay(int autoReplicaFailoverWorkLoopDelay) {
      this.autoReplicaFailoverWorkLoopDelay = autoReplicaFailoverWorkLoopDelay;
      return this;
    }

    public CloudConfigBuilder setAutoReplicaFailoverBadNodeExpiration(int autoReplicaFailoverBadNodeExpiration) {
      this.autoReplicaFailoverBadNodeExpiration = autoReplicaFailoverBadNodeExpiration;
      return this;
    }

    public CloudConfigBuilder setZkCredentialsProviderClass(String zkCredentialsProviderClass) {
      this.zkCredentialsProviderClass = zkCredentialsProviderClass;
      return this;
    }

    public CloudConfigBuilder setZkACLProviderClass(String zkACLProviderClass) {
      this.zkACLProviderClass = zkACLProviderClass;
      return this;
    }

    public CloudConfig build() {
      return new CloudConfig(zkHost, zkClientTimeout, hostPort, hostName, hostContext, useGenericCoreNames, leaderVoteWait, leaderConflictResolveWait, autoReplicaFailoverWaitAfterExpiration, autoReplicaFailoverWorkLoopDelay, autoReplicaFailoverBadNodeExpiration, zkCredentialsProviderClass, zkACLProviderClass);
    }
  }
}
