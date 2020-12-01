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

  private final String zkCredentialsProviderClass;

  private final String zkACLProviderClass;

  private final int createCollectionWaitTimeTillActive;

  private final boolean createCollectionCheckLeaderActive;

  private final String pkiHandlerPrivateKeyPath;

  private final String pkiHandlerPublicKeyPath;

  CloudConfig(String zkHost, int zkClientTimeout, int hostPort, String hostName, String hostContext, boolean useGenericCoreNames,
              int leaderVoteWait, int leaderConflictResolveWait, String zkCredentialsProviderClass, String zkACLProviderClass,
              int createCollectionWaitTimeTillActive, boolean createCollectionCheckLeaderActive, String pkiHandlerPrivateKeyPath,
              String pkiHandlerPublicKeyPath) {
    this.zkHost = zkHost;
    this.zkClientTimeout = zkClientTimeout;
    this.hostPort = hostPort;
    this.hostName = hostName;
    this.hostContext = hostContext;
    this.useGenericCoreNames = useGenericCoreNames;
    this.leaderVoteWait = leaderVoteWait;
    this.leaderConflictResolveWait = leaderConflictResolveWait;
    this.zkCredentialsProviderClass = zkCredentialsProviderClass;
    this.zkACLProviderClass = zkACLProviderClass;
    this.createCollectionWaitTimeTillActive = createCollectionWaitTimeTillActive;
    this.createCollectionCheckLeaderActive = createCollectionCheckLeaderActive;
    this.pkiHandlerPrivateKeyPath = pkiHandlerPrivateKeyPath;
    this.pkiHandlerPublicKeyPath = pkiHandlerPublicKeyPath;

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

  public boolean getGenericCoreNodeNames() {
    return useGenericCoreNames;
  }

  public int getCreateCollectionWaitTimeTillActive() {
    return createCollectionWaitTimeTillActive;
  }

  public boolean isCreateCollectionCheckLeaderActive() {
    return createCollectionCheckLeaderActive;
  }

  public String getPkiHandlerPrivateKeyPath() {
    return pkiHandlerPrivateKeyPath;
  }

  public String getPkiHandlerPublicKeyPath() {
    return pkiHandlerPublicKeyPath;
  }

  public static class CloudConfigBuilder {

    private static final int DEFAULT_ZK_CLIENT_TIMEOUT = 45000;
    private static final int DEFAULT_LEADER_VOTE_WAIT = 180000;  // 3 minutes
    private static final int DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT = 180000;
    private static final int DEFAULT_CREATE_COLLECTION_ACTIVE_WAIT = 45;  // 45 seconds
    private static final boolean DEFAULT_CREATE_COLLECTION_CHECK_LEADER_ACTIVE = false;

    private String zkHost;
    private int zkClientTimeout = Integer.getInteger("zkClientTimeout", DEFAULT_ZK_CLIENT_TIMEOUT);
    private final int hostPort;
    private final String hostName;
    private final String hostContext;
    private boolean useGenericCoreNames;
    private int leaderVoteWait = DEFAULT_LEADER_VOTE_WAIT;
    private int leaderConflictResolveWait = DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT;
    private String zkCredentialsProviderClass;
    private String zkACLProviderClass;
    private int createCollectionWaitTimeTillActive = DEFAULT_CREATE_COLLECTION_ACTIVE_WAIT;
    private boolean createCollectionCheckLeaderActive = DEFAULT_CREATE_COLLECTION_CHECK_LEADER_ACTIVE;
    private String pkiHandlerPrivateKeyPath;
    private String pkiHandlerPublicKeyPath;

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
    
    public CloudConfigBuilder setZkCredentialsProviderClass(String zkCredentialsProviderClass) {
      this.zkCredentialsProviderClass = zkCredentialsProviderClass;
      return this;
    }

    public CloudConfigBuilder setZkACLProviderClass(String zkACLProviderClass) {
      this.zkACLProviderClass = zkACLProviderClass;
      return this;
    }

    public CloudConfigBuilder setCreateCollectionWaitTimeTillActive(int createCollectionWaitTimeTillActive) {
      this.createCollectionWaitTimeTillActive = createCollectionWaitTimeTillActive;
      return this;
    }

    public CloudConfigBuilder setCreateCollectionCheckLeaderActive(boolean createCollectionCheckLeaderActive) {
      this.createCollectionCheckLeaderActive = createCollectionCheckLeaderActive;
      return this;
    }

    public CloudConfigBuilder setPkiHandlerPrivateKeyPath(String pkiHandlerPrivateKeyPath) {
      this.pkiHandlerPrivateKeyPath = pkiHandlerPrivateKeyPath;
      return this;
    }

    public CloudConfigBuilder setPkiHandlerPublicKeyPath(String pkiHandlerPublicKeyPath) {
      this.pkiHandlerPublicKeyPath = pkiHandlerPublicKeyPath;
      return this;
    }

    public CloudConfig build() {
      return new CloudConfig(zkHost, zkClientTimeout, hostPort, hostName, hostContext, useGenericCoreNames, leaderVoteWait,
          leaderConflictResolveWait, zkCredentialsProviderClass, zkACLProviderClass, createCollectionWaitTimeTillActive,
          createCollectionCheckLeaderActive, pkiHandlerPrivateKeyPath, pkiHandlerPublicKeyPath);
    }
  }
}
