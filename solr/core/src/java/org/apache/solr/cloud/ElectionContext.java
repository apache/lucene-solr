package org.apache.solr.cloud;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
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

public abstract class ElectionContext {
  
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;
  
  public ElectionContext(final String shardZkNodeName,
      final String electionPath, final String leaderPath, final ZkNodeProps leaderProps) {
    this.id = shardZkNodeName;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
  }
  
  abstract void runLeaderProcess() throws KeeperException, InterruptedException;
}

final class ShardLeaderElectionContext extends ElectionContext {
  
  private final SolrZkClient zkClient;

  public ShardLeaderElectionContext(final String shardId,
      final String collection, final String shardZkNodeName, ZkNodeProps props, SolrZkClient zkClient) {
    super(shardZkNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/leader_elect/"
        + shardId, ZkStateReader.getShardLeadersPath(collection, shardId),
        props);
    this.zkClient = zkClient;
  }

  @Override
  void runLeaderProcess() throws KeeperException, InterruptedException {
    zkClient.makePath(leaderPath,
        leaderProps == null ? null : ZkStateReader.toJSON(leaderProps),
        CreateMode.EPHEMERAL, true);
  }
}

final class OverseerElectionContext extends ElectionContext {
  
  private final SolrZkClient zkClient;
  private final ZkStateReader stateReader;

  public OverseerElectionContext(final String zkNodeName, SolrZkClient zkClient, ZkStateReader stateReader) {
    super(zkNodeName, "/overseer_elect", null, null);
    this.zkClient = zkClient;
    this.stateReader = stateReader;
  }

  @Override
  void runLeaderProcess() throws KeeperException, InterruptedException {
    new Overseer(zkClient, stateReader);
  }
  
}
