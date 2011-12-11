package org.apache.solr.cloud;

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
  final byte[] leaderProps;
  final String id;
  
  public ElectionContext(final String shardZkNodeName,
      final String electionPath, final byte[] leaderProps) {
    this.id = shardZkNodeName;
    this.electionPath = electionPath;
    this.leaderProps = leaderProps;
  }
  
}

final class ShardLeaderElectionContext extends ElectionContext {
  
  public ShardLeaderElectionContext(final String shardid,
      final String collection, final String shardZkNodeName, final byte[] props) {
    super(shardZkNodeName, "/collections/" + collection + "/leader_elect/"
        + shardid, props);
  }
  
}

final class OverseerElectionContext extends ElectionContext {
  
  public OverseerElectionContext(final String zkNodeName) {
    super(zkNodeName, "/overseer_elect", null);
  }
  
}
