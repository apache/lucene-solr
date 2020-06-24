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
package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ElectionContext implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;
  volatile String leaderSeqPath;
  private SolrZkClient zkClient;

  public ElectionContext(final String coreNodeName,
      final String electionPath, final String leaderPath, final ZkNodeProps leaderProps, final SolrZkClient zkClient) {
    assert zkClient != null;
    this.id = coreNodeName;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
    this.zkClient = zkClient;
  }
  
  public void close() {

  }
  
  public void cancelElection() throws InterruptedException, KeeperException {
    if (leaderSeqPath != null) {
      try {
        log.debug("Canceling election {}", leaderSeqPath);
        zkClient.delete(leaderSeqPath, -1, true);
      } catch (NoNodeException e) {
        // fine
        log.debug("cancelElection did not find election node to remove {}", leaderSeqPath);
      }
    } else {
      log.debug("cancelElection skipped as this context has not been initialized");
    }
  }

  abstract void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException, InterruptedException, IOException;

  public void checkIfIamLeaderFired() {}

  public void joinedElectionFired() {}

  public  ElectionContext copy(){
    throw new UnsupportedOperationException("copy");
  }
}


