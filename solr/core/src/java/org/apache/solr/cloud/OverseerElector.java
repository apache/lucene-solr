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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overseer Elector.
 */
public class OverseerElector extends LeaderElector {
  private final SolrZkClient client;
  private final ZkStateReader reader;
  private static Logger log = LoggerFactory.getLogger(OverseerElector.class);
  
  public OverseerElector(SolrZkClient client, ZkStateReader stateReader) {
    super(client);
    this.client = client;
    this.reader = stateReader;
  }
  
  @Override
  protected void runIamLeaderProcess(ElectionContext context) {
    try {
      new Overseer(client, reader);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.SESSIONEXPIRED
          || e.code() == KeeperException.Code.CONNECTIONLOSS) {
        log.warn("Cannot run overseer leader process, Solr cannot talk to ZK");
        return;
      }
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Could not run leader process", e);
    }
  }
  
}
