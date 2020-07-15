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

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

final class OverseerElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient zkClient;
  private final Overseer overseer;
  private volatile boolean isClosed = false;

  public OverseerElectionContext(final String zkNodeName, SolrZkClient zkClient, Overseer overseer) {
    super(zkNodeName, Overseer.OVERSEER_ELECT, Overseer.OVERSEER_ELECT + "/leader", new ZkNodeProps(ID, zkNodeName), zkClient);
    this.overseer = overseer;
    this.zkClient = zkClient;
  }

  @Override
  void runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException,
          InterruptedException, IOException {
    if (isClosed || zkClient.isClosed()) {
      log.info("Bailing on becoming leader, we are closed");
      return;
    }

    super.runLeaderProcess(context, weAreReplacement, pauseBeforeStartMs);

    synchronized (this) {
      if (!this.isClosed && !overseer.getZkController().getCoreContainer().isShutDown() && (overseer.getUpdaterThread() == null || !overseer.getUpdaterThread().isAlive())) {
        overseer.start(id, context);
      }
    }
  }

  public Overseer getOverseer() {
    return  overseer;
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    try (ParWork closer = new ParWork(this, true)) {
      closer.collect(() -> {
        try {
          super.cancelElection();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception closing Overseer", e);
        }
      });
      closer.collect(() -> {
        try {
          overseer.doClose();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception closing Overseer", e);
        }
      });
      closer.addCollect("overseerElectionContextCancel");
    }
  }

  @Override
  public void close() {
    this.isClosed  = true;
    try (ParWork closer = new ParWork(this, true)) {
      closer.collect(() -> {
        try {
          super.close();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception canceling election", e);
        }
      });
      closer.collect(() -> {
        try {
          overseer.doClose();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception closing Overseer", e);
        }
      });
      closer.addCollect("overseerElectionContextClose");
    }
  }

  @Override
  public ElectionContext copy() {
    return new OverseerElectionContext(id, zkClient, overseer);
  }

  @Override
  public void joinedElectionFired() {

  }

  @Override
  public void checkIfIamLeaderFired() {

  }
}

