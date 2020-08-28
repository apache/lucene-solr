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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Pair;
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
    if (isClosed() || !zkClient.isConnected()) {
      return;
    }

    // TODO: the idea here is that we could clear the Overseer queue
    // if we knew we are the first Overseer in a cluster startup
    // disable until there is more testing in real world vs tests
//    if (!weAreReplacement) {
//      // kills the queues
//      ZkDistributedQueue queue = new ZkDistributedQueue(
//          overseer.getZkController().getZkStateReader().getZkClient(),
//          "/overseer/queue", new Stats(), 0, new ConnectionManager.IsClosed() {
//        public boolean isClosed() {
//          return overseer.isClosed() || overseer.getZkController()
//              .getCoreContainer().isShutDown();
//        }
//      });
//      clearQueue(queue);
//      clearQueue(Overseer.getInternalWorkQueue(zkClient, new Stats()));
//    }

    super.runLeaderProcess(context, weAreReplacement, pauseBeforeStartMs);

    synchronized (this) {
      if (isClosed()) {
        log.info("Bailing on becoming leader, we are closed");
        return;
      }
      if (!isClosed() && !overseer.getZkController().getCoreContainer().isShutDown() && !overseer.isDone() && (overseer.getUpdaterThread() == null || !overseer.getUpdaterThread().isAlive())) {
        try {
          overseer.start(id, context);
        } finally {
          if (isClosed()) {
            overseer.close();
          }
        }
      }
    }
  }

  private void clearQueue(ZkDistributedQueue queue)
      throws KeeperException, InterruptedException {
    while (true) {
      Collection<Pair<String,byte[]>> items = queue.peekElements(1000, 0, null);
      List<String> paths = new ArrayList<>(items.size());
      if (items.size() == 0) {
        break;
      }
      for (Pair<String, byte[]> item : items) {
        paths.add(item.first());
      }
      queue.remove(paths);
    }
  }

  public Overseer getOverseer() {
    return  overseer;
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    try (ParWork closer = new ParWork(this, true)) {
      if (zkClient.isConnected()) {
        closer.collect("cancelElection", () -> {
          try {
            super.cancelElection();
          } catch (Exception e) {
            ParWork.propegateInterrupt(e);
            log.error("Exception closing Overseer", e);
          }
        });
      }
      closer.collect("overseer", () -> {
        try {
          overseer.doClose();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception closing Overseer", e);
        }
      });
    }
  }

  @Override
  public void close() {
    this.isClosed  = true;
    try (ParWork closer = new ParWork(this, true)) {
      closer.collect("superClose", () -> {
        try {
          super.close();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception canceling election", e);
        }
      });
      closer.collect("Overseer", () -> {
        try {
          cancelElection();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception closing Overseer", e);
        }
      });
    }
  }

  @Override
  public void joinedElectionFired() {

  }

  @Override
  public void checkIfIamLeaderFired() {

  }

  @Override
  public boolean isClosed() {
    return isClosed || overseer.getCoreContainer().isShutDown() || zkClient.isClosed() || overseer.getCoreContainer().getZkController().isClosed();
  }
}

