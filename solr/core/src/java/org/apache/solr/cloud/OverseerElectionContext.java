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

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

final class OverseerElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient zkClient;
  private final Overseer overseer;

  public OverseerElectionContext(final String zkNodeName, SolrZkClient zkClient, Overseer overseer) {
    super(zkNodeName, Overseer.OVERSEER_ELECT, Overseer.OVERSEER_ELECT + "/leader", new Replica("overseer:" + overseer.getZkController().getNodeName(), getIDMap(zkNodeName, overseer), "overseer", -1l, "overseer", overseer.getZkStateReader()), null, zkClient);
    this.overseer = overseer;
    this.zkClient = zkClient;
  }

  private static Map<String,Object> getIDMap(String zkNodeName, Overseer overseer) {
    Map<String,Object> idMap = new HashMap<>(2);
    idMap.put("id", zkNodeName);
    idMap.put(ZkStateReader.NODE_NAME_PROP, overseer.getZkController().getNodeName());
    return idMap;
  }

  @Override
  boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException,
          InterruptedException, IOException {
    log.info("Running the leader process for Overseer");

    if (overseer.isDone()) {
      log.info("Already closed, bailing ...");
      throw new AlreadyClosedException();
    }

    // TODO: the idea here is that we could clear the Overseer queue
    // if we knew we are the first Overseer in a cluster startup
    // needs more testing in real world vs tests
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

    try {
      boolean success = super.runLeaderProcess(context, weAreReplacement, pauseBeforeStartMs);
      if (!success) {
        cancelElection();
        return false;
      }
    } catch (Exception e) {
      cancelElection();
      if (e instanceof  RuntimeException) {
        throw e;
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    if (!overseer.getZkController().getCoreContainer().isShutDown() && !overseer.getZkController().isShutdownCalled()
        && !overseer.isDone()) {
      log.info("Starting overseer after winning Overseer election {}", id);
      overseer.start(id, context);
    } else {
      log.info("Will not start Overseer because we are closed");
    }

    return true;
  }

  public Overseer getOverseer() {
    return  overseer;
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    //try (ParWork closer = new ParWork(this, true)) {

    //    closer.collect("cancelElection", () -> {
    try {
      super.cancelElection();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception closing Overseer", e);
    }
    //  });

    //      closer.collect("overseer", () -> {
    //        try {
    //          if (!overseer.isCloseAndDone()) {
    //            overseer.close();
    //          }
    //        } catch (Exception e) {
    //          ParWork.propagateInterrupt(e);
    //          log.error("Exception closing Overseer", e);
    //        }
    //      });
    //   }
  }

  @Override
  public void joinedElectionFired() {

  }

  @Override
  public void checkIfIamLeaderFired() {

  }
}

