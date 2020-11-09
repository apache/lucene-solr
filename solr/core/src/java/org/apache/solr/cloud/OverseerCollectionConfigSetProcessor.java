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

import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.zookeeper.KeeperException;

/**
 * An {@link OverseerTaskProcessor} that handles:
 * 1) collection-related Overseer messages
 * 2) configset-related Overseer messages
 */
public class OverseerCollectionConfigSetProcessor extends OverseerTaskProcessor {

  public OverseerCollectionConfigSetProcessor(CoreContainer cc, String myId, LBHttp2SolrClient overseerLbClient, String adminPath, Stats stats, Overseer overseer) throws KeeperException {
    this(cc, myId, overseerLbClient, adminPath, stats, overseer, overseer.getCollectionQueue(cc.getZkController().getZkStateReader().getZkClient(), stats),
        Overseer.getRunningMap(cc.getZkController().getZkStateReader().getZkClient()), Overseer.getCompletedMap(cc.getZkController().getZkStateReader().getZkClient()),
        Overseer.getFailureMap(cc.getZkController().getZkStateReader().getZkClient()));
  }

  protected OverseerCollectionConfigSetProcessor(CoreContainer cc, String myId, LBHttp2SolrClient overseerLbClient, String adminPath, Stats stats, Overseer overseer, OverseerTaskQueue workQueue,
      DistributedMap runningMap, DistributedMap completedMap, DistributedMap failureMap) {
    super(cc, myId, stats, getOverseerMessageHandlerSelector(cc, myId, overseerLbClient, adminPath, stats, overseer), workQueue, runningMap, completedMap, failureMap);
  }

  private static OverseerMessageHandlerSelector getOverseerMessageHandlerSelector(CoreContainer cc, String myId, LBHttp2SolrClient overseerLbClient, String adminPath, Stats stats, Overseer overseer) {

    final OverseerConfigSetMessageHandler configMessageHandler = new OverseerConfigSetMessageHandler(cc);
    return new OverseerMessageHandlerSelector() {
      @Override
      public void close() throws IOException {

        IOUtils.closeQuietly(configMessageHandler);
      }

      @Override
      public OverseerMessageHandler selectOverseerMessageHandler(ZkNodeProps message) {
        String operation = message.getStr(Overseer.QUEUE_OPERATION);
        if (operation != null && operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
          return configMessageHandler;
        }
        throw new IllegalArgumentException("No handler for " + operation + " " + message);
      }
    };
  }

  @Override
  public void close(boolean closeAndDone) {

  }
}
