package org.apache.solr.cloud;

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

import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;

/**
 * An {@link OverseerTaskProcessor} that handles:
 * 1) collection-related Overseer messages
 * 2) configset-related Overseer messages
 */
public class OverseerCollectionConfigSetProcessor extends OverseerTaskProcessor {

   public OverseerCollectionConfigSetProcessor(ZkStateReader zkStateReader, String myId,
                                     final ShardHandler shardHandler,
                                     String adminPath, Overseer.Stats stats, Overseer overseer,
                                     OverseerNodePrioritizer overseerNodePrioritizer) {
    this(
        zkStateReader,
        myId,
        shardHandler.getShardHandlerFactory(),
        adminPath,
        stats,
        overseer,
        overseerNodePrioritizer,
        Overseer.getCollectionQueue(zkStateReader.getZkClient(), stats),
        Overseer.getRunningMap(zkStateReader.getZkClient()),
        Overseer.getCompletedMap(zkStateReader.getZkClient()),
        Overseer.getFailureMap(zkStateReader.getZkClient())
    );
  }

  protected OverseerCollectionConfigSetProcessor(ZkStateReader zkStateReader, String myId,
                                        final ShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Overseer.Stats stats,
                                        Overseer overseer,
                                        OverseerNodePrioritizer overseerNodePrioritizer,
                                        OverseerTaskQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    super(
        zkStateReader,
        myId,
        shardHandlerFactory,
        adminPath,
        stats,
        getOverseerMessageHandlerSelector(zkStateReader, myId, shardHandlerFactory,
            adminPath, stats, overseer, overseerNodePrioritizer),
        overseerNodePrioritizer,
        workQueue,
        runningMap,
        completedMap,
        failureMap);
  }

  private static OverseerMessageHandlerSelector getOverseerMessageHandlerSelector(
      ZkStateReader zkStateReader,
      String myId,
      final ShardHandlerFactory shardHandlerFactory,
      String adminPath,
      Overseer.Stats stats,
      Overseer overseer,
      OverseerNodePrioritizer overseerNodePrioritizer) {
    final OverseerCollectionMessageHandler collMessageHandler = new OverseerCollectionMessageHandler(
        zkStateReader, myId, shardHandlerFactory, adminPath, stats, overseer, overseerNodePrioritizer);
    final OverseerConfigSetMessageHandler configMessageHandler = new OverseerConfigSetMessageHandler(
        zkStateReader);
    return new OverseerMessageHandlerSelector() {
      @Override
      public OverseerMessageHandler selectOverseerMessageHandler(ZkNodeProps message) {
        String operation = message.getStr(Overseer.QUEUE_OPERATION);
        if (operation != null && operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
          return configMessageHandler;
        }
        return collMessageHandler;
      }
    };
  }
}
