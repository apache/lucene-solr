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

package org.apache.solr.cloud.api.collections;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ShardRequestTracker;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.ExecutorService;

/**
 * Data passed to Collection API command execution, to allow calls from either the {@link OverseerCollectionMessageHandler}
 * when commands are executed on the Overseer, or - in a future change - allow Collection API commands to be executed in a
 * distributed way, unrelated to and not depending upon Overseer abstractions (including overseer collection message handling).
 */
public interface CollectionCommandContext {
  ShardHandler getShardHandler();

  SolrCloudManager getSolrCloudManager();
  ZkStateReader getZkStateReader();

  DistributedClusterStateUpdater getDistributedClusterStateUpdater();
  CoreContainer getCoreContainer();

  default ShardRequestTracker asyncRequestTracker(String asyncId) {
    return new ShardRequestTracker(asyncId, getAdminPath(), getZkStateReader(), getShardHandler().getShardHandlerFactory());
  }

  /**
   * admin path passed to Overseer is a constant, including in tests.
   */
  default String getAdminPath() {
    return CommonParams.CORES_HANDLER_PATH;
  }

  SolrCloseable getCloseableToLatchOn();

  ExecutorService getExecutorService();

  /**
   * This method enables the commands to enqueue to the overseer cluster state update. This should only be used when the command
   * is running in the Overseer (and will throw an exception if called when Collection API is distributed)
   */
  default void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    throw new IllegalStateException("Bug! offerStateUpdate() should not be called when distributed state updates are enabled");
  }

  default String getOverseerId() {
    throw new IllegalStateException("Bug! getOverseerId() default implementation should never be called");
  }

  /**
   * Command delegating to Overseer to retrieve cluster state update stats from a Collection API call. This does not make
   * sense when cluster state updates are distributed given Overseer does not see them and can't collect stats.
   */
  default Stats getOverseerStats() {
    throw new IllegalStateException("Bug! getOverseerStats() should not be called when distributed state updates are enabled");
  }

  /**
   * Method used by Per Replica States implementation to force the cluster state updater to immediately reload a collection from Zookeeper.
   * This method is not used when cluster state updates are distributed.
   */
  default void submitIntraProcessMessage(Overseer.Message message) {
    throw new IllegalStateException("Bug! submitIntraProcessMessage() should not be called when distributed state updates are enabled");
  }
}
