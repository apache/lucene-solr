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

import java.util.concurrent.ExecutorService;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;

/**
 * Context passed to Collection API commands when they execute in the Overseer.
 */
public class OcmhCollectionCommandContext implements CollectionCommandContext {
  private final OverseerCollectionMessageHandler ocmh;

  public OcmhCollectionCommandContext(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public ShardHandler getShardHandler() {
    return ocmh.shardHandlerFactory.getShardHandler();
  }

  @Override
  public SolrCloudManager getSolrCloudManager() {
    return ocmh.cloudManager;
  }

  @Override
  public CoreContainer getCoreContainer() {
    return ocmh.overseer.getCoreContainer();
  }

  @Override
  public ZkStateReader getZkStateReader() {
    return ocmh.zkStateReader;
  }

  @Override
  public DistributedClusterStateUpdater getDistributedClusterStateUpdater() {
    return ocmh.overseer.getDistributedClusterStateUpdater();
  }

  @Override
  public void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    ocmh.overseer.offerStateUpdate(data);
  }

  @Override
  public SolrCloseable getCloseableToLatchOn() {
    return ocmh;
  }

  @Override
  public ExecutorService getExecutorService() {
    return ocmh.tpe;
  }

  @Override
  public String getOverseerId() {
    return ocmh.myId;
  }

  @Override
  public Stats getOverseerStats() {
    return ocmh.stats;
  }

  @Override
  public void submitIntraProcessMessage(Overseer.Message message) {
    ocmh.overseer.submit(message);
  }
}
