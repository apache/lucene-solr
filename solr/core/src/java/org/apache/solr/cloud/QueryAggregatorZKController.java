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
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.BeforeReconnect;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAggregatorZKController extends ZkController {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public QueryAggregatorZKController(final CoreContainer cc, String zkServerAddress, int zkClientConnectTimeout, CloudConfig cloudConfig, final CurrentCoreDescriptorProvider registerOnReconnect)
      throws InterruptedException, TimeoutException, IOException {
    super(cc, zkServerAddress, zkClientConnectTimeout, cloudConfig, registerOnReconnect);
  }

  protected ZkDistributedQueue initOverseerJobQueue() {
    return null;
  }

  protected OverseerTaskQueue initOverseerTaskQueue() {
    return null;
  }

  protected OverseerTaskQueue initOverseerConfigSetQueue() {
    return null;
  }

  protected NodeStateProvider getNodeStateProvider() {
    return null;
  }

  protected SolrZkClient getSolrZkClient(int zkClientConnectTimeout,
                                         CurrentCoreDescriptorProvider registerOnReconnect,
                                         DefaultConnectionStrategy strat,
                                         ZkACLProvider zkACLProvider) {
    return new SolrZkClient(zkServerAddress, clientTimeout, zkClientConnectTimeout, strat,
        // on reconnect, reload cloud info
        new OnReconnect() {

          @Override
          public void command() throws KeeperException.SessionExpiredException {
            log.info("ZooKeeper session re-connected ... refreshing core states after session expiration.");
            try {
              zkStateReader.createClusterStateWatchersAndUpdate();
              createEphemeralLiveQueryNode();
            } catch (Exception e) {
              SolrException.log(log, "", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
          }

        }, new BeforeReconnect() {

      @Override
      public void command() {

      }
    }, zkACLProvider, new ConnectionManager.IsClosed() {

      @Override
      public boolean isClosed() {
        return cc.isShutDown();
      }
    });
  }
}
