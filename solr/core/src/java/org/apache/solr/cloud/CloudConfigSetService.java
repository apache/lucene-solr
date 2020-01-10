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

import java.lang.invoke.MethodHandles;

import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final ZkController zkController;

  public CloudConfigSetService(SolrResourceLoader loader, ZkController zkController) {
    super(loader);
    this.zkController = zkController;
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    try {
      // for back compat with cores that can create collections without the collections API
      if (!zkController.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + cd.getCollectionName(), true)) {
        CreateCollectionCmd.createCollectionZkNode(zkController.getSolrCloudManager().getDistribStateManager(), cd.getCollectionName(), cd.getCloudDescriptor().getParams());
      }
    } catch (KeeperException e) {
      SolrException.log(log, null, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      SolrException.log(log, null, e);
    }

    String configName;
    try {
      configName = zkController.getZkStateReader().readConfigName(cd.getCollectionName());
    } catch (KeeperException ex) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Specified config does not exist in ZooKeeper: " + cd.getCollectionName());
    }
    return new ZkSolrResourceLoader(cd.getInstanceDir(), configName, parentLoader.getClassLoader(),
        cd.getSubstitutableProperties(), zkController);
  }

  @Override
  public String configName(CoreDescriptor cd) {
    return "collection " + cd.getCloudDescriptor().getCollectionName();
  }
}
