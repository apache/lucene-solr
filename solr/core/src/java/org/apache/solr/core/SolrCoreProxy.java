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

package org.apache.solr.core;

import org.apache.solr.update.UpdateHandler;

public class SolrCoreProxy extends SolrCore {

  public SolrCoreProxy(CoreContainer coreContainer, CoreDescriptor cd, ConfigSet coreConfig) {
    super(coreContainer, cd, coreConfig);
    registerCollectionWatcher();
  }

  public SolrCoreProxy(CoreContainer coreContainer, CoreDescriptor coreDescriptor, ConfigSet configSet,
                       String dataDir, UpdateHandler updateHandler,
                       IndexDeletionPolicyWrapper delPolicy, SolrCore prev, boolean reload) {
    super(coreContainer, coreDescriptor, configSet, dataDir, updateHandler, delPolicy, prev, reload);
    registerCollectionWatcher();
  }

  private void registerCollectionWatcher() {
    //This will update the collection state, if there is shard split or move
    if (getCoreContainer().isZooKeeperAware())
      getCoreContainer().getZkController().getZkStateReader().registerDocCollectionWatcher(getName(), collection -> false);
  }

  protected void bufferUpdatesIfConstructing(CoreDescriptor coreDescriptor) {
  }

  /*
  We register for config dir "configs/conf"; thus if user updates "configs/conf" then just reload proxycore.
   */
  protected boolean forceReloadCore() {
    return true;
  }
}
