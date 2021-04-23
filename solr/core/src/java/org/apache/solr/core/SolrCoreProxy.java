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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.UpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrCoreProxy extends SolrCore {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  //to wait during core close
  private Future searcherExecutorFuture = null;
  private boolean isSearchExecutorClosed = false;

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

  protected ExecutorService getExecutorService(CoreContainer coreContainer, String name) {
    //using executor from pool
    final ExecutorService searcherExecutor;
    searcherExecutor = coreContainer.getSearchExecutor(name + System.nanoTime());
    return searcherExecutor;
  }

  protected void searchExecutorWaiter(Future future) {
    searcherExecutorFuture = future;
  }

  protected void searchExecutorClosed() {
    if (isSearchExecutorClosed) {
      //caller should take care of it
      throw new RuntimeException("Core has been closed");
    }
  }

  protected void closeSearchExecutor() {
    try {
      openSearcherLock.lock();
      if (searcherExecutor != null) {
        searcherExecutorFuture.get(60, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    } finally {
      isSearchExecutorClosed = true;
      openSearcherLock.unlock();
    }
  }
}
