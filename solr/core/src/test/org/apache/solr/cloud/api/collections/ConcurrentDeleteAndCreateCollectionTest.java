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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Nightly
public class ConcurrentDeleteAndCreateCollectionTest extends SolrTestCaseJ4 {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private MiniSolrCloudCluster solrCluster;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    solrCluster = new MiniSolrCloudCluster(1, createTempDir(), buildJettyConfig("/solr"));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    if (null != solrCluster) {
      solrCluster.shutdown();
      solrCluster = null;
    }
    super.tearDown();
  }
  
  public void testConcurrentCreateAndDeleteDoesNotFail() {
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;
    final CreateDeleteCollectionThread[] threads = new CreateDeleteCollectionThread[10];
    for (int i = 0; i < threads.length; i++) {
      final String collectionName = "collection" + i;
      uploadConfig(configset("configset-2"), collectionName);
      final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
      final SolrClient solrClient = getHttpSolrClient(baseUrl);
      threads[i] = new CreateDeleteSearchCollectionThread("create-delete-search-" + i, collectionName, collectionName, 
          timeToRunSec, solrClient, failure);
    }
    
    startAll(threads);
    joinAll(threads);
    
    assertNull("concurrent create and delete collection failed: " + failure.get(), failure.get());
  }
  
  public void testConcurrentCreateAndDeleteOverTheSameConfig() {
    final String configName = "testconfig";
    uploadConfig(configset("configset-2"), configName); // upload config once, to be used by all collections
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;
    final CreateDeleteCollectionThread[] threads = new CreateDeleteCollectionThread[2];
    for (int i = 0; i < threads.length; i++) {
      final String collectionName = "collection" + i;
      final SolrClient solrClient = getHttpSolrClient(baseUrl);
      threads[i] = new CreateDeleteCollectionThread("create-delete-" + i, collectionName, configName,
                                                    timeToRunSec, solrClient, failure);
    }

    startAll(threads);
    joinAll(threads);

    assertNull("concurrent create and delete collection failed: " + failure.get(), failure.get());
  }
  
  private void uploadConfig(Path configDir, String configName) {
    try {
      solrCluster.uploadConfigSet(configDir, configName);
    } catch (IOException | KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void joinAll(final CreateDeleteCollectionThread[] threads) {
    for (CreateDeleteCollectionThread t : threads) {
      try {
        t.joinAndClose();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
    }
  }
  
  private void startAll(final Thread[] threads) {
    for (Thread t : threads) {
      t.start();
    }
  }
  
  private static class CreateDeleteCollectionThread extends Thread {
    protected final String collectionName;
    protected final String configName;
    protected final long timeToRunSec;
    protected final SolrClient solrClient;
    protected final AtomicReference<Exception> failure;
    
    public CreateDeleteCollectionThread(String name, String collectionName, String configName, long timeToRunSec,
        SolrClient solrClient, AtomicReference<Exception> failure) {
      super(name);
      this.collectionName = collectionName;
      this.timeToRunSec = timeToRunSec;
      this.solrClient = solrClient;
      this.failure = failure;
      this.configName = configName;
    }
    
    @Override
    public void run() {
      final TimeOut timeout = new TimeOut(timeToRunSec, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      while (! timeout.hasTimedOut() && failure.get() == null) {
        doWork();
      }
    }
    
    protected void doWork() {
      createCollection();
      deleteCollection();
    }
    
    protected void addFailure(Exception e) {
      log.error("Add Failure", e);
      synchronized (failure) {
        if (failure.get() != null) {
          failure.get().addSuppressed(e);
        } else {
          failure.set(e);
        }
      }
    }
    
    private void createCollection() {
      try {
        final CollectionAdminResponse response = CollectionAdminRequest.createCollection(collectionName,configName,1,1)
                .process(solrClient);
        if (response.getStatus() != 0) {
          addFailure(new RuntimeException("failed to create collection " + collectionName));
        }
      } catch (Exception e) {
        addFailure(e);
      }
      
    }
    
    private void deleteCollection() {
      try {
        final CollectionAdminRequest.Delete deleteCollectionRequest
          = CollectionAdminRequest.deleteCollection(collectionName);
        final CollectionAdminResponse response = deleteCollectionRequest.process(solrClient);
        if (response.getStatus() != 0) {
          addFailure(new RuntimeException("failed to delete collection " + collectionName));
        }
      } catch (Exception e) {
        addFailure(e);
      }
    }
    
    public void joinAndClose() throws InterruptedException {
      try {
        super.join(60000);
      } finally {
        IOUtils.closeQuietly(solrClient);
      }
    }
  }
  
  private static class CreateDeleteSearchCollectionThread extends CreateDeleteCollectionThread {

    public CreateDeleteSearchCollectionThread(String name, String collectionName, String configName, long timeToRunSec,
        SolrClient solrClient, AtomicReference<Exception> failure) {
      super(name, collectionName, configName, timeToRunSec, solrClient, failure);
    }
    
    @Override
    protected void doWork() {
      super.doWork();
      searchNonExistingCollection();
    }
    
    private void searchNonExistingCollection() {
      try {
        solrClient.query(collectionName, new SolrQuery("*"));
      } catch (Exception e) {
        if (!e.getMessage().contains("not found") && !e.getMessage().contains("Can not find")) {
          addFailure(e);
        }
      }
    }
    
  }
  
}
