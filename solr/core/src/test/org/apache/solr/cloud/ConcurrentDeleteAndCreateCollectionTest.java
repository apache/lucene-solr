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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

@Nightly
public class ConcurrentDeleteAndCreateCollectionTest extends SolrTestCaseJ4 {
  
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
    solrCluster.shutdown();
    super.tearDown();
  }
  
  public void testConcurrentCreateAndDeleteDoesNotFail() {
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;
    final Thread[] threads = new Thread[10];
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
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;
    final Thread[] threads = new Thread[2];
    for (int i = 0; i < threads.length; i++) {
      final String collectionName = "collection" + i;
      threads[i] = new CreateDeleteCollectionThread("create-delete-" + i, collectionName, configName,
                                                    timeToRunSec, solrClient, failure);
    }

    startAll(threads);
    joinAll(threads);

    assertNull("concurrent create and delete collection failed: " + failure.get(), failure.get());

    try {
      solrClient.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void uploadConfig(Path configDir, String configName) {
    try {
      solrCluster.uploadConfigSet(configDir, configName);
    } catch (IOException | KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void joinAll(final Thread[] threads) {
    for (Thread t : threads) {
      try {
        t.join();
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
      final TimeOut timeout = new TimeOut(timeToRunSec, TimeUnit.SECONDS);
      while (! timeout.hasTimedOut() && failure.get() == null) {
        doWork();
      }
    }
    
    protected void doWork() {
      createCollection();
      deleteCollection();
    }
    
    protected void addFailure(Exception e) {
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
        final CollectionAdminResponse response = new CollectionAdminRequest.Create()
                .setCollectionName(collectionName)
                .setNumShards(1)
                .setReplicationFactor(1)
                .setConfigName(configName).process(solrClient);
        if (response.getStatus() != 0) {
          addFailure(new RuntimeException("failed to create collection " + collectionName));
        }
      } catch (Exception e) {
        addFailure(e);
      }
      
    }
    
    private void deleteCollection() {
      try {
        final CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete()
                .setCollectionName(collectionName);
        
        final CollectionAdminResponse response = deleteCollectionRequest.process(solrClient);
        if (response.getStatus() != 0) {
          addFailure(new RuntimeException("failed to delete collection " + collectionName));
        }
      } catch (Exception e) {
        addFailure(e);
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
