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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

public class ConcurrentDeleteAndCreateCollectionTest extends SolrTestCaseJ4 {
  
  private MiniSolrCloudCluster solrCluster;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    final File solrXml = getFile("solr").toPath().resolve("solr.xml").toFile();
    solrCluster = new MiniSolrCloudCluster(1, createTempDir().toFile(), solrXml, buildJettyConfig("/solr"));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    solrCluster.shutdown();
    super.tearDown();
  }
  
  public void testConcurrentCreateAndDeleteDoesNotFail() {
    final File configDir = getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;
    final Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      final String collectionName = "collection" + i;
      uploadConfig(configDir, collectionName);
      final SolrClient solrClient = new HttpSolrClient(solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString());
      threads[i] = new CreateDeleteCollectionThread("create-delete-" + i, collectionName, timeToRunSec, solrClient, failure);
    }
    
    startAll(threads);
    joinAll(threads);
    
    assertNull("concurrent create and delete collection failed: " + failure.get(), failure.get());
  }
  
  private void uploadConfig(File configDir, String configName) {
    try {
      solrCluster.uploadConfigDir(configDir, configName);
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
    private final String collectionName;
    private final long timeToRunSec;
    private final SolrClient solrClient;
    private final AtomicReference<Exception> failure;
    
    public CreateDeleteCollectionThread(String name, String collectionName,
        long timeToRunSec, SolrClient solrClient, AtomicReference<Exception> failure) {
      super(name);
      this.collectionName = collectionName;
      this.timeToRunSec = timeToRunSec;
      this.solrClient = solrClient;
      this.failure = failure;
    }

    @Override
    public void run() {
      final long timeToStop = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeToRunSec);
      while (System.currentTimeMillis() < timeToStop && failure.get() == null) {
        createCollection(collectionName);
        deleteCollection();
        searchNonExistingCollection();
      }
    }
    
    private void searchNonExistingCollection() {
      try {
        solrClient.query(collectionName, new SolrQuery("*"));
      } catch (Exception e) {
        if (!e.getMessage().contains("not found") && !e.getMessage().contains("Can not find")) {
          synchronized (failure) {
            if (failure.get() != null) {
              failure.get().addSuppressed(e);
            } else {
              failure.set(e);
            }
          }
        }
      }
    }
    
    private void createCollection(String collectionName) {
      try {
        final CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
        createCollectionRequest.setCollectionName(collectionName);
        createCollectionRequest.setNumShards(1);
        createCollectionRequest.setReplicationFactor(1);
        createCollectionRequest.setConfigName(collectionName);
        
        final CollectionAdminResponse response = createCollectionRequest.process(solrClient);
        assertEquals(0, response.getStatus());
      } catch (IOException | SolrServerException e) {
        throw new RuntimeException(e);
      }
      
    }
    
    private void deleteCollection() {
      try {
        final CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
        deleteCollectionRequest.setCollectionName(collectionName);
        
        final CollectionAdminResponse response = deleteCollectionRequest.process(solrClient);
        assertEquals(0, response.getStatus());
      } catch (IOException | SolrServerException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
}