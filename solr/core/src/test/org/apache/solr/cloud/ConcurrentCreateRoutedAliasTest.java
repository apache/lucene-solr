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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.IOUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_MAX_SHARDS_PER_NODE;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_NUM_SHARDS;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_PULL_REPLICAS;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_REPLICATION_FACTOR;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_ROUTER_FIELD;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_ROUTER_NAME;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_SHARDS;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.CREATE_COLLECTION_TLOG_REPLICAS;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.ROUTING_FIELD;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.ROUTING_INCREMENT;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.ROUTING_MAX_FUTURE;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateRoutedAlias.ROUTING_TYPE;

public class ConcurrentCreateRoutedAliasTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private MiniSolrCloudCluster solrCluster;

  // to avoid having to delete stuff...
  volatile int num = 0;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    solrCluster = new MiniSolrCloudCluster(4, createTempDir(), buildJettyConfig("/solr"));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    solrCluster.shutdown();
    super.tearDown();
  }

  @Test
  public void testConcurrentCreateRoutedAliasMinimal() throws IOException, KeeperException.NoNodeException {
    // this is the test where be blow out a bunch of create commands all out at once.
    // other tests are more functionality based, and just use a single thread.

    // Failure of this test very occasionally due to overseer overload would not be worrisome (just bothersome).
    // Any use case creating large numbers of time routed aliases concurrently would be an EXTREMELY odd
    // if not fundamentally broken use case. This test method is just here to guard against any race
    // conditions in the code that could crop up rarely in lower volume usage.

    // That said any failures involving about NPE's or missing parameters or oddities other than overwhelming
    // the overseer queue with retry races emanating from this test should be investigated. Also if it fails
    // frequently that needs to be investigated of course.


    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;

    // Note: this number of threads seems to work regularly with the up-tweaked number of retries (50) in
    // org.apache.solr.common.cloud.ZkStateReader.AliasesManager.applyModificationAndExportToZk()
    // with the original 5 retries this wouldn't reliably pass with 10 threads, but with 50 retries it seems
    // to handle 50 threads about a dozen times without any failure (on a 32 thread processor)
    // it also passed 3/3 at 150 threads and 2/3 with 250 threads on both 1 node and 4 nodes...
    // the failure mode seems to be overseer tasks that are not found. I suspect this happens when enough
    // threads get into retry races and the spam overwhelms the overseer. (that this can happen might imply
    // an issue over there, but I'm not sure, since there is an intentional hard limit on the overseer queue
    // and I haven't tried to count the retries up and figure out if the requests are actually exceeding that
    // limit or not, but the speed of retries might indicate an effectively hot loop, but again, a separate issue.

    // The hope is that the level of concurrency supported by create routed alias and the code it uses is such
    // that this test wouldn't spuriously fail more than once a year. If that's true users should never see
    // an issue in the wild unless they are doing something we probably don't want to support anyway

    final CreateRoutedAliasThread[] threads = new CreateRoutedAliasThread[50];
    int numStart = num;
    for (; num < threads.length + numStart; num++) {
      final String aliasName = "testAlias" + num;
      uploadConfig(configset("_default"), aliasName);
      final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
      final SolrClient solrClient = getHttpSolrClient(baseUrl);


      int i = num - numStart;
      Map<String, String> routingParams = getMinimalRoutingCommands();
      Map<String, String> collectionParams = getMinimalCollectionParams();
      threads[i] = new CreateRoutedAliasThread("create-delete-search-" + i, aliasName, "NOW/HOUR",
          "UTC", routingParams, collectionParams, timeToRunSec, solrClient, failure, false);
    }

    startAll(threads);
    joinAll(threads);

    assertNull("concurrent alias creation failed " + failure.get(), failure.get());
  }


  @Test
  public void testConcurrentCreateRoutedAliasComplex() {
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final int timeToRunSec = 30;

    final CreateRoutedAliasThread[] threads = new CreateRoutedAliasThread[1];
    int numStart = num;
    System.out.println("NUM ==> " +num);
    for (; num < threads.length + numStart; num++) {
      final String aliasName = "testAliasCplx" + num;
      uploadConfig(configset("_default"), aliasName);
      final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
      final SolrClient solrClient = getHttpSolrClient(baseUrl);

      int i = num - numStart;
      Map<String, String> routingParams = getMinimalRoutingCommands();
      Map<String, String> collectionParams = getComplicatedCollectionParams();
      threads[i] = new CreateRoutedAliasThread("create-routed-alias-cplx-" + i,
          aliasName, "2017-12-25_23_24_25","EST", routingParams, collectionParams,
          timeToRunSec, solrClient, failure, true);
    }

    startAll(threads);
    joinAll(threads);

    assertNull("concurrent alias creation failed " + failure.get(), failure.get());
  }

  public Map<String,String> getComplicatedCollectionParams() {
    Map<String, String> result = getMinimalCollectionParams();
    result.put(CREATE_COLLECTION_ROUTER_NAME, "implicit");
    result.put(CREATE_COLLECTION_ROUTER_FIELD, "implicit_s");
    result.put(CREATE_COLLECTION_SHARDS, "foo,bar");
    result.remove(CREATE_COLLECTION_NUM_SHARDS);
    result.remove(CREATE_COLLECTION_REPLICATION_FACTOR);
    result.put(CREATE_COLLECTION_REPLICATION_FACTOR,"2" );
    result.put(CREATE_COLLECTION_NUM_SHARDS,"2");
    result.put(CREATE_COLLECTION_MAX_SHARDS_PER_NODE, "4");
    result.put(CREATE_COLLECTION_PULL_REPLICAS, "2");
    result.put(CREATE_COLLECTION_TLOG_REPLICAS, "2");
    return result;
  }

  static Map<String, String> getMinimalCollectionParams() {
    // needs to be V1 param name below

    HashMap<String, String> cparams = new HashMap<>();
    cparams.put("create-collection.collection.configName", "_default");
    cparams.put(CREATE_COLLECTION_NUM_SHARDS,"1" );
    cparams.put(CREATE_COLLECTION_REPLICATION_FACTOR,"1" );
    return cparams;
  }

  static Map<String, String> getMinimalRoutingCommands() {
    HashMap<String, String> rparams = new HashMap<>();
    rparams.put(ROUTING_TYPE, "time");
    rparams.put(ROUTING_FIELD, "routedFoo_dt");
    rparams.put(ROUTING_INCREMENT, "+12HOUR");
    rparams.put(ROUTING_MAX_FUTURE, String.valueOf(1000 * 60 * 60));
    return rparams;
  }


  private void uploadConfig(Path configDir, String configName) {
    try {
      solrCluster.uploadConfigSet(configDir, configName);
    } catch (IOException | KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void joinAll(final CreateRoutedAliasThread[] threads) {
    for (CreateRoutedAliasThread t : threads) {
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

  private static class CreateRoutedAliasThread extends Thread {
    protected final String aliasName;
    protected final String start;
    protected final String tz;
    protected final Map<String, String> routingParams;
    protected final Map<String, String> collectionParams;
    protected final long timeToRunSec;
    protected final SolrClient solrClient;
    protected final AtomicReference<Exception> failure;
    private final boolean v2;

    public CreateRoutedAliasThread(
        String name, String aliasName, String start, String tz, Map<String,
        String> routingParams, Map<String, String> collectionParams, long timeToRunSec, SolrClient solrClient,
        AtomicReference<Exception> failure, boolean v2) {
      super(name);
      this.aliasName = aliasName;
      this.start = start;
      this.tz = tz;
      this.routingParams = routingParams;
      this.collectionParams = collectionParams;
      this.timeToRunSec = timeToRunSec;
      this.solrClient = solrClient;
      this.failure = failure;
      this.v2 = v2;
    }

    @Override
    public void run() {
        doWork();
    }

    protected void doWork() {
      createAlias();
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

    private void createAlias() {
      try {
        CollectionAdminRequest.CreateRoutedAlias rq = CollectionAdminRequest
            .createRoutedAlias(aliasName, start, "UTC", getMinimalRoutingCommands(), getMinimalCollectionParams());

        final CollectionAdminResponse response = rq.process(solrClient);
        if (response.getStatus() != 0) {
          addFailure(new RuntimeException("failed to create collection " + aliasName));
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


}
