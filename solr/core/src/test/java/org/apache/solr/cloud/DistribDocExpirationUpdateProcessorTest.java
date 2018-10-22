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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.update.processor.DocExpirationUpdateProcessorFactory;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test of {@link DocExpirationUpdateProcessorFactory} in a cloud setup */
@Slow // Has to do some sleeping to wait for a future expiration
public class DistribDocExpirationUpdateProcessorTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "expiry";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("doc-expiry").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 2, 1));
  }

  @Test
  public void test() throws Exception {
    
    // some docs with no expiration
    UpdateRequest req1 = new UpdateRequest();
    for (int i = 1; i <= 100; i++) {
      req1.add(sdoc("id", i));
    }
    req1.commit(cluster.getSolrClient(), COLLECTION);

    // this doc better not already exist
    waitForNoResults(0, params("q","id:999","rows","0","_trace","sanity_check"));
    
    // record the indexversion for each server so we can check later
    // that it only changes for one shard
    final Map<String,Long> initIndexVersions = getIndexVersionOfAllReplicas();
    assertTrue("WTF? no versions?", 0 < initIndexVersions.size());

    // add a doc with a short TTL 
    new UpdateRequest().add(sdoc("id", "999", "tTl_s","+30SECONDS")).commit(cluster.getSolrClient(), COLLECTION);

    // wait for one doc to be deleted
    waitForNoResults(180, params("q","id:999","rows","0","_trace","did_it_expire_yet"));

    // verify only one shard changed
    final Map<String,Long> finalIndexVersions = getIndexVersionOfAllReplicas();
    assertEquals("WTF? not same num versions?", 
                 initIndexVersions.size(),
                 finalIndexVersions.size());
    
    final Set<String> nodesThatChange = new HashSet<String>();
    final Set<String> shardsThatChange = new HashSet<String>();
    
    int coresCompared = 0;
    DocCollection collectionState = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION);
    for (Replica replica : collectionState.getReplicas()) {
      coresCompared++;
      String name = replica.getName();
      String core = replica.getCoreName();
      Long initVersion = initIndexVersions.get(core);
      Long finalVersion = finalIndexVersions.get(core);
      assertNotNull(name + ": no init version for core: " + core, initVersion);
      assertNotNull(name + ": no final version for core: " + core, finalVersion);

      if (!initVersion.equals(finalVersion)) {
        nodesThatChange.add(core + "("+name+")");
        shardsThatChange.add(name);
      }
    }

    assertEquals("Exactly one shard should have changed, instead: " + shardsThatChange
                 + " nodes=(" + nodesThatChange + ")",
                 1, shardsThatChange.size());
    assertEquals("somehow we missed some cores?", 
                 initIndexVersions.size(), coresCompared);

    // TODO: above logic verifies that deleteByQuery happens on all nodes, and ...
    // doesn't affect searcher re-open on shards w/o expired docs ... can we also verify 
    // that *only* one node is sending the deletes ?
    // (ie: no flood of redundant deletes?)

  }

  /**
   * returns a map whose key is the coreNodeName and whose value is what the replication
   * handler returns for the indexversion
   */
  private Map<String,Long> getIndexVersionOfAllReplicas() throws IOException, SolrServerException {
    Map<String,Long> results = new HashMap<String,Long>();

    DocCollection collectionState = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION);

    for (Replica replica : collectionState.getReplicas()) {

      String coreName = replica.getCoreName();
      try (HttpSolrClient client = getHttpSolrClient(replica.getCoreUrl())) {

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "indexversion");
        params.set("_trace", "getIndexVersion");
        params.set("qt", ReplicationHandler.PATH);
        QueryRequest req = new QueryRequest(params);

        NamedList<Object> res = client.request(req);
        assertNotNull("null response from server: " + coreName, res);

        Object version = res.get("indexversion");
        assertNotNull("null version from server: " + coreName, version);
        assertTrue("version isn't a long: " + coreName, version instanceof Long);
        results.put(coreName, (Long) version);

        long numDocs = client.query(params("q", "*:*", "distrib", "false", "rows", "0", "_trace", "counting_docs"))
            .getResults().getNumFound();
        log.info("core=" + coreName + "; ver=" + version +
            "; numDocs=" + numDocs);

      }
    }


    return results;
  }

  /**
   * Executes a query over and over against the cloudClient every 5 seconds 
   * until the numFound is 0 or the maxTimeLimitSeconds is exceeded. 
   * Query is guaranteed to be executed at least once.
   */
  private void waitForNoResults(int maxTimeLimitSeconds,
                                SolrParams params)
      throws SolrServerException, InterruptedException, IOException {

    final TimeOut timeout = new TimeOut(maxTimeLimitSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    long numFound = cluster.getSolrClient().query(COLLECTION, params).getResults().getNumFound();
    while (0L < numFound && ! timeout.hasTimedOut()) {
      Thread.sleep(Math.max(1, Math.min(5000, timeout.timeLeft(TimeUnit.MILLISECONDS))));
      numFound = cluster.getSolrClient().query(COLLECTION, params).getResults().getNumFound();
    }
    assertEquals("Give up waiting for no results: " + params,
                 0L, numFound);
  }

}
