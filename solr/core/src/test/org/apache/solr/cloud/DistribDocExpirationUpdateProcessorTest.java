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
import java.util.Objects;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static java.util.Collections.singletonList;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.apache.solr.update.processor.DocExpirationUpdateProcessorFactory;
import org.apache.solr.util.TimeOut;

import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test of {@link DocExpirationUpdateProcessorFactory} in a cloud setup */
@Slow // Has to do some sleeping to wait for a future expiration
public class DistribDocExpirationUpdateProcessorTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String COLLECTION = null;
  private String USER = null;
  private String PASS = null;

  @After
  public void cleanup() throws Exception {
    shutdownCluster();
    COLLECTION = null;
    USER = null;
    PASS = null;
  }

  /**
   * Modifies the request to inlcude authentication params if needed, returns the request 
   */
  @SuppressWarnings({"rawtypes"})
  private <T extends SolrRequest> T setAuthIfNeeded(T req) {
    if (null != USER) {
      assert null != PASS;
      req.setBasicAuthCredentials(USER, PASS);
    }
    return req;
  }
  
  public void setupCluster(boolean security) throws Exception {
    // we want at most one core per node to force lots of network traffic to try and tickle distributed bugs
    final Builder b = configureCluster(4)
      .addConfig("conf", TEST_PATH().resolve("configsets").resolve("doc-expiry").resolve("conf"));

    COLLECTION = "expiring";
    if (security) {
      USER = "solr";
      PASS = "SolrRocksAgain";
      COLLECTION += "_secure";
      
      final String SECURITY_JSON = Utils.toJSONString
        (Utils.makeMap("authorization",
                       Utils.makeMap("class", RuleBasedAuthorizationPlugin.class.getName(),
                                     "user-role", singletonMap(USER,"admin"),
                                     "permissions", singletonList(Utils.makeMap("name","all",
                                                                                "role","admin"))),
                       "authentication",
                       Utils.makeMap("class", BasicAuthPlugin.class.getName(),
                                     "blockUnknown",true,
                                     "credentials", singletonMap(USER, getSaltedHashedValue(PASS)))));
      b.withSecurityJson(SECURITY_JSON);
    }
    b.configure();

    setAuthIfNeeded(CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2))
      .process(cluster.getSolrClient());

    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 2, 2));
  }

  public void testNoAuth() throws Exception {
    setupCluster(false);
    runTest();
  }


  public void testBasicAuth() throws Exception {
    setupCluster(true);

    // sanity check that our cluster really does require authentication
    assertEquals("sanity check of non authenticated request",
                 401,
                 expectThrows(SolrException.class, () -> {
                     final long ignored = cluster.getSolrClient().query
                       (COLLECTION,
                        params("q", "*:*",
                               "rows", "0",
                               "_trace", "no_auth_sanity_check")).getResults().getNumFound();
                   }).code());
    
    runTest();
  }
  
  private void runTest() throws Exception {
    final int totalNumDocs = atLeast(50);
    
    // Add a bunch of docs; some with extremely short expiration, some with no expiration
    // these should be randomly distributed to each shard
    long numDocsThatNeverExpire = 0;
    {
      final UpdateRequest req = setAuthIfNeeded(new UpdateRequest());
      for (int i = 1; i <= totalNumDocs; i++) {
        final SolrInputDocument doc = sdoc("id", i);

        if (random().nextBoolean()) {
          doc.addField("should_expire_s","yup");
          doc.addField("tTl_s","+1SECONDS");
        } else {
          numDocsThatNeverExpire++;
        }
        
        req.add(doc);
      }
      req.commit(cluster.getSolrClient(), COLLECTION);
    }
    
    // NOTE: don't assume we can find exactly totalNumDocs right now, some may have already been deleted...
    
    // it should not take long for us to get to the point where all 'should_expire_s:yup' docs are gone
    waitForNoResults(30, params("q","should_expire_s:yup","rows","0","_trace","init_batch_check"));

    {
      // ...*NOW* we can assert that exactly numDocsThatNeverExpire should exist...
      final QueryRequest req = setAuthIfNeeded(new QueryRequest
                                               (params("q", "*:*",
                                                       "rows", "0",
                                                       "_trace", "count_non_expire_docs")));

      // NOTE: it's possible that replicas could be out of sync but this query may get lucky and
      // only hit leaders.  we'll compare the counts of every replica in every shard later on...
      assertEquals(numDocsThatNeverExpire,
                   req.process(cluster.getSolrClient(), COLLECTION).getResults().getNumFound());
    }
    
    //
    // now that we've confrmed the basics work, let's check some fine grain stuff...
    //
    
    // first off, sanity check that this special docId doesn't some how already exist
    waitForNoResults(0, params("q","id:special99","rows","0","_trace","sanity_check99"));

    {
      // force a hard commit on all shards (the prior auto-expire would have only done a soft commit)
      // so we can ensure our indexVersion won't change uncessisarily on the un-affected
      // shard when we add & (hard) commit our special doc...
      final UpdateRequest req = setAuthIfNeeded(new UpdateRequest());
      req.commit(cluster.getSolrClient(), COLLECTION);
    }
    
    
    // record important data for each replica core so we can check later
    // that it only changes for the replicas of a single shard after we add/expire a single special doc
    log.info("Fetching ReplicaData BEFORE special doc addition/expiration");
    final Map<String,ReplicaData> initReplicaData = getTestDataForAllReplicas();
    assertTrue("WTF? no replica data?", 0 < initReplicaData.size());

    // add & hard commit a special doc with a short TTL 
    setAuthIfNeeded(new UpdateRequest()).add(sdoc("id", "special99", "should_expire_s","yup","tTl_s","+30SECONDS"))
      .commit(cluster.getSolrClient(), COLLECTION);

    // wait for our special docId to be deleted
    waitForNoResults(180, params("q","id:special99","rows","0","_trace","did_special_doc_expire_yet"));

    // now check all of the replicas to verify a few things:
    // - only the replicas of one shard changed -- no unneccessary churn on other shards
    // - every replica of each single shard should have the same number of docs
    // - the total number of docs should match numDocsThatNeverExpire
    log.info("Fetching ReplicaData AFTER special doc addition/expiration");
    final Map<String,ReplicaData> finalReplicaData = getTestDataForAllReplicas();
    assertEquals("WTF? not same num replicas?", 
                 initReplicaData.size(),
                 finalReplicaData.size());

    final Set<String> coresThatChange = new HashSet<>();
    final Set<String> shardsThatChange = new HashSet<>();
    
    int coresCompared = 0;
    int totalDocsOnAllShards = 0;
    final DocCollection collectionState = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION);
    for (Slice shard : collectionState) {
      boolean firstReplica = true;
      for (Replica replica : shard) {
        coresCompared++;
        assertEquals(shard.getName(), replica.getSlice()); // sanity check
        final String core = replica.getCoreName();
        final ReplicaData initData = initReplicaData.get(core);
        final ReplicaData finalData = finalReplicaData.get(core);
        assertNotNull(shard.getName() + ": no init data for core: " + core, initData);
        assertNotNull(shard.getName() + ": no final data for core: " + core, finalData);

        if (!initData.equals(finalData)) {
          log.error("ReplicaData changed: {} != {}", initData, finalData);
          coresThatChange.add(core + "("+shard.getName()+")");
          shardsThatChange.add(shard.getName());
        }
        
        if (firstReplica) {
          totalDocsOnAllShards += finalData.numDocs;
          firstReplica = false;
        }
      }
    }

    assertEquals("Exactly one shard should have changed, instead: " + shardsThatChange
                 + " cores=(" + coresThatChange + ")",
                 1, shardsThatChange.size());
    assertEquals("somehow we missed some cores?", 
                 initReplicaData.size(), coresCompared);

    assertEquals("Final tally has incorrect numDocsThatNeverExpire",
                 numDocsThatNeverExpire, totalDocsOnAllShards);
    
    // TODO: above logic verifies that deleteByQuery happens on all nodes, and ...
    // doesn't affect searcher re-open on shards w/o expired docs ... can we also verify 
    // that *only* one node is sending the deletes ?
    // (ie: no flood of redundant deletes?)

  }

  /**
   * returns a map whose key is the coreNodeName and whose value is data about that core needed for the test
   */
  private Map<String,ReplicaData> getTestDataForAllReplicas() throws IOException, SolrServerException {
    Map<String,ReplicaData> results = new HashMap<>();

    DocCollection collectionState = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION);

    for (Replica replica : collectionState.getReplicas()) {

      String coreName = replica.getCoreName();
      try (HttpSolrClient client = getHttpSolrClient(replica.getCoreUrl())) {

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "indexversion");
        params.set("_trace", "getIndexVersion");
        params.set("qt", ReplicationHandler.PATH);
        QueryRequest req = setAuthIfNeeded(new QueryRequest(params));

        NamedList<Object> res = client.request(req);
        assertNotNull("null response from server: " + coreName, res);

        Object version = res.get("indexversion");
        assertNotNull("null version from server: " + coreName, version);
        assertTrue("version isn't a long: " + coreName, version instanceof Long);

        long numDocs = 
          setAuthIfNeeded(new QueryRequest
                          (params("q", "*:*",
                                  "distrib", "false",
                                  "rows", "0",
                                  "_trace", "counting_docs"))).process(client).getResults().getNumFound();

        final ReplicaData data = new ReplicaData(replica.getSlice(),coreName,(Long)version,numDocs);
        log.info("{}", data);
        results.put(coreName, data);

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

    final QueryRequest req = setAuthIfNeeded(new QueryRequest(params));
    final TimeOut timeout = new TimeOut(maxTimeLimitSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    
    long numFound = req.process(cluster.getSolrClient(), COLLECTION).getResults().getNumFound();
    while (0L < numFound && ! timeout.hasTimedOut()) {
      Thread.sleep(Math.max(1, Math.min(5000, timeout.timeLeft(TimeUnit.MILLISECONDS))));
      
      numFound = req.process(cluster.getSolrClient(), COLLECTION).getResults().getNumFound();
    }

    assertEquals("Give up waiting for no results: " + params,
                 0L, numFound);
  }

  private static class ReplicaData {
    public final String shardName;
    public final String coreName;
    public final long indexVersion;
    public final long numDocs;
    public ReplicaData(final String shardName,
                       final String coreName,
                       final long indexVersion,
                       final long numDocs) {
      assert null != shardName;
      assert null != coreName;
      
      this.shardName = shardName;
      this.coreName = coreName;
      this.indexVersion = indexVersion;
      this.numDocs = numDocs;
    }
    
    @Override
    public String toString() {
      return "ReplicaData(shard="+shardName+",core="+coreName+
        ",indexVer="+indexVersion+",numDocs="+numDocs+")";
    }
    
    @Override
    public boolean equals(Object other) {
      if (other instanceof ReplicaData) {
        ReplicaData that = (ReplicaData)other;
        return 
          this.shardName.equals(that.shardName) &&
          this.coreName.equals(that.coreName) &&
          (this.indexVersion == that.indexVersion) &&
          (this.numDocs == that.numDocs);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.shardName, this.coreName, this.indexVersion, this.numDocs);
    }
  }
  
}
