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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import org.apache.solr.update.processor.DocExpirationUpdateProcessorFactory; // jdoc
import org.apache.solr.update.processor.DocExpirationUpdateProcessorFactoryTest;

import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/** Test of {@link DocExpirationUpdateProcessorFactory} in a cloud setup */
@Slow // Has to do some sleeping to wait for a future expiration
public class DistribDocExpirationUpdateProcessorTest extends AbstractFullDistribZkTestBase {

  public static Logger log = LoggerFactory.getLogger(DistribDocExpirationUpdateProcessorTest.class);

  public DistribDocExpirationUpdateProcessorTest() {
    configString = DocExpirationUpdateProcessorFactoryTest.CONFIG_XML;
    schemaString = DocExpirationUpdateProcessorFactoryTest.SCHEMA_XML;
  }

  @Override
  protected String getCloudSolrConfig() {
    return configString;
  }

  @Test
  public void test() throws Exception {
    assertTrue("only one shard?!?!?!", 1 < shardToJetty.keySet().size());
    log.info("number of shards: {}", shardToJetty.keySet().size());

    handle.clear();
    handle.put("maxScore", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    // some docs with no expiration
    for (int i = 1; i <= 100; i++) {
      indexDoc(sdoc("id", i));
    }
    commit();
    waitForThingsToLevelOut(30);

    // this doc better not already exist
    waitForNoResults(0, params("q","id:999","rows","0","_trace","sanity_check"));
    
    // record the indexversion for each server so we can check later
    // that it only changes for one shard
    final Map<String,Long> initIndexVersions = getIndexVersionOfAllReplicas();
    assertTrue("WTF? no versions?", 0 < initIndexVersions.size());


    // add a doc with a short TTL 
    indexDoc(sdoc("id", "999", "tTl_s","+30SECONDS"));
    commit();

    // wait for one doc to be deleted
    waitForNoResults(180, params("q","id:999","rows","0","_trace","did_it_expire_yet"));

    // verify only one shard changed
    waitForThingsToLevelOut(30);
    final Map<String,Long> finalIndexVersions = getIndexVersionOfAllReplicas();
    assertEquals("WTF? not same num versions?", 
                 initIndexVersions.size(),
                 finalIndexVersions.size());
    
    final Set<String> nodesThatChange = new HashSet<String>();
    final Set<String> shardsThatChange = new HashSet<String>();
    
    int coresCompared = 0;
    for (String shard : shardToJetty.keySet()) {
      for (CloudJettyRunner replicaRunner : shardToJetty.get(shard)) {
        coresCompared++;

        String core = replicaRunner.coreNodeName;
        Long initVersion = initIndexVersions.get(core);
        Long finalVersion = finalIndexVersions.get(core);
        assertNotNull(shard + ": no init version for core: " + core, initVersion);
        assertNotNull(shard + ": no final version for core: " + core, finalVersion);

        if (!initVersion.equals(finalVersion)) {
          nodesThatChange.add(core + "("+shard+")");
          shardsThatChange.add(shard);
        }
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
    // (ie: no flood of redundent deletes?)

  }

  /**
   * returns a map whose key is the coreNodeName and whose value is what the replication
   * handler returns for the indexversion
   */
  private Map<String,Long> getIndexVersionOfAllReplicas() throws IOException, SolrServerException {
    Map<String,Long> results = new HashMap<String,Long>();

    for (List<CloudJettyRunner> listOfReplicas : shardToJetty.values()) {
      for (CloudJettyRunner replicaRunner : listOfReplicas) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command","indexversion");
        params.set("_trace","getIndexVersion");
        params.set("qt","/replication");
        QueryRequest req = new QueryRequest(params);
    
        NamedList<Object> res = replicaRunner.client.solrClient.request(req);
        assertNotNull("null response from server: " + replicaRunner.coreNodeName, res);

        Object version = res.get("indexversion");
        assertNotNull("null version from server: " + replicaRunner.coreNodeName, version);
        assertTrue("version isn't a long: "+replicaRunner.coreNodeName, 
                   version instanceof Long);
        results.put(replicaRunner.coreNodeName, (Long)version);

        long numDocs = replicaRunner.client.solrClient.query
          (params("q","*:*","distrib","false","rows","0","_trace","counting_docs"))
          .getResults().getNumFound();
        log.info("core=" + replicaRunner.coreNodeName + "; ver=" + version + 
                 "; numDocs=" + numDocs); 

      }
    }

    return results;
  }

  /**
   * Executes a query over and over against the cloudClient every 5 seconds 
   * until the numFound is 0 or the maxTimeLimitSeconds is exceeded. 
   * Query is garunteed to be executed at least once.
   */
  private void waitForNoResults(int maxTimeLimitSeconds,
                                SolrParams params)
      throws SolrServerException, InterruptedException, IOException {

    final TimeOut timeout = new TimeOut(maxTimeLimitSeconds, TimeUnit.SECONDS);
    long numFound = cloudClient.query(params).getResults().getNumFound();
    while (0L < numFound && ! timeout.hasTimedOut()) {
      Thread.sleep(Math.max(1, Math.min(5000, timeout.timeLeft(TimeUnit.MILLISECONDS))));
      numFound = cloudClient.query(params).getResults().getNumFound();
    }
    assertEquals("Give up waiting for no results: " + params,
                 0L, numFound);
  }

}
