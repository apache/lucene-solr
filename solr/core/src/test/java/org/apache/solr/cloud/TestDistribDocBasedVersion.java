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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TestDistribDocBasedVersion extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  String bucket1 = "shard1";      // shard1: top bits:10  80000000:ffffffff
  String bucket2 = "shard2";      // shard2: top bits:00  00000000:7fffffff

  private static String vfield = "my_version_l";


  @BeforeClass
  public static void beforeShardHashingTest() throws Exception {
    useFactory(null);
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-externalversionconstraint.xml";
  }

  public TestDistribDocBasedVersion() {
    schemaString = "schema15.xml";      // we need a string id
    super.sliceCount = 2;


    /***
     hash of a is 3c2569b2 high bits=0 shard=shard3
     hash of b is 95de7e03 high bits=2 shard=shard1
     hash of c is e132d65f high bits=3 shard=shard2
     hash of d is 27191473 high bits=0 shard=shard3
     hash of e is 656c4367 high bits=1 shard=shard4
     hash of f is 2b64883b high bits=0 shard=shard3
     hash of g is f18ae416 high bits=3 shard=shard2
     hash of h is d482b2d3 high bits=3 shard=shard2
     hash of i is 811a702b high bits=2 shard=shard1
     hash of j is ca745a39 high bits=3 shard=shard2
     hash of k is cfbda5d1 high bits=3 shard=shard2
     hash of l is 1d5d6a2c high bits=0 shard=shard3
     hash of m is 5ae4385c high bits=1 shard=shard4
     hash of n is c651d8ac high bits=3 shard=shard2
     hash of o is 68348473 high bits=1 shard=shard4
     hash of p is 986fdf9a high bits=2 shard=shard1
     hash of q is ff8209e8 high bits=3 shard=shard2
     hash of r is 5c9373f1 high bits=1 shard=shard4
     hash of s is ff4acaf1 high bits=3 shard=shard2
     hash of t is ca87df4d high bits=3 shard=shard2
     hash of u is 62203ae0 high bits=1 shard=shard4
     hash of v is bdafcc55 high bits=2 shard=shard1
     hash of w is ff439d1f high bits=3 shard=shard2
     hash of x is 3e9a9b1b high bits=0 shard=shard3
     hash of y is 477d9216 high bits=1 shard=shard4
     hash of z is c1f69a17 high bits=3 shard=shard2
     ***/
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(false);

      doTestDocVersions();
      doTestHardFail();

      commit(); // work arround SOLR-5628

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }

  private void doTestHardFail() throws Exception {
    log.info("### STARTING doTestHardFail");

    // use a leader so we test both forwarding and non-forwarding logic
    solrClient = shardToLeaderJetty.get(bucket1).client.solrClient;

    // solrClient = cloudClient;   CloudSolrServer doesn't currently support propagating error codes

    doTestHardFail("p!doc1");
    doTestHardFail("q!doc1");
    doTestHardFail("r!doc1");
    doTestHardFail("x!doc1");
  }

  private void doTestHardFail(String id) throws Exception {
    vdelete(id, 5, "update.chain","external-version-failhard");
    vadd(id, 10, "update.chain","external-version-failhard");
    vadd(id ,15, "update.chain","external-version-failhard");
    vaddFail(id ,11, 409, "update.chain","external-version-failhard");
    vdeleteFail(id ,11, 409, "update.chain","external-version-failhard");
    vdelete(id, 20, "update.chain","external-version-failhard");
  }

  private void doTestDocVersions() throws Exception {
    log.info("### STARTING doTestDocVersions");
    assertEquals(2, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());

    solrClient = cloudClient;

    vadd("b!doc1", 10);
    vadd("c!doc2", 11);
    vadd("d!doc3", 10);
    vadd("e!doc4", 11);

    doRTG("b!doc1,c!doc2,d!doc3,e!doc4", "10,11,10,11");

    vadd("b!doc1", 5);
    vadd("c!doc2", 10);
    vadd("d!doc3", 9);
    vadd("e!doc4", 8);

    doRTG("b!doc1,c!doc2,d!doc3,e!doc4", "10,11,10,11");

    vadd("b!doc1", 24);
    vadd("c!doc2", 23);
    vadd("d!doc3", 22);
    vadd("e!doc4", 21);

    doRTG("b!doc1,c!doc2,d!doc3,e!doc4", "24,23,22,21");

    vdelete("b!doc1", 20);

    doRTG("b!doc1,c!doc2,d!doc3,e!doc4", "24,23,22,21");

    vdelete("b!doc1", 30);

    doRTG("b!doc1,c!doc2,d!doc3,e!doc4", "30,23,22,21");

    // try delete before add
    vdelete("b!doc123", 100);
    vadd("b!doc123", 99);
    doRTG("b!doc123", "100");
    // now add greater
    vadd("b!doc123", 101);
    doRTG("b!doc123", "101");


    //
    // now test with a non-smart client
    //
    // use a leader so we test both forwarding and non-forwarding logic
    solrClient = shardToLeaderJetty.get(bucket1).client.solrClient;

    vadd("b!doc5", 10);
    vadd("c!doc6", 11);
    vadd("d!doc7", 10);
    vadd("e!doc8", 11);

    doRTG("b!doc5,c!doc6,d!doc7,e!doc8", "10,11,10,11");

    vadd("b!doc5", 5);
    vadd("c!doc6", 10);
    vadd("d!doc7", 9);
    vadd("e!doc8", 8);

    doRTG("b!doc5,c!doc6,d!doc7,e!doc8", "10,11,10,11");

    vadd("b!doc5", 24);
    vadd("c!doc6", 23);
    vadd("d!doc7", 22);
    vadd("e!doc8", 21);

    doRTG("b!doc5,c!doc6,d!doc7,e!doc8", "24,23,22,21");

    vdelete("b!doc5", 20);

    doRTG("b!doc5,c!doc6,d!doc7,e!doc8", "24,23,22,21");

    vdelete("b!doc5", 30);

    doRTG("b!doc5,c!doc6,d!doc7,e!doc8", "30,23,22,21");

    // try delete before add
    vdelete("b!doc1234", 100);
    vadd("b!doc1234", 99);
    doRTG("b!doc1234", "100");
    // now add greater
    vadd("b!doc1234", 101);
    doRTG("b!doc1234", "101");

    commit();

    // check liveness for all docs
    doQuery("b!doc123,101,c!doc2,23,d!doc3,22,e!doc4,21,b!doc1234,101,c!doc6,23,d!doc7,22,e!doc8,21", "q","live_b:true");
    doQuery("b!doc1,30,b!doc5,30", "q","live_b:false");

    // delete by query should just work like normal
    doDBQ("id:b!doc1 OR id:e*");
    commit();

    doQuery("b!doc123,101,c!doc2,23,d!doc3,22,b!doc1234,101,c!doc6,23,d!doc7,22", "q","live_b:true");
    doQuery("b!doc5,30", "q","live_b:false");

  }

  SolrClient solrClient;

  void vdelete(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setParam("del_version", Long.toString(version));
    for (int i=0; i<params.length; i+=2) {
      req.setParam( params[i], params[i+1]);
    }
    solrClient.request(req);
    // req.process(cloudClient);
  }

  void vadd(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.add(sdoc("id", id, vfield, version));
    for (int i=0; i<params.length; i+=2) {
      req.setParam( params[i], params[i+1]);
    }
    solrClient.request(req);
  }

  void vaddFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vadd(id, version, params);
    } catch (SolrException e) {
      failed = true;
      assertEquals(errCode, e.code());
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  void vdeleteFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vdelete(id, version, params);
    } catch (SolrException e) {
      failed = true;
      assertEquals(errCode, e.code());
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }


  void doQuery(String expectedDocs, String... queryParams) throws Exception {

    List<String> strs = StrUtils.splitSmart(expectedDocs, ",", true);
    Map<String, Object> expectedIds = new HashMap<>();
    for (int i=0; i<strs.size(); i+=2) {
      String id = strs.get(i);
      String vS = strs.get(i+1);
      Long v = Long.valueOf(vS);
      expectedIds.put(id,v);
    }

    QueryResponse rsp = cloudClient.query(params(queryParams));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  void doRTG(String ids, String versions) throws Exception {
    Map<String, Object> expectedIds = new HashMap<>();
    List<String> strs = StrUtils.splitSmart(ids, ",", true);
    List<String> verS = StrUtils.splitSmart(versions, ",", true);
    for (int i=0; i<strs.size(); i++) {
      expectedIds.put(strs.get(i), Long.valueOf(verS.get(i)));
    }

    solrClient.query(params("qt", "/get", "ids", ids));

    QueryResponse rsp = cloudClient.query(params("qt","/get", "ids",ids));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  void doRTG(String ids) throws Exception {
    solrClient.query(params("qt", "/get", "ids", ids));

    Set<String> expectedIds = new HashSet<>( StrUtils.splitSmart(ids, ",", true) );

    QueryResponse rsp = cloudClient.query(params("qt","/get", "ids",ids));
    Set<String> obtainedIds = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  // TODO: refactor some of this stuff into the SolrJ client... it should be easier to use
  void doDBQ(String q, String... reqParams) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(q);
    req.setParams(params(reqParams));
    req.process(cloudClient);
  }
}
