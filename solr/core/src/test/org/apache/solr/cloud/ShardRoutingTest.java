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
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;


public class ShardRoutingTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  String bucket1 = "shard1";      // shard1: top bits:10  80000000:bfffffff
  String bucket2 = "shard2";      // shard2: top bits:11  c0000000:ffffffff
  String bucket3 = "shard3";      // shard3: top bits:00  00000000:3fffffff
  String bucket4 = "shard4";      // shard4: top bits:01  40000000:7fffffff


  @BeforeClass
  public static void beforeShardHashingTest() throws Exception {
    // TODO: we use an fs based dir because something
    // like a ram dir will not recover correctly right now
    // because tran log will still exist on restart and ram
    // dir will not persist - perhaps translog can empty on
    // start if using an EphemeralDirectoryFactory 
    useFactory(null);
  }

  public ShardRoutingTest() {
    schemaString = "schema15.xml";      // we need a string id
    super.sliceCount = 4;

    // from negative to positive, the upper bits of the hash ranges should be
    // shard1: top bits:10  80000000:bfffffff
    // shard2: top bits:11  c0000000:ffffffff
    // shard3: top bits:00  00000000:3fffffff
    // shard4: top bits:01  40000000:7fffffff

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

     hash of f1 is 313bf6b1
     hash of f2 is ff143f8

     ***/
  }

  @Test
  @ShardsFixed(num = 8)
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(false);

      doHashingTest();
      doTestNumRequests();
      doAtomicUpdate();

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }




  private void doHashingTest() throws Exception {
    log.info("### STARTING doHashingTest");
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    String shardKeys = ShardParams._ROUTE_;
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.


    doAddDoc("b!doc1");
    doAddDoc("c!doc2");
    doAddDoc("d!doc3");
    doAddDoc("e!doc4");
    doAddDoc("f1!f2!doc5");
    // Check successful addition of a document with a '/' in the id part.
    doAddDoc("f1!f2!doc5/5");

    doRTG("b!doc1");
    doRTG("c!doc2");
    doRTG("d!doc3");
    doRTG("e!doc4");
    doRTG("f1!f2!doc5");
    doRTG("f1!f2!doc5/5");
    doRTG("b!doc1,c!doc2");
    doRTG("d!doc3,e!doc4");

    commit();

    doQuery("b!doc1,c!doc2,d!doc3,e!doc4,f1!f2!doc5,f1!f2!doc5/5", "q","*:*");
    doQuery("b!doc1,c!doc2,d!doc3,e!doc4,f1!f2!doc5,f1!f2!doc5/5", "q","*:*", "shards","shard1,shard2,shard3,shard4");
    doQuery("b!doc1,c!doc2,d!doc3,e!doc4,f1!f2!doc5,f1!f2!doc5/5", "q","*:*", shardKeys,"b!,c!,d!,e!,f1!f2!");
    doQuery("b!doc1", "q","*:*", shardKeys,"b!");
    doQuery("c!doc2", "q","*:*", shardKeys,"c!");
    doQuery("d!doc3,f1!f2!doc5,f1!f2!doc5/5", "q","*:*", shardKeys,"d!");
    doQuery("e!doc4", "q","*:*", shardKeys,"e!");
    doQuery("f1!f2!doc5,d!doc3,f1!f2!doc5/5", "q","*:*", shardKeys,"f1/8!");

    // try using shards parameter
    doQuery("b!doc1", "q","*:*", "shards",bucket1);
    doQuery("c!doc2", "q","*:*", "shards",bucket2);
    doQuery("d!doc3,f1!f2!doc5,f1!f2!doc5/5", "q","*:*", "shards",bucket3);
    doQuery("e!doc4", "q","*:*", "shards",bucket4);


    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b!,c!");
    doQuery("b!doc1,e!doc4", "q","*:*", shardKeys,"b!,e!");

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b,c");     // query shards that would contain *documents* "b" and "c" (i.e. not prefixes).  The upper bits are the same, so the shards should be the same.

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b/1!");   // top bit of hash(b)==1, so shard1 and shard2
    doQuery("d!doc3,e!doc4,f1!f2!doc5,f1!f2!doc5/5", "q","*:*", shardKeys,"d/1!");   // top bit of hash(b)==0, so shard3 and shard4

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b!,c!");

    doQuery("b!doc1,f1!f2!doc5,c!doc2,d!doc3,e!doc4,f1!f2!doc5/5", "q","*:*", shardKeys,"foo/0!");

    // test targeting deleteByQuery at only certain shards
    doDBQ("*:*", shardKeys,"b!");
    commit();
    doQuery("c!doc2,d!doc3,e!doc4,f1!f2!doc5,f1!f2!doc5/5", "q","*:*");
    doAddDoc("b!doc1");

    doDBQ("*:*", shardKeys,"f1!");
    commit();
    doQuery("b!doc1,c!doc2,e!doc4", "q","*:*");
    doAddDoc("f1!f2!doc5");
    doAddDoc("d!doc3");

    doDBQ("*:*", shardKeys,"c!");
    commit();
    doQuery("b!doc1,f1!f2!doc5,d!doc3,e!doc4", "q","*:*");
    doAddDoc("c!doc2");

    doDBQ("*:*", shardKeys,"d!,e!");
    commit();
    doQuery("b!doc1,c!doc2", "q","*:*");
    doAddDoc("d!doc3");
    doAddDoc("e!doc4");
    doAddDoc("f1!f2!doc5");

    commit();

    doDBQ("*:*");
    commit();

    doAddDoc("b!");
    doAddDoc("c!doc1");
    commit();
    doQuery("b!,c!doc1", "q","*:*");
    UpdateRequest req = new UpdateRequest();
    req.deleteById("b!");
    req.process(cloudClient);
    commit();
    doQuery("c!doc1", "q","*:*");

    doDBQ("id:b!");
    commit();
    doQuery("c!doc1", "q","*:*");

    doDBQ("*:*");
    commit();

    doAddDoc("a!b!");
    doAddDoc("b!doc1");
    doAddDoc("c!doc2");
    doAddDoc("d!doc3");
    doAddDoc("e!doc4");
    doAddDoc("f1!f2!doc5");
    doAddDoc("f1!f2!doc5/5");
    commit();
    doQuery("a!b!,b!doc1,c!doc2,d!doc3,e!doc4,f1!f2!doc5,f1!f2!doc5/5", "q","*:*");
  }


  public void doTestNumRequests() throws Exception {
    log.info("### STARTING doTestNumRequests");

    List<CloudJettyRunner> runners = shardToJetty.get(bucket1);
    CloudJettyRunner leader = shardToLeaderJetty.get(bucket1);
    CloudJettyRunner replica =  null;
    for (CloudJettyRunner r : runners) {
      if (r != leader) replica = r;
    }

    long nStart = getNumRequests();
    leader.client.solrClient.add( sdoc("id","b!doc1") );
    long nEnd = getNumRequests();
    assertEquals(2, nEnd - nStart);   // one request to leader, which makes another to a replica


    nStart = getNumRequests();
    replica.client.solrClient.add( sdoc("id","b!doc1") );
    nEnd = getNumRequests();
    assertEquals(3, nEnd - nStart);   // orig request + replica forwards to leader, which forward back to replica.

    nStart = getNumRequests();
    replica.client.solrClient.add( sdoc("id","b!doc1") );
    nEnd = getNumRequests();
    assertEquals(3, nEnd - nStart);   // orig request + replica forwards to leader, which forward back to replica.

    CloudJettyRunner leader2 = shardToLeaderJetty.get(bucket2);


    nStart = getNumRequests();
    replica.client.solrClient.query( params("q","*:*", "shards",bucket1) );
    nEnd = getNumRequests();
    assertEquals(1, nEnd - nStart);   // short circuit should prevent distrib search

    nStart = getNumRequests();
    replica.client.solrClient.query( params("q","*:*", ShardParams._ROUTE_, "b!") );
    nEnd = getNumRequests();
    assertEquals(1, nEnd - nStart);   // short circuit should prevent distrib search

    nStart = getNumRequests();
    leader2.client.solrClient.query( params("q","*:*", ShardParams._ROUTE_, "b!") );
    nEnd = getNumRequests();
    assertEquals(3, nEnd - nStart);   // original + 2 phase distrib search.  we could improve this!

    nStart = getNumRequests();
    leader2.client.solrClient.query( params("q","*:*") );
    nEnd = getNumRequests();
    assertEquals(9, nEnd - nStart);   // original + 2 phase distrib search * 4 shards.

    nStart = getNumRequests();
    leader2.client.solrClient.query( params("q","*:*", ShardParams._ROUTE_, "b!,d!") );
    nEnd = getNumRequests();
    assertEquals(5, nEnd - nStart);   // original + 2 phase distrib search * 2 shards.

    nStart = getNumRequests();
    leader2.client.solrClient.query( params("q","*:*", ShardParams._ROUTE_, "b!,f1!f2!") );
    nEnd = getNumRequests();
    assertEquals(5, nEnd - nStart);
  }

  public void doAtomicUpdate() throws Exception {
    log.info("### STARTING doAtomicUpdate");
    int nClients = clients.size();
    assertEquals(8, nClients);

    int expectedVal = 0;
    for (SolrClient client : clients) {
      client.add(sdoc("id", "b!doc", "foo_i", map("inc",1)));
      expectedVal++;

      QueryResponse rsp = client.query(params("qt","/get", "id","b!doc"));
      Object val = ((Map)rsp.getResponse().get("doc")).get("foo_i");
      assertEquals((Integer)expectedVal, val);
    }

  }

  long getNumRequests() {
    long n = controlJetty.getDebugFilter().getTotalRequests();
    for (JettySolrRunner jetty : jettys) {
      n += jetty.getDebugFilter().getTotalRequests();
    }
    return n;
  }


  void doAddDoc(String id) throws Exception {
    index("id",id);
    // todo - target diff servers and use cloud clients as well as non-cloud clients
  }

  void doRTG(String ids) throws Exception {
    doQuery(ids, "qt", "/get", "ids", ids);
  }

  // TODO: refactor some of this stuff into the SolrJ client... it should be easier to use
  void doDBQ(String q, String... reqParams) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(q);
    req.setParams(params(reqParams));
    req.process(cloudClient);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
