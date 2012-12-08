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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ShardRoutingTest extends AbstractFullDistribZkTestBase {
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
    super.shardCount = 8;
    super.fixShardCount = true;  // we only want to test with exactly 4 slices.

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
     ***/
  }

  @Override
  public void doTest() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("QTime", SKIPVAL);
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(false);

      doHashingTest();

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }




  private void doHashingTest() throws Exception {
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    String shardKeys = ShardParams.SHARD_KEYS;
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    String bucket1 = "shard1";      // shard1: top bits:10  80000000:bfffffff
    String bucket2 = "shard2";      // shard2: top bits:11  c0000000:ffffffff
    String bucket3 = "shard3";      // shard3: top bits:00  00000000:3fffffff
    String bucket4 = "shard4";      // shard4: top bits:01  40000000:7fffffff

    doAddDoc("b!doc1");
    doAddDoc("c!doc2");
    doAddDoc("d!doc3");
    doAddDoc("e!doc4");
    commit();

    doQuery("b!doc1,c!doc2,d!doc3,e!doc4", "q","*:*");
    doQuery("b!doc1,c!doc2,d!doc3,e!doc4", "q","*:*", "shards","shard1,shard2,shard3,shard4");
    doQuery("b!doc1,c!doc2,d!doc3,e!doc4", "q","*:*", shardKeys,"b!,c!,d!,e!");
    doQuery("b!doc1", "q","*:*", shardKeys,"b!");
    doQuery("b!doc1", "q","*:*", "shards",bucket1);
    doQuery("c!doc2", "q","*:*", shardKeys,"c!");
    doQuery("d!doc3", "q","*:*", shardKeys,"d!");
    doQuery("e!doc4", "q","*:*", shardKeys,"e!");

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b!,c!");
    doQuery("b!doc1,e!doc4", "q","*:*", shardKeys,"b!,e!");

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b,c");     // query shards that would contain *documents* "b" and "c" (i.e. not prefixes).  The upper bits are the same, so the shards should be the same.

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b/1!");   // top bit of hash(b)==1, so shard1 and shard2
    doQuery("d!doc3,e!doc4", "q","*:*", shardKeys,"d/1!");   // top bit of hash(b)==0, so shard3 and shard4

    doQuery("b!doc1,c!doc2", "q","*:*", shardKeys,"b!,c!");


    doQuery("b!doc1,c!doc2,d!doc3,e!doc4", "q","*:*", shardKeys,"foo/0!");
  }

  void doAddDoc(String id) throws Exception {
    index("id",id);
    // todo - target diff servers and use cloud clients as well as non-cloud clients
  }

  void doQuery(String expectedDocs, String... queryParams) throws Exception {
    Set<String> expectedIds = new HashSet<String>( StrUtils.splitSmart(expectedDocs, ",", true) );

    QueryResponse rsp = cloudClient.query(params(queryParams));
    Set<String> obtainedIds = new HashSet<String>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
