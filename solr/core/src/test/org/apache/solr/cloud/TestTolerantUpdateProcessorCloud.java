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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.ToleratedUpdateError;
import org.apache.solr.common.ToleratedUpdateError.CmdType;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of TolerantUpdateProcessor using a MiniSolrCloud.  Updates (that include failures which 
 * should be tolerated) are explicitly tested against various initial nodes to confirm correct 
 * behavior regardless of routing.
 *
 * <p>
 * <b>NOTE:</b> This test sets up a static instance of MiniSolrCloud with a single collection 
 * and several clients pointed at specific nodes. These are all re-used across multiple test methods, 
 * and assumes that the state of the cluster is healthy.
 * </p>
 *
 */
public class TestTolerantUpdateProcessorCloud extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_SHARDS = 2; 
  private static final int REPLICATION_FACTOR = 2; 
  private static final int NUM_SERVERS = 5; 
  
  private static final String COLLECTION_NAME = "test_col";
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;

  /** A client for talking directly to the leader of shard1 */
  private static HttpSolrClient S_ONE_LEADER_CLIENT;
  
  /** A client for talking directly to the leader of shard2 */
  private static HttpSolrClient S_TWO_LEADER_CLIENT;

  /** A client for talking directly to a passive replica of shard1 */
  private static HttpSolrClient S_ONE_NON_LEADER_CLIENT;
  
  /** A client for talking directly to a passive replica of shard2 */
  private static HttpSolrClient S_TWO_NON_LEADER_CLIENT;

  /** A client for talking directly to a node that has no piece of the collection */
  private static HttpSolrClient NO_COLLECTION_CLIENT;
  
  /** id field doc routing prefix for shard1 */
  private static final String S_ONE_PRE = "abc!";
  
  /** id field doc routing prefix for shard2 */
  private static final String S_TWO_PRE = "XYZ!";
  
  @BeforeClass
  public static void createMiniSolrCloudCluster() throws Exception {
    
    final String configName = "solrCloudCollectionConfig";
    final File configDir = new File(TEST_HOME() + File.separator + "collection1" + File.separator + "conf");

    configureCluster(NUM_SERVERS)
      .addConfig(configName, configDir.toPath())
      .configure();

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, NUM_SHARDS, REPLICATION_FACTOR)
        .withProperty("config", "solrconfig-distrib-update-processor-chains.xml")
        .withProperty("schema", "schema15.xml") // string id for doc routing prefix
        .process(CLOUD_CLIENT);
    
    cluster.waitForActiveCollection(COLLECTION_NAME, NUM_SHARDS, REPLICATION_FACTOR * NUM_SHARDS);

    ZkStateReader zkStateReader = CLOUD_CLIENT.getZkStateReader();
    // really hackish way to get a URL for specific nodes based on shard/replica hosting
    // inspired by TestMiniSolrCloudCluster
    HashMap<String, String> urlMap = new HashMap<>();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      URL jettyURL = jetty.getBaseUrl();
      String nodeKey = jettyURL.getHost() + ":" + jettyURL.getPort() + jettyURL.getPath().replace("/","_");
      urlMap.put(nodeKey, jettyURL.toString());
    }
    zkStateReader.forceUpdateCollection(COLLECTION_NAME);
    ClusterState clusterState = zkStateReader.getClusterState();
    for (Slice slice : clusterState.getCollection(COLLECTION_NAME).getSlices()) {
      String shardName = slice.getName();
      Replica leader = slice.getLeader();
      assertNotNull("slice has null leader: " + slice.toString(), leader);
      assertNotNull("slice leader has null node name: " + slice.toString(), leader.getNodeName());
      String leaderUrl = urlMap.remove(leader.getNodeName());
      assertNotNull("could not find URL for " + shardName + " leader: " + leader.getNodeName(),
                    leaderUrl);
      assertEquals("expected two total replicas for: " + slice.getName(),
                   2, slice.getReplicas().size());
      
      String passiveUrl = null;
      
      for (Replica replica : slice.getReplicas()) {
        if ( ! replica.equals(leader)) {
          passiveUrl = urlMap.remove(replica.getNodeName());
          assertNotNull("could not find URL for " + shardName + " replica: " + replica.getNodeName(),
                        passiveUrl);
        }
      }
      assertNotNull("could not find URL for " + shardName + " replica", passiveUrl);

      if (shardName.equals("shard1")) {
        S_ONE_LEADER_CLIENT = getHttpSolrClient(leaderUrl + "/" + COLLECTION_NAME + "/");
        S_ONE_NON_LEADER_CLIENT = getHttpSolrClient(passiveUrl + "/" + COLLECTION_NAME + "/");
      } else if (shardName.equals("shard2")) {
        S_TWO_LEADER_CLIENT = getHttpSolrClient(leaderUrl + "/" + COLLECTION_NAME + "/");
        S_TWO_NON_LEADER_CLIENT = getHttpSolrClient(passiveUrl + "/" + COLLECTION_NAME + "/");
      } else {
        fail("unexpected shard: " + shardName);
      }
    }
    assertEquals("Should be exactly one server left (nost hosting either shard)", 1, urlMap.size());
    NO_COLLECTION_CLIENT = getHttpSolrClient(urlMap.values().iterator().next() +
                                              "/" + COLLECTION_NAME + "/");
    
    assertNotNull(S_ONE_LEADER_CLIENT);
    assertNotNull(S_TWO_LEADER_CLIENT);
    assertNotNull(S_ONE_NON_LEADER_CLIENT);
    assertNotNull(S_TWO_NON_LEADER_CLIENT);
    assertNotNull(NO_COLLECTION_CLIENT);

    // sanity check that our S_ONE_PRE & S_TWO_PRE really do map to shard1 & shard2 with default routing
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_ONE_PRE + random().nextInt()),
                                         f("expected_shard_s", "shard1"))).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_TWO_PRE + random().nextInt()),
                                         f("expected_shard_s", "shard2"))).getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
    SolrDocumentList docs = CLOUD_CLIENT.query(params("q", "*:*",
                                                      "fl","id,expected_shard_s,[shard]")).getResults();
    assertEquals(2, docs.getNumFound());
    assertEquals(2, docs.size());
    for (SolrDocument doc : docs) {
      String expected = COLLECTION_NAME + "_" + doc.getFirstValue("expected_shard_s") + "_replica";
      String docShard = doc.getFirstValue("[shard]").toString();
      assertTrue("shard routing prefixes don't seem to be aligned anymore, " +
                 "did someone change the default routing rules? " +
                 "and/or the the default core name rules? " +
                 "and/or the numShards used by this test? ... " +
                 "couldn't find " + expected + " as substring of [shard] == '" + docShard +
                 "' ... for docId == " + doc.getFirstValue("id"),
                 docShard.contains(expected));
    }
  }
  
  @AfterClass
  public static void afterClass() throws IOException {
   close(S_ONE_LEADER_CLIENT); S_ONE_LEADER_CLIENT = null;
   close(S_TWO_LEADER_CLIENT); S_TWO_LEADER_CLIENT = null;
   close(S_ONE_NON_LEADER_CLIENT); S_ONE_NON_LEADER_CLIENT = null;
   close(S_TWO_NON_LEADER_CLIENT); S_TWO_NON_LEADER_CLIENT = null;
   close(NO_COLLECTION_CLIENT); NO_COLLECTION_CLIENT = null;
   close(CLOUD_CLIENT); CLOUD_CLIENT = null;
  }
  
  private static void close(SolrClient client) throws IOException {
    if (client != null) {
      client.close();
    }
  }
  
  @Before
  private void clearCollection() throws Exception {
    assertEquals(0, CLOUD_CLIENT.deleteByQuery("*:*").getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
  }

  public void testSanity() throws Exception {
    
    // verify some basic sanity checking of indexing & querying across the collection
    // w/o using our custom update processor chain
    
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_ONE_PRE + "1"),
                                         f("foo_i", 42))).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_TWO_PRE + "2"),
                                         f("foo_i", 66))).getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());

    for (SolrClient c : Arrays.asList(S_ONE_LEADER_CLIENT, S_TWO_LEADER_CLIENT,
                                      S_ONE_NON_LEADER_CLIENT, S_TWO_NON_LEADER_CLIENT,
                                      NO_COLLECTION_CLIENT, CLOUD_CLIENT)) {
      assertQueryDocIds(c, true, S_ONE_PRE + "1",  S_TWO_PRE + "2");
      assertQueryDocIds(c, false, "id_not_exists");

      // verify adding 2 broken docs causes a clint exception
      SolrException e = expectThrows(SolrException.class,
          "did not get a top level exception when more then 10 docs failed", () ->
              update(params(),
                  doc(f("id", S_ONE_PRE + "X"), f("foo_i", "bogus_val_X")),
                  doc(f("id", S_TWO_PRE + "Y"), f("foo_i", "bogus_val_Y"))
              ).process(c)
      );
      assertEquals("not the type of error we were expecting ("+e.code()+"): " + e.toString(),
          400, e.code());
        
      // verify malformed deleteByQuerys fail
      e = expectThrows(SolrException.class,
          "sanity check for malformed DBQ didn't fail",
          () -> update(params()).deleteByQuery("foo_i:not_a_num").process(c));
      assertEquals("not the expected DBQ failure: " + e.getMessage(), 400, e.code());
      
      // verify opportunistic concurrency deletions fail as we expect when docs are / aren't present
      for (UpdateRequest r : new UpdateRequest[] {
          update(params("commit", "true")).deleteById(S_ONE_PRE + "1", -1L),
          update(params("commit", "true")).deleteById(S_TWO_PRE + "2", -1L),
          update(params("commit", "true")).deleteById("id_not_exists",  1L)    }) {
        e = expectThrows(SolrException.class, "sanity check for opportunistic concurrency delete didn't fail",
            () -> r.process(c)
        );
        assertEquals("not the expected opportunistic concurrency failure code: "
            + r.toString() + " => " + e.getMessage(), 409, e.code());
      }
    }
  }

  //
  public void testVariousDeletesViaCloudClient() throws Exception {
    testVariousDeletes(CLOUD_CLIENT);
  }
  public void testVariousDeletesViaShard1LeaderClient() throws Exception {
    testVariousDeletes(S_ONE_LEADER_CLIENT);
  }
  public void testVariousDeletesViaShard2LeaderClient() throws Exception {
    testVariousDeletes(S_TWO_LEADER_CLIENT);
  }
  public void testVariousDeletesViaShard1NonLeaderClient() throws Exception {
    testVariousDeletes(S_ONE_NON_LEADER_CLIENT);
  }
  public void testVariousDeletesViaShard2NonLeaderClient() throws Exception {
    testVariousDeletes(S_TWO_NON_LEADER_CLIENT);
  }
  public void testVariousDeletesViaNoCollectionClient() throws Exception {
    testVariousDeletes(NO_COLLECTION_CLIENT);
  }
  
  protected static void testVariousDeletes(SolrClient client) throws Exception {
    assertNotNull("client not initialized", client);

    // 2 docs, one on each shard
    final String docId1 = S_ONE_PRE + "42";
    final String docId2 = S_TWO_PRE + "666";
    
    UpdateResponse rsp = null;
    
    // add 1 doc to each shard
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", docId1), f("foo_i", "2001")),
                 doc(f("id", docId2), f("foo_i", "1976"))).process(client);
    assertEquals(0, rsp.getStatus());

    // attempt to delete individual doc id(s) that should fail because of opportunistic concurrency constraints
    for (String id : new String[] { docId1, docId2 }) {
      rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                          "commit", "true")).deleteById(id, -1L).process(client);
      assertEquals(0, rsp.getStatus());
      assertUpdateTolerantErrors("failed opportunistic concurrent delId="+id, rsp,
                                 delIErr(id));
    }
    
    // multiple failed deletes from the same shard (via opportunistic concurrent w/ bogus ids)
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")
                 ).deleteById(S_ONE_PRE + "X", +1L).deleteById(S_ONE_PRE + "Y", +1L).process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by id for 2 bogus docs", rsp,
                               delIErr(S_ONE_PRE + "X"), delIErr(S_ONE_PRE + "Y"));
    assertQueryDocIds(client, true, docId1, docId2);
    
    // multiple failed deletes from the diff shards due to opportunistic concurrency constraints
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")).deleteById(docId2, -1L).deleteById(docId1, -1L).process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by id for 2 docs", rsp,
                               delIErr(docId1), delIErr(docId2));
    assertQueryDocIds(client, true, docId1, docId2);

    // deleteByQuery using malformed query (fail)
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")).deleteByQuery("bogus_field:foo").process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by query", rsp,
                               delQErr("bogus_field:foo"));
    assertQueryDocIds(client, true, docId1, docId2);

    // mix 2 deleteByQuery, one malformed (fail), one that doesn't match anything (ok)
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")
                 ).deleteByQuery("bogus_field:foo").deleteByQuery("foo_i:23").process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by query", rsp,
                               delQErr("bogus_field:foo"));
    assertQueryDocIds(client, true, docId1, docId2);
    
    // mix 2 deleteById using _version_=-1, one for real doc1 (fail), one for bogus id (ok)
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")
                 ).deleteById(docId1, -1L).deleteById("bogus", -1L).process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by id: exists", rsp,
                               delIErr(docId1));
    assertQueryDocIds(client, true, docId1, docId2);
    
    // mix 2 deleteById using _version_=1, one for real doc1 (ok, deleted), one for bogus id (fail)
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")
                 ).deleteById(docId1, +1L).deleteById("bogusId", +1L).process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by id: bogus", rsp,
                               delIErr("bogusId"));
    assertQueryDocIds(client, false, docId1);
    assertQueryDocIds(client, true, docId2);
    
    // mix 2 deleteByQuery, one malformed (fail), one that alctaully removes some docs (ok)
    assertQueryDocIds(client, true, docId2);
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true")
                 ).deleteByQuery("bogus_field:foo").deleteByQuery("foo_i:1976").process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("failed opportunistic concurrent delete by query", rsp,
                               delQErr("bogus_field:foo"));
    assertQueryDocIds(client, false, docId2);

  }

  
  //
  public void testVariousAddsViaCloudClient() throws Exception {
    testVariousAdds(CLOUD_CLIENT);
  }
  public void testVariousAddsViaShard1LeaderClient() throws Exception {
    testVariousAdds(S_ONE_LEADER_CLIENT);
  }
  public void testVariousAddsViaShard2LeaderClient() throws Exception {
    testVariousAdds(S_TWO_LEADER_CLIENT);
  }
  public void testVariousAddsViaShard1NonLeaderClient() throws Exception {
    testVariousAdds(S_ONE_NON_LEADER_CLIENT);
  }
  public void testVariousAddsViaShard2NonLeaderClient() throws Exception {
    testVariousAdds(S_TWO_NON_LEADER_CLIENT);
  }
  public void testVariousAddsViaNoCollectionClient() throws Exception {
    testVariousAdds(NO_COLLECTION_CLIENT);
  }

  protected static void testVariousAdds(SolrClient client) throws Exception {
    assertNotNull("client not initialized", client);
    
    UpdateResponse rsp = null;

    // 2 docs that are both on shard1, the first one should fail
    for (int maxErrors : new int[] { -1, 2, 47, 10 }) {
      // regardless of which of these maxErrors values we use, behavior should be the same...
      rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                          "maxErrors", ""+maxErrors,
                          "commit", "true"),
                   doc(f("id", S_ONE_PRE + "42"), f("foo_i", "bogus_value")),
                   doc(f("id", S_ONE_PRE + "666"), f("foo_i", "1976"))).process(client);
      
      assertEquals(0, rsp.getStatus());
      assertUpdateTolerantAddErrors("single shard, 1st doc should fail", rsp, S_ONE_PRE + "42");
      assertEquals(0, client.commit().getStatus());
      assertQueryDocIds(client, false, S_ONE_PRE + "42");
      assertQueryDocIds(client, true, S_ONE_PRE + "666");

      // ...only diff should be that we get an accurate report of the effective maxErrors
      assertEquals(maxErrors, rsp.getResponseHeader().get("maxErrors"));
    }
    
    // 2 docs that are both on shard1, the second one should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-not-set",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "55"), f("foo_i", "1976")),
                 doc(f("id", S_ONE_PRE + "77"), f("foo_i", "bogus_val"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantAddErrors("single shard, 2nd doc should fail", rsp, S_ONE_PRE + "77");
    assertQueryDocIds(client, false, S_ONE_PRE + "77");
    assertQueryDocIds(client, true, S_ONE_PRE + "666", S_ONE_PRE + "55");
    // since maxErrors is unset, we should get an "unlimited" value back
    assertEquals(-1, rsp.getResponseHeader().get("maxErrors"));

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());

    // 2 docs on 2 diff shards, first of which should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "42"), f("foo_i", "bogus_value")),
                 doc(f("id", S_TWO_PRE + "666"), f("foo_i", "1976"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantAddErrors("two shards, 1st doc should fail", rsp, S_ONE_PRE + "42");
    assertEquals(0, client.commit().getStatus());
    assertQueryDocIds(client, false, S_ONE_PRE + "42");
    assertQueryDocIds(client, true, S_TWO_PRE + "666");
    
    // 2 docs on 2 diff shards, second of which should fail

    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "55"), f("foo_i", "1976")),
                 doc(f("id", S_TWO_PRE + "77"), f("foo_i", "bogus_val"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantAddErrors("two shards, 2nd doc should fail", rsp, S_TWO_PRE + "77");
    assertQueryDocIds(client, false, S_TWO_PRE + "77");
    assertQueryDocIds(client, true, S_TWO_PRE + "666", S_ONE_PRE + "55");

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());

    // many docs from diff shards, 1 from each shard should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "11")),
                 doc(f("id", S_TWO_PRE + "21")),
                 doc(f("id", S_ONE_PRE + "12")),
                 doc(f("id", S_TWO_PRE + "22"), f("foo_i", "bogus_val")),
                 doc(f("id", S_ONE_PRE + "13")),
                 doc(f("id", S_TWO_PRE + "23")),
                 doc(f("id", S_ONE_PRE + "14")),
                 doc(f("id", S_TWO_PRE + "24")),
                 doc(f("id", S_ONE_PRE + "15"), f("foo_i", "bogus_val")),
                 doc(f("id", S_TWO_PRE + "25")),
                 doc(f("id", S_ONE_PRE + "16")),
                 doc(f("id", S_TWO_PRE + "26"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantAddErrors("many docs, 1 from each shard should fail", rsp,
                                  S_ONE_PRE + "15",
                                  S_TWO_PRE + "22");
    assertQueryDocIds(client, false, S_TWO_PRE + "22", S_ONE_PRE + "15");
    assertQueryDocIds(client, true,
                      S_ONE_PRE + "11", S_TWO_PRE + "21", S_ONE_PRE + "12",
                      S_ONE_PRE + "13", S_TWO_PRE + "23", S_ONE_PRE + "14", S_TWO_PRE + "24",
                      S_TWO_PRE + "25", S_ONE_PRE + "16", S_TWO_PRE + "26");

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());

    // many docs from diff shards, 1 from each shard should fail and 1 w/o uniqueKey
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "11")),
                 doc(f("id", S_TWO_PRE + "21")),
                 doc(f("id", S_ONE_PRE + "12")),
                 doc(f("id", S_TWO_PRE + "22"), f("foo_i", "bogus_val")),
                 doc(f("id", S_ONE_PRE + "13")),
                 doc(f("id", S_TWO_PRE + "23")),
                 doc(f("foo_i", "42")),          // no "id"
                 doc(f("id", S_ONE_PRE + "14")),
                 doc(f("id", S_TWO_PRE + "24")),
                 doc(f("id", S_ONE_PRE + "15"), f("foo_i", "bogus_val")),
                 doc(f("id", S_TWO_PRE + "25")),
                 doc(f("id", S_ONE_PRE + "16")),
                 doc(f("id", S_TWO_PRE + "26"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantAddErrors("many docs, 1 from each shard (+ no id) should fail", rsp,
                                  S_ONE_PRE + "15",
                                  "(unknown)",
                                  S_TWO_PRE + "22");
    assertQueryDocIds(client, false, S_TWO_PRE + "22", S_ONE_PRE + "15");
    assertQueryDocIds(client, true,
                      S_ONE_PRE + "11", S_TWO_PRE + "21", S_ONE_PRE + "12",
                      S_ONE_PRE + "13", S_TWO_PRE + "23", S_ONE_PRE + "14", S_TWO_PRE + "24",
                      S_TWO_PRE + "25", S_ONE_PRE + "16", S_TWO_PRE + "26");

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());
    
    // many docs from diff shards, more then 10 (total) should fail
    SolrException e = expectThrows(SolrException.class,
        "did not get a top level exception when more then 10 docs failed",
        () -> update(params("update.chain", "tolerant-chain-max-errors-10", "commit", "true"),
            doc(f("id", S_ONE_PRE + "11")),
            doc(f("id", S_TWO_PRE + "21"), f("foo_i", "bogus_val")),
            doc(f("id", S_ONE_PRE + "12")),
            doc(f("id", S_TWO_PRE + "22"), f("foo_i", "bogus_val")),
            doc(f("id", S_ONE_PRE + "13")),
            doc(f("id", S_TWO_PRE + "23"), f("foo_i", "bogus_val")),
            doc(f("id", S_ONE_PRE + "14"), f("foo_i", "bogus_val")),
            doc(f("id", S_TWO_PRE + "24")),
            doc(f("id", S_ONE_PRE + "15"), f("foo_i", "bogus_val")),
            doc(f("id", S_TWO_PRE + "25")),
            doc(f("id", S_ONE_PRE + "16"), f("foo_i", "bogus_val")),
            doc(f("id", S_TWO_PRE + "26"), f("foo_i", "bogus_val")),
            doc(f("id", S_ONE_PRE + "17")),
            doc(f("id", S_TWO_PRE + "27")),
            doc(f("id", S_ONE_PRE + "18"), f("foo_i", "bogus_val")),
            doc(f("id", S_TWO_PRE + "28"), f("foo_i", "bogus_val")),
            doc(f("id", S_ONE_PRE + "19"), f("foo_i", "bogus_val")),
            doc(f("id", S_TWO_PRE + "29"), f("foo_i", "bogus_val")),
            doc(f("id", S_ONE_PRE + "10")), // may be skipped, more then 10 fails
            doc(f("id", S_TWO_PRE + "20"))  // may be skipped, more then 10 fails
            ).process(client)
    );
    {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed -- see SOLR-8830
      assertEquals("not the type of error we were expecting (" + e.code() + "): " + e.toString(),
          // NOTE: we always expect a 400 because we know that's what we would get from these types of errors
          // on a single node setup -- a 5xx type error isn't something we should have triggered
          400, e.code());

      // verify that the Exceptions metadata can tell us what failed.
      NamedList<String> remoteErrMetadata = e.getMetadata();
      assertNotNull("no metadata in: " + e.toString(), remoteErrMetadata);
      Set<ToleratedUpdateError> actualKnownErrs
          = new LinkedHashSet<ToleratedUpdateError>(remoteErrMetadata.size());
      int actualKnownErrsCount = 0;
      for (int i = 0; i < remoteErrMetadata.size(); i++) {
        ToleratedUpdateError err =
            ToleratedUpdateError.parseMetadataIfToleratedUpdateError(remoteErrMetadata.getName(i),
                remoteErrMetadata.getVal(i));
        if (null == err) {
          // some metadata unrelated to this update processor
          continue;
        }
        actualKnownErrsCount++;
        actualKnownErrs.add(err);
      }
      assertEquals("wrong number of errors in metadata: " + remoteErrMetadata.toString(),
          11, actualKnownErrsCount);
      assertEquals("at least one dup error in metadata: " + remoteErrMetadata.toString(),
          actualKnownErrsCount, actualKnownErrs.size());
      for (ToleratedUpdateError err : actualKnownErrs) {
        assertEquals("only expected type of error is ADD: " + err,
            CmdType.ADD, err.getType());
        assertTrue("failed err msg didn't match expected value: " + err,
            err.getMessage().contains("bogus_val"));
      }
    }

    assertEquals(0, client.commit().getStatus()); // need to force since update didn't finish
    assertQueryDocIds(client, false
                      // explicitly failed
                      , S_TWO_PRE + "21", S_TWO_PRE + "22", S_TWO_PRE + "23", S_ONE_PRE + "14"
                      , S_ONE_PRE + "15", S_ONE_PRE + "16", S_TWO_PRE + "26", S_ONE_PRE + "18"
                      , S_TWO_PRE + "28", S_ONE_PRE + "19", S_TWO_PRE + "29"
                      //
                      // // we can't assert for sure these docs were skipped
                      // // depending on shard we hit, they may have been added async before errors were exceeded
                      // , S_ONE_PRE + "10", S_TWO_PRE + "20" // skipped
                      );
    assertQueryDocIds(client, true,
                      S_ONE_PRE + "11", S_ONE_PRE + "12", S_ONE_PRE + "13", S_TWO_PRE + "24",
                      S_TWO_PRE + "25", S_ONE_PRE + "17", S_TWO_PRE + "27");
    
    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());
    
    // many docs from diff shards, more then 10 from a single shard (two) should fail

    e = expectThrows(SolrException.class, "did not get a top level exception when more then 10 docs failed",
        () -> {
      ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(30);
      docs.add(doc(f("id", S_ONE_PRE + "z")));
      docs.add(doc(f("id", S_TWO_PRE + "z")));
      docs.add(doc(f("id", S_ONE_PRE + "y")));
      docs.add(doc(f("id", S_TWO_PRE + "y")));
      for (int i = 0; i < 11; i++) {
        docs.add(doc(f("id", S_ONE_PRE + i)));
        docs.add(doc(f("id", S_TWO_PRE + i), f("foo_i", "bogus_val")));
      }
      docs.add(doc(f("id", S_ONE_PRE + "x"))); // may be skipped, more then 10 fails
      docs.add(doc(f("id", S_TWO_PRE + "x"))); // may be skipped, more then 10 fails

      update(params("update.chain", "tolerant-chain-max-errors-10",
          "commit", "true"),
          docs.toArray(new SolrInputDocument[docs.size()])).process(client);
    });

    {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed -- see SOLR-8830
      assertEquals("not the type of error we were expecting ("+e.code()+"): " + e.toString(),
          // NOTE: we always expect a 400 because we know that's what we would get from these types of errors
          // on a single node setup -- a 5xx type error isn't something we should have triggered
          400, e.code());

      // verify that the Exceptions metadata can tell us what failed.
      NamedList<String> remoteErrMetadata = e.getMetadata();
      assertNotNull("no metadata in: " + e.toString(), remoteErrMetadata);
      Set<ToleratedUpdateError> actualKnownErrs
          = new LinkedHashSet<ToleratedUpdateError>(remoteErrMetadata.size());
      int actualKnownErrsCount = 0;
      for (int i = 0; i < remoteErrMetadata.size(); i++) {
        ToleratedUpdateError err =
            ToleratedUpdateError.parseMetadataIfToleratedUpdateError(remoteErrMetadata.getName(i),
                remoteErrMetadata.getVal(i));
        if (null == err) {
          // some metadata unrelated to this update processor
          continue;
        }
        actualKnownErrsCount++;
        actualKnownErrs.add(err);
      }
      assertEquals("wrong number of errors in metadata: " + remoteErrMetadata.toString(),
          11, actualKnownErrsCount);
      assertEquals("at least one dup error in metadata: " + remoteErrMetadata.toString(),
          actualKnownErrsCount, actualKnownErrs.size());
      for (ToleratedUpdateError err : actualKnownErrs) {
        assertEquals("only expected type of error is ADD: " + err,
            CmdType.ADD, err.getType());
        assertTrue("failed id had unexpected prefix: " + err,
            err.getId().startsWith(S_TWO_PRE));
        assertTrue("failed err msg didn't match expected value: " + err,
            err.getMessage().contains("bogus_val"));
      }
    }

    assertEquals(0, client.commit().getStatus()); // need to force since update didn't finish
    assertQueryDocIds(client, true
                      , S_ONE_PRE + "z", S_ONE_PRE + "y", S_TWO_PRE + "z", S_TWO_PRE + "y" // first
                      //
                      , S_ONE_PRE + "0", S_ONE_PRE + "1", S_ONE_PRE + "2", S_ONE_PRE + "3", S_ONE_PRE + "4"
                      , S_ONE_PRE + "5", S_ONE_PRE + "6", S_ONE_PRE + "7", S_ONE_PRE + "8", S_ONE_PRE + "9"
                      );
    assertQueryDocIds(client, false
                      // explicitly failed
                      , S_TWO_PRE + "0", S_TWO_PRE + "1", S_TWO_PRE + "2", S_TWO_PRE + "3", S_TWO_PRE + "4"
                      , S_TWO_PRE + "5", S_TWO_PRE + "6", S_TWO_PRE + "7", S_TWO_PRE + "8", S_TWO_PRE + "9"
                      //
                      // // we can't assert for sure these docs were skipped
                      // // depending on shard we hit, they may have been added async before errors were exceeded
                      // , S_ONE_PRE + "x", S_TWO_PRE + "x", // skipped
                      );

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());
    
    // many docs from diff shards, more then 10 don't have any uniqueKey specified

    e = expectThrows(SolrException.class,
        "did not get a top level exception when more then 10 docs mising uniqueKey",
        () -> {
      ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(30);
      docs.add(doc(f("id", S_ONE_PRE + "z")));
      docs.add(doc(f("id", S_TWO_PRE + "z")));
      docs.add(doc(f("id", S_ONE_PRE + "y")));
      docs.add(doc(f("id", S_TWO_PRE + "y")));
      for (int i = 0; i < 11; i++) {
        // no "id" field
        docs.add(doc(f("foo_i", "" + i)));
      }
      docs.add(doc(f("id", S_ONE_PRE + "x"))); // may be skipped, more then 10 fails
      docs.add(doc(f("id", S_TWO_PRE + "x"))); // may be skipped, more then 10 fails

      update(params("update.chain", "tolerant-chain-max-errors-10",
          "commit", "true"),
          docs.toArray(new SolrInputDocument[docs.size()])).process(client);
    });

    {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed -- see SOLR-8830
      assertEquals("not the type of error we were expecting ("+e.code()+"): " + e.toString(),
          // NOTE: we always expect a 400 because we know that's what we would get from these types of errors
          // on a single node setup -- a 5xx type error isn't something we should have triggered
          400, e.code());

      // verify that the Exceptions metadata can tell us what failed.
      NamedList<String> remoteErrMetadata = e.getMetadata();
      assertNotNull("no metadata in: " + e.toString(), remoteErrMetadata);
      int actualKnownErrsCount = 0;
      for (int i = 0; i < remoteErrMetadata.size(); i++) {
        ToleratedUpdateError err =
            ToleratedUpdateError.parseMetadataIfToleratedUpdateError(remoteErrMetadata.getName(i),
                remoteErrMetadata.getVal(i));
        if (null == err) {
          // some metadata unrelated to this update processor
          continue;
        }
        actualKnownErrsCount++;
        assertEquals("only expected type of error is ADD: " + err,
            CmdType.ADD, err.getType());
        assertTrue("failed id didn't match 'unknown': " + err,
            err.getId().contains("unknown"));
      }
      assertEquals("wrong number of errors in metadata: " + remoteErrMetadata.toString(),
          11, actualKnownErrsCount);
    }

    assertEquals(0, client.commit().getStatus()); // need to force since update didn't finish
    assertQueryDocIds(client, true
                      , S_ONE_PRE + "z", S_ONE_PRE + "y", S_TWO_PRE + "z", S_TWO_PRE + "y" // first
                      // // we can't assert for sure these docs were skipped or added
                      // // depending on shard we hit, they may have been added async before errors were exceeded
                      // , S_ONE_PRE + "x", S_TWO_PRE + "x" // skipped
                      );

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());
    
    // many docs from diff shards, more then 10 from a single shard (two) should fail but
    // request should still succeed because of maxErrors=-1 param

    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(30);
    ArrayList<ExpectedErr> expectedErrs = new ArrayList<ExpectedErr>(30);
    docs.add(doc(f("id", S_ONE_PRE + "z")));
    docs.add(doc(f("id", S_TWO_PRE + "z")));
    docs.add(doc(f("id", S_ONE_PRE + "y")));
    docs.add(doc(f("id", S_TWO_PRE + "y")));
    for (int i = 0; i < 11; i++) {
      docs.add(doc(f("id", S_ONE_PRE + i)));
      docs.add(doc(f("id", S_TWO_PRE + i), f("foo_i", "bogus_val")));
      expectedErrs.add(addErr(S_TWO_PRE + i));
    }
    docs.add(doc(f("id", S_ONE_PRE + "x"))); 
    docs.add(doc(f("id", S_TWO_PRE + "x"))); 
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "maxErrors", "-1",
                        "commit", "true"),
                 docs.toArray(new SolrInputDocument[docs.size()])).process(client);
    assertUpdateTolerantErrors("many docs from shard2 fail, but req should succeed", rsp,
                               expectedErrs.toArray(new ExpectedErr[expectedErrs.size()]));
    assertQueryDocIds(client, true
                      , S_ONE_PRE + "z", S_ONE_PRE + "y", S_TWO_PRE + "z", S_TWO_PRE + "y" // first
                      , S_ONE_PRE + "x", S_TWO_PRE + "x" // later
                      );

  }

  //
  public void testAddsMixedWithDeletesViaCloudClient() throws Exception {
    testAddsMixedWithDeletes(CLOUD_CLIENT);
  }
  public void testAddsMixedWithDeletesViaShard1LeaderClient() throws Exception {
    testAddsMixedWithDeletes(S_ONE_LEADER_CLIENT);
  }
  public void testAddsMixedWithDeletesViaShard2LeaderClient() throws Exception {
    testAddsMixedWithDeletes(S_TWO_LEADER_CLIENT);
  }
  public void testAddsMixedWithDeletesViaShard1NonLeaderClient() throws Exception {
    testAddsMixedWithDeletes(S_ONE_NON_LEADER_CLIENT);
  }
  public void testAddsMixedWithDeletesViaShard2NonLeaderClient() throws Exception {
    testAddsMixedWithDeletes(S_TWO_NON_LEADER_CLIENT);
  }
  public void testAddsMixedWithDeletesViaNoCollectionClient() throws Exception {
    testAddsMixedWithDeletes(NO_COLLECTION_CLIENT);
  }
  
  protected static void testAddsMixedWithDeletes(SolrClient client) throws Exception {
    assertNotNull("client not initialized", client);

    // 3 doc ids, exactly one on shard1
    final String docId1  = S_ONE_PRE + "42";
    final String docId21 = S_TWO_PRE + "42";
    final String docId22 = S_TWO_PRE + "666";
    
    UpdateResponse rsp = null;
    
    // add 2 docs, one to each shard
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", docId1), f("foo_i", "2001")),
                 doc(f("id", docId21), f("foo_i", "1976"))).process(client);
    assertEquals(0, rsp.getStatus());

    // add failure on shard2, delete failure on shard1
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", docId22), f("foo_i", "not_a_num")))
      .deleteById(docId1, -1L)
      .process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("shard2 add fail, shard1 delI fail", rsp,
                               delIErr(docId1, "version conflict"),
                               addErr(docId22,"not_a_num"));
    
    // attempt a request containing 4 errors of various types (add, delI, delQ)
    for (String maxErrors : new String[] {"4", "-1", "100"}) {
      // for all of these maxErrors values, the overall request should still succeed
      rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                          "maxErrors", maxErrors,
                          "commit", "true"),
                   doc(f("id", docId22), f("foo_i", "bogus_val")))
        .deleteById(docId1, -1L)
        .deleteByQuery("malformed:[")
        .deleteById(docId21, -1L)
        .process(client);
      
      assertEquals(0, rsp.getStatus());
      assertUpdateTolerantErrors("failed variety of updates", rsp,
                                 delIErr(docId1, "version conflict"),
                                 delQErr("malformed:[", "SyntaxError"),
                                 delIErr(docId21,"version conflict"),
                                 addErr(docId22,"bogus_val"));
    }
    
    // attempt a request containing 4 errors of various types (add, delI, delQ) .. 1 too many

    SolrException e = expectThrows(SolrException.class,
        "did not get a top level exception when more then 4 updates failed",
        () -> update(params("update.chain", "tolerant-chain-max-errors-10",
            "maxErrors", "3",
            "commit", "true"),
            doc(f("id", docId22), f("foo_i", "bogus_val")))
            .deleteById(docId1, -1L)
            .deleteByQuery("malformed:[")
            .deleteById(docId21, -1L)
            .process(client)
    );

    {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed -- see SOLR-8830

      // likewise, we can't make a firm(er) assertion about the response code...
      assertTrue("not the type of error we were expecting ("+e.code()+"): " + e.toString(),
          // should be one these 2 depending on order that the async errors were hit...
          // on a single node setup -- a 5xx type error isn't something we should have triggered
          400 == e.code() || 409 == e.code());

      // verify that the Exceptions metadata can tell us what failed.
      NamedList<String> remoteErrMetadata = e.getMetadata();
      assertNotNull("no metadata in: " + e.toString(), remoteErrMetadata);
      Set<ToleratedUpdateError> actualKnownErrs
          = new LinkedHashSet<ToleratedUpdateError>(remoteErrMetadata.size());
      int actualKnownErrsCount = 0;
      for (int i = 0; i < remoteErrMetadata.size(); i++) {
        ToleratedUpdateError err =
            ToleratedUpdateError.parseMetadataIfToleratedUpdateError(remoteErrMetadata.getName(i),
                remoteErrMetadata.getVal(i));
        if (null == err) {
          // some metadata unrelated to this update processor
          continue;
        }
        actualKnownErrsCount++;
        actualKnownErrs.add(err);
      }
      assertEquals("wrong number of errors in metadata: " + remoteErrMetadata.toString(),
          4, actualKnownErrsCount);
      assertEquals("at least one dup error in metadata: " + remoteErrMetadata.toString(),
          actualKnownErrsCount, actualKnownErrs.size());
    }

    // sanity check our 2 existing docs are still here
    assertQueryDocIds(client, true, docId1, docId21);
    assertQueryDocIds(client, false, docId22);

    // tolerate some failures along with a DELQ that should succeed
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", docId22), f("foo_i", "not_a_num")))
      .deleteById(docId1, -1L)
      .deleteByQuery("zot_i:[42 to gibberish...")
      .deleteByQuery("foo_i:[50 TO 2000}")
      .process(client);
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("mix fails with one valid DELQ", rsp,
                               delIErr(docId1, "version conflict"),
                               delQErr("zot_i:[42 to gibberish..."),
                               addErr(docId22,"not_a_num"));
    // one of our previous docs should have been deleted now
    assertQueryDocIds(client, true, docId1);
    assertQueryDocIds(client, false, docId21, docId22);
                      
  }
  
  /** Asserts that the UpdateResponse contains the specified expectedErrs and no others */
  public static void assertUpdateTolerantErrors(String assertionMsgPrefix,
                                                UpdateResponse response,
                                                ExpectedErr... expectedErrs) {
    @SuppressWarnings("unchecked")
    List<SimpleOrderedMap<String>> errors = (List<SimpleOrderedMap<String>>)
      response.getResponseHeader().get("errors");
    
    assertNotNull(assertionMsgPrefix + ": Null errors: " + response.toString(), errors);
    assertEquals(assertionMsgPrefix + ": Num error ids: " + errors.toString(),
                 expectedErrs.length, errors.size());

    for (SimpleOrderedMap<String> err : errors) {
      String assertErrPre = assertionMsgPrefix + ": " + err.toString();

      String id = err.get("id");
      assertNotNull(assertErrPre + " ... null id", id);
      String type = err.get("type");
      assertNotNull(assertErrPre + " ... null type", type);
      String message = err.get("message");
      assertNotNull(assertErrPre + " ... null message", message);

      // inefficient scan, but good nough for the size of sets we're dealing with
      boolean found = false;
      for (ExpectedErr expected : expectedErrs) {
        if (expected.type.equals(type) && expected.id.equals(id)
            && (null == expected.msgSubStr || message.contains(expected.msgSubStr))) {
          found = true;
          break;
        }
      }
      assertTrue(assertErrPre + " ... unexpected err in: " + response.toString(), found);

    }
  }
  
  /** convinience method when the only type of errors you expect are 'add' errors */
  public static void assertUpdateTolerantAddErrors(String assertionMsgPrefix,
                                                   UpdateResponse response,
                                                   String... errorIdsExpected) {
    ExpectedErr[] expected = new ExpectedErr[errorIdsExpected.length];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = addErr(errorIdsExpected[i]);
    }
    assertUpdateTolerantErrors(assertionMsgPrefix, response, expected);
  }

  /** 
   * Asserts that the specified document ids do/do-not exist in the index, using both the specified client, 
   * and the CLOUD_CLIENT 
   */
  public static void assertQueryDocIds(SolrClient client, boolean shouldExist, String... ids) throws Exception {
    for (String id : ids) {
      assertEquals(client.toString() + " should " + (shouldExist ? "" : "not ") + "find id: " + id,
                   (shouldExist ? 1 : 0),
                   CLOUD_CLIENT.query(params("q", "{!term f=id}" + id)).getResults().getNumFound());
    }
    if (! CLOUD_CLIENT.equals(client) ) {
      assertQueryDocIds(CLOUD_CLIENT, shouldExist, ids);
    }
  }
  
  public static UpdateRequest update(SolrParams params, SolrInputDocument... docs) {
    UpdateRequest r = new UpdateRequest();
    r.setParams(new ModifiableSolrParams(params));
    r.add(Arrays.asList(docs));
    return r;
  }
  
  public static SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument doc = new SolrInputDocument();
    for (SolrInputField f : fields) {
      doc.put(f.getName(), f);
    }
    return doc;
  }
  
  public static SolrInputField f(String fieldName, Object... values) {
    SolrInputField f = new SolrInputField(fieldName);
    f.setValue(values);
    return f;
  }

  /** simple helper struct */
  public static final class ExpectedErr {
    final String type;
    final String id;
    final String msgSubStr; // ignored if null

    public ExpectedErr(String type, String id, String msgSubStr) {
      this.type = type;
      this.id = id;
      this.msgSubStr = msgSubStr;
    }
    public String toString() {
      return "type=<"+type+">,id=<"+id+">,msgSubStr=<"+msgSubStr+">";
    }
  }
  public static ExpectedErr addErr(String id, String msgSubStr) {
    return new ExpectedErr("ADD", id, msgSubStr);
  }
  public static ExpectedErr delIErr(String id, String msgSubStr) {
    return new ExpectedErr("DELID", id, msgSubStr);
  }
  public static ExpectedErr delQErr(String id, String msgSubStr) {
    return new ExpectedErr("DELQ", id, msgSubStr);
  }  
  public static ExpectedErr addErr(String id) {
    return addErr(id, null);
  }
  public static ExpectedErr delIErr(String id) {
    return delIErr(id, null);
  }
  public static ExpectedErr delQErr(String id) {
    return delQErr(id, null);
  }  
}
