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
/**
 * 
 */
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SearchParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests a special case of join queries in which "fromCollection" is equally sharded as "toCollection"
 */
public class ShardedFromCollectionJoinTest extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static String shardedToColl = "to_2x1";
  private static String shardedFromColl = "from_2x1";
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    String configName = "solrCloudCollectionConfig";
    int nodeCount = 2;
    configureCluster(nodeCount)
       .addConfig(configName, configDir)
       .configure();
    
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-tlog.xml" );
    collectionProperties.put("schema", "schema.xml"); 
 
    int shards=2;
    int replicas=1;
    
    assertNotNull(cluster.createCollection(shardedToColl, shards, replicas,
        configName,
        collectionProperties));
    
    Set<String> shardedToCollectionNode = new HashSet<>();
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    ClusterState cs = zkStateReader.getClusterState();
    for (Slice slice : cs.getCollection(shardedToColl).getActiveSlices()){
      for (Replica replica : slice.getReplicas())
        shardedToCollectionNode.add(replica.getNodeName());
    }
    
    assertNotNull(cluster.createCollection(shardedFromColl, shards, replicas,
        configName, StringUtils.join(shardedToCollectionNode,","), null,
        collectionProperties));

    AbstractDistribZkTestBase.waitForRecoveriesToFinish(shardedToColl, zkStateReader, false, true, 30);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(shardedFromColl, zkStateReader, false, true, 30);
  }
  
  /**
   * Test distributed join. This method tests join query on multiple shards and gets result from all shards.
   * It specifically tests scenario of secondary collection being equally sharded same as primary collection
   * 
   * It is a join query between two collections both of which have 2 shards and 2 replica
   *        to_2x2
   *        from_2x2
   *
   * Query:  {!join from =join_s fromIndex =from_2x2 to =join_s}
   *         {!join from =id to =id fromIndex = product}(*:*)
   * FQ:     *:*
   * Params: distrib=true
   * @throws Exception the exception
   */
  @Test
  public void testDistributedJoin() throws Exception{
    log.info("Testing distributed join across cluster!");
    final String fromQ = "*:*";
    CloudSolrClient client = cluster.getSolrClient();
    
    indexDoc(shardedToColl, 1001, "a", null, "b");
    //Using Integer.MAX ensures that document is on shard2
    indexDoc(shardedToColl, Integer.MAX_VALUE,  "a", null, "b");
    
    //Using Integer.MAX ensures that document is on shard2
    indexDoc(shardedFromColl, Integer.MAX_VALUE,"a", null, "b");
    indexDoc(shardedFromColl, 2001, "a", null, "b");
    
    final String joinQ = "{!join from=join_s fromIndex=" + shardedFromColl + " to=join_s}" + fromQ;
    // so the commits fire
    Thread.sleep(2000); 
    
    //Join query with distrib=true so the query goes on all shards
    QueryRequest qr = new QueryRequest(params("collection", shardedToColl, "q", joinQ, "distrib", "true", SearchParams.RANGE_CHECK, "true"));
    
    //QueryResponse rsp = new QueryResponse(cloudClient.request(qr), cloudClient);
    final QueryResponse  rsp = new QueryResponse(client.request(qr), client);
    
    SolrDocumentList hits = rsp.getResults();
    
    assertTrue("Expected 2 doc from 2 different shards, got "+hits, hits.getNumFound() == 2);
  }

  
  @AfterClass
  public static void shutdown() {
    log.info("DistribJoinFromCollectionTest logic complete ... deleting the " + shardedToColl + " and " + shardedFromColl + " collections");

    // try to clean up
    for (String c : new String[]{ shardedToColl, shardedFromColl }) {
      try {
        CollectionAdminRequest.Delete req =  CollectionAdminRequest.deleteCollection(c);
        req.process(cluster.getSolrClient());
      } catch (Exception e) {
        // don't fail the test
        log.warn("Could not delete collection {} after test completed due to: " + e, c);
      }
    }

    log.info("DistribJoinFromCollectionTest succeeded ... shutting down now!");
  }

  protected static Integer indexDoc(String collection, int id, String joinField, String matchField, String getField) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setCommitWithin(50);
    up.setParam("collection", collection);
    SolrInputDocument doc = new SolrInputDocument();
    Integer docId = new Integer(id);
    doc.addField("id", docId);
    doc.addField("join_s", joinField);
    if (matchField != null)
      doc.addField("match_s", matchField);
    if (getField != null)
      doc.addField("get_s", getField);
    up.add(doc);
    cluster.getSolrClient().request(up);
    return docId;
  }
}
