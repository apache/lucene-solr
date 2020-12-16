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

package org.apache.solr.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ManagedSchemaRoundRobinCloudTest extends SolrCloudTestCase {
  private static final String COLLECTION = "managed_coll";
  private static final String CONFIG = "cloud-managed";
  private static final String FIELD_PREFIX = "NumberedField_";
  private static final int NUM_SHARDS = 2;
  private static final int NUM_FIELDS_TO_ADD = 10;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(NUM_SHARDS).addConfig(CONFIG, configset(CONFIG)).configure();
    CollectionAdminRequest.createCollection(COLLECTION, CONFIG, NUM_SHARDS, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, NUM_SHARDS, 1));
  }

  @AfterClass
  public static void clearSysProps() throws Exception {
    System.clearProperty("managed.schema.mutable");
  }

  @Test
  public void testAddFieldsRoundRobin() throws Exception {
    List<HttpSolrClient> clients = new ArrayList<>(NUM_SHARDS);
    try {
      for (int shardNum = 0 ; shardNum < NUM_SHARDS ; ++shardNum) {
        clients.add(getHttpSolrClient(cluster.getJettySolrRunners().get(shardNum).getBaseUrl().toString()));
      }
      int shardNum = 0;
      for (int fieldNum = 0 ; fieldNum < NUM_FIELDS_TO_ADD ; ++fieldNum) {
        addField(clients.get(shardNum), keyValueArrayToMap("name", FIELD_PREFIX + fieldNum, "type", "string"));
        if (++shardNum == NUM_SHARDS) { 
          shardNum = 0;
        }
      }
    } finally {
      for (int shardNum = 0 ; shardNum < NUM_SHARDS ; ++shardNum) {
        clients.get(shardNum).close();
      }
    }
  }

  private void addField(SolrClient client, Map<String,Object> field) throws Exception {
    SchemaResponse.UpdateResponse addFieldResponse = new SchemaRequest.AddField(field).process(client, COLLECTION);
    assertNotNull(addFieldResponse);
    assertEquals(0, addFieldResponse.getStatus());
    assertNull(addFieldResponse.getResponse().get("errors"));
    String fieldName = field.get("name").toString();
    SchemaResponse.FieldResponse fieldResponse = new SchemaRequest.Field(fieldName).process(client, COLLECTION);
    assertNotNull(fieldResponse);
    assertEquals(0, fieldResponse.getStatus());
  }

  private Map<String,Object> keyValueArrayToMap(String... alternatingKeysAndValues) {
    Map<String,Object> map = new HashMap<>();
    for (int i = 0 ; i < alternatingKeysAndValues.length ; i += 2)
      map.put(alternatingKeysAndValues[i], alternatingKeysAndValues[i + 1]);
    return map;
  }
}
