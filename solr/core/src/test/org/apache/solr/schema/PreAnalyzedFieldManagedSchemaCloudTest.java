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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.junit.BeforeClass;
import org.junit.Test;

public class PreAnalyzedFieldManagedSchemaCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION = "managed-preanalyzed";
  private static final String CONFIG = "cloud-managed-preanalyzed";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig(CONFIG, configset(CONFIG)).configure();
    CollectionAdminRequest.createCollection(COLLECTION, CONFIG, 2, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 2, 1));
  }

  @Test
  public void testAdd2Fields() throws Exception {
    addField(keyValueArrayToMap("name", "field1", "type", "string"));
    addField(keyValueArrayToMap("name", "field2", "type", "string"));
  }

  private void addField(Map<String,Object> field) throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    UpdateResponse addFieldResponse = new SchemaRequest.AddField(field).process(client, COLLECTION);
    assertNotNull(addFieldResponse);
    assertEquals(0, addFieldResponse.getStatus());
    assertNull(addFieldResponse.getResponse().get("errors"));
    FieldResponse fieldResponse = new SchemaRequest.Field(field.get("name").toString()).process(client, COLLECTION);
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

