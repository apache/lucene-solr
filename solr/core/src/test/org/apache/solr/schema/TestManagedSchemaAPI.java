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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestManagedSchemaAPI extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();
  }

  @Test
  public void test() throws Exception {
    String collection = "testschemaapi";
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 2)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)

        .process(cluster.getSolrClient());
    testModifyField(collection);
    testReloadAndAddSimple(collection);
    testAddFieldAndDocument(collection);
  }

  private void testReloadAndAddSimple(String collection) throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String fieldName = "myNewField";
    addStringField(fieldName, collection, cloudClient);

    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField(fieldName, "val");
    UpdateRequest ureq = new UpdateRequest().add(doc);
    cloudClient.request(ureq, collection);
  }

  private void testAddFieldAndDocument(String collection) throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String fieldName = "myNewField1";
    addStringField(fieldName, collection, cloudClient);

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "2");
    doc.addField(fieldName, "val1");
    UpdateRequest ureq = new UpdateRequest().add(doc);
    cloudClient.request(ureq, collection);;
  }

  private void addStringField(String fieldName, String collection, CloudSolrClient cloudClient) throws IOException, SolrServerException {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    SchemaRequest.AddField addFieldUpdateSchemaRequest = new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(cloudClient, collection);
    assertEquals(0, addFieldResponse.getStatus());
    assertNull(addFieldResponse.getResponse().get("errors"));

    log.info("added new field={}", fieldName);
  }

  private void testModifyField(String collection) throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    SolrInputDocument doc = new SolrInputDocument("id", "3");
    cloudClient.add(collection, doc);
    cloudClient.commit(collection);

    String fieldName = "id";
    SchemaRequest.Field getFieldRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse getFieldResponse = getFieldRequest.process(cloudClient, collection);
    Map<String, Object> field = getFieldResponse.getField();
    field.put("docValues", true);
    SchemaRequest.ReplaceField replaceRequest = new SchemaRequest.ReplaceField(field);
    SchemaResponse.UpdateResponse replaceResponse = replaceRequest.process(cloudClient, collection);
    assertNull(replaceResponse.getResponse().get("errors"));
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

  }

}
