package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TestManagedSchemaAPI extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private MiniSolrCloudCluster cluster;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("managed.schema.mutable", "true");
    cluster = new MiniSolrCloudCluster(2, createTempDir(),
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML, buildJettyConfig("/solr"));
    cluster.uploadConfigDir(TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf").toFile(), "conf1");
  }


  @Override
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
  }

  @Test
  public void test() throws Exception {
    String collection = "testschemaapi";
    cluster.createCollection(collection, 1, 2, "conf1", null);
    testReloadAndAddSimple(collection);
    testAddFieldAndDocument(collection);
  }

  private void testReloadAndAddSimple(String collection) throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String fieldName = "myNewField";
    addStringField(fieldName, collection, cloudClient);

    CollectionAdminRequest.Reload reloadRequest = new CollectionAdminRequest.Reload();
    reloadRequest.setCollectionName(collection);
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

    log.info("added new field="+fieldName);
  }

}
