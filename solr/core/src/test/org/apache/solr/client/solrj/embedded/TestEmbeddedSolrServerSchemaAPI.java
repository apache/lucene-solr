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
package org.apache.solr.client.solrj.embedded;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestEmbeddedSolrServerSchemaAPI extends SolrTestCaseJ4 {


  @BeforeClass
  public static void initClass() {
    System.setProperty("managed.schema.mutable", "true");
  }

  @AfterClass
  public static void destroyClass() {
    System.clearProperty("managed.schema.mutable");
  }

  @Test
  public void testSchemaAddFieldAndVerifyExistence() throws Exception {
    Path path = createTempDir();

    SolrResourceLoader loader = new SolrResourceLoader(path);
    NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", loader)
        .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
        .build();

    String configSet = "cloud-managed";

    // Backup the schema config as its not reset on reruns of the tests
    File schemaFile = new File(config.getConfigSetBaseDirectory().toFile(), configSet+"/conf/managed-schema");
    File schemaFileBackup = new File(config.getConfigSetBaseDirectory().toFile(), configSet+"/conf/managed-schema.backup");
    assertTrue("The schema intended for backup was not found", schemaFile.exists());
    FileUtils.copyFile(schemaFile, schemaFileBackup);

    assertTrue("The schema backup was not found", schemaFileBackup.exists());

    try (EmbeddedSolrServer server = new EmbeddedSolrServer(config, "collection1")) {

      CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
      createRequest.setCoreName("collection1");
      createRequest.setConfigSet(configSet);
      CoreAdminResponse coreAdminResponse = createRequest.process(server);

      SolrTestCaseJ4.assertResponse(coreAdminResponse);

      Map<String, Object> fieldAttributes = new LinkedHashMap<>();
      String fieldName = "VerificationTest";
      fieldAttributes.put("name", fieldName);
      fieldAttributes.put("type", "string");
      fieldAttributes.put("stored", false);
      fieldAttributes.put("indexed", true);
      fieldAttributes.put("multiValued", true);
      SchemaRequest.AddField addFieldUpdateSchemaRequest = new SchemaRequest.AddField(fieldAttributes);
      SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(server);

      SolrTestCaseJ4.assertResponse(addFieldResponse);


      // This asserts that the field was actually created
      // this is due to the fact that the response gave OK but actually never created the field.
      SchemaRequest.Field fieldRequest = new SchemaRequest.Field(fieldName);
      Map<String, Object> foundFieldAttributes = fieldRequest.process(server).getField();

      assertEquals(fieldAttributes.get("name"), foundFieldAttributes.get("name"));
      assertEquals(fieldAttributes.get("type"), foundFieldAttributes.get("type"));
      assertEquals(fieldAttributes.get("stored"), foundFieldAttributes.get("stored"));
      assertEquals(fieldAttributes.get("indexed"), foundFieldAttributes.get("indexed"));
      assertEquals(fieldAttributes.get("multiValued"), foundFieldAttributes.get("multiValued"));

    }finally {
      // Restore the schema-file
      FileUtils.copyFile(schemaFileBackup, schemaFile);
      schemaFileBackup.deleteOnExit();

    }
  }

  @Test
  public void testSchemaAddFieldAndFailOnImmutable() throws Exception {
    Path path = createTempDir();

    SolrResourceLoader loader = new SolrResourceLoader(path);
    NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", loader)
        .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
        .build();

    String configSet = "minimal";

    try (EmbeddedSolrServer server = new EmbeddedSolrServer(config, "collection1")) {

      CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
      createRequest.setCoreName("collection1");
      createRequest.setConfigSet(configSet);
      CoreAdminResponse coreAdminResponse = createRequest.process(server);

      SolrTestCaseJ4.assertResponse(coreAdminResponse);

      Map<String, Object> fieldAttributes = new LinkedHashMap<>();
      String fieldName = "VerificationTest";
      fieldAttributes.put("name", fieldName);
      fieldAttributes.put("type", "string");
      fieldAttributes.put("stored", false);
      fieldAttributes.put("indexed", true);
      fieldAttributes.put("multiValued", true);
      SchemaRequest.AddField addFieldUpdateSchemaRequest = new SchemaRequest.AddField(fieldAttributes);
      SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(server);

      assertEquals("schema is not editable", SolrTestCaseJ4.getResponseMessage(addFieldResponse));

    }
  }

}
