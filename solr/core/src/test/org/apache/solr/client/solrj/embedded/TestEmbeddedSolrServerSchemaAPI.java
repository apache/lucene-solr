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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

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
  public void testNodeConfigConstructor() throws Exception {
    Path path = createTempDir();

    SolrResourceLoader loader = new SolrResourceLoader(path);
    NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", loader)
        .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
        .build();

    try (EmbeddedSolrServer server = new EmbeddedSolrServer(config, "collection1")) {


      CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
      createRequest.setCoreName("collection1");
      createRequest.setConfigSet("cloud-managed");
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
      SchemaRequest.Field fieldRequest = new SchemaRequest.Field(fieldName);
      Map<String, Object> foundFieldAttributes = fieldRequest.process(server).getField();

      assertEquals(fieldAttributes.get("name"), foundFieldAttributes.get("name"));

    }
  }

}
