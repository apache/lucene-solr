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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldResponse;
import org.apache.solr.common.SolrException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEmbeddedSolrServerSchemaAPI extends SolrTestCaseJ4 {

  private String fieldName = "VerificationTest";
  private static EmbeddedSolrServer server;
  private final Map<String, Object> fieldAttributes;
  {
    Map<String,Object> field = new LinkedHashMap<>();
    field.put("name", fieldName);
    field.put("type", "string");
    field.put("stored", false);
    field.put("indexed", true);
    field.put("multiValued", true);
    fieldAttributes = Collections.unmodifiableMap(field);
  }

  @BeforeClass
  public static void initClass() throws Exception {
    assertNull("no system props clash please", System.getProperty("managed.schema.mutable"));
    System.setProperty("managed.schema.mutable", ""+//true
    random().nextBoolean()
    );
    Path tmpHome = createTempDir("tmp-home");
    Path coreDir = tmpHome.resolve(DEFAULT_TEST_CORENAME);
    copyMinConf(coreDir.toFile(), null, "solrconfig-managed-schema.xml");
    initCore("solrconfig.xml" /*it's renamed to to*/, "schema.xml", tmpHome.toAbsolutePath().toString());
    
    server = new EmbeddedSolrServer(h.getCoreContainer(), DEFAULT_TEST_CORENAME);
  }

  @AfterClass
  public static void destroyClass() throws IOException {
    server.close(); // doubtful
    server = null;
    System.clearProperty("managed.schema.mutable");
  }

  @Before
  public void thereIsNoFieldYet() throws SolrServerException, IOException{
    try{
      FieldResponse process = new SchemaRequest.Field(fieldName)
                  .process(server);
      fail(""+process);
    }catch(SolrException e){
      assertTrue(e.getMessage().contains("No")
           && e.getMessage().contains("VerificationTest"));
    }
  }
  
  @Test
  public void testSchemaAddFieldAndVerifyExistence() throws Exception {
    assumeTrue("it needs to ammend schema", Boolean.getBoolean("managed.schema.mutable"));
    SchemaResponse.UpdateResponse addFieldResponse = new SchemaRequest.AddField(fieldAttributes).process(server);

    assertEquals(addFieldResponse.toString(), 0, addFieldResponse.getStatus());

    // This asserts that the field was actually created
    // this is due to the fact that the response gave OK but actually never created the field.
    Map<String,Object> foundFieldAttributes = new SchemaRequest.Field(fieldName).process(server).getField();
    assertEquals(fieldAttributes, foundFieldAttributes);

    assertEquals("removing " + fieldName, 0,
        new SchemaRequest.DeleteField(fieldName).process(server).getStatus());
  }

  @Test 
  public void testSchemaAddFieldAndFailOnImmutable() throws Exception {
    assumeFalse("it needs a readonly schema", Boolean.getBoolean("managed.schema.mutable"));

      SchemaRequest.AddField addFieldUpdateSchemaRequest = new SchemaRequest.AddField(fieldAttributes);
      SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(server);
      // wt hell???? assertFalse(addFieldResponse.toString(), addFieldResponse.getStatus()==0);
      assertTrue((""+addFieldResponse).contains("schema is not editable"));

  }

}
