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
package org.apache.solr.client.solrj;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrSchemalessExampleTest extends SolrExampleTestsBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    File tempSolrHome = createTempDir().toFile();
    // Schemaless renames schema.xml -> schema.xml.bak, and creates + modifies conf/managed-schema,
    // which violates the test security manager's rules, which disallow writes outside the build dir,
    // so we copy the example/example-schemaless/solr/ directory to a new temp dir where writes are allowed.
    FileUtils.copyFileToDirectory(new File(ExternalPaths.SERVER_HOME, "solr.xml"), tempSolrHome);
    File collection1Dir = new File(tempSolrHome, "collection1");
    FileUtils.forceMkdir(collection1Dir);
    FileUtils.copyDirectoryToDirectory(new File(ExternalPaths.DEFAULT_CONFIGSET), collection1Dir);
    Properties props = new Properties();
    props.setProperty("name","collection1");
    OutputStreamWriter writer = null;
    try {
      writer = new OutputStreamWriter(FileUtils.openOutputStream(
          new File(collection1Dir, "core.properties")), StandardCharsets.UTF_8);
      props.store(writer, null);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception ignore){}
      }
    }
    createAndStartJetty(tempSolrHome.getAbsolutePath());
  }
  @Test
  public void testArbitraryJsonIndexing() throws Exception  {
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.deleteByQuery("*:*");
    client.commit();
    assertNumFound("*:*", 0); // make sure it got in

    // two docs, one with uniqueKey, another without it
    String json = "{\"id\":\"abc1\", \"name\": \"name1\"} {\"name\" : \"name2\"}";

    Http2SolrClient.SimpleResponse resp = Http2SolrClient.POST(client.getBaseURL() + "/update/json/docs", client, json.getBytes(StandardCharsets.UTF_8), "application/json");

    assertEquals(200, resp.status);
    client.commit();
    assertNumFound("*:*", 2);
  }

  @Test
  public void testFieldMutating() throws Exception {
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.deleteByQuery("*:*");
    client.commit();
    assertNumFound("*:*", 0); // make sure it got in
    // two docs, one with uniqueKey, another without it
    String json = "{\"name one\": \"name\"} " +
        "{\"name  two\" : \"name\"}" +
        "{\"first-second\" : \"name\"}" +
        "{\"x+y\" : \"name\"}" +
        "{\"p%q\" : \"name\"}" +
        "{\"p.q\" : \"name\"}" +
        "{\"a&b\" : \"name\"}"
        ;

    Http2SolrClient.SimpleResponse resp = Http2SolrClient.POST(client.getBaseURL() + "/update/json/docs", client, json.getBytes(StandardCharsets.UTF_8), "application/json");
    assertEquals(200, resp.status);
    client.commit();
    List<String> expected = Arrays.asList(
        "name_one",
        "name__two",
        "first-second",
        "a_b",
        "p_q",
        "p.q",
        "x_y");
    HashSet set = new HashSet();
    QueryResponse rsp = assertNumFound("*:*", expected.size());
    for (SolrDocument doc : rsp.getResults()) set.addAll(doc.getFieldNames());
    for (String s : expected) {
      assertTrue(s+" not created "+ rsp ,set.contains(s) );
    }

  }



  @Override
  public SolrClient createNewSolrClient(JettySolrRunner jetty) {
    try {
      // setup the server...
      String url = jetty.getBaseUrl().toString() + "/collection1";
      Http2SolrClient client = getHttpSolrClient(url, DEFAULT_CONNECTION_TIMEOUT);
     // client.setUseMultiPartPost(random().nextBoolean());
      
      if (random().nextBoolean()) {
        client.setParser(new BinaryResponseParser());
        client.setRequestWriter(new BinaryRequestWriter());
      }
      
      return client;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
