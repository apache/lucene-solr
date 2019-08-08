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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 * 
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class SolrExampleJettyTest extends SolrExampleTests {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  public void testBadSetup() {
    // setup the server...
    String url = "http" + (isSSLMode() ? "s" : "") +  "://127.0.0.1/?core=xxx";
    expectThrows(Exception.class, () -> getHttpSolrClient(url));
  }

  @Test
  public void testArbitraryJsonIndexing() throws Exception  {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    client.deleteByQuery("*:*");
    client.commit();
    assertNumFound("*:*", 0); // make sure it got in

    // two docs, one with uniqueKey, another without it
    String json = "{\"id\":\"abc1\", \"name\": \"name1\"} {\"name\" : \"name2\"}";
    HttpClient httpClient = client.getHttpClient();
    HttpPost post = new HttpPost(getUri(client));
    post.setHeader("Content-Type", "application/json");
    post.setEntity(new InputStreamEntity(
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), -1));
    HttpResponse response = httpClient.execute(post, HttpClientUtil.createNewHttpClientRequestContext());
    assertEquals(200, response.getStatusLine().getStatusCode());
    client.commit();
    QueryResponse rsp = getSolrClient().query(new SolrQuery("*:*"));
    assertEquals(2,rsp.getResults().getNumFound());

    SolrDocument doc = rsp.getResults().get(0);
    String src = (String) doc.getFieldValue("_src_");
    Map m = (Map) fromJSONString(src);
    assertEquals("abc1",m.get("id"));
    assertEquals("name1",m.get("name"));

    doc = rsp.getResults().get(1);
    src = (String) doc.getFieldValue("_src_");
    m = (Map) fromJSONString(src);
    assertEquals("name2",m.get("name"));

  }

  private String getUri(HttpSolrClient client) {
    String baseURL = client.getBaseURL();
    return random().nextBoolean() ?
        baseURL.replace("/collection1", "/____v2/cores/collection1/update") :
        baseURL + "/update/json/docs";
  }

  @Ignore
  public void testUtf8QueryPerf() throws Exception {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    client.deleteByQuery("*:*");
    client.commit();
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", "" + i);
      doc2.addField("fld1_s", "1 value 1 value 1 value 1 value 1 value 1 value 1 value ");
      doc2.addField("fld2_s", "2 value 2 value 2 value 2 value 2 value 2 value 2 value 2 value 2 value 2 value ");
      doc2.addField("fld3_s", "3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value ");
      doc2.addField("fld4_s", "4 value 4 value 4 value 4 value 4 value 4 value 4 value 4 value 4 value ");
      doc2.addField("fld5_s", "5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value ");
      docs.add(doc2);
    }
    client.add(docs);
    client.commit();
    QueryResponse rsp = client.query(new SolrQuery("*:*"));
    assertEquals(10, rsp.getResults().getNumFound());


    client.setParser(new BinaryResponseParser() {
      @Override
      public NamedList<Object> processResponse(InputStream body, String encoding) {
        try {
          IOUtils.skip(body, 1024 * 1000);
        } catch (IOException e) {
          e.printStackTrace();
        }
        return rsp.getResponse();
      }
    });


    runQueries(client, 1000, true);
    /*BinaryResponseWriter.useUtf8CharSeq = false;
    System.out.println("BinaryResponseWriter.useUtf8CharSeq = " + BinaryResponseWriter.useUtf8CharSeq);
    runQueries(client, 10000, false);
    BinaryResponseWriter.useUtf8CharSeq = true;
    System.out.println("BinaryResponseWriter.useUtf8CharSeq = " + BinaryResponseWriter.useUtf8CharSeq);*/
    runQueries(client, 10000, false);
  }


  private void runQueries(HttpSolrClient client, int count, boolean warmup) throws SolrServerException, IOException {
    long start = System.nanoTime();
    for (int i = 0; i < count; i++) {
      client.query(new SolrQuery("*:*"));
    }
    if (warmup) return;
    System.out.println("time taken : " + ((System.nanoTime() - start)) / (1000 * 1000));
  }

}
