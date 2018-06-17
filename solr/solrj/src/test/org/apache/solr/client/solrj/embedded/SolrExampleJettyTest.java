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

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.noggit.ObjectBuilder;

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
    createJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  @Ignore // nocommit
  public void testBadSetup()
  {
    try {
      // setup the server...
      String url = "http" + (isSSLMode() ? "s" : "") +  "://127.0.0.1/?core=xxx";
      Http2SolrClient client = getHttpSolrClient(url);
      Assert.fail("HttpSolrServer should not allow a path with a parameter: " + client.getBaseURL());
    }
    catch( Exception ex ) {
      // expected
    }
  }

  @Test
  public void testArbitraryJsonIndexing() throws Exception  {
    Http2SolrClient client = (Http2SolrClient) getSolrClient();
    client.deleteByQuery("*:*");
    client.commit();
    assertNumFound("*:*", 0); // make sure it got in

    // two docs, one with uniqueKey, another without it
    String json = "{\"id\":\"abc1\", \"name\": \"name1\"} {\"name\" : \"name2\"}";
 
    int statusCode = client.httpPost(getUri(client), json.getBytes("UTF-8"), "application/json").status;
    
    assertEquals(200, statusCode);
    client.commit();
    QueryResponse rsp = getSolrClient().query(new SolrQuery("*:*"));
    assertEquals(2,rsp.getResults().getNumFound());

    SolrDocument doc = rsp.getResults().get(0);
    String src = (String) doc.getFieldValue("_src_");
    Map m = (Map) ObjectBuilder.fromJSON(src);
    assertEquals("abc1",m.get("id"));
    assertEquals("name1",m.get("name"));

    doc = rsp.getResults().get(1);
    src = (String) doc.getFieldValue("_src_");
    m = (Map) ObjectBuilder.fromJSON(src);
    assertEquals("name2",m.get("name"));

  }

  private String getUri(Http2SolrClient client) {
    String baseURL = client.getBaseURL();
    return random().nextBoolean() ?
        baseURL.replace("/collection1", "/____v2/cores/collection1/update") :
        baseURL + "/update/json/docs";
  }
}
