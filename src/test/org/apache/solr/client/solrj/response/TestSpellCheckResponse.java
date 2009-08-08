package org.apache.solr.client.solrj.response;
/**
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

import junit.framework.Assert;
import org.apache.solr.client.solrj.SolrExampleTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SpellingParams;

/**
 * Test for SpellCheckComponent's response in Solrj
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestSpellCheckResponse extends SolrExampleTestBase {

  SolrServer server;
  JettySolrRunner jetty;

  int port = 0;
  static final String context = "/example";

  static String field = "name";

  public void setUp() throws Exception {
    super.setUp();

    jetty = new JettySolrRunner(context, 0);
    jetty.start();
    port = jetty.getLocalPort();
    System.out.println("Assigned Port: " + port);
    server = this.createNewSolrServer();
  }

  public void testSpellCheckResponse() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "111");
    doc.setField(field, "Samsung");
    server.add(doc);
    server.commit(true, true);

    SolrQuery query = new SolrQuery("*:*");
    query.set(CommonParams.QT, "/spell");
    query.set("spellcheck", true);
    query.set(SpellingParams.SPELLCHECK_Q, "samsang");
    query.set(SpellingParams.SPELLCHECK_BUILD, true);
    QueryRequest request = new QueryRequest(query);
    SpellCheckResponse response = request.process(server).getSpellCheckResponse();
    Assert.assertEquals("Incorrect spelling results", "samsung", response.getFirstSuggestion("samsang"));
  }

  public void testSpellCheckResponse_Extended() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "111");
    doc.setField(field, "Samsung");
    server.add(doc);
    server.commit(true, true);

    SolrQuery query = new SolrQuery("*:*");
    query.set(CommonParams.QT, "/spell");
    query.set("spellcheck", true);
    query.set(SpellingParams.SPELLCHECK_Q, "samsang");
    query.set(SpellingParams.SPELLCHECK_BUILD, true);
    query.set(SpellingParams.SPELLCHECK_EXTENDED_RESULTS, true);
    QueryRequest request = new QueryRequest(query);
    SpellCheckResponse response = request.process(server).getSpellCheckResponse();
    Assert.assertEquals("Incorrect spelling results", "samsung", response.getFirstSuggestion("samsang"));
  }

  protected SolrServer getSolrServer() {
    return server;
  }

  protected SolrServer createNewSolrServer() {
    try {
      // setup the server...
      String url = "http://localhost:" + port + context;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer(url);
      s.setConnectionTimeout(100); // 1/10th sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
