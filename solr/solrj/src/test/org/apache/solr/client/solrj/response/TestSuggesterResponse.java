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
package org.apache.solr.client.solrj.response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for SuggesterComponent's response in Solrj
 *
 */
public class TestSuggesterResponse extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Before
  public void setUpClient() {
    getSolrClient();
  }

  static String field = "cat";

  @Test
  public void testSuggesterResponseObject() throws Exception {
    addSampleDocs();

    try (SolrClient solrClient = createSuggestSolrClient()) {
      SolrQuery query = new SolrQuery("*:*");
      query.set(CommonParams.QT, "/suggest");
      query.set("suggest.dictionary", "mySuggester");
      query.set("suggest.q", "Com");
      query.set("suggest.build", true);
      QueryRequest request = new QueryRequest(query);
      QueryResponse queryResponse = request.process(solrClient);
      SuggesterResponse response = queryResponse.getSuggesterResponse();
      Map<String, List<Suggestion>> dictionary2suggestions = response.getSuggestions();
      assertTrue(dictionary2suggestions.keySet().contains("mySuggester"));

      List<Suggestion> mySuggester = dictionary2suggestions.get("mySuggester");
      assertEquals("Computational framework", mySuggester.get(0).getTerm());
      assertEquals(0, mySuggester.get(0).getWeight());
      assertEquals("", mySuggester.get(0).getPayload());
      assertEquals("Computer", mySuggester.get(1).getTerm());
      assertEquals(0, mySuggester.get(1).getWeight());
      assertEquals("", mySuggester.get(1).getPayload());
    }

  }

  @Test
  public void testSuggesterResponseTerms() throws Exception {
    addSampleDocs();

    try (SolrClient solrClient = createSuggestSolrClient()) {
      SolrQuery query = new SolrQuery("*:*");
      query.set(CommonParams.QT, "/suggest");
      query.set("suggest.dictionary", "mySuggester");
      query.set("suggest.q", "Com");
      query.set("suggest.build", true);
      QueryRequest request = new QueryRequest(query);
      QueryResponse queryResponse = request.process(solrClient);
      SuggesterResponse response = queryResponse.getSuggesterResponse();
      Map<String, List<String>> dictionary2suggestions = response.getSuggestedTerms();
      assertTrue(dictionary2suggestions.keySet().contains("mySuggester"));

      List<String> mySuggester = dictionary2suggestions.get("mySuggester");
      assertEquals("Computational framework", mySuggester.get(0));
      assertEquals("Computer", mySuggester.get(1));
    }
  }

  @Test
  public void testEmptySuggesterResponse() throws Exception {
    addSampleDocs();

    try (SolrClient solrClient = createSuggestSolrClient()) {
      SolrQuery query = new SolrQuery("*:*");
      query.set(CommonParams.QT, "/suggest");
      query.set("suggest.dictionary", "mySuggester");
      query.set("suggest.q", "Empty");
      query.set("suggest.build", true);
      QueryRequest request = new QueryRequest(query);
      QueryResponse queryResponse = request.process(solrClient);
      SuggesterResponse response = queryResponse.getSuggesterResponse();
      Map<String, List<String>> dictionary2suggestions = response.getSuggestedTerms();
      assertTrue(dictionary2suggestions.keySet().contains("mySuggester"));

      List<String> mySuggester = dictionary2suggestions.get("mySuggester");
      assertEquals(0, mySuggester.size());
    }
  }

  private void addSampleDocs() throws SolrServerException, IOException {
    client.deleteByQuery("*:*");
    client.commit(true, true);
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "111");
    doc.setField(field, "Computer");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.setField("id", "222");
    doc2.setField(field, "Computational framework");
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.setField("id", "333");
    doc3.setField(field, "Laptop");
    client.add(doc);
    client.add(doc2);
    client.add(doc3);
    client.commit(true, true);
  }

  /*
   * Randomizes the ResponseParser to test that both javabin and xml responses parse correctly.  See SOLR-15070
   */
  private SolrClient createSuggestSolrClient() {
    final ResponseParser randomParser = random().nextBoolean() ? new BinaryResponseParser() : new XMLResponseParser();
    return new HttpSolrClient.Builder()
            .withBaseSolrUrl(jetty.getBaseUrl().toString() + "/collection1")
            .withResponseParser(randomParser)
            .build();
  }
}
