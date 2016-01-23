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

import java.util.List;
import junit.framework.Assert;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrExampleTestBase;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.TermsResponse.Term;

/**
 * Test for TermComponent's response in Solrj
 */
public class TermsResponseTest extends SolrExampleTestBase {

  SolrServer server;

  @Override 
  public void setUp() throws Exception {
    super.setUp();
    server = createNewSolrServer();
  }

  @Override
  protected SolrServer getSolrServer() {
    return server;
  }

  @Override
  protected SolrServer createNewSolrServer() {
    return new EmbeddedSolrServer(h.getCoreContainer(), "");
  }

  public void testTermsResponse() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", 1);
    doc.setField("terms_s", "samsung");
    getSolrServer().add(doc);
    getSolrServer().commit(true, true);

    SolrQuery query = new SolrQuery();
    query.setQueryType("/terms");
    query.setTerms(true);
    query.setTermsLimit(5);
    query.setTermsLower("s");
    query.setTermsPrefix("s");
    query.addTermsField("terms_s");
    query.setTermsMinCount(1);
    
    QueryRequest request = new QueryRequest(query);
    List<Term> terms = request.process(getSolrServer()).getTermsResponse().getTerms("terms_s");

    Assert.assertNotNull(terms);
    Assert.assertEquals(terms.size(), 1);

    Term term = terms.get(0);
    Assert.assertEquals(term.getTerm(), "samsung");
    Assert.assertEquals(term.getFrequency(), 1);
  }
}
