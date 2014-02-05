package org.apache.solr.client.solrj.response;

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

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

/**
 * A test for parsing Solr response from query by NoOpResponseParser.
 * @see org.apache.solr.client.solrj.impl.NoOpResponseParser
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-5530">SOLR-5530</a>
 */
public class NoOpResponseParserTest extends SolrJettyTestBase {

  private static InputStream getResponse() throws IOException {
    return new SolrResourceLoader(null, null).openResource("solrj/sampleDateFacetResponse.xml");
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  @Before
  public void doBefore() throws IOException, SolrServerException {
    //add document and commit, and ensure it's there
    SolrServer server1 = getSolrServer();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1234");
    server1.add(doc);
    server1.commit();
  }

  /**
   * Parse response from query using NoOpResponseParser.
   */
  @Test
  public void testQueryParse() throws Exception {
    HttpSolrServer server = (HttpSolrServer) createNewSolrServer();
    SolrQuery query = new SolrQuery("id:1234");
    QueryRequest req = new QueryRequest(query);
    server.setParser(new NoOpResponseParser());
    NamedList<Object> resp = server.request(req);
    String responseString = (String) resp.get("response");

    assertResponse(responseString);
  }

  private void assertResponse(String responseString) throws IOException {
    ResponseParser xmlResponseParser = new XMLResponseParser();
    NamedList expectedResponse = xmlResponseParser.processResponse(IOUtils.toInputStream(responseString, "UTF-8"), "UTF-8");
    List<SolrDocument> documentList = (List<SolrDocument>) expectedResponse.getAll("response").get(0);
    assertEquals(1, documentList.size());
    SolrDocument solrDocument = documentList.get(0);
    assertEquals("1234", String.valueOf(solrDocument.getFieldValue("id")));
  }

  /**
   * Parse response from java.io.Reader.
   */
  @Test
  public void testReaderResponse() throws Exception {
    NoOpResponseParser parser = new NoOpResponseParser();
    final InputStream is = getResponse();
    try {
      assertNotNull(is);
      Reader in = new InputStreamReader(is, "UTF-8");
      NamedList<Object> response = parser.processResponse(in);
      assertNotNull(response.get("response"));
      String expectedResponse = IOUtils.toString(getResponse(), "UTF-8");
      assertEquals(expectedResponse, response.get("response"));
    } finally {
      IOUtils.closeQuietly(is);
    }

  }

  /**
   * Parse response from java.io.InputStream.
   */
  @Test
  public void testInputStreamResponse() throws Exception {
    NoOpResponseParser parser = new NoOpResponseParser();
    final InputStream is = getResponse();
    try {
      assertNotNull(is);
      NamedList<Object> response = parser.processResponse(is, "UTF-8");

      assertNotNull(response.get("response"));
      String expectedResponse = IOUtils.toString(getResponse(), "UTF-8");
      assertEquals(expectedResponse, response.get("response"));
    } finally {
      IOUtils.closeQuietly(is);
    }
  }
}
