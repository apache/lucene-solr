package org.apache.solr.request;

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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException.ErrorCode;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * See SOLR-2854.
 */
public class TestRemoteStreaming extends SolrJettyTestBase {

  private static final File solrHomeDirectory = new File(TEMP_DIR, "TestRemoteStreaming");

  static {
    // does not yet work with ssl
    sslConfig = null;
  }
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    //this one has handleSelect=true which a test here needs
    setupJettyTestHome(solrHomeDirectory, "collection1");
    createJetty(solrHomeDirectory.getAbsolutePath(), null, null);
  }

  @AfterClass
  public static void afterTest() throws Exception {
    cleanUpJettyHome(solrHomeDirectory);
  }

  @Before
  public void doBefore() throws IOException, SolrServerException {
    //add document and commit, and ensure it's there
    SolrServer server1 = getSolrServer();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "1234" );
    server1.add(doc);
    server1.commit();
    assertTrue(searchFindsIt());
  }

  @Test
  public void testMakeDeleteAllUrl() throws Exception {
    getUrlForString(makeDeleteAllUrl());
    assertFalse(searchFindsIt());
  }

  @Test
  public void testStreamUrl() throws Exception {
    HttpSolrServer solrServer = (HttpSolrServer) getSolrServer();
    String streamUrl = solrServer.getBaseURL()+"/select?q=*:*&fl=id&wt=csv";

    String getUrl = solrServer.getBaseURL()+"/debug/dump?wt=xml&stream.url="+URLEncoder.encode(streamUrl,"UTF-8");
    String content = getUrlForString(getUrl);
    assertTrue(content.contains("1234"));
    //System.out.println(content);
  }

  private String getUrlForString(String getUrl) throws IOException {
    Object obj = new URL(getUrl).getContent();
    if (obj instanceof InputStream) {
      InputStream inputStream = (InputStream) obj;
      try {
        StringWriter strWriter = new StringWriter();
        IOUtils.copy(new InputStreamReader(inputStream, "UTF-8"),strWriter);
        return strWriter.toString();
      } finally {
        IOUtils.closeQuietly(inputStream);
      }
    }
    return null;
  }

  /** Do a select query with the stream.url. Solr should fail */
  @Test
  public void testNoUrlAccess() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );//for anything
    query.add("stream.url",makeDeleteAllUrl());
    try {
      getSolrServer().query(query);
      fail();
    } catch (SolrException se) {
      assertSame(ErrorCode.BAD_REQUEST, ErrorCode.getErrorCode(se.code()));
    }
  }

  /** SOLR-3161
   * Technically stream.body isn't remote streaming, but there wasn't a better place for this test method. */
  @Test(expected = SolrException.class)
  public void testQtUpdateFails() throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );//for anything
    query.add("echoHandler","true");
    //sneaky sneaky
    query.add("qt","/update");
    query.add("stream.body","<delete><query>*:*</query></delete>");

    QueryRequest queryRequest = new QueryRequest(query) {
      @Override
      public String getPath() { //don't let superclass substitute qt for the path
        return "/select";
      }
    };
    QueryResponse rsp = queryRequest.process(getSolrServer());
    //!! should *fail* above for security purposes
    String handler = (String) rsp.getHeader().get("handler");
    System.out.println(handler);
  }

  /** Compose a url that if you get it, it will delete all the data. */
  private String makeDeleteAllUrl() throws UnsupportedEncodingException {
    HttpSolrServer solrServer = (HttpSolrServer) getSolrServer();
    String deleteQuery = "<delete><query>*:*</query></delete>";
    return solrServer.getBaseURL()+"/update?commit=true&stream.body="+ URLEncoder.encode(deleteQuery, "UTF-8");
  }

  private boolean searchFindsIt() throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setQuery( "id:1234" );
    QueryResponse rsp = getSolrServer().query(query);
    return rsp.getResults().getNumFound() != 0;
  }
}
