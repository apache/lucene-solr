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
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * See SOLR-2854.
 */
public class TestRemoteStreaming extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  @Before
  public void doBefore() throws IOException, SolrServerException {
    //add document and commit, and ensure it's there
    SolrServer server1 = getSolrServer();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "xxxx" );
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
    CommonsHttpSolrServer solrServer = (CommonsHttpSolrServer) getSolrServer();
    String streamUrl = solrServer.getBaseURL()+"/select?q=*:*&fl=id&wt=csv";

    String getUrl = solrServer.getBaseURL()+"/debug/dump?wt=xml&stream.url="+URLEncoder.encode(streamUrl,"UTF-8");
    String content = getUrlForString(getUrl);
    assertTrue(content.contains("xxxx"));
    //System.out.println(content);
  }

  private String getUrlForString(String getUrl) throws IOException {
    Object obj = new URL(getUrl).getContent();
    if (obj instanceof InputStream) {
      InputStream inputStream = (InputStream) obj;
      try {
        StringWriter strWriter = new StringWriter();
        IOUtils.copy(inputStream,strWriter);
        return strWriter.toString();
      } finally {
        IOUtils.closeQuietly(inputStream);
      }
    }
    return null;
  }

  /** Do a select query with the stream.url. Solr should NOT access that URL, and so the data should be there. */
  @Test
  public void testNoUrlAccess() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );//for anything
    query.add("stream.url",makeDeleteAllUrl());
    getSolrServer().query(query);
    assertTrue(searchFindsIt());//still there
  }

  /** Compose a url that if you get it, it will delete all the data. */
  private String makeDeleteAllUrl() throws UnsupportedEncodingException {
    CommonsHttpSolrServer solrServer = (CommonsHttpSolrServer) getSolrServer();
    String deleteQuery = "<delete><query>*:*</query></delete>";
    return solrServer.getBaseURL()+"/update?commit=true&stream.body="+ URLEncoder.encode(deleteQuery, "UTF-8");
  }

  private boolean searchFindsIt() throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setQuery( "id:xxxx" );
    QueryResponse rsp = getSolrServer().query(query);
    return rsp.getResults().getNumFound() != 0;
  }
}
