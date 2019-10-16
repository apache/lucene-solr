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
package org.apache.solr.request;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * See SOLR-2854.
 */
@SuppressSSL     // does not yet work with ssl yet - uses raw java.net.URL API rather than HttpClient
public class TestRemoteStreaming extends SolrJettyTestBase {
  private static File solrHomeDirectory;
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    //this one has handleSelect=true which a test here needs
    solrHomeDirectory = createTempDir(LuceneTestCase.getTestClass().getSimpleName()).toFile();
    setupJettyTestHome(solrHomeDirectory, "collection1");
    createAndStartJetty(solrHomeDirectory.getAbsolutePath());
  }

  @AfterClass
  public static void afterTest() throws Exception {

  }

  @Before
  public void doBefore() throws IOException, SolrServerException {
    //add document and commit, and ensure it's there
    SolrClient client = getSolrClient();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( "id", "1234" );
    client.add(doc);
    client.commit();
    assertTrue(searchFindsIt());
  }

  @Test
  public void testMakeDeleteAllUrl() throws Exception {
    getUrlForString(makeDeleteAllUrl());
    assertFalse(searchFindsIt());
  }

  @Test
  public void testStreamUrl() throws Exception {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    String streamUrl = client.getBaseURL()+"/select?q=*:*&fl=id&wt=csv";

    String getUrl = client.getBaseURL()+"/debug/dump?wt=xml&stream.url="+URLEncoder.encode(streamUrl,"UTF-8");
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
        IOUtils.copy(new InputStreamReader(inputStream, StandardCharsets.UTF_8),strWriter);
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
    SolrException se = expectThrows(SolrException.class, () -> getSolrClient().query(query));
    assertSame(ErrorCode.BAD_REQUEST, ErrorCode.getErrorCode(se.code()));
  }
  
  /** Compose a url that if you get it, it will delete all the data. */
  private String makeDeleteAllUrl() throws UnsupportedEncodingException {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    String deleteQuery = "<delete><query>*:*</query></delete>";
    return client.getBaseURL()+"/update?commit=true&stream.body="+ URLEncoder.encode(deleteQuery, "UTF-8");
  }

  private boolean searchFindsIt() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery( "id:1234" );
    QueryResponse rsp = getSolrClient().query(query);
    return rsp.getResults().getNumFound() != 0;
  }
}
