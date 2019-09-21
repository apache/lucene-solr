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
package org.apache.solr;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTolerantSearch extends SolrJettyTestBase {
  
  private static SolrClient collection1;
  private static SolrClient collection2;
  private static String shard1;
  private static String shard2;
  private static File solrHome;
  
  private static File createSolrHome() throws Exception {
    File workDir = createTempDir().toFile();
    setupJettyTestHome(workDir, "collection1");
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME() + "/collection1/conf/solrconfig-tolerant-search.xml"), new File(workDir, "/collection1/conf/solrconfig.xml"));
    FileUtils.copyDirectory(new File(workDir, "collection1"), new File(workDir, "collection2"));
    return workDir;
  }
  
  
  @BeforeClass
  public static void createThings() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    solrHome = createSolrHome();
    createAndStartJetty(solrHome.getAbsolutePath());
    String url = jetty.getBaseUrl().toString();
    collection1 = getHttpSolrClient(url + "/collection1");
    collection2 = getHttpSolrClient(url + "/collection2");
    
    String urlCollection1 = jetty.getBaseUrl().toString() + "/" + "collection1";
    String urlCollection2 = jetty.getBaseUrl().toString() + "/" + "collection2";
    shard1 = urlCollection1.replaceAll("https?://", "");
    shard2 = urlCollection2.replaceAll("https?://", "");
    
    //create second core
    try (HttpSolrClient nodeClient = getHttpSolrClient(url)) {
      CoreAdminRequest.Create req = new CoreAdminRequest.Create();
      req.setCoreName("collection2");
      req.setConfigSet("collection1");
      nodeClient.request(req);
    }

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("subject", "batman");
    doc.setField("title", "foo bar");
    collection1.add(doc);
    collection1.commit();
    
    doc.setField("id", "2");
    doc.setField("subject", "superman");
    collection2.add(doc);
    collection2.commit();
    
    doc = new SolrInputDocument();
    doc.setField("id", "3");
    doc.setField("subject", "aquaman");
    doc.setField("title", "foo bar");
    collection1.add(doc);
    collection1.commit();
    
  }
  
  @AfterClass
  public static void destroyThings() throws Exception {
    if (null != collection1) {
      collection1.close();
      collection1 = null;
    }
    if (null != collection2) {
      collection2.close();
      collection2 = null;
    }
    if (null != jetty) {
      jetty.stop();
      jetty=null;
    }
    resetExceptionIgnores();
    systemClearPropertySolrDisableShardsWhitelist();
  }
  
  @SuppressWarnings("unchecked")
  public void testGetFieldsPhaseError() throws SolrServerException, IOException {
    BadResponseWriter.failOnGetFields = true;
    BadResponseWriter.failOnGetTopIds = false;
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");
    query.set("distrib", "true");
    query.set("shards", shard1 + "," + shard2);
    query.set(ShardParams.SHARDS_INFO, "true");
    query.set("debug", "true");
    query.set("stats", "true");
    query.set("stats.field", "id");
    query.set("mlt", "true");
    query.set("mlt.fl", "title");
    query.set("mlt.count", "1");
    query.set("mlt.mintf", "0");
    query.set("mlt.mindf", "0");
    query.setHighlight(true);
    query.addFacetField("id");
    query.setFacet(true);
    
    ignoreException("Dummy exception in BadResponseWriter");

    expectThrows(SolrException.class, () -> collection1.query(query));

    query.set(ShardParams.SHARDS_TOLERANT, "true");
    QueryResponse response = collection1.query(query);
    assertTrue(response.getResponseHeader().getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
    NamedList<Object> shardsInfo = ((NamedList<Object>)response.getResponse().get(ShardParams.SHARDS_INFO));
    boolean foundError = false;
    for (int i = 0; i < shardsInfo.size(); i++) {
      if (shardsInfo.getName(i).contains("collection2")) {
        assertNotNull(((NamedList<Object>)shardsInfo.getVal(i)).get("error"));
        foundError = true;
        break;
      }
    }
    assertTrue(foundError);
    assertEquals("1", response.getResults().get(0).getFieldValue("id"));
    assertEquals("batman", response.getResults().get(0).getFirstValue("subject"));
    unIgnoreException("Dummy exception in BadResponseWriter");
  }
  
  @SuppressWarnings("unchecked")
  public void testGetTopIdsPhaseError() throws SolrServerException, IOException {
    BadResponseWriter.failOnGetTopIds = true;
    BadResponseWriter.failOnGetFields = false;
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");
    query.set("distrib", "true");
    query.set("shards", shard1 + "," + shard2);
    query.set(ShardParams.SHARDS_INFO, "true");
    query.set("debug", "true");
    query.set("stats", "true");
    query.set("stats.field", "id");
    query.set("mlt", "true");
    query.set("mlt.fl", "title");
    query.set("mlt.count", "1");
    query.set("mlt.mintf", "0");
    query.set("mlt.mindf", "0");
    query.setHighlight(true);
    query.addFacetField("id");
    query.setFacet(true);
    
    ignoreException("Dummy exception in BadResponseWriter");

    expectThrows(Exception.class, () -> collection1.query(query));

    query.set(ShardParams.SHARDS_TOLERANT, "true");
    QueryResponse response = collection1.query(query);
    assertTrue(response.getResponseHeader().getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
    NamedList<Object> shardsInfo = ((NamedList<Object>)response.getResponse().get(ShardParams.SHARDS_INFO));
    boolean foundError = false;
    for (int i = 0; i < shardsInfo.size(); i++) {
      if (shardsInfo.getName(i).contains("collection2")) {
        assertNotNull(((NamedList<Object>)shardsInfo.getVal(i)).get("error"));
        foundError = true;
        break;
      }
    }
    assertTrue(foundError);
    assertFalse(""+response, response.getResults().isEmpty());
    assertEquals("1", response.getResults().get(0).getFieldValue("id"));
    assertEquals("batman", response.getResults().get(0).getFirstValue("subject"));
    unIgnoreException("Dummy exception in BadResponseWriter");
  }
  
  public static class BadResponseWriter extends BinaryResponseWriter {
    
    private static boolean failOnGetFields = false;
    private static boolean failOnGetTopIds = false;
    
    public BadResponseWriter() {
      super();
    }
    
    @Override
    public void write(OutputStream out, SolrQueryRequest req,
        SolrQueryResponse response) throws IOException {
      
      // I want to fail on the shard request, not the original user request, and only on the 
      // GET_FIELDS phase 
      if (failOnGetFields && 
          "collection2".equals(req.getCore().getName())
          && "subject:batman OR subject:superman".equals(req.getParams().get("q", ""))
          && req.getParams().get("ids") != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
            "Dummy exception in BadResponseWriter");
      } else if (failOnGetTopIds 
          && "collection2".equals(req.getCore().getName())
          && "subject:batman OR subject:superman".equals(req.getParams().get("q", ""))
          && req.getParams().get("ids") == null
          && req.getParams().getBool("isShard", false) == true) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
            "Dummy exception in BadResponseWriter");
      }
      super.write(out, req, response);
    }
    
    
  }

  
}
