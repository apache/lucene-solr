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
package org.apache.solr.handler;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;

public class PingRequestHandlerTest extends SolrTestCaseJ4 {
  protected int NUM_SERVERS = 5;
  protected int NUM_SHARDS = 2;
  protected int REPLICATION_FACTOR = 2;

  private final String fileName = this.getClass().getName() + ".server-enabled";
  private File healthcheckFile = null;
  private PingRequestHandler handler = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void before() throws IOException {
    File tmpDir = initCoreDataDir;
    // by default, use relative file in dataDir
    healthcheckFile = new File(tmpDir, fileName);
    String fileNameParam = fileName;

    // sometimes randomly use an absolute File path instead 
    if (random().nextBoolean()) {
      healthcheckFile = new File(tmpDir, fileName);
      fileNameParam = healthcheckFile.getAbsolutePath();
    } 
      
    if (healthcheckFile.exists()) FileUtils.forceDelete(healthcheckFile);

    handler = new PingRequestHandler();
    NamedList initParams = new NamedList();
    initParams.add(PingRequestHandler.HEALTHCHECK_FILE_PARAM,
                   fileNameParam);
    handler.init(initParams);
    handler.inform(h.getCore());
  }
  
  public void testPingWithNoHealthCheck() throws Exception {
    
    // for this test, we don't want any healthcheck file configured at all
    handler = new PingRequestHandler();
    handler.init(new NamedList());
    handler.inform(h.getCore());

    SolrQueryResponse rsp = null;
    
    rsp = makeRequest(handler, req());
    assertEquals("OK", rsp.getValues().get("status"));
    
    rsp = makeRequest(handler, req("action","ping"));
    assertEquals("OK", rsp.getValues().get("status")); 

  }
  
  public void testEnablingServer() throws Exception {

    assertTrue(!healthcheckFile.exists());

    // first make sure that ping responds back that the service is disabled
    SolrQueryResponse sqr = makeRequest(handler, req());
    SolrException se = (SolrException) sqr.getException();
    assertEquals(
      "Response should have been replaced with a 503 SolrException.",
      se.code(), SolrException.ErrorCode.SERVICE_UNAVAILABLE.code);

    // now enable

    makeRequest(handler, req("action", "enable"));

    assertTrue(healthcheckFile.exists());
    assertNotNull(FileUtils.readFileToString(healthcheckFile, "UTF-8"));

    // now verify that the handler response with success

    SolrQueryResponse rsp = makeRequest(handler, req());
    assertEquals("OK", rsp.getValues().get("status"));

    // enable when already enabled shouldn't cause any problems
    makeRequest(handler, req("action", "enable"));
    assertTrue(healthcheckFile.exists());

  }
  public void testDisablingServer() throws Exception {

    assertTrue(! healthcheckFile.exists());
        
    healthcheckFile.createNewFile();

    // first make sure that ping responds back that the service is enabled

    SolrQueryResponse rsp = makeRequest(handler, req());
    assertEquals("OK", rsp.getValues().get("status"));

    // now disable
    
    makeRequest(handler, req("action", "disable"));
    
    assertFalse(healthcheckFile.exists());

    // now make sure that ping responds back that the service is disabled    
    SolrQueryResponse sqr = makeRequest(handler, req());
    SolrException se = (SolrException) sqr.getException();
    assertEquals(
      "Response should have been replaced with a 503 SolrException.",
      se.code(), SolrException.ErrorCode.SERVICE_UNAVAILABLE.code);
    
    // disable when already disabled shouldn't cause any problems
    makeRequest(handler, req("action", "disable"));
    assertFalse(healthcheckFile.exists());
    
  }

  
  public void testGettingStatus() throws Exception {
    SolrQueryResponse rsp = null;

    handler.handleEnable(true);
    
    rsp = makeRequest(handler, req("action", "status"));
    assertEquals("enabled", rsp.getValues().get("status"));
 
    handler.handleEnable(false);   
    
    rsp = makeRequest(handler, req("action", "status"));
    assertEquals("disabled", rsp.getValues().get("status"));
 
  }
  
  public void testBadActionRaisesException() throws Exception {
    
    try {
      SolrQueryResponse rsp = makeRequest(handler, req("action", "badaction"));
      fail("Should have thrown a SolrException for the bad action");
    } catch (SolrException se){
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code,se.code());
    }
  }

 public void testPingInClusterWithNoHealthCheck() throws Exception {

    MiniSolrCloudCluster miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), buildJettyConfig("/solr"));

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(NUM_SERVERS, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String collectionName = "testSolrCloudCollection";
      String configName = "solrCloudCollectionConfig";
      miniCluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1").resolve("conf"), configName);
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .process(miniCluster.getSolrClient());

      // Send distributed and non-distributed ping query
      SolrPingWithDistrib reqDistrib = new SolrPingWithDistrib();
      reqDistrib.setDistrib(true);
      SolrPingResponse rsp = reqDistrib.process(cloudSolrClient, collectionName);
      assertEquals(0, rsp.getStatus()); 
      assertTrue(rsp.getResponseHeader().getBooleanArg(("zkConnected")));

      
      SolrPing reqNonDistrib = new SolrPing();
      rsp = reqNonDistrib.process(cloudSolrClient, collectionName);
      assertEquals(0, rsp.getStatus());   
      assertTrue(rsp.getResponseHeader().getBooleanArg(("zkConnected")));

    }
    finally {
      miniCluster.shutdown();
    } 
  }


  /**
   * Helper Method: Executes the request against the handler, returns 
   * the response, and closes the request.
   */
  private SolrQueryResponse makeRequest(PingRequestHandler handler,
                                        SolrQueryRequest req) 
    throws Exception {

    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      handler.handleRequestBody(req, rsp);
    } finally {
      req.close();
    }
    return rsp;
  }

  class SolrPingWithDistrib extends SolrPing {
    public SolrPing setDistrib(boolean distrib) {   
      getParams().add("distrib", distrib ? "true" : "false");
      return this;    
    }      
  }
  

}
