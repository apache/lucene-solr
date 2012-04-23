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

package org.apache.solr.handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class PingRequestHandlerTest extends SolrTestCaseJ4 {

  private String healthcheck = "server-enabled";
  private File healthcheckFile = new File(healthcheck);
  private PingRequestHandler handler = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void before() {
    healthcheckFile.delete();
    handler = new PingRequestHandler();
  }
  
  @Test
  public void testPing() throws Exception {
     
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    
    handler.handleRequestBody(req, rsp);
    String status = (String) rsp.getValues().get("status");
    assertEquals("OK", status);
    req.close();
    
    req = req("action","ping");
    rsp = new SolrQueryResponse();
    
    handler.handleRequestBody(req, rsp);
    status = (String) rsp.getValues().get("status");
    assertEquals("OK", status);
    req.close();    
  }
  
  @Test
  public void testEnablingServer() throws Exception {
    
        
    handler.handleEnable(healthcheck, true);
    
    assertTrue(healthcheckFile.exists());
    
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();

    handler.handlePing(req, rsp);
    String status = (String) rsp.getValues().get("status");
    assertEquals("OK", status);
    req.close();
    
    FileReader fr = new FileReader(healthcheckFile);
  
    BufferedReader br = new BufferedReader(fr); 
    String s = br.readLine();
    assertNotNull(s);
     
  }
  
  @Test
  public void testDisablingServer() throws Exception {
    
        
    healthcheckFile.createNewFile();
    
    handler.handleEnable(healthcheck, false);
    
    assertFalse(healthcheckFile.exists());

    
  }
  
  @Test
  @Ignore // because of how we load the healthcheck file, we have to change it in schema.xml
  public void testGettingStatus() throws Exception {
    
        
    handler.handleEnable(healthcheck, true);
    
    
    SolrQueryRequest req = req("action", "status");
    SolrQueryResponse rsp = new SolrQueryResponse();
        
    handler.handleRequestBody(req, rsp);
    
    String status = (String) rsp.getValues().get("status");
    assertEquals("enabled", status);
    
    req.close();    
 
    handler.handleEnable(healthcheck, false);   
    
    req = req("action", "status");
    rsp = new SolrQueryResponse();
    
    handler.handleRequestBody(req, rsp);
        
    status = (String) rsp.getValues().get("status");
    assertEquals("disabled", status);
       
    req.close();    
 
  }
  
  @Test
  public void testBadActionRaisesException() throws Exception {
    
    SolrQueryRequest req = req("action", "badaction");
    SolrQueryResponse rsp = new SolrQueryResponse();
    
    try {
      handler.handleRequestBody(req, rsp);
      fail("Should have thrown a SolrException for the bad action");
    }
    catch (SolrException se){
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code,se.code());
    }

    req.close();    
    
  }
  
}
