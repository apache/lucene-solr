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

package org.apache.solr.handler.admin;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class InfoHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  @Test
  public void testCoreAdminHandler() throws Exception {

    final CoreContainer cores = h.getCoreContainer();
    InfoHandler infoHandler = cores.getInfoHandler();
    SolrQueryResponse rsp = handleRequest(infoHandler, "properties");
    
    assertNotNull(rsp.getValues().get("system.properties"));

    
    rsp = handleRequest(infoHandler, "threads");
    
    assertNotNull(rsp.getValues().get("system"));
    
    rsp = handleRequest(infoHandler, "logging");
    
    assertNotNull(rsp.getValues().get("watcher"));
    
    try {
      rsp = handleRequest(infoHandler, "info");
      fail("Should have failed with not found");
    } catch(SolrException e) {
      assertEquals(404, e.code());
    }
    
    try {
      rsp = handleRequest(infoHandler, "");
      fail("Should have failed with not found");
    } catch(SolrException e) {
      assertEquals(404, e.code());
    }

    
  }

  private SolrQueryResponse handleRequest(InfoHandler infoHandler, String path)
      throws Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = req();
    req.getContext().put("path", path);
    infoHandler.handleRequestBody(req, rsp);
    return rsp;
  }

}
