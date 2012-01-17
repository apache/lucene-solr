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

package org.apache.solr.update;

import java.util.HashMap;

import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.*;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;



public class UpdateParamsTest extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() { return "schema.xml"; }
  @Override
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  /**
   * Tests that only update.chain and not update.processor works (SOLR-2105)
   */
  public void testUpdateProcessorParamDeprecationRemoved() throws Exception {
    SolrCore core = h.getCore();
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    params.getMap().put("update.processor", "nonexistant");

    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    
    // First check that the old param behaves as it should
    try {
    	handler.handleRequestBody(req, rsp);
    	assertTrue("Old param update.processor should not have any effect anymore", true);
    } catch (Exception e) {
      assertFalse("Got wrong exception while testing update.chain", e.getMessage().equals("unknown UpdateRequestProcessorChain: nonexistant"));
    }
    
    // Then check that the new param behaves correctly
    params.getMap().remove("update.processor");
    params.getMap().put(UpdateParams.UPDATE_CHAIN, "nonexistant");    
    req.setParams(params);
    try {
    	handler.handleRequestBody(req, rsp);
    	assertFalse("Faulty update.chain parameter not causing an error - i.e. it is not detected", true);
    } catch (Exception e) {
    	assertEquals("Got wrong exception while testing update.chain", e.getMessage(), "unknown UpdateRequestProcessorChain: nonexistant");
    }
    
  }

}
