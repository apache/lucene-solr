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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;


/**
 * TODO -- this needs to actually test the results/query etc
 */
public class MoreLikeThisHandlerTest extends AbstractSolrTestCase {

  @Override public String getSchemaFile() { return "schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig.xml"; }
  @Override public void setUp() throws Exception {
    super.setUp();
    lrf = h.getRequestFactory("standard", 0, 20 );
  }
  
  public void testInterface()
  {
    MoreLikeThisHandler mlt = new MoreLikeThisHandler();
    SolrCore core = SolrCore.getSolrCore();
    
    Map<String,String[]> params = new HashMap<String,String[]>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams( params );
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, (SolrParams)mmparams ) {};
    
    // requires 'q' or single content stream
    try {
      mlt.handleRequestBody( req, new SolrQueryResponse() );
    }
    catch( Exception ex ) {} // expected

    // requires 'q' or single content stream
    try {
      ArrayList<ContentStream> streams = new ArrayList<ContentStream>( 2 );
      streams.add( new ContentStreamBase.StringStream( "hello" ) );
      streams.add( new ContentStreamBase.StringStream( "there" ) );
      req.setContentStreams( streams );
      mlt.handleRequestBody( req, new SolrQueryResponse() );
    }
    catch( Exception ex ) {} // expected
  }
}
