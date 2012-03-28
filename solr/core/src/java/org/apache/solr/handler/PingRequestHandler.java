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

import java.io.File;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;


/**
 * Ping solr core
 * 
 * @since solr 1.3
 */
public class PingRequestHandler extends RequestHandlerBase 
{
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    SolrParams params = req.getParams();
    SolrParams required = params.required();
    SolrCore core = req.getCore();
    
    // Check if the service is available
    String healthcheck = core.getSolrConfig().get("admin/healthcheck/text()", null );
    if( healthcheck != null && !new File(healthcheck).exists() ) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Service disabled");
    }
    
    // Get the RequestHandler
    String qt = required.get( CommonParams.QT );
    SolrRequestHandler handler = core.getRequestHandler( qt );
    if( handler == null ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "Unknown RequestHandler: "+qt );
    }
    
    if( handler instanceof PingRequestHandler ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "Cannot execute the PingRequestHandler recursively" );
    }
    
    // Execute the ping query and catch any possible exception
    Throwable ex = null;
    try {
      SolrQueryResponse pingrsp = new SolrQueryResponse();
      core.execute(handler, req, pingrsp );
      ex = pingrsp.getException();
    }
    catch( Throwable th ) {
      ex = th;
    }
    
    // Send an error or an 'OK' message (response code will be 200)
    if( ex != null ) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
          "Ping query caused exception: "+ex.getMessage(), ex );
    }
    rsp.add( "status", "OK" );
  }
  
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getDescription() {
    return "Reports application health to a load-balancer";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
