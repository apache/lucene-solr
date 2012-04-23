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
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

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

  SimpleDateFormat formatRFC3339 = new SimpleDateFormat("yyyy-MM-dd'T'h:m:ss.SZ");
  protected enum ACTIONS {STATUS, ENABLE, DISABLE, PING};
  private String healthcheck = null;
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    
    SolrParams params = req.getParams();
    SolrCore core = req.getCore();
    
    // Check if the service is available
    healthcheck = core.getSolrConfig().get("admin/healthcheck/text()", null );
    
    String actionParam = params.get("action");
    ACTIONS action = null;
    if (actionParam == null){
      action = ACTIONS.PING;
    }
    else {
      try {
        action = ACTIONS.valueOf(actionParam.toUpperCase());
      }
      catch (IllegalArgumentException iae){
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
        "Unknown action: " + actionParam);
      }
    }
    switch(action){
      case PING:
        if( healthcheck != null && !new File(healthcheck).exists() ) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Service disabled");
        }
        handlePing(req, rsp);
        break;
      case ENABLE:
        handleEnable(healthcheck,true);
        break;
      case DISABLE:
        handleEnable(healthcheck,false);
        break;
      case STATUS:
        if( healthcheck == null){
          SolrException e = new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "healthcheck not configured");
          rsp.setException(e);
        }
        else {
          if ( new File(healthcheck).exists() ){
            rsp.add( "status",  "enabled");      
          }
          else {
            rsp.add( "status",  "disabled");      
          }
        }
    }

  }
  
  protected void handlePing(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    
    SolrParams params = req.getParams();
    SolrCore core = req.getCore();
    
    // Get the RequestHandler
    String qt = params.get( CommonParams.QT );//optional; you get the default otherwise
    SolrRequestHandler handler = core.getRequestHandler( qt );
    if( handler == null ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "Unknown RequestHandler (qt): "+qt );
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
  
  protected void handleEnable(String healthcheck, boolean enable) throws Exception
  {
    if (healthcheck == null) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, 
        "No healthcheck file defined.");
    }
    File enableFile = new File(healthcheck);
    if ( enable ) {
      enableFile.createNewFile();
      
      // write out when the file was created
      FileWriter fw = new FileWriter(enableFile);      
      fw.write(formatRFC3339.format(new Date()));
      fw.close(); 
      
    } else {
      if (enableFile.exists() && !enableFile.delete()){
        throw new SolrException( SolrException.ErrorCode.NOT_FOUND,"Did not successfully delete healthcheck file:'"+healthcheck+"'");
      }
    }
  }
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Reports application health to a load-balancer";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
