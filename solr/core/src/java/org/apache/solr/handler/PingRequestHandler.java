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
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 * Ping Request Handler for reporting SolrCore health to a Load Balancer.
 *
 * <p>
 * This handler is designed to be used as the endpoint for an HTTP 
 * Load-Balancer to use when checking the "health" or "up status" of a 
 * Solr server.
 * </p>
 * 
 * <p> 
 * In its simplest form, the PingRequestHandler should be
 * configured with some defaults indicating a request that should be
 * executed.  If the request succeeds, then the PingRequestHandler
 * will respond back with a simple "OK" status.  If the request fails,
 * then the PingRequestHandler will respond back with the
 * corresponding HTTP Error code.  Clients (such as load balancers)
 * can be configured to poll the PingRequestHandler monitoring for
 * these types of responses (or for a simple connection failure) to
 * know if there is a problem with the Solr server.
 * 
 * Note in case isShard=true, PingRequestHandler respond back with 
 * what the delegated handler returns (by default it's /select handler).
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;requestHandler name="/admin/ping" class="solr.PingRequestHandler"&gt;
 *   &lt;lst name="invariants"&gt;
 *     &lt;str name="qt"&gt;/search&lt;/str&gt;&lt;!-- handler to delegate to --&gt;
 *     &lt;str name="q"&gt;some test query&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 *
 * <p>
 * A more advanced option available, is to configure the handler with a 
 * "healthcheckFile" which can be used to enable/disable the PingRequestHandler.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;requestHandler name="/admin/ping" class="solr.PingRequestHandler"&gt;
 *   &lt;!-- relative paths are resolved against the data dir --&gt;
 *   &lt;str name="healthcheckFile"&gt;server-enabled.txt&lt;/str&gt;
 *   &lt;lst name="invariants"&gt;
 *     &lt;str name="qt"&gt;/search&lt;/str&gt;&lt;!-- handler to delegate to --&gt;
 *     &lt;str name="q"&gt;some test query&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 *
 * <ul>
 *   <li>If the health check file exists, the handler will execute the 
 *       delegated query and return status as described above.
 *   </li>
 *   <li>If the health check file does not exist, the handler will return 
 *       an HTTP error even if the server is working fine and the delegated 
 *       query would have succeeded
 *   </li>
 * </ul>
 *
 * <p> 
 * This health check file feature can be used as a way to indicate
 * to some Load Balancers that the server should be "removed from
 * rotation" for maintenance, or upgrades, or whatever reason you may
 * wish.  
 * </p>
 *
 * <p> 
 * The health check file may be created/deleted by any external
 * system, or the PingRequestHandler itself can be used to
 * create/delete the file by specifying an "action" param in a
 * request: 
 * </p>
 *
 * <ul>
 *   <li><code>http://.../ping?action=enable</code>
 *       - creates the health check file if it does not already exist
 *   </li>
 *   <li><code>http://.../ping?action=disable</code>
 *       - deletes the health check file if it exists
 *   </li>
 *   <li><code>http://.../ping?action=status</code>
 *       - returns a status code indicating if the healthcheck file exists 
 *       ("<code>enabled</code>") or not ("<code>disabled</code>")
 *   </li>
 * </ul>
 *
 * @since solr 1.3
 */
public class PingRequestHandler extends RequestHandlerBase implements SolrCoreAware
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String HEALTHCHECK_FILE_PARAM = "healthcheckFile";
  protected enum ACTIONS {STATUS, ENABLE, DISABLE, PING};
  
  private String healthFileName = null;
  private File healthcheck = null;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
    Object tmp = args.get(HEALTHCHECK_FILE_PARAM);
    healthFileName = (null == tmp ? null : tmp.toString());
  }

  @Override
  public void inform( SolrCore core ) {
    if (null != healthFileName) {
      healthcheck = new File(healthFileName);
      if ( ! healthcheck.isAbsolute()) {
        healthcheck = new File(core.getDataDir(), healthFileName);
        healthcheck = healthcheck.getAbsoluteFile();
      }

      if ( ! healthcheck.getParentFile().canWrite()) {
        // this is not fatal, users may not care about enable/disable via 
        // solr request, file might be touched/deleted by an external system
        log.warn("Directory for configured healthcheck file is not writable by solr, PingRequestHandler will not be able to control enable/disable: {}",
                 healthcheck.getParentFile().getAbsolutePath());
      }

    }
    
  }
  
  /**
   * Returns true if the healthcheck flag-file is enabled but does not exist, 
   * otherwise (no file configured, or file configured and exists) 
   * returns false. 
   */
  public boolean isPingDisabled() {
    return (null != healthcheck && ! healthcheck.exists() );
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    
    SolrParams params = req.getParams();
    
    // in this case, we want to default distrib to false so
    // we only ping the single node
    Boolean distrib = params.getBool(DISTRIB);
    if (distrib == null)   {
      ModifiableSolrParams mparams = new ModifiableSolrParams(params);
      mparams.set(DISTRIB, false);
      req.setParams(mparams);
    }
    
    String actionParam = params.get("action");
    ACTIONS action = null;
    if (actionParam == null){
      action = ACTIONS.PING;
    }
    else {
      try {
        action = ACTIONS.valueOf(actionParam.toUpperCase(Locale.ROOT));
      }
      catch (IllegalArgumentException iae){
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
        "Unknown action: " + actionParam);
      }
    }
    switch(action){
      case PING:
        if( isPingDisabled() ) {
          SolrException e = new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, 
                                  "Service disabled");
          rsp.setException(e);
          return;
        }
        handlePing(req, rsp);
        break;
      case ENABLE:
        handleEnable(true);
        break;
      case DISABLE:
        handleEnable(false);
        break;
      case STATUS:
        if( healthcheck == null ){
          SolrException e = new SolrException
            (SolrException.ErrorCode.SERVICE_UNAVAILABLE, 
             "healthcheck not configured");
          rsp.setException(e);
        } else {
          rsp.add( "status", isPingDisabled() ? "disabled" : "enabled" );      
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
      // In case it's a query for shard, use default handler     
      if (params.getBool(ShardParams.IS_SHARD, false)) {
        handler = core.getRequestHandler( null );
        ModifiableSolrParams wparams = new ModifiableSolrParams(params);
        wparams.remove(CommonParams.QT);
        req.setParams(wparams);
      } else { 
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
            "Cannot execute the PingRequestHandler recursively" );
      }
    }
    
    // Execute the ping query and catch any possible exception
    Throwable ex = null;
    
    // In case it's a query for shard, return the result from delegated handler for distributed query to merge result
    if (params.getBool(ShardParams.IS_SHARD, false)) {
      try {
        core.execute(handler, req, rsp );
        ex = rsp.getException(); 
      }
      catch( Exception e ) {
        ex = e;
      }
      // Send an error or return
      if( ex != null ) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
            "Ping query caused exception: "+ex.getMessage(), ex );
      }
    } else {
      try {
        SolrQueryResponse pingrsp = new SolrQueryResponse();
        core.execute(handler, req, pingrsp );
        ex = pingrsp.getException(); 
        NamedList<Object> headers = rsp.getResponseHeader();
        if(headers != null) {
          headers.add("zkConnected", pingrsp.getResponseHeader().get("zkConnected"));
        }
        
      }
      catch( Exception e ) {
        ex = e;
      }
      
      // Send an error or an 'OK' message (response code will be 200)
      if( ex != null ) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
            "Ping query caused exception: "+ex.getMessage(), ex );
      }
      
      rsp.add( "status", "OK" );     
    }   

  }
  
  protected void handleEnable(boolean enable) throws SolrException {
    if (healthcheck == null) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, 
        "No healthcheck file defined.");
    }
    if ( enable ) {
      try {
        // write out when the file was created
        FileUtils.write(healthcheck, Instant.now().toString(), "UTF-8");
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                                "Unable to write healthcheck flag file", e);
      }
    } else {
      try {
        Files.deleteIfExists(healthcheck.toPath());
      } catch (Throwable cause) {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
                                "Did not successfully delete healthcheck file: "
                                +healthcheck.getAbsolutePath(), cause);
      }
    }
  }
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Reports application health to a load-balancer";
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
