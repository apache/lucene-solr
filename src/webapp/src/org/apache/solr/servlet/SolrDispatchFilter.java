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

package org.apache.solr.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.request.QueryResponseWriter;
import org.apache.solr.request.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 */
public class SolrDispatchFilter implements Filter 
{
  final Logger log = Logger.getLogger(SolrDispatchFilter.class.getName());
    
  protected SolrCore core;
  protected SolrRequestParsers parsers;
  protected boolean handleSelect = false;
  protected String pathPrefix = null; // strip this from the begging of a path
  
  public void init(FilterConfig config) throws ServletException 
  {
    log.info("SolrDispatchFilter.init()");
        
    // web.xml configuration
    this.pathPrefix = config.getInitParameter( "path-prefix" );
    this.handleSelect = "true".equals( config.getInitParameter( "handle-select" ) );
    
    log.info("user.dir=" + System.getProperty("user.dir"));
    core = SolrCore.getSolrCore();
    parsers = new SolrRequestParsers( core, SolrConfig.config );
    
    log.info("SolrDispatchFilter.init() done");
  }

  public void destroy() {
    core.close();
  }
  
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException 
  {
    if( request instanceof HttpServletRequest) {
      HttpServletRequest req = (HttpServletRequest)request;
      try {
        String path = req.getServletPath();    
        if( req.getPathInfo() != null ) {
          // this lets you handle /update/commit when /update is a servlet
          path += req.getPathInfo(); 
        }
        if( pathPrefix != null && path.startsWith( pathPrefix ) ) {
          path = path.substring( pathPrefix.length() );
        }
        
        int idx = path.indexOf( ':' );
        if( idx > 0 ) {
          // save the portion after the ':' for a 'handler' path parameter
          path = path.substring( 0, idx );
        }
        
        SolrQueryRequest solrReq = null;
        SolrRequestHandler handler = core.getRequestHandler( path );
        if( handler == null && handleSelect ) {
          if( "/select".equals( path ) || "/select/".equals( path ) ) {
            solrReq = parsers.parse( path, req );
            String qt = solrReq.getParams().get( SolrParams.QT );
            handler = core.getRequestHandler( qt );
            if( handler == null ) {
              throw new SolrException( 400, "unknown handler: "+qt);
            }
          }
        }
        if( handler != null ) {
          if( solrReq == null ) {
            solrReq = parsers.parse( path, req );
          }
          SolrQueryResponse solrRsp = new SolrQueryResponse();
          this.execute( req, handler, solrReq, solrRsp );
          if( solrRsp.getException() != null ) {
            sendError( (HttpServletResponse)response, solrRsp.getException() );
            return;
          }
          
          // Now write it out
          QueryResponseWriter responseWriter = core.getQueryResponseWriter(solrReq);
          response.setContentType(responseWriter.getContentType(solrReq, solrRsp));
          PrintWriter out = response.getWriter();
          responseWriter.write(out, solrReq, solrRsp);
          return;
        }
      }
      catch( Throwable ex ) {
        sendError( (HttpServletResponse)response, ex );
        return;
      }
    }
    
    // Otherwise let the webapp handle the request
    chain.doFilter(request, response);
  }

  protected void execute( HttpServletRequest req, SolrRequestHandler handler, SolrQueryRequest sreq, SolrQueryResponse rsp) {
    // a custom filter could add more stuff to the request before passing it on.
    // for example: sreq.getContext().put( "HttpServletRequest", req );
    core.execute( handler, sreq, rsp );
  }
  
  protected void sendError(HttpServletResponse res, Throwable ex) throws IOException 
  {
    int code=500;
    String trace = "";
    if( ex instanceof SolrException ) {
      code = ((SolrException)ex).code();
    }
    
    // For any regular code, don't include the stack trace
    if( code == 500 || code < 100 ) {  
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      trace = "\n\n"+sw.toString();
      
      SolrException.logOnce(log,null,ex );
      
      // non standard codes have undefined results with various servers
      if( code < 100 ) {
        log.warning( "invalid return code: "+code );
        code = 500;
      }
    }
    res.sendError( code, ex.getMessage() + trace );
  }

  //---------------------------------------------------------------------
  //---------------------------------------------------------------------

  /**
   * Should the filter handle /select even if it is not mapped in solrconfig.xml
   * 
   * This will use consistent error handling for /select?qt=xxx and /update/xml
   * 
   */
  public boolean isHandleSelect() {
    return handleSelect;
  }

  public void setHandleSelect(boolean handleSelect) {
    this.handleSelect = handleSelect;
  }

  /**
   * set the prefix for all paths.  This is useful if you want to apply the
   * filter to something other then *.  
   * 
   * For example, if web.xml specifies:
   * 
   * <filter-mapping>
   *  <filter-name>SolrRequestFilter</filter-name>
   *  <url-pattern>/xxx/*</url-pattern>
   * </filter-mapping>
   * 
   * Make sure to set the PathPrefix to "/xxx" either with this function
   * or in web.xml
   * 
   * <init-param>
   *  <param-name>path-prefix</param-name>
   *  <param-value>/xxx</param-value>
   * </init-param>
   * 
   */
  public void setPathPrefix(String pathPrefix) {
    this.pathPrefix = pathPrefix;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }
}
