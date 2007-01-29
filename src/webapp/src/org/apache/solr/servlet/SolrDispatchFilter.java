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

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.core.Config;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.request.QueryResponseWriter;
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
  
  public void init(FilterConfig config) throws ServletException 
  {
    log.info("SolrDispatchFilter.init()");
    try {
      Context c = new InitialContext();

      /***
      System.out.println("Enumerating JNDI Context=" + c);
      NamingEnumeration<NameClassPair> en = c.list("java:comp/env");
      while (en.hasMore()) {
        NameClassPair ncp = en.next();
        System.out.println("  ENTRY:" + ncp);
      }
      System.out.println("JNDI lookup=" + c.lookup("java:comp/env/solr/home"));
      ***/

      String home = (String)c.lookup("java:comp/env/solr/home");
      if (home!=null) Config.setInstanceDir(home);
    } catch (NoInitialContextException e) {
      log.info("JNDI not configured for Solr (NoInitialContextEx)");
    } catch (NamingException e) {
      log.info("No /solr/home in JNDI");
    }

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
        int idx = path.indexOf( ':' );
        if( idx > 0 ) {
          // save the portion after the ':' for a 'handler' path parameter
          path = path.substring( 0, idx );
        }
        
        SolrRequestHandler handler = core.getRequestHandler( path );
        if( handler != null ) {
          SolrQueryRequest solrReq = parsers.parse( path, req );
          SolrQueryResponse solrRsp = new SolrQueryResponse();
          core.execute( handler, solrReq, solrRsp );
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
}
