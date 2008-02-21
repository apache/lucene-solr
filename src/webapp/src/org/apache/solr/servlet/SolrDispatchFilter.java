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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;
import java.util.logging.Level;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.Config;
import org.apache.solr.core.MultiCore;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.QueryResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 */
public class SolrDispatchFilter implements Filter 
{
  final Logger log = Logger.getLogger(SolrDispatchFilter.class.getName());
  
  protected SolrCore singlecore;
  protected MultiCore multicore;
  protected SolrRequestParsers parsers;
  protected boolean handleSelect = false;
  protected String pathPrefix = null; // strip this from the beginning of a path
  protected String abortErrorMessage = null;
  protected String solrConfigFilename = null;
  
  public void init(FilterConfig config) throws ServletException 
  {
    log.info("SolrDispatchFilter.init()");

    boolean abortOnConfigurationError = true;
    try {
      // web.xml configuration
      this.pathPrefix = config.getInitParameter( "path-prefix" );
      this.solrConfigFilename = config.getInitParameter("solrconfig-filename");
      
      // Find a valid solr core
      SolrCore core = null;
      multicore = MultiCore.getRegistry();
      if( multicore.isEnabled() ) {
        log.info( "Using existing multicore configuration" );
      }
      else {
        String instanceDir = SolrResourceLoader.locateInstanceDir();
        File multiconfig = new File( instanceDir, "multicore.xml" );
        log.info( "looking for multicore.xml: "+multiconfig.getAbsolutePath() );
        if( multiconfig.exists() ) {
          multicore.load( instanceDir, multiconfig );
        }
      }
      if( multicore.isEnabled() ) {
        singlecore = null;
        core = multicore.getDefaultCore();
      }
      else {
        if (this.solrConfigFilename==null) {
          singlecore = new SolrCore( null, null, new SolrConfig(), null );
        } else {
          singlecore = new SolrCore( null, null, new SolrConfig(this.solrConfigFilename), null);
        }
        core = singlecore;
      }
      
      log.info("user.dir=" + System.getProperty("user.dir"));
      
      // Read global configuration
      // Only the first registered core configures the following attributes 
      Config globalConfig = core.getSolrConfig();

      long uploadLimitKB = globalConfig.getInt( 
          "requestDispatcher/requestParsers/@multipartUploadLimitInKB", 2000 ); // 2MB default
      
      boolean enableRemoteStreams = globalConfig.getBool( 
          "requestDispatcher/requestParsers/@enableRemoteStreaming", false ); 

      parsers = new SolrRequestParsers( enableRemoteStreams, uploadLimitKB );
      
      // Let this filter take care of /select?xxx format
      this.handleSelect = globalConfig.getBool( "requestDispatcher/@handleSelect", false ); 
      
      // should it keep going if we hit an error?
      abortOnConfigurationError = globalConfig.getBool("abortOnConfigurationError",true);
    }
    catch( Throwable t ) {
      // catch this so our filter still works
      log.log(Level.SEVERE, "Could not start SOLR. Check solr/home property", t);
      SolrConfig.severeErrors.add( t );
      SolrCore.log( t );
    }
    
    // Optionally abort if we found a sever error
    if( abortOnConfigurationError && SolrConfig.severeErrors.size() > 0 ) {
      StringWriter sw = new StringWriter();
      PrintWriter out = new PrintWriter( sw );
      out.println( "Severe errors in solr configuration.\n" );
      out.println( "Check your log files for more detailed information on what may be wrong.\n" );
      out.println( "If you want solr to continue after configuration errors, change: \n");
      out.println( " <abortOnConfigurationError>false</abortOnConfigurationError>\n" );
      if( multicore.isEnabled() ) {
        out.println( "in multicore.xml\n" );
      } 
      else {
        out.println( "in solrconfig.xml\n" );
      }
      
      for( Throwable t : SolrConfig.severeErrors ) {
        out.println( "-------------------------------------------------------------" );
        t.printStackTrace( out );
      }
      out.flush();
      
      // Servlet containers behave slightly differently if you throw an exception during 
      // initialization.  Resin will display that error for every page, jetty prints it in
      // the logs, but continues normally.  (We will see a 404 rather then the real error)
      // rather then leave the behavior undefined, lets cache the error and spit it out 
      // for every request.
      abortErrorMessage = sw.toString();
      //throw new ServletException( abortErrorMessage );
    }
    
    log.info("SolrDispatchFilter.init() done");
  }

  public void destroy() {
    multicore.shutdown();
    if( singlecore != null ) {
      singlecore.close();
    }
  }
  
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException 
  {
    if( abortErrorMessage != null ) {
      ((HttpServletResponse)response).sendError( 500, abortErrorMessage );
      return;
    }
    
    if( request instanceof HttpServletRequest) {
      SolrQueryRequest solrReq = null;
      HttpServletRequest req = (HttpServletRequest)request;
      HttpServletResponse resp = (HttpServletResponse)response;
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
        
        // By default use the single core.  If multicore is enabled, look for one.
        SolrRequestHandler handler = null;
        SolrCore core = singlecore;
        if( core == null ) {
          // Perhaps this is a muli-core admin page?
          if( path.equals( multicore.getAdminPath() ) ) {
            handler = multicore.getMultiCoreHandler();
          }
          else {
            idx = path.indexOf( "/", 1 );
            if( idx > 1 ) {
              // try to get the corename as a request parameter first
              String corename = path.substring( 1, idx );
              path = path.substring( idx );
              core = multicore.getCore( corename );
              // invalid core name is ok.  It could fall through to some other request
            }
          }
        }
        
        if( core != null ) {
          // Only try to parse the handler *if* a valid core exists
          // when multi-core is enabled, the path can lead to a null core.
          if( handler == null && path.length() > 1 ) { // don't match "" or "/" as valid path
            handler = core.getRequestHandler( path );
          }
          if( handler == null && handleSelect ) {
            if( "/select".equals( path ) || "/select/".equals( path ) ) {
              solrReq = parsers.parse( core, path, req );
              String qt = solrReq.getParams().get( CommonParams.QT );
              if( qt != null && qt.startsWith( "/" ) ) {
                throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Invalid query type.  Do not use /select to access: "+qt);
              }
              handler = core.getRequestHandler( qt );
              if( handler == null ) {
                throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+qt);
              }
            }
          }
        }
        
        if( handler != null ) {
          if( core == null ) {
            core = multicore.getDefaultCore();
          }
          
          if( solrReq == null ) {
            solrReq = parsers.parse( core, path, req );
          }
          
          final SolrConfig conf = core.getSolrConfig();
          final Method reqMethod = Method.getMethod(req.getMethod());

          if (Method.POST != reqMethod) {
            HttpCacheHeaderUtil.setCacheControlHeader(conf, resp);
          }
            
          // unless we have been explicitly told not to, do cache validation
          if (!conf.getHttpCachingConfig().isNever304()) {
            // if we've confirmed cache validation, return immediately
            if (HttpCacheHeaderUtil.doCacheHeaderValidation(solrReq,
                                                            req,resp)) {
              return;
            }
          }
          
          SolrQueryResponse solrRsp = new SolrQueryResponse();
          /* even for HEAD requests, we need to execute the handler to
           * ensure we don't get an error (and to make sure the correct 
           * QueryResponseWriter is selectedand we get the correct
           * Content-Type)
           */
          this.execute( req, handler, solrReq, solrRsp );
          if( solrRsp.getException() != null ) {
            sendError( (HttpServletResponse)response, solrRsp.getException() );
            return;
          }
          
          // Now write it out
          QueryResponseWriter responseWriter = core.getQueryResponseWriter(solrReq);
          response.setContentType(responseWriter.getContentType(solrReq, solrRsp));
          if (Method.HEAD == Method.getMethod(req.getMethod())) {
            // nothing to write out, waited this long just to get ContentType
            return; 
          }
          
          PrintWriter out = response.getWriter();
          responseWriter.write(out, solrReq, solrRsp);
          return;
        }
        // otherwise, let's ensure the core is in the SolrCore request attribute so
        // the servlet/jsp can retrieve it
        else {
          req.setAttribute("org.apache.solr.SolrCore", core);
          
          // Modify the request so each core gets its own /admin
          if( singlecore == null && path.startsWith( "/admin" ) ) {
            req.getRequestDispatcher( path ).forward( request, response );
            return;
          }
        }
      }
      catch( Throwable ex ) {
        sendError( (HttpServletResponse)response, ex );
        return;
      }
      finally {
        if( solrReq != null ) {
          solrReq.close();
        }
      }
    }
    
    // Otherwise let the webapp handle the request
    chain.doFilter(request, response);
  }

  protected void execute( HttpServletRequest req, SolrRequestHandler handler, SolrQueryRequest sreq, SolrQueryResponse rsp) {
    // a custom filter could add more stuff to the request before passing it on.
    // for example: sreq.getContext().put( "HttpServletRequest", req );
    sreq.getCore().execute( handler, sreq, rsp );
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
