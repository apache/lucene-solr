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
import java.util.WeakHashMap;
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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.MultiCore;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.*;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter implements Filter
{
  final Logger log = Logger.getLogger(SolrDispatchFilter.class.getName());

  protected SolrCore singlecore;
  protected MultiCore multicore;
  protected String pathPrefix = null; // strip this from the beginning of a path
  protected String abortErrorMessage = null;
  protected final WeakHashMap<SolrCore, SolrRequestParsers> parsers = new WeakHashMap<SolrCore, SolrRequestParsers>();
  protected String solrConfigFilename = null;

  public void init(FilterConfig config) throws ServletException
  {
    log.info("SolrDispatchFilter.init()");

    boolean abortOnConfigurationError = true;
    try {
      // web.xml configuration
      this.pathPrefix = config.getInitParameter( "path-prefix" );
      this.solrConfigFilename = config.getInitParameter("solrconfig-filename");

      // multicore instantiation
      this.multicore = initMultiCore(config);

      if(multicore != null && multicore.isEnabled() ) {
        abortOnConfigurationError = false;
        singlecore = null;
        // if any core aborts on startup, then abort
        for( SolrCore c : multicore.getCores() ) {
          if( c.getSolrConfig().getBool( "abortOnConfigurationError",false) ) {
            abortOnConfigurationError = true;
            break;
          }
        }
      }
      else {
        SolrConfig cfg = this.solrConfigFilename == null? new SolrConfig() : new SolrConfig(this.solrConfigFilename);
        singlecore = new SolrCore( null, null, cfg, null );
        abortOnConfigurationError = cfg.getBool(
                "abortOnConfigurationError", abortOnConfigurationError);
      }
      log.info("user.dir=" + System.getProperty("user.dir"));
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
      if (multicore != null && multicore.isEnabled()) {
        out.println( "in multicore.xml\n" );
      } else {
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

  /**
   * Initialize the multicore instance.
   * @param config the filter configuration
   * @return the multicore instance or null
   * @throws java.lang.Exception
   */
  protected MultiCore initMultiCore(FilterConfig config) throws Exception {
    MultiCore mcore = new MultiCore();
    String instanceDir = SolrResourceLoader.locateInstanceDir();
    File fconf = new File(instanceDir, "multicore.xml");
    log.info("looking for multicore.xml: " + fconf.getAbsolutePath());
    if (fconf.exists()) {
      mcore.load(instanceDir, fconf);
    }
    return mcore;
  }


  public void destroy() {
    if (multicore != null) {
    multicore.shutdown();
      multicore = null;
    }
    if( singlecore != null ) {
      singlecore.close();
      singlecore = null;
    }
  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    if( abortErrorMessage != null ) {
      ((HttpServletResponse)response).sendError( 500, abortErrorMessage );
      return;
    }

    if( request instanceof HttpServletRequest) {
      HttpServletRequest req = (HttpServletRequest)request;
      HttpServletResponse resp = (HttpServletResponse)response;
      SolrRequestHandler handler = null;
      SolrQueryRequest solrReq = null;

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
        final SolrCore core;
        if (multicore != null && multicore.isEnabled()) {
          req.setAttribute("org.apache.solr.MultiCore", multicore);

          // if this is the multi-core admin page, it will handle it
          if( path.equals( multicore.getAdminPath() ) ) {
            handler = multicore.getMultiCoreHandler();
            // pick a core to use for output generation
            core = multicore.getAdminCore();
            if( core == null ) {
              throw new RuntimeException( "Can not find a valid core for the multicore admin handler" );
            }
          } else {
            //otherwise, we should find a core from the path
            idx = path.indexOf( "/", 1 );
            if( idx > 1 ) {
              // try to get the corename as a request parameter first
              String corename = path.substring( 1, idx );
              path = path.substring( idx );
              core = multicore.getCore( corename );
            } else {
              core = null;
            }
          }
        }
        else {
          core = singlecore;
        }

        // With a valid core...
        if( core != null ) {
          final SolrConfig config = core.getSolrConfig();
          // get or create/cache the parser for the core
          SolrRequestParsers parser = null;
          parser = parsers.get(core);
          if( parser == null ) {
            parser = new SolrRequestParsers(config);
            parsers.put( core, parser );
          }

          // Determine the handler from the url path if not set
          // (we might already have selected the multicore handler)
          if( handler == null && path.length() > 1 ) { // don't match "" or "/" as valid path
            handler = core.getRequestHandler( path );
            // no handler yet but allowed to handle select; let's check
            if( handler == null && parser.isHandleSelect() ) {
              if( "/select".equals( path ) || "/select/".equals( path ) ) {
                solrReq = parser.parse( core, path, req );
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

            // With a valid handler and a valid core...
          if( handler != null ) {
            // if not a /select, create the request
            if( solrReq == null ) {
              solrReq = parser.parse( core, path, req );
            }

            final Method reqMethod = Method.getMethod(req.getMethod());
            if (Method.POST != reqMethod) {
              HttpCacheHeaderUtil.setCacheControlHeader(config, resp);
            }
            // unless we have been explicitly told not to, do cache validation
            // if we fail cache validation, execute the query
            if (config.getHttpCachingConfig().isNever304() ||
                !HttpCacheHeaderUtil.doCacheHeaderValidation(solrReq, req, resp)) {
                SolrQueryResponse solrRsp = new SolrQueryResponse();
                /* even for HEAD requests, we need to execute the handler to
                 * ensure we don't get an error (and to make sure the correct
                 * QueryResponseWriter is selectedand we get the correct
                 * Content-Type)
                 */
                this.execute( req, handler, solrReq, solrRsp );
              // add info to http headers
              //TODO: See SOLR-232 and SOLR-267.  
                /*try {
                  NamedList solrRspHeader = solrRsp.getResponseHeader();
                 for (int i=0; i<solrRspHeader.size(); i++) {
                   ((javax.servlet.http.HttpServletResponse) response).addHeader(("Solr-" + solrRspHeader.getName(i)), String.valueOf(solrRspHeader.getVal(i)));
                 }
                } catch (ClassCastException cce) {
                  log.log(Level.WARNING, "exception adding response header log information", cce);
                }*/
                if( solrRsp.getException() != null ) {
                  sendError( (HttpServletResponse)response, solrRsp.getException() );
                }
                else {
                  // Now write it out
                  QueryResponseWriter responseWriter = core.getQueryResponseWriter(solrReq);
                  response.setContentType(responseWriter.getContentType(solrReq, solrRsp));
                  if (Method.HEAD != Method.getMethod(req.getMethod())) {
                    if (responseWriter instanceof BinaryQueryResponseWriter) {
                      BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
                      binWriter.write(response.getOutputStream(), solrReq, solrRsp);
                    } else {
                      PrintWriter out = response.getWriter();
                      responseWriter.write(out, solrReq, solrRsp);

                    }

                  }
                  //else http HEAD request, nothing to write out, waited this long just to get ContentType
                }
            }
            return; // we are done with a valid handler
          }
          // otherwise (we have a core), let's ensure the core is in the SolrCore request attribute so
          // a servlet/jsp can retrieve it
          else {
            req.setAttribute("org.apache.solr.SolrCore", core);
            // Modify the request so each core gets its own /admin
            if( singlecore == null && path.startsWith( "/admin" ) ) {
              req.getRequestDispatcher( pathPrefix == null ? path : pathPrefix + path ).forward( request, response );
              return;
            }
          }
        }
        log.fine("no handler or core retrieved for " + path + ", follow through...");
      } catch (Throwable ex) {
        sendError( (HttpServletResponse)response, ex );
        return;
      } finally {
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
    // used for logging query stats in SolrCore.execute()
    sreq.getContext().put( "webapp", req.getContextPath() );
    sreq.getCore().execute( handler, sreq, rsp );
  }

  protected void sendError(HttpServletResponse res, Throwable ex) throws IOException {
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
   * Set the prefix for all paths.  This is useful if you want to apply the
   * filter to something other then /*, perhaps because you are merging this
   * filter into a larger web application.
   *
   * For example, if web.xml specifies:
   *
   * <filter-mapping>
   *  <filter-name>SolrRequestFilter</filter-name>
   *  <url-pattern>/xxx/*</url-pattern>
   * </filter-mapping>
   *
   * Make sure to set the PathPrefix to "/xxx" either with this function
   * or in web.xml.
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
