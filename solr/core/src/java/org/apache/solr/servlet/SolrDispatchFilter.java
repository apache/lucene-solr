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

package org.apache.solr.servlet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.request.ServletSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.util.FastWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter implements Filter
{
  final Logger log = LoggerFactory.getLogger(SolrDispatchFilter.class);

  protected volatile CoreContainer cores;

  protected String pathPrefix = null; // strip this from the beginning of a path
  protected String abortErrorMessage = null;
  protected final Map<SolrConfig, SolrRequestParsers> parsers = new WeakHashMap<SolrConfig, SolrRequestParsers>();
  protected final SolrRequestParsers adminRequestParser;
  
  private static final Charset UTF8 = Charset.forName("UTF-8");

  public SolrDispatchFilter() {
    try {
      adminRequestParser = new SolrRequestParsers(new Config(null,"solr",new InputSource(new ByteArrayInputStream("<root/>".getBytes("UTF-8"))),"") );
    } catch (Exception e) {
      //unlikely
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,e);
    }
  }

  public void init(FilterConfig config) throws ServletException
  {
    log.info("SolrDispatchFilter.init()");

    CoreContainer.Initializer init = createInitializer();
    try {
      // web.xml configuration
      this.pathPrefix = config.getInitParameter( "path-prefix" );

      this.cores = init.initialize();
      log.info("user.dir=" + System.getProperty("user.dir"));
    }
    catch( Throwable t ) {
      // catch this so our filter still works
      log.error( "Could not start Solr. Check solr/home property and the logs");
      SolrCore.log( t );
    }

    log.info("SolrDispatchFilter.init() done");
  }
  
  public CoreContainer getCores() {
    return cores;
  }

  /** Method to override to change how CoreContainer initialization is performed. */
  protected CoreContainer.Initializer createInitializer() {
    return new CoreContainer.Initializer();
  }
  
  public void destroy() {
    if (cores != null) {
      cores.shutdown();
      cores = null;
    }    
  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    if( abortErrorMessage != null ) {
      ((HttpServletResponse)response).sendError( 500, abortErrorMessage );
      return;
    }
    
    if (this.cores == null) {
      ((HttpServletResponse)response).sendError( 503, "Server is shutting down" );
      return;
    }
    CoreContainer cores = this.cores;
    SolrCore core = null;
    SolrQueryRequest solrReq = null;
    
    if( request instanceof HttpServletRequest) {
      HttpServletRequest req = (HttpServletRequest)request;
      HttpServletResponse resp = (HttpServletResponse)response;
      SolrRequestHandler handler = null;
      String corename = "";
      try {
        // put the core container in request attribute
        req.setAttribute("org.apache.solr.CoreContainer", cores);
        String path = req.getServletPath();
        if( req.getPathInfo() != null ) {
          // this lets you handle /update/commit when /update is a servlet
          path += req.getPathInfo();
        }
        if( pathPrefix != null && path.startsWith( pathPrefix ) ) {
          path = path.substring( pathPrefix.length() );
        }
        // check for management path
        String alternate = cores.getManagementPath();
        if (alternate != null && path.startsWith(alternate)) {
          path = path.substring(0, alternate.length());
        }
        // unused feature ?
        int idx = path.indexOf( ':' );
        if( idx > 0 ) {
          // save the portion after the ':' for a 'handler' path parameter
          path = path.substring( 0, idx );
        }

        // Check for the core admin page
        if( path.equals( cores.getAdminPath() ) ) {
          handler = cores.getMultiCoreHandler();
          solrReq =  adminRequestParser.parse(null,path, req);
          handleAdminRequest(req, response, handler, solrReq);
          return;
        }
        // Check for the core admin collections url
        if( path.equals( "/admin/collections" ) ) {
          handler = cores.getCollectionsHandler();
          solrReq =  adminRequestParser.parse(null,path, req);
          handleAdminRequest(req, response, handler, solrReq);
          return;
        }
        else {
          //otherwise, we should find a core from the path
          idx = path.indexOf( "/", 1 );
          if( idx > 1 ) {
            // try to get the corename as a request parameter first
            corename = path.substring( 1, idx );
            core = cores.getCore(corename);
            if (core != null) {
              path = path.substring( idx );
            }
          }
          if (core == null) {
            if (!cores.isZooKeeperAware() ) {
              core = cores.getCore("");
            }
          }
        }
        
        if (core == null && cores.isZooKeeperAware()) {
          // we couldn't find the core - lets make sure a collection was not specified instead
          core = getCoreByCollection(cores, corename, path);
          
          if (core != null) {
            // we found a core, update the path
            path = path.substring( idx );
          } else {
            // try the default core
            core = cores.getCore("");
          }
          // TODO: if we couldn't find it locally, look on other nodes
        }

        // With a valid core...
        if( core != null ) {
          final SolrConfig config = core.getSolrConfig();
          // get or create/cache the parser for the core
          SolrRequestParsers parser = null;
          parser = parsers.get(config);
          if( parser == null ) {
            parser = new SolrRequestParsers(config);
            parsers.put(config, parser );
          }

          // Determine the handler from the url path if not set
          // (we might already have selected the cores handler)
          if( handler == null && path.length() > 1 ) { // don't match "" or "/" as valid path
            handler = core.getRequestHandler( path );
            // no handler yet but allowed to handle select; let's check
            if( handler == null && parser.isHandleSelect() ) {
              if( "/select".equals( path ) || "/select/".equals( path ) ) {
                solrReq = parser.parse( core, path, req );
                String qt = solrReq.getParams().get( CommonParams.QT );
                handler = core.getRequestHandler( qt );
                if( handler == null ) {
                  throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+qt);
                }
                if( qt != null && qt.startsWith("/") && (handler instanceof ContentStreamHandlerBase)) {
                  //For security reasons it's a bad idea to allow a leading '/', ex: /select?qt=/update see SOLR-3161
                  //There was no restriction from Solr 1.4 thru 3.5 and it's not supported for update handlers.
                  throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Invalid Request Handler ('qt').  Do not use /select to access: "+qt);
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
            HttpCacheHeaderUtil.setCacheControlHeader(config, resp, reqMethod);
            // unless we have been explicitly told not to, do cache validation
            // if we fail cache validation, execute the query
            if (config.getHttpCachingConfig().isNever304() ||
                !HttpCacheHeaderUtil.doCacheHeaderValidation(solrReq, req, reqMethod, resp)) {
                SolrQueryResponse solrRsp = new SolrQueryResponse();
                /* even for HEAD requests, we need to execute the handler to
                 * ensure we don't get an error (and to make sure the correct
                 * QueryResponseWriter is selected and we get the correct
                 * Content-Type)
                 */
                SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp));
                this.execute( req, handler, solrReq, solrRsp );
                HttpCacheHeaderUtil.checkHttpCachingVeto(solrRsp, resp, reqMethod);
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
               QueryResponseWriter responseWriter = core.getQueryResponseWriter(solrReq);
               writeResponse(solrRsp, response, responseWriter, solrReq, reqMethod);
            }
            return; // we are done with a valid handler
          }
        }
        log.debug("no handler or core retrieved for " + path + ", follow through...");
      } 
      catch (Throwable ex) {
        sendError( core, solrReq, request, (HttpServletResponse)response, ex );
        return;
      } 
      finally {
        if( solrReq != null ) {
          solrReq.close();
        }
        if (core != null) {
          core.close();
        }
        SolrRequestInfo.clearRequestInfo();        
      }
    }

    // Otherwise let the webapp handle the request
    chain.doFilter(request, response);
  }
  
  private SolrCore getCoreByCollection(CoreContainer cores, String corename, String path) {
    String collection = corename;
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    
    ClusterState clusterState = zkStateReader.getClusterState();
    Map<String,Slice> slices = clusterState.getSlices(collection);
    if (slices == null) {
      return null;
    }
    // look for a core on this node
    Set<Entry<String,Slice>> entries = slices.entrySet();
    SolrCore core = null;
    done:
    for (Entry<String,Slice> entry : entries) {
      // first see if we have the leader
      ZkNodeProps leaderProps = clusterState.getLeader(collection, entry.getKey());
      if (leaderProps != null) {
        core = checkProps(cores, path, leaderProps);
      }
      if (core != null) {
        break done;
      }
      
      // check everyone then
      Map<String,Replica> shards = entry.getValue().getReplicasMap();
      Set<Entry<String,Replica>> shardEntries = shards.entrySet();
      for (Entry<String,Replica> shardEntry : shardEntries) {
        Replica zkProps = shardEntry.getValue();
        core = checkProps(cores, path, zkProps);
        if (core != null) {
          break done;
        }
      }
    }
    return core;
  }

  private SolrCore checkProps(CoreContainer cores, String path,
      ZkNodeProps zkProps) {
    String corename;
    SolrCore core = null;
    if (cores.getZkController().getNodeName().equals(zkProps.getStr(ZkStateReader.NODE_NAME_PROP))) {
      corename = zkProps.getStr(ZkStateReader.CORE_NAME_PROP);
      core = cores.getCore(corename);
    }
    return core;
  }

  private void handleAdminRequest(HttpServletRequest req, ServletResponse response, SolrRequestHandler handler,
                                  SolrQueryRequest solrReq) throws IOException {
    SolrQueryResponse solrResp = new SolrQueryResponse();
    final NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
    solrResp.add("responseHeader", responseHeader);
    NamedList toLog = solrResp.getToLog();
    toLog.add("webapp", req.getContextPath());
    toLog.add("path", solrReq.getContext().get("path"));
    toLog.add("params", "{" + solrReq.getParamString() + "}");
    handler.handleRequest(solrReq, solrResp);
    SolrCore.setResponseHeaderValues(handler, solrReq, solrResp);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < toLog.size(); i++) {
      String name = toLog.getName(i);
      Object val = toLog.getVal(i);
      sb.append(name).append("=").append(val).append(" ");
    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard");
    writeResponse(solrResp, response, respWriter, solrReq, Method.getMethod(req.getMethod()));
  }

  private void writeResponse(SolrQueryResponse solrRsp, ServletResponse response,
                             QueryResponseWriter responseWriter, SolrQueryRequest solrReq, Method reqMethod)
          throws IOException {

    // Now write it out
    final String ct = responseWriter.getContentType(solrReq, solrRsp);
    // don't call setContentType on null
    if (null != ct) response.setContentType(ct); 

    if (solrRsp.getException() != null) {
      NamedList info = new SimpleOrderedMap();
      int code = getErrorInfo(solrRsp.getException(),info);
      solrRsp.add("error", info);
      ((HttpServletResponse) response).setStatus(code);
    }
    
    if (Method.HEAD != reqMethod) {
      if (responseWriter instanceof BinaryQueryResponseWriter) {
        BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
        binWriter.write(response.getOutputStream(), solrReq, solrRsp);
      } else {
        String charset = ContentStreamBase.getCharsetFromContentType(ct);
        Writer out = (charset == null || charset.equalsIgnoreCase("UTF-8"))
          ? new OutputStreamWriter(response.getOutputStream(), UTF8)
          : new OutputStreamWriter(response.getOutputStream(), charset);
        out = new FastWriter(out);
        responseWriter.write(out, solrReq, solrRsp);
        out.flush();
      }
    }
    //else http HEAD request, nothing to write out, waited this long just to get ContentType
  }
  
  protected int getErrorInfo(Throwable ex, NamedList info) {
    int code=500;
    if( ex instanceof SolrException ) {
      code = ((SolrException)ex).code();
    }

    String msg = null;
    for (Throwable th = ex; th != null; th = th.getCause()) {
      msg = th.getMessage();
      if (msg != null) break;
    }
    if(msg != null) {
      info.add("msg", msg);
    }
    
    // For any regular code, don't include the stack trace
    if( code == 500 || code < 100 ) {
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      SolrException.log(log, null, ex);
      info.add("trace", sw.toString());

      // non standard codes have undefined results with various servers
      if( code < 100 ) {
        log.warn( "invalid return code: "+code );
        code = 500;
      }
    }
    info.add("code", new Integer(code));
    return code;
  }

  protected void execute( HttpServletRequest req, SolrRequestHandler handler, SolrQueryRequest sreq, SolrQueryResponse rsp) {
    // a custom filter could add more stuff to the request before passing it on.
    // for example: sreq.getContext().put( "HttpServletRequest", req );
    // used for logging query stats in SolrCore.execute()
    sreq.getContext().put( "webapp", req.getContextPath() );
    sreq.getCore().execute( handler, sreq, rsp );
  }

  protected void sendError(SolrCore core, 
      SolrQueryRequest req, 
      ServletRequest request, 
      HttpServletResponse response, 
      Throwable ex) throws IOException {
    try {
      SolrQueryResponse solrResp = new SolrQueryResponse();
      if(ex instanceof Exception) {
        solrResp.setException((Exception)ex);
      }
      else {
        solrResp.setException(new RuntimeException(ex));
      }
      if(core==null) {
        core = cores.getCore(""); // default core
      }
      if(req==null) {
        req = new SolrQueryRequestBase(core,new ServletSolrParams(request)) {};
      }
      QueryResponseWriter writer = core.getQueryResponseWriter(req);
      writeResponse(solrResp, response, writer, req, Method.GET);
    }
    catch( Throwable t ) { // This error really does not matter
      SimpleOrderedMap info = new SimpleOrderedMap();
      int code=getErrorInfo(ex, info);
      response.sendError( code, info.toString() );
    }
  }

  //---------------------------------------------------------------------
  //---------------------------------------------------------------------

  /**
   * Set the prefix for all paths.  This is useful if you want to apply the
   * filter to something other then /*, perhaps because you are merging this
   * filter into a larger web application.
   *
   * For example, if web.xml specifies:
   * <pre class="prettyprint">
   * {@code
   * <filter-mapping>
   *  <filter-name>SolrRequestFilter</filter-name>
   *  <url-pattern>/xxx/*</url-pattern>
   * </filter-mapping>}
   * </pre>
   *
   * Make sure to set the PathPrefix to "/xxx" either with this function
   * or in web.xml.
   *
   * <pre class="prettyprint">
   * {@code
   * <init-param>
   *  <param-name>path-prefix</param-name>
   *  <param-value>/xxx</param-value>
   * </init-param>}
   * </pre>
   */
  public void setPathPrefix(String pathPrefix) {
    this.pathPrefix = pathPrefix;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }
}
