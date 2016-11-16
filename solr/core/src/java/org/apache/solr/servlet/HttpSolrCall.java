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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.servlet.SolrDispatchFilter.Action;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.RTimerTree;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.FORWARD;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PASSTHROUGH;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETRY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETURN;

/**
 * This class represents a call made to Solr
 **/
public class HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final Random random;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      random = new Random();
    } else {
      random = new Random(seed.hashCode());
    }
  }

  protected final SolrDispatchFilter solrDispatchFilter;
  protected final CoreContainer cores;
  protected final HttpServletRequest req;
  protected final HttpServletResponse response;
  protected final boolean retry;
  protected SolrCore core = null;
  protected SolrQueryRequest solrReq = null;
  protected SolrRequestHandler handler = null;
  protected final SolrParams queryParams;
  protected String path;
  protected Action action;
  protected String coreUrl;
  protected SolrConfig config;
  protected Map<String, Integer> invalidStates;

  public RequestType getRequestType() {
    return requestType;
  }

  protected RequestType requestType;


  public List<String> getCollectionsList() {
    return collectionsList;
  }

  protected List<String> collectionsList;

  public HttpSolrCall(SolrDispatchFilter solrDispatchFilter, CoreContainer cores,
               HttpServletRequest request, HttpServletResponse response, boolean retry) {
    this.solrDispatchFilter = solrDispatchFilter;
    this.cores = cores;
    this.req = request;
    this.response = response;
    this.retry = retry;
    this.requestType = RequestType.UNKNOWN;
    queryParams = SolrRequestParsers.parseQueryString(req.getQueryString());
  }

  public String getPath() {
    return path;
  }


  public HttpServletRequest getReq() {
    return req;
  }

  public SolrCore getCore() {
    return core;
  }

  public SolrParams getQueryParams() {
    return queryParams;
  }
  
  void init() throws Exception {
    //The states of client that is invalid in this request
    Aliases aliases = null;
    String corename = "";
    String origCorename = null;
    // set a request timer which can be reused by requests if needed
    req.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimerTree());
    // put the core container in request attribute
    req.setAttribute("org.apache.solr.CoreContainer", cores);
    path = req.getServletPath();
    if (req.getPathInfo() != null) {
      // this lets you handle /update/commit when /update is a servlet
      path += req.getPathInfo();
    }
    // check for management path
    String alternate = cores.getManagementPath();
    if (alternate != null && path.startsWith(alternate)) {
      path = path.substring(0, alternate.length());
    }
    // unused feature ?
    int idx = path.indexOf(':');
    if (idx > 0) {
      // save the portion after the ':' for a 'handler' path parameter
      path = path.substring(0, idx);
    }

    boolean usingAliases = false;

    // Check for container handlers
    handler = cores.getRequestHandler(path);
    if (handler != null) {
      solrReq = SolrRequestParsers.DEFAULT.parse(null, path, req);
      solrReq.getContext().put(CoreContainer.class.getName(), cores);
      requestType = RequestType.ADMIN;
      action = ADMIN;
      return;
    } else {
      //otherwise, we should find a core from the path
      idx = path.indexOf("/", 1);
      if (idx > 1) {
        // try to get the corename as a request parameter first
        corename = path.substring(1, idx);

        // look at aliases
        if (cores.isZooKeeperAware()) {
          origCorename = corename;
          ZkStateReader reader = cores.getZkController().getZkStateReader();
          aliases = reader.getAliases();
          if (aliases != null && aliases.collectionAliasSize() > 0) {
            usingAliases = true;
            String alias = aliases.getCollectionAlias(corename);
            if (alias != null) {
              collectionsList = StrUtils.splitSmart(alias, ",", true);
              corename = collectionsList.get(0);
            }
          }
        }

        core = cores.getCore(corename);
        if (core != null) {
          path = path.substring(idx);
        } else if (cores.isCoreLoading(corename)) { // extra mem barriers, so don't look at this before trying to get core
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCore is loading");
        } else {
          // the core may have just finished loading
          core = cores.getCore(corename);
          if (core != null) {
            path = path.substring(idx);
          } 
        }
      }
      if (core == null) {
        if (!cores.isZooKeeperAware()) {
          core = cores.getCore("");
        }
      }
    }

    if (core == null && cores.isZooKeeperAware()) {
      // we couldn't find the core - lets make sure a collection was not specified instead
      boolean isPreferLeader = false;
      if (path.endsWith("/update") || path.contains("/update/")) {
        isPreferLeader = true;
      }
      core = getCoreByCollection(corename, isPreferLeader);
      if (core != null) {
        // we found a core, update the path
        path = path.substring(idx);
        if (collectionsList == null)
          collectionsList = new ArrayList<>();
        collectionsList.add(corename);
      }

      // if we couldn't find it locally, look on other nodes
      extractRemotePath(corename, origCorename, idx);
      if (action != null) return;
    }

    // With a valid core...
    if (core != null) {
      MDCLoggingContext.setCore(core);
      config = core.getSolrConfig();
      // get or create/cache the parser for the core
      SolrRequestParsers parser = config.getRequestParsers();


      // Determine the handler from the url path if not set
      // (we might already have selected the cores handler)
      extractHandlerFromURLPath(parser);
      if (action != null) return;

      // With a valid handler and a valid core...
      if (handler != null) {
        // if not a /select, create the request
        if (solrReq == null) {
          solrReq = parser.parse(core, path, req);
        }

        if (usingAliases) {
          processAliases(aliases, collectionsList);
        }

        action = PROCESS;
        return; // we are done with a valid handler
      }
    }
    log.debug("no handler or core retrieved for " + path + ", follow through...");

    action = PASSTHROUGH;
  }
  
  /**
   * Extract handler from the URL path if not set.
   * This returns true if the action is set.
   * 
   */
  private void extractHandlerFromURLPath(SolrRequestParsers parser) throws Exception {
    if (handler == null && path.length() > 1) { // don't match "" or "/" as valid path
      handler = core.getRequestHandler(path);

      if (handler == null) {
        //may be a restlet path
        // Handle /schema/* paths via Restlet
        if (path.equals("/schema") || path.startsWith("/schema/")) {
          solrReq = parser.parse(core, path, req);
          SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, new SolrQueryResponse()));
          if (path.equals(req.getServletPath())) {
            // avoid endless loop - pass through to Restlet via webapp
            action = PASSTHROUGH;
            return;
          } else {
            // forward rewritten URI (without path prefix and core/collection name) to Restlet
            action = FORWARD;
            return;
          }
        }

      }
      // no handler yet but allowed to handle select; let's check

      if (handler == null && parser.isHandleSelect()) {
        if ("/select".equals(path) || "/select/".equals(path)) {
          solrReq = parser.parse(core, path, req);
          invalidStates = checkStateIsValid(solrReq.getParams().get(CloudSolrClient.STATE_VERSION));
          String qt = solrReq.getParams().get(CommonParams.QT);
          handler = core.getRequestHandler(qt);
          if (handler == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "unknown handler: " + qt);
          }
          if (qt != null && qt.startsWith("/") && (handler instanceof ContentStreamHandlerBase)) {
            //For security reasons it's a bad idea to allow a leading '/', ex: /select?qt=/update see SOLR-3161
            //There was no restriction from Solr 1.4 thru 3.5 and it's not supported for update handlers.
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid Request Handler ('qt').  Do not use /select to access: " + qt);
          }
        }
      }
    }
  }

  private void extractRemotePath(String corename, String origCorename, int idx) throws UnsupportedEncodingException, KeeperException, InterruptedException {
    if (core == null && idx > 0) {
      coreUrl = getRemotCoreUrl(corename, origCorename);
      // don't proxy for internal update requests
      invalidStates = checkStateIsValid(queryParams.get(CloudSolrClient.STATE_VERSION));
      if (coreUrl != null
          && queryParams
          .get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) == null) {
        path = path.substring(idx);
        if (invalidStates != null) {
          //it does not make sense to send the request to a remote node
          throw new SolrException(SolrException.ErrorCode.INVALID_STATE, new String(Utils.toJSON(invalidStates), org.apache.lucene.util.IOUtils.UTF_8));
        }
        action = REMOTEQUERY;
      } else {
        if (!retry) {
          // we couldn't find a core to work with, try reloading aliases
          // TODO: it would be nice if admin ui elements skipped this...
          ZkStateReader reader = cores.getZkController()
              .getZkStateReader();
          reader.updateAliases();
          action = RETRY;
        }
      }
    }
  }

  /**
   * This method processes the request.
   */
  public Action call() throws IOException {
    MDCLoggingContext.reset();
    MDCLoggingContext.setNode(cores);

    if (cores == null) {
      sendError(503, "Server is shutting down or failed to initialize");
      return RETURN;
    }

    if (solrDispatchFilter.abortErrorMessage != null) {
      sendError(500, solrDispatchFilter.abortErrorMessage);
      return RETURN;
    }

    try {
      init();
      /* Authorize the request if
       1. Authorization is enabled, and
       2. The requested resource is not a known static file
        */
      if (cores.getAuthorizationPlugin() != null && shouldAuthorize()) {
        AuthorizationContext context = getAuthCtx();
        log.debug("AuthorizationContext : {}", context);
        AuthorizationResponse authResponse = cores.getAuthorizationPlugin().authorize(context);
        if (authResponse.statusCode == AuthorizationResponse.PROMPT.statusCode) {
          Map<String, String> headers = (Map) getReq().getAttribute(AuthenticationPlugin.class.getName());
          if (headers != null) {
            for (Map.Entry<String, String> e : headers.entrySet()) response.setHeader(e.getKey(), e.getValue());
          }
          log.debug("USER_REQUIRED "+req.getHeader("Authorization")+" "+ req.getUserPrincipal());
        }
        if (!(authResponse.statusCode == HttpStatus.SC_ACCEPTED) && !(authResponse.statusCode == HttpStatus.SC_OK)) {
          log.info("USER_REQUIRED auth header {} context : {} ", req.getHeader("Authorization"), context);
          sendError(authResponse.statusCode,
              "Unauthorized request, Response code: " + authResponse.statusCode);
          return RETURN;
        }
      }

      HttpServletResponse resp = response;
      switch (action) {
        case ADMIN:
          handleAdminRequest();
          return RETURN;
        case REMOTEQUERY:
          remoteQuery(coreUrl + path, resp);
          return RETURN;
        case PROCESS:
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
            execute(solrRsp);
            HttpCacheHeaderUtil.checkHttpCachingVeto(solrRsp, resp, reqMethod);
            Iterator<Map.Entry<String, String>> headers = solrRsp.httpHeaders();
            while (headers.hasNext()) {
              Map.Entry<String, String> entry = headers.next();
              resp.addHeader(entry.getKey(), entry.getValue());
            }
            QueryResponseWriter responseWriter = core.getQueryResponseWriter(solrReq);
            if (invalidStates != null) solrReq.getContext().put(CloudSolrClient.STATE_VERSION, invalidStates);
            writeResponse(solrRsp, responseWriter, reqMethod);
          }
          return RETURN;
        default: return action;
      }
    } catch (Throwable ex) {
      sendError(ex);
      // walk the the entire cause chain to search for an Error
      Throwable t = ex;
      while (t != null) {
        if (t instanceof Error) {
          if (t != ex) {
            log.error("An Error was wrapped in another exception - please report complete stacktrace on SOLR-6161", ex);
          }
          throw (Error) t;
        }
        t = t.getCause();
      }
      return RETURN;
    } finally {
      MDCLoggingContext.clear();
    }

  }

  private boolean shouldAuthorize() {
    if(PKIAuthenticationPlugin.PATH.equals(path)) return false;
    //admin/info/key is the path where public key is exposed . it is always unsecured
    if (cores.getPkiAuthenticationPlugin() != null && req.getUserPrincipal() != null) {
      boolean b = cores.getPkiAuthenticationPlugin().needsAuthorization(req);
      log.debug("PkiAuthenticationPlugin says authorization required : {} ", b);
      return b;
    }
    return true;
  }

  void destroy() {
    try {
      if (solrReq != null) {
        log.debug("Closing out SolrRequest: {}", solrReq);
        solrReq.close();
      }
    } finally {
      try {
        if (core != null) core.close();
      } finally {
        SolrRequestInfo.clearRequestInfo();
      }
      AuthenticationPlugin authcPlugin = cores.getAuthenticationPlugin();
      if (authcPlugin != null) authcPlugin.closeRequest();
    }
  }

  private void remoteQuery(String coreUrl, HttpServletResponse resp) throws IOException {
    HttpRequestBase method = null;
    HttpEntity httpEntity = null;
    try {
      String urlstr = coreUrl + queryParams.toQueryString();

      boolean isPostOrPutRequest = "POST".equals(req.getMethod()) || "PUT".equals(req.getMethod());
      if ("GET".equals(req.getMethod())) {
        method = new HttpGet(urlstr);
      } else if ("HEAD".equals(req.getMethod())) {
        method = new HttpHead(urlstr);
      } else if (isPostOrPutRequest) {
        HttpEntityEnclosingRequestBase entityRequest =
            "POST".equals(req.getMethod()) ? new HttpPost(urlstr) : new HttpPut(urlstr);
        InputStream in = new CloseShieldInputStream(req.getInputStream()); // Prevent close of container streams
        HttpEntity entity = new InputStreamEntity(in, req.getContentLength());
        entityRequest.setEntity(entity);
        method = entityRequest;
      } else if ("DELETE".equals(req.getMethod())) {
        method = new HttpDelete(urlstr);
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected method type: " + req.getMethod());
      }

      for (Enumeration<String> e = req.getHeaderNames(); e.hasMoreElements(); ) {
        String headerName = e.nextElement();
        if (!"host".equalsIgnoreCase(headerName)
            && !"authorization".equalsIgnoreCase(headerName)
            && !"accept".equalsIgnoreCase(headerName)) {
          method.addHeader(headerName, req.getHeader(headerName));
        }
      }
      // These headers not supported for HttpEntityEnclosingRequests
      if (method instanceof HttpEntityEnclosingRequest) {
        method.removeHeaders(TRANSFER_ENCODING_HEADER);
        method.removeHeaders(CONTENT_LENGTH_HEADER);
      }

      final HttpResponse response
          = solrDispatchFilter.httpClient.execute(method, HttpClientUtil.createNewHttpClientRequestContext());
      int httpStatus = response.getStatusLine().getStatusCode();
      httpEntity = response.getEntity();

      resp.setStatus(httpStatus);
      for (HeaderIterator responseHeaders = response.headerIterator(); responseHeaders.hasNext(); ) {
        Header header = responseHeaders.nextHeader();

        // We pull out these two headers below because they can cause chunked
        // encoding issues with Tomcat
        if (header != null && !header.getName().equalsIgnoreCase(TRANSFER_ENCODING_HEADER)
            && !header.getName().equalsIgnoreCase(CONNECTION_HEADER)) {
          resp.addHeader(header.getName(), header.getValue());
        }
      }

      if (httpEntity != null) {
        if (httpEntity.getContentEncoding() != null)
          resp.setCharacterEncoding(httpEntity.getContentEncoding().getValue());
        if (httpEntity.getContentType() != null) resp.setContentType(httpEntity.getContentType().getValue());

        InputStream is = httpEntity.getContent();
        OutputStream os = resp.getOutputStream();

        IOUtils.copyLarge(is, os);
      }

    } catch (IOException e) {
      sendError(new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error trying to proxy request for url: " + coreUrl, e));
    } finally {
      Utils.consumeFully(httpEntity);
    }

  }

  protected void sendError(Throwable ex) throws IOException {
    Exception exp = null;
    SolrCore localCore = null;
    try {
      SolrQueryResponse solrResp = new SolrQueryResponse();
      if (ex instanceof Exception) {
        solrResp.setException((Exception) ex);
      } else {
        solrResp.setException(new RuntimeException(ex));
      }
      localCore = core;
      if (solrReq == null) {
        final SolrParams solrParams;
        if (req != null) {
          // use GET parameters if available:
          solrParams = SolrRequestParsers.parseQueryString(req.getQueryString());
        } else {
          // we have no params at all, use empty ones:
          solrParams = new MapSolrParams(Collections.<String, String>emptyMap());
        }
        solrReq = new SolrQueryRequestBase(core, solrParams) {
        };
      }
      QueryResponseWriter writer = core.getQueryResponseWriter(solrReq);
      writeResponse(solrResp, writer, Method.GET);
    } catch (Exception e) { // This error really does not matter
      exp = e;
    } finally {
      try {
        if (exp != null) {
          SimpleOrderedMap info = new SimpleOrderedMap();
          int code = ResponseUtils.getErrorInfo(ex, info, log);
          sendError(code, info.toString());
        }
      } finally {
        if (core == null && localCore != null) {
          localCore.close();
        }
      }
    }
  }

  protected void sendError(int code, String message) throws IOException {
    try {
      response.sendError(code, message);
    } catch (EOFException e) {
      log.info("Unable to write error response, client closed connection or we are shutting down", e);
    }
  }

  protected void execute(SolrQueryResponse rsp) {
    // a custom filter could add more stuff to the request before passing it on.
    // for example: sreq.getContext().put( "HttpServletRequest", req );
    // used for logging query stats in SolrCore.execute()
    solrReq.getContext().put("webapp", req.getContextPath());
    solrReq.getCore().execute(handler, solrReq, rsp);
  }

  private void handleAdminRequest() throws IOException {
    SolrQueryResponse solrResp = new SolrQueryResponse();
    SolrCore.preDecorateResponse(solrReq, solrResp);
    handler.handleRequest(solrReq, solrResp);
    SolrCore.postDecorateResponse(handler, solrReq, solrResp);
    if (log.isInfoEnabled() && solrResp.getToLog().size() > 0) {
      log.info(solrResp.getToLogAsString("[admin]"));
    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard");
    writeResponse(solrResp, respWriter, Method.getMethod(req.getMethod()));
  }

  private void processAliases(Aliases aliases,
                              List<String> collectionsList) {
    String collection = solrReq.getParams().get(COLLECTION_PROP);
    if (collection != null) {
      collectionsList = StrUtils.splitSmart(collection, ",", true);
    }
    if (collectionsList != null) {
      Set<String> newCollectionsList = new HashSet<>(
          collectionsList.size());
      for (String col : collectionsList) {
        String al = aliases.getCollectionAlias(col);
        if (al != null) {
          List<String> aliasList = StrUtils.splitSmart(al, ",", true);
          newCollectionsList.addAll(aliasList);
        } else {
          newCollectionsList.add(col);
        }
      }
      if (newCollectionsList.size() > 0) {
        StringBuilder collectionString = new StringBuilder();
        Iterator<String> it = newCollectionsList.iterator();
        int sz = newCollectionsList.size();
        for (int i = 0; i < sz; i++) {
          collectionString.append(it.next());
          if (i < newCollectionsList.size() - 1) {
            collectionString.append(",");
          }
        }
        ModifiableSolrParams params = new ModifiableSolrParams(
            solrReq.getParams());
        params.set(COLLECTION_PROP, collectionString.toString());
        solrReq.setParams(params);
      }
    }
  }

  private void writeResponse(SolrQueryResponse solrRsp, QueryResponseWriter responseWriter, Method reqMethod)
      throws IOException {
    try {
      Object invalidStates = solrReq.getContext().get(CloudSolrClient.STATE_VERSION);
      //This is the last item added to the response and the client would expect it that way.
      //If that assumption is changed , it would fail. This is done to avoid an O(n) scan on
      // the response for each request
      if (invalidStates != null) solrRsp.add(CloudSolrClient.STATE_VERSION, invalidStates);
      // Now write it out
      final String ct = responseWriter.getContentType(solrReq, solrRsp);
      // don't call setContentType on null
      if (null != ct) response.setContentType(ct);

      if (solrRsp.getException() != null) {
        NamedList info = new SimpleOrderedMap();
        int code = ResponseUtils.getErrorInfo(solrRsp.getException(), info, log);
        solrRsp.add("error", info);
        response.setStatus(code);
      }

      if (Method.HEAD != reqMethod) {
        OutputStream out = new CloseShieldOutputStream(response.getOutputStream()); // Prevent close of container streams, see SOLR-8933
        QueryResponseWriterUtil.writeQueryResponse(out, responseWriter, solrReq, solrRsp, ct);
      }
      //else http HEAD request, nothing to write out, waited this long just to get ContentType
    } catch (EOFException e) {
      log.info("Unable to write response, client closed connection or we are shutting down", e);
    }
  }

  private Map<String, Integer> checkStateIsValid(String stateVer) {
    Map<String, Integer> result = null;
    String[] pairs;
    if (stateVer != null && !stateVer.isEmpty() && cores.isZooKeeperAware()) {
      // many have multiple collections separated by |
      pairs = StringUtils.split(stateVer, '|');
      for (String pair : pairs) {
        String[] pcs = StringUtils.split(pair, ':');
        if (pcs.length == 2 && !pcs[0].isEmpty() && !pcs[1].isEmpty()) {
          Integer status = cores.getZkController().getZkStateReader().compareStateVersions(pcs[0], Integer.parseInt(pcs[1]));
          if (status != null) {
            if (result == null) result = new HashMap<>();
            result.put(pcs[0], status);
          }
        }
      }
    }
    return result;
  }

  private SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    if (collection == null) {
      return null;
    }

    Set<String> liveNodes = clusterState.getLiveNodes();

    if (isPreferLeader) {
      List<Replica> leaderReplicas = collection.getLeaderReplicas(cores.getZkController().getNodeName());
      SolrCore core = randomlyGetSolrCore(liveNodes, leaderReplicas);
      if (core != null) return core;
    }

    List<Replica> replicas = collection.getReplicas(cores.getZkController().getNodeName());
    return randomlyGetSolrCore(liveNodes, replicas);
  }

  private SolrCore randomlyGetSolrCore(Set<String> liveNodes, List<Replica> replicas) {
    if (replicas != null) {
      RandomIterator<Replica> it = new RandomIterator<>(random, replicas);
      while (it.hasNext()) {
        Replica replica = it.next();
        if (liveNodes.contains(replica.getNodeName()) && replica.getState() == Replica.State.ACTIVE) {
          SolrCore core = checkProps(replica);
          if (core != null) return core;
        }
      }
    }
    return null;
  }

  private SolrCore checkProps(ZkNodeProps zkProps) {
    String corename;
    SolrCore core = null;
    if (cores.getZkController().getNodeName().equals(zkProps.getStr(NODE_NAME_PROP))) {
      corename = zkProps.getStr(CORE_NAME_PROP);
      core = cores.getCore(corename);
    }
    return core;
  }

  private void getSlicesForCollections(ClusterState clusterState,
                                       Collection<Slice> slices, boolean activeSlices) {
    if (activeSlices) {
      for (Map.Entry<String, DocCollection> entry : clusterState.getCollectionsMap().entrySet()) {
        final Collection<Slice> activeCollectionSlices = entry.getValue().getActiveSlices();
        if (activeCollectionSlices != null) {
          slices.addAll(activeCollectionSlices);
        }
      }
    } else {
      for (Map.Entry<String, DocCollection> entry : clusterState.getCollectionsMap().entrySet()) {
        final Collection<Slice> collectionSlices = entry.getValue().getSlices();
        if (collectionSlices != null) {
          slices.addAll(collectionSlices);
        }
      }
    }
  }

  private String getRemotCoreUrl(String collectionName, String origCorename) {
    ClusterState clusterState = cores.getZkController().getClusterState();
    Collection<Slice> slices = clusterState.getActiveSlices(collectionName);
    boolean byCoreName = false;

    if (slices == null) {
      slices = new ArrayList<>();
      // look by core name
      byCoreName = true;
      getSlicesForCollections(clusterState, slices, true);
      if (slices.isEmpty()) {
        getSlicesForCollections(clusterState, slices, false);
      }
    }

    if (slices.isEmpty()) {
      return null;
    }

    if (collectionsList == null)
      collectionsList = new ArrayList<>();

    collectionsList.add(collectionName);
    String coreUrl = getCoreUrl(collectionName, origCorename, clusterState,
        slices, byCoreName, true);

    if (coreUrl == null) {
      coreUrl = getCoreUrl(collectionName, origCorename, clusterState,
          slices, byCoreName, false);
    }

    return coreUrl;
  }

  private String getCoreUrl(String collectionName,
                            String origCorename, ClusterState clusterState, Collection<Slice> slices,
                            boolean byCoreName, boolean activeReplicas) {
    String coreUrl;
    Set<String> liveNodes = clusterState.getLiveNodes();
    List<Slice> randomizedSlices = new ArrayList<>(slices.size());
    randomizedSlices.addAll(slices);
    Collections.shuffle(randomizedSlices, random);

    for (Slice slice : randomizedSlices) {
      List<Replica> randomizedReplicas = new ArrayList<>();
      randomizedReplicas.addAll(slice.getReplicas());
      Collections.shuffle(randomizedReplicas, random);

      for (Replica replica : randomizedReplicas) {
        if (!activeReplicas || (liveNodes.contains(replica.getNodeName())
            && replica.getState() == Replica.State.ACTIVE)) {

          if (byCoreName && !collectionName.equals(replica.getStr(CORE_NAME_PROP))) {
            // if it's by core name, make sure they match
            continue;
          }
          if (replica.getStr(BASE_URL_PROP).equals(cores.getZkController().getBaseUrl())) {
            // don't count a local core
            continue;
          }

          if (origCorename != null) {
            coreUrl = replica.getStr(BASE_URL_PROP) + "/" + origCorename;
          } else {
            coreUrl = replica.getCoreUrl();
            if (coreUrl.endsWith("/")) {
              coreUrl = coreUrl.substring(0, coreUrl.length() - 1);
            }
          }

          return coreUrl;
        }
      }
    }
    return null;
  }

  private AuthorizationContext getAuthCtx() {

    String resource = getPath();

    SolrParams params = getQueryParams();
    final ArrayList<CollectionRequest> collectionRequests = new ArrayList<>();
    if (getCollectionsList() != null) {
      for (String collection : getCollectionsList()) {
        collectionRequests.add(new CollectionRequest(collection));
      }
    }

    // Extract collection name from the params in case of a Collection Admin request
    if (getPath().equals("/admin/collections")) {
      if (CREATE.isEqual(params.get("action"))||
          RELOAD.isEqual(params.get("action"))||
          DELETE.isEqual(params.get("action")))
        collectionRequests.add(new CollectionRequest(params.get("name")));
      else if (params.get(COLLECTION_PROP) != null)
        collectionRequests.add(new CollectionRequest(params.get(COLLECTION_PROP)));
    }
    
    // Handle the case when it's a /select request and collections are specified as a param
    if(resource.equals("/select") && params.get("collection") != null) {
      collectionRequests.clear();
      for(String collection:params.get("collection").split(",")) {
        collectionRequests.add(new CollectionRequest(collection));
      }
    }
    
    // Populate the request type if the request is select or update
    if(requestType == RequestType.UNKNOWN) {
      if(resource.startsWith("/select") || resource.startsWith("/get"))
        requestType = RequestType.READ;
      if(resource.startsWith("/update"))
        requestType = RequestType.WRITE;
    }

    // There's no collection explicitly mentioned, let's try and extract it from the core if one exists for
    // the purpose of processing this request.
    if (getCore() != null && (getCollectionsList() == null || getCollectionsList().size() == 0)) {
      collectionRequests.add(new CollectionRequest(getCore().getCoreDescriptor().getCollectionName()));
    }

    if (getQueryParams().get(COLLECTION_PROP) != null)
      collectionRequests.add(new CollectionRequest(getQueryParams().get(COLLECTION_PROP)));

    return new AuthorizationContext() {
      @Override
      public SolrParams getParams() {
        return solrReq.getParams();
      }

      @Override
      public Principal getUserPrincipal() {
        return getReq().getUserPrincipal();
      }

      @Override
      public String getHttpHeader(String s) {
        return getReq().getHeader(s);
      }
      
      @Override
      public Enumeration getHeaderNames() {
        return getReq().getHeaderNames();
      }

      @Override
      public List<CollectionRequest> getCollectionRequests() {
        return collectionRequests;
      }

      @Override
      public RequestType getRequestType() {
        return requestType;
      }
      
      public String getResource() {
        return path;
      }

      @Override
      public String getHttpMethod() {
        return getReq().getMethod();
      }

      @Override
      public Object getHandler() {
        return handler;
      }

      @Override
      public String toString() {
        StringBuilder response = new StringBuilder("userPrincipal: [").append(getUserPrincipal()).append("]")
            .append(" type: [").append(requestType.toString()).append("], collections: [");
        for (CollectionRequest collectionRequest : collectionRequests) {
          response.append(collectionRequest.collectionName).append(", ");
        }
        if(collectionRequests.size() > 0)
          response.delete(response.length() - 1, response.length());
        
        response.append("], Path: [").append(resource).append("]");
        response.append(" path : ").append(path).append(" params :").append(solrReq.getParams());
        return response.toString();
      }

      @Override
      public String getRemoteAddr() {
        return getReq().getRemoteAddr();
      }

      @Override
      public String getRemoteHost() {
        return getReq().getRemoteHost();
      }
    };

  }

  static final String CONNECTION_HEADER = "Connection";
  static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
  static final String CONTENT_LENGTH_HEADER = "Content-Length";

  /**
   * A faster method for randomly picking items when you do not need to
   * consume all items.
   */
  private static class RandomIterator<E> implements Iterator<E> {
    private Random rand;
    private ArrayList<E> elements;
    private int size;

    public RandomIterator(Random rand, Collection<E> elements) {
      this.rand = rand;
      this.elements = new ArrayList<>(elements);
      this.size = elements.size();
    }

    @Override
    public boolean hasNext() {
      return size > 0;
    }

    @Override
    public E next() {
      int idx = rand.nextInt(size);
      E e1 = elements.get(idx);
      E e2 = elements.get(size-1);
      elements.set(idx,e2);
      size--;
      return e1;
    }
  }
}
