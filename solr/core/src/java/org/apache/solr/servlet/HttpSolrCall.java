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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.opentracing.Span;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.annotation.SolrThreadSafe;
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
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuditEvent.EventType;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.servlet.SolrDispatchFilter.Action;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.SYSTEM_COLL;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
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
@SolrThreadSafe
public class HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ORIGINAL_USER_PRINCIPAL_HEADER = "originalUserPrincipal";

  public static final String INTERNAL_REQUEST_COUNT = "_forwardedCount";

  public static final Random random;
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
  private boolean mustClearSolrRequestInfo = false;
  protected SolrRequestHandler handler = null;
  protected SolrParams queryParams;
  protected String path;
  protected Action action;
  protected String coreUrl;
  protected SolrConfig config;
  protected Map<String, Integer> invalidStates;

  //The states of client that is invalid in this request
  protected String origCorename; // What's in the URL path; might reference a collection/alias or a Solr core name
  protected List<String> collectionsList; // The list of SolrCloud collections if in SolrCloud (usually 1)

  public RequestType getRequestType() {
    return requestType;
  }

  protected RequestType requestType;

  public HttpSolrCall(SolrDispatchFilter solrDispatchFilter, CoreContainer cores,
               HttpServletRequest request, HttpServletResponse response, boolean retry) {
    this.solrDispatchFilter = solrDispatchFilter;
    this.cores = cores;
    this.req = request;
    this.response = response;
    this.retry = retry;
    this.requestType = RequestType.UNKNOWN;
    req.setAttribute(HttpSolrCall.class.getName(), this);
    // set a request timer which can be reused by requests if needed
    req.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimerTree());
    // put the core container in request attribute
    req.setAttribute("org.apache.solr.CoreContainer", cores);
    path = ServletUtils.getPathAfterContext(req);
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

  /** The collection(s) referenced in this request. Populated in {@link #init()}. Not null. */
  public List<String> getCollectionsList() {
    return collectionsList != null ? collectionsList : Collections.emptyList();
  }

  protected void init() throws Exception {
    // check for management path
    String alternate = cores.getManagementPath();
    if (alternate != null && path.startsWith(alternate)) {
      path = path.substring(0, alternate.length());
    }

    queryParams = SolrRequestParsers.parseQueryString(req.getQueryString());

    // unused feature ?
    int idx = path.indexOf(':');
    if (idx > 0) {
      // save the portion after the ':' for a 'handler' path parameter
      path = path.substring(0, idx);
    }

    // Check for container handlers
    handler = cores.getRequestHandler(path);
    if (handler != null) {
      solrReq = SolrRequestParsers.DEFAULT.parse(null, path, req);
      solrReq.getContext().put(CoreContainer.class.getName(), cores);
      requestType = RequestType.ADMIN;
      action = ADMIN;
      return;
    }

    // Parse a core or collection name from the path and attempt to see if it's a core name
    idx = path.indexOf("/", 1);
    if (idx > 1) {
      origCorename = path.substring(1, idx);

      // Try to resolve a Solr core name
      core = cores.getCore(origCorename);
      if (core != null) {
        path = path.substring(idx);
      } else {
        if (cores.isCoreLoading(origCorename)) { // extra mem barriers, so don't look at this before trying to get core
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCore is loading");
        }
        // the core may have just finished loading
        core = cores.getCore(origCorename);
        if (core != null) {
          path = path.substring(idx);
        } else {
          if (!cores.isZooKeeperAware()) {
            core = cores.getCore("");
          }
        }
      }
    }

    if (cores.isZooKeeperAware()) {
      // init collectionList (usually one name but not when there are aliases)
      String def = core != null ? core.getCoreDescriptor().getCollectionName() : origCorename;
      collectionsList = resolveCollectionListOrAlias(queryParams.get(COLLECTION_PROP, def)); // &collection= takes precedence

      if (core == null) {
        // lookup core from collection, or route away if need to
        String collectionName = collectionsList.isEmpty() ? null : collectionsList.get(0); // route to 1st
        //TODO try the other collections if can't find a local replica of the first?   (and do to V2HttpSolrCall)

        boolean isPreferLeader = (path.endsWith("/update") || path.contains("/update/"));

        core = getCoreByCollection(collectionName, isPreferLeader); // find a local replica/core for the collection
        if (core != null) {
          if (idx > 0) {
            path = path.substring(idx);
          }
        } else {
          // if we couldn't find it locally, look on other nodes
          if (idx > 0) {
            extractRemotePath(collectionName, origCorename);
            if (action == REMOTEQUERY) {
              path = path.substring(idx);
              return;
            }
          }
          //core is not available locally or remotely
          autoCreateSystemColl(collectionName);
          if (action != null) return;
        }
      }
    }

    // With a valid core...
    if (core != null) {
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

        invalidStates = checkStateVersionsAreValid(solrReq.getParams().get(CloudSolrClient.STATE_VERSION));

        addCollectionParamIfNeeded(getCollectionsList());

        action = PROCESS;
        return; // we are done with a valid handler
      }
    }
    log.debug("no handler or core retrieved for {}, follow through...", path);

    action = PASSTHROUGH;
  }

  protected void autoCreateSystemColl(String corename) throws Exception {
    if (core == null &&
        SYSTEM_COLL.equals(corename) &&
        "POST".equals(req.getMethod()) &&
        !cores.getZkController().getClusterState().hasCollection(SYSTEM_COLL)) {
      log.info("Going to auto-create {} collection", SYSTEM_COLL);
      SolrQueryResponse rsp = new SolrQueryResponse();
      String repFactor = String.valueOf(Math.min(3, cores.getZkController().getClusterState().getLiveNodes().size()));
      cores.getCollectionsHandler().handleRequestBody(new LocalSolrQueryRequest(null,
          new ModifiableSolrParams()
              .add(ACTION, CREATE.toString())
              .add( NAME, SYSTEM_COLL)
              .add(REPLICATION_FACTOR, repFactor)), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not auto-create " + SYSTEM_COLL + " collection: "+ Utils.toJSONString(rsp.getValues()));
      }
      TimeOut timeOut = new TimeOut(3, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      for (; ; ) {
        if (cores.getZkController().getClusterState().getCollectionOrNull(SYSTEM_COLL) != null) {
          break;
        } else {
          if (timeOut.hasTimedOut()) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Could not find " + SYSTEM_COLL + " collection even after 3 seconds");
          }
          timeOut.sleep(50);
        }
      }

      action = RETRY;
    }
  }

  /**
   * Resolves the parameter as a potential comma delimited list of collections, and resolves aliases too.
   * One level of aliases pointing to another alias is supported.
   * De-duplicates and retains the order.
   * {@link #getCollectionsList()}
   */
  protected List<String> resolveCollectionListOrAlias(String collectionStr) {
    if (collectionStr == null || collectionStr.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> result = null;
    LinkedHashSet<String> uniqueList = null;
    Aliases aliases = cores.getZkController().getZkStateReader().getAliases();
    List<String> inputCollections = StrUtils.splitSmart(collectionStr, ",", true);
    if (inputCollections.size() > 1) {
      uniqueList = new LinkedHashSet<>();
    }
    for (String inputCollection : inputCollections) {
      List<String> resolvedCollections = aliases.resolveAliases(inputCollection);
      if (uniqueList != null) {
        uniqueList.addAll(resolvedCollections);
      } else {
        result = resolvedCollections;
      }
    }
    if (uniqueList != null) {
      return new ArrayList<>(uniqueList);
    } else {
      return result;
    }
  }

  /**
   * Extract handler from the URL path if not set.
   */
  protected void extractHandlerFromURLPath(SolrRequestParsers parser) throws Exception {
    if (handler == null && path.length() > 1) { // don't match "" or "/" as valid path
      handler = core.getRequestHandler(path);
      // no handler yet but <requestDispatcher> allows us to handle /select with a 'qt' param
      if (handler == null && parser.isHandleSelect()) {
        if ("/select".equals(path) || "/select/".equals(path)) {
          solrReq = parser.parse(core, path, req);
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

  protected void extractRemotePath(String collectionName, String origCorename) throws UnsupportedEncodingException, KeeperException, InterruptedException, SolrException {
    assert core == null;
    coreUrl = getRemoteCoreUrl(collectionName, origCorename);
    // don't proxy for internal update requests
    invalidStates = checkStateVersionsAreValid(queryParams.get(CloudSolrClient.STATE_VERSION));
    if (coreUrl != null
        && queryParams.get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) == null) {
      if (invalidStates != null) {
        //it does not make sense to send the request to a remote node
        throw new SolrException(SolrException.ErrorCode.INVALID_STATE, new String(Utils.toJSON(invalidStates), org.apache.lucene.util.IOUtils.UTF_8));
      }
      action = REMOTEQUERY;
    } else {
      if (!retry) {
        // we couldn't find a core to work with, try reloading aliases & this collection
        cores.getZkController().getZkStateReader().aliasesManager.update();
        cores.getZkController().zkStateReader.forceUpdateCollection(collectionName);
        action = RETRY;
      }
    }
  }

  Action authorize() throws IOException {
    AuthorizationContext context = getAuthCtx();
    log.debug("AuthorizationContext : {}", context);
    AuthorizationResponse authResponse = cores.getAuthorizationPlugin().authorize(context);
    int statusCode = authResponse.statusCode;

    if (statusCode == AuthorizationResponse.PROMPT.statusCode) {
      @SuppressWarnings({"unchecked"})
      Map<String, String> headers = (Map) getReq().getAttribute(AuthenticationPlugin.class.getName());
      if (headers != null) {
        for (Map.Entry<String, String> e : headers.entrySet()) response.setHeader(e.getKey(), e.getValue());
      }
      if (log.isDebugEnabled()) {
        log.debug("USER_REQUIRED {} {}", req.getHeader("Authorization"), req.getUserPrincipal());
      }
      sendError(statusCode,
          "Authentication failed, Response code: " + statusCode);
      if (shouldAudit(EventType.REJECTED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.REJECTED, req, context));
      }
      return RETURN;
    }
    if (statusCode == AuthorizationResponse.FORBIDDEN.statusCode) {
      if (log.isDebugEnabled()) {
        log.debug("UNAUTHORIZED auth header {} context : {}, msg: {}", req.getHeader("Authorization"), context, authResponse.getMessage()); // nowarn
      }
      sendError(statusCode,
          "Unauthorized request, Response code: " + statusCode);
      if (shouldAudit(EventType.UNAUTHORIZED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.UNAUTHORIZED, req, context));
      }
      return RETURN;
    }
    if (!(statusCode == HttpStatus.SC_ACCEPTED) && !(statusCode == HttpStatus.SC_OK)) {
      log.warn("ERROR {} during authentication: {}", statusCode, authResponse.getMessage()); // nowarn
      sendError(statusCode,
          "ERROR during authorization, Response code: " + statusCode);
      if (shouldAudit(EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, req, context));
      }
      return RETURN;
    }
    if (shouldAudit(EventType.AUTHORIZED)) {
      cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.AUTHORIZED, req, context));
    }
    return null;
  }

  /**
   * This method processes the request.
   */
  public Action call() throws IOException {
    MDCLoggingContext.reset();
    Span activeSpan = GlobalTracer.getTracer().activeSpan();
    if (activeSpan != null) {
      MDCLoggingContext.setTracerId(activeSpan.context().toTraceId());
    }

    MDCLoggingContext.setNode(cores);

    if (cores == null) {
      sendError(503, "Server is shutting down or failed to initialize");
      return RETURN;
    }

    if (solrDispatchFilter.abortErrorMessage != null) {
      sendError(500, solrDispatchFilter.abortErrorMessage);
      if (shouldAudit(EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, getReq()));
      }
      return RETURN;
    }

    try {
      init();

      // Perform authorization here, if:
      //    (a) Authorization is enabled, and
      //    (b) The requested resource is not a known static file
      //    (c) And this request should be handled by this node (see NOTE below)
      // NOTE: If the query is to be handled by another node, then let that node do the authorization.
      // In case of authentication using BasicAuthPlugin, for example, the internode request
      // is secured using PKI authentication and the internode request here will contain the
      // original user principal as a payload/header, using which the receiving node should be
      // able to perform the authorization.
      if (cores.getAuthorizationPlugin() != null && shouldAuthorize()
          && !(action == REMOTEQUERY || action == FORWARD)) {
        Action authorizationAction = authorize();
        if (authorizationAction != null) return authorizationAction;
      }

      HttpServletResponse resp = response;
      switch (action) {
        case ADMIN:
          handleAdminRequest();
          return RETURN;
        case REMOTEQUERY:
          SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, new SolrQueryResponse(), action));
          mustClearSolrRequestInfo = true;
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
            SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp, action));
            mustClearSolrRequestInfo = true;
            execute(solrRsp);
            if (shouldAudit()) {
              EventType eventType = solrRsp.getException() == null ? EventType.COMPLETED : EventType.ERROR;
              if (shouldAudit(eventType)) {
                cores.getAuditLoggerPlugin().doAudit(
                        new AuditEvent(eventType, req, getAuthCtx(), solrReq.getRequestTimer().getTime(), solrRsp.getException()));
              }
            }
            HttpCacheHeaderUtil.checkHttpCachingVeto(solrRsp, resp, reqMethod);
            Iterator<Map.Entry<String, String>> headers = solrRsp.httpHeaders();
            while (headers.hasNext()) {
              Map.Entry<String, String> entry = headers.next();
              resp.addHeader(entry.getKey(), entry.getValue());
            }
            QueryResponseWriter responseWriter = getResponseWriter();
            if (invalidStates != null) solrReq.getContext().put(CloudSolrClient.STATE_VERSION, invalidStates);
            writeResponse(solrRsp, responseWriter, reqMethod);
          }
          return RETURN;
        default: return action;
      }
    } catch (Throwable ex) {
      if (shouldAudit(EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, ex, req));
      }
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
    }

  }

  private boolean shouldAudit() {
    return cores.getAuditLoggerPlugin() != null;
  }

  private boolean shouldAudit(AuditEvent.EventType eventType) {
    return shouldAudit() && cores.getAuditLoggerPlugin().shouldLog(eventType);
  }

  private boolean shouldAuthorize() {
    if(PublicKeyHandler.PATH.equals(path)) return false;
    //admin/info/key is the path where public key is exposed . it is always unsecured
    if ("/".equals(path) || "/solr/".equals(path)) return false; // Static Admin UI files must always be served
    if (cores.getPkiAuthenticationSecurityBuilder() != null && req.getUserPrincipal() != null) {
      boolean b = cores.getPkiAuthenticationSecurityBuilder().needsAuthorization(req);
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
        if (mustClearSolrRequestInfo) {
          SolrRequestInfo.clearRequestInfo();
        }
      }
      AuthenticationPlugin authcPlugin = cores.getAuthenticationPlugin();
      if (authcPlugin != null) authcPlugin.closeRequest();
    }
  }

  //TODO using Http2Client
  private void remoteQuery(String coreUrl, HttpServletResponse resp) throws IOException {
    HttpRequestBase method;
    HttpEntity httpEntity = null;

    ModifiableSolrParams updatedQueryParams = new ModifiableSolrParams(queryParams);
    int forwardCount = queryParams.getInt(INTERNAL_REQUEST_COUNT, 0) + 1;
    updatedQueryParams.set(INTERNAL_REQUEST_COUNT, forwardCount);
    String queryStr = updatedQueryParams.toQueryString();

    try {
      String urlstr = coreUrl + queryStr;

      boolean isPostOrPutRequest = "POST".equals(req.getMethod()) || "PUT".equals(req.getMethod());
      if ("GET".equals(req.getMethod())) {
        method = new HttpGet(urlstr);
      } else if ("HEAD".equals(req.getMethod())) {
        method = new HttpHead(urlstr);
      } else if (isPostOrPutRequest) {
        HttpEntityEnclosingRequestBase entityRequest =
            "POST".equals(req.getMethod()) ? new HttpPost(urlstr) : new HttpPut(urlstr);
        InputStream in = req.getInputStream();
        HttpEntity entity = new InputStreamEntity(in, req.getContentLength());
        entityRequest.setEntity(entity);
        method = entityRequest;
      } else if ("DELETE".equals(req.getMethod())) {
        method = new HttpDelete(urlstr);
      } else if ("OPTIONS".equals(req.getMethod())) {
        method = new HttpOptions(urlstr);
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
          
          // NOTE: explicitly using 'setHeader' instead of 'addHeader' so that
          // the remote nodes values for any response headers will overide any that
          // may have already been set locally (ex: by the local jetty's RewriteHandler config)
          resp.setHeader(header.getName(), header.getValue());
        }
      }

      if (httpEntity != null) {
        if (httpEntity.getContentEncoding() != null)
          resp.setHeader(httpEntity.getContentEncoding().getName(), httpEntity.getContentEncoding().getValue());
        if (httpEntity.getContentType() != null) resp.setContentType(httpEntity.getContentType().getValue());

        InputStream is = httpEntity.getContent();
        OutputStream os = resp.getOutputStream();

        IOUtils.copyLarge(is, os);
      }

    } catch (IOException e) {
      sendError(new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error trying to proxy request for url: " + coreUrl + " with _forwardCount: " + forwardCount, e));
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
          solrParams = new MapSolrParams(Collections.emptyMap());
        }
        solrReq = new SolrQueryRequestBase(core, solrParams) {
        };
      }
      QueryResponseWriter writer = getResponseWriter();
      writeResponse(solrResp, writer, Method.GET);
    } catch (Exception e) { // This error really does not matter
      exp = e;
    } finally {
      try {
        if (exp != null) {
          @SuppressWarnings({"rawtypes"})
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
    handleAdmin(solrResp);
    SolrCore.postDecorateResponse(handler, solrReq, solrResp);
    if (solrResp.getToLog().size() > 0) {
      if (log.isInfoEnabled()) { // has to come second and in it's own if to keep ./gradlew check happy.
        log.info(handler != null ? MarkerFactory.getMarker(handler.getClass().getName()) : MarkerFactory.getMarker(HttpSolrCall.class.getName()), solrResp.getToLogAsString("[admin]"));
      }
    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = getResponseWriter();
    writeResponse(solrResp, respWriter, Method.getMethod(req.getMethod()));
    if (shouldAudit()) {
      EventType eventType = solrResp.getException() == null ? EventType.COMPLETED : EventType.ERROR;
      if (shouldAudit(eventType)) {
        cores.getAuditLoggerPlugin().doAudit(
            new AuditEvent(eventType, req, getAuthCtx(), solrReq.getRequestTimer().getTime(), solrResp.getException()));
      }
    }
  }

  /**
   * Returns {@link QueryResponseWriter} to be used.
   * When {@link CommonParams#WT} not specified in the request or specified value doesn't have
   * corresponding {@link QueryResponseWriter} then, returns the default query response writer
   * Note: This method must not return null
   */
  protected QueryResponseWriter getResponseWriter() {
    String wt = solrReq.getParams().get(CommonParams.WT);
    if (core != null) {
      return core.getQueryResponseWriter(wt);
    } else {
      return SolrCore.DEFAULT_RESPONSE_WRITERS.getOrDefault(wt,
          SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard"));
    }
  }

  protected void handleAdmin(SolrQueryResponse solrResp) {
    handler.handleRequest(solrReq, solrResp);
  }

  /**
   * Sets the "collection" parameter on the request to the list of alias-resolved collections for this request.
   * It can be avoided sometimes.
   * Note: {@link org.apache.solr.handler.component.HttpShardHandler} processes this param.
   * @see #getCollectionsList()
   */
  protected void addCollectionParamIfNeeded(List<String> collections) {
    if (collections.isEmpty()) {
      return;
    }
    assert cores.isZooKeeperAware();
    String collectionParam = queryParams.get(COLLECTION_PROP);
    // if there is no existing collection param and the core we go to is for the expected collection,
    //   then we needn't add a collection param
    if (collectionParam == null && // if collection param already exists, we may need to over-write it
        core != null && collections.equals(Collections.singletonList(core.getCoreDescriptor().getCollectionName()))) {
      return;
    }
    String newCollectionParam = StrUtils.join(collections, ',');
    if (newCollectionParam.equals(collectionParam)) {
      return;
    }
    // TODO add a SolrRequest.getModifiableParams ?
    ModifiableSolrParams params = new ModifiableSolrParams(solrReq.getParams());
    params.set(COLLECTION_PROP, newCollectionParam);
    solrReq.setParams(params);
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
        @SuppressWarnings({"rawtypes"})
        NamedList info = new SimpleOrderedMap();
        int code = ResponseUtils.getErrorInfo(solrRsp.getException(), info, log);
        solrRsp.add("error", info);
        response.setStatus(code);
      }

      if (Method.HEAD != reqMethod) {
        OutputStream out = response.getOutputStream();
        QueryResponseWriterUtil.writeQueryResponse(out, responseWriter, solrReq, solrRsp, ct);
      }
      //else http HEAD request, nothing to write out, waited this long just to get ContentType
    } catch (EOFException e) {
      log.info("Unable to write response, client closed connection or we are shutting down", e);
    }
  }

  /** Returns null if the state ({@link CloudSolrClient#STATE_VERSION}) is good; otherwise returns state problems. */
  private Map<String, Integer> checkStateVersionsAreValid(String stateVer) {
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

  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection collection = clusterState.getCollectionOrNull(collectionName, true);
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
        final Slice[] activeCollectionSlices = entry.getValue().getActiveSlicesArr();
        if (activeCollectionSlices != null) {
          Collections.addAll(slices, activeCollectionSlices);
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

  protected String getRemoteCoreUrl(String collectionName, String origCorename) throws SolrException {
    ClusterState clusterState = cores.getZkController().getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
    Slice[] slices = (docCollection != null) ? docCollection.getActiveSlicesArr() : null;
    List<Slice> activeSlices = new ArrayList<>();
    boolean byCoreName = false;

    int totalReplicas = 0;

    if (slices == null) {
      byCoreName = true;
      activeSlices = new ArrayList<>();
      getSlicesForCollections(clusterState, activeSlices, true);
      if (activeSlices.isEmpty()) {
        getSlicesForCollections(clusterState, activeSlices, false);
      }
    } else {
      Collections.addAll(activeSlices, slices);
    }

    for (Slice s: activeSlices) {
      totalReplicas += s.getReplicas().size();
    }
    if (activeSlices.isEmpty()) {
      return null;
    }

    // XXX (ab) most likely this is not needed? it seems all code paths
    // XXX already make sure the collectionName is on the list
    if (!collectionsList.contains(collectionName)) {
      collectionsList = new ArrayList<>(collectionsList);
      collectionsList.add(collectionName);
    }

    // Avoid getting into a recursive loop of requests being forwarded by
    // stopping forwarding and erroring out after (totalReplicas) forwards
    if (queryParams.getInt(INTERNAL_REQUEST_COUNT, 0) > totalReplicas){
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE,
          "No active replicas found for collection: " + collectionName);
    }

    String coreUrl = getCoreUrl(collectionName, origCorename, clusterState,
        activeSlices, byCoreName, true);

    if (coreUrl == null) {
      coreUrl = getCoreUrl(collectionName, origCorename, clusterState,
          activeSlices, byCoreName, false);
    }

    return coreUrl;
  }

  private String getCoreUrl(String collectionName,
                            String origCorename, ClusterState clusterState, List<Slice> slices,
                            boolean byCoreName, boolean activeReplicas) {
    String coreUrl;
    Set<String> liveNodes = clusterState.getLiveNodes();
    Collections.shuffle(slices, random);

    for (Slice slice : slices) {
      List<Replica> randomizedReplicas = new ArrayList<>(slice.getReplicas());
      Collections.shuffle(randomizedReplicas, random);

      for (Replica replica : randomizedReplicas) {
        if (!activeReplicas || (liveNodes.contains(replica.getNodeName())
            && replica.getState() == Replica.State.ACTIVE)) {

          if (byCoreName && !origCorename.equals(replica.getStr(CORE_NAME_PROP))) {
            // if it's by core name, make sure they match
            continue;
          }
          if (replica.getBaseUrl().equals(cores.getZkController().getBaseUrl())) {
            // don't count a local core
            continue;
          }

          if (origCorename != null) {
            coreUrl = replica.getBaseUrl() + "/" + origCorename;
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

  protected Object _getHandler(){
    return handler;
  }

  private AuthorizationContext getAuthCtx() {

    String resource = getPath();

    SolrParams params = getQueryParams();
    final ArrayList<CollectionRequest> collectionRequests = new ArrayList<>();
    for (String collection : getCollectionsList()) {
      collectionRequests.add(new CollectionRequest(collection));
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

    // Populate the request type if the request is select or update
    if(requestType == RequestType.UNKNOWN) {
      if(resource.startsWith("/select") || resource.startsWith("/get"))
        requestType = RequestType.READ;
      if(resource.startsWith("/update"))
        requestType = RequestType.WRITE;
    }

    return new AuthorizationContext() {
      @Override
      public SolrParams getParams() {
        return null == solrReq ? null : solrReq.getParams();
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
      public Enumeration<String> getHeaderNames() {
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
        return _getHandler();
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
        response.append(" path : ").append(path).append(" params :").append(getParams());
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
  List<CommandOperation> parsedCommands;

  public List<CommandOperation> getCommands(boolean validateInput) {
    if (parsedCommands == null) {
      Iterable<ContentStream> contentStreams = solrReq.getContentStreams();
      if (contentStreams == null) parsedCommands = Collections.emptyList();
      else {
        parsedCommands = ApiBag.getCommandOperations(contentStreams.iterator().next(), getValidators(), validateInput);
      }
    }
    return CommandOperation.clone(parsedCommands);
  }
  protected ValidatingJsonMap getSpec() {
    return null;
  }

  @SuppressWarnings({"unchecked"})
  protected Map<String, JsonSchemaValidator> getValidators(){
    return Collections.EMPTY_MAP;
  }

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
