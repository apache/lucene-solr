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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter extends BaseSolrFilter {
  private static final String CONNECTION_HEADER = "Connection";
  private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
  private static final String CONTENT_LENGTH_HEADER = "Content-Length";

  static final Logger log = LoggerFactory.getLogger(SolrDispatchFilter.class);

  protected volatile CoreContainer cores;

  protected String pathPrefix = null; // strip this from the beginning of a path
  protected String abortErrorMessage = null;
  protected final HttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams());
  
  public SolrDispatchFilter() {
  }
  
  @Override
  public void init(FilterConfig config) throws ServletException
  {
    log.info("SolrDispatchFilter.init()");

    try {
      // web.xml configuration
      this.pathPrefix = config.getInitParameter( "path-prefix" );

      this.cores = createCoreContainer();
      log.info("user.dir=" + System.getProperty("user.dir"));
    }
    catch( Throwable t ) {
      // catch this so our filter still works
      log.error( "Could not start Solr. Check solr/home property and the logs");
      SolrCore.log( t );
      if (t instanceof Error) {
        throw (Error) t;
      }
    }

    log.info("SolrDispatchFilter.init() done");
  }

  private ConfigSolr loadConfigSolr(SolrResourceLoader loader) {

    String solrxmlLocation = System.getProperty("solr.solrxml.location", "solrhome");

    if (solrxmlLocation == null || "solrhome".equalsIgnoreCase(solrxmlLocation))
      return ConfigSolr.fromSolrHome(loader, loader.getInstanceDir());

    if ("zookeeper".equalsIgnoreCase(solrxmlLocation)) {
      String zkHost = System.getProperty("zkHost");
      log.info("Trying to read solr.xml from " + zkHost);
      if (StringUtils.isEmpty(zkHost))
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not load solr.xml from zookeeper: zkHost system property not set");
      SolrZkClient zkClient = new SolrZkClient(zkHost, 30000);
      try {
        if (!zkClient.exists("/solr.xml", true))
          throw new SolrException(ErrorCode.SERVER_ERROR, "Could not load solr.xml from zookeeper: node not found");
        byte[] data = zkClient.getData("/solr.xml", null, null, true);
        return ConfigSolr.fromInputStream(loader, new ByteArrayInputStream(data));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not load solr.xml from zookeeper", e);
      } finally {
        zkClient.close();
      }
    }

    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Bad solr.solrxml.location set: " + solrxmlLocation + " - should be 'solrhome' or 'zookeeper'");
  }

  /**
   * Override this to change CoreContainer initialization
   * @return a CoreContainer to hold this server's cores
   */
  protected CoreContainer createCoreContainer() {
    SolrResourceLoader loader = new SolrResourceLoader(SolrResourceLoader.locateSolrHome());
    ConfigSolr config = loadConfigSolr(loader);
    CoreContainer cores = new CoreContainer(loader, config);
    cores.load();
    return cores;
  }
  
  public CoreContainer getCores() {
    return cores;
  }
  
  @Override
  public void destroy() {
    if (cores != null) {
      cores.shutdown();
      cores = null;
    }    
  }
  
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    doFilter(request, response, chain, false);
  }
  
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain, boolean retry) throws IOException, ServletException {
    if( abortErrorMessage != null ) {
      ((HttpServletResponse)response).sendError( 500, abortErrorMessage );
      return;
    }
    
    if (this.cores == null) {
      ((HttpServletResponse)response).sendError( 503, "Server is shutting down or failed to initialize" );
      return;
    }
    CoreContainer cores = this.cores;
    SolrCore core = null;
    SolrQueryRequest solrReq = null;
    Aliases aliases = null;
    
    if( request instanceof HttpServletRequest) {
      HttpServletRequest req = (HttpServletRequest)request;
      HttpServletResponse resp = (HttpServletResponse)response;
      SolrRequestHandler handler = null;
      String corename = "";
      String origCorename = null;
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
          solrReq =  SolrRequestParsers.DEFAULT.parse(null,path, req);
          handleAdminRequest(req, response, handler, solrReq);
          return;
        }
        boolean usingAliases = false;
        List<String> collectionsList = null;
        // Check for the core admin collections url
        handler = cores.getRequestHandler(path);
        if( handler!= null ) {
          solrReq =  SolrRequestParsers.DEFAULT.parse(null,path, req);
          handleAdminRequest(req, response, handler, solrReq);
          return;
        } else {
          //otherwise, we should find a core from the path
          idx = path.indexOf( "/", 1 );
          if( idx > 1 ) {
            // try to get the corename as a request parameter first
            corename = path.substring( 1, idx );
            
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
          }
          
          // if we couldn't find it locally, look on other nodes
          if (core == null && idx > 0) {
            String coreUrl = getRemotCoreUrl(cores, corename, origCorename);
            // don't proxy for internal update requests
            SolrParams queryParams = SolrRequestParsers.parseQueryString(req.getQueryString());
            checkStateIsValid(cores, queryParams.get(CloudSolrServer.STATE_VERSION));
            if (coreUrl != null
                && queryParams
                    .get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) == null) {
              path = path.substring(idx);
              remoteQuery(coreUrl + path, req, solrReq, resp);
              return;
            } else {
              if (!retry) {
                // we couldn't find a core to work with, try reloading aliases
                // TODO: it would be nice if admin ui elements skipped this...
                ZkStateReader reader = cores.getZkController()
                    .getZkStateReader();
                reader.updateAliases();
                doFilter(request, response, chain, true);
                return;
              }
            }
          }
          
          // try the default core
          if (core == null) {
            core = cores.getCore("");
          }
        }

        // With a valid core...
        if( core != null ) {
          final SolrConfig config = core.getSolrConfig();
          // get or create/cache the parser for the core
          SolrRequestParsers parser = config.getRequestParsers();

          // Handle /schema/* and /config/* paths via Restlet
          if( path.equals("/schema") || path.startsWith("/schema/")
              || path.equals("/config") || path.startsWith("/config/")) {
            solrReq = parser.parse(core, path, req);
            SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, new SolrQueryResponse()));
            if( path.equals(req.getServletPath()) ) {
              // avoid endless loop - pass through to Restlet via webapp
              chain.doFilter(request, response);
            } else {
              // forward rewritten URI (without path prefix and core/collection name) to Restlet
              req.getRequestDispatcher(path).forward(request, response);
            }
            return;
          }

          // Determine the handler from the url path if not set
          // (we might already have selected the cores handler)
          if( handler == null && path.length() > 1 ) { // don't match "" or "/" as valid path
            handler = core.getRequestHandler( path );
            // no handler yet but allowed to handle select; let's check
            if( handler == null && parser.isHandleSelect() ) {
              if( "/select".equals( path ) || "/select/".equals( path ) ) {
                solrReq = parser.parse( core, path, req );

                checkStateIsValid(cores,solrReq.getParams().get(CloudSolrServer.STATE_VERSION));
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

            if (usingAliases) {
              processAliases(solrReq, aliases, collectionsList);
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
                Iterator<Entry<String, String>> headers = solrRsp.httpHeaders();
                while (headers.hasNext()) {
                  Entry<String, String> entry = headers.next();
                  resp.addHeader(entry.getKey(), entry.getValue());
                }
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
        // walk the the entire cause chain to search for an Error
        Throwable t = ex;
        while (t != null) {
          if (t instanceof Error)  {
            if (t != ex)  {
              log.error("An Error was wrapped in another exception - please report complete stacktrace on SOLR-6161", ex);
            }
            throw (Error) t;
          }
          t = t.getCause();
        }
        return;
      } finally {
        try {
          if (solrReq != null) {
            log.debug("Closing out SolrRequest: {}", solrReq);
            solrReq.close();
          }
        } finally {
          try {
            if (core != null) {
              core.close();
            }
          } finally {
            SolrRequestInfo.clearRequestInfo();
          }
        }
      }
    }

    // Otherwise let the webapp handle the request
    chain.doFilter(request, response);
  }

  private void checkStateIsValid(CoreContainer cores, String stateVer) {
    if (stateVer != null && !stateVer.isEmpty() && cores.isZooKeeperAware()) {
      // many have multiple collections separated by |
      String[] pairs = StringUtils.split(stateVer, '|');
      for (String pair : pairs) {
        String[] pcs = StringUtils.split(pair, ':');
        if (pcs.length == 2 && !pcs[0].isEmpty() && !pcs[1].isEmpty()) {
          Boolean status = cores.getZkController().getZkStateReader().checkValid(pcs[0], Integer.parseInt(pcs[1]));
          
          if (Boolean.TRUE != status) {
            throw new SolrException(ErrorCode.INVALID_STATE, "STATE STALE: " + pair + "valid : " + status);
          }
        }
      }
    }
  }

  private void processAliases(SolrQueryRequest solrReq, Aliases aliases,
      List<String> collectionsList) {
    String collection = solrReq.getParams().get("collection");
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
        params.set("collection", collectionString.toString());
        solrReq.setParams(params);
      }
    }
  }

  private void remoteQuery(String coreUrl, HttpServletRequest req,
      SolrQueryRequest solrReq, HttpServletResponse resp) throws IOException {
    HttpRequestBase method = null;
    HttpEntity httpEntity = null;
    boolean success = false;
    try {
      String urlstr = coreUrl;
      
      String queryString = req.getQueryString();
      
      urlstr += queryString == null ? "" : "?" + queryString;
      
      URL url = new URL(urlstr);
      boolean isPostOrPutRequest = "POST".equals(req.getMethod()) || "PUT".equals(req.getMethod());

      if ("GET".equals(req.getMethod())) {
        method = new HttpGet(urlstr);
      }
      else if ("HEAD".equals(req.getMethod())) {
        method = new HttpHead(urlstr);
      }
      else if (isPostOrPutRequest) {
        HttpEntityEnclosingRequestBase entityRequest =
          "POST".equals(req.getMethod()) ? new HttpPost(urlstr) : new HttpPut(urlstr);
        HttpEntity entity = new InputStreamEntity(req.getInputStream(), req.getContentLength());
        entityRequest.setEntity(entity);
        method = entityRequest;
      }
      else if ("DELETE".equals(req.getMethod())) {
        method = new HttpDelete(urlstr);
      }
      else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected method type: " + req.getMethod());
      }

      for (Enumeration<String> e = req.getHeaderNames(); e.hasMoreElements();) {
        String headerName = e.nextElement();
        method.addHeader(headerName, req.getHeader(headerName));
      }
      // These headers not supported for HttpEntityEnclosingRequests
      if (method instanceof HttpEntityEnclosingRequest) {
        method.removeHeaders(TRANSFER_ENCODING_HEADER);
        method.removeHeaders(CONTENT_LENGTH_HEADER);
      }

      final HttpResponse response = httpClient.execute(method);
      int httpStatus = response.getStatusLine().getStatusCode();
      httpEntity = response.getEntity();

      resp.setStatus(httpStatus);
      for (HeaderIterator responseHeaders = response.headerIterator(); responseHeaders.hasNext();) {
        Header header = responseHeaders.nextHeader();

        // We pull out these two headers below because they can cause chunked
        // encoding issues with Tomcat
        if (header != null && !header.getName().equals(TRANSFER_ENCODING_HEADER)
          && !header.getName().equals(CONNECTION_HEADER)) {
            resp.addHeader(header.getName(), header.getValue());
        }
      }

      if (httpEntity != null) {
        if (httpEntity.getContentEncoding() != null) resp.setCharacterEncoding(httpEntity.getContentEncoding().getValue());
        if (httpEntity.getContentType() != null) resp.setContentType(httpEntity.getContentType().getValue());

        InputStream is = httpEntity.getContent();
        OutputStream os = resp.getOutputStream();
        try {
          IOUtils.copyLarge(is, os);
          os.flush();
        } finally {
          IOUtils.closeQuietly(os);   // TODO: I thought we weren't supposed to explicitly close servlet streams
          IOUtils.closeQuietly(is);
        }
      }
      success = true;
    } catch (IOException e) {
      sendError(null, solrReq, req, resp, new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error trying to proxy request for url: " + coreUrl, e));
    } finally {
      EntityUtils.consumeQuietly(httpEntity);
      if (method != null && !success) {
        method.abort();
      }
    }

  }
  
  private String getRemotCoreUrl(CoreContainer cores, String collectionName, String origCorename) {
    ClusterState clusterState = cores.getZkController().getClusterState();
    Collection<Slice> slices = clusterState.getActiveSlices(collectionName);
    boolean byCoreName = false;
    
    if (slices == null) {
      slices = new ArrayList<>();
      // look by core name
      byCoreName = true;
      slices = getSlicesForCollections(clusterState, slices, true);
      if (slices == null || slices.size() == 0) {
        slices = getSlicesForCollections(clusterState, slices, false);
      }
    }
    
    if (slices == null || slices.size() == 0) {
      return null;
    }
    
    String coreUrl = getCoreUrl(cores, collectionName, origCorename, clusterState,
        slices, byCoreName, true);
    
    if (coreUrl == null) {
      coreUrl = getCoreUrl(cores, collectionName, origCorename, clusterState,
          slices, byCoreName, false);
    }
    
    return coreUrl;
  }

  private String getCoreUrl(CoreContainer cores, String collectionName,
      String origCorename, ClusterState clusterState, Collection<Slice> slices,
      boolean byCoreName, boolean activeReplicas) {
    String coreUrl;
    Set<String> liveNodes = clusterState.getLiveNodes();
    Iterator<Slice> it = slices.iterator();
    while (it.hasNext()) {
      Slice slice = it.next();
      Map<String,Replica> sliceShards = slice.getReplicasMap();
      for (ZkNodeProps nodeProps : sliceShards.values()) {
        ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(nodeProps);
        if (!activeReplicas || (liveNodes.contains(coreNodeProps.getNodeName())
            && coreNodeProps.getState().equals(ZkStateReader.ACTIVE))) {

          if (byCoreName && !collectionName.equals(coreNodeProps.getCoreName())) {
            // if it's by core name, make sure they match
            continue;
          }
          if (coreNodeProps.getBaseUrl().equals(cores.getZkController().getBaseUrl())) {
            // don't count a local core
            continue;
          }

          if (origCorename != null) {
            coreUrl = coreNodeProps.getBaseUrl() + "/" + origCorename;
          } else {
            coreUrl = coreNodeProps.getCoreUrl();
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

  private Collection<Slice> getSlicesForCollections(ClusterState clusterState,
      Collection<Slice> slices, boolean activeSlices) {
    Set<String> collections = clusterState.getCollections();
    for (String collection : collections) {
      if (activeSlices) {
        slices.addAll(clusterState.getActiveSlices(collection));
      } else {
        slices.addAll(clusterState.getSlices(collection));
      }
    }
    return slices;
  }
  
  private SolrCore getCoreByCollection(CoreContainer cores, String corename, String path) {
    String collection = corename;
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    
    ClusterState clusterState = zkStateReader.getClusterState();
    Map<String,Slice> slices = clusterState.getActiveSlicesMap(collection);
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
    SolrCore.preDecorateResponse(solrReq, solrResp);
    handler.handleRequest(solrReq, solrResp);
    SolrCore.postDecorateResponse(handler, solrReq, solrResp);
    if (log.isInfoEnabled() && solrResp.getToLog().size() > 0) {
      log.info(solrResp.getToLogAsString("[admin] "));
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
      int code = ResponseUtils.getErrorInfo(solrRsp.getException(), info, log);
      solrRsp.add("error", info);
      ((HttpServletResponse) response).setStatus(code);
    }
    
    if (Method.HEAD != reqMethod) {
      QueryResponseWriterUtil.writeQueryResponse(response.getOutputStream(), responseWriter, solrReq, solrRsp, ct);
    }
    //else http HEAD request, nothing to write out, waited this long just to get ContentType
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
    Exception exp = null;
    SolrCore localCore = null;
    try {
      SolrQueryResponse solrResp = new SolrQueryResponse();
      if(ex instanceof Exception) {
        solrResp.setException((Exception)ex);
      }
      else {
        solrResp.setException(new RuntimeException(ex));
      }
      if(core==null) {
        localCore = cores.getCore(""); // default core
      } else {
        localCore = core;
      }
      if(req==null) {
        final SolrParams solrParams;
        if (request instanceof HttpServletRequest) {
          // use GET parameters if available:
          solrParams = SolrRequestParsers.parseQueryString(((HttpServletRequest) request).getQueryString());
        } else {
          // we have no params at all, use empty ones:
          solrParams = new MapSolrParams(Collections.<String,String>emptyMap());
        }
        req = new SolrQueryRequestBase(core, solrParams) {};
      }
      QueryResponseWriter writer = core.getQueryResponseWriter(req);
      writeResponse(solrResp, response, writer, req, Method.GET);
    }
    catch (Exception e) { // This error really does not matter
         exp = e;
    } finally {
      try {
        if (exp != null) {
          SimpleOrderedMap info = new SimpleOrderedMap();
          int code = ResponseUtils.getErrorInfo(ex, info, log);
          response.sendError(code, info.toString());
        }
      } finally {
        if (core == null && localCore != null) {
          localCore.close();
        }
      }
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
