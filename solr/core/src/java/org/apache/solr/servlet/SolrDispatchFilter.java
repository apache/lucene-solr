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

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter extends BaseSolrFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected volatile CoreContainer cores;

  protected String abortErrorMessage = null;
  protected HttpClient httpClient;
  private ArrayList<Pattern> excludePatterns;

  /**
   * Enum to define action that needs to be processed.
   * PASSTHROUGH: Pass through to Restlet via webapp.
   * FORWARD: Forward rewritten URI (without path prefix and core/collection name) to Restlet
   * RETURN: Returns the control, and no further specific processing is needed.
   *  This is generally when an error is set and returned.
   * RETRY:Retry the request. In cases when a core isn't found to work with, this is set.
   */
  enum Action {
    PASSTHROUGH, FORWARD, RETURN, RETRY, ADMIN, REMOTEQUERY, PROCESS
  }
  
  public SolrDispatchFilter() {
  }

  public static final String PROPERTIES_ATTRIBUTE = "solr.properties";

  public static final String SOLRHOME_ATTRIBUTE = "solr.solr.home";

  @Override
  public void init(FilterConfig config) throws ServletException
  {
    log.info("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());

    String exclude = config.getInitParameter("excludePatterns");
    if(exclude != null) {
      String[] excludeArray = exclude.split(",");
      excludePatterns = new ArrayList<>();
      for (String element : excludeArray) {
        excludePatterns.add(Pattern.compile(element));
      }
    }
    try {
      Properties extraProperties = (Properties) config.getServletContext().getAttribute(PROPERTIES_ATTRIBUTE);
      if (extraProperties == null)
        extraProperties = new Properties();

      String solrHome = (String) config.getServletContext().getAttribute(SOLRHOME_ATTRIBUTE);
      ExecutorUtil.addThreadLocalProvider(SolrRequestInfo.getInheritableThreadLocalProvider());

      this.cores = createCoreContainer(solrHome == null ? SolrResourceLoader.locateSolrHome() : Paths.get(solrHome),
                                       extraProperties);
      this.httpClient = cores.getUpdateShardHandler().getHttpClient();
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

  /**
   * Override this to change CoreContainer initialization
   * @return a CoreContainer to hold this server's cores
   */
  protected CoreContainer createCoreContainer(Path solrHome, Properties extraProperties) {
    NodeConfig nodeConfig = loadNodeConfig(solrHome, extraProperties);
    cores = new CoreContainer(nodeConfig, extraProperties, true);
    cores.load();
    return cores;
  }

  /**
   * Get the NodeConfig whether stored on disk, in ZooKeeper, etc.
   * This may also be used by custom filters to load relevant configuration.
   * @return the NodeConfig
   */
  public static NodeConfig loadNodeConfig(Path solrHome, Properties nodeProperties) {

    SolrResourceLoader loader = new SolrResourceLoader(solrHome, null, nodeProperties);
    if (!StringUtils.isEmpty(System.getProperty("solr.solrxml.location"))) {
      log.warn("Solr property solr.solrxml.location is no longer supported. " +
               "Will automatically load solr.xml from ZooKeeper if it exists");
    }

    String zkHost = System.getProperty("zkHost");
    if (!StringUtils.isEmpty(zkHost)) {
      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        if (zkClient.exists("/solr.xml", true)) {
          log.info("solr.xml found in ZooKeeper. Loading...");
          byte[] data = zkClient.getData("/solr.xml", null, null, true);
          return SolrXmlConfig.fromInputStream(loader, new ByteArrayInputStream(data));
        }
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error occurred while loading solr.xml from zookeeper", e);
      }
      log.info("Loading solr.xml from SolrHome (not found in ZooKeeper)");
    }
    return SolrXmlConfig.fromSolrHome(loader, loader.getInstancePath());
  }
  
  public CoreContainer getCores() {
    return cores;
  }
  
  @Override
  public void destroy() {
    if (cores != null) {
      try {
        cores.shutdown();
      } finally {
        cores = null;
      }
    }
  }
  
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    doFilter(request, response, chain, false);
  }
  
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain, boolean retry) throws IOException, ServletException {
    if (!(request instanceof HttpServletRequest)) return;

    if (cores == null || cores.isShutDown()) {
      log.error("Error processing the request. CoreContainer is either not initialized or shutting down.");
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
          "Error processing the request. CoreContainer is either not initialized or shutting down.");
    }

    AtomicReference<ServletRequest> wrappedRequest = new AtomicReference<>();
    if (!authenticateRequest(request, response, wrappedRequest)) { // the response and status code have already been sent
      return;
    }
    if (wrappedRequest.get() != null) {
      request = wrappedRequest.get();
    }
    if (cores.getAuthenticationPlugin() != null) {
      log.debug("User principal: {}", ((HttpServletRequest)request).getUserPrincipal());
    }

    // No need to even create the HttpSolrCall object if this path is excluded.
    if(excludePatterns != null) {
      String requestPath = ((HttpServletRequest) request).getServletPath();
      String extraPath = ((HttpServletRequest)request).getPathInfo();
      if (extraPath != null) { // In embedded mode, servlet path is empty - include all post-context path here for testing 
        requestPath += extraPath;
      }
      for (Pattern p : excludePatterns) {
        Matcher matcher = p.matcher(requestPath);
        if (matcher.lookingAt()) {
          chain.doFilter(request, response);
          return;
        }
      }
    }
    
    HttpSolrCall call = getHttpSolrCall((HttpServletRequest) request, (HttpServletResponse) response, retry);
    try {
      Action result = call.call();
      switch (result) {
        case PASSTHROUGH:
          chain.doFilter(request, response);
          break;
        case RETRY:
          doFilter(request, response, chain, true);
          break;
        case FORWARD:
          request.getRequestDispatcher(call.getPath()).forward(request, response);
          break;
      }  
    } finally {
      call.destroy();
    }
  }
  
  /**
   * Allow a subclass to modify the HttpSolrCall.  In particular, subclasses may
   * want to add attributes to the request and send errors differently
   */
  protected HttpSolrCall getHttpSolrCall(HttpServletRequest request, HttpServletResponse response, boolean retry) {
    return new HttpSolrCall(this, cores, request, response, retry);
  }

  private boolean authenticateRequest(ServletRequest request, ServletResponse response, final AtomicReference<ServletRequest> wrappedRequest) throws IOException {
    final AtomicBoolean isAuthenticated = new AtomicBoolean(false);
    AuthenticationPlugin authenticationPlugin = cores.getAuthenticationPlugin();
    if (authenticationPlugin == null) {
      return true;
    } else {
      //special case when solr is securing inter-node requests
      String header = ((HttpServletRequest) request).getHeader(PKIAuthenticationPlugin.HEADER);
      if (header != null && cores.getPkiAuthenticationPlugin() != null)
        authenticationPlugin = cores.getPkiAuthenticationPlugin();
      try {
        log.debug("Request to authenticate: {}, domain: {}, port: {}", request, request.getLocalName(), request.getLocalPort());
        // upon successful authentication, this should call the chain's next filter.
        authenticationPlugin.doAuthenticate(request, response, new FilterChain() {
          public void doFilter(ServletRequest req, ServletResponse rsp) throws IOException, ServletException {
            isAuthenticated.set(true);
            wrappedRequest.set(req);
          }
        });
      } catch (Exception e) {
        e.printStackTrace();
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error during request authentication, ", e);
      }
    }
    // failed authentication?
    if (!isAuthenticated.get()) {
      response.flushBuffer();
      return false;
    }
    return true;
  }
}
