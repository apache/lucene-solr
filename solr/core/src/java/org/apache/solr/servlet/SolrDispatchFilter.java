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

import javax.management.MBeanServer;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.util.SolrFileCleaningTracker;
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
  
  // Effectively immutable
  private Boolean testMode = null;

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
    // turn on test mode when running tests
    assert testMode = true;
    
    if (testMode == null) {
      testMode = false;
    } else {
      String tm = System.getProperty("solr.tests.doContainerStreamCloseAssert");
      if (tm != null) {
        testMode = Boolean.parseBoolean(tm);
      } else {
        testMode = true;
      }
    }
  }

  public static final String PROPERTIES_ATTRIBUTE = "solr.properties";

  public static final String SOLRHOME_ATTRIBUTE = "solr.solr.home";

  public static final String SOLR_LOG_MUTECONSOLE = "solr.log.muteconsole";

  public static final String SOLR_LOG_LEVEL = "solr.log.level";

  @Override
  public void init(FilterConfig config) throws ServletException
  {
    log.trace("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());

    SolrRequestParsers.fileCleaningTracker = new SolrFileCleaningTracker();
    
    StartupLoggingUtils.checkLogDir();
    logWelcomeBanner();
    String muteConsole = System.getProperty(SOLR_LOG_MUTECONSOLE);
    if (muteConsole != null && !Arrays.asList("false","0","off","no").contains(muteConsole.toLowerCase(Locale.ROOT))) {
      StartupLoggingUtils.muteConsole();
    }
    String logLevel = System.getProperty(SOLR_LOG_LEVEL);
    if (logLevel != null) {
      StartupLoggingUtils.changeLogLevel(logLevel);
    }

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
      setupJvmMetrics();
      log.debug("user.dir=" + System.getProperty("user.dir"));
    }
    catch( Throwable t ) {
      // catch this so our filter still works
      log.error( "Could not start Solr. Check solr/home property and the logs");
      SolrCore.log( t );
      if (t instanceof Error) {
        throw (Error) t;
      }
    }

    log.trace("SolrDispatchFilter.init() done");
  }

  private void setupJvmMetrics()  {
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    SolrMetricManager metricManager = cores.getMetricManager();
    try {
      String registry = SolrMetricManager.getRegistryName(SolrInfoMBean.Group.jvm);
      metricManager.registerAll(registry, new BufferPoolMetricSet(platformMBeanServer), true, "bufferPools");
      metricManager.registerAll(registry, new ClassLoadingGaugeSet(), true, "classLoading");
      metricManager.register(registry, new FileDescriptorRatioGauge(), true, "fileDescriptorRatio");
      metricManager.registerAll(registry, new GarbageCollectorMetricSet(), true, "gc");
      metricManager.registerAll(registry, new MemoryUsageGaugeSet(), true, "memory");
      metricManager.registerAll(registry, new ThreadStatesGaugeSet(), true, "threads"); // todo should we use CachedThreadStatesGaugeSet instead?
    } catch (Exception e) {
      log.warn("Error registering JVM metrics", e);
    }
  }

  private void logWelcomeBanner() {
    log.info(" ___      _       Welcome to Apache Solr™ version {}", solrVersion());
    log.info("/ __| ___| |_ _   Starting in {} mode on port {}", isCloudMode() ? "cloud" : "standalone", getSolrPort());
    log.info("\\__ \\/ _ \\ | '_|  Install dir: {}", System.getProperty("solr.install.dir"));
    log.info("|___/\\___/_|_|    Start time: {}", Instant.now().toString());
  }

  private String solrVersion() {
    String specVer = Version.LATEST.toString();
    try {
      String implVer = SolrCore.class.getPackage().getImplementationVersion();
      return (specVer.equals(implVer.split(" ")[0])) ? specVer : implVer;
    } catch (Exception e) {
      return specVer;
    }
  }

  private String getSolrPort() {
    return System.getProperty("jetty.port");
  }

  /* We are in cloud mode if Java option zkRun exists OR zkHost exists and is non-empty */
  private boolean isCloudMode() {
    return ((System.getProperty("zkHost") != null && !StringUtils.isEmpty(System.getProperty("zkHost")))
    || System.getProperty("zkRun") != null);
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
    try {
      FileCleaningTracker fileCleaningTracker = SolrRequestParsers.fileCleaningTracker;
      if (fileCleaningTracker != null) {
        fileCleaningTracker.exitWhenFinished();
      }
    } catch (Exception e) {
      log.warn("Exception closing FileCleaningTracker", e);
    } finally {
      SolrRequestParsers.fileCleaningTracker = null;
    }

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
    try {

      if (cores == null || cores.isShutDown()) {
        log.error("Error processing the request. CoreContainer is either not initialized or shutting down.");
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
            "Error processing the request. CoreContainer is either not initialized or shutting down.");
      }

      AtomicReference<ServletRequest> wrappedRequest = new AtomicReference<>();
      if (!authenticateRequest(request, response, wrappedRequest)) { // the response and status code have already been
                                                                     // sent
        return;
      }
      if (wrappedRequest.get() != null) {
        request = wrappedRequest.get();
      }

      request = closeShield(request, retry);
      response = closeShield(response, retry);
      
      if (cores.getAuthenticationPlugin() != null) {
        log.debug("User principal: {}", ((HttpServletRequest) request).getUserPrincipal());
      }

      // No need to even create the HttpSolrCall object if this path is excluded.
      if (excludePatterns != null) {
        String requestPath = ((HttpServletRequest) request).getServletPath();
        String extraPath = ((HttpServletRequest) request).getPathInfo();
        if (extraPath != null) { // In embedded mode, servlet path is empty - include all post-context path here for
                                 // testing
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
      ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
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
        ExecutorUtil.setServerThreadFlag(null);
      }
    } finally {
      consumeInputFully((HttpServletRequest) request);
    }
  }
  
  // we make sure we read the full client request so that the client does
  // not hit a connection reset and we can reuse the 
  // connection - see SOLR-8453 and SOLR-8683
  private void consumeInputFully(HttpServletRequest req) {
    try {
      ServletInputStream is = req.getInputStream();
      while (!is.isFinished() && is.read() != -1) {}
    } catch (IOException e) {
      log.info("Could not consume full client request", e);
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
    boolean requestContinues = false;
    final AtomicBoolean isAuthenticated = new AtomicBoolean(false);
    AuthenticationPlugin authenticationPlugin = cores.getAuthenticationPlugin();
    if (authenticationPlugin == null) {
      return true;
    } else {
      // /admin/info/key must be always open. see SOLR-9188
      // tests work only w/ getPathInfo
      //otherwise it's just enough to have getServletPath()
      if (PKIAuthenticationPlugin.PATH.equals(((HttpServletRequest) request).getServletPath()) ||
          PKIAuthenticationPlugin.PATH.equals(((HttpServletRequest) request).getPathInfo())) return true;
      String header = ((HttpServletRequest) request).getHeader(PKIAuthenticationPlugin.HEADER);
      if (header != null && cores.getPkiAuthenticationPlugin() != null)
        authenticationPlugin = cores.getPkiAuthenticationPlugin();
      try {
        log.debug("Request to authenticate: {}, domain: {}, port: {}", request, request.getLocalName(), request.getLocalPort());
        // upon successful authentication, this should call the chain's next filter.
        requestContinues = authenticationPlugin.doAuthenticate(request, response, (req, rsp) -> {
          isAuthenticated.set(true);
          wrappedRequest.set(req);
        });
      } catch (Exception e) {
        log.info("Error authenticating", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error during request authentication, ", e);
      }
    }
    // requestContinues is an optional short circuit, thus we still need to check isAuthenticated.
    // This is because the AuthenticationPlugin doesn't always have enough information to determine if
    // it should short circuit, e.g. the Kerberos Authentication Filter will send an error and not
    // call later filters in chain, but doesn't throw an exception.  We could force each Plugin
    // to implement isAuthenticated to simplify the check here, but that just moves the complexity to
    // multiple code paths.
    if (!requestContinues || !isAuthenticated.get()) {
      response.flushBuffer();
      return false;
    }
    return true;
  }
  
  /**
   * Wrap the request's input stream with a close shield, as if by a {@link CloseShieldInputStream}. If this is a
   * retry, we will assume that the stream has already been wrapped and do nothing.
   *
   * @param request The request to wrap.
   * @param retry If this is an original request or a retry.
   * @return A request object with an {@link InputStream} that will ignore calls to close.
   */
  private ServletRequest closeShield(ServletRequest request, boolean retry) {
    if (testMode && !retry) {
      return new HttpServletRequestWrapper((HttpServletRequest) request) {
        ServletInputStream stream;
        
        @Override
        public ServletInputStream getInputStream() throws IOException {
          // Lazy stream creation
          if (stream == null) {
            stream = new ServletInputStreamWrapper(super.getInputStream()) {
              @Override
              public void close() {
                assert false : "Attempted close of request input stream.";
              }
            };
          }
          return stream;
        }
      };
    } else {
      return request;
    }
  }
  
  /**
   * Wrap the response's output stream with a close shield, as if by a {@link CloseShieldOutputStream}. If this is a
   * retry, we will assume that the stream has already been wrapped and do nothing.
   *
   * @param response The response to wrap.
   * @param retry If this response corresponds to an original request or a retry.
   * @return A response object with an {@link OutputStream} that will ignore calls to close.
   */
  private ServletResponse closeShield(ServletResponse response, boolean retry) {
    if (testMode && !retry) {
      return new HttpServletResponseWrapper((HttpServletResponse) response) {
        ServletOutputStream stream;
        
        @Override
        public ServletOutputStream getOutputStream() throws IOException {
          // Lazy stream creation
          if (stream == null) {
            stream = new ServletOutputStreamWrapper(super.getOutputStream()) {
              @Override
              public void close() {
                assert false : "Attempted close of response output stream.";
              }
            };
          }
          return stream;
        }
      };
    } else {
      return response;
    }
  }
}
