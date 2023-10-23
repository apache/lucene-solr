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
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.lucene.util.Version;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.core.SolrPaths;
import org.apache.solr.metrics.AltBufferPoolMetricSet;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.OperatingSystemMetricSet;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.security.AuditEvent.EventType;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter extends BaseSolrFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected volatile CoreContainer cores;
  protected final CountDownLatch init = new CountDownLatch(1);

  protected String abortErrorMessage = null;
  //TODO using Http2Client
  protected HttpClient httpClient;
  private ArrayList<Pattern> excludePatterns;
  
  private boolean isV2Enabled = !"true".equals(System.getProperty("disable.v2.api", "false"));

  private final String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);
  private SolrMetricManager metricManager;
  private String registryName;
  private volatile boolean closeOnDestroy = true;
  private Properties extraProperties;

  /**
   * Enum to define action that needs to be processed.
   * PASSTHROUGH: Pass through to another filter via webapp.
   * FORWARD: Forward rewritten URI (without path prefix and core/collection name) to another filter in the chain
   * RETURN: Returns the control, and no further specific processing is needed.
   *  This is generally when an error is set and returned.
   * RETRY:Retry the request. In cases when a core isn't found to work with, this is set.
   */
  public enum Action {
    PASSTHROUGH, FORWARD, RETURN, RETRY, ADMIN, REMOTEQUERY, PROCESS
  }
  
  public SolrDispatchFilter() {
  }

  public static final String PROPERTIES_ATTRIBUTE = "solr.properties";

  public static final String SOLRHOME_ATTRIBUTE = "solr.solr.home";

  public static final String SOLR_INSTALL_DIR_ATTRIBUTE = "solr.install.dir";

  public static final String SOLR_DEFAULT_CONFDIR_ATTRIBUTE = "solr.default.confdir";

  public static final String SOLR_LOG_MUTECONSOLE = "solr.log.muteconsole";

  public static final String SOLR_LOG_LEVEL = "solr.log.level";

  @Override
  public void init(FilterConfig config) throws ServletException {
    SSLConfigurationsFactory.current().init();
    if (log.isTraceEnabled()) {
      log.trace("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());
    }
    CoreContainer coresInit = null;
    try {
      // "extra" properties must be init'ed first so we know things like "do we have a zkHost"
      // wrap as defaults (if set) so we can modify w/o polluting the Properties provided by our caller
      this.extraProperties = SolrXmlConfig.wrapAndSetZkHostFromSysPropIfNeeded
        ((Properties) config.getServletContext().getAttribute(PROPERTIES_ATTRIBUTE));
      
      StartupLoggingUtils.checkLogDir();
      if (log.isInfoEnabled()) {
        log.info("Using logger factory {}", StartupLoggingUtils.getLoggerImplStr());
      }
      
      logWelcomeBanner();
      
      String muteConsole = System.getProperty(SOLR_LOG_MUTECONSOLE);
      if (muteConsole != null && !Arrays.asList("false","0","off","no").contains(muteConsole.toLowerCase(Locale.ROOT))) {
        StartupLoggingUtils.muteConsole();
      }
      String logLevel = System.getProperty(SOLR_LOG_LEVEL);
      if (logLevel != null) {
        log.info("Log level override, property solr.log.level={}", logLevel);
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

      coresInit = createCoreContainer(computeSolrHome(config), extraProperties);
      this.httpClient = coresInit.getUpdateShardHandler().getDefaultHttpClient();
      setupJvmMetrics(coresInit);
      
      if (log.isDebugEnabled()) {
        log.debug("user.dir={}", System.getProperty("user.dir"));
      }
    } catch( Throwable t ) {
      // catch this so our filter still works
      log.error( "Could not start Solr. Check solr/home property and the logs");
      SolrCore.log( t );
      if (t instanceof Error) {
        throw (Error) t;
      }
      
    } finally{
      log.trace("SolrDispatchFilter.init() done");
      this.cores = coresInit; // crucially final assignment 
      init.countDown();
    }
  }

  private void setupJvmMetrics(CoreContainer coresInit)  {
    metricManager = coresInit.getMetricManager();
    registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm);
    final Set<String> hiddenSysProps = coresInit.getConfig().getMetricsConfig().getHiddenSysProps();
    try {
      metricManager.registerAll(registryName, new AltBufferPoolMetricSet(), SolrMetricManager.ResolutionStrategy.IGNORE, "buffers");
      metricManager.registerAll(registryName, new ClassLoadingGaugeSet(), SolrMetricManager.ResolutionStrategy.IGNORE, "classes");
      metricManager.registerAll(registryName, new OperatingSystemMetricSet(), SolrMetricManager.ResolutionStrategy.IGNORE, "os");
      metricManager.registerAll(registryName, new GarbageCollectorMetricSet(), SolrMetricManager.ResolutionStrategy.IGNORE, "gc");
      metricManager.registerAll(registryName, new MemoryUsageGaugeSet(), SolrMetricManager.ResolutionStrategy.IGNORE, "memory");
      metricManager.registerAll(registryName, new ThreadStatesGaugeSet(), SolrMetricManager.ResolutionStrategy.IGNORE, "threads"); // todo should we use CachedThreadStatesGaugeSet instead?
      MetricsMap sysprops = new MetricsMap(map -> {
        System.getProperties().forEach((k, v) -> {
          if (!hiddenSysProps.contains(k)) {
            map.putNoEx(String.valueOf(k), v);
          }
        });
      });
      metricManager.registerGauge(null, registryName, sysprops, metricTag, true, "properties", "system");
    } catch (Exception e) {
      log.warn("Error registering JVM metrics", e);
    }
  }

  private void logWelcomeBanner() {
    // _Really_ sorry about how clumsy this is as a result of the logging call checker, but this is the only one
    // that's so ugly so far.
    if (log.isInfoEnabled()) {
      log.info(" ___      _       Welcome to Apache Solr™ version {}", solrVersion());
    }
    if (log.isInfoEnabled()) {
      log.info("/ __| ___| |_ _   Starting in {} mode on port {}", isCloudMode() ? "cloud" : "standalone", getSolrPort());
    }
    if (log.isInfoEnabled()) {
      log.info("\\__ \\/ _ \\ | '_|  Install dir: {}", System.getProperty(SOLR_INSTALL_DIR_ATTRIBUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("|___/\\___/_|_|    Start time: {}", Instant.now());
    }
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

  /** 
   * We are in cloud mode if Java option zkRun exists OR zkHost exists and is non-empty 
   * @see SolrXmlConfig#wrapAndSetZkHostFromSysPropIfNeeded
   * @see #extraProperties
   * @see #init
   */
  private boolean isCloudMode() {
    assert null != extraProperties; // we should never be called w/o this being initialized
    return (null != extraProperties.getProperty(SolrXmlConfig.ZK_HOST)) || (null != System.getProperty("zkRun"));
  }

  /**
   * Returns the effective Solr Home to use for this node
   */
  private static Path computeSolrHome(FilterConfig config) {
    final String solrHome = (String) config.getServletContext().getAttribute(SOLRHOME_ATTRIBUTE);
    return (solrHome == null ? SolrPaths.locateSolrHome() : Paths.get(solrHome));
  }
  
  /**
   * Override this to change CoreContainer initialization
   * @return a CoreContainer to hold this server's cores
   */
  protected CoreContainer createCoreContainer(Path solrHome, Properties nodeProps) {
    NodeConfig nodeConfig = loadNodeConfig(solrHome, nodeProps);
    final CoreContainer coreContainer = new CoreContainer(nodeConfig, true);
    coreContainer.load();
    return coreContainer;
  }

  /**
   * Get the NodeConfig whether stored on disk, in ZooKeeper, etc.
   * This may also be used by custom filters to load relevant configuration.
   * @return the NodeConfig
   */
  public static NodeConfig loadNodeConfig(Path solrHome, Properties nodeProperties) {
    if (!StringUtils.isEmpty(System.getProperty("solr.solrxml.location"))) {
      log.warn("Solr property solr.solrxml.location is no longer supported. Will automatically load solr.xml from ZooKeeper if it exists");
    }
    nodeProperties = SolrXmlConfig.wrapAndSetZkHostFromSysPropIfNeeded(nodeProperties);
    String zkHost = nodeProperties.getProperty(SolrXmlConfig.ZK_HOST);
    if (!StringUtils.isEmpty(zkHost)) {
      int startUpZkTimeOut = Integer.getInteger("waitForZk", 30);
      startUpZkTimeOut *= 1000;
      try (SolrZkClient zkClient = new SolrZkClient(zkHost, startUpZkTimeOut, startUpZkTimeOut)) {
        if (zkClient.exists("/solr.xml", true)) {
          log.info("solr.xml found in ZooKeeper. Loading...");
          byte[] data = zkClient.getData("/solr.xml", null, null, true);
          return SolrXmlConfig.fromInputStream(solrHome, new ByteArrayInputStream(data), nodeProperties);
        }
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error occurred while loading solr.xml from zookeeper", e);
      }
      log.info("Loading solr.xml from SolrHome (not found in ZooKeeper)");
    }

    return SolrXmlConfig.fromSolrHome(solrHome, nodeProperties);
  }
  
  public CoreContainer getCores() {
    return cores;
  }
  
  @Override
  public void destroy() {
    if (closeOnDestroy) {
      close();
    }
  }
  
  public void close() {
    CoreContainer cc = cores;
    cores = null;
    try {
      if (metricManager != null) {
        try {
          metricManager.unregisterGauges(registryName, metricTag);
        } catch (NullPointerException e) {
          // okay
        } catch (Exception e) {
          log.warn("Exception closing FileCleaningTracker", e);
        } finally {
          metricManager = null;
        }
      }
    } finally {
      if (cc != null) {
        httpClient = null;
        cc.shutdown();
      }
      GlobalTracer.get().close();
    }
  }
  
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    doFilter(request, response, chain, false);
  }
  
  public void doFilter(ServletRequest _request, ServletResponse _response, FilterChain chain, boolean retry) throws IOException, ServletException {
    if (!(_request instanceof HttpServletRequest)) return;
    HttpServletRequest request = closeShield((HttpServletRequest)_request, retry);
    HttpServletResponse response = closeShield((HttpServletResponse)_response, retry);
    Scope scope = null;
    Span span = null;
    try {

      if (cores == null || cores.isShutDown()) {
        try {
          init.await();
        } catch (InterruptedException e) { //well, no wait then
        }
        final String msg = "Error processing the request. CoreContainer is either not initialized or shutting down.";
        if (cores == null || cores.isShutDown()) {
          log.error(msg);
          throw new UnavailableException(msg);
        }
      }

      String requestPath = ServletUtils.getPathAfterContext(request);
      // No need to even create the HttpSolrCall object if this path is excluded.
      if (excludePatterns != null) {
        for (Pattern p : excludePatterns) {
          Matcher matcher = p.matcher(requestPath);
          if (matcher.lookingAt()) {
            chain.doFilter(request, response);
            return;
          }
        }
      }

      SpanContext parentSpan = GlobalTracer.get().extract(request);
      Tracer tracer = GlobalTracer.getTracer();

      Tracer.SpanBuilder spanBuilder = null;
      String hostAndPort = request.getServerName() + "_" + request.getServerPort();
      if (parentSpan == null) {
        spanBuilder = tracer.buildSpan(request.getMethod() + ":" + hostAndPort);
      } else {
        spanBuilder = tracer.buildSpan(request.getMethod() + ":" + hostAndPort)
            .asChildOf(parentSpan);
      }

      spanBuilder
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
          .withTag(Tags.HTTP_URL.getKey(), request.getRequestURL().toString());
      span = spanBuilder.start();
      scope = tracer.scopeManager().activate(span);

      AtomicReference<HttpServletRequest> wrappedRequest = new AtomicReference<>();
      if (!authenticateRequest(request, response, wrappedRequest)) { // the response and status code have already been sent
        return;
      }
      if (wrappedRequest.get() != null) {
        request = wrappedRequest.get();
      }

      if (cores.getAuthenticationPlugin() != null) {
        if (log.isDebugEnabled()) {
          log.debug("User principal: {}", request.getUserPrincipal());
        }
      }

      HttpSolrCall call = getHttpSolrCall(request, response, retry);
      ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
      try {
        Action result = call.call();
        switch (result) {
          case PASSTHROUGH:
            chain.doFilter(request, response);
            break;
          case RETRY:
            doFilter(request, response, chain, true); // RECURSION
            break;
          case FORWARD:
            request.getRequestDispatcher(call.getPath()).forward(request, response);
            break;
          case ADMIN:
          case PROCESS:
          case REMOTEQUERY:
          case RETURN:
            break;
        }
      } finally {
        call.destroy();
        ExecutorUtil.setServerThreadFlag(null);
      }
    } finally {
      if (span != null) span.finish();
      if (scope != null) scope.close();

      GlobalTracer.get().clearContext();
      consumeInputFully(request, response);
      SolrRequestInfo.reset();
      SolrRequestParsers.cleanupMultipartFiles(request);
    }
  }
  
  // we make sure we read the full client request so that the client does
  // not hit a connection reset and we can reuse the 
  // connection - see SOLR-8453 and SOLR-8683
  private void consumeInputFully(HttpServletRequest req, HttpServletResponse response) {
    try {
      ServletInputStream is = req.getInputStream();
      while (!is.isFinished() && is.read() != -1) {}
    } catch (IOException e) {
      if (req.getHeader(HttpHeaders.EXPECT) != null && response.isCommitted()) {
        log.debug("No input stream to consume from client");
      } else {
        log.info("Could not consume full client request", e);
      }
    }
  }
  
  /**
   * Allow a subclass to modify the HttpSolrCall.  In particular, subclasses may
   * want to add attributes to the request and send errors differently
   */
  protected HttpSolrCall getHttpSolrCall(HttpServletRequest request, HttpServletResponse response, boolean retry) {
    String path = ServletUtils.getPathAfterContext(request);

    if (isV2Enabled && (path.startsWith("/____v2/") || path.equals("/____v2"))) {
      return new V2HttpCall(this, cores, request, response, false);
    } else {
      return new HttpSolrCall(this, cores, request, response, retry);
    }
  }

  private boolean authenticateRequest(HttpServletRequest request, HttpServletResponse response, final AtomicReference<HttpServletRequest> wrappedRequest) throws IOException {
    boolean requestContinues = false;
    final AtomicBoolean isAuthenticated = new AtomicBoolean(false);
    AuthenticationPlugin authenticationPlugin = cores.getAuthenticationPlugin();
    if (authenticationPlugin == null) {
      if (shouldAudit(EventType.ANONYMOUS)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ANONYMOUS, request));
      }
      return true;
    } else {
      // /admin/info/key must be always open. see SOLR-9188
      String requestPath = ServletUtils.getPathAfterContext(request);
      if (PublicKeyHandler.PATH.equals(requestPath)) {
        log.debug("Pass through PKI authentication endpoint");
        return true;
      }
      // /solr/ (Admin UI) must be always open to allow displaying Admin UI with login page  
      if ("/solr/".equals(requestPath) || "/".equals(requestPath)) {
        log.debug("Pass through Admin UI entry point");
        return true;
      }
      String header = request.getHeader(PKIAuthenticationPlugin.HEADER);
      String headerV2 = request.getHeader(PKIAuthenticationPlugin.HEADER_V2);
      if ((header != null || headerV2 != null) && cores.getPkiAuthenticationSecurityBuilder() != null)
        authenticationPlugin = cores.getPkiAuthenticationSecurityBuilder();
      try {
        if (log.isDebugEnabled()) {
          log.debug("Request to authenticate: {}, domain: {}, port: {}", request, request.getLocalName(), request.getLocalPort());
        }
        // upon successful authentication, this should call the chain's next filter.
        requestContinues = authenticationPlugin.authenticate(request, response, (req, rsp) -> {
          isAuthenticated.set(true);
          wrappedRequest.set((HttpServletRequest) req);
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
      if (shouldAudit(EventType.REJECTED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.REJECTED, request));
      }
      return false;
    }
    if (shouldAudit(EventType.AUTHENTICATED)) {
      cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.AUTHENTICATED, request));
    }
    return true;
  }
  
  public static class ClosedServletInputStream extends ServletInputStream {
    
    public static final ClosedServletInputStream CLOSED_SERVLET_INPUT_STREAM = new ClosedServletInputStream();

    @Override
    public int read() {
      return -1;
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setReadListener(ReadListener arg0) {}
  }
  
  public static class ClosedServletOutputStream extends ServletOutputStream {
    
    public static final ClosedServletOutputStream CLOSED_SERVLET_OUTPUT_STREAM = new ClosedServletOutputStream();
    
    @Override
    public void write(final int b) throws IOException {
      throw new IOException("write(" + b + ") failed: stream is closed");
    }
    
    @Override
    public void flush() throws IOException {
      throw new IOException("flush() failed: stream is closed");
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setWriteListener(WriteListener arg0) {
      throw new RuntimeException("setWriteListener() failed: stream is closed");
    }
  }

  private static String CLOSE_STREAM_MSG = "Attempted close of http request or response stream - in general you should not do this, "
      + "you may spoil connection reuse and possibly disrupt a client. If you must close without actually needing to close, "
      + "use a CloseShield*Stream. Closing or flushing the response stream commits the response and prevents us from modifying it. "
      + "Closing the request stream prevents us from gauranteeing ourselves that streams are fully read for proper connection reuse."
      + "Let the container manage the lifecycle of these streams when possible.";
 

  /**
   * Check if audit logging is enabled and should happen for given event type
   * @param eventType the audit event
   */
  private boolean shouldAudit(AuditEvent.EventType eventType) {
    return cores.getAuditLoggerPlugin() != null && cores.getAuditLoggerPlugin().shouldLog(eventType);
  }
  
  /**
   * Wrap the request's input stream with a close shield. If this is a
   * retry, we will assume that the stream has already been wrapped and do nothing.
   *
   * Only the container should ever actually close the servlet output stream.
   *
   * @param request The request to wrap.
   * @param retry If this is an original request or a retry.
   * @return A request object with an {@link InputStream} that will ignore calls to close.
   */
  public static HttpServletRequest closeShield(HttpServletRequest request, boolean retry) {
    if (!retry) {
      return new HttpServletRequestWrapper(request) {

        @Override
        public ServletInputStream getInputStream() throws IOException {

          return new ServletInputStreamWrapper(super.getInputStream()) {
            @Override
            public void close() {
              // even though we skip closes, we let local tests know not to close so that a full understanding can take
              // place
              assert Thread.currentThread().getStackTrace()[2].getClassName().matches(
                  "org\\.apache\\.(?:solr|lucene).*") ? false : true : CLOSE_STREAM_MSG;
              this.stream = ClosedServletInputStream.CLOSED_SERVLET_INPUT_STREAM;
            }
          };

        }
      };
    } else {
      return request;
    }
  }
  
  /**
   * Wrap the response's output stream with a close shield. If this is a
   * retry, we will assume that the stream has already been wrapped and do nothing.
   *
   * Only the container should ever actually close the servlet request stream.
   *
   * @param response The response to wrap.
   * @param retry If this response corresponds to an original request or a retry.
   * @return A response object with an {@link OutputStream} that will ignore calls to close.
   */
  public static HttpServletResponse closeShield(HttpServletResponse response, boolean retry) {
    if (!retry) {
      return new HttpServletResponseWrapper(response) {

        @Override
        public ServletOutputStream getOutputStream() throws IOException {

          return new ServletOutputStreamWrapper(super.getOutputStream()) {
            @Override
            public void close() {
              // even though we skip closes, we let local tests know not to close so that a full understanding can take
              // place
              assert Thread.currentThread().getStackTrace()[2].getClassName().matches(
                  "org\\.apache\\.(?:solr|lucene).*") ? false
                      : true : CLOSE_STREAM_MSG;
              stream = ClosedServletOutputStream.CLOSED_SERVLET_OUTPUT_STREAM;
            }
          };
        }

      };
    } else {
      return response;
    }
  }
  
  public void closeOnDestroy(boolean closeOnDestroy) {
    this.closeOnDestroy = closeOnDestroy;
  }
}
