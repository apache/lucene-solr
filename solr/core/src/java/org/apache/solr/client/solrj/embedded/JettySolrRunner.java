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
package org.apache.solr.client.solrj.embedded;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.BindException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.TimeOut;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.server.session.DefaultSessionIdManager;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.Source;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ReservedThreadExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Run solr using jetty
 *
 * @since solr 1.3
 */
public class JettySolrRunner {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int THREAD_POOL_MAX_THREADS = 10000;
  // NOTE: needs to be larger than SolrHttpClient.threadPoolSweeperMaxIdleTime
  private static final int THREAD_POOL_MAX_IDLE_TIME_MS = 260000;

  Server server;

  volatile FilterHolder dispatchFilter;
  volatile FilterHolder debugFilter;

  private boolean waitOnSolr = false;
  private int jettyPort = -1;

  private final JettyConfig config;
  private final String solrHome;
  private final Properties nodeProperties;

  private volatile boolean startedBefore = false;

  private LinkedList<FilterHolder> extraFilters;

  private static final String excludePatterns = "/partials/.+,/libs/.+,/css/.+,/js/.+,/img/.+,/templates/.+";

  private int proxyPort = -1;

  private final boolean enableProxy;

  private SocketProxy proxy;

  private String protocol;

  private String host;

  private volatile boolean started = false;

  public static class DebugFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private AtomicLong nRequests = new AtomicLong();

    List<Delay> delays = new ArrayList<>();

    public long getTotalRequests() {
      return nRequests.get();

    }

    /**
     * Introduce a delay of specified milliseconds for the specified request.
     *
     * @param reason Info message logged when delay occurs
     * @param count The count-th request will experience a delay
     * @param delay There will be a delay of this many milliseconds
     */
    public void addDelay(String reason, int count, int delay) {
      delays.add(new Delay(reason, count, delay));
    }

    /**
     * Remove any delay introduced before.
     */
    public void unsetDelay() {
      delays.clear();
    }


    @Override
    public void init(FilterConfig filterConfig) throws ServletException { }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      nRequests.incrementAndGet();
      executeDelay();
      filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() { }

    private void executeDelay() {
      int delayMs = 0;
      for (Delay delay: delays) {
        log.info("Delaying {}, for reason: {}", delay.delayValue, delay.reason);
        if (delay.counter.decrementAndGet() == 0) {
          delayMs += delay.delayValue;
        }
      }

      if (delayMs > 0) {
        log.info("Pausing this socket connection for {}ms...", delayMs);
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        log.info("Waking up after the delay of {}ms...", delayMs);
      }
    }

  }

  /**
   * Create a new JettySolrRunner.
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the solr home directory to use
   * @param context the context to run in
   * @param port the port to run on
   */
  public JettySolrRunner(String solrHome, String context, int port) {
    this(solrHome, JettyConfig.builder().setContext(context).setPort(port).build());
  }


  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome    the base path to run from
   * @param config the configuration
   */
  public JettySolrRunner(String solrHome, JettyConfig config) {
    this(solrHome, new Properties(), config);
  }

  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome            the solrHome to use
   * @param nodeProperties      the container properties
   * @param config         the configuration
   */
  public JettySolrRunner(String solrHome, Properties nodeProperties, JettyConfig config) {
    this(solrHome, nodeProperties, config, false);
  }

  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome            the solrHome to use
   * @param nodeProperties      the container properties
   * @param config         the configuration
   * @param enableProxy       enables proxy feature to disable connections
   */
  public JettySolrRunner(String solrHome, Properties nodeProperties, JettyConfig config, boolean enableProxy) {
    this.enableProxy = enableProxy;
    this.solrHome = solrHome;
    this.config = config;
    this.nodeProperties = nodeProperties;

    if (enableProxy) {
      try {
        proxy = new SocketProxy(0, config.sslConfig != null && config.sslConfig.isSSLMode());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      setProxyPort(proxy.getListenPort());
    }

    this.init(this.config.port);
  }

  private void init(int port) {

    QueuedThreadPool qtp = new QueuedThreadPool();
    qtp.setMaxThreads(THREAD_POOL_MAX_THREADS);
    qtp.setIdleTimeout(THREAD_POOL_MAX_IDLE_TIME_MS);
    qtp.setReservedThreads(0);
    server = new Server(qtp);
    server.manage(qtp);
    server.setStopAtShutdown(config.stopAtShutdown);

    if (System.getProperty("jetty.testMode") != null) {
      // if this property is true, then jetty will be configured to use SSL
      // leveraging the same system properties as java to specify
      // the keystore/truststore if they are set unless specific config
      // is passed via the constructor.
      //
      // This means we will use the same truststore, keystore (and keys) for
      // the server as well as any client actions taken by this JVM in
      // talking to that server, but for the purposes of testing that should
      // be good enough
      final SslContextFactory.Server sslcontext = SSLConfig.createContextFactory(config.sslConfig);

      HttpConfiguration configuration = new HttpConfiguration();
      ServerConnector connector;
      if (sslcontext != null) {
        configuration.setSecureScheme("https");
        configuration.addCustomizer(new SecureRequestCustomizer());
        HttpConnectionFactory http1ConnectionFactory = new HttpConnectionFactory(configuration);

        if (config.onlyHttp1 || !Constants.JRE_IS_MINIMUM_JAVA9) {
          connector = new ServerConnector(server, new SslConnectionFactory(sslcontext,
              http1ConnectionFactory.getProtocol()),
              http1ConnectionFactory);
        } else {
          sslcontext.setCipherComparator(HTTP2Cipher.COMPARATOR);

          connector = new ServerConnector(server);
          SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(sslcontext, "alpn");
          connector.addConnectionFactory(sslConnectionFactory);
          connector.setDefaultProtocol(sslConnectionFactory.getProtocol());

          HTTP2ServerConnectionFactory http2ConnectionFactory = new HTTP2ServerConnectionFactory(configuration);

          ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory(
              http2ConnectionFactory.getProtocol(),
              http1ConnectionFactory.getProtocol());
          alpn.setDefaultProtocol(http1ConnectionFactory.getProtocol());
          connector.addConnectionFactory(alpn);
          connector.addConnectionFactory(http1ConnectionFactory);
          connector.addConnectionFactory(http2ConnectionFactory);
        }
      } else {
        if (config.onlyHttp1) {
          connector = new ServerConnector(server, new HttpConnectionFactory(configuration));
        } else {
          connector = new ServerConnector(server, new HttpConnectionFactory(configuration),
              new HTTP2CServerConnectionFactory(configuration));
        }
      }

      connector.setReuseAddress(true);
      connector.setPort(port);
      connector.setHost(config.host);
      connector.setIdleTimeout(THREAD_POOL_MAX_IDLE_TIME_MS);
      server.setConnectors(new Connector[] {connector});
      server.setSessionIdManager(new DefaultSessionIdManager(server, new Random()));
    } else {
      HttpConfiguration configuration = new HttpConfiguration();
      ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(configuration), new HTTP2CServerConnectionFactory(configuration));

      connector.setReuseAddress(true);
      connector.setPort(port);
      connector.setHost(config.host);
      connector.setIdleTimeout(THREAD_POOL_MAX_IDLE_TIME_MS);
      server.setConnectors(new Connector[] {connector});
    }

    HandlerWrapper chain;
    {
    // Initialize the servlets
    final ServletContextHandler root = new ServletContextHandler(server, config.context, ServletContextHandler.SESSIONS);

    server.addLifeCycleListener(new LifeCycle.Listener() {

      @Override
      public void lifeCycleStopping(LifeCycle arg0) {
      }

      @Override
      public void lifeCycleStopped(LifeCycle arg0) {}

      @Override
      public void lifeCycleStarting(LifeCycle arg0) {

      }

      @Override
      public void lifeCycleStarted(LifeCycle arg0) {

        jettyPort = getFirstConnectorPort();
        int port = jettyPort;
        if (proxyPort != -1) port = proxyPort;
        nodeProperties.setProperty("hostPort", Integer.toString(port));
        nodeProperties.setProperty("hostContext", config.context);

        root.getServletContext().setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
        root.getServletContext().setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, solrHome);

        log.info("Jetty properties: {}", nodeProperties);

        debugFilter = root.addFilter(DebugFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST) );
        extraFilters = new LinkedList<>();
        for (Map.Entry<Class<? extends Filter>, String> entry : config.extraFilters.entrySet()) {
          extraFilters.add(root.addFilter(entry.getKey(), entry.getValue(), EnumSet.of(DispatcherType.REQUEST)));
        }

        for (Map.Entry<ServletHolder, String> entry : config.extraServlets.entrySet()) {
          root.addServlet(entry.getKey(), entry.getValue());
        }
        dispatchFilter = root.getServletHandler().newFilterHolder(Source.EMBEDDED);
        dispatchFilter.setHeldClass(SolrDispatchFilter.class);
        dispatchFilter.setInitParameter("excludePatterns", excludePatterns);
        // Map dispatchFilter in same path as in web.xml
        root.addFilter(dispatchFilter, "/*", EnumSet.of(DispatcherType.REQUEST));

        synchronized (JettySolrRunner.this) {
          waitOnSolr = true;
          JettySolrRunner.this.notify();
        }
      }

      @Override
      public void lifeCycleFailure(LifeCycle arg0, Throwable arg1) {
        System.clearProperty("hostPort");
      }
    });
    // Default servlet as a fall-through
    root.addServlet(Servlet404.class, "/");
    chain = root;
    }

    chain = injectJettyHandlers(chain);

    if(config.enableV2) {
      RewriteHandler rwh = new RewriteHandler();
      rwh.setHandler(chain);
      rwh.setRewriteRequestURI(true);
      rwh.setRewritePathInfo(false);
      rwh.setOriginalPathAttribute("requestedPath");
      rwh.addRule(new RewritePatternRule("/api/*", "/solr/____v2"));
      chain = rwh;
    }
    GzipHandler gzipHandler = new GzipHandler();
    gzipHandler.setHandler(chain);

    gzipHandler.setMinGzipSize(23);
    gzipHandler.setCheckGzExists(false);
    gzipHandler.setCompressionLevel(-1);
    gzipHandler.setExcludedAgentPatterns(".*MSIE.6\\.0.*");
    gzipHandler.setIncludedMethods("GET");

    server.setHandler(gzipHandler);
  }

  /** descendants may inject own handler chaining it to the given root
   * and then returning that own one*/
  protected HandlerWrapper injectJettyHandlers(HandlerWrapper chain) {
    return chain;
  }


  /**
   * @return the {@link SolrDispatchFilter} for this node
   */
  public SolrDispatchFilter getSolrDispatchFilter() { return (SolrDispatchFilter) dispatchFilter.getFilter(); }

  /**
   * @return the {@link CoreContainer} for this node
   */
  public CoreContainer getCoreContainer() {
    if (getSolrDispatchFilter() == null || getSolrDispatchFilter().getCores() == null) {
      return null;
    }
    return getSolrDispatchFilter().getCores();
  }

  public String getNodeName() {
    if (getCoreContainer() == null) {
      return null;
    }
    return getCoreContainer().getZkController().getNodeName();
  }

  public boolean isRunning() {
    return server.isRunning() && dispatchFilter != null && dispatchFilter.isRunning();
  }

  public boolean isStopped() {
    return (server.isStopped() && dispatchFilter == null) || (server.isStopped() && dispatchFilter.isStopped()
        && ((QueuedThreadPool) server.getThreadPool()).isStopped());
  }

  // ------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------

  /**
   * Start the Jetty server
   *
   * If the server has been started before, it will restart using the same port
   *
   * @throws Exception if an error occurs on startup
   */
  public void start() throws Exception {
    start(true);
  }

  /**
   * Start the Jetty server
   *
   * @param reusePort when true, will start up on the same port as used by any
   *                  previous runs of this JettySolrRunner.  If false, will use
   *                  the port specified by the server's JettyConfig.
   *
   * @throws Exception if an error occurs on startup
   */
  public void start(boolean reusePort) throws Exception {
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();

    try {
      int port = reusePort && jettyPort != -1 ? jettyPort : this.config.port;
      log.info("Start Jetty (configured port={}, binding port={})", this.config.port, port);


      // if started before, make a new server
      if (startedBefore) {
        waitOnSolr = false;
        init(port);
      } else {
        startedBefore = true;
      }

      if (!server.isRunning()) {
        if (config.portRetryTime > 0) {
          retryOnPortBindFailure(config.portRetryTime, port);
        } else {
          server.start();
        }
      }
      synchronized (JettySolrRunner.this) {
        int cnt = 0;
        while (!waitOnSolr || !dispatchFilter.isRunning() || getCoreContainer() == null) {
          this.wait(100);
          if (cnt++ == 15) {
            throw new RuntimeException("Jetty/Solr unresponsive");
          }
        }
      }

      if (config.waitForLoadingCoresToFinishMs != null && config.waitForLoadingCoresToFinishMs > 0L) {
        waitForLoadingCoresToFinish(config.waitForLoadingCoresToFinishMs);
      }

      setProtocolAndHost();

      if (enableProxy) {
        if (started) {
          proxy.reopen();
        } else {
          proxy.open(getBaseUrl().toURI());
        }
      }

    } finally {
      started  = true;
      if (prevContext != null)  {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }


  private void setProtocolAndHost() {
    String protocol = null;

    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new IllegalStateException("Jetty Server has no Connectors");
    }
    ServerConnector c = (ServerConnector) conns[0];

    protocol = c.getDefaultProtocol().toLowerCase(Locale.ROOT).startsWith("ssl") ? "https" : "http";

    this.protocol = protocol;
    this.host = c.getHost();
  }

  private void retryOnPortBindFailure(int portRetryTime, int port) throws Exception, InterruptedException {
    TimeOut timeout = new TimeOut(portRetryTime, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    int tryCnt = 1;
    while (true) {
      try {
        tryCnt++;
        log.info("Trying to start Jetty on port {} try number {} ...", port, tryCnt);
        server.start();
        break;
      } catch (IOException ioe) {
        Exception e = lookForBindException(ioe);
        if (e instanceof BindException) {
          log.info("Port is in use, will try again until timeout of {}", timeout);
          server.stop();
          Thread.sleep(3000);
          if (!timeout.hasTimedOut()) {
            continue;
          }
        }

        throw e;
      }
    }
  }

  /**
   * Traverses the cause chain looking for a BindException. Returns either a bind exception
   * that was found in the chain or the original argument.
   *
   * @param ioe An IOException that might wrap a BindException
   * @return A bind exception if present otherwise ioe
   */
  Exception lookForBindException(IOException ioe) {
    Exception e = ioe;
    while(e.getCause() != null && !(e == e.getCause()) && ! (e instanceof BindException)) {
      if (e.getCause() instanceof Exception) {
        e = (Exception) e.getCause();
        if (e instanceof BindException) {
          return e;
        }
      }
    }
    return ioe;
  }

  /**
   * Stop the Jetty server
   *
   * @throws Exception if an error occurs on shutdown
   */
  public void stop() throws Exception {
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String,String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();
    try {
      Filter filter = dispatchFilter.getFilter();

      // we want to shutdown outside of jetty cutting us off
      SolrDispatchFilter sdf = getSolrDispatchFilter();
      ExecutorService customThreadPool = null;
      if (sdf != null) {
        customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("jettyShutDown"));

        sdf.closeOnDestroy(false);
//        customThreadPool.submit(() -> {
//          try {
//            sdf.close();
//          } catch (Throwable t) {
//            log.error("Error shutting down Solr", t);
//          }
//        });
        try {
          sdf.close();
        } catch (Throwable t) {
          log.error("Error shutting down Solr", t);
        }
      }

      QueuedThreadPool qtp = (QueuedThreadPool) server.getThreadPool();
      ReservedThreadExecutor rte = qtp.getBean(ReservedThreadExecutor.class);

      server.stop();

      if (server.getState().equals(Server.FAILED)) {
        filter.destroy();
        if (extraFilters != null) {
          for (FilterHolder f : extraFilters) {
            f.getFilter().destroy();
          }
        }
      }

      // stop timeout is 0, so we will interrupt right away
      while(!qtp.isStopped()) {
        qtp.stop();
        if (qtp.isStopped()) {
          Thread.sleep(50);
        }
      }

      // we tried to kill everything, now we wait for executor to stop
      qtp.setStopTimeout(Integer.MAX_VALUE);
      qtp.stop();
      qtp.join();

      if (rte != null) {
        // we try and wait for the reserved thread executor, but it doesn't always seem to work
        // so we actually set 0 reserved threads at creation

        rte.stop();

        TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        timeout.waitFor("Timeout waiting for reserved executor to stop.", ()
            -> rte.isStopped());
      }

      if (customThreadPool != null) {
        ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
      }

      do {
        try {
          server.join();
        } catch (InterruptedException e) {
          // ignore
        }
      } while (!server.isStopped());

    } finally {
      if (enableProxy) {
        proxy.close();
      }

      if (prevContext != null) {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @exception RuntimeException if there is no Connector
   */
  private int getFirstConnectorPort() {
    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new RuntimeException("Jetty Server has no Connectors");
    }
    return ((ServerConnector) conns[0]).getLocalPort();
  }


  /**
   * Returns the Local Port of the jetty Server.
   *
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort() {
    return getLocalPort(false);
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @param internalPort pass true to get the true jetty port rather than the proxy port if configured
   *
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort(boolean internalPort) {
    if (jettyPort == -1) {
      throw new IllegalStateException("You cannot get the port until this instance has started");
    }
    if (internalPort ) {
      return jettyPort;
    }
    return (proxyPort != -1) ? proxyPort : jettyPort;
  }

  /**
   * Sets the port of a local socket proxy that sits infront of this server; if set
   * then all client traffic will flow through the proxy, giving us the ability to
   * simulate network partitions very easily.
   */
  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * Returns a base URL consisting of the protocol, host, and port for a
   * Connector in use by the Jetty Server contained in this runner.
   */
  public URL getBaseUrl() {
    try {
      return new URL(protocol, host, jettyPort, config.context);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public URL getBaseURLV2(){
    try {
      return new URL(protocol, host, jettyPort, "/api");
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

  }
  /**
   * Returns a base URL consisting of the protocol, host, and port for a
   * Connector in use by the Jetty Server contained in this runner.
   */
  public URL getProxyBaseUrl() {
    try {
      return new URL(protocol, host, getLocalPort(), config.context);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public SolrClient newClient() {
    return new HttpSolrClient.Builder(getBaseUrl().toString()).build();
  }

  public SolrClient newClient(int connectionTimeoutMillis, int socketTimeoutMillis) {
    return new HttpSolrClient.Builder(getBaseUrl().toString())
        .withConnectionTimeout(connectionTimeoutMillis)
        .withSocketTimeout(socketTimeoutMillis)
        .build();
  }

  public DebugFilter getDebugFilter() {
    return (DebugFilter)debugFilter.getFilter();
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------

  /**
   * This is a stupid hack to give jetty something to attach to
   */
  public static class Servlet404 extends HttpServlet {
    @Override
    public void service(HttpServletRequest req, HttpServletResponse res)
        throws IOException {
      res.sendError(404, "Can not find: " + req.getRequestURI());
    }
  }

  /**
   * A main class that starts jetty+solr This is useful for debugging
   */
  public static void main(String[] args) {
    try {
    JettySolrRunner jetty = new JettySolrRunner(".", "/solr", 8983);
    jetty.start();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * @return the Solr home directory of this JettySolrRunner
   */
  public String getSolrHome() {
    return solrHome;
  }

  /**
   * @return this node's properties
   */
  public Properties getNodeProperties() {
    return nodeProperties;
  }

  private void waitForLoadingCoresToFinish(long timeoutMs) {
    if (dispatchFilter != null) {
      SolrDispatchFilter solrFilter = (SolrDispatchFilter) dispatchFilter.getFilter();
      CoreContainer cores = solrFilter.getCores();
      if (cores != null) {
        cores.waitForLoadingCoresToFinish(timeoutMs);
      } else {
        throw new IllegalStateException("The CoreContainer is not set!");
      }
    } else {
      throw new IllegalStateException("The dispatchFilter is not set!");
    }
  }

  static class Delay {
    final AtomicInteger counter;
    final int delayValue;
    final String reason;

    public Delay(String reason, int counter, int delay) {
      this.reason = reason;
      this.counter = new AtomicInteger(counter);
      this.delayValue = delay;
    }
  }

  public SocketProxy getProxy() {
    return proxy;
  }
}
