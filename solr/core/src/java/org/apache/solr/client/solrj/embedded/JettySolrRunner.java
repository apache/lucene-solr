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

import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.apache.solr.common.util.SolrScheduledExecutorScheduler;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
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
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.Source;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.BindException;
import java.net.URI;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Run solr using jetty
 *
 * @since solr 1.3
 */
public class JettySolrRunner implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // NOTE: should be larger than HttpClientUtil.DEFAULT_SO_TIMEOUT or typical client SO timeout
  private static final int THREAD_POOL_MAX_IDLE_TIME_MS = 60000;

  final Server server;
  private final ServerConnector connector;

  volatile FilterHolder dispatchFilter;
  volatile FilterHolder debugFilter;
  volatile FilterHolder qosFilter;

  private volatile int jettyPort = -1;

  private final JettyConfig config;
  private final String solrHome;
  private final Properties nodeProperties;

  private volatile boolean startedBefore = false;

  private LinkedList<FilterHolder> extraFilters;

  private static final String excludePatterns = "/partials/.+,/libs/.+,/css/.+,/js/.+,/img/.+,/templates/.+,/tpl/.+";

  private volatile int proxyPort = -1;

  private final boolean enableProxy;

  private SocketProxy proxy;

  private String protocol;

  private String host;

  private volatile boolean started = false;
  private volatile String nodeName;
  private volatile boolean isClosed;


  private final Scheduler scheduler;
  private final SolrQueuedThreadPool qtp;
  private volatile boolean closed;

  public String getContext() {
    return config.context;
  }

  public static class DebugFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicLong nRequests = new AtomicLong();

    private Set<Delay> delays = ConcurrentHashMap.newKeySet(12);

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
        this.log.info("Pausing this socket connection for {}ms...", delayMs);
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          SolrZkClient.checkInterrupted(e);
          throw new RuntimeException(e);
        }
        this.log.info("Waking up after the delay of {}ms...", delayMs);
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
    assert ObjectReleaseTracker.track(this);

    SecurityManager s = System.getSecurityManager();
    ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    scheduler = new SolrScheduledExecutorScheduler("jetty-scheduler", null, group);

    this.enableProxy = enableProxy;
    this.solrHome = solrHome;
    this.config = config;
    this.nodeProperties = nodeProperties;
    nodeProperties.setProperty("hostContext", config.context);

    if (enableProxy || config.enableProxy) {
      try {
        proxy = new SocketProxy(0, config.sslConfig != null && config.sslConfig.isSSLMode());
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new RuntimeException(e);
      }
      setProxyPort(proxy.getListenPort());
    }

    int port = this.config.port;

    if (config.qtp != null) {
      qtp = config.qtp;
    } else {
      // leave as match with prod setup
      qtp = new SolrQueuedThreadPool("JettySolrRunner qtp");
    }

    server = new Server(qtp);

    server.setStopTimeout(0); // will wait gracefull for stoptime / 2, then interrupts

    server.setStopAtShutdown(false);

    //if (System.getProperty("jetty.testMode") != null) {

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
    configuration.setRequestHeaderSize(16 * 1024);
    configuration.setResponseHeaderSize(16 * 1024);

    // https://github.com/jersey/jersey/issues/3691
    // https://github.com/eclipse/jetty.project/issues/1891
    configuration.setNotifyRemoteAsyncErrors(false);

    if (sslcontext != null) {
      configuration.setSecureScheme("https");
      configuration.addCustomizer(new SecureRequestCustomizer());
      configuration.setIdleTimeout(TimeUnit.MINUTES.toMillis(10));
      HttpConnectionFactory http1ConnectionFactory = new HttpConnectionFactory(configuration);

      if (config.onlyHttp1 || !Constants.JRE_IS_MINIMUM_JAVA9) {
        connector = new ServerConnector(server, qtp, scheduler, null, 1, 2, new SslConnectionFactory(sslcontext, http1ConnectionFactory.getProtocol()), http1ConnectionFactory);
      } else {
        sslcontext.setCipherComparator(HTTP2Cipher.COMPARATOR);

        connector = new ServerConnector(server, qtp, scheduler, null, 1, 2);
        SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(sslcontext, "alpn");
        connector.addConnectionFactory(sslConnectionFactory);
        connector.setDefaultProtocol(sslConnectionFactory.getProtocol());

        HTTP2ServerConnectionFactory http2ConnectionFactory = new HTTP2ServerConnectionFactory(configuration);

        http2ConnectionFactory.setMaxConcurrentStreams(1024);
        http2ConnectionFactory.setInputBufferSize(8192);
        http2ConnectionFactory.setStreamIdleTimeout(TimeUnit.MINUTES.toMillis(10));

        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory(http2ConnectionFactory.getProtocol(), http1ConnectionFactory.getProtocol());
        alpn.setDefaultProtocol(http1ConnectionFactory.getProtocol());
        connector.addConnectionFactory(alpn);
        connector.addConnectionFactory(http1ConnectionFactory);
        connector.addConnectionFactory(http2ConnectionFactory);
      }
    } else {
      if (config.onlyHttp1) {
        connector = new ServerConnector(server, qtp, scheduler, null, 1, 2, new HttpConnectionFactory(configuration));
      } else {
        connector = new ServerConnector(server, qtp, scheduler, null, 1, 2, new HttpConnectionFactory(configuration), new HTTP2CServerConnectionFactory(configuration));
      }
    }
    connector.setIdleTimeout(TimeUnit.MINUTES.toMillis(10));
    connector.setReuseAddress(true);
    connector.setPort(port);
    connector.setHost("127.0.0.1");

    server.setConnectors(new Connector[] {connector});


    //server.setDumpAfterStart(true);
    //server.setDumpBeforeStop(true);

    HandlerWrapper chain;
    {
      // Initialize the servlets
      final ServletContextHandler root = new ServletContextHandler(server, config.context, ServletContextHandler.NO_SESSIONS);

      server.addLifeCycleListener(new LifeCycle.Listener() {

        @Override
        public void lifeCycleStopping(LifeCycle arg0) {
        }

        @Override
        public void lifeCycleStopped(LifeCycle arg0) {
        }

        @Override
        public void lifeCycleStarting(LifeCycle arg0) {
          if (startedBefore) {
            root.getServletContext().setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
            root.getServletContext().setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, solrHome);
          }
        }

        @Override
        public void lifeCycleStarted(LifeCycle arg0) {

          if (!startedBefore) {
            startedBefore = true;

            root.getServletContext().setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
            root.getServletContext().setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, solrHome);
            root.getServletContext().setAttribute(SolrDispatchFilter.INIT_CALL, (Runnable) () -> {
              jettyPort = getFirstConnectorPort();
              int port1 = jettyPort;
              if (proxyPort != -1) port1 = proxyPort;
              nodeProperties.setProperty("hostPort", String.valueOf(port1));

            });

            debugFilter = root.addFilter(DebugFilter.class, "*", EnumSet.of(DispatcherType.REQUEST));
            extraFilters = new LinkedList<>();
            for (Map.Entry<Class<? extends Filter>,String> entry : config.extraFilters.entrySet()) {
              extraFilters.add(root.addFilter(entry.getKey(), entry.getValue(), EnumSet.of(DispatcherType.REQUEST)));
            }

            for (Map.Entry<ServletHolder,String> entry : config.extraServlets.entrySet()) {
              root.addServlet(entry.getKey(), entry.getValue());
            }

           // qosFilter = root.getServletHandler().newFilterHolder(Source.EMBEDDED);
           // qosFilter.setHeldClass(SolrQoSFilter.class);
           // qosFilter.setAsyncSupported(true);

            dispatchFilter = root.getServletHandler().newFilterHolder(Source.EMBEDDED);
            dispatchFilter.setHeldClass(SolrDispatchFilter.class);
            dispatchFilter.setInitParameter("excludePatterns", excludePatterns);

            root.addServlet(Servlet404.class, "/*");

            // Map dispatchFilter in same path as in web.xml
            dispatchFilter.setAsyncSupported(true);
           // root.addFilter(qosFilter, "*", EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));
            root.addFilter(dispatchFilter, "*", EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));

            if (log.isDebugEnabled()) log.debug("Jetty loaded and ready to go");
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

    // no listener for jettysolrrunner, it requires one jetty per jvm
    //server.addLifeCycleListener(new SolrLifcycleListener());

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

    gzipHandler.setMinGzipSize(23); // https://github.com/eclipse/jetty.project/issues/4191
    gzipHandler.setCheckGzExists(false);
    gzipHandler.setCompressionLevel(-1);
    gzipHandler.setExcludedAgentPatterns(".*MSIE.6\\.0.*");
    gzipHandler.setIncludedMethods("GET");

    server.setHandler(gzipHandler);
    // ShutdownThread.deregister(server);
  }


  /** descendants may inject own handler chaining it to the given root
   * and then returning that own one*/
  protected HandlerWrapper injectJettyHandlers(HandlerWrapper chain) {
    return chain;
  }

  @Override
  public String toString() {
    return "JettySolrRunner: " + jettyPort;
  }

  /**
   * @return the {@link SolrDispatchFilter} for this node
   */
  public SolrDispatchFilter getSolrDispatchFilter() { return dispatchFilter == null ? null : (SolrDispatchFilter) dispatchFilter.getFilter(); }

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
    return nodeName;
  }

  public boolean isRunning() {
    return server.isRunning();
  }

  public boolean isStopped() {
    return (server.isStopped() && dispatchFilter == null) || (server.isStopped() && dispatchFilter.isStopped());
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
    start(true, true);
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
  public void start(boolean reusePort, boolean wait) throws Exception {
    closed = false;
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();

    try {
      int port = reusePort && jettyPort != -1 ? jettyPort : this.config.port;
      log.info("Start Jetty (configured port={}, binding port={})", this.config.port, port);
      if (startedBefore) {
        connector.setPort(port);
      }

      // if !started before, make a new server

      boolean success = false;
      if (!server.isRunning()) {
        if (config.portRetryTime > 0) {
          retryOnPortBindFailure(port);
        } else {
          server.start();
        }
      }

      if (getCoreContainer() != null && getCoreContainer().isZooKeeperAware()) {
        nodeName = getCoreContainer().getZkController().getNodeName();
        if (nodeName == null) throw new IllegalStateException();
      }

      setProtocolAndHost();

      if (enableProxy) {
        if (started) {
          proxy.reopen();
        } else {
          proxy.open(new URI(getBaseUrl()));
        }
      }

      if (getCoreContainer() != null && System.getProperty("zkHost") != null && wait) {
        SolrZkClient zkClient = getCoreContainer().getZkController().getZkClient();
        CountDownLatch latch = new CountDownLatch(1);

        ClusterReadyWatcher watcher = new ClusterReadyWatcher(latch, zkClient);
        try {
          Stat stat = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE, watcher);
          if (stat == null) {
            log.info("Collections znode not found, waiting on latch");
            try {
              success = latch.await(10000, TimeUnit.MILLISECONDS);
              if (!success) {
                log.warn("Timedout waiting to see {} node in zk", ZkStateReader.COLLECTIONS_ZKNODE);
              }
              if (log.isDebugEnabled()) log.debug("Done waiting on latch");
            } catch (InterruptedException e) {
              ParWork.propagateInterrupt(e);
              throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
            }
          }
        } catch (KeeperException e) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
        } finally {
          IOUtils.closeQuietly(watcher);
        }
// if we need this, us client, not reader
//        log.info("waitForNode: {}", getNodeName());
//
//        ZkStateReader reader = getCoreContainer().getZkController().getZkStateReader();
//
//        reader.waitForLiveNodes(30, TimeUnit.SECONDS, (n) -> n != null && getNodeName() != null && n.contains(getNodeName()));
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

  private void retryOnPortBindFailure(int port) throws Exception {
    int tryCnt = 1;
    while (tryCnt < 3) {
      try {
        if (log.isDebugEnabled()) log.debug(" {} try number {} ...", port, tryCnt);
        server.start();
        break;
      } catch (IOException ioe) {
        Exception e = lookForBindException(ioe);
        if (e instanceof BindException) {
          log.info("Port is in use, will try again");
          server.stop();

          tryCnt++;
          continue;
        }

        throw e;
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

  @Override
  public void close() throws IOException {
    close(true);
  }

  public void close(boolean wait) throws IOException {
    if (closed) return;
    closed = true;

    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String,String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();
    CoreContainer coreContainer = getCoreContainer();
    try {

      try {
        server.stop();
      } catch (Exception e) {
        log.error("Error stopping jetty server", e);
      }

      scheduler.stop();

      if (config.qtp == null) {
        qtp.close();
      }

      try {
        server.join();
      } catch (InterruptedException e) {
        SolrZkClient.checkInterrupted(e);
        throw new RuntimeException(e);
      }

//      if (wait && coreContainer != null && coreContainer
//          .isZooKeeperAware()) {
//        log.info("waitForJettyToStop: {}", getLocalPort());
//        String nodeName = getNodeName();
//        if (nodeName == null) {
//          log.info("Cannot wait for Jetty with null node name");
//        } else {
//
//          log.info("waitForNode: {}", getNodeName());
//
//          ZkStateReader reader = coreContainer.getZkController().getZkStateReader();
//
//          try {
//            if (!reader.isClosed() && reader.getZkClient().isConnected()) {
//              reader.waitForLiveNodes(10, TimeUnit.SECONDS, (n) -> !n.contains(nodeName));
//            }
//          } catch (InterruptedException e) {
//            ParWork.propagateInterrupt(e);
//          } catch (TimeoutException e) {
//            log.error("Timeout waiting for live node");
//          }
//        }
//      }

    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      log.error("", e);
      throw new RuntimeException(e);
    } finally {

      if (enableProxy) {
        proxy.close();
      }

      assert ObjectReleaseTracker.release(this);
      if (prevContext != null) {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Stop the Jetty server
   *
   * @throws Exception if an error occurs on shutdown
   */
  public void stop() throws Exception {
    stop(true);
  }

  public void stop(boolean wait) throws Exception {
    close(wait);
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
  public String getBaseUrl() {
      return protocol +"://" + host + ":" + jettyPort + config.context;
  }

  public String getBaseURLV2(){
    return protocol +"://" + host + ":" + jettyPort + "/api";
  }
  /**
   * Returns a base URL consisting of the protocol, host, and port for a
   * Connector in use by the Jetty Server contained in this runner.
   */
  public String getProxyBaseUrl() {
    return protocol +"://" + host + ":" + getLocalPort() + config.context;
  }

  public SolrClient newClient() {
    return new Http2SolrClient.Builder(getBaseUrl().toString()).
            withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient()).build();
  }

  public SolrClient newClient(String collection) {
    return new Http2SolrClient.Builder(getBaseUrl() + "/" + collection).
        withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient()).build();
  }


  public SolrClient newHttp1Client() {
    return new HttpSolrClient.Builder(getBaseUrl()).
        withHttpClient(getCoreContainer().getUpdateShardHandler().getDefaultHttpClient()).build();
  }

  public SolrClient newClient(int connectionTimeoutMillis, int socketTimeoutMillis) {
    return new Http2SolrClient.Builder(getBaseUrl().toString())
        .connectionTimeout(connectionTimeoutMillis)
        .idleTimeout(socketTimeoutMillis)
        .withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient())
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
      res.sendError(404, "Can not find (404 from jetty Solr Runner): " + req.getRequestURI());
    }
  }

  /**
   * A main class that starts jetty+solr This is useful for debugging
   */
  public static void main(String[] args) throws Exception {
    JettySolrRunner jetty = new JettySolrRunner(".", "/solr", 8983);
    jetty.start();
  }

  /**
   * @return the Solr home directory of this JettySolrRunner
   */
  public String getSolrHome() {
    return solrHome;
  }

  public String getHost() {
    return host;
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

  private static class ClusterReadyWatcher implements Watcher, Closeable {

    private final CountDownLatch latch;
    private final SolrZkClient zkClient;

    public ClusterReadyWatcher(CountDownLatch latch, SolrZkClient zkClient) {
      this.latch = latch;
      this.zkClient = zkClient;
    }

    @Override
    public void process(WatchedEvent event) {
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }   log.info("Got event on live node watcher {}", event.toString());
      if (event.getType() == Event.EventType.NodeCreated) {
        latch.countDown();
      } else {
        try {
          Stat stat = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE, this);
          if (stat != null) {
            latch.countDown();
          }
        } catch (KeeperException e) {
          SolrException.log(log, e);
          return;
        } catch (InterruptedException e) {
          log.info("interrupted");
          return;
        }
      }
    }

    @Override
    public void close() throws IOException {
      try {
        zkClient.getSolrZooKeeper().removeWatches(ZkStateReader.COLLECTIONS_ZKNODE, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }
}
