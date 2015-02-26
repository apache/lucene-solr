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

import org.apache.solr.servlet.SolrDispatchFilter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Run solr using jetty
 * 
 * @since solr 1.3
 */
public class JettySolrRunner {

  private static final AtomicLong JETTY_ID_COUNTER = new AtomicLong();

  Server server;

  FilterHolder dispatchFilter;
  FilterHolder debugFilter;

  private String solrConfigFilename;
  private String schemaFilename;
  private final String coreRootDirectory;

  private boolean waitOnSolr = false;

  private int lastPort = -1;
  private final JettyConfig config;

  private String shards;

  private String dataDir;
  private String solrUlogDir;
  
  private volatile boolean startedBefore = false;

  private final String solrHome;

  private String coreNodeName;

  private final String name;

  private LinkedList<FilterHolder> extraFilters;
  
  private int proxyPort = -1;

  public static class DebugFilter implements Filter {
    public int requestsToKeep = 10;
    private AtomicLong nRequests = new AtomicLong();

    public long getTotalRequests() {
      return nRequests.get();

    }

    // TODO: keep track of certain number of last requests
    private LinkedList<HttpServletRequest> requests = new LinkedList<>();


    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      nRequests.incrementAndGet();
      filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {
    }
  }

  public JettySolrRunner(String solrHome, String context, int port) {
    this(solrHome, JettyConfig.builder().setContext(context).setPort(port).build());
  }

  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName) {
    this(solrHome, solrConfigFilename, schemaFileName, JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .build());
  }

  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port,
      String solrConfigFilename, String schemaFileName, boolean stopAtShutdown) {
    this(solrHome, solrConfigFilename, schemaFileName, JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .build());
  }

  /**
   * Constructor taking an ordered list of additional (servlet holder -&gt; path spec) mappings
   * to add to the servlet context
   * @deprecated use {@link #JettySolrRunner(String,String,String,JettyConfig)}
   */
  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port,
      String solrConfigFilename, String schemaFileName, boolean stopAtShutdown,
      SortedMap<ServletHolder,String> extraServlets) {
    this(solrHome, solrConfigFilename, schemaFileName, JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .build());
  }

  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName,
                         boolean stopAtShutdown, SortedMap<ServletHolder, String> extraServlets, SSLConfig sslConfig) {
    this(solrHome, solrConfigFilename, schemaFileName, JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .withSSLConfig(sslConfig)
        .build());
  }

  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName,
                         boolean stopAtShutdown, SortedMap<ServletHolder, String> extraServlets, SSLConfig sslConfig,
                         SortedMap<Class<? extends Filter>, String> extraRequestFilters) {
    this(solrHome, solrConfigFilename, schemaFileName, JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .withFilters(extraRequestFilters)
        .withSSLConfig(sslConfig)
        .build());
  }

  /**
   * Construct a JettySolrRunner
   *
   * @param solrHome    the base path to run from
   * @param jettyConfig the configuration
   */
  public JettySolrRunner(String solrHome, JettyConfig jettyConfig) {
    this(solrHome, null, null, jettyConfig);
  }

  /**
   * Construct a JettySolrRunner
   *
   * @param solrHome            the base path to run from
   * @param solrConfigFilename  the name of the solrconfig file to use
   * @param schemaFileName      the name of the schema file to use
   * @param jettyConfig         the configuration
   */
  public JettySolrRunner(String solrHome, String solrConfigFilename, String schemaFileName, JettyConfig jettyConfig) {

    this.solrConfigFilename = solrConfigFilename;
    this.schemaFilename = schemaFileName;

    this.name = "jetty-" + JETTY_ID_COUNTER.incrementAndGet();
    this.coreRootDirectory = System.getProperty("coreRootDirectory", null);

    this.config = jettyConfig;
    this.solrHome = solrHome;

    this.init(this.config.port);
  }
  
  private void init(int port) {

    System.setProperty("solr.solr.home", solrHome);

    QueuedThreadPool qtp = new QueuedThreadPool();
    qtp.setMaxThreads(10000);
    qtp.setIdleTimeout((int) TimeUnit.SECONDS.toMillis(5));
    qtp.setStopTimeout((int) TimeUnit.MINUTES.toMillis(1));
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
      final SslContextFactory sslcontext = SSLConfig.createContextFactory(config.sslConfig);

      ServerConnector connector;
      if (sslcontext != null) {
        HttpConfiguration configuration = new HttpConfiguration();
        configuration.setSecureScheme("https");
        configuration.addCustomizer(new SecureRequestCustomizer());
        connector = new ServerConnector(server, new SslConnectionFactory(sslcontext, "http/1.1"),
            new HttpConnectionFactory(configuration));
      } else {
        connector = new ServerConnector(server, new HttpConnectionFactory());
      }

      connector.setReuseAddress(true);
      connector.setSoLingerTime(0);
      connector.setPort(port);
      connector.setHost("127.0.0.1");

      // Enable Low Resources Management
      LowResourceMonitor lowResources = new LowResourceMonitor(server);
      lowResources.setLowResourcesIdleTimeout(1500);
      lowResources.setMaxConnections(10000);
      server.addBean(lowResources);

      server.setConnectors(new Connector[] {connector});
      server.setSessionIdManager(new HashSessionIdManager(new Random()));
    } else {
      ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory());
      connector.setPort(port);
      server.setConnectors(new Connector[] {connector});
    }

    // Initialize the servlets
    final ServletContextHandler root = new ServletContextHandler(server, config.context, ServletContextHandler.SESSIONS);
    root.addFilter(GzipFilter.class, "*", EnumSet.of(DispatcherType.REQUEST));
    server.addLifeCycleListener(new LifeCycle.Listener() {

      @Override
      public void lifeCycleStopping(LifeCycle arg0) {
        System.clearProperty("hostPort");
      }

      @Override
      public void lifeCycleStopped(LifeCycle arg0) {}

      @Override
      public void lifeCycleStarting(LifeCycle arg0) {
        synchronized (JettySolrRunner.this) {
          waitOnSolr = true;
          JettySolrRunner.this.notify();
        }
      }

      @Override
      public void lifeCycleStarted(LifeCycle arg0) {

        lastPort = getFirstConnectorPort();

        System.setProperty("hostPort", Integer.toString(lastPort));
        if (solrConfigFilename != null) System.setProperty("solrconfig",
            solrConfigFilename);
        if (schemaFilename != null) System.setProperty("schema", 
            schemaFilename);
        if (coreRootDirectory != null)
          System.setProperty("coreRootDirectory", coreRootDirectory);

        debugFilter = root.addFilter(DebugFilter.class, "*", EnumSet.of(DispatcherType.REQUEST) );
        extraFilters = new LinkedList<>();
        for (Class<? extends Filter> filterClass : config.extraFilters.keySet()) {
          extraFilters.add(root.addFilter(filterClass, config.extraFilters.get(filterClass),
              EnumSet.of(DispatcherType.REQUEST)));
        }

        for (ServletHolder servletHolder : config.extraServlets.keySet()) {
          String pathSpec = config.extraServlets.get(servletHolder);
          root.addServlet(servletHolder, pathSpec);
        }

        dispatchFilter = root.addFilter(SolrDispatchFilter.class, "*", EnumSet.of(DispatcherType.REQUEST) );

        if (solrConfigFilename != null) System.clearProperty("solrconfig");
        if (schemaFilename != null) System.clearProperty("schema");
        System.clearProperty("solr.solr.home");
      }

      @Override
      public void lifeCycleFailure(LifeCycle arg0, Throwable arg1) {
        System.clearProperty("hostPort");
      }
    });

    // for some reason, there must be a servlet for this to get applied
    root.addServlet(Servlet404.class, "/*");

  }



  public FilterHolder getDispatchFilter() {
    return dispatchFilter;
  }

  public boolean isRunning() {
    return server.isRunning();
  }
  
  public boolean isStopped() {
    return server.isStopped();
  }

  // ------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------

  public void start() throws Exception {
    start(true);
  }

  public void start(boolean waitForSolr) throws Exception {
    // if started before, make a new server
    if (startedBefore) {
      waitOnSolr = false;
      init(lastPort);
    } else {
      startedBefore = true;
    }
    
    if (dataDir != null) {
      System.setProperty("solr.data.dir", dataDir);
    }
    if (solrUlogDir != null) {
      System.setProperty("solr.ulog.dir", solrUlogDir);
    }
    if (shards != null) {
      System.setProperty("shard", shards);
    }
    if (coreNodeName != null) {
      System.setProperty("coreNodeName", coreNodeName);
    }
    try {
      
      if (!server.isRunning()) {
        server.start();
      }
      synchronized (JettySolrRunner.this) {
        int cnt = 0;
        while (!waitOnSolr) {
          this.wait(100);
          if (cnt++ == 5) {
            throw new RuntimeException("Jetty/Solr unresponsive");
          }
        }
      }
    } finally {
      
      System.clearProperty("shard");
      System.clearProperty("solr.data.dir");
      System.clearProperty("coreNodeName");
      System.clearProperty("solr.ulog.dir");
    }
    
  }

  public void stop() throws Exception {

    Filter filter = dispatchFilter.getFilter();

    server.stop();

    //server.destroy();
    if (server.getState().equals(Server.FAILED)) {
      filter.destroy();
      if (extraFilters != null) {
        for (FilterHolder f : extraFilters) {
          f.getFilter().destroy();
        }
      }
    }
    
    server.join();
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
    return (proxyPort != -1) ? proxyPort : ((ServerConnector) conns[0]).getLocalPort();
  }
  
  /**
   * Returns the Local Port of the jetty Server.
   * 
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort() {
    if (lastPort == -1) {
      throw new IllegalStateException("You cannot get the port until this instance has started");
    }
    return (proxyPort != -1) ? proxyPort : lastPort;
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
    String protocol = null;
    try {
      Connector[] conns = server.getConnectors();
      if (0 == conns.length) {
        throw new IllegalStateException("Jetty Server has no Connectors");
      }
      ServerConnector c = (ServerConnector) conns[0];
      if (c.getLocalPort() < 0) {
        throw new IllegalStateException("Jetty Connector is not open: " + 
                                        c.getLocalPort());
      }
      protocol = c.getDefaultProtocol().equals("SSL-http/1.1")  ? "https" : "http";
      return new URL(protocol, c.getHost(), c.getLocalPort(), config.context);

    } catch (MalformedURLException e) {
      throw new  IllegalStateException
        ("Java could not make sense of protocol: " + protocol, e);
    }
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

  public void setShards(String shardList) {
     this.shards = shardList;
  }

  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }
  
  public void setUlogDir(String ulogDir) {
    this.solrUlogDir = ulogDir;
  }

  public void setCoreNodeName(String coreNodeName) {
    this.coreNodeName = coreNodeName;
  }

  public String getSolrHome() {
    return solrHome;
  }
}

class NoLog implements Logger {
  private static boolean debug = System.getProperty("DEBUG", null) != null;

  private final String name;

  public NoLog() {
    this(null);
  }

  public NoLog(String name) {
    this.name = name == null ? "" : name;
  }

  @Override
  public boolean isDebugEnabled() {
    return debug;
  }

  @Override
  public void setDebugEnabled(boolean enabled) {
    debug = enabled;
  }

  @Override
  public void debug(String msg, Throwable th) {
  }

  @Override
  public Logger getLogger(String name) {
    if ((name == null && this.name == null)
        || (name != null && name.equals(this.name)))
      return this;
    return new NoLog(name);
  }

  @Override
  public String toString() {
    return "NOLOG[" + name + "]";
  }

  @Override
  public void debug(Throwable arg0) {
    
  }

  @Override
  public void debug(String arg0, Object... arg1) {
    
  }

  @Override
  public void debug(String s, long l) {

  }

  @Override
  public String getName() {
    return toString();
  }

  @Override
  public void ignore(Throwable arg0) {
    
  }

  @Override
  public void info(Throwable arg0) {
    
  }

  @Override
  public void info(String arg0, Object... arg1) {
    
  }

  @Override
  public void info(String arg0, Throwable arg1) {
    
  }

  @Override
  public void warn(Throwable arg0) {
    
  }

  @Override
  public void warn(String arg0, Object... arg1) {
    
  }

  @Override
  public void warn(String arg0, Throwable arg1) {
  }
}
