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

import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

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

import org.apache.solr.servlet.SolrDispatchFilter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.GzipHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;

/**
 * Run solr using jetty
 * 
 * @since solr 1.3
 */
public class JettySolrRunner {
  Server server;

  FilterHolder dispatchFilter;
  FilterHolder debugFilter;

  String context;

  private String solrConfigFilename;
  private String schemaFilename;

  private boolean waitOnSolr = false;

  private int lastPort = -1;

  private String shards;

  private String dataDir;
  
  private volatile boolean startedBefore = false;

  private String solrHome;

  private boolean stopAtShutdown;

  public static class DebugFilter implements Filter {
    public int requestsToKeep = 10;
    private AtomicLong nRequests = new AtomicLong();

    public long getTotalRequests() {
      return nRequests.get();

    }

    // TODO: keep track of certain number of last requests
    private LinkedList<HttpServletRequest> requests = new LinkedList<HttpServletRequest>();


    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      nRequests.incrementAndGet();

      /***
      HttpServletRequest req = (HttpServletRequest)servletRequest;
      HttpServletResponse resp = (HttpServletResponse)servletResponse;

      String path = req.getServletPath();
      if( req.getPathInfo() != null ) {
        // this lets you handle /update/commit when /update is a servlet
        path += req.getPathInfo();
      }
      System.out.println("###################### FILTER request " + servletRequest);
      System.out.println("\t\tgetServletPath="+req.getServletPath());
      System.out.println("\t\tgetPathInfo="+req.getPathInfo());
      ***/

      filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {
    }
  }





  public JettySolrRunner(String solrHome, String context, int port) {
    this.init(solrHome, context, port, true);
  }

  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName) {
    this.init(solrHome, context, port, true);
    this.solrConfigFilename = solrConfigFilename;
    this.schemaFilename = schemaFileName;
  }
  
  public JettySolrRunner(String solrHome, String context, int port,
      String solrConfigFilename, String schemaFileName, boolean stopAtShutdown) {
    this.init(solrHome, context, port, stopAtShutdown);
    this.solrConfigFilename = solrConfigFilename;
    this.schemaFilename = schemaFileName;
  }

  private void init(String solrHome, String context, int port, boolean stopAtShutdown) {
    this.context = context;
    server = new Server(port);

    this.solrHome = solrHome;
    this.stopAtShutdown = stopAtShutdown;
    server.setStopAtShutdown(stopAtShutdown);
    if (!stopAtShutdown) {
      server.setGracefulShutdown(0);
    }
    System.setProperty("solr.solr.home", solrHome);
    if (System.getProperty("jetty.testMode") != null) {
      SelectChannelConnector connector = new SelectChannelConnector();
      connector.setPort(port);
      connector.setReuseAddress(true);
      connector.setLowResourcesMaxIdleTime(1500);
      QueuedThreadPool threadPool = (QueuedThreadPool) connector
          .getThreadPool();
      if (threadPool != null) {
        threadPool.setMaxThreads(10000);
        threadPool.setMaxIdleTimeMs(5000);
        if (!stopAtShutdown) {
          threadPool.setMaxStopTimeMs(100);
        }
      }
      
      server.setConnectors(new Connector[] {connector});
      server.setSessionIdManager(new HashSessionIdManager(new Random()));
    } else {
      
      for (Connector connector : server.getConnectors()) {
        QueuedThreadPool threadPool = null;
        if (connector instanceof SocketConnector) {
          threadPool = (QueuedThreadPool) ((SocketConnector) connector)
              .getThreadPool();
        }
        if (connector instanceof SelectChannelConnector) {
          threadPool = (QueuedThreadPool) ((SelectChannelConnector) connector)
              .getThreadPool();
        }
        
        if (threadPool != null) {
          threadPool.setMaxThreads(10000);
          threadPool.setMaxIdleTimeMs(5000);
          if (!stopAtShutdown) {
            threadPool.setMaxStopTimeMs(100);
          }
        }
        
      }

    }

    // Initialize the servlets
    final ServletContextHandler root = new ServletContextHandler(server,context,ServletContextHandler.SESSIONS);
    root.setHandler(new GzipHandler());
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
//        SolrDispatchFilter filter = new SolrDispatchFilter();
//        FilterHolder fh = new FilterHolder(filter);
        debugFilter = root.addFilter(DebugFilter.class, "*", EnumSet.of(DispatcherType.REQUEST) );
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
      init(solrHome, context, lastPort, stopAtShutdown);
    } else {
      startedBefore = true;
    }
    
    if( dataDir != null) {
      System.setProperty("solr.data.dir", dataDir);
    }
    if(shards != null) {
      System.setProperty("shard", shards);
    }
    
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
    
    System.clearProperty("shard");
    System.clearProperty("solr.data.dir");
  }

  public void stop() throws Exception {
    // we try and do a bunch of extra stop stuff because
    // jetty doesn't like to stop if it started
    // and ended up in a failure state (like when it cannot get the port)
    if (server.getState().equals(Server.FAILED)) {
      Connector[] connectors = server.getConnectors();
      for (Connector connector : connectors) {
        connector.stop();
      }
    }
    Filter filter = dispatchFilter.getFilter();
    ThreadPool threadPool = server.getThreadPool();
    server.getServer().stop();
    server.stop();
    if (threadPool instanceof QueuedThreadPool) {
      ((QueuedThreadPool) threadPool).setMaxStopTimeMs(30000);
      ((QueuedThreadPool) threadPool).stop();
      ((QueuedThreadPool) threadPool).join();
    }
    //server.destroy();
    if (server.getState().equals(Server.FAILED)) {
      filter.destroy();
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
    return conns[0].getLocalPort();
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
    return lastPort;
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
