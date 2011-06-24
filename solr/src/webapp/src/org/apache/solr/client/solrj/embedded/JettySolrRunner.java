/**
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
import java.util.Random;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.servlet.SolrDispatchFilter;
import org.mortbay.component.LifeCycle;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.HashSessionIdManager;
import org.mortbay.log.Logger;

/**
 * Run solr using jetty
 * 
 * @since solr 1.3
 */
public class JettySolrRunner {
  Server server;

  FilterHolder dispatchFilter;

  String context;

  private String solrConfigFilename;

  private boolean waitOnSolr = false;

  public JettySolrRunner(String context, int port) {
    this.init(context, port);
  }

  public JettySolrRunner(String context, int port, String solrConfigFilename) {
    this.init(context, port);
    this.solrConfigFilename = solrConfigFilename;
  }

  private void init(String context, int port) {
    this.context = context;
    server = new Server(port);
    server.setStopAtShutdown(true);
    if (System.getProperty("jetty.testMode") != null) {
      // SelectChannelConnector connector = new SelectChannelConnector();
      // Normal SocketConnector is what solr's example server uses by default
      SocketConnector connector = new SocketConnector();
      connector.setPort(port);
      connector.setReuseAddress(true);
      server.setConnectors(new Connector[] { connector });
      server.setSessionIdManager(new HashSessionIdManager(new Random()));
    }

    // Initialize the servlets
    final Context root = new Context(server, context, Context.SESSIONS);
    server.addLifeCycleListener(new LifeCycle.Listener() {

      public void lifeCycleStopping(LifeCycle arg0) {
        System.clearProperty("hostPort");
      }

      public void lifeCycleStopped(LifeCycle arg0) {}

      public void lifeCycleStarting(LifeCycle arg0) {
        synchronized (JettySolrRunner.this) {
          waitOnSolr = true;
          JettySolrRunner.this.notify();
        }
      }

      public void lifeCycleStarted(LifeCycle arg0) {
        System.setProperty("hostPort", Integer.toString(getLocalPort()));
        if (solrConfigFilename != null)
          System.setProperty("solrconfig", solrConfigFilename);
        dispatchFilter = root.addFilter(SolrDispatchFilter.class, "*",
            Handler.REQUEST);
        if (solrConfigFilename != null)
          System.clearProperty("solrconfig");
      }

      public void lifeCycleFailure(LifeCycle arg0, Throwable arg1) {
        System.clearProperty("hostPort");
      }
    });

    // for some reason, there must be a servlet for this to get applied
    root.addServlet(Servlet404.class, "/*");

  }

  // ------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------

  public void start() throws Exception {
    start(true);
  }

  public void start(boolean waitForSolr) throws Exception {
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
  }

  public void stop() throws Exception {
    if (server.isRunning()) {
      server.stop();
      server.join();
    }
  }

  /**
   * Returns the Local Port of the first Connector found for the jetty Server.
   * 
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort() {
    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new RuntimeException("Jetty Server has no Connectors");
    }
    return conns[0].getLocalPort();
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
      JettySolrRunner jetty = new JettySolrRunner("/solr", 8983);
      jetty.start();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
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

  public boolean isDebugEnabled() {
    return debug;
  }

  public void setDebugEnabled(boolean enabled) {
    debug = enabled;
  }

  public void info(String msg, Object arg0, Object arg1) {
  }

  public void debug(String msg, Throwable th) {
  }

  public void debug(String msg, Object arg0, Object arg1) {
  }

  public void warn(String msg, Object arg0, Object arg1) {
  }

  public void warn(String msg, Throwable th) {
  }

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
}
