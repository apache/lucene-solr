package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.JMException;

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AtomicLongMap;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkTestServer {
  public static final int TICK_TIME = 1000;

  private static Logger log = LoggerFactory.getLogger(ZkTestServer.class);
  
  protected final ZKServerMain zkServer = new ZKServerMain();

  private String zkDir;

  private int clientPort;

  private volatile Thread zooThread;
  
  private int theTickTime = TICK_TIME;

  static public enum LimitViolationAction {
    IGNORE,
    REPORT,
    FAIL,
  }

  class ZKServerMain {

    private ServerCnxnFactory cnxnFactory;
    private ZooKeeperServer zooKeeperServer;
    private LimitViolationAction violationReportAction = LimitViolationAction.REPORT;
    private WatchLimiter limiter = new WatchLimiter(1, LimitViolationAction.IGNORE);

    protected void initializeAndRun(String[] args) throws ConfigException,
        IOException {
      try {
        ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
        log.warn("Unable to register log4j JMX control", e);
      }
      
      ServerConfig config = new ServerConfig();
      if (args.length == 1) {
        config.parse(args[0]);
      } else {
        config.parse(args);
      }
      
      runFromConfig(config);
    }

    private class WatchLimit {
      private long limit;
      private final String desc;

      private LimitViolationAction action;
      private AtomicLongMap<String> counters = AtomicLongMap.create();
      private ConcurrentHashMap<String,Long> maxCounters = new ConcurrentHashMap<>();

      WatchLimit(long limit, String desc, LimitViolationAction action) {
        this.limit = limit;
        this.desc = desc;
        this.action = action;
      }

      public void setAction(LimitViolationAction action) {
        this.action = action;
      }

      public void setLimit(long limit) {
        this.limit = limit;
      }

      public void updateForWatch(String key, Watcher watcher) {
        if (watcher != null) {
          log.debug("Watch added: {}: {}", desc, key);
          long count = counters.incrementAndGet(key);
          Long lastCount = maxCounters.get(key);
          if (lastCount == null || count > lastCount) {
            maxCounters.put(key, count);
          }
          if (count > limit && action != LimitViolationAction.IGNORE) {
            String msg = "Number of watches created in parallel for data: " + key +
                ", type: " + desc + " exceeds limit (" + count + " > " + limit + ")";
            log.warn("{}", msg);
            if (action == LimitViolationAction.FAIL) throw new AssertionError(msg);
          }
        }
      }

      public void updateForFire(WatchedEvent event) {
        log.debug("Watch fired: {}: {}", desc, event.getPath());
        counters.decrementAndGet(event.getPath());
      }

      private String reportLimitViolations() {
        Object[] maxKeys = maxCounters.keySet().toArray();
        Arrays.sort(maxKeys, new Comparator<Object>() {
          private final Comparator<Long> valComp = Ordering.natural().reverse();
          @Override
          public int compare(Object o1, Object o2) {
            return valComp.compare(maxCounters.get(o1), maxCounters.get(o2));
          }
        });

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object key : maxKeys) {
          long value = maxCounters.get(key);
          if (value <= limit) continue;
          if (first) {
            sb.append("\nMaximum concurrent ").append(desc).append(" watches above limit:\n\n");
            first = false;
          }
          sb.append("\t").append(maxCounters.get(key)).append('\t').append(key).append('\n');
        }
        return sb.toString();
      }
    }

    public class WatchLimiter {
      WatchLimit statLimit;
      WatchLimit dataLimit;
      WatchLimit childrenLimit;

      private WatchLimiter (long limit, LimitViolationAction action) {
        statLimit = new WatchLimit(limit, "create/delete", action);
        dataLimit = new WatchLimit(limit, "data", action);
        childrenLimit = new WatchLimit(limit, "children", action);
      }

      public void setAction(LimitViolationAction action) {
        statLimit.setAction(action);
        dataLimit.setAction(action);
        childrenLimit.setAction(action);
      }

      public void setLimit(long limit) {
        statLimit.setLimit(limit);
        dataLimit.setLimit(limit);
        childrenLimit.setLimit(limit);
      }

      public String reportLimitViolations() {
        return statLimit.reportLimitViolations() +
            dataLimit.reportLimitViolations() +
            childrenLimit.reportLimitViolations();
      }

      private void updateForFire(WatchedEvent event) {
        switch (event.getType()) {
          case None:
            break;
          case NodeCreated:
          case NodeDeleted:
            statLimit.updateForFire(event);
            break;
          case NodeDataChanged:
            dataLimit.updateForFire(event);
            break;
          case NodeChildrenChanged:
            childrenLimit.updateForFire(event);
            break;
        }
      }
    }

    private class TestServerCnxn extends NIOServerCnxn {

      private final WatchLimiter limiter;

      public TestServerCnxn(ZooKeeperServer zk, SocketChannel sock, SelectionKey sk,
                            NIOServerCnxnFactory factory, WatchLimiter limiter) throws IOException {
        super(zk, sock, sk, factory);
        this.limiter = limiter;
      }

      @Override
      public synchronized void process(WatchedEvent event) {
        limiter.updateForFire(event);
        super.process(event);
      }
    }

    private class TestServerCnxnFactory extends NIOServerCnxnFactory {

      private final WatchLimiter limiter;

      public TestServerCnxnFactory(WatchLimiter limiter) throws IOException {
        super();
        this.limiter = limiter;
      }

      @Override
      protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk) throws IOException {
        return new TestServerCnxn(zkServer, sock, sk, this, limiter);
      }
    }

    private class TestZKDatabase extends ZKDatabase {

      private final WatchLimiter limiter;

      public TestZKDatabase(FileTxnSnapLog snapLog, WatchLimiter limiter) {
        super(snapLog);
        this.limiter = limiter;
      }

      @Override
      public Stat statNode(String path, ServerCnxn serverCnxn) throws KeeperException.NoNodeException {
        limiter.statLimit.updateForWatch(path, serverCnxn);
        return super.statNode(path, serverCnxn);
      }

      @Override
      public byte[] getData(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
        limiter.dataLimit.updateForWatch(path, watcher);
        return super.getData(path, stat, watcher);
      }

      @Override
      public List<String> getChildren(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
        limiter.childrenLimit.updateForWatch(path, watcher);
        return super.getChildren(path, stat, watcher);
      }
    }

    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException If there is a low-level I/O error.
     */
    public void runFromConfig(ServerConfig config) throws IOException {
      log.info("Starting server");
      try {
        // Note that this thread isn't going to be doing anything else,
        // so rather than spawning another thread, we will just call
        // run() in this thread.
        // create a file logger url from the command line args
        FileTxnSnapLog ftxn = new FileTxnSnapLog(new File(
            config.getDataLogDir()), new File(config.getDataDir()));
        zooKeeperServer = new ZooKeeperServer(ftxn, config.getTickTime(),
            config.getMinSessionTimeout(), config.getMaxSessionTimeout(),
            null /* this is not used */, new TestZKDatabase(ftxn, limiter));
        cnxnFactory = new TestServerCnxnFactory(limiter);
        cnxnFactory.configure(config.getClientPortAddress(),
            config.getMaxClientCnxns());
        cnxnFactory.startup(zooKeeperServer);
        cnxnFactory.join();
       // if (zooKeeperServer.isRunning()) {
          zkServer.shutdown();
       // }
        if (violationReportAction != LimitViolationAction.IGNORE) {
          String limitViolations = limiter.reportLimitViolations();
          if (!limitViolations.isEmpty()) {
            log.warn("Watch limit violations: {}", limitViolations);
            if (violationReportAction == LimitViolationAction.FAIL) {
              throw new AssertionError("Parallel watch limits violated");
            }
          }
        }
      } catch (InterruptedException e) {
        // warn, but generally this is ok
        log.warn("Server interrupted", e);
      }
    }

    /**
     * Shutdown the serving instance
     * @throws IOException If there is a low-level I/O error.
     */
    protected void shutdown() throws IOException {
      zooKeeperServer.shutdown();
      ZKDatabase zkDb = zooKeeperServer.getZKDatabase();
      if (cnxnFactory != null && cnxnFactory.getLocalPort() != 0) {
        waitForServerDown(getZkHost() + ":" + getPort(), 5000);
      }
      if (cnxnFactory != null) {
        cnxnFactory.shutdown();
        try {
          cnxnFactory.join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (zkDb != null) {
        zkDb.close();
      }
    }

    public int getLocalPort() {
      if (cnxnFactory == null) {
        throw new IllegalStateException("A port has not yet been selected");
      }
      int port;
      try {
        port = cnxnFactory.getLocalPort();
      } catch (NullPointerException e) {
        throw new IllegalStateException("A port has not yet been selected");
      }
      if (port == 0) {
        throw new IllegalStateException("A port has not yet been selected");
      }
      return port;
    }

    public void setViolationReportAction(LimitViolationAction violationReportAction) {
      this.violationReportAction = violationReportAction;
    }

    public WatchLimiter getLimiter() {
      return limiter;
    }
  }

  public ZkTestServer(String zkDir) {
    this.zkDir = zkDir;
  }

  public ZkTestServer(String zkDir, int port) {
    this.zkDir = zkDir;
    this.clientPort = port;
    String reportAction = System.getProperty("tests.zk.violationReportAction");
    if (reportAction != null) {
      log.info("Overriding violation report action to: {}", reportAction);
      setViolationReportAction(LimitViolationAction.valueOf(reportAction));
    }
    String limiterAction = System.getProperty("tests.zk.limiterAction");
    if (limiterAction != null) {
      log.info("Overriding limiter action to: {}", limiterAction);
      getLimiter().setAction(LimitViolationAction.valueOf(limiterAction));
    }
  }

  public String getZkHost() {
    return "127.0.0.1:" + zkServer.getLocalPort();
  }

  public String getZkAddress() {
    return "127.0.0.1:" + zkServer.getLocalPort() + "/solr";
  }

  public int getPort() {
    return zkServer.getLocalPort();
  }
  
  public void expire(final long sessionId) {
    zkServer.zooKeeperServer.expire(new Session() {
      @Override
      public long getSessionId() {
        return sessionId;
      }

      @Override
      public int getTimeout() {
        return 4000;
      }

      @Override
      public boolean isClosing() {
        return false;
      }
    });
  }

  public void run() throws InterruptedException {
    log.info("STARTING ZK TEST SERVER");
    // we don't call super.setUp
    zooThread = new Thread() {
      
      @Override
      public void run() {
        ServerConfig config = new ServerConfig() {

          {
            setClientPort(ZkTestServer.this.clientPort);
            this.dataDir = zkDir;
            this.dataLogDir = zkDir;
            this.tickTime = theTickTime;
          }
          
          public void setClientPort(int clientPort) {
            if (clientPortAddress != null) {
              try {
                this.clientPortAddress = new InetSocketAddress(
                        InetAddress.getByName(clientPortAddress.getHostName()), clientPort);
              } catch (UnknownHostException e) {
                throw new RuntimeException(e);
              }
            } else {
              this.clientPortAddress = new InetSocketAddress(clientPort);
            }
            System.out.println("client port:" + this.clientPortAddress);
          }
        };

        try {
          zkServer.runFromConfig(config);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
    };

    zooThread.setDaemon(true);
    zooThread.start();

    int cnt = 0;
    int port = -1;
    try {
       port = getPort();
    } catch(IllegalStateException e) {

    }
    while (port < 1) {
      Thread.sleep(100);
      try {
        port = getPort();
      } catch(IllegalStateException e) {

      }
      if (cnt == 500) {
        throw new RuntimeException("Could not get the port for ZooKeeper server");
      }
      cnt++;
    }
    log.info("start zk server on port:" + port);
  }

  @SuppressWarnings("deprecation")
  public void shutdown() throws IOException, InterruptedException {
    // TODO: this can log an exception while trying to unregister a JMX MBean
    zkServer.shutdown();
    try {
      zooThread.join();
    } catch (NullPointerException e) {
      // okay
    }
  }
  
  public static boolean waitForServerDown(String hp, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        HostPort hpobj = parseHostPortList(hp).get(0);
        send4LetterWord(hpobj.host, hpobj.port, "stat");
      } catch (IOException e) {
        return true;
      }
      
      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }
  
  public static class HostPort {
    String host;
    int port;
    
    HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }
  
  /**
   * Send the 4letterword
   * @param host the destination host
   * @param port the destination port
   * @param cmd the 4letterword
   * @return server response

   */
  public static String send4LetterWord(String host, int port, String cmd)
          throws IOException
  {
    log.info("connecting to " + host + " " + port);
    BufferedReader reader = null;
    try (Socket sock = new Socket(host, port)) {
      OutputStream outstream = sock.getOutputStream();
      outstream.write(cmd.getBytes(StandardCharsets.US_ASCII));
      outstream.flush();
      // this replicates NC - close the output stream before reading
      sock.shutdownOutput();

      reader =
          new BufferedReader(
              new InputStreamReader(sock.getInputStream(), "US-ASCII"));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }
      return sb.toString();
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
  
  public static List<HostPort> parseHostPortList(String hplist) {
    ArrayList<HostPort> alist = new ArrayList<>();
    for (String hp : hplist.split(",")) {
      int idx = hp.lastIndexOf(':');
      String host = hp.substring(0, idx);
      int port;
      try {
        port = Integer.parseInt(hp.substring(idx + 1));
      } catch (RuntimeException e) {
        throw new RuntimeException("Problem parsing " + hp + e.toString());
      }
      alist.add(new HostPort(host, port));
    }
    return alist;
  }

  public int getTheTickTime() {
    return theTickTime;
  }

  public void setTheTickTime(int theTickTime) {
    this.theTickTime = theTickTime;
  }

  public String getZkDir() {
    return zkDir;
  }

  public void setViolationReportAction(LimitViolationAction violationReportAction) {
    zkServer.setViolationReportAction(violationReportAction);
  }

  public ZKServerMain.WatchLimiter getLimiter() {
    return zkServer.getLimiter();
  }
}
