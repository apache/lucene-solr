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
package org.apache.solr.cloud;

import com.google.common.util.concurrent.AtomicLongMap;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
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

import javax.management.JMException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ZkTestServer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static File SOLRHOME;
  static {
    try {
      SOLRHOME = new File(SolrTestCaseJ4.TEST_HOME());
    } catch (RuntimeException e) {
      log.warn("TEST_HOME() does not exist - solrj test?");
      // solrj tests not working with TEST_HOME()
      // must override getSolrHome
    }
  }

  public static final int TIMEOUT = 45000;
  public static final int TICK_TIME = 1000;

  protected final ZKServerMain zkServer = new ZKServerMain();

  private volatile Path zkDir;

  private volatile int clientPort;

  private volatile Thread zooThread;

  private volatile int theTickTime = TICK_TIME;
  // SOLR-12101 - provide defaults to avoid max timeout 20 enforced by our server instance when tick time is 1000
  private volatile int maxSessionTimeout = 90000;
  private volatile int minSessionTimeout = 3000;

  protected volatile SolrZkClient rootClient;
  protected volatile SolrZkClient chRootClient;

  private volatile ZKDatabase zkDb;

  static public enum LimitViolationAction {
    IGNORE,
    REPORT,
    FAIL,
  }

  class ZKServerMain {

    private volatile ServerCnxnFactory cnxnFactory;
    private volatile ZooKeeperServer zooKeeperServer;
    private volatile LimitViolationAction violationReportAction = LimitViolationAction.REPORT;
    private volatile WatchLimiter limiter = new WatchLimiter(1, LimitViolationAction.IGNORE);

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

      private volatile LimitViolationAction action;
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
        String[] maxKeys = maxCounters.keySet().toArray(new String[maxCounters.size()]);
        Arrays.sort(maxKeys, new Comparator<String>() {
          private final Comparator<Long> valComp = Comparator.<Long>naturalOrder().reversed();
          @Override
          public int compare(String o1, String o2) {
            return valComp.compare(maxCounters.get(o1), maxCounters.get(o2));
          }
        });

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String key : maxKeys) {
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
          case ChildWatchRemoved:
            break;
          case DataWatchRemoved:
            break;
        }
      }
    }

    private class TestServerCnxn extends NIOServerCnxn {

      private final WatchLimiter limiter;

      public TestServerCnxn(ZooKeeperServer zk, SocketChannel sock, SelectionKey sk,
                            NIOServerCnxnFactory factory, WatchLimiter limiter) throws IOException {
        super(zk, sock, sk, factory, null);
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
      ObjectReleaseTracker.track(this);
      log.info("Starting server");
      try {
        // ZooKeeper maintains a static collection of AuthenticationProviders, so
        // we make sure the SASL provider is loaded so that it can be used in
        // subsequent tests.
        System.setProperty("zookeeper.authProvider.1",
          "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        // Note that this thread isn't going to be doing anything else,
        // so rather than spawning another thread, we will just call
        // run() in this thread.
        // create a file logger url from the command line args
        FileTxnSnapLog ftxn = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());

        zooKeeperServer = new ZooKeeperServer(ftxn, config.getTickTime(),
            config.getMinSessionTimeout(), config.getMaxSessionTimeout(),
            new TestZKDatabase(ftxn, limiter));
        cnxnFactory = new TestServerCnxnFactory(limiter);
        cnxnFactory.configure(config.getClientPortAddress(),
            config.getMaxClientCnxns());
        cnxnFactory.startup(zooKeeperServer);
        cnxnFactory.join();

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

      // shutting down the cnxnFactory will close the zooKeeperServer
      // zooKeeperServer.shutdown();

      ZKDatabase zkDb = zooKeeperServer.getZKDatabase();
      try {
        if (cnxnFactory != null) {
          while (true) {
            cnxnFactory.shutdown();
            try {
              cnxnFactory.join();
              break;
            } catch (InterruptedException e) {
              // Thread.currentThread().interrupt();
              // don't keep interrupted status
            }
          }
        }
        if (zkDb != null) {
          zkDb.close();
        }

        if (cnxnFactory != null && cnxnFactory.getLocalPort() != 0) {
          waitForServerDown(getZkHost(), 30000);
        }
      } finally {

        ObjectReleaseTracker.release(this);
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

  public ZkTestServer(Path zkDir) throws Exception {
    this(zkDir, 0);
  }

  public ZkTestServer(Path zkDir, int port) throws KeeperException, InterruptedException {
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

    ObjectReleaseTracker.track(this);
  }

  private void init(boolean solrFormat) throws Exception {
    try {
      rootClient = new SolrZkClient(getZkHost(), TIMEOUT, 30000);
    } catch (Exception e) {
      log.error("error making rootClient, trying one more time", e);
      rootClient = new SolrZkClient(getZkHost(), TIMEOUT, 30000);
    }

    if (solrFormat) {
      tryCleanSolrZkNode();
      makeSolrZkNode();
    }

    chRootClient = new SolrZkClient(getZkAddress(), AbstractZkTestCase.TIMEOUT, 30000);
  }

  public String getZkHost() {
    String hostName = System.getProperty("hostName", "127.0.0.1");
    return hostName + ":" + zkServer.getLocalPort();
  }

  public String getZkAddress() {
    return getZkAddress("/solr");
  }

  /**
   * Get a connection string for this server with a given chroot
   * @param chroot the chroot
   * @return the connection string
   */
  public String getZkAddress(String chroot) {
    if (!chroot.startsWith("/")) {
      chroot = "/" + chroot;
    }
    return getZkHost() + chroot;
  }

  /**
   * Check that a path exists in this server
   * @param path the path to check
   * @throws IOException if an IO exception occurs
   */
  public void ensurePathExists(String path) throws IOException {
    try (SolrZkClient client = new SolrZkClient(getZkHost(), 10000)) {
      client.makePath(path, null, CreateMode.PERSISTENT, null, false, true, 0);
    } catch (InterruptedException | KeeperException e) {
      e.printStackTrace();
      throw new IOException("Error checking path " + path, SolrZkClient.checkInterrupted(e));
    }
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

  public ZKDatabase getZKDatabase() {
    return zkServer.zooKeeperServer.getZKDatabase();
  }

  public void setZKDatabase(ZKDatabase zkDb) {
    this.zkDb = zkDb;
    zkServer.zooKeeperServer.setZKDatabase(zkDb);
  }

  public void run() throws InterruptedException, IOException {
    run(true);
  }

  public void run(boolean solrFormat) throws InterruptedException, IOException {
    log.info("STARTING ZK TEST SERVER");
    try {
      if (zooThread != null) {
        throw new IllegalStateException("ZK TEST SERVER IS ALREADY RUNNING");
      }
      // we don't call super.distribSetUp
      zooThread = new Thread("ZkTestServer Run Thread") {

        @Override
        public void run() {
          ServerConfig config = new ServerConfig() {

            {
              setClientPort(ZkTestServer.this.clientPort);
              this.dataDir = zkDir.toFile();
              this.dataLogDir = zkDir.toFile();
              this.tickTime = theTickTime;
              this.maxSessionTimeout = ZkTestServer.this.maxSessionTimeout;
              this.minSessionTimeout = ZkTestServer.this.minSessionTimeout;
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
              log.info("client port:" + this.clientPortAddress);
            }
          };
          try {
            zkServer.runFromConfig(config);
          } catch (Throwable t) {
            log.error("zkServer error", t);
          }
        }
      };

      ObjectReleaseTracker.track(zooThread);
      zooThread.start();

      int cnt = 0;
      int port = -1;
      try {
        port = getPort();
      } catch (IllegalStateException e) {

      }
      while (port < 1) {
        Thread.sleep(100);
        try {
          port = getPort();
        } catch (IllegalStateException e) {

        }
        if (cnt == 500) {
          throw new RuntimeException("Could not get the port for ZooKeeper server");
        }
        cnt++;
      }
      log.info("start zk server on port:" + port);

      waitForServerUp(getZkHost(), 30000);

      init(solrFormat);
    } catch (Exception e) {
      log.error("Error trying to run ZK Test Server", e);
      throw new RuntimeException(e);
    }
  }

  public void shutdown() throws IOException, InterruptedException {
    log.info("Shutting down ZkTestServer.");
    try {
      IOUtils.closeQuietly(rootClient);
      IOUtils.closeQuietly(chRootClient);
    } finally {

      // TODO: this can log an exception while trying to unregister a JMX MBean
      try {
        zkServer.shutdown();
      } catch (Exception e) {
        log.error("Exception shutting down ZooKeeper Test Server",e);
      }

      if (zkDb != null) {
        zkDb.close();
      }

      while (true) {
        try {
          zooThread.join();
          ObjectReleaseTracker.release(zooThread);
          zooThread = null;
          break;
        } catch (InterruptedException e) {
          // don't keep interrupted status
        } catch (NullPointerException e) {
          // okay
          break;
        }
      }
    }
    ObjectReleaseTracker.release(this);
  }

  public static boolean waitForServerDown(String hp, long timeoutMs) {
    log.info("waitForServerDown: {}", hp);
    final TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (true) {
      try {
        HostPort hpobj = parseHostPortList(hp).get(0);
        send4LetterWord(hpobj.host, hpobj.port, "stat");
      } catch (IOException e) {
        return true;
      }

      if (timeout.hasTimedOut()) {
        throw new RuntimeException("Time out waiting for ZooKeeper shutdown!");
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  public static boolean waitForServerUp(String hp, long timeoutMs) {
    log.info("waitForServerUp: {}", hp);
    final TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (true) {
      try {
        HostPort hpobj = parseHostPortList(hp).get(0);
        send4LetterWord(hpobj.host, hpobj.port, "stat");
        return true;
      } catch (IOException e) {
        e.printStackTrace();
      }

      if (timeout.hasTimedOut()) {
        throw new RuntimeException("Time out waiting for ZooKeeper to startup!");
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  public static class HostPort {
    String host;
    int port;

    HostPort(String host, int port) {
      assert !host.contains(":") : host;
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

      reader = new BufferedReader(
          new InputStreamReader(sock.getInputStream(), StandardCharsets.US_ASCII));
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
    log.info("parse host and port list: " + hplist);
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

  public Path getZkDir() {
    return zkDir;
  }

  public void setViolationReportAction(LimitViolationAction violationReportAction) {
    zkServer.setViolationReportAction(violationReportAction);
  }

  public ZKServerMain.WatchLimiter getLimiter() {
    return zkServer.getLimiter();
  }

  public int getMaxSessionTimeout() {
    return maxSessionTimeout;
  }

  public int getMinSessionTimeout() {
    return minSessionTimeout;
  }

  public void setMaxSessionTimeout(int maxSessionTimeout) {
    this.maxSessionTimeout = maxSessionTimeout;
  }

  public void setMinSessionTimeout(int minSessionTimeout) {
    this.minSessionTimeout = minSessionTimeout;
  }

  void buildZooKeeper(String config,
      String schema) throws Exception {
    buildZooKeeper(SOLRHOME, config, schema);
  }

  public static void putConfig(String confName, SolrZkClient zkClient, File solrhome, final String name)
      throws Exception {
    putConfig(confName, zkClient, null, solrhome, name, name);
  }


  public static void putConfig(String confName, SolrZkClient zkClient, File solrhome, final String srcName,
                                 String destName) throws Exception {
      putConfig(confName, zkClient, null, solrhome, srcName, destName);
  }

  public static void putConfig(String confName, SolrZkClient zkClient, String zkChroot, File solrhome,
                               final String srcName, String destName) throws Exception {
    File file = new File(solrhome, "collection1"
        + File.separator + "conf" + File.separator + srcName);
    if (!file.exists()) {
      log.info("skipping " + file.getAbsolutePath() + " because it doesn't exist");
      return;
    }

    String destPath = "/configs/" + confName + "/" + destName;
    if (zkChroot != null) {
      destPath = zkChroot + destPath;
    }
    log.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }

  // static to share with distrib test
  public void buildZooKeeper(File solrhome, String config, String schema) throws Exception {

    Map<String,Object> props = new HashMap<>();
    props.put("configName", "conf1");
    final ZkNodeProps zkProps = new ZkNodeProps(props);


    List<Op> ops = new ArrayList<>(2);
    String path = "/collections";
    ops.add(Op.create(path, null, chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    path = "/collections/collection1";
    ops.add(Op.create(path, Utils.toJSON(zkProps), chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    path = "/collections/collection1/shards";
    ops.add(Op.create(path, null, chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    path = "/collections/control_collection";
    ops.add(Op.create(path, Utils.toJSON(zkProps), chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    path = "/collections/control_collection/shards";
    ops.add(Op.create(path, null, chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    path = "/configs";
    ops.add(Op.create(path, null, chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    path = "/configs/conf1";
    ops.add(Op.create(path, null, chRootClient.getZkACLProvider().getACLsToAdd(path),  CreateMode.PERSISTENT));
    chRootClient.multi(ops, true);

    // this workaround is acceptable until we remove legacyCloud because we just init a single core here
    String defaultClusterProps = "{\""+ZkStateReader.LEGACY_CLOUD+"\":\"true\"}";
    chRootClient.makePath(ZkStateReader.CLUSTER_PROPS, defaultClusterProps.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
    // for now, always upload the config and schema to the canonical names
    putConfig("conf1", chRootClient, solrhome, config, "solrconfig.xml");
    putConfig("conf1", chRootClient, solrhome, schema, "schema.xml");

    putConfig("conf1", chRootClient, solrhome, "solrconfig.snippet.randomindexconfig.xml");
    putConfig("conf1", chRootClient, solrhome, "stopwords.txt");
    putConfig("conf1", chRootClient, solrhome, "protwords.txt");
    putConfig("conf1", chRootClient, solrhome, "currency.xml");
    putConfig("conf1", chRootClient, solrhome, "enumsConfig.xml");
    putConfig("conf1", chRootClient, solrhome, "open-exchange-rates.json");
    putConfig("conf1", chRootClient, solrhome, "mapping-ISOLatin1Accent.txt");
    putConfig("conf1", chRootClient, solrhome, "old_synonyms.txt");
    putConfig("conf1", chRootClient, solrhome, "synonyms.txt");
  }

  public void makeSolrZkNode() throws Exception {
    rootClient.makePath("/solr", false, true);
  }

  public void tryCleanSolrZkNode() throws Exception {
    tryCleanPath("/solr");
  }

  void tryCleanPath(String path) throws Exception {
    if (rootClient.exists(path, true)) {
      rootClient.clean(path);
    }
  }

  protected void printLayout() throws Exception {
    rootClient.printLayoutToStream(System.out);
  }

  public SolrZkClient getZkClient() {
    return chRootClient;
  }
}
