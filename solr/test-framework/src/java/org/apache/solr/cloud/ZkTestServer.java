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

import javax.management.JMException;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.jmx.ManagedUtil;
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

public class ZkTestServer implements Closeable {

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

  private volatile CloseTracker closeTracker;

  private Path zkMonitoringFile;

  public static final int TIMEOUT = 45000;
  public static final int TICK_TIME = 1000;


  protected final ZKServerMain zkServer = new ZKServerMain();

  private volatile Path zkDir;

  private volatile int clientPort;

  private volatile Thread zooThread;

  private volatile int theTickTime = TICK_TIME;
  // SOLR-12101 - provide defaults to avoid max timeout 20 enforced by our server instance when tick time is 1000
  private volatile int maxSessionTimeout = 45000;
  private volatile int minSessionTimeout = 1000;


  protected volatile SolrZkClient chRootClient;

  public volatile CountDownLatch startupWait = new CountDownLatch(1);

  private static class ShortTimeoutSession implements Session {
    private final long sessionId;

    public ShortTimeoutSession(long sessionId) {
      this.sessionId = sessionId;
    }

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
  }

  class ZKServerMain {

    private volatile ServerCnxnFactory cnxnFactory;
    private volatile ZooKeeperServer zooKeeperServer;

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

    /**
     * Run from a ServerConfig.
     *
     * @param config
     *          ServerConfig to use.
     * @throws IOException
     *           If there is a low-level I/O error.
     */
    public void runFromConfig(ServerConfig config) throws IOException {
      assert ObjectReleaseTracker.track(this);
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
                config.getMinSessionTimeout(), config.getMaxSessionTimeout(), -1,
                new ZKDatabase(ftxn), "");
        cnxnFactory = new NIOServerCnxnFactory(); // MRM TODO: look again at the netty impl
        cnxnFactory.configure(config.getClientPortAddress(),
                config.getMaxClientCnxns());
        cnxnFactory.startup(zooKeeperServer);

        startupWait.countDown();

        log.info("ZK Port:" + zooKeeperServer.getClientPort());
        cnxnFactory.join();

      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    /**
     * Shutdown the serving instance
     *
     * @throws IOException
     *           If there is a low-level I/O error.
     */
    protected void shutdown() throws IOException {
      try {
        //        zooKeeperServer.shutdown(true);
        //      ZKDatabase zkDb = zooKeeperServer.getZKDatabase();
        //    if (zkDb != null) zkDb.clear();

        if (zooKeeperServer != null) zooKeeperServer.shutdown(true);
        if (cnxnFactory != null) {
          cnxnFactory.closeAll(ServerCnxn.DisconnectReason.CONNECTION_CLOSE_FORCED);
          cnxnFactory.shutdown();
        }


      } finally {
        assert ObjectReleaseTracker.release(this);
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
  }

  public ZkTestServer(Path zkDir) throws Exception {
    this(zkDir, 0);
  }

  public ZkTestServer(Path zkDir, int port) throws KeeperException, InterruptedException {
    this.zkDir = zkDir;
    this.clientPort = port;
    String zkMonFile = System.getProperty("solr.tests.zkmonfile");
    if (zkMonFile != null) {
      zkMonitoringFile = Paths.get(System.getProperty("solr.tests.zkmonfile"));
    }
    assert ObjectReleaseTracker.track(this);
  }

  private void init(boolean solrFormat) throws Exception {
    chRootClient = new SolrZkClient(getZkHost(), AbstractZkTestCase.TIMEOUT, 5000);
    chRootClient.start();
    if (solrFormat) {
      try {
        makeSolrZkNode();
      } catch (KeeperException.NodeExistsException e) {
        chRootClient.clean("/solr");
        makeSolrZkNode();
      }
    }
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
   *
   * @param chroot
   *          the chroot
   * @return the connection string
   */
  public String getZkAddress(String chroot) {
    if (!chroot.startsWith("/")) {
      chroot = "/" + chroot;
    }
    return getZkHost() + chroot;
  }

  public int getPort() {
    return zkServer.getLocalPort();
  }

  public void expire(final long sessionId) {
    zkServer.zooKeeperServer.expire(new ShortTimeoutSession(sessionId));
  }

  public ZKDatabase getZKDatabase() {
    return zkServer.zooKeeperServer.getZKDatabase();
  }

  public void setZKDatabase(ZKDatabase zkDb) {
    zkServer.zooKeeperServer.setZKDatabase(zkDb);
  }

  public synchronized void run() throws InterruptedException, IOException {
    run(false);
  }

  public synchronized void run(boolean solrFormat) throws InterruptedException, IOException {
    log.info("STARTING ZK TEST SERVER dataDir={}", this.zkDir);


    // docs say no config for netty yet
   // System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
   // System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
    System.setProperty(SolrZkServer.ZK_WHITELIST_PROPERTY, "*");
    System.setProperty("zookeeper.admin.enableServer", "false");
    if (zooThread != null) {
      throw new AlreadyClosedException();
    }
    if (closeTracker != null) {
      throw new AlreadyClosedException();
    }
    assert (closeTracker = new CloseTracker()) != null;

    try {
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
                  this.clientPortAddress = new InetSocketAddress(InetAddress.getByName(clientPortAddress.getHostName()), clientPort);
                } catch (UnknownHostException e) {
                  throw new RuntimeException(e);
                }
              } else {
                this.clientPortAddress = new InetSocketAddress(clientPort);
              }

            }
          };
          try {
            zkServer.runFromConfig(config);
          } catch (Throwable t) {
            log.error("zkServer error", t);
          }
        }
      };

      assert ObjectReleaseTracker.track(zooThread);

      zooThread.start();

      boolean success = startupWait.await(5, TimeUnit.SECONDS);
      if (!success) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting for zk test server to start");
      }

      init(solrFormat);
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error trying to run ZK Test Server", e);
    }
  }

  @Override
  public void close() {
    try {
      shutdown();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public synchronized void shutdown() throws IOException, InterruptedException {
    log.info("Shutting down ZkTestServer.");
    assert closeTracker != null ? closeTracker.close() : true;
    try {
      if (chRootClient != null && chRootClient.isConnected()) {
        chRootClient.printLayout();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt("Exception trying to print zk layout to log on shutdown", e);
    }
    if (zkMonitoringFile != null && chRootClient != null && zkServer != null) {
      try {
        writeZkMonitorFile(zkMonitoringFile, chRootClient);
      } catch (Exception e2) {
        ParWork.propagateInterrupt("Exception trying to write zk layout to file on shutdown", e2);
      }
    }

    ParWork.close(chRootClient);
    chRootClient = null;

    if (zkServer != null)  {
      zkServer.shutdown();
      if (zooThread != null) {

        zooThread.join(10000);

        if (zooThread.isAlive()) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Zookeeper thread still running");
        }
        assert ObjectReleaseTracker.release(zooThread);
      }
    }

    startupWait = new CountDownLatch(1);

    zooThread = null;
    assert ObjectReleaseTracker.release(this);

//    if (cleaupDir) {
//      Files.walk(getZkDir())
//              .map(Path::toFile)
//              .sorted((o1, o2) -> -o1.compareTo(o2))
//              .forEach(File::delete);
//    }
  }

  private static void writeZkMonitorFile(Path outputFile, SolrZkClient zkClient) {
    synchronized (zkClient) {
      zkClient.printLayoutToFile(outputFile);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    SolrZkClient zkClient = new SolrZkClient("127.0.0.1:2181", 30000);
    zkClient.start();
    Path outfile = Paths.get(System.getProperty("user.home"), "zkout.zk");
    while (true) {
      writeZkMonitorFile(outfile, zkClient);
      Thread.sleep(1000);
    }

  }

//  public static boolean waitForServerDown(String hp, long timeoutMs) {
//    log.info("waitForServerDown: {}", hp);
//    final TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
//    while (true) {
//      try {
//        HostPort hpobj = parseHostPortList(hp).get(0);
//        send4LetterWord(hpobj.host, hpobj.port, "stat");
//      } catch (IOException e) {
//        return true;
//      }
//
//      if (timeout.hasTimedOut()) {
//        throw new RuntimeException("Time out waiting for ZooKeeper shutdown!");
//      }
//      try {
//        Thread.sleep(250);
//      } catch (InterruptedException e) {
//        SolrZkClient.checkInterrupted(e);
//      }
//    }
//  }

  // public static boolean waitForServerUp(String hp, long timeoutMs) {
  // log.info("waitForServerUp: {}", hp);
  // final TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
  // while (true) {
  // try {
  // HostPort hpobj = parseHostPortList(hp).get(0);
  // send4LetterWord(hpobj.host, hpobj.port, "stat");
  // return true;
  // } catch (IOException e) {
  // e.printStackTrace();
  // }
  //
  // if (timeout.hasTimedOut()) {
  // throw new RuntimeException("Time out waiting for ZooKeeper to startup!");
  // }
  // try {
  // Thread.sleep(500);
  // } catch (InterruptedException e) {
  // DW.propegateInterrupt(e);
  // }
  // }
  // }

  public static class HostPort {
    String host;
    int port;

    HostPort(String host, int port) {
      assert !host.contains(":") : host;
      this.host = host;
      this.port = port;
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
  public void buildZooKeeper() throws Exception {
    // this workaround is acceptable until we remove legacyCloud because we just init a single core here
    String defaultClusterProps = "{}";
    chRootClient.makePath("/solr" + ZkStateReader.CLUSTER_PROPS, defaultClusterProps.getBytes(StandardCharsets.UTF_8),
            CreateMode.PERSISTENT, false);
  }

  public void makeSolrZkNode() throws Exception {
     chRootClient.mkdir("/solr");
  }

  public void tryCleanSolrZkNode(SolrZkClient zkClient) throws Exception {
    tryCleanPath(zkClient, "/solr");
  }

  void tryCleanPath(SolrZkClient zkClient, String path) throws Exception {
    zkClient.clean(path);
  }

  protected void printLayout() throws Exception {
    chRootClient.printLayoutToStream(System.out);
  }

  public SolrZkClient getZkClient() {
    return chRootClient;
  }
}
