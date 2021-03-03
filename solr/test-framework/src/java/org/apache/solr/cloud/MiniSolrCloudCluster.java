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

import javax.servlet.Filter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Mini" SolrCloud cluster to be used for testing
 */
public class MiniSolrCloudCluster {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ZOOKEEPER_SERVER1_DATA = "zookeeper/server1/data";

  private static final String ZK_HOST = "zkHost";

  private static final String URL_SCHEME_HTTPS = "{'urlScheme':'https'}";

  private static final String SOLR_XML = "/solr.xml";

  private static final String SOLR_SECURITY_JSON = "/security.json";

  private static final int STARTUP_WAIT_SECONDS = 10;

  public static final String SOLR_TESTS_SHARDS_WHITELIST = "solr.tests.shardsWhitelist";

  public static final String DEFAULT_CLOUD_SOLR_XML = "<solr>\n" +
      "\n" +
      "  <str name=\"shareSchema\">${shareSchema:false}</str>\n" +
      "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n" +
      "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n" +
      "  <str name=\"collectionsHandler\">${collectionsHandler:solr.CollectionsHandler}</str>\n" +
      "\n" +
      "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n" +
      "    <str name=\"urlScheme\">${urlScheme:}</str>\n" +
      "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n" +
      "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n" +
      "    <str name=\"shardsWhitelist\">${"+SOLR_TESTS_SHARDS_WHITELIST+":}</str>\n" +
      "  </shardHandlerFactory>\n" +
      "\n" +
      "  <solrcloud>\n" +
      "    <str name=\"host\">127.0.0.1</str>\n" +
      "    <int name=\"hostPort\">${hostPort:8983}</int>\n" +
      "    <str name=\"hostContext\">${hostContext:solr}</str>\n" +
      "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:15000}</int>\n" +
      "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
      "    <int name=\"leaderVoteWait\">${leaderVoteWait:5000}</int>\n" +
      "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
      "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
      "    <str name=\"zkCredentialsProvider\">${zkCredentialsProvider:org.apache.solr.common.cloud.DefaultZkCredentialsProvider}</str> \n" +
      "    <str name=\"zkACLProvider\">${zkACLProvider:org.apache.solr.common.cloud.DefaultZkACLProvider}</str> \n" +
      "    <str name=\"pkiHandlerPrivateKeyPath\">${pkiHandlerPrivateKeyPath:cryptokeys/priv_key512_pkcs8.pem}</str> \n" +
      "    <str name=\"pkiHandlerPublicKeyPath\">${pkiHandlerPublicKeyPath:cryptokeys/pub_key512.der}</str> \n" +
      "  </solrcloud>\n" +
      "  <metrics>\n" +
      "    <reporter name=\"default\"  enabled=\"false\" class=\"org.apache.solr.metrics.reporters.SolrJmxReporter\">\n" +
          "      <bool name=\"enabled\">false</bool>\n" +
      "      <str name=\"rootName\">solr_${hostPort:8983}</str>\n" +
      "    </reporter>\n" +
      "  </metrics>\n" +
      "  \n" +
      "</solr>\n";

  private final Object startupWait = new Object();
  private volatile SolrZkClient solrZkClient;
  private volatile ZkTestServer zkServer; // non-final due to injectChaos()
  private final boolean externalZkServer;
  private final List<JettySolrRunner> jettys = new CopyOnWriteArrayList<>();
  private final Path baseDir;
  private CloudHttp2SolrClient solrClient;
  private final JettyConfig jettyConfig;
  private final boolean trackJettyMetrics;

  private final AtomicInteger nodeIds = new AtomicInteger();

  private volatile ZkStateReader zkStateReader;

  /**
   * Create a MiniSolrCloudCluster with default solr.xml
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param jettyConfig Jetty configuration
   *
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, Path baseDir, JettyConfig jettyConfig) throws Exception {
    this(numServers, baseDir, DEFAULT_CLOUD_SOLR_XML, jettyConfig, null);
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   *
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, Path baseDir, String solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters) throws Exception {
    this(numServers, hostContext, baseDir, solrXml, extraServlets, extraRequestFilters, null);
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   *
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, Path baseDir, String solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters,
      SSLConfig sslConfig) throws Exception {
    this(numServers, baseDir, solrXml, JettyConfig.builder()
        .setContext(hostContext)
        .withSSLConfig(sslConfig)
        .withFilters(extraRequestFilters)
        .withServlets(extraServlets)
        .build());
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   *
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig) throws Exception {
    this(numServers, baseDir, solrXml, jettyConfig, null);
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @param zkTestServer ZkTestServer to use.  If null, one will be created
   *
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig,
      ZkTestServer zkTestServer) throws Exception {
    this(numServers, baseDir, solrXml, jettyConfig, zkTestServer, Optional.empty());
  }

  /**
   * Create a MiniSolrCloudCluster.
   * Note - this constructor visibility is changed to package protected so as to
   * discourage its usage. Ideally *new* functionality should use {@linkplain SolrCloudTestCase}
   * to configure any additional parameters.
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @param zkTestServer ZkTestServer to use.  If null, one will be created
   * @param securityJson A string representation of security.json file (optional).
   *
   * @throws Exception if there was an error starting the cluster
   */
   MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig,
      ZkTestServer zkTestServer, Optional<String> securityJson) throws Exception {
     this(numServers, baseDir, solrXml, jettyConfig,
         zkTestServer,securityJson, false, true);
   }
  /**
   * Create a MiniSolrCloudCluster.
   * Note - this constructor visibility is changed to package protected so as to
   * discourage its usage. Ideally *new* functionality should use {@linkplain SolrCloudTestCase}
   * to configure any additional parameters.
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @param zkTestServer ZkTestServer to use.  If null, one will be created
   * @param securityJson A string representation of security.json file (optional).
   * @param trackJettyMetrics supply jetties with metrics registry
   *
   * @throws Exception if there was an error starting the cluster
   */
   MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig,
      ZkTestServer zkTestServer, Optional<String> securityJson, boolean trackJettyMetrics, boolean formatZk) throws Exception {
     assert ObjectReleaseTracker.track(this);
    try {
      Objects.requireNonNull(securityJson);
      this.baseDir = Objects.requireNonNull(baseDir);
      this.jettyConfig = Objects.requireNonNull(jettyConfig);
      this.trackJettyMetrics = trackJettyMetrics;

      log.info("Starting cluster of {} servers in {}", numServers, baseDir);

      Files.createDirectories(baseDir);

      this.externalZkServer = zkTestServer != null;
      if (!externalZkServer) {
        Path zkDir = baseDir.resolve(ZOOKEEPER_SERVER1_DATA);
        this.zkServer = new ZkTestServer(zkDir);
        try {
          this.zkServer.run(formatZk);
        } catch (Exception e) {
          log.error("Error starting Zk Test Server, trying again ...");
          this.zkServer.shutdown();
          this.zkServer = new ZkTestServer(zkDir);
          this.zkServer.run();
        }

        SolrZkClient zkClient = this.zkServer.getZkClient();

        log.info("Using zkClient host={} to create solr.xml", zkClient.getZkServerAddress());
        zkClient.mkdir("/solr" + SOLR_XML, solrXml.getBytes(Charset.defaultCharset()));

        if (jettyConfig.sslConfig != null && jettyConfig.sslConfig.isSSLMode()) {
          zkClient.mkdir("/solr" + ZkStateReader.CLUSTER_PROPS,
                  URL_SCHEME_HTTPS.getBytes(StandardCharsets.UTF_8));
        }
        if (securityJson.isPresent()) { // configure Solr security
          zkClient.makePath("/solr" + SOLR_SECURITY_JSON, securityJson.get().getBytes(Charset.defaultCharset()), true);
        }
      } else {
        zkServer = zkTestServer;
        this.zkServer.getZkClient().mkDirs("/solr" + SOLR_XML, solrXml.getBytes(Charset.defaultCharset()));
      }

      // tell solr to look in zookeeper for solr.xml
      System.setProperty(ZK_HOST, zkServer.getZkAddress());

      List<Callable<JettySolrRunner>> startups = new ArrayList<>(numServers);
      for (int i = 0; i < numServers; ++i) {
        startups.add(() -> startJettySolrRunner(newNodeName(), jettyConfig.context, jettyConfig));
      }

      try {
        try (ParWork worker = new ParWork(this, false, false)) {
          worker.collect("start-jettys", startups);
        }
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        this.shutdown();
        throw e;
      }

      // build the client
      solrClient = buildZkReaderAndSolrClient();
      solrZkClient = zkStateReader.getZkClient();

    } catch (Throwable t) {
      shutdown();
      ParWork.propagateInterrupt(t);
      throw new SolrException(ErrorCode.SERVER_ERROR, t);
    }
  }

  private void waitForAllNodes(int numServers, int timeoutSeconds) throws IOException, InterruptedException, TimeoutException {
    log.info("waitForAllNodes: numServers={}", numServers);

    int numRunning = 0;
    TimeOut timeout = new TimeOut(timeoutSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    synchronized (startupWait) {
      while (numServers != numRunningJettys(getJettySolrRunners())) {
        if (timeout.hasTimedOut()) {
          throw new IllegalStateException("giving up waiting for all jetty instances to be running. numServers=" + numServers
                  + " numRunning=" + numRunning);
        }

        startupWait.wait(500);
      }
    }

    List<JettySolrRunner> runners = getJettySolrRunners();

    for (JettySolrRunner runner : runners) {
      waitForNode(runner, 10);
    }

    log.info("Done waitForAllNodes: numServers={}", numServers);
  }

  private int numRunningJettys(List<JettySolrRunner> runners) {
    int numRunning = 0;
    for (JettySolrRunner jetty : runners) {
      if (jetty.isRunning()) {
        numRunning++;
      }
    }
    return numRunning;
  }

  public void waitForNode(JettySolrRunner jetty, int timeoutSeconds)
      throws IOException, InterruptedException, TimeoutException {
    if (jetty.getNodeName() == null) {
      log.info("Cannot wait for Jetty with null node name");
      throw new IllegalArgumentException("Cannot wait for Jetty with null node name");
    }
    if (log.isInfoEnabled()) {
      log.info("waitForNode: {}", jetty.getNodeName());
    }

    ZkStateReader reader = getSolrClient().getZkStateReader();

    reader.waitForLiveNodes(timeoutSeconds, TimeUnit.SECONDS, (n) -> n != null && jetty != null && jetty.getNodeName() != null && n.contains(jetty.getNodeName()));

  }

  /**
   * This method wait till all Solr JVMs ( Jettys ) are running . It waits up to the timeout (in seconds) for the JVMs to
   * be up before throwing IllegalStateException. This is called automatically on cluster startup and so is only needed
   * when starting additional Jetty instances.
   *
   * @param timeout
   *          number of seconds to wait before throwing an IllegalStateException
   * @throws IOException
   *           if there was an error communicating with ZooKeeper
   * @throws InterruptedException
   *           if the calling thread is interrupted during the wait operation
   * @throws TimeoutException on timeout before all nodes being ready
   */
  public void waitForAllNodes(int timeout) throws IOException, InterruptedException, TimeoutException {
    waitForAllNodes(jettys.size(), timeout);
  }

  private String newNodeName() {
    return "node" + nodeIds.incrementAndGet();
  }

  private Path createInstancePath(String name) throws IOException {
    Path instancePath = baseDir.resolve(name);
    Files.createDirectory(instancePath);
    return instancePath;
  }

  /**
   * @return ZooKeeper server used by the MiniCluster
   */
  public ZkTestServer getZkServer() {
    return zkServer;
  }

  /**
   * @return Unmodifiable list of all the currently started Solr Jettys.
   */
  public List<JettySolrRunner> getJettySolrRunners() {
    return Collections.unmodifiableList(jettys);
  }

  /**
   * @return a randomly-selected Jetty
   */
  public JettySolrRunner getRandomJetty(Random random) {
    return getRandomJetty(random, null);
  }

  /**
   * @return a randomly-selected Jetty, but prefer NOT jetty
   */
  public JettySolrRunner getRandomJetty(Random random, JettySolrRunner jetty) {
    // TODO we could return overseer more often on NIGHTLY runs
    if (jettys.size() == 1) {
      return jettys.get(0);
    }

    JettySolrRunner overseerRunner;

    try {
      overseerRunner = getCurrentOverseerJetty();

      List<JettySolrRunner> runners = new ArrayList<>(jettys);
      runners.remove(overseerRunner);

      if (jetty != null && jettys.size() > 2) {
        runners.remove(jetty);
      }

      int index = random.nextInt(runners.size());
      return runners.get(index);
    } catch (NoOpenOverseerFoundException e) {
      int index = random.nextInt(jettys.size());
      return jettys.get(index);
    }
  }

  /**
   * Start a new Solr instance
   *
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   *
   * @return new Solr instance
   *
   */
  public JettySolrRunner startJettySolrRunner(String name, String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters) throws Exception {
    return startJettySolrRunner(name, hostContext, extraServlets, extraRequestFilters, null);
  }

  /**
   * Start a new Solr instance
   *
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   *
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(String name, String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters, SSLConfig sslConfig) throws Exception {
    return startJettySolrRunner(name, hostContext, JettyConfig.builder()
        .withServlets(extraServlets)
        .withFilters(extraRequestFilters)
        .withSSLConfig(sslConfig)
        .withExecutor(jettyConfig.qtp)
        .build());
  }

  public JettySolrRunner getJettySolrRunner(int index) {
    return jettys.get(index);
  }

  /**
   * Start a new Solr instance on a particular servlet context
   *
   * @param name the instance name
   * @param hostContext the context to run on
   * @param config a JettyConfig for the instance's {@link org.apache.solr.client.solrj.embedded.JettySolrRunner}
   *
   * @return a JettySolrRunner
   */
  public JettySolrRunner startJettySolrRunner(String name, String hostContext, JettyConfig config) throws Exception {
    Path runnerPath = createInstancePath(name);
    String context = getHostContextSuitableForServletContext(hostContext);
    JettyConfig newConfig = JettyConfig.builder(config).setContext(context).build();
    JettySolrRunner jetty = !trackJettyMetrics 
        ? new JettySolrRunner(runnerPath.toString(), newConfig)
         :new JettySolrRunnerWithMetrics(runnerPath.toString(), newConfig);
    jetty.start(true, true);
    jettys.add(jetty);
    synchronized (startupWait) {
      startupWait.notifyAll();
    }
    return jetty;
  }
  
  /**
   * Start a new Solr instance, using the default config
   *
   * @return a JettySolrRunner
   */
  public JettySolrRunner startJettySolrRunner() throws Exception {
    return startJettySolrRunner(newNodeName(), jettyConfig.context, jettyConfig);
  }

  /**
   * Stop a Solr instance
   * @param index the index of node in collection returned by {@link #getJettySolrRunners()}
   * @return the shut down node
   */
  public JettySolrRunner stopJettySolrRunner(int index) throws Exception {
    JettySolrRunner jetty = jettys.get(index);
    jetty.stop();
    jettys.remove(index);
    return jetty;
  }

  /**
   * Add a previously stopped node back to the cluster
   * @param jetty a {@link JettySolrRunner} previously returned by {@link #stopJettySolrRunner(int)}
   * @return the started node
   * @throws Exception on error
   */
  public JettySolrRunner startJettySolrRunner(JettySolrRunner jetty) throws Exception {
    jetty.start(true, false);
    if (!jettys.contains(jetty)) jettys.add(jetty);
    return jetty;
  }

  /**
   * Stop the given Solr instance. It will be removed from the cluster's list of running instances.
   * @param jetty a {@link JettySolrRunner} to be stopped
   * @return the same {@link JettySolrRunner} instance provided to this method
   * @throws Exception on error
   */
  public JettySolrRunner stopJettySolrRunner(JettySolrRunner jetty) throws Exception {
    return stopJettySolrRunner(jetty,true );
  }

  /**
   * Stop the given Solr instance. It will be removed from the cluster's list of running instances.
   * @param jetty a {@link JettySolrRunner} to be stopped
   * @return the same {@link JettySolrRunner} instance provided to this method
   * @throws Exception on error
   */
  public JettySolrRunner stopJettySolrRunner(JettySolrRunner jetty, boolean wait) throws Exception {
    jetty.stop(wait);
    jettys.remove(jetty);
    return jetty;
  }

  public void uploadConfigSet(Path configDir, String configName)
          throws IOException, KeeperException, InterruptedException {
    ZkConfigManager manager = zkStateReader.getConfigManager();
    manager.uploadConfigDir(configDir, configName);
  }

  /**
   * Upload a config set
   * @param configDir a path to the config set to upload
   * @param configName the name to give the configset
   */
  public void uploadConfigSet(Path configDir, String configName, String rootZkNode)
      throws IOException, KeeperException, InterruptedException {
    ZkConfigManager manager = new ZkConfigManager(zkServer.getZkClient(), rootZkNode);
    manager.uploadConfigDir(configDir, configName);
  }

  /** Delete all collections (and aliases) */
  public void deleteAllCollections() throws Exception {
    ZkStateReader reader = solrClient.getZkStateReader();

    reader.aliasesManager.applyModificationAndExportToZk(aliases -> Aliases.EMPTY);

    final Set<String> collections = reader.getClusterState().getCollectionsMap().keySet();
    try (ParWork work = new ParWork(this, false, false)) {
      collections.forEach(collection -> {
         work.collect("", ()->{
           try {
             CollectionAdminRequest.deleteCollection(collection).process(solrClient);
           } catch (Exception e) {
             throw new SolrException(ErrorCode.SERVER_ERROR, e);
           }
         });
      });
    }
  }
  
  public void deleteAllConfigSets() throws SolrServerException, IOException {

    List<String> configSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();

    for (String configSet : configSetNames) {
      if (configSet.equals("_default")) {
        continue;
      }
      new ConfigSetAdminRequest.Delete()
          .setConfigSetName(configSet)
          .process(solrClient);
    }
  }

  /**
   * Shut down the cluster, including all Solr nodes and ZooKeeper
   */
  public synchronized void shutdown() throws Exception {
    try {
      List<Callable<JettySolrRunner>> shutdowns = new ArrayList<>(jettys.size());
      for (final JettySolrRunner jetty : jettys) {
        shutdowns.add(() -> stopJettySolrRunner(jetty, true));
      }
      jettys.clear();

      try (ParWork parWork = new ParWork(this, false, false)) {
        parWork.collect(shutdowns);
      }

      IOUtils.closeQuietly(solrClient);

      IOUtils.closeQuietly(zkStateReader);

      if (!externalZkServer) {
        IOUtils.closeQuietly(zkServer);
      }

    } finally {
      System.clearProperty("zkHost");
      solrClient = null;
      assert ObjectReleaseTracker.release(this);
    }

  }

  public Path getBaseDir() {
    return baseDir;
  }

  public CloudHttp2SolrClient getSolrClient() {
    return solrClient;
  }

  public SolrZkClient getZkClient() {
    return solrZkClient;
  }

  protected CloudHttp2SolrClient buildZkReaderAndSolrClient() {
    // return new CloudHttp2SolrClient.Builder(Collections.singletonList(zkServer.getZkHost()), Optional.of("/solr")).build();
    zkStateReader = new ZkStateReader(zkServer.getZkAddress(), 15000, 30000);
    zkStateReader.createClusterStateWatchersAndUpdate();
    return new CloudHttp2SolrClient.Builder(zkStateReader).build();
  }

  public CloudHttp2SolrClient buildSolrClient() {
    return new CloudHttp2SolrClient.Builder(Collections.singletonList(zkServer.getZkHost()), Optional.of("/solr")).build();
  }

  private static String getHostContextSuitableForServletContext(String ctx) {
    if (ctx == null || "".equals(ctx)) ctx = "/solr";
    if (ctx.endsWith("/")) ctx = ctx.substring(0,ctx.length()-1);
    if (!ctx.startsWith("/")) ctx = "/" + ctx;
    return ctx;
  }

  private Exception checkForExceptions(String message, Collection<Future<JettySolrRunner>> futures) throws InterruptedException {
    Exception parsed = new Exception(message);
    boolean ok = true;
    for (Future<JettySolrRunner> future : futures) {
      try {
        future.get();
      }
      catch (ExecutionException e) {
        parsed.addSuppressed(e.getCause());
        ok = false;
      }
      catch (InterruptedException e) {
        Thread.interrupted();
        throw e;
      }
    }
    return ok ? null : parsed;
  }

  /**
   * Return the jetty that a particular replica resides on
   */
  public JettySolrRunner getReplicaJetty(Replica replica) {
    for (JettySolrRunner jetty : jettys) {
      if (replica.getCoreUrl().startsWith(jetty.getBaseUrl()))
        return jetty;
    }
    for (JettySolrRunner jetty : jettys) {
      if (replica.getCoreUrl().startsWith(jetty.getProxyBaseUrl()))
        return jetty;
    }
    throw new IllegalArgumentException("Cannot find Jetty for a replica with core url " + replica.getCoreUrl());
  }

  public JettySolrRunner getJetty(String baseUrl) {
    for (JettySolrRunner jetty : jettys) {
      if (baseUrl.equals(jetty.getBaseUrl()))
        return jetty;
    }
    throw new IllegalArgumentException("Cannot find Jetty with baseUrl " + baseUrl + " " + jettys);
  }

  protected SocketProxy getProxyForReplica(Replica replica) throws Exception {
    String replicaBaseUrl = replica.getBaseUrl();

    for (JettySolrRunner j : jettys) {
      if (replicaBaseUrl.replaceAll("/$", "").equals(j.getProxyBaseUrl().replaceAll("/$", ""))) {
        if (j.getProxy() == null) {
          throw new IllegalStateException("proxy is not enabled for " + JettySolrRunner.class.getSimpleName());
        }
        return j.getProxy();
      }
    }

    throw new IllegalArgumentException("No proxy found for replica " + replica.getName());
  }

  /**
   * Make the zookeeper session on a particular jetty expire
   */
  public void expireZkSession(JettySolrRunner jetty) {
    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      SolrZkClient zkClient = cores.getZkController().getZkClient();
      zkClient.getSolrZooKeeper().closeCnxn();
      long sessionId = zkClient.getSessionId();
      zkServer.expire(sessionId);
      if (log.isInfoEnabled()) {
        log.info("Expired zookeeper session {} from node {}", sessionId, jetty.getBaseUrl());
      }
    }
  }

  public  void injectChaos(Random random) throws Exception {
    if (LuceneTestCase.TEST_NIGHTLY && false) { // MRM TODO:
      synchronized (this) {
        // sometimes we restart one of the jetty nodes
        if (random.nextBoolean()) {
          JettySolrRunner jetty = jettys.get(random.nextInt(jettys.size()));
          jetty.stop();
          log.info("============ Restarting jetty");
          jetty.start();
        }

        // sometimes we restart zookeeper
        if (random.nextBoolean()) {
          zkServer.shutdown();
          log.info("============ Restarting zookeeper");
          zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
          zkServer.run(false);
        }

        // sometimes we cause a connection loss - sometimes it will hit the overseer
        if (random.nextBoolean()) {
          JettySolrRunner jetty = jettys.get(random.nextInt(jettys.size()));
          ChaosMonkey.causeConnectionLoss(jetty);
        }
      }
    }
  }

  public JettySolrRunner getCurrentOverseerJetty() throws NoOpenOverseerFoundException {
    for (int i = 0; i < jettys.size(); i++) {
      JettySolrRunner runner = getJettySolrRunner(i);
      if (runner.getCoreContainer() != null) {
        if (!runner.getCoreContainer().getZkController().getOverseer().isClosed()) {
          return runner;
        }
      }
    }

    throw new NoOpenOverseerFoundException("No open overseer found");
  }

  public Replica getNonLeaderReplica(String collection) {

    DocCollection coll = solrClient.getZkStateReader().getClusterState().getCollection(collection);
    if (coll != null) {
      for (Replica replica : coll.getReplicas()) {
        if (replica.getStr("leader") == null) {
          return replica;
        }
      }
    }
    throw new IllegalArgumentException("Could not find suitable Replica");
  }

  public Overseer getOpenOverseer() {
    List<Overseer> overseers = new ArrayList<>();
    for (int i = 0; i < jettys.size(); i++) {
      JettySolrRunner runner = getJettySolrRunner(i);
      if (runner.getCoreContainer() != null) {
        overseers.add(runner.getCoreContainer().getZkController().getOverseer());
      }
    }

    return getOpenOverseer(overseers);
  }
  
  public static Overseer getOpenOverseer(List<Overseer> overseers) {
    ArrayList<Overseer> shuffledOverseers = new ArrayList<Overseer>(overseers);
    Collections.shuffle(shuffledOverseers, LuceneTestCase.random());
    for (Overseer overseer : shuffledOverseers) {
      if (!overseer.isClosed()) {
        return overseer;
      }
    }
    throw new SolrException(ErrorCode.NOT_FOUND, "No open Overseer found");
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas) {
    waitForActiveCollection(collection, wait, unit, shards, totalReplicas, false);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas, boolean exact) {
    zkStateReader.waitForActiveCollection(collection, wait, unit, shards, totalReplicas, exact);
  }

  public void waitForActiveCollection(String collection, int shards, int totalReplicas) {
    if (collection == null) throw new IllegalArgumentException("null collection");
    waitForActiveCollection(collection,  10, TimeUnit.SECONDS, shards, totalReplicas);
  }

  public void waitForActiveCollection(String collection, int shards, int totalReplicas, boolean exact) {

    waitForActiveCollection(collection,  10, TimeUnit.SECONDS, shards, totalReplicas, exact);
  }

  public void waitForJettyToStop(JettySolrRunner runner) throws TimeoutException {
    if (log.isInfoEnabled()) {
      log.info("waitForJettyToStop: {}", runner.getLocalPort());
    }
    String nodeName = runner.getNodeName();
    if (nodeName == null) {
      if (log.isInfoEnabled()) {
        log.info("Cannot wait for Jetty with null node name");}
      return;
    }

    if (log.isInfoEnabled()) {
      log.info("waitForNode: {}", runner.getNodeName());
    }


    ZkStateReader reader = getSolrClient().getZkStateReader();

    try {
      reader.waitForLiveNodes(10, TimeUnit.SECONDS, (n) -> !n.contains(nodeName));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, "interrupted", e);
    }
  }

  public JettySolrRunner getJettyForShard(String collection, String shard) {
   return getJettyForShard(collection, shard, 0);
  }

  public JettySolrRunner getJettyForShard(String collection, String shard, int index) {

    DocCollection coll = solrClient.getZkStateReader().getClusterState().getCollection(collection);
    if (coll != null) {
      return getReplicaJetty((Replica) coll.getSlice(shard).getReplicas().toArray()[index]);

    }
    throw new IllegalArgumentException("Could not find suitable Replica");
  }

  public List<JettySolrRunner> getJettysForShard(String collection, String slice) {
    List<JettySolrRunner> jettys = new ArrayList<>();
    DocCollection coll = solrClient.getZkStateReader().getClusterState().getCollection(collection);
    if (coll != null) {
      Slice replicas = coll.getSlice(slice);
      for (Replica replica : replicas) {
        JettySolrRunner jetty = getReplicaJetty(replica);
        if (!jettys.contains(jetty)) {
          jettys.add(jetty);
        }
      }
      return jettys;
    }
    throw new IllegalArgumentException("Could not find suitable Replica");
  }

  public JettySolrRunner getShardLeaderJetty(String collection, String shard) {
    DocCollection coll = solrClient.getZkStateReader().getClusterState().getCollection(collection);
    if (coll != null) {
      Slice slice = coll.getSlice(shard);
      if (slice != null) {
         return getReplicaJetty(slice.getLeader());
      }
    }
    return null;
  }

  public void stopJettyRunners() {
    List<JettySolrRunner> runners = getJettySolrRunners();
    try (ParWork work = new ParWork("StopJettyRunners", false, false)) {
      for (JettySolrRunner runner : runners) {
        work.collect("", () -> {
          try {
            runner.stop();
          } catch (Exception e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        });
      }
    }
  }

  public void startJettyRunners() {
    List<JettySolrRunner> runners = getJettySolrRunners();
    try (ParWork work = new ParWork("StopJettyRunners", false, false)) {
      for (JettySolrRunner runner : runners) {
        work.collect("", () -> {
          try {
            runner.start();
          } catch (Exception e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        });
      }
    }
  }

  /** @lucene.experimental */
  public static final class JettySolrRunnerWithMetrics extends JettySolrRunner {
    public JettySolrRunnerWithMetrics(String solrHome, JettyConfig config) {
      super(solrHome, config);
    }

    private volatile MetricRegistry metricRegistry;

    @Override
    protected HandlerWrapper injectJettyHandlers(HandlerWrapper chain) {
      metricRegistry = new MetricRegistry();
      com.codahale.metrics.jetty9.InstrumentedHandler metrics 
          = new com.codahale.metrics.jetty9.InstrumentedHandler(
               metricRegistry);
      metrics.setHandler(chain);
      return metrics;
    }

    /** @return optional subj. It may be null, if it's not yet created. */
    public MetricRegistry getMetricRegistry() {
      return metricRegistry;
    }
  }

}
