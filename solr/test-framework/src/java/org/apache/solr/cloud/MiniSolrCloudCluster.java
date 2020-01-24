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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.Filter;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.CloudCollectionsListener;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/**
 * "Mini" SolrCloud cluster to be used for testing
 */
public class MiniSolrCloudCluster {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
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
      "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n" +
      "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
      "    <int name=\"leaderVoteWait\">${leaderVoteWait:10000}</int>\n" +
      "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
      "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
      "    <str name=\"zkCredentialsProvider\">${zkCredentialsProvider:org.apache.solr.common.cloud.DefaultZkCredentialsProvider}</str> \n" +
      "    <str name=\"zkACLProvider\">${zkACLProvider:org.apache.solr.common.cloud.DefaultZkACLProvider}</str> \n" +
      "  </solrcloud>\n" +
      "  <metrics>\n" +
      "    <reporter name=\"default\" class=\"org.apache.solr.metrics.reporters.SolrJmxReporter\">\n" +
      "      <str name=\"rootName\">solr_${hostPort:8983}</str>\n" +
      "    </reporter>\n" +
      "  </metrics>\n" +
      "  \n" +
      "</solr>\n";

  private volatile ZkTestServer zkServer; // non-final due to injectChaos()
  private final boolean externalZkServer;
  private final List<JettySolrRunner> jettys = new CopyOnWriteArrayList<>();
  private final Path baseDir;
  private final CloudSolrClient solrClient;
  private final JettyConfig jettyConfig;
  private final boolean trackJettyMetrics;

  private final AtomicInteger nodeIds = new AtomicInteger();


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
         zkTestServer,securityJson, false);
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
      ZkTestServer zkTestServer, Optional<String> securityJson, boolean trackJettyMetrics) throws Exception {

    Objects.requireNonNull(securityJson);
    this.baseDir = Objects.requireNonNull(baseDir);
    this.jettyConfig = Objects.requireNonNull(jettyConfig);
    this.trackJettyMetrics = trackJettyMetrics;

    log.info("Starting cluster of {} servers in {}", numServers, baseDir);

    Files.createDirectories(baseDir);

    this.externalZkServer = zkTestServer != null;
    if (!externalZkServer) {
      Path zkDir = baseDir.resolve("zookeeper/server1/data");
      zkTestServer = new ZkTestServer(zkDir);
      try {
        zkTestServer.run();
      } catch (Exception e) {
        log.error("Error starting Zk Test Server, trying again ...");
        zkTestServer.shutdown();
        zkTestServer = new ZkTestServer(zkDir);
        zkTestServer.run();
      }
    }
    this.zkServer = zkTestServer;

    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT)) {
      zkClient.makePath("/solr/solr.xml", solrXml.getBytes(Charset.defaultCharset()), true);
      if (jettyConfig.sslConfig != null && jettyConfig.sslConfig.isSSLMode()) {
        zkClient.makePath("/solr" + ZkStateReader.CLUSTER_PROPS, "{'urlScheme':'https'}".getBytes(StandardCharsets.UTF_8), true);
      }
      if (securityJson.isPresent()) { // configure Solr security
        zkClient.makePath("/solr/security.json", securityJson.get().getBytes(Charset.defaultCharset()), true);
      }
    }

    // tell solr to look in zookeeper for solr.xml
    System.setProperty("zkHost", zkServer.getZkAddress());

    List<Callable<JettySolrRunner>> startups = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; ++i) {
      startups.add(() -> startJettySolrRunner(newNodeName(), jettyConfig.context, jettyConfig));
    }

    final ExecutorService executorLauncher = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("jetty-launcher"));
    Collection<Future<JettySolrRunner>> futures = executorLauncher.invokeAll(startups);
    ExecutorUtil.shutdownAndAwaitTermination(executorLauncher);
    Exception startupError = checkForExceptions("Error starting up MiniSolrCloudCluster", futures);
    if (startupError != null) {
      try {
        this.shutdown();
      }
      catch (Throwable t) {
        startupError.addSuppressed(t);
      }
      throw startupError;
    }

    solrClient = buildSolrClient();
    
    if (numServers > 0) {
      waitForAllNodes(numServers, 60);
    }

  }

  private void waitForAllNodes(int numServers, int timeoutSeconds) throws IOException, InterruptedException, TimeoutException {
    log.info("waitForAllNodes: numServers={}", numServers);
    
    int numRunning = 0;
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    
    while (true) {
      if (timeout.hasTimedOut()) {
        throw new IllegalStateException("giving up waiting for all jetty instances to be running. numServers=" + numServers
            + " numRunning=" + numRunning);
      }
      numRunning = 0;
      for (JettySolrRunner jetty : getJettySolrRunners()) {
        if (jetty.isRunning()) {
          numRunning++;
        }
      }
      if (numServers == numRunning) {
        break;
      }
      Thread.sleep(100);
    }
    
    ZkStateReader reader = getSolrClient().getZkStateReader();
    for (JettySolrRunner jetty : getJettySolrRunners()) {
      reader.waitForLiveNodes(30, TimeUnit.SECONDS, (o, n) -> n.contains(jetty.getNodeName()));
    }
  }

  public void waitForNode(JettySolrRunner jetty, int timeoutSeconds)
      throws IOException, InterruptedException, TimeoutException {
    log.info("waitForNode: {}", jetty.getNodeName());

    ZkStateReader reader = getSolrClient().getZkStateReader();

    reader.waitForLiveNodes(30, TimeUnit.SECONDS, (o, n) -> n.contains(jetty.getNodeName()));

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
    int index = random.nextInt(jettys.size());
    return jettys.get(index);
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
    jetty.start();
    jettys.add(jetty);
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
    jetty.start(false);
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
    jetty.stop();
    jettys.remove(jetty);
    return jetty;
  }

  /**
   * Upload a config set
   * @param configDir a path to the config set to upload
   * @param configName the name to give the configset
   */
  public void uploadConfigSet(Path configDir, String configName) throws IOException, KeeperException, InterruptedException {
    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null)) {
      ZkConfigManager manager = new ZkConfigManager(zkClient);
      manager.uploadConfigDir(configDir, configName);
    }
  }

  /** Delete all collections (and aliases) */
  public void deleteAllCollections() throws Exception {
    try (ZkStateReader reader = new ZkStateReader(solrClient.getZkStateReader().getZkClient())) {
      final CountDownLatch latch = new CountDownLatch(1);
      reader.registerCloudCollectionsListener(new CloudCollectionsListener() {
        
        @Override
        public void onChange(Set<String> oldCollections, Set<String> newCollections) {
          if (newCollections != null && newCollections.size() == 0) {
            latch.countDown();
          }
        }
      });
      
      reader.createClusterStateWatchersAndUpdate(); // up to date aliases & collections
      reader.aliasesManager.applyModificationAndExportToZk(aliases -> Aliases.EMPTY);
      for (String collection : reader.getClusterState().getCollectionStates().keySet()) {
        CollectionAdminRequest.deleteCollection(collection).process(solrClient);
      }
      
      boolean success = latch.await(60, TimeUnit.SECONDS);
      if (!success) {
        throw new IllegalStateException("Still waiting to see all collections removed from clusterstate.");
      }
      
      for (String collection : reader.getClusterState().getCollectionStates().keySet()) {
        reader.waitForState(collection, 15, TimeUnit.SECONDS, (collectionState) -> collectionState == null ? true : false);
      }
     
    } 
    
    // may be deleted, but may not be gone yet - we only wait to not see it in ZK, not for core unloads
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (true) {
      
      if( timeout.hasTimedOut() ) {
        throw new TimeoutException("Timed out waiting for all collections to be fully removed.");
      }
      
      boolean allContainersEmpty = true;
      for(JettySolrRunner jetty : jettys) {
        CoreContainer cc = jetty.getCoreContainer();
        if (cc != null && cc.getCores().size() != 0) {
          allContainersEmpty = false;
        }
      }
      if (allContainersEmpty) {
        break;
      }
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
  public void shutdown() throws Exception {
    try {
    
      IOUtils.closeQuietly(solrClient);
      List<Callable<JettySolrRunner>> shutdowns = new ArrayList<>(jettys.size());
      for (final JettySolrRunner jetty : jettys) {
        shutdowns.add(() -> stopJettySolrRunner(jetty));
      }
      jettys.clear();
      final ExecutorService executorCloser = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("jetty-closer"));
      Collection<Future<JettySolrRunner>> futures = executorCloser.invokeAll(shutdowns);
      ExecutorUtil.shutdownAndAwaitTermination(executorCloser);
      Exception shutdownError = checkForExceptions("Error shutting down MiniSolrCloudCluster", futures);
      if (shutdownError != null) {
        throw shutdownError;
      }
    } finally {
      try {
        if (!externalZkServer) {
          zkServer.shutdown();
        }
      } finally {
        System.clearProperty("zkHost");
      }
    }
  }

  public Path getBaseDir() {
    return baseDir;
  }

  public CloudSolrClient getSolrClient() {
    return solrClient;
  }

  public SolrZkClient getZkClient() {
    return solrClient.getZkStateReader().getZkClient();
  }
  
  protected CloudSolrClient buildSolrClient() {
    return new Builder(Collections.singletonList(getZkServer().getZkAddress()), Optional.empty())
        .withSocketTimeout(90000).withConnectionTimeout(15000).build(); // we choose 90 because we run in some harsh envs
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
      if (jetty.isStopped()) continue;
      if (replica.getCoreUrl().startsWith(jetty.getBaseUrl().toString()))
        return jetty;
    }
    throw new IllegalArgumentException("Cannot find Jetty for a replica with core url " + replica.getCoreUrl());
  }

  /**
   * Make the zookeeper session on a particular jetty expire
   */
  public void expireZkSession(JettySolrRunner jetty) {
    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      SolrZkClient zkClient = cores.getZkController().getZkClient();
      zkClient.getSolrZooKeeper().closeCnxn();
      long sessionId = zkClient.getSolrZooKeeper().getSessionId();
      zkServer.expire(sessionId);
      log.info("Expired zookeeper session {} from node {}", sessionId, jetty.getBaseUrl());
    }
  }

  public synchronized void injectChaos(Random random) throws Exception {

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
    log.info("waitForActiveCollection: {}", collection);
    CollectionStatePredicate predicate = expectedShardsAndActiveReplicas(shards, totalReplicas);

    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      getSolrClient().waitForState(collection, wait, unit, (n, c) -> {
        state.set(c);
        liveNodesLastSeen.set(n);

        return predicate.matches(n, c);
      });
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException("Failed while waiting for active collection" + "\n" + e.getMessage() + "\nLive Nodes: " + Arrays.toString(liveNodesLastSeen.get().toArray())
          + "\nLast available state: " + state.get());
    }

  }

  public void waitForActiveCollection(String collection, int shards, int totalReplicas) {
    waitForActiveCollection(collection,  30, TimeUnit.SECONDS, shards, totalReplicas);
  }
  
  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null)
        return false;
      if (collectionState.getSlices().size() != expectedShards) {
        return false;
      }
      
      int activeReplicas = 0;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes)) {
            activeReplicas++;
          }
        }
      }
      if (activeReplicas == expectedReplicas) {
        return true;
      }

      return false;
    };
  }

  public void waitForJettyToStop(JettySolrRunner runner) throws TimeoutException {
    log.info("waitForJettyToStop: {}", runner.getLocalPort());
    TimeOut timeout = new TimeOut(15, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while(!timeout.hasTimedOut()) {
      if (runner.isStopped()) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    if (timeout.hasTimedOut()) {
      throw new TimeoutException("Waiting for Jetty to stop timed out");
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
