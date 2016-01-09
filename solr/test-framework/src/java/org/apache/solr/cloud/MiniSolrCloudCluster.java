package org.apache.solr.cloud;

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

import javax.servlet.Filter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * "Mini" SolrCloud cluster to be used for testing
 */
public class MiniSolrCloudCluster {
  
  public static final String DEFAULT_CLOUD_SOLR_XML = "<solr>\n" +
      "\n" +
      "  <str name=\"shareSchema\">${shareSchema:false}</str>\n" +
      "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n" +
      "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n" +
      "\n" +
      "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n" +
      "    <str name=\"urlScheme\">${urlScheme:}</str>\n" +
      "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n" +
      "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n" +
      "  </shardHandlerFactory>\n" +
      "\n" +
      "  <solrcloud>\n" +
      "    <str name=\"host\">127.0.0.1</str>\n" +
      "    <int name=\"hostPort\">${hostPort:8983}</int>\n" +
      "    <str name=\"hostContext\">${hostContext:solr}</str>\n" +
      "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n" +
      "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
      "    <int name=\"leaderVoteWait\">10000</int>\n" +
      "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
      "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
      "  </solrcloud>\n" +
      "  \n" +
      "</solr>\n";

  private final ZkTestServer zkServer;
  private final boolean externalZkServer;
  private final List<JettySolrRunner> jettys = new CopyOnWriteArrayList<>();
  private final Path baseDir;
  private final CloudSolrClient solrClient;
  private final JettyConfig jettyConfig;

  private final ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("jetty-launcher"));

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
  public MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig, ZkTestServer zkTestServer) throws Exception {

    this.baseDir = baseDir;
    this.jettyConfig = jettyConfig;

    Files.createDirectories(baseDir);

    this.externalZkServer = zkTestServer != null;
    if (!externalZkServer) {
      String zkDir = baseDir.resolve("zookeeper/server1/data").toString();
      zkTestServer = new ZkTestServer(zkDir);
      zkTestServer.run();
    }
    this.zkServer = zkTestServer;

    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null)) {
      zkClient.makePath("/solr/solr.xml", solrXml.getBytes(Charset.defaultCharset()), true);
      if (jettyConfig.sslConfig != null && jettyConfig.sslConfig.isSSLMode()) {
        zkClient.makePath("/solr" + ZkStateReader.CLUSTER_PROPS, "{'urlScheme':'https'}".getBytes(Charsets.UTF_8), true);
      }
    }

    // tell solr to look in zookeeper for solr.xml
    System.setProperty("zkHost", zkServer.getZkAddress());

    List<Callable<JettySolrRunner>> startups = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; ++i) {
      startups.add(new Callable<JettySolrRunner>() {
        @Override
        public JettySolrRunner call() throws Exception {
          return startJettySolrRunner(newNodeName(), jettyConfig.context, jettyConfig);
        }
      });
    }

    Collection<Future<JettySolrRunner>> futures = executor.invokeAll(startups);
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

    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT, 45000, null)) {
      int numliveNodes = 0;
      int retries = 60;
      String liveNodesPath = "/solr/live_nodes";
      // Wait up to 60 seconds for number of live_nodes to match up number of servers
      do {
        if (zkClient.exists(liveNodesPath, true)) {
          numliveNodes = zkClient.getChildren(liveNodesPath, null, true).size();
          if (numliveNodes == numServers) {
            break;
          }
        }
        retries--;
        if (retries == 0) {
          throw new IllegalStateException("Solr servers failed to register with ZK."
              + " Current count: " + numliveNodes + "; Expected count: " + numServers);
        }

        Thread.sleep(1000);
      } while (numliveNodes != numServers);
    }

    solrClient = buildSolrClient();
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
    JettySolrRunner jetty = new JettySolrRunner(runnerPath.toString(), newConfig);
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

  protected JettySolrRunner startJettySolrRunner(JettySolrRunner jetty) throws Exception {
    jetty.start();
    jettys.add(jetty);
    return jetty;
  }

  protected JettySolrRunner stopJettySolrRunner(JettySolrRunner jetty) throws Exception {
    jetty.stop();
    return jetty;
  }
  
  public void uploadConfigDir(File configDir, String configName) throws IOException, KeeperException, InterruptedException {
    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null)) {
      ZkConfigManager manager = new ZkConfigManager(zkClient);
      manager.uploadConfigDir(configDir.toPath(), configName);
    }
  }
  
  public NamedList<Object> createCollection(String name, int numShards, int replicationFactor, 
      String configName, Map<String, String> collectionProperties) throws SolrServerException, IOException {
    return createCollection(name, numShards, replicationFactor, configName, null, null, collectionProperties);
  }

  public NamedList<Object> createCollection(String name, int numShards, int replicationFactor, 
      String configName, String createNodeSet, String asyncId, Map<String, String> collectionProperties) throws SolrServerException, IOException {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CollectionAction.CREATE.name());
    params.set(CoreAdminParams.NAME, name);
    params.set("numShards", numShards);
    params.set("replicationFactor", replicationFactor);
    params.set("collection.configName", configName);
    if (null != createNodeSet) {
      params.set(OverseerCollectionMessageHandler.CREATE_NODE_SET, createNodeSet);
    }
    if (null != asyncId) {
      params.set(CommonAdminParams.ASYNC, asyncId);
    }
    if(collectionProperties != null) {
      for(Map.Entry<String, String> property : collectionProperties.entrySet()){
        params.set(CoreAdminParams.PROPERTY_PREFIX + property.getKey(), property.getValue());
      }
    }
    
    return makeCollectionsRequest(params);
  }

  public NamedList<Object> deleteCollection(String name) throws SolrServerException, IOException {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CollectionAction.DELETE.name());
    params.set(CoreAdminParams.NAME, name);

    return makeCollectionsRequest(params);
  }

  private NamedList<Object> makeCollectionsRequest(final ModifiableSolrParams params) throws SolrServerException, IOException {
    
    final QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    return solrClient.request(request);
  }

  /**
   * Shut down the cluster, including all Solr nodes and ZooKeeper
   */
  public void shutdown() throws Exception {
    try {
      if (solrClient != null)
        solrClient.close();
      List<Callable<JettySolrRunner>> shutdowns = new ArrayList<>(jettys.size());
      for (final JettySolrRunner jetty : jettys) {
        shutdowns.add(new Callable<JettySolrRunner>() {
          @Override
          public JettySolrRunner call() throws Exception {
            return stopJettySolrRunner(jetty);
          }
        });
      }
      jettys.clear();
      Collection<Future<JettySolrRunner>> futures = executor.invokeAll(shutdowns);
      Exception shutdownError = checkForExceptions("Error shutting down MiniSolrCloudCluster", futures);
      if (shutdownError != null) {
        throw shutdownError;
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(2, TimeUnit.SECONDS);
      try {
        if (!externalZkServer) {
          zkServer.shutdown();
        }
      } finally {
        System.clearProperty("zkHost");
      }
    }
  }
  
  public CloudSolrClient getSolrClient() {
    return solrClient;
  }
  
  protected CloudSolrClient buildSolrClient() {
    return new CloudSolrClient(getZkServer().getZkAddress());
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
}
