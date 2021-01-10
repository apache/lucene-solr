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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.LiveNodesPredicate;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SolrCloud tests
 * <p>
 * Derived tests should call {@link #configureCluster(int)} in a {@code BeforeClass}
 * static method or {@code Before} setUp method.  This configures and starts a {@link MiniSolrCloudCluster}, available
 * via the {@code cluster} variable.  Cluster shutdown is handled automatically if using {@code BeforeClass}.
 *
 * <pre>
 *   <code>
 *   {@literal @}BeforeClass
 *   public static void setupCluster() {
 *     configureCluster(NUM_NODES)
 *        .addConfig("configname", pathToConfig)
 *        .configure();
 *   }
 *   </code>
 * </pre>
 */
public class SolrCloudTestCase extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static Boolean USE_PER_REPLICA_STATE =  Boolean.FALSE;

  public static final int DEFAULT_TIMEOUT = 45; // this is an important timeout for test stability - can't be too short

  private static class Config {
    final String name;
    final Path path;
    private Config(String name, Path path) {
      this.name = name;
      this.path = path;
    }
  }

  @BeforeClass
  public static void b4Class() {
    USE_PER_REPLICA_STATE = Boolean.parseBoolean(System.getProperty("use.perreplica", String.valueOf(random().nextBoolean())));
    if(USE_PER_REPLICA_STATE) {
      log.info("Using per-replica state");
    }
  }

  /**
   * Builder class for a MiniSolrCloudCluster
   */
  public static class Builder {

    private final int nodeCount;
    private final Path baseDir;
    private String solrxml = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML;
    private JettyConfig.Builder jettyConfigBuilder = JettyConfig.builder().setContext("/solr").withSSLConfig(sslConfig.buildServerSSLConfig());
    private Optional<String> securityJson = Optional.empty();

    private List<Config> configs = new ArrayList<>();
    private Map<String, Object> clusterProperties = new HashMap<>();

    private boolean trackJettyMetrics;

    /**
     * Create a builder
     *
     * @param nodeCount the number of nodes in the cluster
     * @param baseDir   a base directory for the cluster
     */
    public Builder(int nodeCount, Path baseDir) {
      this.nodeCount = nodeCount;
      this.baseDir = baseDir;
    }

    /**
     * Use a JettyConfig.Builder to configure the cluster's jetty servers
     */
    public Builder withJettyConfig(Consumer<JettyConfig.Builder> fun) {
      fun.accept(jettyConfigBuilder);
      return this;
    }

    /**
     * Use the provided string as solr.xml content
     */
    public Builder withSolrXml(String solrXml) {
      this.solrxml = solrXml;
      return this;
    }

    /**
     * Read solr.xml from the provided path
     */
    public Builder withSolrXml(Path solrXml) {
      try {
        this.solrxml = new String(Files.readAllBytes(solrXml), Charset.defaultCharset());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    /**
     * Configure the specified security.json for the {@linkplain MiniSolrCloudCluster}
     *
     * @param securityJson The path specifying the security.json file
     * @return the instance of {@linkplain Builder}
     */
    public Builder withSecurityJson(Path securityJson) {
      try {
        this.securityJson = Optional.of(new String(Files.readAllBytes(securityJson), Charset.defaultCharset()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    /**
     * Configure the specified security.json for the {@linkplain MiniSolrCloudCluster}
     *
     * @param securityJson The string specifying the security.json configuration
     * @return the instance of {@linkplain Builder}
     */
    public Builder withSecurityJson(String securityJson) {
      this.securityJson = Optional.of(securityJson);
      return this;
    }

    /**
     * Upload a collection config before tests start
     *
     * @param configName the config name
     * @param configPath the path to the config files
     */
    public Builder addConfig(String configName, Path configPath) {
      this.configs.add(new Config(configName, configPath));
      return this;
    }

    /**
     * Set a cluster property
     *
     * @param propertyName  the property name
     * @param propertyValue the property value
     */
    public Builder withProperty(String propertyName, String propertyValue) {
      this.clusterProperties.put(propertyName, propertyValue);
      return this;
    }

    public Builder withMetrics(boolean trackJettyMetrics) {
      this.trackJettyMetrics = trackJettyMetrics;
      return this;
    }

    /**
     * Configure and run the {@link MiniSolrCloudCluster}
     *
     * @throws Exception if an error occurs on startup
     */
    public MiniSolrCloudCluster configure() throws Exception {
      return cluster = build();
    }

    /**
     * Configure, run and return the {@link MiniSolrCloudCluster}
     *
     * @throws Exception if an error occurs on startup
     */
    public MiniSolrCloudCluster build() throws Exception {
      JettyConfig jettyConfig = jettyConfigBuilder.build();
      MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(nodeCount, baseDir, solrxml, jettyConfig,
          null, securityJson, trackJettyMetrics);
      CloudSolrClient client = cluster.getSolrClient();
      for (Config config : configs) {
        ((ZkClientClusterStateProvider)client.getClusterStateProvider()).uploadConfig(config.path, config.name);
      }

      if (clusterProperties.size() > 0) {
        ClusterProperties props = new ClusterProperties(cluster.getSolrClient().getZkStateReader().getZkClient());
        for (Map.Entry<String, Object> entry : clusterProperties.entrySet()) {
          props.setClusterProperty(entry.getKey(), entry.getValue());
        }
      }
      return cluster;
    }

    public Builder withDefaultClusterProperty(String key, String value) {
      @SuppressWarnings({"unchecked"})
      HashMap<String, Object> defaults = (HashMap<String, Object>) this.clusterProperties.get(CollectionAdminParams.DEFAULTS);
      if (defaults == null) {
        defaults = new HashMap<>();
        this.clusterProperties.put(CollectionAdminParams.DEFAULTS, defaults);
      }
      @SuppressWarnings({"unchecked"})
      HashMap<String, Object> cluster = (HashMap<String, Object>) defaults.get(CollectionAdminParams.CLUSTER);
      if (cluster == null) {
        cluster = new HashMap<>();
        defaults.put(CollectionAdminParams.CLUSTER, cluster);
      }
      cluster.put(key, value);
      return this;
    }
  }

  /**
   * The cluster
   */
  protected static volatile MiniSolrCloudCluster cluster;

  protected static SolrZkClient zkClient() {
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    if (reader == null)
      cluster.getSolrClient().connect();
    return cluster.getSolrClient().getZkStateReader().getZkClient();
  }

  /**
   * Call this to configure a cluster of n nodes.
   * <p>
   * NB you must call {@link Builder#configure()} to start the cluster
   *
   * @param nodeCount the number of nodes
   */
  protected static Builder configureCluster(int nodeCount) {
    return new Builder(nodeCount, createTempDir());
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    if (cluster != null) {
      try {
        cluster.shutdown();
      } finally {
        cluster = null;
      }
    }
  }

  @Before
  public void checkClusterConfiguration() {

  }

  /* Cluster helper methods ************************************/

  /**
   * Get the collection state for a particular collection
   */
  protected static DocCollection getCollectionState(String collectionName) {
    return cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName);
  }

  protected static void waitForState(String message, String collection, CollectionStatePredicate predicate) {
    waitForState(message, collection, predicate, DEFAULT_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Wait for a particular collection state to appear in the cluster client's state reader
   * <p>
   * This is a convenience method using the {@link #DEFAULT_TIMEOUT}
   *
   * @param message    a message to report on failure
   * @param collection the collection to watch
   * @param predicate  a predicate to match against the collection state
   */
  protected static void waitForState(String message, String collection, CollectionStatePredicate predicate, int timeout, TimeUnit timeUnit) {
    log.info("waitForState ({}): {}", collection, message);
    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      cluster.getSolrClient().waitForState(collection, timeout, timeUnit, (n, c) -> {
        state.set(c);
        liveNodesLastSeen.set(n);
        return predicate.matches(n, c);
      });
    } catch (Exception e) {
      fail(message + "\n" + e.getMessage() + "\nLive Nodes: " + Arrays.toString(liveNodesLastSeen.get().toArray()) + "\nLast available state: " + state.get());
    }
  }

  /**
   * Return a {@link CollectionStatePredicate} that returns true if a collection has the expected
   * number of shards and active replicas
   */
  public static CollectionStatePredicate clusterShape(int expectedShards, int expectedReplicas) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null)
        return false;
      if (collectionState.getSlices().size() != expectedShards)
        return false;
      return compareActiveReplicaCountsForShards(expectedReplicas, liveNodes, collectionState);
    };
  }

  /**
   * Return a {@link CollectionStatePredicate} that returns true if a collection has the expected
   * number of active shards and active replicas
   */
  public static CollectionStatePredicate activeClusterShape(int expectedShards, int expectedReplicas) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null)
        return false;
      if (log.isInfoEnabled()) {
        log.info("active slice count: {} expected: {}", collectionState.getActiveSlices().size(), expectedShards);
      }
      if (collectionState.getActiveSlices().size() != expectedShards)
        return false;
      return compareActiveReplicaCountsForShards(expectedReplicas, liveNodes, collectionState);
    };
  }

  public static LiveNodesPredicate containsLiveNode(String node) {
    return (oldNodes, newNodes) -> {
      return newNodes.contains(node);
    };
  }

  public static LiveNodesPredicate missingLiveNode(String node) {
    return (oldNodes, newNodes) -> {
      return !newNodes.contains(node);
    };
  }

  public static LiveNodesPredicate missingLiveNodes(List<String> nodes) {
    return (oldNodes, newNodes) -> {
      boolean success = true;
      for (String lostNodeName : nodes) {
        if (newNodes.contains(lostNodeName)) {
          success = false;
          break;
        }
      }
      return success;
    };
  }

  private static boolean compareActiveReplicaCountsForShards(int expectedReplicas, Set<String> liveNodes, DocCollection collectionState) {
    int activeReplicas = 0;
    for (Slice slice : collectionState) {
      for (Replica replica : slice) {
        if (replica.isActive(liveNodes)) {
          activeReplicas++;
        }
      }
    }

    log.info("active replica count: {} expected replica count: {}", activeReplicas, expectedReplicas);

    return activeReplicas == expectedReplicas;

  }

  /**
   * Get a (reproducibly) random shard from a {@link DocCollection}
   */
  protected static Slice getRandomShard(DocCollection collection) {
    List<Slice> shards = new ArrayList<>(collection.getActiveSlices());
    if (shards.size() == 0)
      fail("Couldn't get random shard for collection as it has no shards!\n" + collection.toString());
    Collections.shuffle(shards, random());
    return shards.get(0);
  }

  /**
   * Get a (reproducibly) random replica from a {@link Slice}
   */
  protected static Replica getRandomReplica(Slice slice) {
    List<Replica> replicas = new ArrayList<>(slice.getReplicas());
    if (replicas.size() == 0)
      fail("Couldn't get random replica from shard as it has no replicas!\n" + slice.toString());
    Collections.shuffle(replicas, random());
    return replicas.get(0);
  }

  /**
   * Get a (reproducibly) random replica from a {@link Slice} matching a predicate
   */
  protected static Replica getRandomReplica(Slice slice, Predicate<Replica> matchPredicate) {
    List<Replica> replicas = new ArrayList<>(slice.getReplicas());
    if (replicas.size() == 0)
      fail("Couldn't get random replica from shard as it has no replicas!\n" + slice.toString());
    Collections.shuffle(replicas, random());
    for (Replica replica : replicas) {
      if (matchPredicate.test(replica))
        return replica;
    }
    fail("Couldn't get random replica that matched conditions\n" + slice.toString());
    return null;  // just to keep the compiler happy - fail will always throw an Exception
  }

  /**
   * Get the {@link CoreStatus} data for a {@link Replica}
   * <p>
   * This assumes that the replica is hosted on a live node.
   */
  protected static CoreStatus getCoreStatus(Replica replica) throws IOException, SolrServerException {
    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString(), cluster.getSolrClient().getHttpClient())) {
      return CoreAdminRequest.getCoreStatus(replica.getCoreName(), client);
    }
  }

  @SuppressWarnings({"rawtypes"})
  protected NamedList waitForResponse(Predicate<NamedList> predicate, SolrRequest request, int intervalInMillis, int numRetries, String messageOnFail) {
    log.info("waitForResponse: {}", request);
    int i = 0;
    for (; i < numRetries; i++) {
      try {
        NamedList<Object> response = cluster.getSolrClient().request(request);
        if (predicate.test(response)) return response;
        Thread.sleep(intervalInMillis);
      } catch (RuntimeException rte) {
        throw rte;
      } catch (Exception e) {
        throw new RuntimeException("error executing request", e);
      }
    }
    fail("Tried " + i + " times , could not succeed. " + messageOnFail);
    return null;
  }

  /**
   * Ensure that the given number of solr instances are running. If less instances are found then new instances are
   * started. If extra instances are found then they are stopped.
   *
   * @param nodeCount the number of Solr instances that should be running at the end of this method
   * @throws Exception on error
   */
  public static void ensureRunningJettys(int nodeCount, int timeoutSeconds) throws Exception {
    // ensure that exactly nodeCount jetty nodes are running
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    List<JettySolrRunner> copyOfJettys = new ArrayList<>(jettys);
    int numJetties = copyOfJettys.size();
    for (int i = nodeCount; i < numJetties; i++) {
      cluster.stopJettySolrRunner(copyOfJettys.get(i));
    }
    for (int i = copyOfJettys.size(); i < nodeCount; i++) {
      // start jetty instances
      cluster.startJettySolrRunner();
    }
    // refresh the count from the source
    jettys = cluster.getJettySolrRunners();
    numJetties = jettys.size();
    for (int i = 0; i < numJetties; i++) {
      if (!jettys.get(i).isRunning()) {
        cluster.startJettySolrRunner(jettys.get(i));
      }
    }
    cluster.waitForAllNodes(timeoutSeconds);
  }

  public static Map<String, String> mapReplicasToReplicaType(DocCollection collection) {
    Map<String, String> replicaTypeMap = new HashMap<>();
    for (Slice slice : collection.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String coreUrl = replica.getCoreUrl();
        // It seems replica reports its core URL with a trailing slash while shard
        // info returned from the query doesn't. Oh well. We will include both, just in case
        replicaTypeMap.put(coreUrl, replica.getType().toString());
        if (coreUrl.endsWith("/")) {
          replicaTypeMap.put(coreUrl.substring(0, coreUrl.length() - 1), replica.getType().toString());
        }else {
          replicaTypeMap.put(coreUrl + "/", replica.getType().toString());
        }
      }
    }
    return replicaTypeMap;
  }
}
