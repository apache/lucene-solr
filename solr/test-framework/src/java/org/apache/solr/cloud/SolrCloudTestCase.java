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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
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
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.Before;

/**
 * Base class for SolrCloud tests
 *
 * Derived tests should call {@link #configureCluster(int)} in a {@code BeforeClass}
 * static method.  This configures and starts a {@link MiniSolrCloudCluster}, available
 * via the {@code cluster} variable.  Cluster shutdown is handled automatically.
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

  public static final int DEFAULT_TIMEOUT = 30;

  private static class Config {
    final String name;
    final Path path;

    private Config(String name, Path path) {
      this.name = name;
      this.path = path;
    }
  }

  /**
   * Builder class for a MiniSolrCloudCluster
   */
  protected static class Builder {

    private final int nodeCount;
    private final Path baseDir;
    private String solrxml = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML;
    private JettyConfig jettyConfig = buildJettyConfig("/solr");
    private Optional<String> securityJson = Optional.empty();

    private List<Config> configs = new ArrayList<>();
    private Map<String, String> clusterProperties = new HashMap<>();

    /**
     * Create a builder
     * @param nodeCount the number of nodes in the cluster
     * @param baseDir   a base directory for the cluster
     */
    public Builder(int nodeCount, Path baseDir) {
      this.nodeCount = nodeCount;
      this.baseDir = baseDir;
    }

    /**
     * Use a {@link JettyConfig} to configure the cluster's jetty servers
     */
    public Builder withJettyConfig(JettyConfig jettyConfig) {
      this.jettyConfig = jettyConfig;
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
     * @param configName the config name
     * @param configPath the path to the config files
     */
    public Builder addConfig(String configName, Path configPath) {
      this.configs.add(new Config(configName, configPath));
      return this;
    }

    /**
     * Set a cluster property
     * @param propertyName the property name
     * @param propertyValue the property value
     */
    public Builder withProperty(String propertyName, String propertyValue) {
      this.clusterProperties.put(propertyName, propertyValue);
      return this;
    }

    /**
     * Configure and run the {@link MiniSolrCloudCluster}
     * @throws Exception if an error occurs on startup
     */
    public void configure() throws Exception {
      cluster = new MiniSolrCloudCluster(nodeCount, baseDir, solrxml, jettyConfig, null, securityJson);
      CloudSolrClient client = cluster.getSolrClient();
      for (Config config : configs) {
        ((ZkClientClusterStateProvider)client.getClusterStateProvider()).uploadConfig(config.path, config.name);
      }

      if (clusterProperties.size() > 0) {
        ClusterProperties props = new ClusterProperties(cluster.getSolrClient().getZkStateReader().getZkClient());
        for (Map.Entry<String, String> entry : clusterProperties.entrySet()) {
          props.setClusterProperty(entry.getKey(), entry.getValue());
        }
      }
    }

  }

  /** The cluster */
  protected static MiniSolrCloudCluster cluster;

  protected static SolrZkClient zkClient() {
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    if (reader == null)
      cluster.getSolrClient().connect();
    return cluster.getSolrClient().getZkStateReader().getZkClient();
  }

  /**
   * Call this to configure a cluster of n nodes.
   *
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
      cluster.shutdown();
    }
    cluster = null;
  }

  @Before
  public void checkClusterConfiguration() {
    if (cluster == null)
      throw new RuntimeException("MiniSolrCloudCluster not configured - have you called configureCluster().configure()?");
  }

  /* Cluster helper methods ************************************/

  /**
   * Get the collection state for a particular collection
   */
  protected DocCollection getCollectionState(String collectionName) {
    return cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName);
  }

  /**
   * Wait for a particular collection state to appear in the cluster client's state reader
   *
   * This is a convenience method using the {@link #DEFAULT_TIMEOUT}
   *
   * @param message     a message to report on failure
   * @param collection  the collection to watch
   * @param predicate   a predicate to match against the collection state
   */
  protected void waitForState(String message, String collection, CollectionStatePredicate predicate) {
    AtomicReference<DocCollection> state = new AtomicReference<>();
    try {
      cluster.getSolrClient().waitForState(collection, DEFAULT_TIMEOUT, TimeUnit.SECONDS, (n, c) -> {
        state.set(c);
        return predicate.matches(n, c);
      });
    } catch (Exception e) {
      fail(message + "\n" + e.getMessage() + "\nLast available state: " + state.get());
    }
  }

  /**
   * Return a {@link CollectionStatePredicate} that returns true if a collection has the expected
   * number of shards and replicas
   */
  public static CollectionStatePredicate clusterShape(int expectedShards, int expectedReplicas) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null)
        return false;
      if (collectionState.getSlices().size() != expectedShards)
        return false;
      for (Slice slice : collectionState) {
        int activeReplicas = 0;
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            activeReplicas++;
        }
        if (activeReplicas != expectedReplicas)
          return false;
      }
      return true;
    };
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
   *
   * This assumes that the replica is hosted on a live node.
   */
  protected static CoreStatus getCoreStatus(Replica replica) throws IOException, SolrServerException {
    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString(), cluster.getSolrClient().getHttpClient())) {
      return CoreAdminRequest.getCoreStatus(replica.getCoreName(), client);
    }
  }

}
