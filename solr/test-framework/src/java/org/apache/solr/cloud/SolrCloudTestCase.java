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
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
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

    private List<Config> configs = new ArrayList<>();

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
     * Upload a collection config before tests start
     * @param configName the config name
     * @param configPath the path to the config files
     */
    public Builder addConfig(String configName, Path configPath) {
      this.configs.add(new Config(configName, configPath));
      return this;
    }

    /**
     * Configure and run the {@link MiniSolrCloudCluster}
     * @throws Exception if an error occurs on startup
     */
    public void configure() throws Exception {
      cluster = new MiniSolrCloudCluster(nodeCount, baseDir, solrxml, jettyConfig);
      CloudSolrClient client = cluster.getSolrClient();
      for (Config config : configs) {
        client.uploadConfig(config.path, config.name);
      }
    }

  }

  /** The cluster */
  protected static MiniSolrCloudCluster cluster;

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
    if (cluster != null)
      cluster.shutdown();
  }

  @Before
  public void checkClusterConfiguration() {
    if (cluster == null)
      throw new RuntimeException("MiniSolrCloudCluster not configured - have you called configureCluster().configure()?");
  }

}
