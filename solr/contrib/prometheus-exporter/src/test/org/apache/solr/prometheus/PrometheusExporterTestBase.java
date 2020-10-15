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

package org.apache.solr.prometheus;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.prometheus.utils.Helpers;
import org.junit.BeforeClass;

public class PrometheusExporterTestBase extends SolrCloudTestCase {

  public static final String COLLECTION = "collection1";
  public static final String CONF_NAME = COLLECTION + "_config";
  public static final String CONF_DIR = getFile("solr/" + COLLECTION + "/conf").getAbsolutePath();
  public static final int NUM_SHARDS = 2;
  public static final int NUM_REPLICAS = 2;
  public static final int MAX_SHARDS_PER_NODE = 1;
  public static final int NUM_NODES = (NUM_SHARDS * NUM_REPLICAS + (MAX_SHARDS_PER_NODE - 1)) / MAX_SHARDS_PER_NODE;
  public static final int TIMEOUT = 60;

  public static final ImmutableMap<String, Double> FACET_VALUES = ImmutableMap.<String, Double>builder()
      .put("electronics", 14.0)
      .put("currency", 4.0)
      .put("memory", 3.0)
      .put("and", 2.0)
      .put("card", 2.0)
      .put("connector", 2.0)
      .put("drive", 2.0)
      .put("graphics", 2.0)
      .put("hard", 2.0)
      .put("search", 2.0)
      .build();

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NUM_NODES)
        .addConfig(CONF_NAME, getFile(CONF_DIR).toPath())
        .configure();

    CollectionAdminRequest
        .createCollection(COLLECTION, CONF_NAME, NUM_SHARDS, NUM_REPLICAS)
        .setMaxShardsPerNode(MAX_SHARDS_PER_NODE)
        .process(cluster.getSolrClient());

    AbstractDistribZkTestBase
        .waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), true, true, TIMEOUT);

    Helpers.indexAllDocs(cluster.getSolrClient());
  }


}
