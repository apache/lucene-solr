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
package org.apache.solr.prometheus.exporter;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;

/**
 * Test base class.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SolrExporterTestBase extends SolrCloudTestCase {
  public static String COLLECTION = "collection1";
  public static String CONF_NAME = COLLECTION + "_config";
  public static String CONF_DIR = getFile("configsets/" + COLLECTION + "/conf").getAbsolutePath();
  public static int NUM_SHARDS = 2;
  public static int NUM_REPLICAS = 2;
  public static int MAX_SHARDS_PER_NODE = 1;
  public static int NUM_NODES = (NUM_SHARDS * NUM_REPLICAS + (MAX_SHARDS_PER_NODE - 1)) / MAX_SHARDS_PER_NODE;
  public static int TIMEOUT = 60;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES)
        .addConfig(CONF_NAME, getFile(CONF_DIR).toPath())
        .configure();

    CollectionAdminRequest
        .createCollection(COLLECTION, CONF_NAME, NUM_SHARDS, NUM_REPLICAS)
        .setMaxShardsPerNode(MAX_SHARDS_PER_NODE)
        .process(cluster.getSolrClient());

    AbstractDistribZkTestBase
        .waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), true, true, TIMEOUT);
  }
}
