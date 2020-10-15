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

package org.apache.solr.cloud.autoscaling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkDistributedQueueFactory;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetricTriggerTest extends SolrCloudTestCase {

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the listener to fire on first run!");
    return true;
  };

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME,
        "conf", 1, 1);
    CloudSolrClient solrClient = cluster.getSolrClient();
    create.setMaxShardsPerNode(1);
    create.process(solrClient);
    cluster.waitForActiveCollection(DEFAULT_TEST_COLLECTION_NAME, 1, 1);
  }

  @Test
  public void test() throws Exception {
    CoreDescriptor coreDescriptor = cluster.getJettySolrRunner(0).getCoreContainer().getCoreDescriptors().iterator().next();
    String shardId = coreDescriptor.getCloudDescriptor().getShardId();
    String coreName = coreDescriptor.getName();
    String replicaName = Utils.parseMetricsReplicaName(DEFAULT_TEST_COLLECTION_NAME, coreName);
    long waitForSeconds = 2 + random().nextInt(5);
    String registry = SolrCoreMetricManager.createRegistryName(true, DEFAULT_TEST_COLLECTION_NAME, shardId, replicaName, null);
    String tag = "metrics:" + registry + ":ADMIN./admin/file.requests";

    Map<String, Object> props = createTriggerProps(waitForSeconds, tag, 1.0d, null, DEFAULT_TEST_COLLECTION_NAME, null, null);

    final List<TriggerEvent> events = new ArrayList<>();
    SolrZkClient zkClient = cluster.getSolrClient().getZkStateReader().getZkClient();
    SolrResourceLoader loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
    try (SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkClient), cluster.getSolrClient())) {
      try (MetricTrigger metricTrigger = new MetricTrigger("metricTrigger")) {
        metricTrigger.configure(loader, cloudManager, props);
        metricTrigger.setProcessor(noFirstRunProcessor);
        metricTrigger.run();
        metricTrigger.setProcessor(event -> events.add(event));
        assertEquals(0, events.size());
        Thread.sleep(waitForSeconds * 1000 + 2000);
        metricTrigger.run();
        assertEquals(1, events.size());
      }

      events.clear();
      tag = "metrics:" + registry + ":ADMIN./admin/file.handlerStart";
      props = createTriggerProps(waitForSeconds, tag, null, 100.0d, DEFAULT_TEST_COLLECTION_NAME, null, null);
      try (MetricTrigger metricTrigger = new MetricTrigger("metricTrigger")) {
        metricTrigger.configure(loader, cloudManager, props);
        metricTrigger.setProcessor(noFirstRunProcessor);
        metricTrigger.run();
        metricTrigger.setProcessor(event -> events.add(event));
        assertEquals(0, events.size());
        Thread.sleep(waitForSeconds * 1000 + 2000);
        metricTrigger.run();
        assertEquals(1, events.size());
      }
    }
  }

  private Map<String, Object> createTriggerProps(long waitForSeconds, String metric, Double below, Double above, String collection, String shard, String node) {
    Map<String, Object> props = new HashMap<>();
    props.put("metric", metric);
    if (above != null) {
      props.put("above", above);
    }
    if (below != null) {
      props.put("below", below);
    }
    if (collection != null) {
      props.put("collection", collection);
    }
    if (shard != null) {
      props.put("shard", shard);
    }
    if (node != null) {
      props.put("node", node);
    }
    props.put("event", "metric");
    props.put("waitFor", waitForSeconds);
    props.put("enabled", true);

    List<Map<String, String>> actions = new ArrayList<>(3);
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    actions.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    actions.add(map);
    props.put("actions", actions);
    return props;
  }
}
