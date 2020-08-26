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
package org.apache.solr.metrics.reporters.solr;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.codahale.metrics.Metric;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrShardReporterTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public SolrShardReporterTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @BeforeClass
  public static void shardReporterBeforeClass() {
    System.setProperty("solr.allowPaths", "*");
  }

  @AfterClass
  public static void shardReporterAfterClass() {
    System.clearProperty("solr.allowPaths");
  }

  @Override
  public String getSolrXml() {
    return "solr-solrreporter.xml";
  }

  @Test
  public void test() throws Exception {
    waitForRecoveriesToFinish("control_collection",
        jettys.get(0).getCoreContainer().getZkController().getZkStateReader(), false);
    waitForRecoveriesToFinish("collection1",
        jettys.get(0).getCoreContainer().getZkController().getZkStateReader(), false);
    printLayout();
    // wait for at least two reports
    Thread.sleep(10000);
    ClusterState state = jettys.get(0).getCoreContainer().getZkController().getClusterState();
    for (JettySolrRunner jetty : jettys) {
      CoreContainer cc = jetty.getCoreContainer();
      SolrMetricManager metricManager = cc.getMetricManager();
      for (final String coreName : cc.getLoadedCoreNames()) {
        CoreDescriptor cd = cc.getCoreDescriptor(coreName);
        if (cd.getCloudDescriptor() == null) { // not a cloud collection
          continue;
        }
        CloudDescriptor cloudDesc = cd.getCloudDescriptor();
        DocCollection docCollection = state.getCollection(cloudDesc.getCollectionName());
        String replicaName = Utils.parseMetricsReplicaName(cloudDesc.getCollectionName(), coreName);
        if (replicaName == null) {
          replicaName = cloudDesc.getCoreNodeName();
        }
        String registryName = SolrCoreMetricManager.createRegistryName(true,
            cloudDesc.getCollectionName(), cloudDesc.getShardId(), replicaName, null);
        String leaderRegistryName = SolrCoreMetricManager.createLeaderRegistryName(true,
            cloudDesc.getCollectionName(), cloudDesc.getShardId());
        boolean leader = cloudDesc.isLeader();
        Slice slice = docCollection.getSlice(cloudDesc.getShardId());
        int numReplicas = slice.getReplicas().size();
        if (leader) {
          assertTrue(metricManager.registryNames() + " doesn't contain " + leaderRegistryName,
              metricManager.registryNames().contains(leaderRegistryName));
          Map<String, Metric> metrics = metricManager.registry(leaderRegistryName).getMetrics();
          metrics.forEach((k, v) -> {
            assertTrue("Unexpected type of " + k + ": " + v.getClass().getName() + ", " + v,
                v instanceof AggregateMetric);
            AggregateMetric am = (AggregateMetric)v;
            if (!k.startsWith("REPLICATION.peerSync")) {
              assertEquals(coreName + "::" + registryName + "::" + k + ": " + am.toString(), numReplicas, am.size());
            }
          });
        } else {
          assertFalse(metricManager.registryNames() + " contains " + leaderRegistryName +
              " but it's not a leader!",
              metricManager.registryNames().contains(leaderRegistryName));
          Map<String, Metric> metrics = metricManager.registry(leaderRegistryName).getMetrics();
          metrics.forEach((k, v) -> {
            assertTrue("Unexpected type of " + k + ": " + v.getClass().getName() + ", " + v,
                v instanceof AggregateMetric);
            AggregateMetric am = (AggregateMetric)v;
            if (!k.startsWith("REPLICATION.peerSync")) {
              assertEquals(coreName + "::" + registryName + "::" + k + ": " + am.toString(), 1, am.size());
            }
          });
        }
        assertTrue(metricManager.registryNames() + " doesn't contain " + registryName,
            metricManager.registryNames().contains(registryName));
      }
    }
    SolrMetricManager metricManager = controlJetty.getCoreContainer().getMetricManager();
    assertTrue(metricManager.registryNames().contains("solr.cluster"));
  }
}
