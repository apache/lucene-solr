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
package org.apache.solr.metrics;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.NodeConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class MetricsDisabledCloudTest extends SolrCloudTestCase {

  @BeforeClass
  public static void startCluster() throws Exception {
    System.setProperty("metricsEnabled", "false");
    configureCluster(2).configure();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("test",
        "config", 1, 2);
  }

  @Test
  public void testBasic() throws Exception {
    NodeConfig cfg = cluster.getRandomJetty(random()).getCoreContainer().getNodeConfig();
    MetricsConfig metricsConfig = cfg.getMetricsConfig();
    assertFalse("metrics should be disabled", metricsConfig.isEnabled());
    SolrMetricManager metricManager = cluster.getRandomJetty(random()).getCoreContainer().getMetricManager();
    assertTrue("wrong type of supplier: " + metricManager.getCounterSupplier(),
        metricManager.getCounterSupplier() instanceof MetricSuppliers.NoOpCounterSupplier);
    assertTrue("wrong type of supplier: " + metricManager.getHistogramSupplier(),
        metricManager.getHistogramSupplier() instanceof MetricSuppliers.NoOpHistogramSupplier);
    assertTrue("wrong type of supplier: " + metricManager.getTimerSupplier(),
        metricManager.getTimerSupplier() instanceof MetricSuppliers.NoOpTimerSupplier);
    assertTrue("wrong type of supplier: " + metricManager.getMeterSupplier(),
        metricManager.getMeterSupplier() instanceof MetricSuppliers.NoOpMeterSupplier);
    for (String registryName : metricManager.registryNames()) {
      if (!registryName.startsWith("solr.core.")) {
        continue;
      }
      MetricRegistry registry = metricManager.registry(registryName);
      registry.getMetrics().forEach((name, metric) -> {
        assertTrue("should be NoOpMetric but was: " + name + "=" +
            metric + "(" + metric.getClass() + ")",
            metric instanceof MetricSuppliers.NoOpMetric);
      });
    }
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    shutdownCluster();
  }
}
