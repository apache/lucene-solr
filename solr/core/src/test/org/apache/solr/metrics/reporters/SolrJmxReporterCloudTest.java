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
package org.apache.solr.metrics.reporters;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.Query;
import javax.management.QueryExp;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.reporters.jmx.JmxMetricsReporter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrJmxReporterCloudTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static MBeanServer mBeanServer;
  private static String COLLECTION = SolrJmxReporterCloudTest.class.getSimpleName() + "_collection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    // make sure there's an MBeanServer
    mBeanServer = ManagementFactory.getPlatformMBeanServer();
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .setMaxShardsPerNode(2)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
  }
  @AfterClass
  public static void releaseMBeanServer() {
    mBeanServer = null;
  }
  

  @Test
  public void testJmxReporter() throws Exception {
    CollectionAdminRequest.reloadCollection(COLLECTION).processAndWait(cluster.getSolrClient(), 60);
    CloudSolrClient solrClient = cluster.getSolrClient();
    // index some docs
    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "id-" + i);
      solrClient.add(COLLECTION, doc);
    }
    solrClient.commit(COLLECTION);
    // make sure searcher is present
    solrClient.query(COLLECTION, params(CommonParams.Q, "*:*"));

    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      SolrMetricManager manager = runner.getCoreContainer().getMetricManager();
      for (String registry : manager.registryNames()) {
        Map<String, SolrMetricReporter> reporters = manager.getReporters(registry);
        long jmxReporters = reporters.entrySet().stream().filter(e -> e.getValue() instanceof SolrJmxReporter).count();
        reporters.forEach((k, v) -> {
          if (!(v instanceof SolrJmxReporter)) {
            return;
          }
          if (!((SolrJmxReporter)v).getDomain().startsWith("solr.core")) {
            return;
          }
          if (!((SolrJmxReporter)v).isActive()) {
            return;
          }
          QueryExp exp = Query.eq(Query.attr(JmxMetricsReporter.INSTANCE_TAG), Query.value(Integer.toHexString(v.hashCode())));
          Set<ObjectInstance> beans = mBeanServer.queryMBeans(null, exp);
          if (((SolrJmxReporter) v).isStarted() && beans.isEmpty() && jmxReporters < 2) {
            if (log.isInfoEnabled()) {
              log.info("DocCollection: {}", getCollectionState(COLLECTION));
            }
            fail("JMX reporter " + k + " for registry " + registry + " failed to register any beans!");
          } else {
            Set<String> categories = new HashSet<>();
            beans.forEach(bean -> {
              String cat = bean.getObjectName().getKeyProperty("category");
              if (cat != null) {
                categories.add(cat);
              }
            });
            log.info("Registered categories: {}", categories);
            assertTrue("Too few categories: " + categories, categories.size() > 5);
          }
        });
      }
    }
  }
}
