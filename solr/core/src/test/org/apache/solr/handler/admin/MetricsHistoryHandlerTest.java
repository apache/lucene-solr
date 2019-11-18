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

package org.apache.solr.handler.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rrd4j.core.RrdDb;

/**
 *
 */
@LogLevel("org.apache.solr.cloud=DEBUG")
public class MetricsHistoryHandlerTest extends SolrCloudTestCase {

  private volatile static SolrCloudManager cloudManager;
  private volatile static SolrMetricManager metricManager;
  private volatile static TimeSource timeSource;
  private volatile static SolrClient solrClient;
  private volatile static boolean simulated;
  private volatile static int SPEED;

  private volatile static MetricsHistoryHandler handler;
  private volatile static MetricsHandler metricsHandler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    simulated = random().nextBoolean();
    Map<String, Object> args = new HashMap<>();
    args.put(MetricsHistoryHandler.SYNC_PERIOD_PROP, 1);
    args.put(MetricsHistoryHandler.COLLECT_PERIOD_PROP, 1);
    if (simulated) {
      SPEED = 50;
      cloudManager = SimCloudManager.createCluster(1, TimeSource.get("simTime:" + SPEED));
      // wait for defaults to be applied - due to accelerated time sometimes we may miss this
      cloudManager.getTimeSource().sleep(10000);
      AutoScalingConfig cfg = cloudManager.getDistribStateManager().getAutoScalingConfig();
      assertFalse("autoscaling config is empty", cfg.isEmpty());
      metricManager = ((SimCloudManager)cloudManager).getMetricManager();
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
      // need to register the factory here, before we start the real cluster
      metricsHandler = new MetricsHandler(metricManager);
      SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, SolrInfoBean.Group.node.toString(), "");
      handler = new MetricsHistoryHandler(cloudManager.getClusterStateProvider().getLiveNodes().iterator().next(),
          metricsHandler, solrClient, cloudManager, args);
      handler.initializeMetrics(solrMetricsContext, CommonParams.METRICS_HISTORY_PATH);
    }
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    
    if (!simulated) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      metricManager = cluster.getJettySolrRunner(0).getCoreContainer().getMetricManager();
      solrClient = cluster.getSolrClient();
      metricsHandler = new MetricsHandler(metricManager);
      handler = new MetricsHistoryHandler(cluster.getJettySolrRunner(0).getNodeName(), metricsHandler, solrClient, cloudManager, args);
      SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, SolrInfoBean.Group.node.toString(), "");
      handler.initializeMetrics(solrMetricsContext, CommonParams.METRICS_HISTORY_PATH);
      SPEED = 1;
    }
    timeSource = cloudManager.getTimeSource();

    // create .system collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL,
        "conf", 1, 1);
    create.process(solrClient);
    CloudUtil.waitForState(cloudManager, "failed to create " + CollectionAdminParams.SYSTEM_COLL,
        CollectionAdminParams.SYSTEM_COLL, CloudUtil.clusterShape(1, 1));
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (handler != null) {
      handler.close();
    }
    if (simulated) {
      cloudManager.close();
    }
  }

  @Test
  //Commented 14-Oct-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 15-Sep-2018
  public void testBasic() throws Exception {
    timeSource.sleep(10000);
    List<Pair<String, Long>> list = handler.getFactory().list(100);
    // solr.jvm, solr.node, solr.collection..system
    assertEquals(list.toString(), 3, list.size());
    for (Pair<String, Long> p : list) {
      RrdDb db = new RrdDb(MetricsHistoryHandler.URI_PREFIX + p.first(), true, handler.getFactory());
      int dsCount = db.getDsCount();
      int arcCount = db.getArcCount();
      assertTrue("dsCount should be > 0, was " + dsCount, dsCount > 0);
      assertEquals("arcCount", 5, arcCount);
      db.close();
    }
  }
}
