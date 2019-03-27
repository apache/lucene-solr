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

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.security.BasicAuthIntegrationTest;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rrd4j.core.RrdDb;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *
 */
@LogLevel("org.apache.solr.cloud=DEBUG")
public class MetricsHistoryHandlerPkiAuthPluginTest extends SolrCloudTestCase {

  private volatile static SolrCloudManager cloudManager;
  private volatile static SolrMetricManager metricManager;
  private volatile static CoreContainer coreContainer;
  private volatile static TimeSource timeSource;
  private volatile static SolrClient solrClient;
  private volatile static boolean simulated;
  private volatile static int SPEED;
  private volatile static Map<String, Object> args;

  private volatile static MetricsHistoryHandler handler;
  private volatile static MetricsHandler metricsHandler;

  @BeforeClass
  public static void beforeClass() throws Exception {
     args = new HashMap<>();
    args.put(MetricsHistoryHandler.SYNC_PERIOD_PROP, 1);
    args.put(MetricsHistoryHandler.COLLECT_PERIOD_PROP, 1);
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    coreContainer = cluster.getJettySolrRunner(0).getCoreContainer();
    cloudManager = coreContainer.getZkController().getSolrCloudManager();
    metricManager = coreContainer.getMetricManager();
    solrClient = cluster.getSolrClient();
    metricsHandler = new MetricsHandler(metricManager);
    handler = new MetricsHistoryHandler(cluster.getJettySolrRunner(0).getNodeName(), metricsHandler, solrClient, cloudManager, args);
    handler.initializeMetrics(metricManager, SolrInfoBean.Group.node.toString(), "", CommonParams.METRICS_HISTORY_PATH);
    SPEED = 1;

    timeSource = cloudManager.getTimeSource();

    // create .system collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL,
        "conf", 1, 1);
    create.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to create " + CollectionAdminParams.SYSTEM_COLL,
        CollectionAdminParams.SYSTEM_COLL, CloudTestUtils.clusterShape(1, 1));
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

  public void runSuccess() throws Exception {
    List<Pair<String, Long>> list = handler.getFactory().list(100);
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

  @Test
  // @BasicAuth(bugUrl="https://issues.apache.org/jira/browse/SOLR-12860") // added 26-Mar-2019
  // Internode communications for the metrics history handler
  public void testAuthenticationBasicUsingPkiAuthPlugin() throws Exception {

    protectConfigsHandler();
    runSuccess();
  }

  private void protectConfigsHandler() throws Exception {
    String authcPrefix = "/admin/authentication";
    String authzPrefix = "/admin/authorization";

    String securityJson = "{\n" +
        "  'authentication':{\n" +
        "    'blockUnknown': true, \n" +
        "    'class':'solr.BasicAuthPlugin',\n" +
        "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
        "  'authorization':{\n" +
        "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
        "    'user-role':{'solr':'admin'},\n" +
        "    'permissions':[{'name':'security-edit','role':'admin'}, {'name':'config-edit','role':'admin'}]}}";

    HttpClient cl = null;
    //Thread.sleep(5000); // Wait server start
    try {
      cl = HttpClientUtil.createClient(null);
      JettySolrRunner randomJetty = cluster.getRandomJetty(random());
      String baseUrl = randomJetty.getBaseUrl().toString();

      zkClient().setData("/security.json", securityJson.replaceAll("'", "\"").getBytes(UTF_8), true);
      BasicAuthIntegrationTest.verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 50);
      BasicAuthIntegrationTest.verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/class", "solr.RuleBasedAuthorizationPlugin", 50);
    } catch (Exception ex){
      throw ex;
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
    }
    Thread.sleep(5000); // TODO: Without a delay, the test fails. Some problem with Authc/Authz framework?
  }


}
