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

import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link ScheduledTrigger}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
// 12-Jun-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 26-Mar-2018
public class ScheduledTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch triggerFiredLatch;
  private static final Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static final AtomicReference<Map<String, Object>> actionContextPropertiesRef = new AtomicReference<>();

  @Before
  public void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    
    triggerFiredLatch = new CountDownLatch(1);
  }
  
  @After
  public void afterTest() throws Exception {
    shutdownCluster();
    events.clear();
    actionContextPropertiesRef.set(null);
  }

  @Test
  // commented 15-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testScheduledTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();

    // this collection will place 2 cores on 1st node and 1 core on 2nd node
    String collectionName = "testScheduledTrigger";
    CollectionAdminRequest.createCollection(collectionName, 1, 3)
        .setMaxShardsPerNode(5).process(solrClient);
    
    cluster.waitForActiveCollection(collectionName, 1, 3);

    // create a policy which allows only 1 core per node thereby creating a violation for the above collection
    String setClusterPolicy = "{\n" +
        "  \"set-cluster-policy\" : [\n" +
        "    {\"cores\" : \"<2\", \"node\" : \"#EACH\"}\n" +
        "  ]\n" +
        "}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicy);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // start a new node which can be used to balance the cluster as per policy
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'sched_trigger_integration1'," +
        "'event' : 'scheduled'," +
        "'startTime' : '" + new Date().toInstant().toString() + "'" +
        "'every' : '+3SECONDS'" +
        "'actions' : [" +
        "{'name' : 'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name' : 'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name' : 'recorder', 'class': '" + ContextPropertiesRecorderAction.class.getName() + "'}" +
        "]}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertTrue("ScheduledTrigger did not fire in time", triggerFiredLatch.await(45, TimeUnit.SECONDS));
    assertEquals(1, events.size());
    Map<String, Object> actionContextProps = actionContextPropertiesRef.get();
    assertNotNull(actionContextProps);
    TriggerEvent event = events.iterator().next();
    List<SolrRequest> operations = (List<SolrRequest>) actionContextProps.get("operations");
    assertNotNull(operations);
    assertEquals(1, operations.size());
    for (SolrRequest operation : operations) {
      SolrParams params = operation.getParams();
      assertEquals(newNode.getNodeName(), params.get("targetNode"));
    }
  }

  public static class ContextPropertiesRecorderAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      actionContextPropertiesRef.set(actionContext.getProperties());
      try {
        events.add(event);
        triggerFiredLatch.countDown();
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      }
    }
  }

}
