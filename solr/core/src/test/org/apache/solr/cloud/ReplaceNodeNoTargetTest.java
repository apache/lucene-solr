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


import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.ReplaceNodeTest.createReplaceNodeRequest;

@LuceneTestCase.AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-11067")
public class ReplaceNodeNoTargetTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(6)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  protected String getSolrXml() {
    return "solr.xml";
  }


  @Test
  @LuceneTestCase.AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-11067")
  public void test() throws Exception {
    String coll = "replacenodetest_coll_notarget";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String node2bdecommissioned = l.get(0);
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'replica':'<5', 'shard': '#EACH', 'node': '#ANY'}]}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    log.info("Creating collection...");
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0);
    cloudClient.request(create);
    cluster.waitForActiveCollection(coll, 5, 10);

    if (log.isInfoEnabled()) {
      log.info("Current core status list for node we plan to decommision: {} => {}",
          node2bdecommissioned,
          getCoreStatusForNamedNode(cloudClient, node2bdecommissioned).getCoreStatus());
      log.info("Decommisioning node: {}", node2bdecommissioned);
    }

    createReplaceNodeRequest(node2bdecommissioned, null, null).processAsync("001", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("001");
    boolean success = false;
    CollectionAdminRequest.RequestStatusResponse rsp = null;
    for (int i = 0; i < 300; i++) {
      rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertFalse("async replace node request aparently failed: " + rsp.toString(),
                  
                  rsp.getRequestStatus() == RequestStatusState.FAILED);
      Thread.sleep(50);
    }
    assertTrue("async replace node request should have finished successfully by now, last status: " + rsp,
               success);
    CoreAdminResponse status = getCoreStatusForNamedNode(cloudClient, node2bdecommissioned);
    assertEquals("Expected no cores for decommisioned node: "
                 + status.getCoreStatus().toString(),
                 0, status.getCoreStatus().size());
  }

  /**
   * Given a cloud client and a nodename, build an HTTP client for that node, and ask it for it's core status
   */
  private CoreAdminResponse getCoreStatusForNamedNode(final CloudSolrClient cloudClient,
                                                      final String nodeName) throws Exception {
    
    try (HttpSolrClient coreclient = getHttpSolrClient
         (cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      return CoreAdminRequest.getStatus(null, coreclient);
    }
  }

}
