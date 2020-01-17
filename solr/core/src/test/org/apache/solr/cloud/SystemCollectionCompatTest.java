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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.util.IdUtils;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SystemCollectionCompatTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(2)
        .addConfig("conf1", configset("cloud-managed"))
        .configure();
    if (! log.isWarnEnabled()) {
      fail("Test requires that log-level is at-least WARN, but WARN is disabled");
    }
  }

  private SolrCloudManager cloudManager;
  private CloudSolrClient solrClient;

  @Before
  public void setupSystemCollection() throws Exception {
    ZkController zkController = cluster.getJettySolrRunner(0).getCoreContainer().getZkController();
    cloudManager = zkController.getSolrCloudManager();
    solrClient = new CloudSolrClientBuilder(Collections.singletonList(zkController.getZkServerAddress()),
        Optional.empty()).build();
    CollectionAdminRequest.OverseerStatus status = new CollectionAdminRequest.OverseerStatus();
    CollectionAdminResponse adminResponse = status.process(solrClient);
    String overseerLeader = (String) adminResponse.getResponse().get("leader");
    Set<String> nodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
    nodes.remove(overseerLeader);
    // put .system replicas on other nodes that the overseer
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2)
        .setCreateNodeSet(String.join(",", nodes))
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(CollectionAdminParams.SYSTEM_COLL,  1, 2);
    // send a dummy doc to the .system collection
    SolrInputDocument doc = new SolrInputDocument(
        "id", IdUtils.timeRandomId(),
        CommonParams.TYPE, "dummy");
    doc.addField("time_l", cloudManager.getTimeSource().getEpochTimeNs());
    doc.addField("timestamp", new Date());
    solrClient.add(CollectionAdminParams.SYSTEM_COLL, doc);
    solrClient.commit(CollectionAdminParams.SYSTEM_COLL);

    Map<String, Long> coreStartTimes = new HashMap<>();
    DocCollection coll = cloudManager.getClusterStateProvider().getCollection(CollectionAdminParams.SYSTEM_COLL);
    for (Replica r : coll.getReplicas()) {
      coreStartTimes.put(r.getName(), getCoreStatus(r).getCoreStartTime().getTime());
    }
    // trigger compat report by changing the schema
    SchemaRequest req = new SchemaRequest();
    SchemaResponse rsp = req.process(solrClient, CollectionAdminParams.SYSTEM_COLL);
    Map<String, Object> field = getSchemaField("timestamp", rsp);
    // make some obviously incompatible changes
    field.put("type", "string");
    field.put("docValues", false);
    SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(field);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(solrClient, CollectionAdminParams.SYSTEM_COLL);
    assertEquals(replaceFieldResponse.toString(), 0, replaceFieldResponse.getStatus());
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(CollectionAdminParams.SYSTEM_COLL);
    CollectionAdminResponse response = reloadRequest.process(solrClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    // wait for the reload of all replicas to complete
    RetryUtil.retryUntil("Timed out waiting for core to reload", 30, 1000, TimeUnit.MILLISECONDS, () -> {
      boolean allReloaded = true;
      for (Replica r : coll.getReplicas()) {
        long previousTime = coreStartTimes.get(r.getName());
        try {
          long currentTime = getCoreStatus(r).getCoreStartTime().getTime();
          allReloaded = allReloaded && (previousTime < currentTime);
        } catch (Exception e) {
          log.warn("Error retrieving replica status of " + Utils.toJSONString(r), e);
          allReloaded = false;
        }
      }
      return allReloaded;
    });
    cluster.waitForActiveCollection(CollectionAdminParams.SYSTEM_COLL,  1, 2);

  }

  @After
  public void doAfter() throws Exception {
    log.info("doAfter: deleting all collections...");
    cluster.deleteAllCollections();

    if (null != solrClient) {
      solrClient.close();
      solrClient = null;
    }
  }

  private Map<String, Object> getSchemaField(String name, SchemaResponse schemaResponse) {
    List<Map<String, Object>> fields = schemaResponse.getSchemaRepresentation().getFields();
    for (Map<String, Object> field : fields) {
      if (name.equals(field.get("name"))) {
        return field;
      }
    }
    return null;
  }

  @Test
  public void testBackCompat() throws Exception {
    CollectionAdminRequest.OverseerStatus status = new CollectionAdminRequest.OverseerStatus();
    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminResponse adminResponse = status.process(solrClient);
    NamedList<Object> response = adminResponse.getResponse();
    String leader = (String) response.get("leader");
    log.info("Overseer Status indicates that the overseer is: {}");
    JettySolrRunner overseerNode = null;
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner runner = jettySolrRunners.get(i);
      if (runner.getNodeName().equals(leader)) {
        overseerNode = runner;
        break;
      }
    }
    assertNotNull(overseerNode);
    LogWatcherConfig watcherCfg = new LogWatcherConfig(true, null, "WARN", 100);
    LogWatcher watcher = LogWatcher.newRegisteredLogWatcher(watcherCfg, null);

    watcher.reset();

    // restart Overseer to trigger the back-compat check
    log.info("Stopping Overseer Node: {} ({})", overseerNode.getNodeName(), overseerNode.getLocalPort());
    cluster.stopJettySolrRunner(overseerNode);
    log.info("Waiting for new overseer election...");
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, cloudManager.getTimeSource());
    while (!timeOut.hasTimedOut()) {
      adminResponse = status.process(solrClient);
      response = adminResponse.getResponse();
      String newLeader = (String) response.get("leader");
      if (newLeader != null && !leader.equals(newLeader)) {
        log.info("...new overseer is: {}", newLeader);
        break;
      }
      timeOut.sleep(200);
    }
    if (timeOut.hasTimedOut()) {
      fail("time out waiting for new Overseer leader");
    }

    TimeOut timeOut1 = new TimeOut(60, TimeUnit.SECONDS, cloudManager.getTimeSource());
    boolean foundWarning = false;
    boolean foundSchemaWarning = false;

    // TODO: replace this polling logic with a LogWatcher that uses a queue we can await() on...
    log.info("Polling for log watcher to detect expected log messages...");
    while (!timeOut1.hasTimedOut()) {
      timeOut1.sleep(1000);
      SolrDocumentList history = watcher.getHistory(-1, null);
      for (SolrDocument doc : history) {
        if (!Overseer.class.getName().equals(doc.getFieldValue("logger"))) {
          continue;
        }
        if (doc.getFieldValue("message").toString().contains("re-indexing")) {
          log.info("Found re-indexing message: {}", doc.getFieldValue("message"));
          foundWarning = true;
        }
        if (doc.getFieldValue("message").toString().contains("timestamp")) {
          log.info("Found timestamp message: {}", doc.getFieldValue("message"));
          foundSchemaWarning = true;
        }
      }
      if (foundWarning && foundSchemaWarning) {
        break;
      }
    }
    log.info("Done polling log watcher: foundWarning={} foundSchemaWarning={}", foundWarning, foundSchemaWarning);
    assertTrue("re-indexing warning not found", foundWarning);
    assertTrue("timestamp field incompatibility warning not found", foundSchemaWarning);
  }

}
