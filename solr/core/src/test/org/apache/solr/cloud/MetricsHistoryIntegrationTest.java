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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@LuceneTestCase.Slow
@LogLevel("org.apache.solr.handler.admin=DEBUG")
public class MetricsHistoryIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    boolean simulated = TEST_NIGHTLY ? random().nextBoolean() : true;
    if (simulated) {
      cloudManager = SimCloudManager.createCluster(1, TimeSource.get("simTime:50"));
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
    }
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    if (!simulated) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      solrClient = cluster.getSolrClient();
    }
    timeSource = cloudManager.getTimeSource();
    // create .system
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 1)
        .process(solrClient);
    CloudUtil.waitForState(cloudManager, CollectionAdminParams.SYSTEM_COLL,
        30, TimeUnit.SECONDS, CloudUtil.clusterShape(1, 1));
    solrClient.query(CollectionAdminParams.SYSTEM_COLL, params(CommonParams.Q, "*:*"));
    // sleep a little to allow the handler to collect some metrics
    if (simulated) {
      timeSource.sleep(90000);
    } else {
      timeSource.sleep(100000);
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (cloudManager instanceof SimCloudManager) {
      cloudManager.close();
    }
    solrClient = null;
    cloudManager = null;
  }

  @Test
  public void testList() throws Exception {
    NamedList<Object> rsp = solrClient.request(createHistoryRequest(params(CommonParams.ACTION, "list")));
    assertNotNull(rsp);
    // expected solr.jvm, solr.node and solr.collection..system
    @SuppressWarnings({"unchecked"})
    SimpleOrderedMap<Object> lst = (SimpleOrderedMap<Object>) rsp.get("metrics");
    assertNotNull(lst);
    assertEquals(lst.toString(), 3, lst.size());
    assertNotNull(lst.toString(), lst.get("solr.jvm"));
    assertNotNull(lst.toString(), lst.get("solr.node"));
    assertNotNull(lst.toString(), lst.get("solr.collection..system"));
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testStatus() throws Exception {
    NamedList<Object> rsp = solrClient.request(createHistoryRequest(
        params(CommonParams.ACTION, "status", CommonParams.NAME, "solr.jvm")));
    assertNotNull(rsp);
    NamedList<Object> map = (NamedList<Object>)rsp.get("metrics");
    assertEquals(map.toString(), 1, map.size());
    map = (NamedList<Object>)map.get("solr.jvm");
    assertNotNull(map);
    NamedList<Object> status = (NamedList<Object>)map.get("status");
    assertNotNull(status);
    assertEquals(status.toString(), 7, status.size());
    List<Object> lst = (List<Object>)status.get("datasources");
    assertNotNull(lst);
    assertEquals(lst.toString(), 3, lst.size());
    lst = (List<Object>)status.get("archives");
    assertNotNull(lst);
    assertEquals(lst.toString(), 5, lst.size());
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testGet() throws Exception {
    NamedList<Object> rsp = solrClient.request(createHistoryRequest(params(
        CommonParams.ACTION, "get", CommonParams.NAME, "solr.jvm")));
    assertNotNull(rsp);
    // default format is LIST
    NamedList<Object> data = (NamedList<Object>)rsp.findRecursive("metrics", "solr.jvm", "data");
    assertNotNull(data);
    data.forEach((k, v) -> {
      NamedList<Object> entry = (NamedList<Object>)v;
      List<Object> lst = entry.getAll("timestamps");
      assertNotNull(lst);
      assertTrue("timestamps", lst.size() > 0);
      // 3 metrics, so the total size of values is 3 * the size of timestamps
      entry = (NamedList<Object>)entry.get("values");
      assertNotNull(entry);
      assertEquals(lst.size() * 3, entry.size());
    });

    // get STRING
    rsp = solrClient.request(createHistoryRequest(params(
        CommonParams.ACTION, "get", CommonParams.NAME, "solr.jvm", "format", "string")));
    data = (NamedList<Object>)rsp.findRecursive("metrics", "solr.jvm", "data");
    assertNotNull(data);
    data.forEach((k, v) -> {
      NamedList<Object> entry = (NamedList<Object>)v;
      List<Object> lst = entry.getAll("timestamps");
      assertNotNull(lst);
      assertEquals("timestamps", 1, lst.size());
      String timestampString = (String)lst.get(0);
      String[] timestamps = timestampString.split(("\n"));
      assertTrue(timestampString, timestamps.length > 1);
      entry = (NamedList<Object>)entry.get("values");
      assertNotNull(entry);
      assertEquals(3, entry.size());
      entry.forEach((vk, vv) -> {
        String valString = (String)vv;
        String[] values = valString.split("\n");
        assertEquals(valString, timestamps.length, values.length);
      });
    });

    // get GRAPH
    rsp = solrClient.request(createHistoryRequest(params(
        CommonParams.ACTION, "get", CommonParams.NAME, "solr.jvm", "format", "graph")));
    data = (NamedList<Object>)rsp.findRecursive("metrics", "solr.jvm", "data");
    assertNotNull(data);
    data.forEach((k, v) -> {
      NamedList<Object> entry = (NamedList<Object>) v;
      entry = (NamedList<Object>)entry.get("values");
      assertNotNull(entry);
      assertEquals(3, entry.size());
      entry.forEach((vk, vv) -> {
        String valString = (String)vv;
        byte[] img = Base64.base64ToByteArray(valString);
        try {
          ImageIO.read(new ByteArrayInputStream(img));
        } catch (IOException e) {
          fail("Error reading image data: " + e.toString());
        }
      });
    });
  }

  @SuppressWarnings({"rawtypes"})
  public static SolrRequest createHistoryRequest(SolrParams params) {
    return new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics/history", params);
  }

}
