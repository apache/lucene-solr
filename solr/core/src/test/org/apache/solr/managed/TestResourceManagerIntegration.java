package org.apache.solr.managed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.ResourcePoolConfig;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.CaffeineCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestResourceManagerIntegration extends SolrCloudTestCase {

  private static ResourceManager resourceManager;
  private static CloudSolrClient solrClient;
  private static SolrCloudManager cloudManager;

  private static final String COLLECTION = TestResourceManagerIntegration.class.getName() + "_collection";
  private static final int NUM_DOCS = 100;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("_default"))
        .configure();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .setMaxShardsPerNode(5)
        .process(cluster.getSolrClient());
    CloudUtil.waitForState(cloudManager, "failed to create collection", COLLECTION, CloudUtil.clusterShape(2, 2));
    resourceManager = cluster.getJettySolrRunner(0).getCoreContainer().getResourceManagerApi().getResourceManager();
    solrClient = cluster.getSolrClient();
    solrClient.setDefaultCollection(COLLECTION);
    // add some docs
    UpdateRequest ureq = new UpdateRequest();
    for (int i = 0; i < NUM_DOCS; i++) {
      ureq.add(new SolrInputDocument("id", "id-" + i, "text_ws", TestUtil.randomAnalysisString(random(), 10, true)));
    }
    ureq.process(solrClient);
    solrClient.commit();
  }

  // pool API
  @Test
  public void testPoolAPI() throws Exception {

    // READ API
    Set<String> expectedPools = resourceManager.getDefaultPoolConfigs().keySet();
    // read all
    {
      V2Request req = new V2Request.Builder("/cluster/resources")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.GET)
          .withParams(params("values", "true", "components", "true", "limits", "true", "params", "true"))
          .build();

      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals(rsp.toString(), expectedPools.size(), result.size());
      expectedPools.forEach(p -> {
        assertNotNull("default pool " + p + " is missing!", result.get(p));
        NamedList<Object> pool = (NamedList<Object>) result.get(p);
        assertTrue(pool.toString(), pool.get("type") != null && pool.get("type").toString().equals("cache"));
        assertTrue(pool.toString(), pool.get("size") != null && ((Number)pool.get("size")).intValue() > 0);
        assertTrue(pool.toString(), pool.get("limits") != null);
        assertTrue(pool.toString(), pool.get("params") != null);
        assertTrue(pool.toString(), pool.get("components") != null);
        assertTrue(pool.toString(), pool.get("values") != null);
      });
    }

    // read one
    {
      String poolName = expectedPools.iterator().next();

      V2Request req = new V2Request.Builder("/cluster/resources/" + poolName)
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.GET)
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals(rsp.toString(), 1, result.size());
      assertNotNull(rsp.toString(), result.get(poolName));
    }

    // WRITE API

    // create
    {
      ResourcePoolConfig config = new ResourcePoolConfig();
      config.name = "testPool";
      config.type = "cache";
      config.poolLimits.put("foo", "bar");
      config.poolLimits.put("baz", 10);
      config.poolParams.put("foo", "bar");

      V2Request req = new V2Request.Builder("/cluster/resources")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("create", config))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertNotNull(result.toString(), result.get("success"));

      config.name = "testPool2";
      req = new V2Request.Builder("/cluster/resources")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("create", config))
          .build();
      rsp = req.process(cluster.getSolrClient());
      result = (NamedList<Object>)rsp.getResponse().get("result");
      assertNotNull(result.toString(), result.get("success"));
    }

    // set params
    {
      Map<String, Object> map = new HashMap<>();
      map.put("foo", null);
      map.put("bar", "xyz");
      map.put("baz", 10);
      V2Request req = new V2Request.Builder("/cluster/resources/testPool")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("setparams", map))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertNotNull("missing pool testPool params: " + result, result.get("testPool"));
      Map<String, Object> newParams = (Map<String, Object>) result.get("testPool");
      assertNull("foo param should be removed: " + newParams, newParams.get("foo"));
      assertEquals(newParams.toString(), "xyz", newParams.get("bar"));
      assertTrue("baz should be a number: " + newParams, newParams.get("baz") instanceof Number);
      assertEquals(newParams.toString(), 10, ((Number)newParams.get("baz")).intValue());
    }

    // set limits
    {
      Map<String, Object> map = new HashMap<>();
      map.put("foo", null);
      map.put("maxSize", -10);
      map.put("maxRamMB", 100);
      map.put("baz", 10);
      V2Request req = new V2Request.Builder("/cluster/resources/testPool")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("setlimits", map))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertNotNull("missing pool testPool limits: " + result, result.get("testPool"));
      Map<String, Object> newLimits = (Map<String, Object>) result.get("testPool");
      assertNull("foo limit should be removed: " + newLimits, newLimits.get("foo"));
      assertEquals(newLimits.toString(), "xyz", newLimits.get("bar"));
      assertTrue("baz should be a number: " + newLimits, newLimits.get("baz") instanceof Number);
      assertEquals(newLimits.toString(), 10, ((Number)newLimits.get("baz")).intValue());
      assertTrue("maxSize should be a number: " + newLimits, newLimits.get("maxSize") instanceof Number);
      assertEquals(newLimits.toString(), -10, ((Number)newLimits.get("maxSize")).intValue());
      assertTrue("maxRamMB should be a number: " + newLimits, newLimits.get("maxRamMB") instanceof Number);
      assertEquals(newLimits.toString(), 100, ((Number)newLimits.get("maxRamMB")).intValue());
    }

    // delete

    // delete by path
    {
      V2Request req = new V2Request.Builder("/cluster/resources/testPool")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", null))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals(result.toString(), "success", result.get("testPool"));
    }
    // delete by payload
    {
      V2Request req = new V2Request.Builder("/cluster/resources")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", "testPool2"))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals(result.toString(), "success", result.get("testPool2"));
    }
    // attempt deleting a predefined pool
    {
      String poolName = expectedPools.iterator().next();
      V2Request req = new V2Request.Builder("/cluster/resources")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", poolName))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals(result.toString(), "ignored - cannot delete default pool", result.get(poolName));
    }
  }

  @Test
  public void testComponentAPI() throws Exception {

    // READ API
    Set<String> expectedPools = resourceManager.getDefaultPoolConfigs().keySet();
    List<String> randomPools = new ArrayList<>(expectedPools);
    Collections.shuffle(randomPools, random());
    String componentsPath = "/node/resources/" + randomPools.get(0) + "/components";
    List<String> componentIds = new ArrayList<>();
    // list
    {
      V2Request req = new V2Request.Builder(componentsPath)
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.GET)
          .withParams(params("values", "true", "limits", "true", "initialLimits", "true"))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      // 4 replicas, 2 of each shard
      assertEquals(result.toString(), 4, result.size());
      result.asMap(10).forEach((cmp, obj) -> {
        // capture component id-s
        componentIds.add(String.valueOf(cmp));

        Map<String, Object> status = (Map<String, Object>) obj;
        assertEquals(status.toString(), CaffeineCache.class.getName(), status.get("class"));
        assertNotNull("limits are missing: " + status.toString(), status.get("limits"));
        Map<String, Object> limits = (Map<String, Object>) status.get("limits");
        assertNotNull("maxSize limit is missing: " + status.toString(), limits.get("maxSize"));
        assertNotNull("maxRamMB limit is missing: " + status.toString(), limits.get("maxRamMB"));
        assertNotNull("initialLimits are missing: " + status.toString(), status.get("initialLimits"));
        assertNotNull("values are missing: " + status.toString(), status.get("values"));
      });
    }

    Collections.shuffle(componentIds, random());

    // WRITE API

    // setlimits for all
    {
      Map<String, Object> limits = new HashMap<>();
      limits.put("maxSize", 11);
      limits.put("maxRamMB", 111);
      V2Request req = new V2Request.Builder(componentsPath)
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("setlimits", limits))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals("exactly 4 components should be present: " + result.toString(), 4, result.size());
      result.asMap(10).forEach((cmp, obj) -> {
        Map<String, Object> newLimits = (Map<String, Object>)obj;
        assertEquals(newLimits.toString(), 11, ((Number) newLimits.get("maxSize")).intValue());
        assertEquals(newLimits.toString(), 111, ((Number) newLimits.get("maxRamMB")).intValue());
      });
    }

    String componentId = componentIds.get(0);
    String prefix = componentId.substring(0, componentId.indexOf("replica"));

    // setlimits for prefix
    {
      Map<String, Object> limits = new HashMap<>();
      limits.put("maxSize", 10);
      limits.put("maxRamMB", 110);
      V2Request req = new V2Request.Builder(componentsPath + "/" + prefix)
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("setlimits", limits))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals("exactly 2 components should be present: " + result.toString(), 2, result.size());
      result.asMap(10).forEach((cmp, obj) -> {
        Map<String, Object> newLimits = (Map<String, Object>)obj;
        assertEquals(newLimits.toString(), 10, ((Number) newLimits.get("maxSize")).intValue());
        assertEquals(newLimits.toString(), 110, ((Number) newLimits.get("maxRamMB")).intValue());
      });
    }

    // delete by prefix
    List<String> removed = new ArrayList<>();
    {
      V2Request req = new V2Request.Builder(componentsPath + "/" + prefix)
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", null))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals("exactly 2 components should be removed: " + result.toString(), 2, result.size());
      result.asMap(10).forEach((cmp, obj) -> {
        assertEquals(result.toString(), "removed", obj);
        removed.add(String.valueOf(cmp));
      });
    }

    // delete by id
    componentIds.removeAll(removed);
    componentId = componentIds.get(0);
    {
      V2Request req = new V2Request.Builder(componentsPath)
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", componentId))
          .build();
      V2Response rsp = req.process(cluster.getSolrClient());
      NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
      assertEquals("exactly 1 component should be removed: " + result.toString(), 1, result.size());
      result.asMap(10).forEach((cmp, obj) -> {
        assertEquals(result.toString(), "removed", obj);
      });
    }
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    cloudManager = null;
    solrClient.close();
    solrClient = null;
    resourceManager = null;
  }
}
