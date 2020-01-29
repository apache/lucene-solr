package org.apache.solr.managed;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.solr.common.util.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestResourceManagerIntegration extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

    // read all
    V2Request req = new V2Request.Builder("/cluster/resources")
        .forceV2(true)
        .withMethod(SolrRequest.METHOD.GET)
        .withParams(params("values", "true", "components", "true", "limits", "true", "params", "true"))
        .build();

    V2Response rsp = req.process(cluster.getSolrClient());
    Set<String> expectedPools = resourceManager.getDefaultPoolConfigs().keySet();
    NamedList<Object> result = (NamedList<Object>)rsp.getResponse().get("result");
    assertEquals(rsp.toString(), expectedPools.size(), result.size());
    expectedPools.forEach(p -> {
      assertNotNull("default pool " + p + " is missing!", result.get(p));
      NamedList<Object> pool = (NamedList<Object>) result.get(p);
      assertTrue(pool.toString(), pool.get("type") != null && pool.get("type").toString().equals("cache"));
      assertTrue(pool.toString(), pool.get("size") != null && ((Number)pool.get("size")).intValue() > 0);
      assertTrue(pool.toString(), pool.get("poolLimits") != null);
      assertTrue(pool.toString(), pool.get("poolParams") != null);
      assertTrue(pool.toString(), pool.get("components") != null);
      assertTrue(pool.toString(), pool.get("totalValues") != null);
    });

    // read one
    String poolName = expectedPools.iterator().next();

    req = new V2Request.Builder("/cluster/resources/" + poolName)
        .forceV2(true)
        .withMethod(SolrRequest.METHOD.GET)
        .build();
    rsp = req.process(cluster.getSolrClient());
    NamedList<Object> result1 = (NamedList<Object>)rsp.getResponse().get("result");
    assertEquals(rsp.toString(), 1, result1.size());
    assertNotNull(rsp.toString(), result1.get(poolName));

    // create one
    ResourcePoolConfig config = new ResourcePoolConfig();
    config.name = "foo";
    config.type = "cache";
    config.poolLimits.put("foo", "bar");
    config.poolLimits.put("baz", 10);
    config.poolParams.put("foo", "bar");

    req = new V2Request.Builder("/cluster/resources")
        .forceV2(true)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(Collections.singletonMap("create", config))
        .build();
    rsp = req.process(cluster.getSolrClient());
    NamedList<Object> result2 = (NamedList<Object>)rsp.getResponse().get("result");
    assertNotNull(result2.toString(), result2.get("success"));

    // set params
    Map<String, Object> map = new HashMap<>();
    map.put("foo", null);
    map.put("bar", "xyz");
    req = new V2Request.Builder("/cluster/resources/foo")
        .forceV2(true)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(Collections.singletonMap("setparams", map))
        .build();
    rsp = req.process(cluster.getSolrClient());
    NamedList<Object> result3 = (NamedList<Object>)rsp.getResponse().get("result");
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    cloudManager = null;
    solrClient.close();
    solrClient = null;
    resourceManager = null;
  }
}
