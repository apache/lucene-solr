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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Counter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link MetricsHandler}
 */
public class MetricsHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {

    initCore("solrconfig-minimal.xml", "schema.xml");
    h.getCoreContainer().waitForLoadingCoresToFinish(30000);

    // manually register & seed some metrics in solr.jvm and solr.jetty for testing via handler
    // (use "solrtest_" prefix just in case the jvm or jetty ads a "foo" metric at some point)
    Counter c = h.getCoreContainer().getMetricManager().counter(null, "solr.jvm", "solrtest_foo");
    c.inc();
    c = h.getCoreContainer().getMetricManager().counter(null, "solr.jetty", "solrtest_foo");
    c.inc(2);
    // test escapes
    c = h.getCoreContainer().getMetricManager().counter(null, "solr.jetty", "solrtest_foo:bar");
    c.inc(3);
  }

  @AfterClass
  public static void cleanupMetrics() throws Exception {
    if (null != h) {
      h.getCoreContainer().getMetricManager().registry("solr.jvm").remove("solrtest_foo");
      h.getCoreContainer().getMetricManager().registry("solr.jetty").remove("solrtest_foo");
      h.getCoreContainer().getMetricManager().registry("solr.jetty").remove("solrtest_foo:bar");
    }
  }

  @Test
  public void test() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json"), resp);
    @SuppressWarnings({"rawtypes"})
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    Object o = nl.get("SEARCHER.new.errors");
    assertNotNull(o); // counter type
    assertTrue(o instanceof MapWriter);
    // response wasn't serialized so we get here whatever MetricUtils produced instead of NamedList
    assertNotNull(((MapWriter) o)._get("count", null));
    assertEquals(0L, ((MapWriter) nl.get("SEARCHER.new.errors"))._get("count", null));
    nl = (NamedList) values.get("solr.node");
    assertNotNull(nl.get("CONTAINER.cores.loaded")); // int gauge
    assertEquals(1, ((MapWriter) nl.get("CONTAINER.cores.loaded"))._get("value", null));
    assertNotNull(nl.get("ADMIN./admin/authorization.clientErrors")); // timer type
    Map<String, Object> map = new HashMap<>();
    ((MapWriter) nl.get("ADMIN./admin/authorization.clientErrors")).toMap(map);
    assertEquals(5, map.size());

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "group", "jvm,jetty"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    // "collection" works too, because it's a prefix for "collection1"
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "registry", "solr.core.collection,solr.jvm"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.core.collection1"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    // "collection" works too, because it's a prefix for "collection1"
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "registry", "solr.core.collection", "registry", "solr.jvm"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.core.collection1"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "group", "jvm,jetty"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "group", "jvm", "group", "jetty"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "group", "node", "type", "counter"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(1, values.size());
    values = (NamedList) values.get("solr.node");
    assertNotNull(values);
    assertNull(values.get("ADMIN./admin/authorization.errors")); // this is a timer node

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "prefix", "CONTAINER.cores,CONTAINER.threadPool"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(1, values.size());
    assertEquals(13, ((NamedList) values.get("solr.node")).size());
    assertNotNull(values.get("solr.node"));
    values = (NamedList) values.get("solr.node");
    assertNotNull(values.get("CONTAINER.cores.lazy")); // this is a gauge node
    assertNotNull(values.get("CONTAINER.threadPool.coreContainerWorkExecutor.completed"));
    assertNotNull(values.get("CONTAINER.threadPool.coreLoadExecutor.completed"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "prefix", "CONTAINER.cores", "regex", "C.*thread.*completed"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertNotNull(values.get("solr.node"));
    values = (NamedList) values.get("solr.node");
    assertEquals(5, values.size());
    assertNotNull(values.get("CONTAINER.threadPool.coreContainerWorkExecutor.completed"));
    assertNotNull(values.get("CONTAINER.threadPool.coreLoadExecutor.completed"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", "prefix", "CACHE.core.fieldCache", "property", "entries_count", MetricsHandler.COMPACT_PARAM, "true"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertNotNull(values.get("solr.core.collection1"));
    values = (NamedList) values.get("solr.core.collection1");
    assertEquals(1, values.size());
    @SuppressWarnings({"rawtypes"})
    MapWriter writer = (MapWriter) values.get("CACHE.core.fieldCache");
    assertNotNull(writer);
    assertNotNull(writer._get("entries_count", null));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "group", "jvm", "prefix", "CONTAINER.cores"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(0, values.size());

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json", "group", "node", "type", "timer", "prefix", "CONTAINER.cores"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    @SuppressWarnings({"rawtypes"})
    SimpleOrderedMap map1 = (SimpleOrderedMap) values.get("metrics");
    assertEquals(0, map1.size());
    handler.close();
  }

  @Test
  public void testCompact() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", MetricsHandler.COMPACT_PARAM, "true"), resp);
    @SuppressWarnings({"rawtypes"})
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    Object o = nl.get("SEARCHER.new.errors");
    assertNotNull(o); // counter type
    assertTrue(o instanceof Number);
    handler.close();
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testPropertyFilter() throws Exception {
    assertQ(req("*:*"), "//result[@numFound='0']");

    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.COMPACT_PARAM, "true", "group", "core", "prefix", "CACHE.searcher"), resp);
    @SuppressWarnings({"rawtypes"})
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    assertTrue(nl.size() > 0);
    nl.forEach((k, v) -> {
      assertTrue(v instanceof MapWriter);
      Map<String, Object> map = new HashMap<>();
      ((MapWriter) v).toMap(map);
      assertTrue(map.size() > 2);
    });

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.COMPACT_PARAM, "true", "group", "core", "prefix", "CACHE.searcher",
        "property", "inserts", "property", "size"), resp);
    values = resp.getValues();
    values = (NamedList) values.get("metrics");
    nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    assertTrue(nl.size() > 0);
    nl.forEach((k, v) -> {
      assertTrue(v instanceof MapWriter);
      Map<String, Object> map = new HashMap<>();
      ((MapWriter) v).toMap(map);
      assertEquals("k=" + k + ", v=" + map, 2, map.size());
      assertNotNull(map.get("inserts"));
      assertNotNull(map.get("size"));
    });
    handler.close();
  }

  @Test
  public void testKeyMetrics() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    String key1 = "solr.core.collection1:CACHE.core.fieldCache";
    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key1), resp);
    @SuppressWarnings({"rawtypes"})
    NamedList values = resp.getValues();
    Object val = values.findRecursive("metrics", key1);
    assertNotNull(val);
    assertTrue(val instanceof MapWriter);
    assertTrue(((MapWriter)val)._size() >= 2);

    String key2 = "solr.core.collection1:CACHE.core.fieldCache:entries_count";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key2), resp);
    val = resp.getValues()._get("metrics/" + key2, null);
    assertNotNull(val);
    assertTrue(val instanceof Number);

    String key3 = "solr.jetty:solrtest_foo\\:bar";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key3), resp);

    val = resp.getValues()._get( "metrics/" + key3, null);
    assertNotNull(val);
    assertTrue(val instanceof Number);
    assertEquals(3, ((Number) val).intValue());

    // test multiple keys
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key1, MetricsHandler.KEY_PARAM, key2, MetricsHandler.KEY_PARAM, key3), resp);

    val = resp.getValues()._get( "metrics/" + key1, null);
    assertNotNull(val);
    val = resp.getValues()._get( "metrics/" + key2, null);
    assertNotNull(val);
    val = resp.getValues()._get( "metrics/" + key3, null);
    assertNotNull(val);

    String key4 = "solr.core.collection1:QUERY./select.requestTimes:1minRate";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key4), resp);
    // the key contains a slash, need explicit list of path elements
    val = resp.getValues()._get(Arrays.asList("metrics", key4), null);
    assertNotNull(val);
    assertTrue(val instanceof Number);

    // test errors

    // invalid keys
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, "foo", MetricsHandler.KEY_PARAM, "foo:bar:baz:xyz"), resp);
    values = resp.getValues();
    @SuppressWarnings({"rawtypes"})
    NamedList metrics = (NamedList) values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "foo"));
    assertNotNull(values.findRecursive("errors", "foo:bar:baz:xyz"));

    // unknown registry
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, "foo:bar:baz"), resp);
    values = resp.getValues();
    metrics = (NamedList) values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "foo:bar:baz"));

    // unknown metric
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, "solr.jetty:unknown:baz"), resp);
    values = resp.getValues();
    metrics = (NamedList) values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "solr.jetty:unknown:baz"));

    handler.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExprMetrics() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    String key1 = "solr\\.core\\..*:.*/select\\.request.*:.*Rate";
    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.EXPR_PARAM, key1), resp);
    // response structure is like in the case of non-key params
    Object val = resp.getValues().findRecursive( "metrics", "solr.core.collection1", "QUERY./select.requestTimes");
    assertNotNull(val);
    assertTrue(val instanceof MapWriter);
    Map<String, Object> map = new HashMap<>();
    ((MapWriter) val).toMap(map);
    assertEquals(map.toString(), 4, map.size()); // mean, 1, 5, 15
    assertNotNull(map.toString(), map.get("meanRate"));
    assertNotNull(map.toString(), map.get("1minRate"));
    assertNotNull(map.toString(), map.get("5minRate"));
    assertNotNull(map.toString(), map.get("15minRate"));
    assertEquals(map.toString(), ((Number) map.get("1minRate")).doubleValue(), 0.0, 0.0);
    map.clear();

    String key2 = "solr\\.core\\..*:.*/select\\.request.*";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.EXPR_PARAM, key2), resp);
    // response structure is like in the case of non-key params
    val = resp.getValues().findRecursive( "metrics", "solr.core.collection1");
    assertNotNull(val);
    Object v = ((SimpleOrderedMap<Object>) val).get("QUERY./select.requestTimes");
    assertNotNull(v);
    assertTrue(v instanceof MapWriter);
    ((MapWriter) v).toMap(map);
    assertEquals(map.toString(), 14, map.size());
    assertNotNull(map.toString(), map.get("1minRate"));
    assertEquals(map.toString(), ((Number) map.get("1minRate")).doubleValue(), 0.0, 0.0);
    map.clear();
    // select requests counter
    v = ((SimpleOrderedMap<Object>) val).get("QUERY./select.requests");
    assertNotNull(v);
    assertTrue(v instanceof Number);

    // test multiple expressions producing overlapping metrics - should be no dupes

    // this key matches also sub-metrics of /select, eg. /select.distrib, /select.local, ...
    String key3 = "solr\\.core\\..*:.*/select.*\\.requestTimes:count";
    resp = new SolrQueryResponse();
    // ORDER OF PARAMS MATTERS HERE! see the refguide
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.EXPR_PARAM, key2, MetricsHandler.EXPR_PARAM, key1, MetricsHandler.EXPR_PARAM, key3), resp);
    val = resp.getValues().findRecursive( "metrics", "solr.core.collection1");
    assertNotNull(val);
    // for requestTimes only the full set of values from the first expr should be present
    assertNotNull(val);
    SimpleOrderedMap<Object> values = (SimpleOrderedMap<Object>) val;
    assertEquals(values.jsonStr(), 4, values.size());
    List<Object> multipleVals = values.getAll("QUERY./select.requestTimes");
    assertEquals(multipleVals.toString(), 1, multipleVals.size());
    v = values.get("QUERY./select.local.requestTimes");
    assertTrue(v instanceof MapWriter);
    ((MapWriter) v).toMap(map);
    assertEquals(map.toString(), 1, map.size());
    assertTrue(map.toString(), map.containsKey("count"));
    map.clear();
    v = values.get("QUERY./select.distrib.requestTimes");
    assertTrue(v instanceof MapWriter);
    ((MapWriter) v).toMap(map);
    assertEquals(map.toString(), 1, map.size());
    assertTrue(map.toString(), map.containsKey("count"));
  }

  @Test
  public void testMetricsUnload() throws Exception {

    SolrCore core = h.getCoreContainer().getCore("collection1");//;.getRequestHandlers().put("/dumphandler", new DumpRequestHandler());
    RefreshablePluginHolder pluginHolder =null;
    try {
      PluginInfo info = new PluginInfo(SolrRequestHandler.TYPE, Utils.makeMap("name", "/dumphandler", "class", DumpRequestHandler.class.getName()));
      DumpRequestHandler requestHandler = new DumpRequestHandler();
      requestHandler.gaugevals =  Utils.makeMap("d_k1","v1", "d_k2","v2");
      pluginHolder = new RefreshablePluginHolder(info, requestHandler);
      core.getRequestHandlers().put("/dumphandler",

          pluginHolder);
    } finally {
      core.close();
    }



    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", MetricsHandler.COMPACT_PARAM, "true", "key", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge"),
        resp);

    assertEquals("v1", resp.getValues()._getStr(Arrays.asList("metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge","d_k1"), null));
    assertEquals("v2", resp.getValues()._getStr(Arrays.asList("metrics","solr.core.collection1:QUERY./dumphandler.dumphandlergauge","d_k2"), null));
    pluginHolder.closeHandler();
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", MetricsHandler.COMPACT_PARAM, "true", "key", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge"),
        resp);

    assertEquals(null, resp.getValues()._getStr(Arrays.asList("metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge","d_k1"), null));
    assertEquals(null, resp.getValues()._getStr(Arrays.asList("metrics","solr.core.collection1:QUERY./dumphandler.dumphandlergauge","d_k2"), null));

    DumpRequestHandler requestHandler = new DumpRequestHandler();
    requestHandler.gaugevals =  Utils.makeMap("d_k1","v1.1", "d_k2","v2.1");
    pluginHolder.reset(requestHandler);
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", MetricsHandler.COMPACT_PARAM, "true", "key", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge"),
        resp);

    assertEquals("v1.1", resp.getValues()._getStr(Arrays.asList("metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge","d_k1"), null));
    assertEquals("v2.1", resp.getValues()._getStr(Arrays.asList("metrics","solr.core.collection1:QUERY./dumphandler.dumphandlergauge","d_k2"), null));


  }

  static class RefreshablePluginHolder extends PluginBag.PluginHolder<SolrRequestHandler> {

    private DumpRequestHandler rh;
    private SolrMetricsContext metricsInfo;

    public RefreshablePluginHolder(PluginInfo info, DumpRequestHandler rh) {
      super(info);
      this.rh = rh;
    }

    @Override
    public boolean isLoaded() {
      return true;
    }

    void closeHandler() throws Exception {
      this.metricsInfo = rh.getSolrMetricsContext();
//      if(metricsInfo.tag.contains(String.valueOf(rh.hashCode()))){
//        //this created a new child metrics
//        metricsInfo = metricsInfo.getParent();
//      }
      this.rh.close();
    }

    void reset(DumpRequestHandler rh) throws Exception {
        this.rh = rh;
        if(metricsInfo != null)
        this.rh.initializeMetrics(metricsInfo, "/dumphandler");
    }


    @Override
    public SolrRequestHandler get() {
      return rh;
    }
  }

  public static class DumpRequestHandler extends RequestHandlerBase {

    static String key = DumpRequestHandler.class.getName();
    Map<String, Object> gaugevals ;
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      rsp.add("key", key);
    }

    @Override
    public String getDescription() {
      return "DO nothing";
    }

    @Override
    public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
      super.initializeMetrics(parentContext, scope);
      MetricsMap metrics = new MetricsMap(map -> gaugevals.forEach((k, v) -> map.putNoEx(k, v)));
      solrMetricsContext.gauge(this,
           metrics,  true, "dumphandlergauge", getCategory().toString(), scope);

    }

    @Override
    public Boolean registerV2() {
      return Boolean.FALSE;
    }
  }
}
