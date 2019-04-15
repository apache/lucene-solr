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

import java.util.Map;

import com.codahale.metrics.Counter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;
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

    // manually register some metrics in solr.jvm and solr.jetty - TestHarness doesn't init them
    Counter c = h.getCoreContainer().getMetricManager().counter(null, "solr.jvm", "foo");
    c.inc();
    c = h.getCoreContainer().getMetricManager().counter(null, "solr.jetty", "foo");
    c.inc(2);
    // test escapes
    c = h.getCoreContainer().getMetricManager().counter(null, "solr.jetty", "foo:bar");
    c.inc(3);
  }

  @Test
  public void test() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", MetricsHandler.COMPACT_PARAM, "false", CommonParams.WT, "json"), resp);
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    Object o = nl.get("SEARCHER.new.errors");
    assertNotNull(o); // counter type
    assertTrue(o instanceof Map);
    // response wasn't serialized so we get here whatever MetricUtils produced instead of NamedList
    assertNotNull(((Map) o).get("count"));
    assertEquals(0L, ((Map) nl.get("SEARCHER.new.errors")).get("count"));
    nl = (NamedList) values.get("solr.node");
    assertNotNull(nl.get("CONTAINER.cores.loaded")); // int gauge
    assertEquals(1, ((Map) nl.get("CONTAINER.cores.loaded")).get("value"));
    assertNotNull(nl.get("ADMIN./admin/authorization.clientErrors")); // timer type
    assertEquals(5, ((Map) nl.get("ADMIN./admin/authorization.clientErrors")).size());

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
    assertEquals(11, ((NamedList)values.get("solr.node")).size());
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
    Map m = (Map)values.get("CACHE.core.fieldCache");
    assertNotNull(m);
    assertNotNull(m.get("entries_count"));

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
    SimpleOrderedMap map = (SimpleOrderedMap) values.get("metrics");
    assertEquals(0, map.size());
  }

  @Test
  public void testCompact() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", MetricsHandler.COMPACT_PARAM, "true"), resp);
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    Object o = nl.get("SEARCHER.new.errors");
    assertNotNull(o); // counter type
    assertTrue(o instanceof Number);
  }

  @Test
  public void testPropertyFilter() throws Exception {
    assertQ(req("*:*"), "//result[@numFound='0']");

    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.COMPACT_PARAM, "true", "group", "core", "prefix", "CACHE.searcher"), resp);
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    assertTrue(nl.size() > 0);
    nl.forEach((k, v) -> {
      assertTrue(v instanceof Map);
      Map map = (Map)v;
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
      assertTrue(v instanceof Map);
      Map map = (Map)v;
      assertEquals(2, map.size());
      assertNotNull(map.get("inserts"));
      assertNotNull(map.get("size"));
    });
  }

  @Test
  public void testKeyMetrics() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    String key1 = "solr.core.collection1:CACHE.core.fieldCache";
    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key1), resp);
    NamedList values = resp.getValues();
    Object val = values.findRecursive("metrics", key1);
    assertNotNull(val);
    assertTrue(val instanceof Map);
    assertTrue(((Map)val).size() >= 2);

    String key2 = "solr.core.collection1:CACHE.core.fieldCache:entries_count";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key2), resp);
    values = resp.getValues();
    val = values.findRecursive("metrics", key2);
    assertNotNull(val);
    assertTrue(val instanceof Number);

    String key3 = "solr.jetty:foo\\:bar";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key3), resp);
    values = resp.getValues();
    val = values.findRecursive("metrics", key3);
    assertNotNull(val);
    assertTrue(val instanceof Number);
    assertEquals(3, ((Number)val).intValue());

    // test multiple keys
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key1, MetricsHandler.KEY_PARAM, key2, MetricsHandler.KEY_PARAM, key3), resp);
    values = resp.getValues();
    val = values.findRecursive("metrics", key1);
    assertNotNull(val);
    val = values.findRecursive("metrics", key2);
    assertNotNull(val);
    val = values.findRecursive("metrics", key3);
    assertNotNull(val);

    String key4 = "solr.core.collection1:QUERY./select.requestTimes:1minRate";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, key4), resp);
    values = resp.getValues();
    val = values.findRecursive("metrics", key4);
    assertNotNull(val);
    assertTrue(val instanceof Number);

    // test errors

    // invalid keys
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, "foo", MetricsHandler.KEY_PARAM, "foo:bar:baz:xyz"), resp);
    values = resp.getValues();
    NamedList metrics = (NamedList)values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "foo"));
    assertNotNull(values.findRecursive("errors", "foo:bar:baz:xyz"));

    // unknown registry
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, "foo:bar:baz"), resp);
    values = resp.getValues();
    metrics = (NamedList)values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "foo:bar:baz"));

    // unknown metric
    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json",
        MetricsHandler.KEY_PARAM, "solr.jetty:unknown:baz"), resp);
    values = resp.getValues();
    metrics = (NamedList)values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "solr.jetty:unknown:baz"));
  }
}
