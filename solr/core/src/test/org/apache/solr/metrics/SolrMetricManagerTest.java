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

package org.apache.solr.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.reporters.MockMetricReporter;
import org.junit.Test;

public class SolrMetricManagerTest extends SolrTestCaseJ4 {

  @Test
  public void testSwapRegistries() throws Exception {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    Map<String, Counter> metrics1 = SolrMetricTestUtils.getRandomMetrics(r, true);
    Map<String, Counter> metrics2 = SolrMetricTestUtils.getRandomMetrics(r, true);
    String fromName = "from-" + TestUtil.randomSimpleString(r, 1, 10);
    String toName = "to-" + TestUtil.randomSimpleString(r, 1, 10);
    // register test metrics
    for (Map.Entry<String, Counter> entry : metrics1.entrySet()) {
      metricManager.registerMetric(null, fromName, entry.getValue(), false, entry.getKey(), "metrics1");
    }
    for (Map.Entry<String, Counter> entry : metrics2.entrySet()) {
      metricManager.registerMetric(null, toName, entry.getValue(), false, entry.getKey(), "metrics2");
    }
    assertEquals(metrics1.size(), metricManager.registry(fromName).getMetrics().size());
    assertEquals(metrics2.size(), metricManager.registry(toName).getMetrics().size());

    // swap
    metricManager.swapRegistries(fromName, toName);
    // check metrics
    Map<String, Metric> fromMetrics = metricManager.registry(fromName).getMetrics();
    assertEquals(metrics2.size(), fromMetrics.size());
    for (Map.Entry<String, Counter> entry : metrics2.entrySet()) {
      Object value = fromMetrics.get(SolrMetricManager.mkName(entry.getKey(), "metrics2"));
      assertNotNull(value);
      assertEquals(entry.getValue(), value);
    }
    Map<String, Metric> toMetrics = metricManager.registry(toName).getMetrics();
    assertEquals(metrics1.size(), toMetrics.size());
    for (Map.Entry<String, Counter> entry : metrics1.entrySet()) {
      Object value = toMetrics.get(SolrMetricManager.mkName(entry.getKey(), "metrics1"));
      assertNotNull(value);
      assertEquals(entry.getValue(), value);
    }
  }

  @Test
  public void testRegisterAll() throws Exception {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(r, true);
    MetricRegistry mr = new MetricRegistry();
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      mr.register(entry.getKey(), entry.getValue());
    }

    String registryName = TestUtil.randomSimpleString(r, 1, 10);
    assertEquals(0, metricManager.registry(registryName).getMetrics().size());
    metricManager.registerAll(registryName, mr, false);
    // this should simply skip existing names
    metricManager.registerAll(registryName, mr, true);
    // this should produce error
    expectThrows(IllegalArgumentException.class, () -> metricManager.registerAll(registryName, mr, false));
  }

  @Test
  public void testClearMetrics() throws Exception {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(r, true);
    String registryName = TestUtil.randomSimpleString(r, 1, 10);

    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      metricManager.registerMetric(null, registryName, entry.getValue(), false, entry.getKey(), "foo", "bar");
    }
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      metricManager.registerMetric(null, registryName, entry.getValue(), false, entry.getKey(), "foo", "baz");
    }
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      metricManager.registerMetric(null, registryName, entry.getValue(), false, entry.getKey(), "foo");
    }

    assertEquals(metrics.size() * 3, metricManager.registry(registryName).getMetrics().size());

    // clear all metrics with prefix "foo.bar."
    Set<String> removed = metricManager.clearMetrics(registryName, "foo", "bar.");
    assertEquals(metrics.size(), removed.size());
    for (String s : removed) {
      assertTrue(s.startsWith("foo.bar."));
    }
    removed = metricManager.clearMetrics(registryName, "foo", "baz.");
    assertEquals(metrics.size(), removed.size());
    for (String s : removed) {
      assertTrue(s.startsWith("foo.baz."));
    }
    // perhaps surprisingly, this works too - see PrefixFilter docs
    removed = metricManager.clearMetrics(registryName, "fo");
    assertEquals(metrics.size(), removed.size());
    for (String s : removed) {
      assertTrue(s.startsWith("foo."));
    }
  }

  @Test
  public void testSimpleMetrics() throws Exception {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    String registryName = TestUtil.randomSimpleString(r, 1, 10);

    metricManager.counter(null, registryName, "simple_counter", "foo", "bar");
    metricManager.timer(null, registryName, "simple_timer", "foo", "bar");
    metricManager.meter(null, registryName, "simple_meter", "foo", "bar");
    metricManager.histogram(null, registryName, "simple_histogram", "foo", "bar");
    Map<String, Metric> metrics = metricManager.registry(registryName).getMetrics();
    assertEquals(4, metrics.size());
    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
      assertTrue(entry.getKey().startsWith("foo.bar.simple_"));
    }
  }

  @Test
  public void testRegistryName() throws Exception {
    Random r = random();

    String name = TestUtil.randomSimpleString(r, 1, 10);

    String result = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, name, "collection1");
    assertEquals("solr.core." + name + ".collection1", result);
    // try it with already prefixed name - group will be ignored
    result = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, result);
    assertEquals("solr.core." + name + ".collection1", result);
    // try it with already prefixed name but with additional segments
    result = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, result, "shard1", "replica1");
    assertEquals("solr.core." + name + ".collection1.shard1.replica1", result);
  }

  @Test
  public void testReporters() throws Exception {

    SolrResourceLoader loader = new SolrResourceLoader();
    SolrMetricManager metricManager = new SolrMetricManager();

    PluginInfo[] plugins = new PluginInfo[] {
        createPluginInfo("universal_foo", null, null),
        createPluginInfo("multigroup_foo", "jvm, node, core", null),
        createPluginInfo("multiregistry_foo", null, "solr.node, solr.core.collection1"),
        createPluginInfo("specific_foo", null, "solr.core.collection1"),
        createPluginInfo("node_foo", "node", null),
        createPluginInfo("core_foo", "core", null)
    };
    String tag = "xyz";
    metricManager.loadReporters(plugins, loader, null, null, tag, SolrInfoBean.Group.node);
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters(
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));
    assertEquals(4, reporters.size());
    assertTrue(reporters.containsKey("universal_foo@" + tag));
    assertTrue(reporters.containsKey("multigroup_foo@" + tag));
    assertTrue(reporters.containsKey("node_foo@" + tag));
    assertTrue(reporters.containsKey("multiregistry_foo@" + tag));

    metricManager.loadReporters(plugins, loader, null, null, tag, SolrInfoBean.Group.core, "collection1");
    reporters = metricManager.getReporters(
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, "collection1"));
    assertEquals(5, reporters.size());
    assertTrue(reporters.containsKey("universal_foo@" + tag));
    assertTrue(reporters.containsKey("multigroup_foo@" + tag));
    assertTrue(reporters.containsKey("specific_foo@" + tag));
    assertTrue(reporters.containsKey("core_foo@" + tag));
    assertTrue(reporters.containsKey("multiregistry_foo@" + tag));

    metricManager.loadReporters(plugins, loader, null, null, tag, SolrInfoBean.Group.jvm);
    reporters = metricManager.getReporters(
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));
    assertEquals(2, reporters.size());
    assertTrue(reporters.containsKey("universal_foo@" + tag));
    assertTrue(reporters.containsKey("multigroup_foo@" + tag));

    metricManager.removeRegistry("solr.jvm");
    reporters = metricManager.getReporters(
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));
    assertEquals(0, reporters.size());

    metricManager.removeRegistry("solr.node");
    reporters = metricManager.getReporters(
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));
    assertEquals(0, reporters.size());

    metricManager.removeRegistry("solr.core.collection1");
    reporters = metricManager.getReporters(
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, "collection1"));
    assertEquals(0, reporters.size());

  }

  @Test
  public void testDefaultCloudReporterPeriodUnchanged() throws Exception {
    assertEquals(60, SolrMetricManager.DEFAULT_CLOUD_REPORTER_PERIOD);
  }

  private PluginInfo createPluginInfo(String name, String group, String registry) {
    Map<String,String> attrs = new HashMap<>();
    attrs.put("name", name);
    attrs.put("class", MockMetricReporter.class.getName());
    if (group != null) {
      attrs.put("group", group);
    }
    if (registry != null) {
      attrs.put("registry", registry);
    }
    NamedList initArgs = new NamedList();
    initArgs.add("configurable", "true");
    return new PluginInfo("SolrMetricReporter", attrs, initArgs, null);
  }
}
