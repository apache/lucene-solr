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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.schema.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrMetricReporterTest extends SolrTestCaseJ4 {

  private SolrCoreMetricManager metricManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");
  }

  @Before
  public void beforeTest() {
    metricManager = new SolrCoreMetricManager(h.getCore());
  }

  @After
  public void afterTest() throws Exception {
    metricManager.close();
  }

  @Test
  public void testInit() throws Exception {
    Random random = random();

    MockReporter reporter = new MockReporter(h.getCore().getName());

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, MockReporter.class.getName());
    attrs.put(CoreAdminParams.NAME, TestUtil.randomUnicodeString(random));

    boolean shouldDefineConfigurable = random.nextBoolean();
    String configurable = TestUtil.randomUnicodeString(random);
    if (shouldDefineConfigurable) attrs.put("configurable", configurable);

    boolean shouldDefinePlugin = random.nextBoolean();
    String type = TestUtil.randomUnicodeString(random);
    PluginInfo pluginInfo = shouldDefinePlugin ? new PluginInfo(type, attrs) : null;

    try {
      reporter.init(pluginInfo);
      assertTrue(reporter.didValidate);
      assertNotNull(reporter.configurable);
      assertEquals(configurable, reporter.configurable);
      assertTrue(pluginInfo != null && attrs.get("configurable") == configurable);
    } catch (IllegalStateException e) {
      assertTrue(reporter.didValidate);
      assertNull(reporter.configurable);
      assertTrue(pluginInfo == null || attrs.get("configurable") == null);
    } finally {
      reporter.close();
    }
  }

  public static class MockReporter extends SolrMetricReporter {
    String configurable;
    boolean didValidate = false;

    MockReporter(String registryName) {
      super(registryName);
    }

    @Override
    public void init(PluginInfo pluginInfo) {
      super.init(pluginInfo);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected void validate() throws IllegalStateException {
      didValidate = true;
      if (configurable == null) {
        throw new IllegalStateException("MockReporter::configurable not configured.");
      }
    }

    public void setConfigurable(String configurable) {
      this.configurable = configurable;
    }
  }
}
