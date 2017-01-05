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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.metrics.reporters.MockMetricReporter;
import org.apache.solr.schema.FieldType;
import org.junit.Test;

public class SolrMetricReporterTest extends LuceneTestCase {

  @Test
  public void testInit() throws Exception {
    Random random = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    final String registryName = TestUtil.randomSimpleString(random);
    final MockMetricReporter reporter = new MockMetricReporter(metricManager, registryName);

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, MockMetricReporter.class.getName());
    attrs.put(CoreAdminParams.NAME, TestUtil.randomUnicodeString(random));

    boolean shouldDefineConfigurable = random.nextBoolean();
    String configurable = TestUtil.randomUnicodeString(random);
    if (shouldDefineConfigurable) attrs.put("configurable", configurable);

    boolean shouldDefinePlugin = random.nextBoolean();
    String type = TestUtil.randomUnicodeString(random);
    PluginInfo pluginInfo = shouldDefinePlugin ? new PluginInfo(type, attrs) : null;

    try {
      reporter.init(pluginInfo);
      assertNotNull(pluginInfo);
      assertEquals(configurable, attrs.get("configurable"));
      assertTrue(reporter.didValidate);
      assertNotNull(reporter.configurable);
      assertEquals(configurable, reporter.configurable);
    } catch (IllegalStateException e) {
      assertTrue(pluginInfo == null || attrs.get("configurable") == null);
      assertTrue(reporter.didValidate);
      assertNull(reporter.configurable);
    } finally {
      reporter.close();
    }
  }
}
