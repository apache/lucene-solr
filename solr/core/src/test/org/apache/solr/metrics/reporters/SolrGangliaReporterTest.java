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

package org.apache.solr.metrics.reporters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.util.TestHarness;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 *
 */
public class SolrGangliaReporterTest extends SolrTestCaseJ4 {
  @Test
  public void testReporter() throws Exception {
    Path home = Paths.get(TEST_HOME());
    // define these properties, they are used in solrconfig.xml
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    GMetric ganglia = mock(GMetric.class);
    final List<String> names = new ArrayList<>();
    doAnswer(invocation -> {
        final Object[] args = invocation.getArguments();
        names.add((String)args[0]);
        return null;
    }).when(ganglia).announce(anyString(), anyString(), any(GMetricType.class), anyString(), any(GMetricSlope.class), anyInt(), anyInt(), anyString());
    String solrXml = FileUtils.readFileToString(Paths.get(home.toString(), "solr-gangliareporter.xml").toFile(), "UTF-8");
    NodeConfig cfg = SolrXmlConfig.fromString(new SolrResourceLoader(home), solrXml);
    CoreContainer cc = createCoreContainer(cfg,
        new TestHarness.TestCoresLocator(DEFAULT_TEST_CORENAME, initCoreDataDir.getAbsolutePath(), "solrconfig.xml", "schema.xml"));
    h.coreName = DEFAULT_TEST_CORENAME;
    SolrMetricManager metricManager = cc.getMetricManager();
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.node");
    assertEquals(1, reporters.size());
    SolrMetricReporter reporter = reporters.get("test");
    assertNotNull(reporter);
    assertTrue(reporter instanceof SolrGangliaReporter);
    SolrGangliaReporter gangliaReporter = (SolrGangliaReporter)reporter;
    gangliaReporter.setGMetric(ganglia);
    gangliaReporter.start();
    Thread.sleep(5000);
    assertTrue(names.size() >= 3);
    for (String name : names) {
      assertTrue(name, name.startsWith("test.solr.node.cores."));
    }
  }

}
