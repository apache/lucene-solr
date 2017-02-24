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
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.util.TestHarness;
import org.junit.Test;

/**
 *
 */
public class SolrSlf4jReporterTest extends SolrTestCaseJ4 {

  @Test
  public void testReporter() throws Exception {
    LogWatcherConfig watcherCfg = new LogWatcherConfig(true, null, null, 100);
    LogWatcher watcher = LogWatcher.newRegisteredLogWatcher(watcherCfg, null);
    watcher.setThreshold("INFO");
    Path home = Paths.get(TEST_HOME());
    // define these properties, they are used in solrconfig.xml
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    String solrXml = FileUtils.readFileToString(Paths.get(home.toString(), "solr-slf4jreporter.xml").toFile(), "UTF-8");
    NodeConfig cfg = SolrXmlConfig.fromString(new SolrResourceLoader(home), solrXml);
    CoreContainer cc = createCoreContainer(cfg,
        new TestHarness.TestCoresLocator(DEFAULT_TEST_CORENAME, initCoreDataDir.getAbsolutePath(), "solrconfig.xml", "schema.xml"));
    h.coreName = DEFAULT_TEST_CORENAME;
    SolrMetricManager metricManager = cc.getMetricManager();
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.node");
    assertEquals(2, reporters.size());
    SolrMetricReporter reporter = reporters.get("test1");
    assertNotNull(reporter);
    assertTrue(reporter instanceof SolrSlf4jReporter);
    reporter = reporters.get("test2");
    assertNotNull(reporter);
    assertTrue(reporter instanceof SolrSlf4jReporter);

    watcher.reset();
    Thread.sleep(5000);

    SolrDocumentList history = watcher.getHistory(-1, null);
    // dot-separated names are treated like class names and collapsed
    // in regular log output, but here we get the full name
    assertTrue(history.stream().filter(d -> "solr.node".equals(d.getFirstValue("logger"))).count() > 0);
    assertTrue(history.stream().filter(d -> "foobar".equals(d.getFirstValue("logger"))).count() > 0);
  }
}
