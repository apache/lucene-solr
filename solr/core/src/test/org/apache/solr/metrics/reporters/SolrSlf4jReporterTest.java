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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.util.TestHarness;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrSlf4jReporterTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testReporter() throws Exception {
    ensureLoggingConfiguredAppropriately();
    Path home = Paths.get(TEST_HOME());
    // define these properties, they are used in solrconfig.xml
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    String solrXml = FileUtils.readFileToString(Paths.get(home.toString(), "solr-slf4jreporter.xml").toFile(), "UTF-8");
    NodeConfig cfg = SolrXmlConfig.fromString(home, solrXml);
    CoreContainer cc = createCoreContainer(cfg, new TestHarness.TestCoresLocator
                                           (DEFAULT_TEST_CORENAME, initAndGetDataDir().getAbsolutePath(),
                                            "solrconfig.xml", "schema.xml"));
                                           
    h.coreName = DEFAULT_TEST_CORENAME;
    SolrMetricManager metricManager = cc.getMetricManager();
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.node");
    assertTrue(reporters.toString(), reporters.size() >= 2);
    SolrMetricReporter reporter1 = reporters.get("test1");
    assertNotNull(reporter1);
    assertTrue(reporter1 instanceof SolrSlf4jReporter);
    SolrMetricReporter reporter2 = reporters.get("test2");
    assertNotNull(reporter2);
    assertTrue(reporter2 instanceof SolrSlf4jReporter);

    LogWatcherConfig watcherCfg = new LogWatcherConfig(true, null, null, 100);
    @SuppressWarnings({"rawtypes"})
    LogWatcher watcher = LogWatcher.newRegisteredLogWatcher(watcherCfg, null);
    watcher.setThreshold("INFO");

    watcher.reset();
    int cnt = 20;
    boolean active;
    do {
      Thread.sleep(1000);
      cnt--;
      active = ((SolrSlf4jReporter)reporter1).isActive() && ((SolrSlf4jReporter)reporter2).isActive();
    } while (!active && cnt > 0);
    if (!active) {
      fail("One or more reporters didn't become active in 20 seconds");
    }
    Thread.sleep(10000);

    SolrDocumentList history = watcher.getHistory(-1, null);
    // dot-separated names are treated like class names and collapsed
    // in regular log output, but here we get the full name
    if (history.stream().filter(d -> "solr.node".equals(d.getFirstValue("logger"))).count() == 0) {
      fail("No 'solr.node' logs in: " + history.toString());
    }
    if (history.stream().filter(d -> "foobar".equals(d.getFirstValue("logger"))).count() == 0) {
      fail("No 'foobar' logs in: " + history.toString());
    }
    if (history.stream().filter(d -> "x:collection1".equals(d.getFirstValue("core"))).count() == 0) {
      fail("No 'solr.core' or MDC context in logs: " + history.toString());
    }
  }

  private static void ensureLoggingConfiguredAppropriately() throws Exception {
    if (! log.isInfoEnabled()) {
      fail("Test requires that log-level is at-least INFO, but INFO is disabled");
    }
  }
}
