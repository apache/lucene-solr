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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

/**
 *
 */
public class SolrGraphiteReporterTest extends SolrTestCaseJ4 {

  @Test
  public void testReporter() throws Exception {
    Path home = Paths.get(TEST_HOME());
    // define these properties, they are used in solrconfig.xml
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    MockGraphite mock = new MockGraphite();
    try {
      mock.start();
      Thread.sleep(1000);
      // define the port where MockGraphite is running
      System.setProperty("mock-graphite-port", String.valueOf(mock.port));
      String solrXml = FileUtils.readFileToString(Paths.get(home.toString(), "solr-graphitereporter.xml").toFile(), "UTF-8");
      NodeConfig cfg = SolrXmlConfig.fromString(new SolrResourceLoader(home), solrXml);
      CoreContainer cc = createCoreContainer(cfg,
          new TestHarness.TestCoresLocator(DEFAULT_TEST_CORENAME, initCoreDataDir.getAbsolutePath(), "solrconfig.xml", "schema.xml"));
      h.coreName = DEFAULT_TEST_CORENAME;
      SolrMetricManager metricManager = cc.getMetricManager();
      Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.node");
      assertEquals(1, reporters.size());
      SolrMetricReporter reporter = reporters.get("test");
      assertNotNull(reporter);
      assertTrue(reporter instanceof SolrGraphiteReporter);
      Thread.sleep(5000);
      assertTrue(mock.lines.size() >= 3);
      for (String line : mock.lines) {
        assertTrue(line, line.startsWith("test.solr.node.cores."));
      }
    } finally {
      mock.close();
    }
  }

  private class MockGraphite extends Thread {
    private List<String> lines = new ArrayList<>();
    private ServerSocket server = null;
    private int port;
    private boolean stop;

    MockGraphite() throws Exception {
      server = new ServerSocket(0);
      port = server.getLocalPort();
      stop = false;
    }

    public void run() {
      while (!stop) {
        try {
          Socket s = server.accept();
          BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"));
          String line;
          while ((line = br.readLine()) != null) {
            lines.add(line);
          }
        } catch (Exception e) {
          stop = true;
        }
      }
    }

    public void close() throws Exception {
      stop = true;
      if (server != null) {
        server.close();
      }
    }
  }

}
