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

package org.apache.solr.cloud;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class MiniSolrCloudClusterTest extends SolrTestCase {

  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());

  @Test
  public void testErrorsInStartup() throws Exception {

    AtomicInteger jettyIndex = new AtomicInteger();

    MiniSolrCloudCluster cluster = null;
    try {
      cluster = new MiniSolrCloudCluster(3, createTempDir(), JettyConfig.builder().build()) {
        @Override
        public JettySolrRunner startJettySolrRunner(String name, String context, JettyConfig config) throws Exception {
          if (jettyIndex.incrementAndGet() != 2)
            return super.startJettySolrRunner(name, context, config);
          throw new IOException("Fake exception on startup!");
        }
      };
      fail("Expected an exception to be thrown from MiniSolrCloudCluster");
    }
    catch (Exception e) {
      assertEquals("Error starting up MiniSolrCloudCluster", e.getMessage());
      assertEquals("Expected one suppressed exception", 1, e.getSuppressed().length);
      assertEquals("Fake exception on startup!", e.getSuppressed()[0].getMessage());
    }
    finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  @Test
  public void testErrorsInShutdown() throws Exception {

    AtomicInteger jettyIndex = new AtomicInteger();

    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(3, createTempDir(), JettyConfig.builder().build()) {
      @Override
      public JettySolrRunner stopJettySolrRunner(JettySolrRunner jetty) throws Exception {
        JettySolrRunner j = super.stopJettySolrRunner(jetty);
        if (jettyIndex.incrementAndGet() == 2)
          throw new IOException("Fake IOException on shutdown!");
        return j;
      }
    };

    Exception ex = expectThrows(Exception.class, cluster::shutdown);
    assertEquals("Error shutting down MiniSolrCloudCluster", ex.getMessage());
    assertEquals("Expected one suppressed exception", 1, ex.getSuppressed().length);
    assertEquals("Fake IOException on shutdown!", ex.getSuppressed()[0].getMessage());
  }

  @Test
  public void testExtraFilters() throws Exception {
    JettyConfig.Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    jettyConfig.withFilter(JettySolrRunner.DebugFilter.class, "*");
    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(random().nextInt(3) + 1, createTempDir(), jettyConfig.build());
    cluster.shutdown();
  }

  public void testSolrHomeAndResourceLoaders() throws Exception {
    final String SOLR_HOME_PROP = "solr.solr.home";
    // regardless of what sys prop may be set, everything in the cluster should use solr home dirs under the 
    // configured base dir -- and nothing in the call stack should be "setting" the sys prop to make that work...
    final String fakeSolrHome = createTempDir().toAbsolutePath().toString();
    System.setProperty(SOLR_HOME_PROP, fakeSolrHome);

    // mock FS from createTempDir don't play nice using 'startsWith' when the solr stack reconsistutes the path from string
    // so we have to go the string route here as well...
    final Path workDir = Paths.get(createTempDir().toAbsolutePath().toString());
    
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(1, workDir, JettyConfig.builder().build());
    try {
      final JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);
      assertTrue(jetty.getCoreContainer().getSolrHome() + " vs " + workDir,
                 // mock dirs from createTempDir() don't play nice with startsWith, so we have to use the string value
                 Paths.get(jetty.getCoreContainer().getSolrHome()).startsWith(workDir));
      assertEquals(jetty.getCoreContainer().getSolrHome(),
                   jetty.getCoreContainer().getResourceLoader().getInstancePath().toAbsolutePath().toString());

      assertTrue(CollectionAdminRequest.createCollection("test", 1,1).process(cluster.getSolrClient()).isSuccess());
      final SolrCore core = jetty.getCoreContainer().getCores().get(0);
      assertTrue(core.getInstancePath() + " vs " + workDir,
                 core.getInstancePath().startsWith(workDir));
      assertEquals(core.getInstancePath(),
                   core.getResourceLoader().getInstancePath());
    } finally {
      cluster.shutdown();
    }
    assertEquals("There is no reason why anything should have set this sysprop",
                 fakeSolrHome, System.getProperty(SOLR_HOME_PROP));
  }
  
  public void testMultipleClustersDiffZk() throws Exception {
    final MiniSolrCloudCluster x = new MiniSolrCloudCluster(1, createTempDir(), JettyConfig.builder().build());
    try {
      final MiniSolrCloudCluster y = new MiniSolrCloudCluster(1, createTempDir(), JettyConfig.builder().build());
      try {
        
        // sanity check we're not talking to ourselves
        assertNotSame(x.getZkServer(), y.getZkServer());
        assertNotEquals(x.getZkServer().getZkAddress(), y.getZkServer().getZkAddress());
        
        // baseline check
        assertEquals(1, x.getJettySolrRunners().size());
        assertZkHost("x", x.getZkServer().getZkAddress(), x.getJettySolrRunners().get(0));
        
        assertEquals(1, y.getJettySolrRunners().size());
        assertZkHost("y", y.getZkServer().getZkAddress(), y.getJettySolrRunners().get(0));
        
        // adding nodes should be isolated
        final JettySolrRunner j2x = x.startJettySolrRunner();
        assertZkHost("x2", x.getZkServer().getZkAddress(), j2x);
        assertEquals(2, x.getJettySolrRunners().size());
        assertEquals(1, y.getJettySolrRunners().size());
        
        final JettySolrRunner j2y = y.startJettySolrRunner();
        assertZkHost("y2", y.getZkServer().getZkAddress(), j2y);
        assertEquals(2, x.getJettySolrRunners().size());
        assertEquals(2, y.getJettySolrRunners().size());
      } finally {
        y.shutdown();
      }
    } finally {
      x.shutdown();
    }
  }
  
  public void testJettyUsingSysProp() throws Exception {
    try {
      // this cluster will use a sysprop to communicate zkHost to it's nodes -- not node props in the servlet context
      final MiniSolrCloudCluster x = new MiniSolrCloudCluster(1, createTempDir(), JettyConfig.builder().build()) {
        @Override
        public JettySolrRunner startJettySolrRunner(String name, String hostContext, JettyConfig config) throws Exception {
          System.setProperty("zkHost", getZkServer().getZkAddress());
          
          final Properties nodeProps = new Properties();
          nodeProps.setProperty("test-from-sysprop", "yup");
          
          Path runnerPath = createTempDir(name);
          JettyConfig newConfig = JettyConfig.builder(config).setContext("/blarfh").build();
          JettySolrRunner jetty = new JettySolrRunner(runnerPath.toString(), nodeProps, newConfig);
          return super.startJettySolrRunner(jetty);
        }
      };
      try {
        // baseline check
        assertEquals(1, x.getJettySolrRunners().size());
        assertZkHost("x", x.getZkServer().getZkAddress(), x.getJettySolrRunners().get(0));
        
        // verify MiniSolrCloudCluster's impl didn't change out from under us making test useless
        assertEquals("yup", x.getJettySolrRunners().get(0).getNodeProperties().getProperty("test-from-sysprop"));
        assertNull(x.getJettySolrRunners().get(0).getNodeProperties().getProperty("zkHost"));
        
      } finally {
        x.shutdown();
      }
    }finally {
      System.clearProperty("zkHost");
    }
  }
  
  private static void assertZkHost(final String msg, final String zkHost, final JettySolrRunner node) {
    assertEquals(zkHost, node.getCoreContainer().getNodeConfig().getCloudConfig().getZkHost());
  }
}
