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
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
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

}
