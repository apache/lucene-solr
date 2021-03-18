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

import java.lang.invoke.MethodHandles;

import java.util.Set;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;

import static org.apache.solr.cloud.SolrCloudTestCase.clusterShape;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWaitForStateWithJettyShutdowns extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");
  }

  @LuceneTestCase.Nightly // can be slow (at least on low end hardware)
  public void testWaitForStateAfterShutDown() throws Exception {
    final String col_name = "test_col";
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster
      (1, SolrTestUtil.createTempDir(), buildJettyConfig("/solr"));
    try {
      log.info("Create our collection");
      CollectionAdminRequest.createCollection(col_name, "_default", 1, 1).waitForFinalState(true).process(cluster.getSolrClient());
                                           
      log.info("Shutdown 1 node");
      final JettySolrRunner nodeToStop = cluster.getJettySolrRunner(0);
      nodeToStop.stop();

      log.info("Now check if waitForState will recognize we already have the exepcted state");
      cluster.waitForActiveCollection(col_name, 5000, TimeUnit.MILLISECONDS, 1, 0);
    } finally {
      cluster.shutdown();
    }
  }

  public void testWaitForStateBeforeShutDown() throws Exception {
    final String col_name = "test_col";
    final ExecutorService executor = getTestExecutor();
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster
      (1, SolrTestUtil.createTempDir(), buildJettyConfig("/solr"));
    try {
      log.info("Create our collection");
      CollectionAdminRequest.createCollection(col_name, "_default", 1, 1).process(cluster.getSolrClient());
      
      final Future<?> backgroundWaitForState = executor.submit
        (() -> {
          try {
            cluster.getSolrClient().waitForState(col_name, 10, TimeUnit.SECONDS, clusterShape(1, 0));
          } catch (Exception e) {
            log.error("background thread got exception", e);
            throw new RuntimeException(e);
          }
          return;
        }, null);

      log.info("Shutdown 1 node");
      final JettySolrRunner nodeToStop = cluster.getJettySolrRunner(0);
      nodeToStop.stop();

      // now that we're confident that node has stoped, check if a waitForState
      // call will detect the missing replica -- shouldn't need long wait times...
      log.info("Checking Future result to see if waitForState finished successfully");
      try {
        backgroundWaitForState.get();
      } catch (ExecutionException e) {
        log.error("background waitForState exception", e);
        throw e;
      }
      
    } finally {
      cluster.shutdown();
    }
  }
    
  public static final class LatchCountingPredicateWrapper implements CollectionStatePredicate {
    private final CountDownLatch latch;
    private final CollectionStatePredicate inner;
    public LatchCountingPredicateWrapper(final CountDownLatch latch, final CollectionStatePredicate inner) {
      this.latch = latch;
      this.inner = inner;
    }
    public boolean matches(Set<String> liveNodes, DocCollection collectionState) {
      final boolean result = inner.matches(liveNodes, collectionState);
      if (log.isInfoEnabled()) {
        log.info("Predicate called: result={}, (pre)latch={}, liveNodes={}, state={}",
            result, latch.getCount(), liveNodes, collectionState);
      }
      latch.countDown();
      return result;
    }
  }
}
