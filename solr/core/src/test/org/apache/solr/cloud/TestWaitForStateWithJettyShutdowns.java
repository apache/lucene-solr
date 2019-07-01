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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;

import static org.apache.solr.cloud.SolrCloudTestCase.clusterShape;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWaitForStateWithJettyShutdowns extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testWaitForStateAfterShutDown() throws Exception {
    final String col_name = "test_col";
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster
      (1, createTempDir(), buildJettyConfig("/solr"));
    try {
      log.info("Create our collection");
      CollectionAdminRequest.createCollection(col_name, "_default", 1, 1).process(cluster.getSolrClient());
      
      log.info("Sanity check that our collection has come online");
      cluster.getSolrClient().waitForState(col_name, 30, TimeUnit.SECONDS, clusterShape(1, 1));
                                           
      log.info("Shutdown 1 node");
      final JettySolrRunner nodeToStop = cluster.getJettySolrRunner(0);
      nodeToStop.stop();
      log.info("Wait to confirm our node is fully shutdown");
      cluster.waitForJettyToStop(nodeToStop);

      // now that we're confident that node has stoped, check if a waitForState
      // call will detect the missing replica -- shouldn't need long wait times (we know it's down)...
      log.info("Now check if waitForState will recognize we already have the exepcted state");
      cluster.getSolrClient().waitForState(col_name, 500, TimeUnit.MILLISECONDS, clusterShape(1, 0));
                                           
      
    } finally {
      cluster.shutdown();
    }
  }

  public void testWaitForStateBeforeShutDown() throws Exception {
    final String col_name = "test_col";
    final ExecutorService executor = ExecutorUtil.newMDCAwareFixedThreadPool
      (1, new DefaultSolrThreadFactory("background_executor"));
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster
      (1, createTempDir(), buildJettyConfig("/solr"));
    try {
      log.info("Create our collection");
      CollectionAdminRequest.createCollection(col_name, "_default", 1, 1).process(cluster.getSolrClient());
      
      log.info("Sanity check that our collection has come online");
      cluster.getSolrClient().waitForState(col_name, 30, TimeUnit.SECONDS,
                                           SolrCloudTestCase.clusterShape(1, 1));


      // HACK implementation detail...
      //
      // we know that in the current implementation, waitForState invokes the predicate twice
      // independently of the current state of the collection and/or wether the predicate succeeds.
      // If this implementation detail changes, (ie: so that it's only invoked once)
      // then this number needs to change -- but the test fundementally depends on the implementation
      // calling the predicate at least once, which should also be neccessary for any future impl
      // (to verify that it didn't "miss" the state change when creating the watcher)
      final CountDownLatch latch = new CountDownLatch(2);
      
      final Future<?> backgroundWaitForState = executor.submit
        (() -> {
          try {
            cluster.getSolrClient().waitForState(col_name, 180, TimeUnit.SECONDS,
                                                 new LatchCountingPredicateWrapper(latch,
                                                                                   clusterShape(1, 0)));
          } catch (Exception e) {
            log.error("background thread got exception", e);
            throw new RuntimeException(e);
          }
          return;
        }, null);
      
      log.info("Awaiting latch...");
      if (! latch.await(120, TimeUnit.SECONDS)) {
        fail("timed out Waiting a ridiculous amount of time for the waitForState latch -- did impl change?");
      }

      log.info("Shutdown 1 node");
      final JettySolrRunner nodeToStop = cluster.getJettySolrRunner(0);
      nodeToStop.stop();
      log.info("Wait to confirm our node is fully shutdown");
      cluster.waitForJettyToStop(nodeToStop);

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
      ExecutorUtil.shutdownAndAwaitTermination(executor);
      cluster.shutdown();
    }
  }
    
  public final class LatchCountingPredicateWrapper implements CollectionStatePredicate {
    private final CountDownLatch latch;
    private final CollectionStatePredicate inner;
    public LatchCountingPredicateWrapper(final CountDownLatch latch, final CollectionStatePredicate inner) {
      this.latch = latch;
      this.inner = inner;
    }
    public boolean matches(Set<String> liveNodes, DocCollection collectionState) {
      final boolean result = inner.matches(liveNodes, collectionState);
      log.info("Predicate called: result={}, (pre)latch={}, liveNodes={}, state={}",
               result, latch.getCount(), liveNodes, collectionState);
      latch.countDown();
      return result;
    }
  }
}
