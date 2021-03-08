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

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParallelCommitExecutionTest extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  private static int expectCount;
  private static volatile int tooHigh;

  private static volatile CountDownLatch countdown;
  private static final AtomicInteger countup = new AtomicInteger();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // multi replicas matters; for the initial parallel commit execution tests, only consider repFactor=1
    final int repFactor = 1;//random().nextBoolean() ? 1 : 2;
    final int numShards = TestUtil.nextInt(random(), 1, 4);
    final int numNodes = (numShards * repFactor);
    expectCount = numNodes;

    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    configureCluster(numNodes).addConfig(configName, configDir).configure();

    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig-parallel-commit.xml");
    collectionProperties.put("schema", "schema_latest.xml");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);
    waitForRecoveriesToFinish(CLOUD_CLIENT);
  }

  @AfterClass
  private static void afterClass() throws Exception {
    if (null != CLOUD_CLIENT) {
      CLOUD_CLIENT.close();
      CLOUD_CLIENT = null;
    }
  }

  private static void initSyncVars(boolean tooHigh) {
    final int ct;
    if (tooHigh) {
      ParallelCommitExecutionTest.tooHigh = TOO_HIGH_INCREMENT;
      ct = expectCount + TOO_HIGH_INCREMENT;
    } else {
      ParallelCommitExecutionTest.tooHigh = 0;
      ct = expectCount;
    }
    countdown = new CountDownLatch(ct);
    countup.set(0);
  }

  @Test
  public void testParallelOk() throws Exception {
    initSyncVars(false);
    UpdateResponse rsp = CLOUD_CLIENT.commit(true, true);
    // nocommit: why are we getting status:0 in response that clearly failed? It looks like no matter
    // how many distrib commits fail, as long as the local commit succeeds, it reports success. (if the
    // local commit fails, it apparently does report failure).
    // that's why we're doing this "countup" thing -- because there's apparently no other way to know
    // whether the commit succeeded or not?
    // we expect `countup` to be incremented by exactly `expectCount` number of parallel commits
    assertEquals(expectCount, countup.get());
  }

  private static final int TOO_HIGH_INCREMENT = 1;
  @Test
  public void testParallelFail() throws Exception {
    // artificially set countdown too high; we're really testing the test code, since there's no way
    // to mock the "incorrect" behavior (i.e., "serial" requests)
    initSyncVars(true);
    UpdateResponse rsp = CLOUD_CLIENT.commit(true, true);

    // If we could mock a "real" scenario, we'd expect the distrib requests to timeout waiting
    // on the countdown latch (and thus not increment `countup`), but the "local" shard would
    // then succeed, resulting in `countup` being incremented exactly once. Here we've set `countdown`
    // artificially high, but to approximately mock this, we get the "local" shard to recognize
    // the "tooHigh" scenario, and succeed anyway, incrementing `countup`.
    assertEquals(1, countup.get());
    // sanity check: because we actually expect things to be working properly, we expect `countdown`
    // to be exactly "TOO_HIGH_INCREMENT" too high.
    assertEquals(TOO_HIGH_INCREMENT, countdown.getCount());
  }

  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assert null != client.getDefaultCollection();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(client.getDefaultCollection(),
                                                        client.getZkStateReader(),
                                                        true, true, 330);
  }

  public static class CheckFactory extends UpdateRequestProcessorFactory {
    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new Check(next);
    }
  }

  public static class Check extends UpdateRequestProcessor {

    public Check(UpdateRequestProcessor next) {
      super(next);
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      super.processCommit(cmd);
      countdown.countDown();
      // In the "tooHigh" scenario, for the one-and-only local (non-distrib) shard, force success (after waiting) to
      // mock the "serial blocking" behavior.
      final boolean mustSucceed = tooHigh == TOO_HIGH_INCREMENT && cmd.getReq().getParams().get("update.distrib") == null;
      try {
        if (!countdown.await(5, TimeUnit.SECONDS) && !mustSucceed) {
          throw new RuntimeException("done waiting");
        }
        countup.incrementAndGet();
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
