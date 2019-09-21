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
package org.apache.solr.cloud.cdcr;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.Nightly;

@Nightly // test is too long for non nightly
public class CdcrOpsAndBoundariesTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  MiniSolrCloudCluster target, source;
  CloudSolrClient sourceSolrClient, targetSolrClient;
  private static String SOURCE_COLLECTION = "cdcr-source";
  private static String TARGET_COLLECTION = "cdcr-target";
  private static String ALL_Q = "*:*";

  @Before
  public void before() throws Exception {
    target = new MiniSolrCloudCluster(1, createTempDir(TARGET_COLLECTION), buildJettyConfig("/solr"));
    System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());
    source = new MiniSolrCloudCluster(1, createTempDir(SOURCE_COLLECTION), buildJettyConfig("/solr"));
  }

  @After
  public void after() throws Exception {
    if (null != target) {
      target.shutdown();
      target = null;
    }
    if (null != source) {
      source.shutdown();
      source = null;
    }
  }

  /**
   * Check the ops statistics.
   */
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testOps() throws Exception {
    createCollections();

    // Start CDCR
    CdcrTestsUtil.cdcrRestart(sourceSolrClient);

    // Index documents
    CdcrTestsUtil.indexRandomDocs(100, sourceSolrClient);
    double opsAll = 0.0;
    NamedList ops = null;

    // calculate ops
    int itr = 10;
    while (itr-- > 0 && opsAll == 0.0) {
      NamedList rsp = CdcrTestsUtil.invokeCdcrAction(sourceSolrClient, CdcrParams.CdcrAction.OPS).getResponse();
      NamedList collections = (NamedList) ((NamedList) rsp.get(CdcrParams.OPERATIONS_PER_SECOND)).getVal(0);
      ops = (NamedList) collections.get(TARGET_COLLECTION);
      opsAll = (Double) ops.get(CdcrParams.COUNTER_ALL);
      Thread.sleep(250); // wait for cdcr to complete and check
    }
    // asserts ops values
    double opsAdds = (Double) ops.get(CdcrParams.COUNTER_ADDS);
    assertTrue(opsAll > 0);
    assertTrue(opsAdds > 0);
    double opsDeletes = (Double) ops.get(CdcrParams.COUNTER_DELETES);
    assertEquals(0, opsDeletes, 0);

    // Delete 10 documents: 10-19
    List<String> ids;
    for (int id = 0; id < 50; id++) {
      ids = new ArrayList<>();
      ids.add(Integer.toString(id));
      sourceSolrClient.deleteById(ids, 1);
      int dbq_id = 50 + id;
      sourceSolrClient.deleteByQuery("id:" + dbq_id, 1);
    }

    itr = 10;
    while (itr-- > 0) {
      NamedList rsp = CdcrTestsUtil.invokeCdcrAction(sourceSolrClient, CdcrParams.CdcrAction.OPS).getResponse();
      NamedList collections = (NamedList) ((NamedList) rsp.get(CdcrParams.OPERATIONS_PER_SECOND)).getVal(0);
      ops = (NamedList) collections.get(TARGET_COLLECTION);
      opsAll = (Double) ops.get(CdcrParams.COUNTER_ALL);
      Thread.sleep(250); // wait for cdcr to complete and check
    }
    // asserts ops values
    opsAdds = (Double) ops.get(CdcrParams.COUNTER_ADDS);
    opsDeletes = (Double) ops.get(CdcrParams.COUNTER_DELETES);
    assertTrue(opsAll > 0);
    assertTrue(opsAdds > 0);
    assertTrue(opsDeletes > 0);

    deleteCollections();
  }

  @Test
  public void testTargetCollectionNotAvailable() throws Exception {
    createCollections();

    // send start action to first shard
    CdcrTestsUtil.cdcrStart(sourceSolrClient);

    assertNotSame(null, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    // sleep for a bit to ensure that replicator threads are started
    Thread.sleep(3000);

    target.deleteAllCollections();

    CdcrTestsUtil.indexRandomDocs(6, sourceSolrClient);
    assertEquals(6L, sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound());

    // we need to wait until the replicator thread is triggered
    int cnt = 15; // timeout after 15 seconds
    AssertionError lastAssertionError = null;
    while (cnt > 0) {
      try {
        QueryResponse rsp = CdcrTestsUtil.invokeCdcrAction(sourceSolrClient, CdcrParams.CdcrAction.ERRORS);
        NamedList collections = (NamedList) ((NamedList) rsp.getResponse().get(CdcrParams.ERRORS)).getVal(0);
        NamedList errors = (NamedList) collections.get(TARGET_COLLECTION);
        assertTrue(0 < (Long) errors.get(CdcrParams.CONSECUTIVE_ERRORS));
        NamedList lastErrors = (NamedList) errors.get(CdcrParams.LAST);
        assertNotNull(lastErrors);
        assertTrue(0 < lastErrors.size());
        deleteCollections();
        return;
      } catch (AssertionError e) {
        lastAssertionError = e;
        cnt--;
        Thread.sleep(1000);
      }
    }

    deleteCollections();
    throw new AssertionError("Timeout while trying to assert replication errors", lastAssertionError);
  }

  @Test
  public void testReplicationStartStop() throws Exception {
    createCollections();

    CdcrTestsUtil.indexRandomDocs(10, sourceSolrClient);
    CdcrTestsUtil.cdcrStart(sourceSolrClient);

    assertEquals(10, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    CdcrTestsUtil.cdcrStop(sourceSolrClient);

    CdcrTestsUtil.indexRandomDocs(110, sourceSolrClient);

    // Start again CDCR, the source cluster should reinitialise its log readers
    // with the latest checkpoints

    CdcrTestsUtil.cdcrRestart(sourceSolrClient);

    assertEquals(110, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    deleteCollections();
  }

  /**
   * Check that batch updates with deletes
   */
  @Test
  public void testBatchAddsWithDelete() throws Exception {
    createCollections();

    // Start CDCR
    CdcrTestsUtil.cdcrRestart(sourceSolrClient);
    // Index 50 documents
    CdcrTestsUtil.indexRandomDocs(50, sourceSolrClient);

    // Delete 10 documents: 10-19
    List<String> ids = new ArrayList<>();
    for (int id = 10; id < 20; id++) {
      ids.add(Integer.toString(id));
    }
    sourceSolrClient.deleteById(ids, 10);

    CdcrTestsUtil.indexRandomDocs(50, 60, sourceSolrClient);

    // Delete 1 document: 50
    ids = new ArrayList<>();
    ids.add(Integer.toString(50));
    sourceSolrClient.deleteById(ids, 10);

    CdcrTestsUtil.indexRandomDocs(60, 70, sourceSolrClient);

    assertEquals(59, CdcrTestsUtil.waitForClusterToSync(59, sourceSolrClient));
    assertEquals(59, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    deleteCollections();
  }

  /**
   * Checks that batches are correctly constructed when batch boundaries are reached.
   */
  @Test
  public void testBatchBoundaries() throws Exception {
    createCollections();

    // Start CDCR
    CdcrTestsUtil.cdcrRestart(sourceSolrClient);

    log.info("Indexing documents");

    CdcrTestsUtil.indexRandomDocs(1000, sourceSolrClient);

    assertEquals(1000, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    deleteCollections();
  }

  /**
   * Check resilience of replication with delete by query executed on targets
   */
  @Test
  public void testResilienceWithDeleteByQueryOnTarget() throws Exception {
    createCollections();

    // Start CDCR
    CdcrTestsUtil.cdcrRestart(sourceSolrClient);

    CdcrTestsUtil.indexRandomDocs(50, sourceSolrClient);

    assertEquals(50, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    sourceSolrClient.deleteByQuery(ALL_Q, 1);

    assertEquals(0, CdcrTestsUtil.waitForClusterToSync(0, sourceSolrClient));
    assertEquals(0, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    CdcrTestsUtil.indexRandomDocs(51, 101, sourceSolrClient);

    assertEquals(50, CdcrTestsUtil.waitForClusterToSync
        (sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound(), targetSolrClient));

    targetSolrClient.deleteByQuery(ALL_Q, 1);

    assertEquals(50, sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound());
    assertEquals(0, CdcrTestsUtil.waitForClusterToSync(0, targetSolrClient));

    CdcrTestsUtil.indexRandomDocs(102, 152, sourceSolrClient);

    assertEquals(100, sourceSolrClient.query(new SolrQuery(ALL_Q)).getResults().getNumFound());
    assertEquals(50, CdcrTestsUtil.waitForClusterToSync(50, targetSolrClient));

    deleteCollections();
  }

  private void createSourceCollection() throws Exception {
    source.uploadConfigSet(configset(SOURCE_COLLECTION), SOURCE_COLLECTION);
    CollectionAdminRequest.createCollection(SOURCE_COLLECTION, SOURCE_COLLECTION, 1, 1)
        .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
        .process(source.getSolrClient());
    Thread.sleep(1000);
    sourceSolrClient = source.getSolrClient();
    sourceSolrClient.setDefaultCollection(SOURCE_COLLECTION);
  }

  private void createTargetCollection() throws Exception {
    target.uploadConfigSet(configset(TARGET_COLLECTION), TARGET_COLLECTION);
    CollectionAdminRequest.createCollection(TARGET_COLLECTION, TARGET_COLLECTION, 1, 1)
        .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
        .process(target.getSolrClient());
    Thread.sleep(1000);
    targetSolrClient = target.getSolrClient();
    targetSolrClient.setDefaultCollection(TARGET_COLLECTION);
  }

  private void deleteSourceCollection() throws Exception {
    source.deleteAllCollections();
  }

  private void deleteTargetcollection() throws Exception {
    target.deleteAllCollections();
  }

  private void createCollections() throws Exception {
    createTargetCollection();
    createSourceCollection();
  }

  private void deleteCollections() throws Exception {
    deleteSourceCollection();
    deleteTargetcollection();
  }

}
