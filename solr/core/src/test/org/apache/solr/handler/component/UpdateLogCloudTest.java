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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateLogCloudTest extends SolrCloudTestCase {

  private static String COLLECTION;
  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 4;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // create and configure cluster
    configureCluster(NUM_SHARDS*NUM_REPLICAS /* nodeCount */)
    .addConfig("conf", configset("cloud-dynamic"))
    .configure();
  }

  @Before
  public void beforeTest() throws Exception {

    // decide collection name ...
    COLLECTION = "collection"+(1+random().nextInt(100)) ;

    // create an empty collection
    CollectionAdminRequest
    .createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
    .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);
  }

  @After
  public void afterTest() throws Exception {
    CollectionAdminRequest
    .deleteCollection(COLLECTION)
    .process(cluster.getSolrClient());
  }

  @Test
  public void test() throws Exception {

    int specialIdx = 0;

    final List<SolrClient> solrClients = new ArrayList<>();
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      if (!jettySolrRunner.getBaseUrl().toString().equals(
          getCollectionState(COLLECTION).getLeader("shard1").getBaseUrl())) {
        specialIdx = solrClients.size();
      }
      solrClients.add(jettySolrRunner.newClient());
    }

    cluster.getJettySolrRunner(specialIdx).stop();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);

    new UpdateRequest()
    .add(sdoc("id", "1", "a_t", "one"))
    .deleteById("2")
    .deleteByQuery("a_t:three")
    .commit(cluster.getSolrClient(), COLLECTION);

    cluster.getJettySolrRunner(specialIdx).start();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);

    int idx = 0;
    for (SolrClient solrClient : solrClients) {
      implTest(solrClient, idx==specialIdx ? 0 : 3);
      ++idx;
    }

    for (SolrClient solrClient : solrClients) {
      solrClient.close();
    }

  }

  @SuppressWarnings("unchecked")
  private void implTest(SolrClient solrClient, int numExpected) throws Exception {

    final QueryRequest reqV = new QueryRequest(params("qt","/get", "getVersions","12345"));
    final NamedList<?> rspV = solrClient.request(reqV, COLLECTION);
    final List<Long> versions = (List<Long>)rspV.get("versions");
    assertEquals(versions.toString(), numExpected, versions.size());
    if (numExpected == 0) {
      return;
    }

    final LinkedList<Long> absVersions = new LinkedList<>();
    for (Long version : versions) {
      absVersions.add(Math.abs(version));
    }
    Collections.sort(absVersions);
    final Long minVersion = absVersions.getFirst();
    final Long maxVersion = absVersions.getLast();

    for (boolean skipDbq : new boolean[] { false, true }) {
      final QueryRequest reqU = new QueryRequest(params("qt","/get", "getUpdates", minVersion + "..."+maxVersion, "skipDbq", Boolean.toString(skipDbq)));
      final NamedList<?> rspU = solrClient.request(reqU, COLLECTION);
      final List<?> updatesList = (List<?>)rspU.get("updates");
      assertEquals(updatesList.toString(), numExpected, updatesList.size());
    }

  }

}
