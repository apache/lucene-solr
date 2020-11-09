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
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

/**
 * See SOLR-9504
 */
@Ignore // nocommit debug, we don't get a leader till the addReplica fails - is it hanging onto a higher leader ephem election node?
public class TestLeaderElectionWithEmptyReplica extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "solr_9504";

  @BeforeClass
  public static void beforeClass() throws Exception {

    useFactory(null);
    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "config", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void test() throws Exception {
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();
    solrClient.setDefaultCollection(COLLECTION_NAME);
    for (int i=0; i<10; i++)  {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      solrClient.add(doc);
    }
    solrClient.commit();

    // find the leader node

    JettySolrRunner replicaJetty = cluster.getShardLeaderJetty(COLLECTION_NAME, "s1");


    // kill the leader
    replicaJetty.stop();

    // add a replica (asynchronously)
    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, "s1");
    String asyncId = addReplica.processAsync(solrClient);

    //cluster.waitForActiveCollection(COLLECTION_NAME, 1, 2);


    // bring the old leader node back up
    replicaJetty.start();

    cluster.getZkClient().printLayout();

    cluster.waitForActiveCollection(COLLECTION_NAME, 1, 2);

    // now query each replica and check for consistency
    assertConsistentReplicas(solrClient, solrClient.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getSlice("s1"));

    // sanity check that documents still exist
    QueryResponse response = solrClient.query(new SolrQuery("*:*"));
    assertEquals("Indexed documents not found", 10, response.getResults().getNumFound());
  }

  private static int assertConsistentReplicas(CloudHttp2SolrClient cloudClient, Slice shard) throws SolrServerException, IOException {
    long numFound = Long.MIN_VALUE;
    int count = 0;
    for (Replica replica : shard.getReplicas()) {
      Http2SolrClient client = new Http2SolrClient.Builder(replica.getCoreUrl())
          .withHttpClient(cloudClient.getHttpClient()).build();
      QueryResponse response = client.query(new SolrQuery("q", "*:*", "distrib", "false"));
      log.info("Found numFound={} on replica: {}", response.getResults().getNumFound(), replica.getCoreUrl());
      if (numFound == Long.MIN_VALUE)  {
        numFound = response.getResults().getNumFound();
      } else  {
        assertEquals("Shard " + shard.getName() + " replicas do not have same number of documents", numFound, response.getResults().getNumFound());
      }
      count++;
    }
    return count;
  }
}
