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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;

/**
 * Test which asserts that shards.tolerant=true works even if one shard is down
 * and also asserts that a meaningful exception is thrown when shards.tolerant=false
 * See SOLR-7566
 */
public class TestDownShardTolerantSearch extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void searchingShouldFailWithoutTolerantSearchSetToTrue() throws Exception {

    CollectionAdminRequest.createCollection("tolerant", "conf", 2, 1)
        .process(cluster.getSolrClient());

    UpdateRequest update = new UpdateRequest();
    for (int i = 0; i < 100; i++) {
      update.add("id", Integer.toString(i));
    }
    update.commit(cluster.getSolrClient(), "tolerant");

    QueryResponse response = cluster.getSolrClient().query("tolerant", new SolrQuery("*:*").setRows(1));
    assertThat(response.getStatus(), is(0));
    assertThat(response.getResults().getNumFound(), is(100L));

    JettySolrRunner stoppedServer = cluster.stopJettySolrRunner(0);
    
    cluster.waitForJettyToStop(stoppedServer);

    response = cluster.getSolrClient().query("tolerant", new SolrQuery("*:*").setRows(1).setParam(ShardParams.SHARDS_TOLERANT, true));
    assertThat(response.getStatus(), is(0));
    assertTrue(response.getResults().getNumFound() > 0);

    SolrServerException e = expectThrows(SolrServerException.class,
        "Request should have failed because we killed shard1 jetty",
        () -> cluster.getSolrClient().query("tolerant", new SolrQuery("*:*").setRows(1)
            .setParam(ShardParams.SHARDS_TOLERANT, false))
    );
    assertNotNull(e.getCause());
    assertTrue("Error message from server should have the name of the down shard",
        e.getCause().getMessage().contains("shard"));
  }
}
