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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;

/**
 * Test which asserts that shards.tolerant=true works even if one shard is down
 * and also asserts that a meaningful exception is thrown when shards.tolerant=false
 * See SOLR-7566
 */
public class TestDownShardTolerantSearch extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TestDownShardTolerantSearch() {
    sliceCount = 2;
  }

  @Test
  @ShardsFixed(num = 2)
  public void searchingShouldFailWithoutTolerantSearchSetToTrue() throws Exception {
    waitForRecoveriesToFinish(true);

    indexAbunchOfDocs();
    commit();
    QueryResponse response = cloudClient.query(new SolrQuery("*:*").setRows(1));
    assertThat(response.getStatus(), is(0));
    assertThat(response.getResults().getNumFound(), is(66L));

    ChaosMonkey.kill(shardToJetty.get(SHARD1).get(0));

    response = cloudClient.query(new SolrQuery("*:*").setRows(1).setParam(ShardParams.SHARDS_TOLERANT, true));
    assertThat(response.getStatus(), is(0));
    assertTrue(response.getResults().getNumFound() > 0);

    try {
      cloudClient.query(new SolrQuery("*:*").setRows(1).setParam(ShardParams.SHARDS_TOLERANT, false));
      fail("Request should have failed because we killed shard1 jetty");
    } catch (SolrServerException e) {
      log.info("error from server", e);
      assertNotNull(e.getCause());
      assertTrue("Error message from server should have the name of the down shard",
          e.getCause().getMessage().contains(SHARD1));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
