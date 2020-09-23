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
package org.apache.solr.search.stats;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.junit.Test;

public class TestExactStatsCache extends TestBaseStatsCache {
  private int docId = 0;

  @Override
  protected String getStatsCacheClassName() {
    return ExactStatsCache.class.getName();
  }

  @Test
  @ShardsFixed(num = 3)
  public void testShardsTolerant() throws Exception {
    del("*:*");
    commit();
    for (int i = 0; i < clients.size(); i++) {
      int shard = i + 1;
      index_specific(i, id, docId++, "a_t", "one two three",
              "shard_i", shard);
      index_specific(i, id, docId++, "a_t", "one two three four five",
              "shard_i", shard);
    }
    commit();
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    checkShardsTolerantQuery("q", "a_t:one", "debugQuery", "true", "fl", "*,score");
  }

  protected void checkShardsTolerantQuery(Object... q) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }

    // query a random server
    params.set(ShardParams.SHARDS, getShardsStringWithOneDeadShard());
    params.set(ShardParams.SHARDS_TOLERANT, "true");
    int which = r.nextInt(clients.size());
    SolrClient client = clients.get(which);
    QueryResponse rsp = client.query(params);
    checkPartialResponse(rsp);
  }

  protected String getShardsStringWithOneDeadShard() {
    if (deadServers == null) return shards;

    StringBuilder sb = new StringBuilder();
    for (int shardN = 0; shardN < shardsArr.length; shardN++) {
      if (sb.length() > 0) sb.append(',');

      String shard;
      if (shardsArr.length == 1 || shardN != shardsArr.length - 1) {
        shard = shardsArr[shardN];
      } else {
        if (deadServers[0].endsWith("/")) shard = deadServers[0] + DEFAULT_TEST_COLLECTION_NAME;
        else shard = deadServers[0] + "/" + DEFAULT_TEST_CORENAME;
      }
      sb.append(shard);
    }

    return sb.toString();
  }


  protected void checkPartialResponse(QueryResponse shardRsp) {
    System.out.println("======================= Shard Response =======================");
    System.out.println("");
    System.out.println(shardRsp);
    SolrDocumentList shardList = shardRsp.getResults();
    assertEquals(4, shardList.size());
    assertEquals(4, shardList.getNumFound());
  }
}