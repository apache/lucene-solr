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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TriLevelCompositeIdRoutingTest extends ShardRoutingTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final int MAX_APP_ID;
  final int MAX_USER_ID;
  final int MAX_DOC_ID;
  final int NUM_ADDS;


  @BeforeClass
  public static void beforeTriLevelCompositeIdRoutingTest() throws Exception {
    // TODO: we use an fs based dir because something
    // like a ram dir will not recover correctly right now
    // because tran log will still exist on restart and ram
    // dir will not persist - perhaps translog can empty on
    // start if using an EphemeralDirectoryFactory 
    useFactory(null);
  }

  public TriLevelCompositeIdRoutingTest() {
    schemaString = "schema15.xml";      // we need a string id
    
    sliceCount = TestUtil.nextInt(random(), 1, (TEST_NIGHTLY ? 5 : 3)); // this is the number of *SHARDS*
    int replicationFactor = rarely() ? 2 : 1; // replication is not the focus of this test
    fixShardCount(replicationFactor * sliceCount); // total num cores, one per node

    MAX_APP_ID = atLeast(5);
    MAX_USER_ID = atLeast(10);
    MAX_DOC_ID = atLeast(20);
    NUM_ADDS = atLeast(200);
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13369")
  @Test
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(true);

      // NOTE: we might randomly generate the same uniqueKey value multiple times,
      // (which is a valid test case, they should route to the same shard both times)
      // so we track each expectedId in a set for later sanity checking
      final Set<String> expectedUniqueKeys = new HashSet<>();
      for (int i = 0; i < NUM_ADDS; i++) {
        final int appId = r.nextInt(MAX_APP_ID) + 1;
        final int userId = r.nextInt(MAX_USER_ID) + 1;
        // skew the odds so half the time we have no mask, and half the time we
        // have an even distribution of 1-16 bits
        final int bitMask = Math.max(0, r.nextInt(32)-15);
        
        String id = "app" + appId + (bitMask <= 0 ? "" : ("/" + bitMask))
          + "!" + "user" + userId
          + "!" + "doc" + r.nextInt(MAX_DOC_ID);
        
        doAddDoc(id);
        expectedUniqueKeys.add(id);
      }
      
      commit();
      
      final Map<String, String> routePrefixMap = new HashMap<>();
      final Set<String> actualUniqueKeys = new HashSet<>();
      for (int i = 1; i <= sliceCount; i++) {
        final String shardId = "shard" + i;
        final Set<String> uniqueKeysInShard = fetchUniqueKeysFromShard(shardId);
        
        { // sanity check our uniqueKey values aren't duplicated across shards
          final Set<String> uniqueKeysOnDuplicateShards = new HashSet<>(uniqueKeysInShard);
          uniqueKeysOnDuplicateShards.retainAll(actualUniqueKeys);
          assertEquals(shardId + " contains some uniqueKeys that were already found on a previous shard",
                       Collections.emptySet(),  uniqueKeysOnDuplicateShards);
          actualUniqueKeys.addAll(uniqueKeysInShard);
        }
        
        // foreach uniqueKey, extract it's route prefix and confirm those aren't spread across multiple shards
        for (String uniqueKey : uniqueKeysInShard) {
          final String routePrefix = uniqueKey.substring(0, uniqueKey.lastIndexOf('!'));
          log.debug("shard( {} ) : uniqueKey( {} ) -> routePrefix( {} )", shardId, uniqueKey, routePrefix);
          assertNotNull("null prefix WTF? " + uniqueKey, routePrefix);
          
          final String otherShard = routePrefixMap.put(routePrefix, shardId);
          if (null != otherShard)
            // if we already had a mapping, make sure it's an earlier doc from our current shard...
            assertEquals("routePrefix " + routePrefix + " found in multiple shards",
                         shardId, otherShard);
        }
      }

      assertEquals("Docs missing?", expectedUniqueKeys.size(), actualUniqueKeys.size());
      
      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }
  
  void doAddDoc(String id) throws Exception {
    index("id", id);
    // todo - target diff servers and use cloud clients as well as non-cloud clients
  }

  private Set<String> fetchUniqueKeysFromShard(final String shardId) throws Exception {
    // NUM_ADDS is an absolute upper bound on the num docs in the index
    QueryResponse rsp = cloudClient.query(params("q", "*:*", "rows", ""+NUM_ADDS, "shards", shardId));
    Set<String> uniqueKeys = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      final String id = (String) doc.get("id");
      assertNotNull("null id WTF? " + doc.toString(), id);
      uniqueKeys.add(id);
    }
    return uniqueKeys;
  }
}
