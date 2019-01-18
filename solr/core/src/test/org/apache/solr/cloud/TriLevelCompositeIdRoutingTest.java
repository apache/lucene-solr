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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


@Slow
public class TriLevelCompositeIdRoutingTest extends ShardRoutingTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  int NUM_APPS = 5;
  int NUM_USERS = 10;
  int NUM_DOCS = 100;


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
    sliceCount = TEST_NIGHTLY ? 12 : 2;             // a lot of slices for more ranges and buckets
    fixShardCount(TEST_NIGHTLY ? 24 : 3);
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(true);

      doTriLevelHashingTest();
      doTriLevelHashingTestWithBitMask();

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }

  private void doTriLevelHashingTest() throws Exception {
    log.info("### STARTING doTriLevelHashingTest");
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    for (int i = 0; i < NUM_DOCS; i++) {
      int appId = r.nextInt(NUM_APPS) + 1;
      int userId = r.nextInt(NUM_USERS) + 1;

      String id = "app" + appId + "!" + "user" + userId + "!" + "doc" + r.nextInt(100);
      doAddDoc(id);

    }

    commit();

    HashMap<String, Integer> idMap = new HashMap<>();

    for (int i = 1; i <= sliceCount; i++) {

      Set<String> ids = doQueryGetUniqueIdKeys("q", "*:*", "shards", "shard" + i);
      for (String id : ids) {
        assertFalse("Found the same route key [" + id + "] in 2 shards.", idMap.containsKey(id));
        idMap.put(getKey(id), i);
      }
    }

  }


  private void doTriLevelHashingTestWithBitMask() throws Exception {
    log.info("### STARTING doTriLevelHashingTestWithBitMask");
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.
    del("*:*");

    for (int i = 0; i < NUM_DOCS; i++) {
      int appId = r.nextInt(NUM_APPS) + 1;
      int userId = r.nextInt(NUM_USERS) + 1;
      int bitMask = r.nextInt(16) + 1;

      String id = "app" + appId + "/" + bitMask + "!" + "user" + userId + "!" + "doc" + r.nextInt(100);
      doAddDoc(id);

    }

    commit();

    HashMap<String, Integer> idMap = new HashMap<>();

    for (int i = 1; i <= sliceCount; i++) {

      Set<String> ids = doQueryGetUniqueIdKeys("q", "*:*", "shards", "shard" + i);
      for (String id : ids) {
        assertFalse("Found the same route key [" + id + "] in 2 shards.", idMap.containsKey(id));
        idMap.put(getKey(id), i);
      }
    }

  }

  void doAddDoc(String id) throws Exception {
    index("id", id);
    // todo - target diff servers and use cloud clients as well as non-cloud clients
  }

  Set<String> doQueryGetUniqueIdKeys(String... queryParams) throws Exception {
    QueryResponse rsp = cloudClient.query(params(queryParams));
    Set<String> obtainedIdKeys = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIdKeys.add(getKey((String) doc.get("id")));
    }
    return obtainedIdKeys;
  }

  private String getKey(String id) {
    return id.substring(0, id.lastIndexOf('!'));
  }
}
