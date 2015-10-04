package org.apache.solr.cloud;

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

import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
@Nightly
public class LeaderInitiatedRecoveryOnShardRestartTest extends AbstractFullDistribZkTestBase {
  
  protected static final transient Logger log = LoggerFactory.getLogger(LeaderInitiatedRecoveryOnShardRestartTest.class);
  

  public LeaderInitiatedRecoveryOnShardRestartTest() {
    super();
    sliceCount = 1;
    fixShardCount(2);
  }
  
  @Test
  public void testRestartWithAllInLIR() throws Exception {
    waitForThingsToLevelOut(30000);

    String testCollectionName = "all_in_lir";
    String shardId = "shard1";
    createCollection(testCollectionName, 1, 2, 1);

    cloudClient.setDefaultCollection(testCollectionName);

    Map<String,Object> stateObj = Utils.makeMap();
    stateObj.put(ZkStateReader.STATE_PROP, "down");
    stateObj.put("createdByNodeName", "test");
    stateObj.put("createdByCoreNodeName", "test");
    
    byte[] znodeData = Utils.toJSON(stateObj);
    
    SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
    zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node1", znodeData, true);
    zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node2", znodeData, true);
    
    printLayout();
    
    for (JettySolrRunner jetty : jettys) {
      ChaosMonkey.stop(jetty);
    }
    
    Thread.sleep(2000);
    
    for (JettySolrRunner jetty : jettys) {
      ChaosMonkey.start(jetty);
    }
    
    // recoveries will not finish without SOLR-8075
    waitForRecoveriesToFinish(testCollectionName, true);
  }
}
