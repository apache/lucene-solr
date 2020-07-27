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
package org.apache.solr.cloud.autoscaling.sim;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.junit.Test;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestSimScenario extends SimSolrCloudTestCase {

  // simple scenario to test .autoAddReplicas trigger
  String autoAddReplicasScenario =
      "# standard comment\n" +
      "// java comment\n" +
      "create_cluster numNodes=2 // inline comment\n" +
      "load_autoscaling json={'cluster-policy'+:+[{'replica'+:+'<3',+'shard'+:+'#EACH',+'collection'+:+'testCollection','node':'#ANY'}]}&defaultWaitFor=10\n" +
      "solr_request /admin/collections?action=CREATE&autoAddReplicas=true&name=testCollection&numShards=2&replicationFactor=2&maxShardsPerNode=2\n" +
      "wait_collection collection=testCollection&shards=2&replicas=2\n" +
      "event_listener trigger=.auto_add_replicas&stage=SUCCEEDED\n" +
      "kill_nodes node=${_random_node_}\n" +
      "wait_event trigger=.auto_add_replicas&wait=60\n" +
      "wait_collection collection=testCollection&shards=2&replicas=2\n" +
      "save_snapshot path=${snapshotPath}\n";

  @Test
  public void testAutoAddReplicas() throws Exception {
    String snapshotPath = createTempDir() + "/snapshot";
    try (SimScenario scenario = SimScenario.load(autoAddReplicasScenario)) {
      scenario.context.put("snapshotPath", snapshotPath);
      scenario.run();
    }
    SnapshotCloudManager snapshotCloudManager = SnapshotCloudManager.readSnapshot(new File(snapshotPath));
    CloudUtil.waitForState(snapshotCloudManager, "testCollection", 1, TimeUnit.SECONDS,
        CloudUtil.clusterShape(2, 2));
  }

  String testSuggestionsScenario =
      "create_cluster numNodes=2\n" +
      "solr_request /admin/collections?action=CREATE&autoAddReplicas=true&name=testCollection&numShards=2&replicationFactor=2&maxShardsPerNode=2\n" +
      "wait_collection collection=testCollection&shards=2&replicas=2\n" +
      "ctx_set key=myNode&value=${_random_node_}\n" +
      "solr_request /admin/collections?action=ADDREPLICA&collection=testCollection&shard=shard1&node=${myNode}\n" +
      "solr_request /admin/collections?action=ADDREPLICA&collection=testCollection&shard=shard1&node=${myNode}\n" +
      "loop_start iterations=${iterative}\n" +
      "  calculate_suggestions\n" +
      "  apply_suggestions\n" +
      "  solr_request /admin/collections?action=ADDREPLICA&collection=testCollection&shard=shard1&node=${myNode}\n" +
      "  solr_request /admin/collections?action=ADDREPLICA&collection=testCollection&shard=shard1&node=${myNode}\n" +
      "loop_end\n" +
      "loop_start iterations=${justCalc}\n" +
      "  calculate_suggestions\n" +
      "  save_snapshot path=${snapshotPath}/${_loop_iter_}\n" +
      "loop_end\n" +
      "dump redact=true";

  @Test
  public void testSuggestions() throws Exception {
    String snapshotPath = createTempDir() + "/snapshot";
    try (SimScenario scenario = SimScenario.load(testSuggestionsScenario)) {
      scenario.context.put("snapshotPath", snapshotPath);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos, true, "UTF-8");
      scenario.console = ps;
      scenario.context.put("iterative", "0");
      scenario.context.put("justCalc", "1");
      scenario.run();
      @SuppressWarnings({"unchecked"})
      List<Suggester.SuggestionInfo> suggestions = (List<Suggester.SuggestionInfo>)scenario.context.get(SimScenario.SUGGESTIONS_CTX_PROP);
      assertNotNull(suggestions);
      assertEquals(suggestions.toString(), 1, suggestions.size());
      // reconstruct the snapshot from the dump
      @SuppressWarnings({"unchecked"})
      Map<String, Object> snapshot = (Map<String, Object>)Utils.fromJSON(baos.toByteArray());
      @SuppressWarnings({"unchecked"})
      Map<String, Object> autoscalingState = (Map<String, Object>)snapshot.get(SnapshotCloudManager.AUTOSCALING_STATE_KEY);
      assertNotNull(autoscalingState);
      assertEquals(autoscalingState.toString(), 1, autoscalingState.size());
      assertTrue(autoscalingState.toString(), autoscalingState.containsKey("suggestions"));
      @SuppressWarnings({"unchecked"})
      List<Map<String, Object>> snapSuggestions = (List<Map<String, Object>>)autoscalingState.get("suggestions");
      assertEquals(snapSuggestions.toString(), 1, snapSuggestions.size());
      // _loop_iter_ should be present and 0 (first iteration)
      assertEquals("0", scenario.context.get(SimScenario.LOOP_ITER_PROP));
    }
    // try looping more times
    try (SimScenario scenario = SimScenario.load(testSuggestionsScenario)) {
      scenario.context.put("iterative", "10");
      scenario.context.put("justCalc", "0");
      scenario.run();
      assertEquals("9", scenario.context.get(SimScenario.LOOP_ITER_PROP));
    }

  }

  String indexingScenario =
      "create_cluster numNodes=100\n" +
      "solr_request /admin/collections?action=CREATE&autoAddReplicas=true&name=testCollection&numShards=2&replicationFactor=2&maxShardsPerNode=2\n" +
      "wait_collection collection=testCollection&shards=2&replicas=2\n" +
      "solr_request /admin/autoscaling?httpMethod=POST&stream.body=" +
          "{'set-trigger':{'name':'indexSizeTrigger','event':'indexSize','waitFor':'10s','aboveDocs':1000,'enabled':true,"+
          "'actions':[{'name':'compute_plan','class':'solr.ComputePlanAction'},{'name':'execute_plan','class':'solr.ExecutePlanAction'}]}}\n" +
      "event_listener trigger=indexSizeTrigger&stage=SUCCEEDED\n" +
      "index_docs collection=testCollection&numDocs=3000\n" +
      "run\n" +
      "wait_event trigger=indexSizeTrigger&wait=60\n" +
      "assert condition=not_null&key=_trigger_event_indexSizeTrigger\n" +
      "assert condition=equals&key=_trigger_event_indexSizeTrigger/eventType&expected=INDEXSIZE\n" +
      "assert condition=equals&key=_trigger_event_indexSizeTrigger/properties/requestedOps[0]/action&expected=SPLITSHARD\n" +
      "wait_collection collection=testCollection&shards=6&withInactive=true&requireLeaders=false&replicas=2";

  @Test
  public void testIndexing() throws Exception {
    try (SimScenario scenario = SimScenario.load(indexingScenario)) {
      scenario.run();
    }
  }

  String splitShardScenario =
      "create_cluster numNodes=2\n" +
          "solr_request /admin/collections?action=CREATE&name=testCollection&numShards=2&replicationFactor=2&maxShardsPerNode=5\n" +
          "wait_collection collection=testCollection&shards=2&replicas=2\n" +
          "set_shard_metrics collection=testCollection&shard=shard1&INDEX.sizeInBytes=1000000000\n" +
          "set_node_metrics nodeset=#ANY&freedisk=1.5\n" +
          "solr_request /admin/collection?action=SPLITSHARD&collection=testCollection&shard=shard1&splitMethod=${method}\n" +
          "wait_collection collection=testCollection&shards=4&&withInactive=true&replicas=2&requireLeaders=true\n"
      ;
  @Test
  public void testSplitShard() throws Exception {
    try (SimScenario scenario = SimScenario.load(splitShardScenario)) {
      scenario.context.put("method", "REWRITE");
      scenario.run();
    } catch (Exception e) {
      assertTrue(e.toString(), e.toString().contains("not enough free disk"));
    }
    try (SimScenario scenario = SimScenario.load(splitShardScenario)) {
      scenario.context.put("method", "LINK");
      scenario.run();
    } catch (Exception e) {
      fail("should have succeeded with method LINK, but failed: " + e.toString());
    }
  }
}
