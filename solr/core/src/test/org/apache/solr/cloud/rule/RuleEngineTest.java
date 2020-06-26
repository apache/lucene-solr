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
package org.apache.solr.cloud.rule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.autoscaling.DelegatingCloudManager;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.Snitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.solr.cloud.rule.Rule.parseRule;
import static org.apache.solr.common.util.Utils.makeMap;

public class RuleEngineTest extends SolrTestCaseJ4{
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testPlacement2(){


    String s = "{" +
        "  '127.0.0.1:49961_':{" +
        "    'node':'127.0.0.1:49961_'," +
        "    'freedisk':992," +
        "    'cores':1}," +
        "  '127.0.0.1:49955_':{" +
        "    'node':'127.0.0.1:49955_'," +
        "    'freedisk':992," +
        "    'cores':1}," +
        "  '127.0.0.1:49952_':{" +
        "    'node':'127.0.0.1:49952_'," +
        "    'freedisk':992," +
        "    'cores':1}," +
        "  '127.0.0.1:49947_':{" +
        "    'node':'127.0.0.1:49947_'," +
        "    'freedisk':992," +
        "    'cores':1}," +
        "  '127.0.0.1:49958_':{" +
        "    'node':'127.0.0.1:49958_'," +
        "    'freedisk':992," +
        "    'cores':1}}";
    MockSnitch.nodeVsTags = (Map) Utils.fromJSON(s.getBytes(StandardCharsets.UTF_8));
    Map shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);

    List<Rule> rules = parseRules("[{'cores':'<4'}, {" +
            "'replica':'1',shard:'*','node':'*'}," +
            " {'freedisk':'>1'}]");

    Map<ReplicaPosition, String> mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null ).getNodeMappings();
    assertNotNull(mapping);

    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null ).getNodeMappings();
    assertNotNull(mapping);

    rules = parseRules("[{role:'!overseer'}, {'freedisk':'>1'}]" );
    Map<String, Object> snitchSession = new HashMap<>();
    List<String> preferredOverseerNodes = ImmutableList.of("127.0.0.1:49947_", "127.0.0.1:49952_");
    ReplicaAssigner replicaAssigner = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null) {

      @Override
      protected SnitchContext getSnitchCtx(String node, SnitchInfoImpl info, SolrCloudManager cloudManager) {
        return new ServerSnitchContext(info, node, snitchSession,cloudManager){
          @Override
          @SuppressWarnings({"rawtypes"})
          public Map getZkJson(String path) {
            if(ZkStateReader.ROLES.equals(path)){
              return Collections.singletonMap("overseer", preferredOverseerNodes);
            }
            return null;
          }

        };
      }

    };
    mapping = replicaAssigner.getNodeMappings();
    assertNotNull(mapping);

    for (String nodeName : mapping.values()) {
      assertFalse(preferredOverseerNodes.contains(nodeName));
    }

  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testPlacement3(){
    String s = "{" +
        "  '127.0.0.1:49961_':{" +
        "    'node':'127.0.0.1:49961_'," +
        "    'freedisk':992," +
        "    'cores':1}," +
        "  '127.0.0.2:49955_':{" +
        "    'node':'127.0.0.1:49955_'," +
        "    'freedisk':995," +
        "    'cores':1}," +
        "  '127.0.0.3:49952_':{" +
        "    'node':'127.0.0.1:49952_'," +
        "    'freedisk':990," +
        "    'cores':1}," +
        "  '127.0.0.1:49947_':{" +
        "    'node':'127.0.0.1:49947_'," +
        "    'freedisk':980," +
        "    'cores':1}," +
        "  '127.0.0.2:49958_':{" +
        "    'node':'127.0.0.1:49958_'," +
        "    'freedisk':970," +
        "    'cores':1}}";
    MockSnitch.nodeVsTags = (Map) Utils.fromJSON(s.getBytes(StandardCharsets.UTF_8));
    //test not
    List<Rule> rules = parseRules(
         "[{cores:'<4'}, " +
            "{replica:'1',shard:'*',node:'*'}," +
            "{node:'!127.0.0.1:49947_'}," +
            "{freedisk:'>1'}]");
    Map shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);
    Map<ReplicaPosition, String> mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings();
    assertNotNull(mapping);
    assertFalse(mapping.containsValue("127.0.0.1:49947_"));

    rules = parseRules(
         "[{cores:'<4'}, " +
            "{replica:'1',node:'*'}," +
            "{freedisk:'>980'}]");
    shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings0();
    assertNull(mapping);


    rules = parseRules(
        "[{cores:'<4'}, " +
            "{replica:'1',node:'*'}," +
            "{freedisk:'>980~'}]");
    shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings0();
    assertNotNull(mapping);
    assertFalse(mapping.containsValue("127.0.0.2:49958_"));

    rules = parseRules(
        "[{cores:'<4'}, " +
            "{replica:'1',shard:'*',host:'*'}]"
            );
    shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings();
    assertNotNull(mapping);

    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, Collections.emptyList(),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), new DelegatingCloudManager(null){
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new NodeStateProvider() {
          @Override
          public void close() throws IOException { }

          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            return (Map<String, Object>) MockSnitch.nodeVsTags.get(node);
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            return null;
          }
        };
      }
    }, null).getNodeMappings();
    assertNotNull(mapping);



    rules = parseRules(
        "[{cores:'<4'}, " +
            "{replica:'1',shard:'**',host:'*'}]"
            );
    shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings0();
    assertNull(mapping);

    rules = parseRules(
        "[{cores:'<4'}, " +
            "{replica:'1~',shard:'**',host:'*'}]"
            );
    shardVsReplicaCount = makeMap("shard1", 2, "shard2", 2);
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings();
    assertNotNull(mapping);


  }

  @SuppressWarnings({"rawtypes"})
  private List<Rule> parseRules(String s) {

    List maps = (List) Utils.fromJSON(s.getBytes(StandardCharsets.UTF_8));

    List<Rule> rules = new ArrayList<>();
    for (Object map : maps) rules.add(new Rule((Map) map));
    return rules;
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testPlacement() throws Exception {
    String rulesStr = "rack:*,replica:<2";
    List<Rule> rules = parse(Arrays.asList(rulesStr));
    Map shardVsReplicaCount = makeMap("shard1", 3, "shard2", 3);
    Map nodeVsTags = makeMap(
        "node1:80", makeMap("rack", "178"),
        "node2:80", makeMap("rack", "179"),
        "node3:80", makeMap("rack", "180"),
        "node4:80", makeMap("rack", "181"),
        "node5:80", makeMap("rack", "182")
    );
    MockSnitch.nodeVsTags = nodeVsTags;
    Map<ReplicaPosition, String> mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null, null).getNodeMappings0();
    assertNull(mapping);
    rulesStr = "rack:*,replica:<2~";
    rules = parse(Arrays.asList(rulesStr));
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null ,null).getNodeMappings();
    assertNotNull(mapping);

    rulesStr = "rack:*,shard:*,replica:<2";//for each shard there can be a max of 1 replica
    rules = parse(Arrays.asList(rulesStr));
    mapping = new ReplicaAssigner(
        rules,
        shardVsReplicaCount, singletonList(MockSnitch.class.getName()),
        new HashMap(), new ArrayList<>(MockSnitch.nodeVsTags.keySet()), null,null ).getNodeMappings();
    assertNotNull(mapping);
  }

  public static class MockSnitch extends Snitch {
    @SuppressWarnings({"rawtypes"})
    static Map nodeVsTags = Collections.emptyMap();

    @Override
    @SuppressWarnings({"unchecked"})
    public void getTags(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
      ctx.getTags().putAll((Map<? extends String, ?>) nodeVsTags.get(solrNode));
    }

    @Override
    public boolean isKnownTag(String tag) {
      @SuppressWarnings({"rawtypes"})
      Map next = (Map) nodeVsTags.values().iterator().next();
      return next.containsKey(tag);
    }
  }

  public static List<Rule> parse(List<String> rules) throws IOException {
    assert rules != null && !rules.isEmpty();
    ArrayList<Rule> result = new ArrayList<>();
    for (String s : rules) {
      if (s == null || s.trim().isEmpty()) continue;
      result.add(new Rule(parseRule(s)));
    }
    return result;
  }
}
