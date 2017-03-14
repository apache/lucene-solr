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

package org.apache.solr.recipe;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;

public class TestRuleSorter extends SolrTestCaseJ4 {
  public void testRuleParsing() {

    String rules = "{" +
        "conditions:[{node:'!overseer', strict:false}, " +
        "{replica:'<2',node:'*', shard:'#EACH'}]," +
        " preferences:[" +
        "{minimize:cores , precision:2}," +
        "{maximize:freedisk, precision:50}, " +
        "{minimize:heap, precision:1000}]}";


    Map nodeValues = (Map) Utils.fromJSONString( "{" +
        "node1:{cores:12, freedisk: 334, heap:10480}," +
        "node2:{cores:4, freedisk: 749, heap:6873}," +
        "node3:{cores:7, freedisk: 262, heap:7834}," +
        "node4:{cores:8, freedisk: 375, heap:16900}" +
        "}");

    RuleSorter ruleSorter = new RuleSorter((Map<String, Object>) Utils.fromJSONString(rules));
    RuleSorter.Session session = ruleSorter.createSession(Arrays.asList("node1", "node2","node3","node4"), (node, valuesMap) -> {
      Map n = (Map) nodeValues.get(node);
      valuesMap.entrySet().stream().forEach(e -> e.setValue(n.get(e.getKey())));
    });
    session.sort();
    List<RuleSorter.Row> l = session.getSorted();
    assertEquals("node1",l.get(0).node);
    assertEquals("node3",l.get(1).node);
    assertEquals("node4",l.get(2).node);
    assertEquals("node2",l.get(3).node);


//    System.out.println(session);

  }




}
