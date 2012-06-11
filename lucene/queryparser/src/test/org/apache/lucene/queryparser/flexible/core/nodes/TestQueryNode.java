package org.apache.lucene.queryparser.flexible.core.nodes;

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

import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;

public class TestQueryNode extends LuceneTestCase {
 
  /* LUCENE-2227 bug in QueryNodeImpl.add() */
  public void testAddChildren() throws Exception {
    QueryNode nodeA = new FieldQueryNode("foo", "A", 0, 1);
    QueryNode nodeB = new FieldQueryNode("foo", "B", 1, 2);
    BooleanQueryNode bq = new BooleanQueryNode(
        Arrays.asList(nodeA));
    bq.add(Arrays.asList(nodeB));
    assertEquals(2, bq.getChildren().size());
  }
  
  /* LUCENE-3045 bug in QueryNodeImpl.containsTag(String key)*/
  public void testTags() throws Exception {
    QueryNode node = new FieldQueryNode("foo", "A", 0, 1);
    
    node.setTag("TaG", new Object());
    assertTrue(node.getTagMap().size() > 0);
    assertTrue(node.containsTag("tAg"));
    assertTrue(node.getTag("tAg") != null);
    
  }
  
}
