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

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ShardTermsTest extends SolrTestCase {
  @Test
  public void testIncreaseTerms() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 0L);
    ShardTerms terms = new ShardTerms(map, 0);
    terms = terms.increaseTerms("leader", Collections.singleton("replica"));
    assertEquals(1L, terms.getTerm("leader").longValue());

    map.put("leader", 2L);
    map.put("live-replica", 2L);
    map.put("dead-replica", 1L);
    terms = new ShardTerms(map, 0);
    assertNull(terms.increaseTerms("leader", Collections.singleton("dead-replica")));

    terms = terms.increaseTerms("leader", Collections.singleton("leader"));
    assertEquals(3L, terms.getTerm("live-replica").longValue());
    assertEquals(2L, terms.getTerm("leader").longValue());
    assertEquals(1L, terms.getTerm("dead-replica").longValue());
  }
}
