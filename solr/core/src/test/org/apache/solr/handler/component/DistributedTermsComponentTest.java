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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4.SuppressPointFields;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.junit.Test;

/**
 * Test for TermsComponent distributed querying
 *
 *
 * @since solr 1.5
 */
@SuppressPointFields(bugUrl="https://issues.apache.org/jira/browse/SOLR-11173")
public class DistributedTermsComponentTest extends BaseDistributedSearchTestCase {

  @Test
  public void test() throws Exception {
    
    del("*:*");
    index(id, 18, "b_t", "snake a,b spider shark snail slug seal", "foo_i", "1");
    index(id, 19, "b_t", "snake spider shark snail slug", "foo_i", "2");
    index(id, 20, "b_t", "snake spider shark snail", "foo_i", "3");
    index(id, 21, "b_t", "snake spider shark", "foo_i", "2");
    index(id, 22, "b_t", "snake spider", "c_t", "snake spider");
    index(id, 23, "b_t", "snake", "c_t", "snake");
    index(id, 24, "b_t", "ant zebra", "c_t", "ant zebra");
    index(id, 25, "b_t", "zebra", "c_t", "zebra");
    commit();

    handle.clear();
    handle.put("terms", UNORDERED);

    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.lower", "s");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "sn", "terms.lower", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.list", "snake,zebra,ant,bad");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_i", "terms.list", "2,3,1");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_i", "terms.stats", "true","terms.list", "2,3,1");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.list", "snake,zebra", "terms.ttf", "true");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.fl", "c_t", "terms.list", "snake,ant,zebra", "terms.ttf", "true");
  }
}
