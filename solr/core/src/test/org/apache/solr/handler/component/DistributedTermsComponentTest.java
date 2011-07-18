package org.apache.solr.handler.component;

/**
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

import org.apache.solr.BaseDistributedSearchTestCase;

/**
 * Test for TermsComponent distributed querying
 *
 *
 * @since solr 1.5
 */
public class DistributedTermsComponentTest extends BaseDistributedSearchTestCase {

  @Override
  public void doTest() throws Exception {
    del("*:*");
    index(id, 18, "b_t", "snake spider shark snail slug seal");
    index(id, 19, "b_t", "snake spider shark snail slug");
    index(id, 20, "b_t", "snake spider shark snail");
    index(id, 21, "b_t", "snake spider shark");
    index(id, 22, "b_t", "snake spider");
    index(id, 23, "b_t", "snake");
    index(id, 24, "b_t", "ant zebra");
    index(id, 25, "b_t", "zebra");
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);

    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.lower", "s");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "sn", "terms.lower", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.sort", "index");
  }
}
