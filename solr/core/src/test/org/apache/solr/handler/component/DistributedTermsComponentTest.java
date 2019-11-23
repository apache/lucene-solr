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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

/**
 * Test for TermsComponent distributed querying
 *
 *
 * @since solr 1.5
 */
public class DistributedTermsComponentTest extends BaseDistributedSearchTestCase {

  @Test
  public void test() throws Exception {
    Random random = random();
    del("*:*");

    index(id, random.nextInt(), "b_t", "snake a,b spider shark snail slug seal", "foo_i_p", "1");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_i_p");
    del("*:*");

    // verify point field on empty index
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_i_p");

    index(id, random.nextInt(), "b_t", "snake a,b spider shark snail slug seal", "foo_i", "1");
    index(id, random.nextInt(), "b_t", "snake spider shark snail slug", "foo_i", "2", "foo_date_p", "2015-01-03T14:30:00Z");
    index(id, random.nextInt(), "b_t", "snake spider shark snail", "foo_i", "3");
    index(id, random.nextInt(), "b_t", "snake spider shark", "foo_i", "2", "foo_date_p", "2014-03-15T12:00:00Z");
    index(id, random.nextInt(), "b_t", "snake spider", "c_t", "snake spider", "foo_date_p", "2014-03-15T12:00:00Z");
    index(id, random.nextInt(), "b_t", "snake", "c_t", "snake", "foo_date_p", "2014-03-15T12:00:00Z");
    index(id, random.nextInt(), "b_t", "ant zebra", "c_t", "ant zebra", "foo_date_p", "2015-01-03T14:30:00Z");
    index(id, random.nextInt(), "b_t", "zebra", "c_t", "zebra", "foo_date_p", "2015-01-03T14:30:00Z");
    commit();

    handle.clear();
    handle.put("terms", UNORDERED);

    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.lower", "s");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "sn", "terms.lower", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn");
    // terms.sort
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.sort", "index");
    // terms.list
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.list", "snake,zebra,ant,bad");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_i", "terms.list", "2,3,1");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_i", "terms.stats", "true","terms.list", "2,3,1");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.list", "snake,zebra", "terms.ttf", "true");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.fl", "c_t", "terms.list", "snake,ant,zebra", "terms.ttf", "true");

    // for date point field
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_date_p");
    // terms.ttf=true doesn't work for point fields
    //query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "foo_date_p", "terms.ttf", "true");
  }
  
  protected QueryResponse query(Object... q) throws Exception {
    if (Stream.of(q).noneMatch(s->s.equals("terms.list"))) { 
      // SOLR-9243 doesn't support max/min count
      for (int i = 0; i < q.length; i+=2) {
        if (q[i].equals("terms.sort") && q[i+1].equals("index") || rarely()) {
          List<Object> params = new ArrayList<Object>(Arrays.asList(q));
          if (usually()) {
            params.add("terms.mincount");
            params.add(random().nextInt(4)-1);
          }
          if (usually()) {
            params.add("terms.maxcount");
            params.add(random().nextInt(4)-1);
          }
          q = params.toArray(new Object[params.size()]);
          break;
        }
      }
    }
    return super.query(q);
  }
}
