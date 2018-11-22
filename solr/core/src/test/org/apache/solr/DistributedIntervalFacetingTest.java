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
package org.apache.solr;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.IntervalFacet.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SuppressSSL(bugUrl="https://issues.apache.org/jira/browse/SOLR-9182 - causes OOM")
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class DistributedIntervalFacetingTest extends
    BaseDistributedSearchTestCase {

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema-distrib-interval-faceting.xml";
    configString = "solrconfig-basic.xml";
  }

  @Test
  public void test() throws Exception {
    del("*:*");
    commit();
    testRandom();
    del("*:*");
    commit();
    testSolrJ();
  }

  private void testSolrJ() throws Exception {
    indexr("id", "0", "test_i_dv", "0", "test_s_dv", "AAA");
    indexr("id", "1", "test_i_dv", "1", "test_s_dv", "BBB");
    indexr("id", "2", "test_i_dv", "2", "test_s_dv", "AAA");
    indexr("id", "3", "test_i_dv", "3", "test_s_dv", "CCC");
    commit();
    
    QueryResponse response = controlClient.query(new SolrQuery("*:*"));
    assertEquals(4, response.getResults().getNumFound());
    
    SolrQuery q = new SolrQuery("*:*");
    String[] intervals =  new String[]{"[0,1)","[1,2)", "[2,3)", "[3,*)"};
    q.addIntervalFacets("test_i_dv", intervals);
    response = controlClient.query(q);
    assertEquals(1, response.getIntervalFacets().size());
    assertEquals("test_i_dv", response.getIntervalFacets().get(0).getField());
    assertEquals(4, response.getIntervalFacets().get(0).getIntervals().size());
    for (int i = 0; i < response.getIntervalFacets().get(0).getIntervals().size(); i++) {
      Count count = response.getIntervalFacets().get(0).getIntervals().get(i);
      assertEquals(intervals[i], count.getKey());
      assertEquals(1, count.getCount());
    }
    
    q = new SolrQuery("*:*");
    q.addIntervalFacets("test_i_dv", intervals);
    q.addIntervalFacets("test_s_dv", new String[]{"{!key='AAA'}[AAA,AAA]", "{!key='BBB'}[BBB,BBB]", "{!key='CCC'}[CCC,CCC]"});
    response = controlClient.query(q);
    assertEquals(2, response.getIntervalFacets().size());
    
    int stringIntervalIndex = "test_s_dv".equals(response.getIntervalFacets().get(0).getField())?0:1;
        
    assertEquals("test_i_dv", response.getIntervalFacets().get(1-stringIntervalIndex).getField());
    assertEquals("test_s_dv", response.getIntervalFacets().get(stringIntervalIndex).getField());
    
    for (int i = 0; i < response.getIntervalFacets().get(1-stringIntervalIndex).getIntervals().size(); i++) {
      Count count = response.getIntervalFacets().get(1-stringIntervalIndex).getIntervals().get(i);
      assertEquals(intervals[i], count.getKey());
      assertEquals(1, count.getCount());
    }
    
    List<Count> stringIntervals = response.getIntervalFacets().get(stringIntervalIndex).getIntervals();
    assertEquals(3, stringIntervals.size());
    assertEquals("AAA", stringIntervals.get(0).getKey());
    assertEquals(2, stringIntervals.get(0).getCount());
    
    assertEquals("BBB", stringIntervals.get(1).getKey());
    assertEquals(1, stringIntervals.get(1).getCount());
    
    assertEquals("CCC", stringIntervals.get(2).getKey());
    assertEquals(1, stringIntervals.get(2).getCount());
  }

  private void testRandom() throws Exception {
    // All field values will be a number between 0 and cardinality
    int cardinality = 1000000;
    // Fields to use for interval faceting
    String[] fields = new String[]{"test_s_dv", "test_i_dv", "test_l_dv", "test_f_dv", "test_d_dv",
        "test_ss_dv", "test_is_dv", "test_fs_dv", "test_ls_dv", "test_ds_dv"};
    for (int i = 0; i < atLeast(500); i++) {
      if (random().nextInt(50) == 0) {
        //have some empty docs
        indexr("id", String.valueOf(i));
        continue;
      }

      if (random().nextInt(100) == 0 && i > 0) {
        //delete some docs
        del("id:" + String.valueOf(i - 1));
      }
      Object[] docFields = new Object[(random().nextInt(5)) * 10 + 12];
      docFields[0] = "id";
      docFields[1] = String.valueOf(i);
      docFields[2] = "test_s_dv";
      docFields[3] = String.valueOf(random().nextInt(cardinality));
      docFields[4] = "test_i_dv";
      docFields[5] = String.valueOf(random().nextInt(cardinality));
      docFields[6] = "test_l_dv";
      docFields[7] = String.valueOf(random().nextInt(cardinality));
      docFields[8] = "test_f_dv";
      docFields[9] = String.valueOf(random().nextFloat() * cardinality);
      docFields[10] = "test_d_dv";
      docFields[11] = String.valueOf(random().nextDouble() * cardinality);
      for (int j = 12; j < docFields.length; ) {
        docFields[j++] = "test_ss_dv";
        docFields[j++] = String.valueOf(random().nextInt(cardinality));
        docFields[j++] = "test_is_dv";
        docFields[j++] = String.valueOf(random().nextInt(cardinality));
        docFields[j++] = "test_ls_dv";
        docFields[j++] = String.valueOf(random().nextInt(cardinality));
        docFields[j++] = "test_fs_dv";
        docFields[j++] = String.valueOf(random().nextFloat() * cardinality);
        docFields[j++] = "test_ds_dv";
        docFields[j++] = String.valueOf(random().nextDouble() * cardinality);
      }
      indexr(docFields);
      if (random().nextInt(50) == 0) {
        commit();
      }
    }
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);


    for (int i = 0; i < atLeast(100); i++) {
      doTestQuery(cardinality, fields);
    }

  }

  /**
   * Executes one query using interval faceting and compares with the same query using
   * facet query with the same range
   */
  private void doTestQuery(int cardinality, String[] fields) throws Exception {
    String[] startOptions = new String[]{"(", "["};
    String[] endOptions = new String[]{")", "]"};
    // the query should match some documents in most cases
    Integer[] qRange = getRandomRange(cardinality, "id");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "id:[" + qRange[0] + " TO " + qRange[1] + "]");
    params.set("facet", "true");
    params.set("rows", "0");
    String field = fields[random().nextInt(fields.length)]; //choose from any of the fields
    if (random().nextBoolean()) {
      params.set("facet.interval", field);
    } else  {
      params.set("facet.interval", getFieldWithKey(field));
    }
    // number of intervals
    for (int i = 0; i < 1 + random().nextInt(20); i++) {
      Integer[] interval = getRandomRange(cardinality, field);
      String open = startOptions[interval[0] % 2];
      String close = endOptions[interval[1] % 2];
      params.add("f." + field + ".facet.interval.set", open + interval[0] + "," + interval[1] + close);
    }
    query(params);

  }

  private String getFieldWithKey(String field) {
    return "{!key='_some_key_for_" + field + "_" + random().nextInt() + "'}" + field;
  }

  /**
   * Returns a random range. It's guaranteed that the first
   * number will be lower than the second, and both of them
   * between 0 (inclusive) and <code>max</code> (exclusive).
   * If the fieldName is "test_s_dv" or "test_ss_dv" (the
   * two fields used for Strings), the comparison will be done
   * alphabetically
   */
  private Integer[] getRandomRange(int max, String fieldName) {
    Integer[] values = new Integer[2];
    values[0] = random().nextInt(max);
    values[1] = random().nextInt(max);
    if ("test_s_dv".equals(fieldName) || "test_ss_dv".equals(fieldName)) {
      Arrays.sort(values, (o1, o2) -> String.valueOf(o1).compareTo(String.valueOf(o2)));
    } else {
      Arrays.sort(values);
    }
    return values;
  }
}
