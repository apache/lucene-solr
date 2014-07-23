package org.apache.solr;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

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
@Slow
@LuceneTestCase.SuppressCodecs({"Lucene40", "Lucene41", "Lucene42", "Lucene43"})
public class DistributedIntervalFacetingTest extends
    BaseDistributedSearchTestCase {

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema-distrib-interval-faceting.xml";
    configString = "solrconfig-basic.xml";
  }

  @Override
  public void doTest() throws Exception {
    del("*:*");
    commit();
    testRandom();
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
    params.set("facet.interval", field);
    // number of intervals
    for (int i = 0; i < 1 + random().nextInt(20); i++) {
      Integer[] interval = getRandomRange(cardinality, field);
      String open = startOptions[interval[0] % 2];
      String close = endOptions[interval[1] % 2];
      params.add("f." + field + ".facet.interval.set", open + interval[0] + "," + interval[1] + close);
    }
    query(params);

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
      Arrays.sort(values, new Comparator<Integer>() {

        @Override
        public int compare(Integer o1, Integer o2) {
          return String.valueOf(o1).compareTo(String.valueOf(o2));
        }
      });
    } else {
      Arrays.sort(values);
    }
    return values;
  }
}
