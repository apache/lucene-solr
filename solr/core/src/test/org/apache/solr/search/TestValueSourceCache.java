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
package org.apache.solr.search;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// NOTE: this is a direct result of SOLR-2829
public class TestValueSourceCache extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    _func = QParser.getParser(null, FunctionQParserPlugin.NAME, lrf.makeRequest());
  }

  static QParser _func;
  
  @AfterClass
  public static void afterClass() throws Exception {
    _func = null;
  }

  Query getQuery(String query) throws SyntaxError {
    _func.setString(query);
    return _func.parse();
  }

  // This is actually also tested by the tests for val_d1 below, but the bug was reported against geodist()...
  @Test
  public void testGeodistSource() throws SyntaxError {
    Query q_home = getQuery("geodist(home_ll, 45.0, 43.0)");
    Query q_work = getQuery("geodist(work_ll, 45.0, 43.0)");
    Query q_home2 = getQuery("geodist(home_ll, 45.0, 43.0)");
    QueryUtils.checkUnequal(q_work, q_home);
    QueryUtils.checkEqual(q_home, q_home2);
  }

  @Test
  public void testNumerics() throws SyntaxError {
    String[] templates = new String[]{
        "sum(#v0, #n0)",
        "product(pow(#v0,#n0),#v1,#n1)",
        "log(#v0)",
        "log(sum(#n0,#v0,#v1,#n1))",
        "scale(map(#v0,#n0,#n1,#n2),#n3,#n4)",
    };
    String[] numbers = new String[]{
        "1,2,3,4,5",
        "1.0,2.0,3.0,4.0,5.0",
        "1,2.0,3,4.0,5",
        "1.0,2,3.0,4,5.0",
        "1000000,2000000,3000000,4000000,5000000"
    };
    String[] types = new String[]{
        "val1_f1",
        "val1_d1",
        "val1_b1",
        "val1_i1",
        "val1_l1",
        "val1_b1",
    };
    for (String template : templates) {
      for (String nums : numbers) {
        for (String type : types) {
          tryQuerySameTypes(template, nums, type);
          tryQueryDiffTypes(template, nums, types);
        }
      }
    }
  }

  // This test should will fail because q1 and q3 evaluate as equal unless
  // fixes for bug 2829 are in place.
  void tryQuerySameTypes(String template, String numbers, String type) throws SyntaxError {
    String s1 = template;
    String s2 = template;
    String s3 = template;

    String[] numParts = numbers.split(",");
    String type2 = type.replace("val1", "val2");
    for (int idx = 0; s1.contains("#"); ++idx) {
      String patV = "#v" + Integer.toString(idx);
      String patN = "#n" + Integer.toString(idx);
      s1 = s1.replace(patV, type).replace(patN, numParts[idx]);
      s2 = s2.replace(patV, type).replace(patN, numParts[idx]);
      s3 = s3.replace(patV, type2).replace(patN, numParts[idx]);
    }

    //SolrQueryRequest req1 = req( "q","*:*", "fq", s1);

    Query q1 = getQuery(s1);
    Query q2 = getQuery(s2);
    Query q3 = getQuery(s3);
    QueryUtils.checkEqual(q1, q2);
    QueryUtils.checkUnequal(q1, q3);
  }

  // These should always and forever fail, and would have failed without the fixes for 2829, but why not make
  // some more tests just in case???
  void tryQueryDiffTypes(String template, String numbers, String[] types) throws SyntaxError {
    String s1 = template;
    String s2 = template;

    String[] numParts = numbers.split(",");
    for (int idx = 0; s1.contains("#"); ++idx) {
      String patV = "#v" + Integer.toString(idx);
      String patN = "#n" + Integer.toString(idx);
      s1 = s1.replace(patV, types[idx % types.length]).replace(patN, numParts[idx]);
      s2 = s2.replace(patV, types[(idx + 1) % types.length]).replace(patN, numParts[idx]);
    }
    Query q1 = getQuery(s1);
    Query q2 = getQuery(s2);
    QueryUtils.checkUnequal(q1, q2);
  }
}
