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
package org.apache.solr.analytics.legacy.facet;


import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyQueryFacetTest extends LegacyAbstractAnalyticsFacetTest {
  static String fileName = "queryFacets.txt";

  public final int INT = 71;
  public final int LONG = 36;
  public final int FLOAT = 93;
  public final int DOUBLE = 49;
  public final int DATE = 12;
  public final int STRING = 28;
  public final int NUM_LOOPS = 100;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queryTest() throws Exception {
    h.update("<delete><query>*:*</query></delete>");
    //INT
    ArrayList<ArrayList<Integer>> int1TestStart = new ArrayList<>();
    int1TestStart.add(new ArrayList<Integer>());
    ArrayList<ArrayList<Integer>> int2TestStart = new ArrayList<>();
    int2TestStart.add(new ArrayList<Integer>());

    //LONG
    ArrayList<ArrayList<Long>> longTestStart = new ArrayList<>();
    longTestStart.add(new ArrayList<Long>());
    longTestStart.add(new ArrayList<Long>());

    //FLOAT
    ArrayList<ArrayList<Float>> floatTestStart = new ArrayList<>();
    floatTestStart.add(new ArrayList<Float>());
    floatTestStart.add(new ArrayList<Float>());
    floatTestStart.add(new ArrayList<Float>());

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      int dt = j%DATE;
      int s = j%STRING;
      assertU(adoc("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f,
          "double_dd", "" + d,  "date_dtd", (1800+dt) + "-12-31T23:59:59.999Z", "string_sd", "abc" + Integer.toString(s).charAt(0)));

      if (f<=50) {
        int1TestStart.get(0).add(i);
      }
      if (f<=30) {
        int2TestStart.get(0).add(i);
      }
      if (Integer.toString(s).charAt(0)=='1') {
        longTestStart.get(0).add(l);
      }
      if (Integer.toString(s).charAt(0)=='2') {
        longTestStart.get(1).add(l);
      }
      if (l>=30) {
        floatTestStart.get(0).add(f);
      }
      if (d<=50) {
        floatTestStart.get(1).add(f);
      }
      if (l>=20) {
        floatTestStart.get(2).add(f);
      }

      if (usually()) {
        assertU(commit()); // to have several segments
      }
    }

    assertU(commit());

    //Query ascending tests
    setResponse(h.query(request(fileToStringArr(LegacyQueryFacetTest.class, fileName))));

    //Int One
    ArrayList<Double> int1 = getDoubleList("ir", "queryFacets", "float1", "double", "sum");
    ArrayList<Double> int1Test = calculateNumberStat(int1TestStart, "sum");
    assertEquals(getRawResponse(), int1, int1Test);
    //Int Two
    ArrayList<Integer> int2 = getIntegerList("ir", "queryFacets", "float2", "int", "percentile_8");
    ArrayList<Integer> int2Test = (ArrayList<Integer>)calculateStat(int2TestStart, "perc_8");
    assertEquals(getRawResponse(), int2, int2Test);

    //Long
    ArrayList<Double> long1 = getDoubleList("lr", "queryFacets", "string", "double", "median");
    ArrayList<Double> long1Test = calculateNumberStat(longTestStart, "median");
    assertEquals(getRawResponse(),long1,long1Test);

    //Float
    ArrayList<Double> float1 = getDoubleList("fr", "queryFacets", "lad", "double", "mean");
    ArrayList<Double> float1Test = calculateNumberStat(floatTestStart, "mean");
    assertEquals(getRawResponse(), float1, float1Test);
  }

}
