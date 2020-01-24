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
import java.util.List;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LegacyQueryFacetCloudTest extends LegacyAbstractAnalyticsFacetCloudTest {
  private static final int INT = 71;
  private static final int LONG = 36;
  private static final int FLOAT = 93;
  private static final int DOUBLE = 49;
  private static final int DATE = 12;
  private static final int STRING = 7;
  private static final int NUM_LOOPS = 100;

  private static final ArrayList<ArrayList<Integer>> int1TestStart = new ArrayList<>();
  private static final ArrayList<ArrayList<Integer>> int2TestStart = new ArrayList<>();
  private static final ArrayList<ArrayList<Long>> longTestStart = new ArrayList<>();
  private static final ArrayList<ArrayList<Float>> floatTestStart = new ArrayList<>();

  @After
  public void afterTest() throws Exception {
    int1TestStart.clear();
    int2TestStart.clear();
    longTestStart.clear();
    floatTestStart.clear();
  }

  @Before
  public void beforeTest() throws Exception {

    //INT
    int1TestStart.add(new ArrayList<Integer>());
    int2TestStart.add(new ArrayList<Integer>());

    //LONG
    longTestStart.add(new ArrayList<Long>());
    longTestStart.add(new ArrayList<Long>());

    //FLOAT
    floatTestStart.add(new ArrayList<Float>());
    floatTestStart.add(new ArrayList<Float>());
    floatTestStart.add(new ArrayList<Float>());

    UpdateRequest req = new UpdateRequest();

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      int dt = j%DATE;
      int s = j%STRING;

      List<String> fields = new ArrayList<>();
      fields.add("id"); fields.add("1000"+j);
      fields.add("int_id"); fields.add("" + i);
      fields.add("long_ld"); fields.add("" + l);
      fields.add("float_fd"); fields.add("" + f);
      fields.add("double_dd"); fields.add("" + d);
      fields.add("date_dtd"); fields.add((1000+dt) + "-01-01T23:59:59Z");
      fields.add("string_sd"); fields.add("abc" + s);
      req.add(fields.toArray(new String[0]));

      if (f<=50) {
        int1TestStart.get(0).add(i);
      }
      if (f<=30) {
        int2TestStart.get(0).add(i);
      }
      if (s == 1) {
        longTestStart.get(0).add(l);
      }
      if (s == 2) {
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
    }

    req.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queryTest() throws Exception {
    String[] params = new String[] {
        "o.ir.s.sum", "sum(int_id)",
        "o.ir.s.mean", "mean(int_id)",
        "o.ir.s.median", "median(int_id)",
        "o.ir.s.percentile_8", "percentile(8,int_id)",
        "o.ir.qf", "float1",
        "o.ir.qf.float1.q", "float_fd:[* TO 50]",
        "o.ir.qf", "float2",
        "o.ir.qf.float2.q", "float_fd:[* TO 30]",

        "o.lr.s.sum", "sum(long_ld)",
        "o.lr.s.mean", "mean(long_ld)",
        "o.lr.s.median", "median(long_ld)",
        "o.lr.s.percentile_8", "percentile(8,long_ld)",
        "o.lr.qf", "string",
        "o.lr.qf.string.q", "string_sd:abc1",
        "o.lr.qf.string.q", "string_sd:abc2",

        "o.fr.s.sum", "sum(float_fd)",
        "o.fr.s.mean", "mean(float_fd)",
        "o.fr.s.median", "median(float_fd)",
        "o.fr.s.percentile_8", "percentile(8,float_fd)",
        "o.fr.qf", "lad",
        "o.fr.qf.lad.q", "long_ld:[20 TO *]",
        "o.fr.qf.lad.q", "long_ld:[30 TO *]",
        "o.fr.qf.lad.q", "double_dd:[* TO 50]"
    };

    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int One
    ArrayList<Double> int1 = getValueList(response, "ir", "queryFacets", "float1", "sum", false);
    ArrayList<Double> int1Test = calculateFacetedNumberStat(int1TestStart, "sum");
    assertEquals(responseStr, int1, int1Test);
    //Int Two
    ArrayList<Integer> int2 = getValueList(response, "ir", "queryFacets", "float2", "percentile_8", false);
    ArrayList<Integer> int2Test = (ArrayList<Integer>)calculateFacetedStat(int2TestStart, "perc_8");
    assertEquals(responseStr, int2, int2Test);

    //Long
    ArrayList<Double> long1 = getValueList(response, "lr", "queryFacets", "string", "median", false);
    ArrayList<Double> long1Test = calculateFacetedNumberStat(longTestStart, "median");
    assertEquals(responseStr,long1,long1Test);

    //Float
    ArrayList<Double> float1 = getValueList(response, "fr", "queryFacets", "lad", "mean", false);
    ArrayList<Double> float1Test = calculateFacetedNumberStat(floatTestStart, "mean");
    assertEquals(responseStr, float1, float1Test);
  }

}
