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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyFieldFacetExtrasCloudTest extends LegacyAbstractAnalyticsFacetCloudTest {
  public static final int INT = 21;
  public static final int LONG = 22;
  public static final int FLOAT = 23;
  public static final int DOUBLE = 24;
  public static final int DATE = 25;
  public static final int STRING = 26;
  public static final int NUM_LOOPS = 100;

  //INT
  static ArrayList<ArrayList<Integer>> intLongTestStart;
  static ArrayList<ArrayList<Integer>> intFloatTestStart;
  static ArrayList<ArrayList<Integer>> intDoubleTestStart;
  static ArrayList<ArrayList<Integer>> intStringTestStart;

  @BeforeClass
  public static void beforeTest() throws Exception {

    //INT
    intLongTestStart = new ArrayList<>();
    intFloatTestStart = new ArrayList<>();
    intDoubleTestStart = new ArrayList<>();
    intStringTestStart = new ArrayList<>();

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
      fields.add("date_dtd"); fields.add((1800+dt) + "-12-31T23:59:59.999Z");
      fields.add("string_sd"); fields.add("abc" + s);
      req.add(fields.toArray(new String[0]));

      //Long
      if (j-LONG<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intLongTestStart.add(list1);
      } else {
        intLongTestStart.get((int)l).add(i);
      }
      //String
      if (j-FLOAT<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intFloatTestStart.add(list1);
      } else {
        intFloatTestStart.get((int)f).add(i);
      }
      //String
      if (j-DOUBLE<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intDoubleTestStart.add(list1);
      } else {
        intDoubleTestStart.get((int)d).add(i);
      }
      //String
      if (j-STRING<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intStringTestStart.add(list1);
      } else {
        intStringTestStart.get(s).add(i);
      }
    }

    req.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void limitTest() throws Exception {
    String[] params = new String[] {
        "o.lr.s.mean", "mean(int_id)",
        "o.lr.s.median", "median(int_id)",
        "o.lr.s.count", "count(int_id)",
        "o.lr.s.percentile_20", "percentile(20,int_id)",
        "o.lr.ff.long_ld", "long_ld",
        "o.lr.ff.long_ld.ss", "mean",
        "o.lr.ff.long_ld.sd", "asc",
        "o.lr.ff.long_ld.limit", "5",
        "o.lr.ff.float_fd", "float_fd",
        "o.lr.ff.float_fd.ss", "median",
        "o.lr.ff.float_fd.sd", "desc",
        "o.lr.ff.float_fd.limit", "3",
        "o.lr.ff.double_dd", "double_dd",
        "o.lr.ff.double_dd.ss", "count",
        "o.lr.ff.double_dd.sd", "asc",
        "o.lr.ff.double_dd.limit", "7",
        "o.lr.ff.string_sd", "string_sd",
        "o.lr.ff.string_sd.ss", "percentile_20",
        "o.lr.ff.string_sd.sd", "desc",
        "o.lr.ff.string_sd.limit", "1"
    };

    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    Collection<Double> lon = getValueList(response, "lr", "fieldFacets", "long_ld", "mean", false);
    assertEquals(responseStr, lon.size(),5);
    Collection<Double> flo = getValueList(response, "lr", "fieldFacets", "float_fd", "median", false);
    assertEquals(responseStr, flo.size(),3);
    Collection<Long> doub = getValueList(response, "lr", "fieldFacets", "double_dd", "count", false);
    assertEquals(responseStr, doub.size(),7);
    Collection<Integer> string = getValueList(response, "lr", "fieldFacets", "string_sd", "percentile_20", false);
    assertEquals(responseStr, string.size(),1);
  }

  @Test
  public void offsetTest() throws Exception {
    String[] params = new String[] {
        "o.offAll.s.mean", "mean(int_id)",
        "o.offAll.ff", "long_ld",
        "o.offAll.ff.long_ld.ss", "mean",
        "o.offAll.ff.long_ld.sd", "asc",
        "o.offAll.ff.long_ld.limit", "7",

        "o.off0.s.mean", "mean(int_id)",
        "o.off0.ff", "long_ld",
        "o.off0.ff.long_ld.ss", "mean",
        "o.off0.ff.long_ld.sd", "asc",
        "o.off0.ff.long_ld.limit", "2",
        "o.off0.ff.long_ld.offset", "0",

        "o.off1.s.mean", "mean(int_id)",
        "o.off1.ff", "long_ld",
        "o.off1.ff.long_ld.ss", "mean",
        "o.off1.ff.long_ld.sd", "asc",
        "o.off1.ff.long_ld.limit", "2",
        "o.off1.ff.long_ld.offset", "2",

        "o.off2.s.mean", "mean(int_id)",
        "o.off2.ff", "long_ld",
        "o.off2.ff.long_ld.ss", "mean",
        "o.off2.ff.long_ld.sd", "asc",
        "o.off2.ff.long_ld.limit", "3",
        "o.off2.ff.long_ld.offset", "4"
    };

    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    Collection<Double> lon;

    List<Double> all = new ArrayList<>();
    lon = getValueList(response, "off0", "fieldFacets", "long_ld", "mean", false);
    assertEquals(responseStr, lon.size(),2);
    assertArrayEquals(new Double[]{ 1.5,  2.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);

    lon = getValueList(response, "off1", "fieldFacets", "long_ld", "mean", false);
    assertEquals(responseStr, lon.size(),2);
    assertArrayEquals(new Double[]{ 3.0,  4.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);

    lon = getValueList(response, "off2", "fieldFacets", "long_ld", "mean", false);
    assertEquals(responseStr, lon.size(),3);
    assertArrayEquals(new Double[]{ 5.0,  5.75, 6.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);

    lon = getValueList(response, "offAll", "fieldFacets", "long_ld", "mean", false);
    assertEquals(responseStr, lon.size(),7);
    assertArrayEquals(all.toArray(new Double[0]), lon.toArray(new Double[0]));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sortTest() throws Exception {
    String[] params = new String[] {
        "o.sr.s.mean", "mean(int_id)",
        "o.sr.s.median", "median(int_id)",
        "o.sr.s.count", "count(int_id)",
        "o.sr.s.percentile_20", "percentile(20,int_id)",
        "o.sr.ff", "long_ld",
        "o.sr.ff.long_ld.ss", "mean",
        "o.sr.ff.long_ld.sd", "asc",
        "o.sr.ff", "float_fd",
        "o.sr.ff.float_fd.ss", "median",
        "o.sr.ff.float_fd.sd", "desc",
        "o.sr.ff", "double_dd",
        "o.sr.ff.double_dd.ss", "count",
        "o.sr.ff.double_dd.sd", "asc",
        "o.sr.ff", "string_sd",
        "o.sr.ff.string_sd.ss", "percentile_20",
        "o.sr.ff.string_sd.sd", "desc"
    };

    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    Collection<Double> lon = getValueList(response, "sr", "fieldFacets", "long_ld", "mean", false);
    ArrayList<Double> longTest = calculateFacetedNumberStat(intLongTestStart, "mean");
    Collections.sort(longTest);
    assertEquals(responseStr, longTest,lon);

    Collection<Double> flo = getValueList(response, "sr", "fieldFacets", "float_fd", "median", false);
    ArrayList<Double> floatTest = calculateFacetedNumberStat(intFloatTestStart, "median");
    Collections.sort(floatTest,Collections.reverseOrder());
    assertEquals(responseStr, floatTest,flo);

    Collection<Long> doub = getValueList(response, "sr", "fieldFacets", "double_dd", "count", false);
    ArrayList<Long> doubleTest = (ArrayList<Long>)calculateFacetedStat(intDoubleTestStart, "count");
    Collections.sort(doubleTest);
    assertEquals(responseStr, doubleTest,doub);

    Collection<Integer> string = getValueList(response, "sr", "fieldFacets", "string_sd", "percentile_20", false);
    ArrayList<Integer> stringTest = (ArrayList<Integer>)calculateFacetedStat(intStringTestStart, "perc_20");
    Collections.sort(stringTest,Collections.reverseOrder());
    assertEquals(responseStr, stringTest,string);
  }

}
