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

import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyFieldFacetExtrasTest extends LegacyAbstractAnalyticsFacetTest {
  static String fileName = "fieldFacetExtras.txt";

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
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    //INT
    intLongTestStart = new ArrayList<>();
    intFloatTestStart = new ArrayList<>();
    intDoubleTestStart = new ArrayList<>();
    intStringTestStart = new ArrayList<>();

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      int dt = j%DATE;
      int s = j%STRING;
      assertU(adoc("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f,
          "double_dd", "" + d,  "date_dtd", (1800+dt) + "-12-31T23:59:59.999Z", "string_sd", "abc" + s));
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

      if (usually()) {
        assertU(commit()); // to have several segments
      }
    }

    assertU(commit());
    setResponse(h.query(request(fileToStringArr(LegacyFieldFacetExtrasTest.class, fileName))));
  }

  @Test
  public void limitTest() throws Exception {

    Collection<Double> lon = getDoubleList("lr", "fieldFacets", "long_ld", "double", "mean");
    assertEquals(getRawResponse(), lon.size(),5);
    Collection<Double> flo = getDoubleList("lr", "fieldFacets", "float_fd", "double", "median");
    assertEquals(getRawResponse(), flo.size(),3);
    Collection<Long> doub = getLongList("lr", "fieldFacets", "double_dd", "long", "count");
    assertEquals(getRawResponse(), doub.size(),7);
    Collection<Integer> string = getIntegerList("lr", "fieldFacets", "string_sd", "int", "percentile_20");
    assertEquals(getRawResponse(), string.size(),1);
  }

  @Test
  public void offsetTest() throws Exception {

    Collection<Double> lon;

    List<Double> all = new ArrayList<>();
    lon = getDoubleList("off0", "fieldFacets", "long_ld", "double", "mean");
    assertEquals(getRawResponse(), lon.size(),2);
    assertArrayEquals(new Double[]{ 1.5,  2.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);

    lon = getDoubleList("off1", "fieldFacets", "long_ld", "double", "mean");
    assertEquals(getRawResponse(), lon.size(),2);
    assertArrayEquals(new Double[]{ 3.0,  4.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);

    lon = getDoubleList("off2", "fieldFacets", "long_ld", "double", "mean");
    assertEquals(getRawResponse(), lon.size(),3);
    assertArrayEquals(new Double[]{ 5.0,  5.75, 6.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);

    lon = getDoubleList("offAll", "fieldFacets", "long_ld", "double", "mean");
    assertEquals(getRawResponse(), lon.size(),7);
    assertArrayEquals(all.toArray(new Double[0]), lon.toArray(new Double[0]));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sortTest() throws Exception {
    Collection<Double> lon = getDoubleList("sr", "fieldFacets", "long_ld", "double", "mean");
    ArrayList<Double> longTest = calculateNumberStat(intLongTestStart, "mean");
    Collections.sort(longTest);
    assertEquals(getRawResponse(), longTest,lon);

    Collection<Double> flo = getDoubleList("sr", "fieldFacets", "float_fd", "double", "median");
    ArrayList<Double> floatTest = calculateNumberStat(intFloatTestStart, "median");
    Collections.sort(floatTest,Collections.reverseOrder());
    assertEquals(getRawResponse(), floatTest,flo);

    Collection<Long> doub = getLongList("sr", "fieldFacets", "double_dd", "long", "count");
    ArrayList<Long> doubleTest = (ArrayList<Long>)calculateStat(intDoubleTestStart, "count");
    Collections.sort(doubleTest);
    assertEquals(getRawResponse(), doubleTest,doub);

    Collection<Integer> string = getIntegerList("sr", "fieldFacets", "string_sd", "int", "percentile_20");
    ArrayList<Integer> stringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "perc_20");
    Collections.sort(stringTest,Collections.reverseOrder());
    assertEquals(getRawResponse(), stringTest,string);
  }

}
