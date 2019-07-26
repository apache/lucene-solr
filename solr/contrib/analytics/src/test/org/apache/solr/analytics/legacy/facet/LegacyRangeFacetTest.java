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


public class LegacyRangeFacetTest extends LegacyAbstractAnalyticsFacetTest {
  static String fileName = "rangeFacets.txt";

  public static final int INT = 71;
  public static final int LONG = 36;
  public static final int FLOAT = 93;
  public static final int DOUBLE = 48;
  public static final int DATE = 52;
  public static final int STRING = 28;
  public static final int NUM_LOOPS = 100;

  //INT
  static ArrayList<ArrayList<Integer>> intLongTestStart;
  static ArrayList<ArrayList<Integer>> intDoubleTestStart;
  static ArrayList<ArrayList<Integer>> intDateTestStart;

  //FLOAT
  static ArrayList<ArrayList<Float>> floatLongTestStart;
  static ArrayList<ArrayList<Float>> floatDoubleTestStart;
  static ArrayList<ArrayList<Float>> floatDateTestStart;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    //INT
    intLongTestStart = new ArrayList<>();
    intDoubleTestStart = new ArrayList<>();
    intDateTestStart = new ArrayList<>();

    //FLOAT
    floatLongTestStart = new ArrayList<>();
    floatDoubleTestStart = new ArrayList<>();
    floatDateTestStart = new ArrayList<>();

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      int dt = j%DATE;
      int s = j%STRING;
      assertU(adoc("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f,
          "double_dd", "" + d,  "date_dtd", (1000+dt) + "-01-01T23:59:59Z", "string_sd", "abc" + s));
      //Longs
      if (j-LONG<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intLongTestStart.add(list1);
        ArrayList<Float> list2 = new ArrayList<>();
        list2.add(f);
        floatLongTestStart.add(list2);
      } else {
        intLongTestStart.get((int)l).add(i);
        floatLongTestStart.get((int)l).add(f);
      }
      //Doubles
      if (j-DOUBLE<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intDoubleTestStart.add(list1);
        ArrayList<Float> list2 = new ArrayList<>();
        list2.add(f);
        floatDoubleTestStart.add(list2);
      } else {
        intDoubleTestStart.get((int)d).add(i);
        floatDoubleTestStart.get((int)d).add(f);
      }
      //Dates
      if (j-DATE<0) {
        ArrayList<Integer> list1 = new ArrayList<>();
        list1.add(i);
        intDateTestStart.add(list1);
        ArrayList<Float> list2 = new ArrayList<>();
        list2.add(f);
        floatDateTestStart.add(list2);
      } else {
        intDateTestStart.get(dt).add(i);
        floatDateTestStart.get(dt).add(f);
      }

      if (usually()) {
        assertU(commit());  // to have several segments
      }
    }

    assertU(commit());

    setResponse(h.query(request(fileToStringArr(LegacyRangeFacetTest.class, fileName))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void rangeTest() throws Exception {

    //Int Long
    ArrayList<Long> intLong = getLongList("ri", "rangeFacets", "long_ld", "long", "count");
    ArrayList<Long> intLongTest = calculateStat(transformLists(intLongTestStart, 5, 30, 5
                                                        , false, true, false, false, false), "count");
    assertEquals(getRawResponse(), intLong,intLongTest);
    //Int Double
    ArrayList<Double> intDouble = getDoubleList("ri", "rangeFacets", "double_dd", "double", "mean");
    ArrayList<Double> intDoubleTest = calculateNumberStat(transformLists(intDoubleTestStart, 3, 39, 7
                                                          , false, false, true, false, true), "mean");
    assertEquals(getRawResponse(), intDouble,intDoubleTest);
    //Int Date
    ArrayList<Long> intDate = getLongList("ri", "rangeFacets", "date_dtd", "long", "count");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(transformLists(intDateTestStart, 7, 44, 7
                                                      , false, true, false, true, true), "count");
    assertEquals(getRawResponse(), intDate,intDateTest);

    //Float Long
    ArrayList<Double> floatLong = getDoubleList("rf", "rangeFacets", "long_ld", "double", "median");
    ArrayList<Double> floatLongTest = calculateNumberStat(transformLists(floatLongTestStart, 0, 29, 4
                                                          , false, true, true, true, true), "median");
    assertEquals(getRawResponse(), floatLong,floatLongTest);
    //Float Double
    ArrayList<Long> floatDouble = getLongList("rf", "rangeFacets", "double_dd", "long", "count");
    ArrayList<Long> floatDoubleTest = (ArrayList<Long>)calculateStat(transformLists(floatDoubleTestStart, 4, 47, 11
                                                                     , false, false, false, true, false), "count");
    assertEquals(getRawResponse(), floatDouble,floatDoubleTest);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void hardendRangeTest() throws Exception {
    //Int Long
    ArrayList<Double> intLong = getDoubleList("hi", "rangeFacets", "long_ld", "double", "sum");
    ArrayList<Double> intLongTest = calculateNumberStat(transformLists(intLongTestStart, 5, 30, 5
                                                        , true, true, false, false, false), "sum");
    assertEquals(getRawResponse(), intLong,intLongTest);
    //Int Double
    ArrayList<Double> intDouble = getDoubleList("hi", "rangeFacets", "double_dd", "double", "mean");
    ArrayList<Double> intDoubleTest = calculateNumberStat(transformLists(intDoubleTestStart, 3, 39, 7
                                                          , true, false, true, false, true), "mean");
    assertEquals(getRawResponse(), intDouble,intDoubleTest);
    //Int Date
    ArrayList<Long> intDate = getLongList("hi", "rangeFacets", "date_dtd", "long", "count");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(transformLists(intDateTestStart, 7, 44, 7
                                                      , true, true, false, true, true), "count");
    assertEquals(getRawResponse(), intDate,intDateTest);

    //Float Long
    ArrayList<Double> floatLong = getDoubleList("hf", "rangeFacets", "long_ld", "double", "median");
    ArrayList<Double> floatLongTest = calculateNumberStat(transformLists(floatLongTestStart, 0, 29, 4
                                                          , true, true, true, true, true), "median");
    assertEquals(getRawResponse(), floatLong,floatLongTest);
    //Float Double
    ArrayList<Long> floatDouble = getLongList("hf", "rangeFacets", "double_dd", "long", "count");
    ArrayList<Long> floatDoubleTest = (ArrayList<Long>)calculateStat(transformLists(floatDoubleTestStart, 4, 47, 11
                                                                     , true, false, false, true, false), "count");
    assertEquals(getRawResponse(), floatDouble,floatDoubleTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiGapTest() throws Exception {
    //Int Long
    ArrayList<Double> intLong = getDoubleList("mi", "rangeFacets", "long_ld", "double", "sum");
    ArrayList<Double> intLongTest = calculateNumberStat(transformLists(intLongTestStart, 5, 30, "4,2,6,3"
                                                        , false, true, false, false, false), "sum");
    assertEquals(getRawResponse(), intLong,intLongTest);
    //Int Double
    ArrayList<Double> intDouble = getDoubleList("mi", "rangeFacets", "double_dd", "double", "mean");
    ArrayList<Double> intDoubleTest = calculateNumberStat(transformLists(intDoubleTestStart, 3, 39, "3,1,7"
                                                          , false, false, true, false, true), "mean");
    assertEquals(getRawResponse(), intDouble,intDoubleTest);
    //Int Date
    ArrayList<Long> intDate = getLongList("mi", "rangeFacets", "date_dtd", "long", "count");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(transformLists(intDateTestStart, 7, 44, "2,7"
                                                      , false, true, false, true, true), "count");
    assertEquals(getRawResponse(), intDate,intDateTest);

    //Float Long
    ArrayList<Double> floatLong = getDoubleList("mf", "rangeFacets", "long_ld", "double", "median");
    ArrayList<Double> floatLongTest = calculateNumberStat(transformLists(floatLongTestStart, 0, 29, "1,4"
                                                          , false, true, true, true, true), "median");;
    assertEquals(getRawResponse(), floatLong,floatLongTest);
    //Float Double
    ArrayList<Long> floatDouble = getLongList("mf", "rangeFacets", "double_dd", "long", "count");
    ArrayList<Long> floatDoubleTest = (ArrayList<Long>)calculateStat(transformLists(floatDoubleTestStart, 4, 47, "2,3,11"
                                                          , false, false, false, true, false), "count");
    assertEquals(getRawResponse(), floatDouble,floatDoubleTest);
  }

  private <T> ArrayList<ArrayList<T>> transformLists(ArrayList<ArrayList<T>> listsStart, int start, int end, int gap
      , boolean hardend, boolean incLow, boolean incUp, boolean incEdge, boolean incOut) {
    int off = (end-start)%gap;
    if (!hardend && off>0) {
      end+=gap-off;
    }

    ArrayList<ArrayList<T>> lists = new ArrayList<>();
    ArrayList<T> between = new ArrayList<>();
    if (incLow && incUp) {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        ArrayList<T> list = new ArrayList<>();
        for (int j = i; j<=i+gap && j<=end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start; i<listsStart.size() && i<=end; i++) {
        between.addAll(listsStart.get(i));
      }
    } else if (incLow && !incUp) {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        ArrayList<T> list = new ArrayList<>();
        for (int j = i; j<i+gap && j<end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start; i<listsStart.size() && i<end; i++) {
        between.addAll(listsStart.get(i));
      }
    } else if (!incLow && incUp) {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        ArrayList<T> list = new ArrayList<>();
        for (int j = i+1; j<=i+gap && j<=end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start+1; i<listsStart.size() && i<=end; i++) {
        between.addAll(listsStart.get(i));
      }
    } else {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        ArrayList<T> list = new ArrayList<>();
        for (int j = i+1; j<i+gap && j<end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start+1; i<listsStart.size() && i<end; i++) {
        between.addAll(listsStart.get(i));
      }
    }

    if (incEdge && !incLow && start>=0) {
      lists.get(0).addAll(listsStart.get(start));
      between.addAll(listsStart.get(start));
    }
    if (incEdge && !incUp && end<listsStart.size()) {
      lists.get(lists.size()-1).addAll(listsStart.get(end));
      between.addAll(listsStart.get(end));
    }
    ArrayList<T> before = new ArrayList<>();
    ArrayList<T> after = new ArrayList<>();
    if (incOut || !(incLow||incEdge)) {
      for (int i = 0; i<=start; i++) {
        before.addAll(listsStart.get(i));
      }
    } else {
      for (int i = 0; i<start; i++) {
        before.addAll(listsStart.get(i));
      }
    }
    if (incOut || !(incUp||incEdge)) {
      for (int i = end; i<listsStart.size(); i++) {
        after.addAll(listsStart.get(i));
      }
    }
    else {
      for (int i = end+1; i<listsStart.size(); i++) {
        after.addAll(listsStart.get(i));
      }
    }
    lists.add(before);
    lists.add(after);
    lists.add(between);
    return lists;
  }

  private <T> ArrayList<ArrayList<T>> transformLists(ArrayList<ArrayList<T>> listsStart, int start, int end, String gapString
      , boolean hardend, boolean incLow, boolean incUp, boolean incEdge, boolean incOut) {
    String[] stringGaps = gapString.split(",");
    int[] gaps = new int[stringGaps.length];
    for (int i = 0; i<gaps.length; i++) {
      gaps[i] = Integer.parseInt(stringGaps[i]);
    }
    int bigGap = 0;
    int last = gaps[gaps.length-1];
    for (int i = 0; i<gaps.length-1; i++) {
      bigGap += gaps[i];
    }
    int off = (end-start-bigGap)%last;
    if (!hardend && off>0) {
      end+=last-off;
    }

    ArrayList<ArrayList<T>> lists = new ArrayList<>();
    ArrayList<T> between = new ArrayList<>();
    int gap = 0;
    int gapCounter = 0;
    if (incLow && incUp) {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        if (gapCounter<gaps.length) {
          gap = gaps[gapCounter++];
        }
        ArrayList<T> list = new ArrayList<>();
        for (int j = i; j<=i+gap && j<=end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start; i<listsStart.size() && i<=end; i++) {
        between.addAll(listsStart.get(i));
      }
    } else if (incLow && !incUp) {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        if (gapCounter<gaps.length) {
          gap = gaps[gapCounter++];
        }
        ArrayList<T> list = new ArrayList<>();
        for (int j = i; j<i+gap && j<end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start; i<listsStart.size() && i<end; i++) {
        between.addAll(listsStart.get(i));
      }
    } else if (!incLow && incUp) {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        if (gapCounter<gaps.length) {
          gap = gaps[gapCounter++];
        }
        ArrayList<T> list = new ArrayList<>();
        for (int j = i+1; j<=i+gap && j<=end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start+1; i<listsStart.size() && i<=end; i++) {
        between.addAll(listsStart.get(i));
      }
    } else {
      for (int i = start; i<end && i<listsStart.size(); i+=gap) {
        if (gapCounter<gaps.length) {
          gap = gaps[gapCounter++];
        }
        ArrayList<T> list = new ArrayList<>();
        for (int j = i+1; j<i+gap && j<end && j<listsStart.size(); j++) {
          list.addAll(listsStart.get(j));
        }
        lists.add(list);
      }
      for (int i = start+1; i<listsStart.size() && i<end; i++) {
        between.addAll(listsStart.get(i));
      }
    }

    if (incEdge && !incLow && start>=0) {
      lists.get(0).addAll(listsStart.get(start));
      between.addAll(listsStart.get(start));
    }
    if (incEdge && !incUp && end<listsStart.size()) {
      lists.get(lists.size()-1).addAll(listsStart.get(end));
      between.addAll(listsStart.get(end));
    }
    ArrayList<T> before = new ArrayList<>();
    ArrayList<T> after = new ArrayList<>();
    if (incOut || !(incLow||incEdge)) {
      for (int i = 0; i<=start; i++) {
        before.addAll(listsStart.get(i));
      }
    } else {
      for (int i = 0; i<start; i++) {
        before.addAll(listsStart.get(i));
      }
    }
    if (incOut || !(incUp||incEdge)) {
      for (int i = end; i<listsStart.size(); i++) {
        after.addAll(listsStart.get(i));
      }
    }
    else {
      for (int i = end+1; i<listsStart.size(); i++) {
        after.addAll(listsStart.get(i));
      }
    }
    lists.add(before);
    lists.add(after);
    lists.add(between);
    return lists;
  }

}
