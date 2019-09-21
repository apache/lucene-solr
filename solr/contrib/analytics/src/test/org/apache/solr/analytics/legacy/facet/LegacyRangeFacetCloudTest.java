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
import org.junit.Before;
import org.junit.Test;


public class LegacyRangeFacetCloudTest extends LegacyAbstractAnalyticsFacetCloudTest{
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

  @Before
  public void beforeTest() throws Exception {

    //INT
    intLongTestStart = new ArrayList<>();
    intDoubleTestStart = new ArrayList<>();
    intDateTestStart = new ArrayList<>();

    //FLOAT
    floatLongTestStart = new ArrayList<>();
    floatDoubleTestStart = new ArrayList<>();
    floatDateTestStart = new ArrayList<>();

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
    }

    req.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void rangeTest() throws Exception {
    String[] params = new String[] {
        "o.ri.s.sum", "sum(int_id)",
        "o.ri.s.mean", "mean(int_id)",
        "o.ri.s.median", "median(int_id)",
        "o.ri.s.count", "count(int_id)",
        "o.ri.rf", "long_ld",
        "o.ri.rf.long_ld.st", "5",
        "o.ri.rf.long_ld.e", "30",
        "o.ri.rf.long_ld.g", "5",
        "o.ri.rf.long_ld.ib", "lower",
        "o.ri.rf.long_ld.or", "all",
        "o.ri.rf", "double_dd",
        "o.ri.rf.double_dd.st", "3",
        "o.ri.rf.double_dd.e", "39",
        "o.ri.rf.double_dd.g", "7",
        "o.ri.rf.double_dd.ib", "upper",
        "o.ri.rf.double_dd.ib", "outer",
        "o.ri.rf.double_dd.or", "all",
        "o.ri.rf", "date_dtd",
        "o.ri.rf.date_dtd.st", "1007-01-01T23:59:59Z",
        "o.ri.rf.date_dtd.e", "1044-01-01T23:59:59Z",
        "o.ri.rf.date_dtd.g", "+7YEARS",
        "o.ri.rf.date_dtd.ib", "lower",
        "o.ri.rf.date_dtd.ib", "edge",
        "o.ri.rf.date_dtd.ib", "outer",
        "o.ri.rf.date_dtd.or", "all",

        "o.rf.s.sum", "sum(float_fd)",
        "o.rf.s.mean", "mean(float_fd)",
        "o.rf.s.median", "median(float_fd)",
        "o.rf.s.count", "count(float_fd)",
        "o.rf.rf", "long_ld",
        "o.rf.rf.long_ld.st", "0",
        "o.rf.rf.long_ld.e", "29",
        "o.rf.rf.long_ld.g", "4",
        "o.rf.rf.long_ld.ib", "all",
        "o.rf.rf.long_ld.or", "all",
        "o.rf.rf", "double_dd",
        "o.rf.rf.double_dd.st", "4",
        "o.rf.rf.double_dd.e", "47",
        "o.rf.rf.double_dd.g", "11",
        "o.rf.rf.double_dd.ib", "edge",
        "o.rf.rf.double_dd.or", "all",
        "o.rf.rf", "date_dtd",
        "o.rf.rf.date_dtd.st", "1004-01-01T23:59:59Z",
        "o.rf.rf.date_dtd.e", "1046-01-01T23:59:59Z",
        "o.rf.rf.date_dtd.g", "+5YEARS",
        "o.rf.rf.date_dtd.ib", "upper",
        "o.rf.rf.date_dtd.ib", "edge",
        "o.rf.rf.date_dtd.or", "all"
    };

    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int Long
    ArrayList<Long> intLong = getValueList(response, "ri", "rangeFacets", "long_ld", "count", false);
    ArrayList<Long> intLongTest = calculateFacetedStat(transformLists(intLongTestStart, 5, 30, 5
                                                        , false, true, false, false, false), "count");
    assertEquals(responseStr, intLong,intLongTest);
    //Int Double
    ArrayList<Double> intDouble = getValueList(response, "ri", "rangeFacets", "double_dd", "mean", false);
    ArrayList<Double> intDoubleTest = calculateFacetedNumberStat(transformLists(intDoubleTestStart, 3, 39, 7
                                                          , false, false, true, false, true), "mean");
    assertEquals(responseStr, intDouble,intDoubleTest);
    //Int Date
    ArrayList<Long> intDate = getValueList(response, "ri", "rangeFacets", "date_dtd", "count", false);
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateFacetedStat(transformLists(intDateTestStart, 7, 44, 7
                                                      , false, true, false, true, true), "count");
    assertEquals(responseStr, intDate,intDateTest);

    //Float Long
    ArrayList<Double> floatLong = getValueList(response, "rf", "rangeFacets", "long_ld", "median", false);
    ArrayList<Double> floatLongTest = calculateFacetedNumberStat(transformLists(floatLongTestStart, 0, 29, 4
                                                          , false, true, true, true, true), "median");
    assertEquals(responseStr, floatLong,floatLongTest);
    //Float Double
    ArrayList<Long> floatDouble = getValueList(response, "rf", "rangeFacets", "double_dd", "count", false);
    ArrayList<Long> floatDoubleTest = (ArrayList<Long>)calculateFacetedStat(transformLists(floatDoubleTestStart, 4, 47, 11
                                                                     , false, false, false, true, false), "count");
    assertEquals(responseStr, floatDouble,floatDoubleTest);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void hardendRangeTest() throws Exception {
    String[] params = new String[] {
        "o.hi.s.sum", "sum(int_id)",
        "o.hi.s.mean", "mean(int_id)",
        "o.hi.s.median", "median(int_id)",
        "o.hi.s.count", "count(int_id)",
        "o.hi.rf", "long_ld",
        "o.hi.rf.long_ld.st", "5",
        "o.hi.rf.long_ld.e", "30",
        "o.hi.rf.long_ld.g", "5",
        "o.hi.rf.long_ld.he", "true",
        "o.hi.rf.long_ld.ib", "lower",
        "o.hi.rf.long_ld.or", "all",
        "o.hi.rf", "double_dd",
        "o.hi.rf.double_dd.st", "3",
        "o.hi.rf.double_dd.e", "39",
        "o.hi.rf.double_dd.g", "7",
        "o.hi.rf.double_dd.he", "true",
        "o.hi.rf.double_dd.ib", "upper",
        "o.hi.rf.double_dd.ib", "outer",
        "o.hi.rf.double_dd.or", "all",
        "o.hi.rf", "date_dtd",
        "o.hi.rf.date_dtd.st", "1007-01-01T23:59:59Z",
        "o.hi.rf.date_dtd.e", "1044-01-01T23:59:59Z",
        "o.hi.rf.date_dtd.g", "+7YEARS",
        "o.hi.rf.date_dtd.he", "true",
        "o.hi.rf.date_dtd.ib", "lower",
        "o.hi.rf.date_dtd.ib", "edge",
        "o.hi.rf.date_dtd.ib", "outer",
        "o.hi.rf.date_dtd.or", "all",

        "o.hf.s.sum", "sum(float_fd)",
        "o.hf.s.mean", "mean(float_fd)",
        "o.hf.s.median", "median(float_fd)",
        "o.hf.s.count", "count(float_fd)",
        "o.hf.rf", "long_ld",
        "o.hf.rf.long_ld.st", "0",
        "o.hf.rf.long_ld.e", "29",
        "o.hf.rf.long_ld.g", "4",
        "o.hf.rf.long_ld.he", "true",
        "o.hf.rf.long_ld.ib", "all",
        "o.hf.rf.long_ld.or", "all",
        "o.hf.rf", "double_dd",
        "o.hf.rf.double_dd.st", "4",
        "o.hf.rf.double_dd.e", "47",
        "o.hf.rf.double_dd.g", "11",
        "o.hf.rf.double_dd.he", "true",
        "o.hf.rf.double_dd.ib", "edge",
        "o.hf.rf.double_dd.or", "all",
        "o.hf.rf", "date_dtd",
        "o.hf.rf.date_dtd.st", "1004-01-01T23:59:59Z",
        "o.hf.rf.date_dtd.e", "1046-01-01T23:59:59Z",
        "o.hf.rf.date_dtd.g", "+5YEARS",
        "o.hf.rf.date_dtd.he", "true",
        "o.hf.rf.date_dtd.ib", "upper",
        "o.hf.rf.date_dtd.ib", "edge",
        "o.hf.rf.date_dtd.or", "all"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int Long
    ArrayList<Double> intLong = getValueList(response, "hi", "rangeFacets", "long_ld", "sum", false);
    ArrayList<Double> intLongTest = calculateFacetedNumberStat(transformLists(intLongTestStart, 5, 30, 5
                                                        , true, true, false, false, false), "sum");
    assertEquals(responseStr, intLong,intLongTest);
    //Int Double
    ArrayList<Double> intDouble = getValueList(response, "hi", "rangeFacets", "double_dd", "mean", false);
    ArrayList<Double> intDoubleTest = calculateFacetedNumberStat(transformLists(intDoubleTestStart, 3, 39, 7
                                                          , true, false, true, false, true), "mean");
    assertEquals(responseStr, intDouble,intDoubleTest);
    //Int Date
    ArrayList<Long> intDate = getValueList(response, "hi", "rangeFacets", "date_dtd", "count", false);
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateFacetedStat(transformLists(intDateTestStart, 7, 44, 7
                                                      , true, true, false, true, true), "count");
    assertEquals(responseStr, intDate,intDateTest);

    //Float Long
    ArrayList<Double> floatLong = getValueList(response, "hf", "rangeFacets", "long_ld", "median", false);
    ArrayList<Double> floatLongTest = calculateFacetedNumberStat(transformLists(floatLongTestStart, 0, 29, 4
                                                          , true, true, true, true, true), "median");
    assertEquals(responseStr, floatLong,floatLongTest);
    //Float Double
    ArrayList<Long> floatDouble = getValueList(response, "hf", "rangeFacets", "double_dd", "count", false);
    ArrayList<Long> floatDoubleTest = (ArrayList<Long>)calculateFacetedStat(transformLists(floatDoubleTestStart, 4, 47, 11
                                                                     , true, false, false, true, false), "count");
    assertEquals(responseStr, floatDouble,floatDoubleTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiGapTest() throws Exception {
    String[] params = new String[] {
        "o.mi.s.sum", "sum(int_id)",
        "o.mi.s.mean", "mean(int_id)",
        "o.mi.s.median", "median(int_id)",
        "o.mi.s.count", "count(int_id)",
        "o.mi.rf", "long_ld",
        "o.mi.rf.long_ld.st", "5",
        "o.mi.rf.long_ld.e", "30",
        "o.mi.rf.long_ld.g", "4,2,6,3",
        "o.mi.rf.long_ld.ib", "lower",
        "o.mi.rf.long_ld.or", "all",
        "o.mi.rf", "double_dd",
        "o.mi.rf.double_dd.st", "3",
        "o.mi.rf.double_dd.e", "39",
        "o.mi.rf.double_dd.g", "3,1,7",
        "o.mi.rf.double_dd.ib", "upper",
        "o.mi.rf.double_dd.ib", "outer",
        "o.mi.rf.double_dd.or", "all",
        "o.mi.rf", "date_dtd",
        "o.mi.rf.date_dtd.st", "1007-01-01T23:59:59Z",
        "o.mi.rf.date_dtd.e", "1044-01-01T23:59:59Z",
        "o.mi.rf.date_dtd.g", "+2YEARS,+7YEARS",
        "o.mi.rf.date_dtd.ib", "lower",
        "o.mi.rf.date_dtd.ib", "edge",
        "o.mi.rf.date_dtd.ib", "outer",
        "o.mi.rf.date_dtd.or", "all",

        "o.mf.s.sum", "sum(float_fd)",
        "o.mf.s.mean", "mean(float_fd)",
        "o.mf.s.median", "median(float_fd)",
        "o.mf.s.count", "count(float_fd)",
        "o.mf.rf", "long_ld",
        "o.mf.rf.long_ld.st", "0",
        "o.mf.rf.long_ld.e", "29",
        "o.mf.rf.long_ld.g", "1,4",
        "o.mf.rf.long_ld.ib", "all",
        "o.mf.rf.long_ld.or", "all",
        "o.mf.rf", "double_dd",
        "o.mf.rf.double_dd.st", "4",
        "o.mf.rf.double_dd.e", "47",
        "o.mf.rf.double_dd.g", "2,3,11",
        "o.mf.rf.double_dd.ib", "edge",
        "o.mf.rf.double_dd.or", "all",
        "o.mf.rf", "date_dtd",
        "o.mf.rf.date_dtd.st", "1004-01-01T23:59:59Z",
        "o.mf.rf.date_dtd.e", "1046-01-01T23:59:59Z",
        "o.mf.rf.date_dtd.g", "+4YEARS,+5YEARS",
        "o.mf.rf.date_dtd.ib", "upper",
        "o.mf.rf.date_dtd.ib", "edge",
        "o.mf.rf.date_dtd.or", "all"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int Long
    ArrayList<Double> intLong = getValueList(response, "mi", "rangeFacets", "long_ld", "sum", false);
    ArrayList<Double> intLongTest = calculateFacetedNumberStat(transformLists(intLongTestStart, 5, 30, "4,2,6,3"
                                                        , false, true, false, false, false), "sum");
    assertEquals(responseStr, intLong,intLongTest);
    //Int Double
    ArrayList<Double> intDouble = getValueList(response, "mi", "rangeFacets", "double_dd", "mean", false);
    ArrayList<Double> intDoubleTest = calculateFacetedNumberStat(transformLists(intDoubleTestStart, 3, 39, "3,1,7"
                                                          , false, false, true, false, true), "mean");
    assertEquals(responseStr, intDouble,intDoubleTest);
    //Int Date
    ArrayList<Long> intDate = getValueList(response, "mi", "rangeFacets", "date_dtd", "count", false);
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateFacetedStat(transformLists(intDateTestStart, 7, 44, "2,7"
                                                      , false, true, false, true, true), "count");
    assertEquals(responseStr, intDate,intDateTest);

    //Float Long
    ArrayList<Double> floatLong = getValueList(response, "mf", "rangeFacets", "long_ld", "median", false);
    ArrayList<Double> floatLongTest = calculateFacetedNumberStat(transformLists(floatLongTestStart, 0, 29, "1,4"
                                                          , false, true, true, true, true), "median");;
    assertEquals(responseStr, floatLong,floatLongTest);
    //Float Double
    ArrayList<Long> floatDouble = getValueList(response, "mf", "rangeFacets", "double_dd", "count", false);
    ArrayList<Long> floatDoubleTest = (ArrayList<Long>)calculateFacetedStat(transformLists(floatDoubleTestStart, 4, 47, "2,3,11"
                                                          , false, false, false, true, false), "count");
    assertEquals(responseStr, floatDouble,floatDoubleTest);
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
