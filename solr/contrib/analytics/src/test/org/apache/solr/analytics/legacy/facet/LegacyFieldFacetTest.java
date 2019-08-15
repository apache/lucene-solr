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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Node;


public class LegacyFieldFacetTest extends LegacyAbstractAnalyticsFacetTest{
  static String fileName = "fieldFacets.txt";

  public static final int INT = 71;
  public static final int LONG = 36;
  public static final int LONGM = 50;
  public static final int FLOAT = 73;
  public static final int FLOATM = 84;
  public static final int DOUBLE = 49;
  public static final int DATE = 12;
  public static final int DATEM = 30;
  public static final int STRING = 28;
  public static final int STRINGM = 40;
  public static final int NUM_LOOPS = 100;

  //INT
  private static ArrayList<ArrayList<Integer>> intDateTestStart;
  private static ArrayList<Long> intDateTestMissing;
  private static ArrayList<ArrayList<Integer>> intStringTestStart;
  private static ArrayList<Long> intStringTestMissing;

  //LONG
  private static ArrayList<ArrayList<Long>> longDateTestStart;
  private static ArrayList<Long> longDateTestMissing;
  private static ArrayList<ArrayList<Long>> longStringTestStart;
  private static ArrayList<Long> longStringTestMissing;

  //FLOAT
  private static ArrayList<ArrayList<Float>> floatDateTestStart;
  private static ArrayList<Long> floatDateTestMissing;
  private static ArrayList<ArrayList<Float>> floatStringTestStart;
  private static ArrayList<Long> floatStringTestMissing;

  //DOUBLE
  private static ArrayList<ArrayList<Double>> doubleDateTestStart;
  private static ArrayList<Long> doubleDateTestMissing;
  private static ArrayList<ArrayList<Double>> doubleStringTestStart;
  private static ArrayList<Long> doubleStringTestMissing;

  //DATE
  private static ArrayList<ArrayList<String>> dateIntTestStart;
  private static ArrayList<Long> dateIntTestMissing;
  private static ArrayList<ArrayList<String>> dateLongTestStart;
  private static ArrayList<Long> dateLongTestMissing;

  //String
  private static ArrayList<ArrayList<String>> stringIntTestStart;
  private static ArrayList<Long> stringIntTestMissing;
  private static ArrayList<ArrayList<String>> stringLongTestStart;
  private static ArrayList<Long> stringLongTestMissing;

  //Multi-Valued
  private static ArrayList<ArrayList<Integer>> multiLongTestStart;
  private static ArrayList<Long> multiLongTestMissing;
  private static ArrayList<ArrayList<Integer>> multiStringTestStart;
  private static ArrayList<Long> multiStringTestMissing;
  private static ArrayList<ArrayList<Integer>> multiDateTestStart;
  private static ArrayList<Long> multiDateTestMissing;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    defaults.put("int", 0);
    defaults.put("long", 0L);
    defaults.put("float", (float) 0);
    defaults.put("double", (double) 0);
    defaults.put("date", "1800-12-31T23:59:59Z");
    defaults.put("string", "str0");

    //INT
    intDateTestStart = new ArrayList<>();
    intDateTestMissing = new ArrayList<>();
    intStringTestStart = new ArrayList<>();
    intStringTestMissing = new ArrayList<>();

    //LONG
    longDateTestStart = new ArrayList<>();
    longDateTestMissing = new ArrayList<>();
    longStringTestStart = new ArrayList<>();
    longStringTestMissing = new ArrayList<>();

    //FLOAT
    floatDateTestStart = new ArrayList<>();
    floatDateTestMissing = new ArrayList<>();
    floatStringTestStart = new ArrayList<>();
    floatStringTestMissing = new ArrayList<>();

    //DOUBLE
    doubleDateTestStart = new ArrayList<>();
    doubleDateTestMissing = new ArrayList<>();
    doubleStringTestStart = new ArrayList<>();
    doubleStringTestMissing = new ArrayList<>();

    //DATE
    dateIntTestStart = new ArrayList<>();
    dateIntTestMissing = new ArrayList<>();
    dateLongTestStart = new ArrayList<>();
    dateLongTestMissing = new ArrayList<>();

    //String
    stringIntTestStart = new ArrayList<>();
    stringIntTestMissing = new ArrayList<>();
    stringLongTestStart = new ArrayList<>();
    stringLongTestMissing = new ArrayList<>();

    //Multi-Valued
    multiLongTestStart = new ArrayList<>();
    multiLongTestMissing = new ArrayList<>();
    multiStringTestStart = new ArrayList<>();
    multiStringTestMissing = new ArrayList<>();
    multiDateTestStart = new ArrayList<>();
    multiDateTestMissing = new ArrayList<>();

    boolean multiCanHaveDuplicates = Boolean.getBoolean(NUMERIC_POINTS_SYSPROP);

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      long lm = j%LONGM;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      int dt = j%DATE;
      int dtm = j%DATEM;
      int s = j%STRING;
      int sm = j%STRINGM;


      List<String> fields = new ArrayList<>();
      fields.add("id"); fields.add("1000"+j);

      if( i != 0 ) {
        fields.add("int_id"); fields.add("" + i);
      }
      if( l != 0l ) {
        fields.add("long_ld"); fields.add("" + l);
        fields.add("long_ldm"); fields.add("" + l);
      }
      if( lm != 0l ) {
        fields.add("long_ldm"); fields.add("" + lm);
      }
      if( f != 0.0f ) {
        fields.add("float_fd"); fields.add("" + f);
      }
      if( d != 0.0d ) {
        fields.add("double_dd"); fields.add("" + d);
      }
      if( dt != 0 ) {
        fields.add("date_dtd"); fields.add((1800+dt) + "-12-31T23:59:59Z");
        fields.add("date_dtdm"); fields.add((1800+dt) + "-12-31T23:59:59Z");
      }
      if ( dtm != 0 ) {
        fields.add("date_dtdm"); fields.add((1800+dtm) + "-12-31T23:59:59Z");
      }
      if ( s != 0 ) {
        fields.add("string_sd"); fields.add("str" + s);
        fields.add("string_sdm"); fields.add("str" + s);
      }
      if ( sm != 0 ) {
        fields.add("string_sdm"); fields.add("str" + sm);
      }
      assertU(adoc(fields.toArray(new String[0])));

      if( dt != 0 ) {
        //Dates
        if ( j-DATE < 0 ) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if( i != 0 ) {
            list1.add(i);
            intDateTestMissing.add(0l);
          } else {
            intDateTestMissing.add(1l);
          }
          intDateTestStart.add(list1);
          ArrayList<Long> list2 = new ArrayList<>();
          if( l != 0l ) {
            list2.add(l);
            longDateTestMissing.add(0l);
          } else {
            longDateTestMissing.add(1l);
          }
          longDateTestStart.add(list2);
          ArrayList<Float> list3 = new ArrayList<>();
          if ( f != 0.0f ) {
            list3.add(f);
            floatDateTestMissing.add(0l);
          } else {
            floatDateTestMissing.add(1l);

          }
          floatDateTestStart.add(list3);
          ArrayList<Double> list4 = new ArrayList<>();
          if( d != 0.0d ) {
            list4.add(d);
            doubleDateTestMissing.add(0l);
          } else {
            doubleDateTestMissing.add(1l);
          }
          doubleDateTestStart.add(list4);
          ArrayList<Integer> list5 = new ArrayList<>();
          if( i != 0 ) {
            list5.add(i);
            multiDateTestMissing.add(0l);
          } else {
            multiDateTestMissing.add(1l);

          }
          multiDateTestStart.add(list5);
        } else {
          if( i != 0 ) intDateTestStart.get(dt-1).add(i); else increment(intDateTestMissing,dt-1);
          if( l != 0l ) longDateTestStart.get(dt-1).add(l); else increment(longDateTestMissing,dt-1);
          if( f != 0.0f ) floatDateTestStart.get(dt-1).add(f); else increment(floatDateTestMissing,dt-1);
          if( d != 0.0d ) doubleDateTestStart.get(dt-1).add(d); else increment(doubleDateTestMissing,dt-1);
          if( i != 0 ) multiDateTestStart.get(dt-1).add(i); else increment(multiDateTestMissing,dt-1);
        }
      }

      if ( dtm != 0 ) {
        if ( j-DATEM < 0 && dtm != dt ) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if( i != 0 ) {
            list1.add(i);
            multiDateTestMissing.add(0l);
          } else {
            multiDateTestMissing.add(1l);
          }
          multiDateTestStart.add(list1);
        } else if ( dtm != dt || multiCanHaveDuplicates ) {
          if( i != 0 ) multiDateTestStart.get(dtm-1).add(i); else increment(multiDateTestMissing,dtm-1);
        }
      }

      if( s != 0 ){
        //Strings
        if ( j-STRING < 0 ) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if( i != 0 ) {
            list1.add(i);
            intStringTestMissing.add(0l);
          } else {
            intStringTestMissing.add(1l);
          }
          intStringTestStart.add(list1);
          ArrayList<Long> list2 = new ArrayList<>();
          if( l != 0l ) {
            list2.add(l);
            longStringTestMissing.add(0l);
          } else {
            longStringTestMissing.add(1l);
          }
          longStringTestStart.add(list2);
          ArrayList<Float> list3 = new ArrayList<>();
          if( f != 0.0f ){
            list3.add(f);
            floatStringTestMissing.add(0l);
          } else {
            floatStringTestMissing.add(1l);
          }
          floatStringTestStart.add(list3);
          ArrayList<Double> list4 = new ArrayList<>();
          if( d != 0.0d ) {
            list4.add(d);
            doubleStringTestMissing.add(0l);
          } else {
            doubleStringTestMissing.add(1l);
          }
          doubleStringTestStart.add(list4);
          ArrayList<Integer> list5 = new ArrayList<>();
          if( i != 0 ) {
            list5.add(i);
            multiStringTestMissing.add(0l);
          } else {
            multiStringTestMissing.add(1l);
          }
          multiStringTestStart.add(list5);
        } else {
          if( i != 0 ) intStringTestStart.get(s-1).add(i); else increment(intStringTestMissing,s-1);
          if( l != 0l ) longStringTestStart.get(s-1).add(l); else increment(longStringTestMissing,s-1);
          if( f != 0.0f ) floatStringTestStart.get(s-1).add(f); else increment(floatStringTestMissing,s-1);
          if( d != 0.0d ) doubleStringTestStart.get(s-1).add(d); else increment(doubleStringTestMissing,s-1);
          if( i != 0 ) multiStringTestStart.get(s-1).add(i); else increment(multiStringTestMissing,s-1);
        }
      }

      //Strings
      if( sm != 0 ){
        if ( j-STRINGM < 0 && sm != s ) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if( i != 0 ){
            list1.add(i);
            multiStringTestMissing.add(0l);
          } else {
            multiStringTestMissing.add(1l);
          }
          multiStringTestStart.add(list1);
        } else if ( sm != s ) {
          if( i != 0 ) multiStringTestStart.get(sm-1).add(i); else increment(multiStringTestMissing,sm-1);
        }
      }

      //Int
      if( i != 0 ) {
        if ( j-INT < 0 ) {
          ArrayList<String> list1 = new ArrayList<>();
          if( dt != 0 ){
            list1.add((1800+dt) + "-12-31T23:59:59Z");
            dateIntTestMissing.add(0l);
          } else {
            dateIntTestMissing.add(1l);
          }
          dateIntTestStart.add(list1);
          ArrayList<String> list2 = new ArrayList<>();
          if( s != 0 ) {
            list2.add("str"+s);
            stringIntTestMissing.add(0l);
          } else {
            stringIntTestMissing.add(1l);
          }
          stringIntTestStart.add(list2);
        } else {
          if( dt != 0 ) dateIntTestStart.get(i-1).add((1800+dt) + "-12-31T23:59:59Z"); else increment(dateIntTestMissing,i-1);
          if( s != 0 ) stringIntTestStart.get(i-1).add("str"+s); else increment(stringIntTestMissing,i-1);
        }
      }

      //Long
      if( l != 0 ) {
        if ( j-LONG < 0 ) {
          ArrayList<String> list1 = new ArrayList<>();
          if( dt != 0 ){
            list1.add((1800+dt) + "-12-31T23:59:59Z");
            dateLongTestMissing.add(0l);
          } else {
            dateLongTestMissing.add(1l);
          }
          dateLongTestStart.add(list1);
          ArrayList<String> list2 = new ArrayList<>();
          if( s != 0 ) {
            list2.add("str"+s);
            stringLongTestMissing.add(0l);
          } else {
            stringLongTestMissing.add(1l);
          }
          stringLongTestStart.add(list2);
          ArrayList<Integer> list3 = new ArrayList<>();
          if( i != 0 ) {
            list3.add(i);
            multiLongTestMissing.add(0l);
          } else {
            multiLongTestMissing.add(1l);
          }
          multiLongTestStart.add(list3);
        } else {
          if( dt != 0 ) dateLongTestStart.get((int)l-1).add((1800+dt) + "-12-31T23:59:59Z"); else increment(dateLongTestMissing,(int)l-1);
          if( s != 0 ) stringLongTestStart.get((int)l-1).add("str"+s); else increment(stringLongTestMissing,(int)l-1);
          if( i != 0 ) multiLongTestStart.get((int)l-1).add(i); else increment(multiLongTestMissing,(int)l-1);
        }
      }

      //Long
      if( lm != 0 ) {
        if ( j-LONGM < 0 && lm != l ) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if( i != 0 ) {
            list1.add(i);
            multiLongTestMissing.add(0l);
          } else {
            multiLongTestMissing.add(1l);
          }
          multiLongTestStart.add(list1);
        } else if ( lm != l || multiCanHaveDuplicates ) {
          if( i != 0 ) multiLongTestStart.get((int)lm-1).add(i); else increment( multiLongTestMissing,(int)lm-1);
        }
      }

      if (usually()) {
        assertU(commit()); // to have several segments
      }
    }

    assertU(commit());
    String[] reqFacetParamas = fileToStringArr(LegacyFieldFacetTest.class, fileName);
    String[] reqParamas = new String[reqFacetParamas.length + 2];
    System.arraycopy(reqFacetParamas, 0, reqParamas, 0, reqFacetParamas.length);
    reqParamas[reqFacetParamas.length] = "solr";
    reqParamas[reqFacetParamas.length+1] = "asc";
    setResponse(h.query(request(reqFacetParamas)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sumTest() throws Exception {
    //Int Date
    Collection<Double> intDate = getDoubleList("sum","fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "sum");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Double> intString = getDoubleList("sum","fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "sum");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Double> longDate = getDoubleList("sum","fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "sum");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Double> longString = getDoubleList("sum","fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "sum");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Double> floatDate = getDoubleList("sum","fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "sum");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Double> floatString = getDoubleList("sum","fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "sum");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("sum","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "sum");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Double> doubleString = getDoubleList("sum","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "sum");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void meanTest() throws Exception {
    //Int Date
    Collection<Double> intDate = getDoubleList("mean","fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "mean");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Double> intString = getDoubleList("mean","fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "mean");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Double> longDate = getDoubleList("mean","fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "mean");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Double> longString = getDoubleList("mean","fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "mean");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Double> floatDate = getDoubleList("mean","fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "mean");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Double> floatString = getDoubleList("mean","fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "mean");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("mean","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "mean");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Double> doubleString = getDoubleList("mean","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "mean");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void stddevFacetAscTest() throws Exception {
    //Int Date
    ArrayList<Double> intDate = getDoubleList("stddev","fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "stddev");
    checkStddevs(intDate,intDateTest);
    //Int String
    ArrayList<Double> intString = getDoubleList("stddev","fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "stddev");
    checkStddevs(intString,intStringTest);

    //Long Date
    ArrayList<Double> longDate = getDoubleList("stddev","fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "stddev");
    checkStddevs(longDate,longDateTest);
    //Long String
    ArrayList<Double> longString = getDoubleList("stddev","fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "stddev");
    checkStddevs(longString,longStringTest);

    //Float Date
    ArrayList<Double> floatDate = getDoubleList("stddev","fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "stddev");
    checkStddevs(floatDate,floatDateTest);
    //Float String
    ArrayList<Double> floatString = getDoubleList("stddev","fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "stddev");
    checkStddevs(floatString,floatStringTest);

    //Double Date
    ArrayList<Double> doubleDate = getDoubleList("stddev","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "stddev");
    checkStddevs(doubleDate,doubleDateTest);
    //Double String
    ArrayList<Double> doubleString = getDoubleList("stddev","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "stddev");
    checkStddevs(doubleString,doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void medianFacetAscTest() throws Exception {
    //Int Date
    Collection<Double> intDate = getDoubleList( "median","fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "median");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Double> intString = getDoubleList("median","fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "median");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Double> longDate = getDoubleList("median","fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "median");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Double> longString = getDoubleList("median","fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "median");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Double> floatDate = getDoubleList("median","fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "median");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Double> floatString = getDoubleList("median","fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "median");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("median","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "median");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Double> doubleString = getDoubleList("median","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "median");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc20Test() throws Exception {
    //Int Date
    Collection<Integer> intDate = getIntegerList("percentile_20n","fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "perc_20");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Integer> intString = getIntegerList("percentile_20n","fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "perc_20");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Long> longDate = getLongList("percentile_20n","fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "perc_20");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Long> longString = getLongList("percentile_20n","fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "perc_20");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Float> floatDate = getFloatList("percentile_20n","fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "perc_20");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Float> floatString = getFloatList("percentile_20n","fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "perc_20");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("percentile_20n","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "perc_20");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Double> doubleString = getDoubleList("percentile_20n","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "perc_20");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);

    //Date Int
    Collection<String> dateInt = getStringList("percentile_20","fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "perc_20");
    assertEquals(getRawResponse(),dateInt,dateIntTest);
    //Date Long
    Collection<String> dateString = getStringList("percentile_20","fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "perc_20");
    assertEquals(getRawResponse(),dateString,dateLongTest);

    //String Int
    Collection<String> stringInt = getStringList("percentile_20","fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "perc_20");
    assertEquals(getRawResponse(),stringInt,stringIntTest);
    //String Long
    Collection<String> stringLong = getStringList("percentile_20","fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "perc_20");
    assertEquals(getRawResponse(),stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc60Test() throws Exception {
    //Int Date
    Collection<Integer> intDate = getIntegerList("percentile_60n","fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "perc_60");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Integer> intString = getIntegerList("percentile_60n","fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "perc_60");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Long> longDate = getLongList("percentile_60n","fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "perc_60");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Long> longString = getLongList("percentile_60n","fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "perc_60");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Float> floatDate = getFloatList("percentile_60n","fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "perc_60");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Float> floatString = getFloatList("percentile_60n","fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "perc_60");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("percentile_60n","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "perc_60");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Double> doubleString = getDoubleList("percentile_60n","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "perc_60");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);

    //Date Int
    Collection<String> dateInt = getStringList("percentile_60","fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "perc_60");
    assertEquals(getRawResponse(),dateInt,dateIntTest);
    //Date Long
    Collection<String> dateString = getStringList("percentile_60","fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "perc_60");
    assertEquals(getRawResponse(),dateString,dateLongTest);

    //String Int
    Collection<String> stringInt = getStringList("percentile_60","fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "perc_60");
    assertEquals(getRawResponse(),stringInt,stringIntTest);
    //String Long
    Collection<String> stringLong = getStringList("percentile_60","fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "perc_60");
    assertEquals(getRawResponse(),stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void minTest() throws Exception {
    //Int Date
    Collection<Integer> intDate = getIntegerList("minn","fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "min");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Integer> intString = getIntegerList("minn","fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "min");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Long> longDate = getLongList("minn","fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "min");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Long> longString = getLongList("minn","fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "min");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Float> floatDate = getFloatList("minn","fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "min");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Float> floatString = getFloatList("minn","fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "min");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("minn","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "min");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Double> doubleString = getDoubleList("minn","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "min");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);

    //Date Int
    Collection<String> dateInt = getStringList("min","fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "min");
    assertEquals(getRawResponse(),dateInt,dateIntTest);
    //Date Long
    Collection<String> dateString = getStringList("min","fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "min");
    assertEquals(getRawResponse(),dateString,dateLongTest);

    //String Int
    Collection<String> stringInt = getStringList("min","fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "min");
    assertEquals(getRawResponse(),stringInt,stringIntTest);
    //String Long
    Collection<String> stringLong = getStringList("min","fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "min");
    assertEquals(getRawResponse(),stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void maxTest() throws Exception {
    //Int Date
    Collection<Integer> intDate = getIntegerList("maxn","fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "max");
    assertEquals(getRawResponse(),intDate,intDateTest);

    //Int String
    Collection<Integer> intString = getIntegerList("maxn","fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "max");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Long> longDate = getLongList("maxn","fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "max");
    assertEquals(getRawResponse(),longDate,longDateTest);

    //Long String
    Collection<Long> longString = getLongList("maxn","fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "max");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Float> floatDate = getFloatList("maxn","fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "max");
    assertEquals(getRawResponse(),floatDate,floatDateTest);

    //Float String
    Collection<Float> floatString = getFloatList("maxn","fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "max");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Double> doubleDate = getDoubleList("maxn","fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "max");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);

    //Double String
    Collection<Double> doubleString = getDoubleList("maxn","fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "max");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);

    //String Int
    Collection<String> stringInt = getStringList("max","fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "max");
    assertEquals(getRawResponse(),stringInt,stringIntTest);

    //String Long
    Collection<String> stringLong = getStringList("max","fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "max");
    assertEquals(getRawResponse(),stringLong,stringLongTest);

    //Date Int
    Collection<String> dateInt = getStringList("max","fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "max");
    assertEquals(getRawResponse(),dateInt,dateIntTest);

    //Date Long
    Collection<String> dateString = getStringList("max","fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "max");
    assertEquals(getRawResponse(),dateString,dateLongTest);

  }

  @SuppressWarnings("unchecked")
  @Test
  public void uniqueTest() throws Exception {
    //Int Date
    Collection<Long> intDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "int");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(intDateTestStart, "unique");
    assertEquals(getRawResponse(),intDate,intDateTest);
    //Int String
    Collection<Long> intString = getLongList("uniquen", "fieldFacets", "string_sd", "long", "int");
    ArrayList<Long> intStringTest = (ArrayList<Long>)calculateStat(intStringTestStart, "unique");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Long> longDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "unique");
    assertEquals(getRawResponse(),longDate,longDateTest);
    //Long String
    Collection<Long> longString = getLongList("uniquen", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "unique");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Long> floatDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "float");
    ArrayList<Long> floatDateTest = (ArrayList<Long>)calculateStat(floatDateTestStart, "unique");
    assertEquals(getRawResponse(),floatDate,floatDateTest);
    //Float String
    Collection<Long> floatString = getLongList("uniquen", "fieldFacets", "string_sd", "long", "float");
    ArrayList<Long> floatStringTest = (ArrayList<Long>)calculateStat(floatStringTestStart, "unique");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Long> doubleDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "double");
    ArrayList<Long> doubleDateTest = (ArrayList<Long>)calculateStat(doubleDateTestStart, "unique");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);
    //Double String
    Collection<Long> doubleString = getLongList("uniquen", "fieldFacets", "string_sd", "long", "double");
    ArrayList<Long> doubleStringTest = (ArrayList<Long>)calculateStat(doubleStringTestStart, "unique");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);

    //Date Int
    Collection<Long> dateInt = getLongList("unique", "fieldFacets", "int_id", "long", "date");
    ArrayList<Long> dateIntTest = (ArrayList<Long>)calculateStat(dateIntTestStart, "unique");
    assertEquals(getRawResponse(),dateInt,dateIntTest);
    //Date Long
    Collection<Long> dateString = getLongList("unique", "fieldFacets", "long_ld", "long", "date");
    ArrayList<Long> dateLongTest = (ArrayList<Long>)calculateStat(dateLongTestStart, "unique");
    assertEquals(getRawResponse(),dateString,dateLongTest);

    //String Int
    Collection<Long> stringInt = getLongList("unique", "fieldFacets", "int_id", "long", "str");
    ArrayList<Long> stringIntTest = (ArrayList<Long>)calculateStat(stringIntTestStart, "unique");
    assertEquals(getRawResponse(),stringInt,stringIntTest);
    //String Long
    Collection<Long> stringLong = getLongList("unique", "fieldFacets", "long_ld", "long", "str");
    ArrayList<Long> stringLongTest = (ArrayList<Long>)calculateStat(stringLongTestStart, "unique");
    assertEquals(getRawResponse(),stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void countTest() throws Exception {
    //Int Date
    Collection<Long> intDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "int");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(intDateTestStart, "count");
    assertEquals(getRawResponse(),intDate,intDateTest);

    //Int String
    Collection<Long> intString = getLongList("countn", "fieldFacets", "string_sd", "long", "int");
    ArrayList<Long> intStringTest = (ArrayList<Long>)calculateStat(intStringTestStart, "count");
    assertEquals(getRawResponse(),intString,intStringTest);

    //Long Date
    Collection<Long> longDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "count");
    assertEquals(getRawResponse(),longDate,longDateTest);

    //Long String
    Collection<Long> longString = getLongList("countn", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "count");
    assertEquals(getRawResponse(),longString,longStringTest);

    //Float Date
    Collection<Long> floatDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "float");
    ArrayList<Long> floatDateTest = (ArrayList<Long>)calculateStat(floatDateTestStart, "count");
    assertEquals(getRawResponse(),floatDate,floatDateTest);

    //Float String
    Collection<Long> floatString = getLongList("countn", "fieldFacets", "string_sd", "long", "float");
    ArrayList<Long> floatStringTest = (ArrayList<Long>)calculateStat(floatStringTestStart, "count");
    assertEquals(getRawResponse(),floatString,floatStringTest);

    //Double Date
    Collection<Long> doubleDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "double");
    ArrayList<Long> doubleDateTest = (ArrayList<Long>)calculateStat(doubleDateTestStart, "count");
    assertEquals(getRawResponse(),doubleDate,doubleDateTest);

    //Double String
    Collection<Long> doubleString = getLongList("countn", "fieldFacets", "string_sd", "long", "double");
    ArrayList<Long> doubleStringTest = (ArrayList<Long>)calculateStat(doubleStringTestStart, "count");
    assertEquals(getRawResponse(),doubleString,doubleStringTest);

    //Date Int
    Collection<Long> dateInt = getLongList("count", "fieldFacets", "int_id", "long", "date");
    ArrayList<Long> dateIntTest = (ArrayList<Long>)calculateStat(dateIntTestStart, "count");
    assertEquals(getRawResponse(),dateIntTest,dateInt);

    //Date Long
    Collection<Long> dateLong = getLongList("count", "fieldFacets", "long_ld", "long", "date");
    ArrayList<Long> dateLongTest = (ArrayList<Long>)calculateStat(dateLongTestStart, "count");
    assertEquals(getRawResponse(),dateLong,dateLongTest);

    //String Int
    Collection<Long> stringInt = getLongList("count", "fieldFacets", "int_id", "long", "str");
    ArrayList<Long> stringIntTest = (ArrayList<Long>)calculateStat(stringIntTestStart, "count");
    assertEquals(getRawResponse(),stringInt,stringIntTest);

    //String Long
    Collection<Long> stringLong = getLongList("count", "fieldFacets", "long_ld", "long", "str");
    ArrayList<Long> stringLongTest = (ArrayList<Long>)calculateStat(stringLongTestStart, "count");
    assertEquals(getRawResponse(),stringLong,stringLongTest);
  }

  @Test
  public void missingTest() throws Exception {
    //Int Date
    Collection<Long> intDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "int");
    setLatestType("int");
    assertEquals(getRawResponse(),intDateTestMissing,intDate);

    //Int String
    Collection<Long> intString = getLongList("missingn", "fieldFacets", "string_sd", "long", "int");
    assertEquals(getRawResponse(),intStringTestMissing,intString);

    //Long Date
    Collection<Long> longDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "long");
    setLatestType("long");
    assertEquals(getRawResponse(),longDateTestMissing,longDate);

    //Long String
    Collection<Long> longString = getLongList("missingn", "fieldFacets", "string_sd", "long", "long");
    assertEquals(getRawResponse(),longStringTestMissing,longString);

    //Float Date
    Collection<Long> floatDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "float");
    setLatestType("float");
    assertEquals(getRawResponse(),floatDateTestMissing,floatDate);

    //Float String
    Collection<Long> floatString = getLongList("missingn", "fieldFacets", "string_sd", "long", "float");
    assertEquals(getRawResponse(),floatStringTestMissing,floatString);

    //Double Date
    Collection<Long> doubleDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "double");
    setLatestType("double");
    assertEquals(getRawResponse(),doubleDateTestMissing,doubleDate);

    //Double String
    Collection<Long> doubleString = getLongList("missingn", "fieldFacets", "string_sd", "long", "double");
    assertEquals(getRawResponse(),doubleStringTestMissing,doubleString);

    //Date Int
    Collection<Long> dateInt = getLongList("missing", "fieldFacets", "int_id", "long", "date");
    setLatestType("date");
    assertEquals(getRawResponse(),dateIntTestMissing,dateInt);

    //Date Long
    Collection<Long> dateLong = getLongList("missing", "fieldFacets", "long_ld", "long", "date");
    assertEquals(getRawResponse(),dateLongTestMissing,dateLong);

    //String Int
    Collection<Long> stringInt = getLongList("missing", "fieldFacets", "int_id", "long", "str");
    setLatestType("string");
    assertEquals(getRawResponse(),stringIntTestMissing,stringInt);

    //String Long
    Collection<Long> stringLong = getLongList("missing", "fieldFacets", "long_ld", "long", "str");
    assertEquals(getRawResponse(),stringLongTestMissing,stringLong);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTest() throws Exception {
    //Long
    Collection<Double> lon = getDoubleList("multivalued", "fieldFacets", "long_ldm", "double", "mean");
    ArrayList<Double> longTest = calculateNumberStat(multiLongTestStart, "mean");
    assertEquals(getRawResponse(),lon,longTest);
    //Date
    Collection<Double> date = getDoubleList("multivalued", "fieldFacets", "date_dtdm", "double", "mean");
    ArrayList<Double> dateTest = calculateNumberStat(multiDateTestStart, "mean");
    assertEquals(getRawResponse(),date,dateTest);
    //String
    Collection<Double> string = getDoubleList("multivalued", "fieldFacets", "string_sdm", "double", "mean");
    ArrayList<Double> stringTest = calculateNumberStat(multiStringTestStart, "mean");
    assertEquals(getRawResponse(),string,stringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void missingFacetTest() throws Exception {
    //int MultiDate
    String xPath = "/response/lst[@name='stats']/lst[@name='missingf']/lst[@name='fieldFacets']/lst[@name='date_dtdm']/lst[@name='(MISSING)']";
    Node missingNodeXPath = getNode(xPath);
    assertNotNull(getRawResponse(), missingNodeXPath);

    ArrayList<Double> string = getDoubleList("missingf", "fieldFacets", "date_dtdm", "double", "mean");
    //super.removeNodes(xPath, string);
    ArrayList<Double> stringTest = calculateNumberStat(multiDateTestStart, "mean");
    assertEquals(getRawResponse(), string,stringTest);

    //Int String
    xPath = "/response/lst[@name='stats']/lst[@name='missingf']/lst[@name='fieldFacets']/lst[@name='string_sd']/lst[@name='(MISSING)']";
    missingNodeXPath = getNode(xPath);
    String missingNodeXPathStr = xPath;
    assertNotNull(getRawResponse(), missingNodeXPath);

    xPath = "/response/lst[@name='stats']/lst[@name='missingf']/lst[@name='fieldFacets']/lst[@name='string_sd']/lst[@name='str0']";
    assertNull(getRawResponse(), getNode(xPath));

    List<Double> intString = getDoubleList("missingf", "fieldFacets", "string_sd", "double", "mean");
    //removeNodes(missingNodeXPathStr, intString);
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "mean");
    assertEquals(getRawResponse(), intString,intStringTest);

    //Int Date
    Collection<Double> intDate = getDoubleList("missingf", "fieldFacets", "date_dtd", "double", "mean");
    ArrayList<ArrayList<Double>> intDateMissingTestStart = (ArrayList<ArrayList<Double>>) intDateTestStart.clone();
    ArrayList<Double> intDateTest = calculateNumberStat(intDateMissingTestStart, "mean");
    assertEquals(getRawResponse(),intDate,intDateTest);
  }

  private void checkStddevs(ArrayList<Double> list1, ArrayList<Double> list2) {
    Collections.sort(list1);
    Collections.sort(list2);
    for (int i = 0; i<list1.size(); i++) {
      if ((Math.abs(list1.get(i)-list2.get(i))<.00000000001) == false) {
        Assert.assertEquals(getRawResponse(), list1.get(i), list2.get(i), 0.00000000001);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void assertEquals(String mes, Object actual, Object expected) {
    Collections.sort((List<Comparable>) actual);
    Collections.sort((List<Comparable>)  expected);
    Assert.assertEquals(mes, actual, expected);
  }
}
