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
package org.apache.solr.analytics.legacy;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyNoFacetTest extends LegacyAbstractAnalyticsTest {
  static String fileName = "noFacets.txt";

  static public final int INT = 71;
  static public final int LONG = 36;
  static public final int FLOAT = 93;
  static public final int DOUBLE = 49;
  static public final int DATE = 12;
  static public final int STRING = 28;
  static public final int NUM_LOOPS = 100;

  //INT
  static ArrayList<Integer> intTestStart;
  static long intMissing = 0;

  //LONG
  static ArrayList<Long> longTestStart;
  static long longMissing = 0;

  //FLOAT
  static ArrayList<Float> floatTestStart;
  static long floatMissing = 0;

  //DOUBLE
  static ArrayList<Double> doubleTestStart;
  static long doubleMissing = 0;

  //DATE
  static ArrayList<String> dateTestStart;
  static long dateMissing = 0;

  //STR
  static ArrayList<String> stringTestStart;
  static long stringMissing = 0;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");
    defaults.put("int_id", 0);
    defaults.put("long_ld", 0L);
    defaults.put("float_fd", (float) 0);
    defaults.put("double_dd", (double) 0);
    defaults.put("date_dtd", "1800-12-31T23:59:59Z");
    defaults.put("string_sd", "str0");

    intTestStart = new ArrayList<>();
    intMissing = 0;
    longTestStart = new ArrayList<>();
    longMissing = 0;
    floatTestStart = new ArrayList<>();
    floatMissing = 0;
    doubleTestStart = new ArrayList<>();
    doubleMissing = 0;
    dateTestStart = new ArrayList<>();
    dateMissing = 0;
    stringTestStart = new ArrayList<>();
    stringMissing = 0;

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      String dt = (1800+j%DATE) + "-12-31T23:59:59Z";
      String s = "str" + (j%STRING);
      List<String> fields = new ArrayList<>();
      fields.add("id"); fields.add("1000"+j);

      if( i != 0 ){
        fields.add("int_id"); fields.add("" + i);
        intTestStart.add(i);
      } else intMissing++;

      if( l != 0l ){
        fields.add("long_ld"); fields.add("" + l);
        longTestStart.add(l);
      } else longMissing++;

      if( f != 0.0f ){
        fields.add("float_fd"); fields.add("" + f);
        floatTestStart.add(f);
      } else floatMissing++;

      if( d != 0.0d ){
        fields.add("double_dd"); fields.add("" + d);
        doubleTestStart.add(d);
      } else doubleMissing++;

      if( (j%DATE) != 0 ){
        fields.add("date_dtd"); fields.add(dt);
        dateTestStart.add(dt);
      } else dateMissing++;

      if( (j%STRING) != 0 ){
        fields.add("string_sd"); fields.add(s);
        stringTestStart.add(s);
      } else stringMissing++;

      assertU(adoc(fields.toArray(new String[0])));


      if (usually()) {
        assertU(commit());  // to have several segments
      }
    }

    assertU(commit());

    //Sort ascending tests
    setResponse(h.query(request(fileToStringArr(LegacyNoFacetTest.class, fileName))));
  }

  @Test
  public void sumTest() throws Exception {
    //Int
    Double intResult = (Double)getStatResult("sr", "int_id", VAL_TYPE.DOUBLE);
    Double intTest = calculateNumberStat(intTestStart, "sum");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Double longResult = (Double)getStatResult("sr", "long_ld", VAL_TYPE.DOUBLE);
    Double longTest = calculateNumberStat(longTestStart, "sum");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Double floatResult = (Double)getStatResult("sr", "float_fd", VAL_TYPE.DOUBLE);
    Double floatTest = calculateNumberStat(floatTestStart, "sum");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult("sr", "double_dd", VAL_TYPE.DOUBLE);
        Double doubleTest = calculateNumberStat(doubleTestStart, "sum");
    assertEquals(getRawResponse(), doubleResult,doubleTest);
  }

  @Test
  public void meanTest() throws Exception {
    //Int
    Double intResult = (Double)getStatResult("mr", "int_id", VAL_TYPE.DOUBLE);
    Double intTest = calculateNumberStat(intTestStart, "mean");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Double longResult = (Double)getStatResult("mr", "long_ld", VAL_TYPE.DOUBLE);
    Double longTest = calculateNumberStat(longTestStart, "mean");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Double floatResult = (Double)getStatResult("mr", "float_fd", VAL_TYPE.DOUBLE);
    Double floatTest = calculateNumberStat(floatTestStart, "mean");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult("mr", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = calculateNumberStat(doubleTestStart, "mean");
    assertEquals(getRawResponse(), doubleResult,doubleTest);
  }

  @Test
  public void stddevTest() throws Exception {
    //Int
    Double intResult = (Double)getStatResult("str", "int_id", VAL_TYPE.DOUBLE);
    Double intTest = calculateNumberStat(intTestStart, "stddev");
    assertEquals(getRawResponse(), intResult, intTest, 0.00000000001);

    //Long
    Double longResult = (Double)getStatResult("str", "long_ld", VAL_TYPE.DOUBLE);
    Double longTest = calculateNumberStat(longTestStart, "stddev");
    assertEquals(getRawResponse(), longResult, longTest, 0.00000000001);

    //Float
    Double floatResult = (Double)getStatResult("str", "float_fd", VAL_TYPE.DOUBLE);
    Double floatTest = calculateNumberStat(floatTestStart, "stddev");
    assertEquals(getRawResponse(), floatResult, floatTest, 0.00000000001);


    //Double
    Double doubleResult = (Double)getStatResult("str", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = calculateNumberStat(doubleTestStart, "stddev");
    assertEquals(getRawResponse(), doubleResult, doubleTest, 0.00000000001);
  }

  @Test
  public void medianTest() throws Exception {
    //Int
    Double intResult = (Double)getStatResult("medr", "int_id", VAL_TYPE.DOUBLE);
    Double intTest = calculateNumberStat(intTestStart, "median");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Double longResult = (Double)getStatResult("medr", "long_ld", VAL_TYPE.DOUBLE);
    Double longTest = calculateNumberStat(longTestStart, "median");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Double floatResult = (Double)getStatResult("medr", "float_fd", VAL_TYPE.DOUBLE);
    Double floatTest = calculateNumberStat(floatTestStart, "median");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult("medr", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = calculateNumberStat(doubleTestStart, "median");
    assertEquals(getRawResponse(), doubleResult,doubleTest);
  }

  @Test
  public void perc20Test() throws Exception {
    //Int 20
    Integer intResult = (Integer)getStatResult("p2r", "int_id", VAL_TYPE.INTEGER);
    Integer intTest = (Integer)calculateStat(intTestStart, "perc_20");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long 20
    Long longResult = (Long)getStatResult("p2r", "long_ld", VAL_TYPE.LONG);
    Long longTest = (Long)calculateStat(longTestStart, "perc_20");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float 20
    Float floatResult = (Float)getStatResult("p2r", "float_fd", VAL_TYPE.FLOAT);
    Float floatTest = (Float)calculateStat(floatTestStart, "perc_20");
    //assertEquals(getRawResponse(), floatResult,floatTest);

    //Double 20
    Double doubleResult = (Double)getStatResult("p2r", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = (Double)calculateStat(doubleTestStart, "perc_20");
    assertEquals(getRawResponse(), doubleResult,doubleTest);

    //Date 20
    String dateResult = (String)getStatResult("p2r", "date_dtd", VAL_TYPE.DATE);
    String dateTest = (String)calculateStat(dateTestStart, "perc_20");
    assertEquals(getRawResponse(), dateResult,dateTest);

    //String 20
    String stringResult = (String)getStatResult("p2r", "string_sd", VAL_TYPE.STRING);
    String stringTest = (String)calculateStat(stringTestStart, "perc_20");
    assertEquals(getRawResponse(), stringResult,stringTest);
  }

  @Test
  public void perc60Test() throws Exception {
    //Int 60
    Integer intResult = (Integer)getStatResult("p6r", "int_id", VAL_TYPE.INTEGER);
    Integer intTest = (Integer)calculateStat(intTestStart, "perc_60");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long 60
    Long longResult = (Long)getStatResult("p6r", "long_ld", VAL_TYPE.LONG);
    Long longTest = (Long)calculateStat(longTestStart, "perc_60");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float 60
    Float floatResult = (Float)getStatResult("p6r", "float_fd", VAL_TYPE.FLOAT);
    Float floatTest = (Float)calculateStat(floatTestStart, "perc_60");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double 60
    Double doubleResult = (Double)getStatResult("p6r", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = (Double)calculateStat(doubleTestStart, "perc_60");
    assertEquals(getRawResponse(), doubleResult,doubleTest);

    //Date 60
    String dateResult = (String)getStatResult("p6r", "date_dtd", VAL_TYPE.DATE);
    String dateTest = (String)calculateStat(dateTestStart, "perc_60");
    assertEquals(getRawResponse(), dateResult,dateTest);

    //String 60
    String stringResult = (String)getStatResult("p6r", "string_sd", VAL_TYPE.STRING);
    String stringTest = (String)calculateStat(stringTestStart, "perc_60");
    assertEquals(getRawResponse(), stringResult,stringTest);
  }

  @Test
  public void minTest() throws Exception {
    //Int
    Integer intResult = ((Integer)getStatResult("mir", "int_id", VAL_TYPE.INTEGER));
    Integer intTest = (Integer)calculateStat(intTestStart, "min");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Long longResult = ((Long)getStatResult("mir", "long_ld", VAL_TYPE.LONG));
    Long longTest = (Long)calculateStat(longTestStart, "min");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Float floatResult = ((Float)getStatResult("mir", "float_fd", VAL_TYPE.FLOAT));
    Float floatTest = (Float)calculateStat(floatTestStart, "min");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult("mir", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = (Double)calculateStat(doubleTestStart, "min");
    assertEquals(getRawResponse(), doubleResult,doubleTest);

    //Date
    String dateResult = (String)getStatResult("mir", "date_dtd", VAL_TYPE.DATE);
    String dateTest = (String)calculateStat(dateTestStart, "min");
    assertEquals(getRawResponse(), dateResult,dateTest);

    //String
    String stringResult = (String)getStatResult("mir", "string_sd", VAL_TYPE.STRING);
    String stringTest = (String)calculateStat(stringTestStart, "min");
    assertEquals(getRawResponse(), stringResult,stringTest);
  }

  @Test
  public void maxTest() throws Exception {
    //Int
    Integer intResult = ((Integer)getStatResult("mar", "int_id", VAL_TYPE.INTEGER));
    Integer intTest = (Integer)calculateStat(intTestStart, "max");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Long longResult = ((Long)getStatResult("mar", "long_ld", VAL_TYPE.LONG));
    Long longTest = (Long)calculateStat(longTestStart, "max");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Float floatResult = ((Float)getStatResult("mar", "float_fd", VAL_TYPE.FLOAT));
    Float floatTest = (Float)calculateStat(floatTestStart, "max");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult("mar", "double_dd", VAL_TYPE.DOUBLE);
    Double doubleTest = (Double)calculateStat(doubleTestStart, "max");
    assertEquals(getRawResponse(), doubleResult,doubleTest);

    //Date
    String dateResult = (String)getStatResult("mar", "date_dtd", VAL_TYPE.DATE);
    String dateTest = (String)calculateStat(dateTestStart, "max");
    assertEquals(getRawResponse(), dateResult,dateTest);

    //String
    String stringResult = (String)getStatResult("mar", "string_sd", VAL_TYPE.STRING);
    String stringTest = (String)calculateStat(stringTestStart, "max");
    assertEquals(getRawResponse(), stringResult,stringTest);
  }

  @Test
  public void uniqueTest() throws Exception {
    //Int
    Long intResult = (Long)getStatResult("ur", "int_id", VAL_TYPE.LONG);
    Long intTest = (Long)calculateStat(intTestStart, "unique");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Long longResult = (Long)getStatResult("ur", "long_ld", VAL_TYPE.LONG);
    Long longTest = (Long)calculateStat(longTestStart, "unique");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Long floatResult = (Long)getStatResult("ur", "float_fd", VAL_TYPE.LONG);
    Long floatTest = (Long)calculateStat(floatTestStart, "unique");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Long doubleResult = (Long)getStatResult("ur", "double_dd", VAL_TYPE.LONG);
    Long doubleTest = (Long)calculateStat(doubleTestStart, "unique");
    assertEquals(getRawResponse(), doubleResult,doubleTest);

    //Date
    Long dateResult = (Long)getStatResult("ur", "date_dtd", VAL_TYPE.LONG);
    Long dateTest = (Long)calculateStat(dateTestStart, "unique");
    assertEquals(getRawResponse(), dateResult,dateTest);

    //String
    Long stringResult = (Long)getStatResult("ur", "string_sd", VAL_TYPE.LONG);
    Long stringTest = (Long)calculateStat(stringTestStart, "unique");
    assertEquals(getRawResponse(), stringResult,stringTest);
  }

  @Test
  public void countTest() throws Exception {
    //Int
    Long intResult = (Long)getStatResult("cr", "int_id", VAL_TYPE.LONG);
    Long intTest = (Long)calculateStat(intTestStart, "count");
    assertEquals(getRawResponse(), intResult,intTest);

    //Long
    Long longResult = (Long)getStatResult("cr", "long_ld", VAL_TYPE.LONG);
    Long longTest = (Long)calculateStat(longTestStart, "count");
    assertEquals(getRawResponse(), longResult,longTest);

    //Float
    Long floatResult = (Long)getStatResult("cr", "float_fd", VAL_TYPE.LONG);
    Long floatTest = (Long)calculateStat(floatTestStart, "count");
    assertEquals(getRawResponse(), floatResult,floatTest);

    //Double
    Long doubleResult = (Long)getStatResult("cr", "double_dd", VAL_TYPE.LONG);
    Long doubleTest = (Long)calculateStat(doubleTestStart, "count");
    assertEquals(getRawResponse(), doubleResult,doubleTest);

    //Date
    Long dateResult = (Long)getStatResult("cr", "date_dtd", VAL_TYPE.LONG);
    Long dateTest = (Long)calculateStat(dateTestStart, "count");
    assertEquals(getRawResponse(), dateResult,dateTest);

    //String
    Long stringResult = (Long)getStatResult("cr", "string_sd", VAL_TYPE.LONG);
    Long stringTest = (Long)calculateStat(stringTestStart, "count");
    assertEquals(getRawResponse(), stringResult,stringTest);
  }

  @Test
  public void missingDefaultTest() throws Exception {
    //Int
    long intResult = (Long)getStatResult("misr", "int_id", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), intMissing,intResult);

    //Long
    long longResult = (Long)getStatResult("misr", "long_ld", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), longMissing,longResult);

    //Float
    long floatResult = (Long)getStatResult("misr", "float_fd", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), floatMissing,floatResult);

    //Double
    long doubleResult = (Long)getStatResult("misr", "double_dd", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), doubleMissing,doubleResult);

    //Date
    long dateResult = (Long)getStatResult("misr", "date_dtd", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), dateMissing,dateResult);

    //String
    long stringResult = (Long)getStatResult("misr", "string_sd", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), stringMissing, stringResult);
  }

}
