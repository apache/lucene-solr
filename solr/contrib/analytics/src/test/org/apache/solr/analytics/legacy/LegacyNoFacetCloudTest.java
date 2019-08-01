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

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.Test;

public class LegacyNoFacetCloudTest extends LegacyAbstractAnalyticsCloudTest {
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

  @Before
  public void populate() throws Exception {
    intTestStart = new ArrayList<>();
    longTestStart = new ArrayList<>();
    floatTestStart = new ArrayList<>();
    doubleTestStart = new ArrayList<>();
    dateTestStart = new ArrayList<>();
    stringTestStart = new ArrayList<>();
    intMissing = 0;
    longMissing = 0;
    doubleMissing = 0;
    floatMissing = 0;
    dateMissing = 0;
    stringMissing = 0;

    UpdateRequest req = new UpdateRequest();
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

      req.add(fields.toArray(new String[0]));
    }
    req.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void sumTest() throws Exception {
    String[] params = new String[] {
        "o.sr.s.int_id", "sum(int_id)",
        "o.sr.s.long_ld", "sum(long_ld)",
        "o.sr.s.float_fd", "sum(float_fd)",
        "o.sr.s.double_dd", "sum(double_dd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Double intResult = getValue(response, "sr", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "sum");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Double longResult = getValue(response, "sr", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "sum");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Double floatResult = getValue(response, "sr", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "sum");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Double doubleResult = getValue(response, "sr", "double_dd");
        Double doubleTest = (Double) calculateNumberStat(doubleTestStart, "sum");
    assertEquals(responseStr, doubleResult,doubleTest);
  }

  @Test
  public void meanTest() throws Exception {
    String[] params = new String[] {
        "o.mr.s.int_id", "mean(int_id)",
        "o.mr.s.long_ld", "mean(long_ld)",
        "o.mr.s.float_fd", "mean(float_fd)",
        "o.mr.s.double_dd", "mean(double_dd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Double intResult = getValue(response, "mr", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "mean");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Double longResult = getValue(response, "mr", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "mean");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Double floatResult = getValue(response, "mr", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "mean");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Double doubleResult = getValue(response, "mr", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "mean");
    assertEquals(responseStr, doubleResult,doubleTest);
  }

  @Test
  public void stddevTest() throws Exception {
    String[] params = new String[] {
        "o.str.s.int_id", "stddev(int_id)",
        "o.str.s.long_ld", "stddev(long_ld)",
        "o.str.s.float_fd", "stddev(float_fd)",
        "o.str.s.double_dd", "stddev(double_dd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Double intResult = getValue(response, "str", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "stddev");
    assertEquals(responseStr, intResult, intTest, 0.00000000001);

    //Long
    Double longResult = getValue(response, "str", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "stddev");
    assertEquals(responseStr, longResult, longTest, 0.00000000001);

    //Float
    Double floatResult = getValue(response, "str", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "stddev");
    assertEquals(responseStr, floatResult, floatTest, 0.00000000001);


    //Double
    Double doubleResult = getValue(response, "str", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "stddev");
    assertEquals(responseStr, doubleResult, doubleTest, 0.00000000001);
  }

  @Test
  public void medianTest() throws Exception {
    String[] params = new String[] {
        "o.medr.s.int_id", "median(int_id)",
        "o.medr.s.long_ld", "median(long_ld)",
        "o.medr.s.float_fd", "median(float_fd)",
        "o.medr.s.double_dd", "median(double_dd)",
        "o.medr.s.date_dtd", "median(date_dtd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Double intResult = getValue(response, "medr", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "median");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Double longResult = getValue(response, "medr", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "median");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Double floatResult = getValue(response, "medr", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "median");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Double doubleResult = getValue(response, "medr", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "median");
    assertEquals(responseStr, doubleResult,doubleTest);

    // TODO: Add test for date median
  }

  @Test
  public void perc20Test() throws Exception {
    String[] params = new String[] {
        "o.p2r.s.int_id", "percentile(20,int_id)",
        "o.p2r.s.long_ld", "percentile(20,long_ld)",
        "o.p2r.s.float_fd", "percentile(20,float_fd)",
        "o.p2r.s.double_dd", "percentile(20,double_dd)",
        "o.p2r.s.date_dtd", "string(percentile(20,date_dtd))",
        "o.p2r.s.string_sd", "percentile(20,string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int 20
    Integer intResult = getValue(response, "p2r", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "perc_20");
    assertEquals(responseStr, intResult,intTest);

    //Long 20
    Long longResult = getValue(response, "p2r", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "perc_20");
    assertEquals(responseStr, longResult,longTest);

    //Float 20
    Float floatResult = getValue(response, "p2r", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "perc_20");
    assertEquals(responseStr, floatResult,floatTest);

    //Double 20
    Double doubleResult = getValue(response, "p2r", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "perc_20");
    assertEquals(responseStr, doubleResult,doubleTest);

    //Date 20
    String dateResult = getValue(response, "p2r", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "perc_20");
    assertEquals(responseStr, dateResult,dateTest);

    //String 20
    String stringResult = getValue(response, "p2r", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "perc_20");
    assertEquals(responseStr, stringResult,stringTest);
  }

  @Test
  public void perc60Test() throws Exception {
    String[] params = new String[] {
        "o.p6r.s.int_id", "percentile(60,int_id)",
        "o.p6r.s.long_ld", "percentile(60,long_ld)",
        "o.p6r.s.float_fd", "percentile(60,float_fd)",
        "o.p6r.s.double_dd", "percentile(60,double_dd)",
        "o.p6r.s.date_dtd", "string(percentile(60,date_dtd))",
        "o.p6r.s.string_sd", "percentile(60,string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int 60
    Integer intResult = getValue(response, "p6r", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "perc_60");
    assertEquals(responseStr, intResult,intTest);

    //Long 60
    Long longResult = getValue(response, "p6r", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "perc_60");
    assertEquals(responseStr, longResult,longTest);

    //Float 60
    Float floatResult = getValue(response, "p6r", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "perc_60");
    assertEquals(responseStr, floatResult,floatTest);

    //Double 60
    Double doubleResult = getValue(response, "p6r", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "perc_60");
    assertEquals(responseStr, doubleResult,doubleTest);

    //Date 60
    String dateResult = getValue(response, "p6r", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "perc_60");
    assertEquals(responseStr, dateResult,dateTest);

    //String 60
    String stringResult = getValue(response, "p6r", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "perc_60");
    assertEquals(responseStr, stringResult,stringTest);
  }

  @Test
  public void minTest() throws Exception {
    String[] params = new String[] {
        "o.mir.s.int_id", "min(int_id)",
        "o.mir.s.long_ld", "min(long_ld)",
        "o.mir.s.float_fd", "min(float_fd)",
        "o.mir.s.double_dd", "min(double_dd)",
        "o.mir.s.date_dtd", "string(min(date_dtd))",
        "o.mir.s.string_sd", "min(string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Integer intResult = getValue(response, "mir", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "min");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Long longResult = getValue(response, "mir", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "min");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Float floatResult = getValue(response, "mir", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "min");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Double doubleResult = getValue(response, "mir", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "min");
    assertEquals(responseStr, doubleResult,doubleTest);

    //Date
    String dateResult = getValue(response, "mir", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "min");
    assertEquals(responseStr, dateResult,dateTest);

    //String
    String stringResult = getValue(response, "mir", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "min");
    assertEquals(responseStr, stringResult,stringTest);
  }

  @Test
  public void maxTest() throws Exception {
    String[] params = new String[] {
        "o.mar.s.int_id", "max(int_id)",
        "o.mar.s.long_ld", "max(long_ld)",
        "o.mar.s.float_fd", "max(float_fd)",
        "o.mar.s.double_dd", "max(double_dd)",
        "o.mar.s.date_dtd", "string(max(date_dtd))",
        "o.mar.s.string_sd", "max(string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Integer intResult = getValue(response, "mar", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "max");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Long longResult = getValue(response, "mar", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "max");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Float floatResult = getValue(response, "mar", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "max");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Double doubleResult = getValue(response, "mar", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "max");
    assertEquals(responseStr, doubleResult,doubleTest);

    //Date
    String dateResult = getValue(response, "mar", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "max");
    assertEquals(responseStr, dateResult,dateTest);

    //String
    String stringResult = getValue(response, "mar", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "max");
    assertEquals(responseStr, stringResult,stringTest);
  }

  @Test
  public void uniqueTest() throws Exception {
    String[] params = new String[] {
        "o.ur.s.int_id", "unique(int_id)",
        "o.ur.s.long_ld", "unique(long_ld)",
        "o.ur.s.float_fd", "unique(float_fd)",
        "o.ur.s.double_dd", "unique(double_dd)",
        "o.ur.s.date_dtd", "unique(date_dtd)",
        "o.ur.s.string_sd", "unique(string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Long intResult = getValue(response, "ur", "int_id");
    Long intTest = (Long)calculateStat(intTestStart, "unique");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Long longResult = getValue(response, "ur", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "unique");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Long floatResult = getValue(response, "ur", "float_fd");
    Long floatTest = (Long)calculateStat(floatTestStart, "unique");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Long doubleResult = getValue(response, "ur", "double_dd");
    Long doubleTest = (Long)calculateStat(doubleTestStart, "unique");
    assertEquals(responseStr, doubleResult,doubleTest);

    //Date
    Long dateResult = getValue(response, "ur", "date_dtd");
    Long dateTest = (Long)calculateStat(dateTestStart, "unique");
    assertEquals(responseStr, dateResult,dateTest);

    //String
    Long stringResult = getValue(response, "ur", "string_sd");
    Long stringTest = (Long)calculateStat(stringTestStart, "unique");
    assertEquals(responseStr, stringResult,stringTest);
  }

  @Test
  public void countTest() throws Exception {
    String[] params = new String[] {
        "o.cr.s.int_id", "count(int_id)",
        "o.cr.s.long_ld", "count(long_ld)",
        "o.cr.s.float_fd", "count(float_fd)",
        "o.cr.s.double_dd", "count(double_dd)",
        "o.cr.s.date_dtd", "count(date_dtd)",
        "o.cr.s.string_sd", "count(string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    Long intResult = getValue(response, "cr", "int_id");
    Long intTest = (Long)calculateStat(intTestStart, "count");
    assertEquals(responseStr, intResult,intTest);

    //Long
    Long longResult = getValue(response, "cr", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "count");
    assertEquals(responseStr, longResult,longTest);

    //Float
    Long floatResult = getValue(response, "cr", "float_fd");
    Long floatTest = (Long)calculateStat(floatTestStart, "count");
    assertEquals(responseStr, floatResult,floatTest);

    //Double
    Long doubleResult = getValue(response, "cr", "double_dd");
    Long doubleTest = (Long)calculateStat(doubleTestStart, "count");
    assertEquals(responseStr, doubleResult,doubleTest);

    //Date
    Long dateResult = getValue(response, "cr", "date_dtd");
    Long dateTest = (Long)calculateStat(dateTestStart, "count");
    assertEquals(responseStr, dateResult,dateTest);

    //String
    Long stringResult = getValue(response, "cr", "string_sd");
    Long stringTest = (Long)calculateStat(stringTestStart, "count");
    assertEquals(responseStr, stringResult,stringTest);
  }

  @Test
  public void missingDefaultTest() throws Exception {
    String[] params = new String[] {
        "o.misr.s.int_id", "missing(int_id)",
        "o.misr.s.long_ld", "missing(long_ld)",
        "o.misr.s.float_fd", "missing(float_fd)",
        "o.misr.s.double_dd", "missing(double_dd)",
        "o.misr.s.date_dtd", "missing(date_dtd)",
        "o.misr.s.string_sd", "missing(string_sd)"
    };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    //Int
    long intResult = getValue(response, "misr", "int_id");
    assertEquals(responseStr, intMissing,intResult);

    //Long
    long longResult = getValue(response, "misr", "long_ld");
    assertEquals(responseStr, longMissing,longResult);

    //Float
    long floatResult = getValue(response, "misr", "float_fd");
    assertEquals(responseStr, floatMissing,floatResult);

    //Double
    long doubleResult = getValue(response, "misr", "double_dd");
    assertEquals(responseStr, doubleMissing,doubleResult);

    //Date
    long dateResult = getValue(response, "misr", "date_dtd");
    assertEquals(responseStr, dateMissing,dateResult);

    //String
    long stringResult = getValue(response, "misr", "string_sd");
    assertEquals(responseStr, stringMissing, stringResult);
  }

}
