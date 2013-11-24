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

package org.apache.solr.analytics;


import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class NoFacetTest extends AbstractAnalyticsStatsTest {
  static String fileName = "core/src/test-files/analytics/requestFiles/noFacets.txt";

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
  
  //STRING
  static ArrayList<String> stringTestStart; 
  static long stringMissing = 0;
  
  static String response;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");
    defaults.put("int_id", new Integer(0));
    defaults.put("long_ld", new Long(0));
    defaults.put("float_fd", new Float(0));
    defaults.put("double_dd", new Double(0));
    defaults.put("date_dtd", "1800-12-31T23:59:59Z");
    defaults.put("string_sd", "str0");
    
    intTestStart = new ArrayList<Integer>(); 
    longTestStart = new ArrayList<Long>(); 
    floatTestStart = new ArrayList<Float>(); 
    doubleTestStart = new ArrayList<Double>(); 
    dateTestStart = new ArrayList<String>(); 
    stringTestStart = new ArrayList<String>(); 
    
    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      String dt = (1800+j%DATE) + "-12-31T23:59:59Z";
      String s = "str" + (j%STRING);
      List<String> fields = new ArrayList<String>();
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
      
      fields.add("int_i"); fields.add("" + i);
      fields.add("long_l"); fields.add("" + l);
      fields.add("float_f"); fields.add("" + f);
      fields.add("double_d"); fields.add("" + d);
      
      assertU(adoc(fields.toArray(new String[0])));
      
      
      if (usually()) {
        assertU(commit());  // to have several segments
      }
    }
    
    assertU(commit()); 
    
    //Sort ascending tests
    response = h.query(request(fileToStringArr(fileName)));
  }
      
  @Test
  public void sumTest() throws Exception { 
    //Int
    Double intResult = (Double)getStatResult(response, "sr", "double", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "sum");
    assertEquals(intResult,intTest);
    
    //Long
    Double longResult = (Double)getStatResult(response, "sr", "double", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "sum");
    assertEquals(longResult,longTest);
    
    //Float
    Double floatResult = (Double)getStatResult(response, "sr", "double", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "sum");
    assertEquals(floatResult,floatTest);
    
    //Double
    Double doubleResult = (Double)getStatResult(response, "sr", "double", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "sum");
    assertEquals(doubleResult,doubleTest);
  }
  
  @Test
  public void sumOfSquaresTest() throws Exception { 
    //Int
    Double intResult = (Double)getStatResult(response, "sosr", "double", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "sumOfSquares");
    assertEquals(intResult,intTest);
    
    //Long
    Double longResult = (Double)getStatResult(response, "sosr", "double", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "sumOfSquares");
    assertEquals(longResult,longTest);
    
    //Float
    Double floatResult = (Double)getStatResult(response, "sosr", "double", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "sumOfSquares");
    assertEquals(floatResult,floatTest);
    
    //Double
    Double doubleResult = (Double)getStatResult(response, "sosr", "double", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "sumOfSquares");
    assertEquals(doubleResult,doubleTest);
  }
  
  @Test
  public void meanTest() throws Exception { 
    //Int
    Double intResult = (Double)getStatResult(response, "mr", "double", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "mean");
    assertEquals(intResult,intTest);
    
    //Long
    Double longResult = (Double)getStatResult(response, "mr", "double", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "mean");
    assertEquals(longResult,longTest);
    
    //Float
    Double floatResult = (Double)getStatResult(response, "mr", "double", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "mean");
    assertEquals(floatResult,floatTest);
    
    //Double
    Double doubleResult = (Double)getStatResult(response, "mr", "double", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "mean");
    assertEquals(doubleResult,doubleTest);
  }
  
  @Test @Ignore("SOLR-5488") 
  public void stddevTest() throws Exception { 
    //Int
    Double intResult = (Double)getStatResult(response, "str", "double", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "stddev");
    assertTrue(Math.abs(intResult-intTest)<.00000000001);
    
    //Long
    Double longResult = (Double)getStatResult(response, "str", "double", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "stddev");
    assertTrue(Math.abs(longResult-longTest)<.00000000001);
    
    //Float
    Double floatResult = (Double)getStatResult(response, "str", "double", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "stddev");
    assertTrue("Oops: (double raws) " + Double.doubleToRawLongBits(floatResult) + " - "
        + Double.doubleToRawLongBits(floatTest) + " < " + Double.doubleToRawLongBits(.00000000001) +
        " Calculated diff " + Double.doubleToRawLongBits(floatResult - floatTest)
        + " Let's see what the JVM thinks these bits are. FloatResult:  " + floatResult.toString() +
        " floatTest: " + floatTest.toString() + " Diff " + Double.toString(floatResult - floatTest),
        Math.abs(floatResult - floatTest) < .00000000001);


    //Double
    Double doubleResult = (Double)getStatResult(response, "str", "double", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "stddev");
    assertTrue(Math.abs(doubleResult-doubleTest)<.00000000001);
  }
  
  @Test
  public void medianTest() throws Exception { 
    //Int
    Double intResult = (Double)getStatResult(response, "medr", "double", "int_id");
    Double intTest = (Double)calculateNumberStat(intTestStart, "median");
    assertEquals(intResult,intTest);
    
    //Long
    Double longResult = (Double)getStatResult(response, "medr", "double", "long_ld");
    Double longTest = (Double)calculateNumberStat(longTestStart, "median");
    assertEquals(longResult,longTest);
    
    //Float
    Double floatResult = (Double)getStatResult(response, "medr", "double", "float_fd");
    Double floatTest = (Double)calculateNumberStat(floatTestStart, "median");
    assertEquals(floatResult,floatTest);
    
    //Double
    Double doubleResult = (Double)getStatResult(response, "medr", "double", "double_dd");
    Double doubleTest = (Double)calculateNumberStat(doubleTestStart, "median");
    assertEquals(doubleResult,doubleTest);
  }
  
  @Test
  public void perc20Test() throws Exception {
    //Int 20
    Integer intResult = (Integer)getStatResult(response, "p2r", "int", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "perc_20");
    assertEquals(intResult,intTest);

    //Long 20
    Long longResult = (Long)getStatResult(response, "p2r", "long", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "perc_20");
    assertEquals(longResult,longTest);

    //Float 20
    Float floatResult = (Float)getStatResult(response, "p2r", "float", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "perc_20");
    assertEquals(floatResult,floatTest);

    //Double 20
    Double doubleResult = (Double)getStatResult(response, "p2r", "double", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "perc_20");
    assertEquals(doubleResult,doubleTest);

    //Date 20
    String dateResult = (String)getStatResult(response, "p2r", "date", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "perc_20");
    assertEquals(dateResult,dateTest);

    //String 20
    String stringResult = (String)getStatResult(response, "p2r", "str", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "perc_20");
    assertEquals(stringResult,stringTest);
  }
  
  @Test
  public void perc60Test() throws Exception { 
    //Int 60
    Integer intResult = (Integer)getStatResult(response, "p6r", "int", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "perc_60");
    assertEquals(intResult,intTest);

    //Long 60
    Long longResult = (Long)getStatResult(response, "p6r", "long", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "perc_60");
    assertEquals(longResult,longTest);

    //Float 60
    Float floatResult = (Float)getStatResult(response, "p6r", "float", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "perc_60");
    assertEquals(floatResult,floatTest);

    //Double 60
    Double doubleResult = (Double)getStatResult(response, "p6r", "double", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "perc_60");
    assertEquals(doubleResult,doubleTest);

    //Date 60
    String dateResult = (String)getStatResult(response, "p6r", "date", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "perc_60");
    assertEquals(dateResult,dateTest);

    //String 60
    String stringResult = (String)getStatResult(response, "p6r", "str", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "perc_60");
    assertEquals(stringResult,stringTest);
  }
  
  @Test
  public void minTest() throws Exception { 
    //Int
    Integer intResult = (Integer)getStatResult(response, "mir", "int", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "min");
    assertEquals(intResult,intTest);

    //Long
    Long longResult = (Long)getStatResult(response, "mir", "long", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "min");
    assertEquals(longResult,longTest);

    //Float
    Float floatResult = (Float)getStatResult(response, "mir", "float", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "min");
    assertEquals(floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult(response, "mir", "double", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "min");
    assertEquals(doubleResult,doubleTest);

    //Date
    String dateResult = (String)getStatResult(response, "mir", "date", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "min");
    assertEquals(dateResult,dateTest);

    //String
    String stringResult = (String)getStatResult(response, "mir", "str", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "min");
    assertEquals(stringResult,stringTest);
  }
  
  @Test
  public void maxTest() throws Exception { 
    //Int
    Integer intResult = (Integer)getStatResult(response, "mar", "int", "int_id");
    Integer intTest = (Integer)calculateStat(intTestStart, "max");
    assertEquals(intResult,intTest);

    //Long
    Long longResult = (Long)getStatResult(response, "mar", "long", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "max");
    assertEquals(longResult,longTest);

    //Float
    Float floatResult = (Float)getStatResult(response, "mar", "float", "float_fd");
    Float floatTest = (Float)calculateStat(floatTestStart, "max");
    assertEquals(floatResult,floatTest);

    //Double
    Double doubleResult = (Double)getStatResult(response, "mar", "double", "double_dd");
    Double doubleTest = (Double)calculateStat(doubleTestStart, "max");
    assertEquals(doubleResult,doubleTest);

    //Date
    String dateResult = (String)getStatResult(response, "mar", "date", "date_dtd");
    String dateTest = (String)calculateStat(dateTestStart, "max");
    assertEquals(dateResult,dateTest);

    //String
    String stringResult = (String)getStatResult(response, "mar", "str", "string_sd");
    String stringTest = (String)calculateStat(stringTestStart, "max");
    assertEquals(stringResult,stringTest);
  }
  
  @Test
  public void uniqueTest() throws Exception { 
    //Int
    Long intResult = (Long)getStatResult(response, "ur", "long", "int_id");
    Long intTest = (Long)calculateStat(intTestStart, "unique");
    assertEquals(intResult,intTest);

    //Long
    Long longResult = (Long)getStatResult(response, "ur", "long", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "unique");
    assertEquals(longResult,longTest);

    //Float
    Long floatResult = (Long)getStatResult(response, "ur", "long", "float_fd");
    Long floatTest = (Long)calculateStat(floatTestStart, "unique");
    assertEquals(floatResult,floatTest);

    //Double
    Long doubleResult = (Long)getStatResult(response, "ur", "long", "double_dd");
    Long doubleTest = (Long)calculateStat(doubleTestStart, "unique");
    assertEquals(doubleResult,doubleTest);

    //Date
    Long dateResult = (Long)getStatResult(response, "ur", "long", "date_dtd");
    Long dateTest = (Long)calculateStat(dateTestStart, "unique");
    assertEquals(dateResult,dateTest);

    //String
    Long stringResult = (Long)getStatResult(response, "ur", "long", "string_sd");
    Long stringTest = (Long)calculateStat(stringTestStart, "unique");
    assertEquals(stringResult,stringTest);
  }
  
  @Test
  public void countTest() throws Exception { 
    //Int
    Long intResult = (Long)getStatResult(response, "cr", "long", "int_id");
    Long intTest = (Long)calculateStat(intTestStart, "count");
    assertEquals(intResult,intTest);

    //Long
    Long longResult = (Long)getStatResult(response, "cr", "long", "long_ld");
    Long longTest = (Long)calculateStat(longTestStart, "count");
    assertEquals(longResult,longTest);

    //Float
    Long floatResult = (Long)getStatResult(response, "cr", "long", "float_fd");
    Long floatTest = (Long)calculateStat(floatTestStart, "count");
    assertEquals(floatResult,floatTest);

    //Double
    Long doubleResult = (Long)getStatResult(response, "cr", "long", "double_dd");
    Long doubleTest = (Long)calculateStat(doubleTestStart, "count");
    assertEquals(doubleResult,doubleTest);

    //Date
    Long dateResult = (Long)getStatResult(response, "cr", "long", "date_dtd");
    Long dateTest = (Long)calculateStat(dateTestStart, "count");
    assertEquals(dateResult,dateTest);

    //String
    Long stringResult = (Long)getStatResult(response, "cr", "long", "string_sd");
    Long stringTest = (Long)calculateStat(stringTestStart, "count");
    assertEquals(stringResult,stringTest);
  }  
    
  @Test
  public void missingDefaultTest() throws Exception { 
    //Int
    long intResult = (Long)getStatResult(response, "misr", "long", "int_id");
    assertEquals(intMissing,intResult);

    //Long
    long longResult = (Long)getStatResult(response, "misr", "long", "long_ld");
    assertEquals(longMissing,longResult);

    //Float
    long floatResult = (Long)getStatResult(response, "misr", "long", "float_fd");
    assertEquals(floatMissing,floatResult);

    //Double
    long doubleResult = (Long)getStatResult(response, "misr", "long", "double_dd");
    assertEquals(doubleMissing,doubleResult);

    //Date
    long dateResult = (Long)getStatResult(response, "misr", "long", "date_dtd");
    assertEquals(dateMissing,dateResult);

    //String
    long stringResult = (Long)getStatResult(response, "misr", "long", "string_sd");
    assertEquals(stringMissing, stringResult);
  }

}
