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

package org.apache.solr.analytics.expression;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ObjectArrays;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class ExpressionTest extends SolrTestCaseJ4 {
  static String fileName = "core/src/test-files/analytics/requestFiles/expressions.txt";
  
  protected static final String[] BASEPARMS = new String[]{ "q", "*:*", "indent", "true", "stats", "true", "olap", "true", "rows", "0" };
  protected static final HashMap<String,Object> defaults = new HashMap<String,Object>();

  static public final int INT = 71;
  static public final int LONG = 36;
  static public final int FLOAT = 93;
  static public final int DOUBLE = 49;
  static public final int DATE = 12;
  static public final int STRING = 28;
  static public final int NUM_LOOPS = 100;
  
  static String response;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");
    
    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      String dt = (1800+j%DATE) + "-12-31T23:59:59Z";
      String s = "str" + (j%STRING);
      assertU(adoc("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f, 
            "double_dd", "" + d,  "date_dtd", dt, "string_sd", s));
      
      if (usually()) {
        commit(); // to have several segments
      }
    }
    
    assertU(commit()); 
    
    //Sort ascending tests
    response = h.query(request(fileToStringArr(fileName)));
  }
      
  @Test
  public void addTest() throws Exception { 
    double sumResult = (Double)getStatResult(response, "ar", "double", "sum");
    double uniqueResult = ((Long)getStatResult(response, "ar", "long", "unique")).doubleValue();
    double result = (Double)getStatResult(response, "ar", "double", "su");
    assertTrue(sumResult+uniqueResult==result);

    double meanResult = (Double)getStatResult(response, "ar", "double", "mean");
    double medianResult = (Double)getStatResult(response, "ar", "double", "median");
    double countResult = ((Long)getStatResult(response, "ar", "long", "count")).doubleValue();
    result = (Double)getStatResult(response, "ar", "double", "mcm");
    assertTrue(meanResult+countResult+medianResult==result);
  }
  
  @Test
  public void multiplyTest() throws Exception { 
    double sumResult = (Double)getStatResult(response, "mr", "double", "sum");
    double uniqueResult = ((Long)getStatResult(response, "mr", "long", "unique")).doubleValue();
    double result = (Double)getStatResult(response, "mr", "double", "su");
    assertTrue(sumResult*uniqueResult==result);
    
    double meanResult = (Double)getStatResult(response, "mr", "double", "mean");
    double medianResult = (Double)getStatResult(response, "mr", "double", "median");
    double countResult = ((Long)getStatResult(response, "mr", "long", "count")).doubleValue();
    result = (Double)getStatResult(response, "mr", "double", "mcm");
    assertTrue(meanResult*countResult*medianResult==result);
  }
  
  @Test
  public void divideTest() throws Exception { 
    double sumResult = (Double)getStatResult(response, "dr", "double", "sum");
    double uniqueResult = ((Long)getStatResult(response, "dr", "long", "unique")).doubleValue();
    double result = (Double)getStatResult(response, "dr", "double", "su");
    assertTrue(sumResult/uniqueResult==result);
    
    double meanResult = (Double)getStatResult(response, "dr", "double", "mean");
    double countResult = ((Long)getStatResult(response, "dr", "long", "count")).doubleValue();
    result = (Double)getStatResult(response, "dr", "double", "mc");
    assertTrue(meanResult/countResult==result);
  }
  
  @Test
  public void powerTest() throws Exception { 
    double sumResult = (Double)getStatResult(response, "pr", "double", "sum");
    double uniqueResult = ((Long)getStatResult(response, "pr", "long", "unique")).doubleValue();
    double result = (Double)getStatResult(response, "pr", "double", "su");
    assertTrue(Math.pow(sumResult,uniqueResult)==result);
    
    double meanResult = (Double)getStatResult(response, "pr", "double", "mean");
    double countResult = ((Long)getStatResult(response, "pr", "long", "count")).doubleValue();
    result = (Double)getStatResult(response, "pr", "double", "mc");
    assertTrue(Math.pow(meanResult,countResult)==result);
  }
  
  @Test
  public void negateTest() throws Exception { 
    double sumResult = (Double)getStatResult(response, "nr", "double", "sum");
    double result = (Double)getStatResult(response, "nr", "double", "s");
    assertTrue(-1*sumResult==result);

    double countResult = ((Long)getStatResult(response, "nr", "long", "count")).doubleValue();
    result = (Double)getStatResult(response, "nr", "double", "c");
    assertTrue(-1*countResult==result);
  }
  
  @Test
  public void absoluteValueTest() throws Exception { 
    double sumResult = (Double)getStatResult(response, "avr", "double", "sum");
    double result = (Double)getStatResult(response, "avr", "double", "s");
    assertTrue(sumResult==result);

    double countResult = ((Long)getStatResult(response, "avr", "long", "count")).doubleValue();
    result = (Double)getStatResult(response, "avr", "double", "c");
    assertTrue(countResult==result);
  }
  
  @Test
  public void constantNumberTest() throws Exception { 
    double result = (Double)getStatResult(response, "cnr", "double", "c8");
    assertTrue(8==result);

    result = (Double)getStatResult(response, "cnr", "double", "c10");
    assertTrue(10==result);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void dateMathTest() throws Exception { 
    String math = (String)getStatResult(response, "dmr", "str", "cme");
    DateMathParser date = new DateMathParser();
    date.setNow(TrieDateField.parseDate((String)getStatResult(response, "dmr", "date", "median")));
    String dateMath = (String)getStatResult(response, "dmr", "date", "dmme");
    assertTrue(TrieDateField.parseDate(dateMath).equals(date.parseMath(math)));
    
    math = (String)getStatResult(response, "dmr", "str", "cma");
    date = new DateMathParser();
    date.setNow(TrieDateField.parseDate((String)getStatResult(response, "dmr", "date", "max")));
    dateMath = (String)getStatResult(response, "dmr", "date", "dmma");
    assertTrue(TrieDateField.parseDate(dateMath).equals(date.parseMath(math)));
  }
  
  @Test
  public void constantDateTest() throws Exception { 
    String date = (String)getStatResult(response, "cdr", "date", "cd1");
    String str = (String)getStatResult(response, "cdr", "str", "cs1");
    assertTrue(date.equals(str));
    
    date = (String)getStatResult(response, "cdr", "date", "cd2");
    str = (String)getStatResult(response, "cdr", "str", "cs2");
    assertTrue(date.equals(str));
  }
  
  @Test
  public void constantStringTest() throws Exception { 
    String str = (String)getStatResult(response, "csr", "str", "cs1");
    assertTrue(str.equals("this is the first"));

    str = (String)getStatResult(response, "csr", "str", "cs2");
    assertTrue(str.equals("this is the second"));

    str = (String)getStatResult(response, "csr", "str", "cs3");
    assertTrue(str.equals("this is the third"));
  }
  
  @Test
  public void concatenateTest() throws Exception { 
    StringBuilder builder = new StringBuilder();
    builder.append((String)getStatResult(response, "cr", "str", "csmin"));
    builder.append((String)getStatResult(response, "cr", "str", "min"));
    String concat = (String)getStatResult(response, "cr", "str", "ccmin");
    assertTrue(concat.equals(builder.toString()));

    builder.setLength(0);
    builder.append((String)getStatResult(response, "cr", "str", "csmax"));
    builder.append((String)getStatResult(response, "cr", "str", "max"));
    concat = (String)getStatResult(response, "cr", "str", "ccmax");
    assertTrue(concat.equals(builder.toString()));
  }
  
  @Test
  public void reverseTest() throws Exception { 
    StringBuilder builder = new StringBuilder();
    builder.append((String)getStatResult(response, "rr", "str", "min"));
    String rev = (String)getStatResult(response, "rr", "str", "rmin");
    assertTrue(rev.equals(builder.reverse().toString()));

    builder.setLength(0);
    builder.append((String)getStatResult(response, "rr", "str", "max"));
    rev = (String)getStatResult(response, "rr", "str", "rmax");
    assertTrue(rev.equals(builder.reverse().toString()));
  }
  
  public Object getStatResult(String response, String request, String type, String name) {
    String cat = "\n  <lst name=\""+request+"\">";
    String begin = "<"+type+" name=\""+name+"\">";
    String end = "</"+type+">";
    int beginInt = response.indexOf(begin, response.indexOf(cat))+begin.length();
    int endInt = response.indexOf(end, beginInt);
    String resultStr = response.substring(beginInt, endInt);
    if (type.equals("double")) {
      return Double.parseDouble(resultStr);
    } else if (type.equals("int")) {
      return Integer.parseInt(resultStr);
    } else if (type.equals("long")) {
      return Long.parseLong(resultStr);
    } else if (type.equals("float")) {
      return Float.parseFloat(resultStr);
    } else {
      return resultStr;
    }
  }
  
  public static SolrQueryRequest request(String...args){
    return SolrTestCaseJ4.req( ObjectArrays.concat(BASEPARMS, args,String.class) );
  }
  
  public static String[] fileToStringArr(String fileName) throws FileNotFoundException {
    Scanner file = new Scanner(new File(ExternalPaths.SOURCE_HOME, fileName), "UTF-8");
    ArrayList<String> strList = new ArrayList<String>();
    while (file.hasNextLine()) {
      String line = file.nextLine();
      if (line.length()<2) {
        continue;
      }
      String[] param = line.split("=");
      strList.add(param[0]);
      strList.add(param[1]);
    }
    return strList.toArray(new String[0]);
  }
}
