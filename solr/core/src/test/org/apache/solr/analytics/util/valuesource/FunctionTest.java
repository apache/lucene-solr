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

package org.apache.solr.analytics.util.valuesource;


import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.analytics.AbstractAnalyticsStatsTest;
import org.apache.solr.analytics.facet.AbstractAnalyticsFacetTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class FunctionTest extends AbstractAnalyticsStatsTest {
  static String fileName = "core/src/test-files/analytics/requestFiles/functions.txt";

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
      int i = j%INT+1;
      long l = j%LONG+1;
      float f = j%FLOAT+1;
      double d = j%DOUBLE+1;
      double d0 = j%DOUBLE;
      String dt = (1800+j%DATE) + "-06-30T23:59:59Z";
      String s = "str" + (j%STRING);

      double add_if = (double)i+f;
      double add_ldf = (double)l+d+f;
      double mult_if = (double)i*f;
      double mult_ldf = (double)l*d*f;
      double div_if = (double)i/f;
      double div_ld = (double)l/d;
      double pow_if = Math.pow(i,f);
      double pow_ld = Math.pow(l,d);
      double neg_i = (double)i*-1;
      double neg_l = (double)l*-1;
      String dm_2y = (1802+j%DATE) + "-06-30T23:59:59Z";
      String dm_2m = (1800+j%DATE) + "-08-30T23:59:59Z";
      String concat_first = "this is the first"+s;
      String concat_second = "this is the second"+s;
      String rev = new StringBuilder(s).reverse().toString();
      
      assertU(adoc(AbstractAnalyticsFacetTest.filter("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f, 
            "double_dd", "" + d,  "date_dtd", dt, "string_sd", s,
            "add_if_dd", ""+add_if, "add_ldf_dd", ""+add_ldf, "mult_if_dd", ""+mult_if, "mult_ldf_dd", ""+mult_ldf,
            "div_if_dd", ""+div_if, "div_ld_dd", ""+div_ld, "pow_if_dd", ""+pow_if, "pow_ld_dd", ""+pow_ld,
            "neg_i_dd", ""+neg_i, "neg_l_dd", ""+neg_l, "const_8_dd", "8", "const_10_dd", "10", "dm_2y_dtd", dm_2y, "dm_2m_dtd", dm_2m,
            "const_00_dtd", "1800-06-30T23:59:59Z", "const_04_dtd", "1804-06-30T23:59:59Z", "const_first_sd", "this is the first", "const_second_sd", "this is the second",
            "concat_first_sd", concat_first, "concat_second_sd", concat_second, "rev_sd", rev, "miss_dd", ""+d0 )));
      
      
      if (usually()) {
        commit(); // to have several segments
      }
    }
    
    assertU(commit()); 
    
    response = h.query(request(fileToStringArr(fileName)));
  }
      
  @Test
  public void addTest() throws Exception { 
    double result = (Double)getStatResult(response, "ar", "double", "sum");
    double calculated = (Double)getStatResult(response, "ar", "double", "sumc");
    assertTrue(result==calculated);

    result = (Double)getStatResult(response, "ar", "double", "mean");
    calculated = (Double)getStatResult(response, "ar", "double", "meanc");
    assertTrue(result==calculated);
  }
  
  @Test
  public void multiplyTest() throws Exception { 
    double result = (Double)getStatResult(response, "mr", "double", "sum");
    double calculated = (Double)getStatResult(response, "mr", "double", "sumc");
    assertTrue(result==calculated);
    
    result = (Double)getStatResult(response, "mr", "double", "mean");
    calculated = (Double)getStatResult(response, "mr", "double", "meanc");
    assertTrue(result==calculated);
  }
  
  @Test
  public void divideTest() throws Exception { 
    Double result = (Double)getStatResult(response, "dr", "double", "sum");
    Double calculated = (Double)getStatResult(response, "dr", "double", "sumc");
    assertTrue(result.equals(calculated));
    
    result = (Double)getStatResult(response, "dr", "double", "mean");
    calculated = (Double)getStatResult(response, "dr", "double", "meanc");
    assertTrue(result.equals(calculated));
  }
  
  @Test
  public void powerTest() throws Exception { 
    double result = (Double)getStatResult(response, "pr", "double", "sum");
    double calculated = (Double)getStatResult(response, "pr", "double", "sumc");
    assertTrue(result==calculated);
    
    result = (Double)getStatResult(response, "pr", "double", "mean");
    calculated = (Double)getStatResult(response, "pr", "double", "meanc");
    assertTrue(result==calculated);
  }
  
  @Test
  public void negateTest() throws Exception { 
    double result = (Double)getStatResult(response, "nr", "double", "sum");
    double calculated = (Double)getStatResult(response, "nr", "double", "sumc");
    assertTrue(result==calculated);
    
    result = (Double)getStatResult(response, "nr", "double", "mean");
    calculated = (Double)getStatResult(response, "nr", "double", "meanc");
    assertTrue(result==calculated);
  }
  
  @Test
  public void absoluteValueTest() throws Exception { 
    double result = (Double)getStatResult(response, "avr", "double", "sum");
    double calculated = (Double)getStatResult(response, "avr", "double", "sumc");
    assertTrue(result==calculated);
    
    result = (Double)getStatResult(response, "avr", "double", "mean");
    calculated = (Double)getStatResult(response, "avr", "double", "meanc");
    assertTrue(result==calculated);
  }
  
  @Test
  public void constantNumberTest() throws Exception { 
    double result = (Double)getStatResult(response, "cnr", "double", "sum");
    double calculated = (Double)getStatResult(response, "cnr", "double", "sumc");
    assertTrue(result==calculated);
    
    result = (Double)getStatResult(response, "cnr", "double", "mean");
    calculated = (Double)getStatResult(response, "cnr", "double", "meanc");
    assertTrue(result==calculated);
  }
  
  @Test
  public void dateMathTest() throws Exception { 
    String result = (String)getStatResult(response, "dmr", "date", "median");
    String calculated = (String)getStatResult(response, "dmr", "date", "medianc");
    assertTrue(result.equals(calculated));
    
    result = (String)getStatResult(response, "dmr", "date", "max");
    calculated = (String)getStatResult(response, "dmr", "date", "maxc");
    assertTrue(result.equals(calculated));
  }
  
  @Test
  public void constantDateTest() throws Exception { 
    String result = (String)getStatResult(response, "cdr", "date", "median");
    String calculated = (String)getStatResult(response, "cdr", "date", "medianc");
    assertTrue(result.equals(calculated));
    
    result = (String)getStatResult(response, "cdr", "date", "max");
    calculated = (String)getStatResult(response, "cdr", "date", "maxc");
    assertTrue(result.equals(calculated));
  }
  
  @Test
  public void constantStringTest() throws Exception { 
    String result = (String)getStatResult(response, "csr", "str", "min");
    String calculated = (String)getStatResult(response, "csr", "str", "minc");
    assertTrue(result.equals(calculated));
    
    result = (String)getStatResult(response, "csr", "str", "max");
    calculated = (String)getStatResult(response, "csr", "str", "maxc");
    assertTrue(result.equals(calculated));
  }
  
  @Test
  public void concatenateTest() throws Exception { 
    String result = (String)getStatResult(response, "cr", "str", "min");
    String calculated = (String)getStatResult(response, "cr", "str", "minc");
    assertTrue(result.equals(calculated));
    
    result = (String)getStatResult(response, "cr", "str", "max");
    calculated = (String)getStatResult(response, "cr", "str", "maxc");
    assertTrue(result.equals(calculated));
  }
  
  @Test
  public void reverseTest() throws Exception { 
    String result = (String)getStatResult(response, "rr", "str", "min");
    String calculated = (String)getStatResult(response, "rr", "str", "minc");
    assertTrue(result.equals(calculated));
    
    result = (String)getStatResult(response, "rr", "str", "max");
    calculated = (String)getStatResult(response, "rr", "str", "maxc");
    assertTrue(result.equals(calculated));
  }
  
  @Test
  public void missingTest() throws Exception { 
    double min = (Double)getStatResult(response, "ms", "double", "min");
    double max = (Double)getStatResult(response, "ms", "double", "max");
    Assert.assertEquals((Double)48.0,(Double)max);
    Assert.assertEquals((Double)1.0,(Double)min);
  }

}
