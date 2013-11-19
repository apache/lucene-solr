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

package org.apache.solr.analytics.facet;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class FieldFacetExtrasTest extends AbstractAnalyticsFacetTest {
  static String fileName = "core/src/test-files/analytics/requestFiles/fieldFacetExtras.txt";

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
  
  static String response;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    //INT
    intLongTestStart = new ArrayList<ArrayList<Integer>>(); 
    intFloatTestStart = new ArrayList<ArrayList<Integer>>(); 
    intDoubleTestStart = new ArrayList<ArrayList<Integer>>(); 
    intStringTestStart = new ArrayList<ArrayList<Integer>>(); 

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
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(i);
        intLongTestStart.add(list1);
      } else {
        intLongTestStart.get((int)l).add(i);
      }
      //String
      if (j-FLOAT<0) {
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(i);
        intFloatTestStart.add(list1);
      } else {
        intFloatTestStart.get((int)f).add(i);
      }
      //String
      if (j-DOUBLE<0) {
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(i);
        intDoubleTestStart.add(list1);
      } else {
        intDoubleTestStart.get((int)d).add(i);
      }
      //String
      if (j-STRING<0) {
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(i);
        intStringTestStart.add(list1);
      } else {
        intStringTestStart.get(s).add(i);
      }
      
      if (usually()) {
        commit(); // to have several segments
      }
    }
    
    assertU(commit()); 
    response = h.query(request(fileToStringArr(fileName)));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void limitTest() throws Exception { 

    String longLimit = getFacetXML(response, "lr", "fieldFacets", "long_ld");
    Collection<Double> lon = (ArrayList<Double>)xmlToList(longLimit, "double", "mean");
    assertEquals(lon.size(),5);
    String floatLimit = getFacetXML(response, "lr", "fieldFacets", "float_fd");
    Collection<Double> flo = (ArrayList<Double>)xmlToList(floatLimit, "double", "median");
    assertEquals(flo.size(),3);
    String doubleLimit = getFacetXML(response, "lr", "fieldFacets", "double_dd");
    Collection<Long> doub = (ArrayList<Long>)xmlToList(doubleLimit, "long", "count");
    assertEquals(doub.size(),7);
    String stringLimit = getFacetXML(response, "lr", "fieldFacets", "string_sd");   
    Collection<Integer> string = (ArrayList<Integer>)xmlToList(stringLimit, "int", "percentile_20");
    assertEquals(string.size(),1);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void offsetTest() throws Exception { 

    String xml;
    Collection<Double> lon;
   
    List<Double> all = new ArrayList<Double>();
    xml = getFacetXML(response, "off0", "fieldFacets", "long_ld");
    lon = (ArrayList<Double>)xmlToList(xml, "double", "mean");
    assertEquals(lon.size(),2);
    assertArrayEquals(new Double[]{ 1.5,  2.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);
    
    xml = getFacetXML(response, "off1", "fieldFacets", "long_ld");
    lon = (ArrayList<Double>)xmlToList(xml, "double", "mean");
    assertEquals(lon.size(),2);
    assertArrayEquals(new Double[]{ 3.0,  4.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);
    
    xml = getFacetXML(response, "off2", "fieldFacets", "long_ld");
    lon = (ArrayList<Double>)xmlToList(xml, "double", "mean");
    assertEquals(lon.size(),3);
    assertArrayEquals(new Double[]{ 5.0,  5.75, 6.0 }, lon.toArray(new Double[0]));
    all.addAll(lon);
    
    xml = getFacetXML(response, "offAll", "fieldFacets", "long_ld");
    lon = (ArrayList<Double>)xmlToList(xml, "double", "mean");
    assertEquals(lon.size(),7);
    assertArrayEquals(all.toArray(new Double[0]), lon.toArray(new Double[0]));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void sortTest() throws Exception { 
    String longSort = getFacetXML(response, "sr", "fieldFacets", "long_ld");
    Collection<Double> lon = (ArrayList<Double>)xmlToList(longSort, "double", "mean");
    ArrayList<Double> longTest = calculateNumberStat(intLongTestStart, "mean");
    Collections.sort(longTest);
    assertEquals(longTest,lon);
    
    String floatSort = getFacetXML(response, "sr", "fieldFacets", "float_fd");
    Collection<Double> flo = (ArrayList<Double>)xmlToList(floatSort, "double", "median");
    ArrayList<Double> floatTest = calculateNumberStat(intFloatTestStart, "median");
    Collections.sort(floatTest,Collections.reverseOrder());
    assertEquals(floatTest,flo);
    
    String doubleSort = getFacetXML(response, "sr", "fieldFacets", "double_dd");
    Collection<Long> doub = (ArrayList<Long>)xmlToList(doubleSort, "long", "count");
    ArrayList<Long> doubleTest = (ArrayList<Long>)calculateStat(intDoubleTestStart, "count");
    Collections.sort(doubleTest);
    assertEquals(doubleTest,doub);
    
    String stringSort = getFacetXML(response, "sr", "fieldFacets", "string_sd");   
    Collection<Integer> string = (ArrayList<Integer>)xmlToList(stringSort, "int", "percentile_20");
    ArrayList<Integer> stringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "perc_20");
    Collections.sort(stringTest,Collections.reverseOrder());
    assertEquals(stringTest,string);
  }

}
