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
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class FieldFacetTest extends AbstractAnalyticsFacetTest{
  static String fileName = "core/src/test-files/analytics/requestFiles/fieldFacets.txt";

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
  
  static String response;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");
    
    defaults.put("int", new Integer(0));
    defaults.put("long", new Long(0));
    defaults.put("float", new Float(0));
    defaults.put("double", new Double(0));
    defaults.put("date", "1800-12-31T23:59:59Z");
    defaults.put("string", "str0");

    //INT
    intDateTestStart = new ArrayList<ArrayList<Integer>>(); 
    intDateTestMissing = new ArrayList<Long>(); 
    intStringTestStart = new ArrayList<ArrayList<Integer>>(); 
    intStringTestMissing = new ArrayList<Long>(); 
    
    //LONG
    longDateTestStart = new ArrayList<ArrayList<Long>>(); 
    longDateTestMissing = new ArrayList<Long>(); 
    longStringTestStart = new ArrayList<ArrayList<Long>>(); 
    longStringTestMissing = new ArrayList<Long>(); 
    
    //FLOAT
    floatDateTestStart = new ArrayList<ArrayList<Float>>(); 
    floatDateTestMissing = new ArrayList<Long>(); 
    floatStringTestStart = new ArrayList<ArrayList<Float>>(); 
    floatStringTestMissing = new ArrayList<Long>(); 
    
    //DOUBLE
    doubleDateTestStart = new ArrayList<ArrayList<Double>>(); 
    doubleDateTestMissing = new ArrayList<Long>(); 
    doubleStringTestStart = new ArrayList<ArrayList<Double>>(); 
    doubleStringTestMissing = new ArrayList<Long>(); 
    
    //DATE
    dateIntTestStart = new ArrayList<ArrayList<String>>(); 
    dateIntTestMissing = new ArrayList<Long>(); 
    dateLongTestStart = new ArrayList<ArrayList<String>>(); 
    dateLongTestMissing = new ArrayList<Long>(); 
    
    //String
    stringIntTestStart = new ArrayList<ArrayList<String>>(); 
    stringIntTestMissing = new ArrayList<Long>(); 
    stringLongTestStart = new ArrayList<ArrayList<String>>(); 
    stringLongTestMissing = new ArrayList<Long>(); 
    
    //Multi-Valued
    multiLongTestStart = new ArrayList<ArrayList<Integer>>(); 
    multiLongTestMissing = new ArrayList<Long>(); 
    multiStringTestStart = new ArrayList<ArrayList<Integer>>(); 
    multiStringTestMissing = new ArrayList<Long>(); 
    multiDateTestStart = new ArrayList<ArrayList<Integer>>(); 
    multiDateTestMissing = new ArrayList<Long>(); 

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
      if (dt==0 && dtm == 0) {
        assertU(adoc(filter("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f, 
          "double_dd", "" + d,  "date_dtd", (1800+dt) + "-12-31T23:59:59Z", "string_sd", "str" + s,
          "long_ldm", "" + l, "long_ldm", ""+lm, "string_sdm", "str" + s, "string_sdm", "str"+sm)));
      } else if (dt == 0) {
        assertU(adoc(filter("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f, 
            "double_dd", "" + d,  "date_dtd", (1800+dt) + "-12-31T23:59:59Z", "string_sd", "str" + s,
            "long_ldm", "" + l, "long_ldm", ""+lm, "string_sdm", "str" + s, "string_sdm", "str"+sm,
            "date_dtdm", (1800+dtm) + "-12-31T23:59:59Z")));
      } else if (dtm == 0) {
        assertU(adoc(filter("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f, 
            "double_dd", "" + d,  "date_dtd", (1800+dt) + "-12-31T23:59:59Z", "string_sd", "str" + s,
            "long_ldm", "" + l, "long_ldm", ""+lm, "string_sdm", "str" + s, "string_sdm", "str"+sm,
            "date_dtdm", (1800+dt) + "-12-31T23:59:59Z")));
      } else {
        assertU(adoc(filter("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f, 
            "double_dd", "" + d,  "date_dtd", (1800+dt) + "-12-31T23:59:59Z", "string_sd", "str" + s,
            "long_ldm", "" + l, "long_ldm", ""+lm, "string_sdm", "str" + s, "string_sdm", "str"+sm,
            "date_dtdm", (1800+dt) + "-12-31T23:59:59Z", "date_dtdm", (1800+dtm) + "-12-31T23:59:59Z")));
      }
      
      if( dt != 0 ){
        //Dates
        if (j-DATE<0) {
          ArrayList<Integer> list1 = new ArrayList<Integer>();
          if( i != 0 ){
            list1.add(i);
            intDateTestMissing.add(0l);
          } else {
            intDateTestMissing.add(1l);
          }
          intDateTestStart.add(list1);
          ArrayList<Long> list2 = new ArrayList<Long>();
          if( l != 0l ){
            list2.add(l);
            longDateTestMissing.add(0l);
          } else {
            longDateTestMissing.add(1l);
          }
          longDateTestStart.add(list2);
          ArrayList<Float> list3 = new ArrayList<Float>();
          if ( f != 0.0f ){
            list3.add(f);
            floatDateTestMissing.add(0l);
          } else {
            floatDateTestMissing.add(1l);
            
          }
          floatDateTestStart.add(list3);
          ArrayList<Double> list4 = new ArrayList<Double>();
          if( d != 0.0d ){
            list4.add(d);
            doubleDateTestMissing.add(0l);
          } else {
            doubleDateTestMissing.add(1l);
          }
          doubleDateTestStart.add(list4);
          ArrayList<Integer> list5 = new ArrayList<Integer>();
          if( i != 0 ){
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
      
      if (j-DATEM<0 && dtm!=dt && dtm!=0) {
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        if( i != 0 ){
          list1.add(i);
          multiDateTestMissing.add(0l);
        } else {
          multiDateTestMissing.add(1l);
        }
        multiDateTestStart.add(list1);
      } else if (dtm!=dt && dtm!=0) {
        if( i != 0 ) multiDateTestStart.get(dtm-1).add(i);
      }
      
      if( s != 0 ){
        //Strings
        if (j-STRING<0) {
          ArrayList<Integer> list1 = new ArrayList<Integer>();
          if( i != 0 ){
            list1.add(i);
            intStringTestMissing.add(0l);
          } else {
            intStringTestMissing.add(1l);
          }
          intStringTestStart.add(list1);
          ArrayList<Long> list2 = new ArrayList<Long>();
          if( l != 0l ){
            list2.add(l);
            longStringTestMissing.add(0l);
          } else {
            longStringTestMissing.add(1l);
          }
          longStringTestStart.add(list2);
          ArrayList<Float> list3 = new ArrayList<Float>();
          if( f != 0.0f ){
            list3.add(f);
            floatStringTestMissing.add(0l);
          } else {
            floatStringTestMissing.add(1l);
          }
          floatStringTestStart.add(list3);
          ArrayList<Double> list4 = new ArrayList<Double>();
          if( d != 0.0d ){
            list4.add(d);
            doubleStringTestMissing.add(0l);
          } else {
            doubleStringTestMissing.add(1l);
          }
          doubleStringTestStart.add(list4);
          ArrayList<Integer> list5 = new ArrayList<Integer>();
          if( i != 0 ){
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
        if (j-STRINGM<0&&sm!=s) {
          ArrayList<Integer> list1 = new ArrayList<Integer>();
          if( i != 0 ){
            list1.add(i);
            multiStringTestMissing.add(0l);
          } else {
            multiStringTestMissing.add(1l);
          }
          multiStringTestStart.add(list1);
        } else if (sm!=s) {
          if( i != 0 ) multiStringTestStart.get(sm-1).add(i); else increment(multiStringTestMissing,sm-1);
        }
      }
      
      //Int
      if( i != 0 ){
        if (j-INT<0) {
          ArrayList<String> list1 = new ArrayList<String>();
          if( dt != 0 ){
            list1.add((1800+dt) + "-12-31T23:59:59Z");
            dateIntTestMissing.add(0l);
          } else {
            dateIntTestMissing.add(1l);
          }
          dateIntTestStart.add(list1);
          ArrayList<String> list2 = new ArrayList<String>();
          if( s != 0 ){
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
      if( l != 0 ){
        if (j-LONG<0) {
          ArrayList<String> list1 = new ArrayList<String>();
          if( dt != 0 ){
            list1.add((1800+dt) + "-12-31T23:59:59Z");
            dateLongTestMissing.add(0l);
          } else {
            dateLongTestMissing.add(1l);
          }
          dateLongTestStart.add(list1);
          ArrayList<String> list2 = new ArrayList<String>();
          if( s != 0 ){
            list2.add("str"+s);
            stringLongTestMissing.add(0l);
          } else {
            stringLongTestMissing.add(1l);
          }
          stringLongTestStart.add(list2);
          ArrayList<Integer> list3 = new ArrayList<Integer>();
          if( i != 0 ){
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
      if( lm != 0 ){
        if (j-LONGM<0&&lm!=l) {
          ArrayList<Integer> list1 = new ArrayList<Integer>();
          if( i != 0 ){
            list1.add(i);
            multiLongTestMissing.add(0l);
          } else {
            multiLongTestMissing.add(1l);
          }
          multiLongTestStart.add(list1);
        } else if (lm!=l) {
          if( i != 0 ) multiLongTestStart.get((int)lm-1).add(i); else increment( multiLongTestMissing,(int)lm-1);
        }
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
  public void sumTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "sum","fieldFacets", "date_dtd");
    Collection<Double> intDate = (ArrayList<Double>)xmlToList(intDateFacet, "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "sum");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "sum","fieldFacets", "string_sd");
    Collection<Double> intString = (ArrayList<Double>)xmlToList(intStringFacet, "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "sum");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "sum","fieldFacets", "date_dtd");
    Collection<Double> longDate = (ArrayList<Double>)xmlToList(longDateFacet, "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "sum");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "sum","fieldFacets", "string_sd");
    Collection<Double> longString = (ArrayList<Double>)xmlToList(longStringFacet, "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "sum");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "sum","fieldFacets", "date_dtd");   
    Collection<Double> floatDate = (ArrayList<Double>)xmlToList(floatDateFacet, "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "sum");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "sum","fieldFacets", "string_sd");    
    Collection<Double> floatString = (ArrayList<Double>)xmlToList(floatStringFacet, "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "sum");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "sum","fieldFacets", "date_dtd");  
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "sum");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "sum","fieldFacets", "string_sd");    
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "sum");
    assertEquals(doubleString,doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void meanTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "mean","fieldFacets", "date_dtd");
    Collection<Double> intDate = (ArrayList<Double>)xmlToList(intDateFacet, "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "mean");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "mean","fieldFacets", "string_sd");
    Collection<Double> intString = (ArrayList<Double>)xmlToList(intStringFacet, "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "mean");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "mean","fieldFacets", "date_dtd");
    Collection<Double> longDate = (ArrayList<Double>)xmlToList(longDateFacet, "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "mean");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "mean","fieldFacets", "string_sd");
    Collection<Double> longString = (ArrayList<Double>)xmlToList(longStringFacet, "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "mean");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "mean","fieldFacets", "date_dtd");   
    Collection<Double> floatDate = (ArrayList<Double>)xmlToList(floatDateFacet, "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "mean");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "mean","fieldFacets", "string_sd");    
    Collection<Double> floatString = (ArrayList<Double>)xmlToList(floatStringFacet, "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "mean");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "mean","fieldFacets", "date_dtd");  
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "mean");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "mean","fieldFacets", "string_sd");    
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "mean");
    assertEquals(doubleString,doubleStringTest);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void sumOfSquaresFacetAscTest() throws Exception {
    //Int Date
    String intDateFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "date_dtd");
    Collection<Double> intDate = (ArrayList<Double>)xmlToList(intDateFacet, "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "sumOfSquares");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "string_sd");
    Collection<Double> intString = (ArrayList<Double>)xmlToList(intStringFacet, "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "sumOfSquares");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "date_dtd");
    Collection<Double> longDate = (ArrayList<Double>)xmlToList(longDateFacet, "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "sumOfSquares");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "string_sd");
    Collection<Double> longString = (ArrayList<Double>)xmlToList(longStringFacet, "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "sumOfSquares");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "date_dtd");   
    Collection<Double> floatDate = (ArrayList<Double>)xmlToList(floatDateFacet, "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "sumOfSquares");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "string_sd");    
    Collection<Double> floatString = (ArrayList<Double>)xmlToList(floatStringFacet, "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "sumOfSquares");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "date_dtd");  
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "sumOfSquares");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "sumOfSquares","fieldFacets", "string_sd");    
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "sumOfSquares");
    assertEquals(doubleString,doubleStringTest);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void stddevFacetAscTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "stddev","fieldFacets", "date_dtd");
    ArrayList<Double> intDate = (ArrayList<Double>)xmlToList(intDateFacet, "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "stddev");
    assertTrue(checkStddevs(intDate,intDateTest));
    //Int String
    String intStringFacet = getFacetXML(response, "stddev","fieldFacets", "string_sd");
    ArrayList<Double> intString = (ArrayList<Double>)xmlToList(intStringFacet, "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "stddev");
    assertTrue(checkStddevs(intString,intStringTest));

    //Long Date
    String longDateFacet = getFacetXML(response, "stddev","fieldFacets", "date_dtd");
    ArrayList<Double> longDate = (ArrayList<Double>)xmlToList(longDateFacet, "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "stddev");
    assertTrue(checkStddevs(longDate,longDateTest));
    //Long String
    String longStringFacet = getFacetXML(response, "stddev","fieldFacets", "string_sd");
    ArrayList<Double> longString = (ArrayList<Double>)xmlToList(longStringFacet, "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "stddev");
    assertTrue(checkStddevs(longString,longStringTest));

    //Float Date
    String floatDateFacet = getFacetXML(response, "stddev","fieldFacets", "date_dtd");   
    ArrayList<Double> floatDate = (ArrayList<Double>)xmlToList(floatDateFacet, "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "stddev");
    assertTrue(checkStddevs(floatDate,floatDateTest));
    //Float String
    String floatStringFacet = getFacetXML(response, "stddev","fieldFacets", "string_sd");    
    ArrayList<Double> floatString = (ArrayList<Double>)xmlToList(floatStringFacet, "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "stddev");
    assertTrue(checkStddevs(floatString,floatStringTest));

    //Double Date
    String doubleDateFacet = getFacetXML(response, "stddev","fieldFacets", "date_dtd");  
    ArrayList<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "stddev");
    assertTrue(checkStddevs(doubleDate,doubleDateTest));
    //Double String
    String doubleStringFacet = getFacetXML(response, "stddev","fieldFacets", "string_sd");    
    ArrayList<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "stddev");
    assertTrue(checkStddevs(doubleString,doubleStringTest));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void medianFacetAscTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "median","fieldFacets", "date_dtd");
    Collection<Double> intDate = (ArrayList<Double>)xmlToList(intDateFacet, "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "median");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "median","fieldFacets", "string_sd");
    Collection<Double> intString = (ArrayList<Double>)xmlToList(intStringFacet, "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "median");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "median","fieldFacets", "date_dtd");
    Collection<Double> longDate = (ArrayList<Double>)xmlToList(longDateFacet, "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "median");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "median","fieldFacets", "string_sd");
    Collection<Double> longString = (ArrayList<Double>)xmlToList(longStringFacet, "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "median");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "median","fieldFacets", "date_dtd");   
    Collection<Double> floatDate = (ArrayList<Double>)xmlToList(floatDateFacet, "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "median");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "median","fieldFacets", "string_sd");    
    Collection<Double> floatString = (ArrayList<Double>)xmlToList(floatStringFacet, "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "median");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "median","fieldFacets", "date_dtd");  
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "median");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "median","fieldFacets", "string_sd");    
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "median");
    assertEquals(doubleString,doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc20Test() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "percentile_20n","fieldFacets", "date_dtd"); 
    Collection<Integer> intDate = (ArrayList<Integer>)xmlToList(intDateFacet, "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "perc_20");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "percentile_20n","fieldFacets", "string_sd");    
    Collection<Integer> intString = (ArrayList<Integer>)xmlToList(intStringFacet, "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "perc_20");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "percentile_20n","fieldFacets", "date_dtd");    
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "perc_20");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "percentile_20n","fieldFacets", "string_sd");    
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "perc_20");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "percentile_20n","fieldFacets", "date_dtd");    
    Collection<Float> floatDate = (ArrayList<Float>)xmlToList(floatDateFacet, "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "perc_20");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "percentile_20n","fieldFacets", "string_sd");       
    Collection<Float> floatString = (ArrayList<Float>)xmlToList(floatStringFacet, "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "perc_20");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "percentile_20n","fieldFacets", "date_dtd");    
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "perc_20");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "percentile_20n","fieldFacets", "string_sd");      
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "perc_20");
    assertEquals(doubleString,doubleStringTest);

    //Date Int
    String dateIntFacet = getFacetXML(response, "percentile_20","fieldFacets", "int_id"); 
    Collection<String> dateInt = (ArrayList<String>)xmlToList(dateIntFacet, "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "perc_20");
    assertEquals(dateInt,dateIntTest);
    //Date Long
    String dateStringFacet = getFacetXML(response, "percentile_20","fieldFacets", "long_ld");       
    Collection<String> dateString = (ArrayList<String>)xmlToList(dateStringFacet, "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "perc_20");
    assertEquals(dateString,dateLongTest);

    //String Int
    String stringIntFacet = getFacetXML(response, "percentile_20","fieldFacets", "int_id");   
    Collection<String> stringInt = (ArrayList<String>)xmlToList(stringIntFacet, "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "perc_20");
    assertEquals(stringInt,stringIntTest);
    //String Long
    String stringLongFacet = getFacetXML(response, "percentile_20","fieldFacets", "long_ld");     
    Collection<String> stringLong = (ArrayList<String>)xmlToList(stringLongFacet, "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "perc_20");
    assertEquals(stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc60Test() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "percentile_60n","fieldFacets", "date_dtd"); 
    Collection<Integer> intDate = (ArrayList<Integer>)xmlToList(intDateFacet, "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "perc_60");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "percentile_60n","fieldFacets", "string_sd");    
    Collection<Integer> intString = (ArrayList<Integer>)xmlToList(intStringFacet, "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "perc_60");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "percentile_60n","fieldFacets", "date_dtd");    
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "perc_60");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "percentile_60n","fieldFacets", "string_sd");    
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "perc_60");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "percentile_60n","fieldFacets", "date_dtd");    
    Collection<Float> floatDate = (ArrayList<Float>)xmlToList(floatDateFacet, "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "perc_60");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "percentile_60n","fieldFacets", "string_sd");       
    Collection<Float> floatString = (ArrayList<Float>)xmlToList(floatStringFacet, "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "perc_60");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "percentile_60n","fieldFacets", "date_dtd");    
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "perc_60");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "percentile_60n","fieldFacets", "string_sd");      
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "perc_60");
    assertEquals(doubleString,doubleStringTest);

    //Date Int
    String dateIntFacet = getFacetXML(response, "percentile_60","fieldFacets", "int_id"); 
    Collection<String> dateInt = (ArrayList<String>)xmlToList(dateIntFacet, "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "perc_60");
    assertEquals(dateInt,dateIntTest);
    //Date Long
    String dateStringFacet = getFacetXML(response, "percentile_60","fieldFacets", "long_ld");       
    Collection<String> dateString = (ArrayList<String>)xmlToList(dateStringFacet, "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "perc_60");
    assertEquals(dateString,dateLongTest);

    //String Int
    String stringIntFacet = getFacetXML(response, "percentile_60","fieldFacets", "int_id");   
    Collection<String> stringInt = (ArrayList<String>)xmlToList(stringIntFacet, "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "perc_60");
    assertEquals(stringInt,stringIntTest);
    //String Long
    String stringLongFacet = getFacetXML(response, "percentile_60","fieldFacets", "long_ld");     
    Collection<String> stringLong = (ArrayList<String>)xmlToList(stringLongFacet, "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "perc_60");
    assertEquals(stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void minTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "minn","fieldFacets", "date_dtd"); 
    Collection<Integer> intDate = (ArrayList<Integer>)xmlToList(intDateFacet, "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "min");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "minn","fieldFacets", "string_sd");    
    Collection<Integer> intString = (ArrayList<Integer>)xmlToList(intStringFacet, "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "min");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "minn","fieldFacets", "date_dtd");    
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "min");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "minn","fieldFacets", "string_sd");    
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "min");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "minn","fieldFacets", "date_dtd");    
    Collection<Float> floatDate = (ArrayList<Float>)xmlToList(floatDateFacet, "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "min");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "minn","fieldFacets", "string_sd");       
    Collection<Float> floatString = (ArrayList<Float>)xmlToList(floatStringFacet, "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "min");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "minn","fieldFacets", "date_dtd");    
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "min");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "minn","fieldFacets", "string_sd");      
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "min");
    assertEquals(doubleString,doubleStringTest);

    //Date Int
    String dateIntFacet = getFacetXML(response, "min","fieldFacets", "int_id"); 
    Collection<String> dateInt = (ArrayList<String>)xmlToList(dateIntFacet, "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "min");
    assertEquals(dateInt,dateIntTest);
    //Date Long
    String dateStringFacet = getFacetXML(response, "min","fieldFacets", "long_ld");       
    Collection<String> dateString = (ArrayList<String>)xmlToList(dateStringFacet, "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "min");
    assertEquals(dateString,dateLongTest);

    //String Int
    String stringIntFacet = getFacetXML(response, "min","fieldFacets", "int_id");   
    Collection<String> stringInt = (ArrayList<String>)xmlToList(stringIntFacet, "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "min");
    assertEquals(stringInt,stringIntTest);
    //String Long
    String stringLongFacet = getFacetXML(response, "min","fieldFacets", "long_ld");     
    Collection<String> stringLong = (ArrayList<String>)xmlToList(stringLongFacet, "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "min");
    assertEquals(stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void maxTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "maxn","fieldFacets", "date_dtd"); 
    Collection<Integer> intDate = (ArrayList<Integer>)xmlToList(intDateFacet, "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>)calculateStat(intDateTestStart, "max");
    assertEquals(intDate,intDateTest);
    
    //Int String
    String intStringFacet = getFacetXML(response, "maxn","fieldFacets", "string_sd");    
    Collection<Integer> intString = (ArrayList<Integer>)xmlToList(intStringFacet, "int", "int");
    ArrayList<Integer> intStringTest = (ArrayList<Integer>)calculateStat(intStringTestStart, "max");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "maxn","fieldFacets", "date_dtd");    
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "max");
    assertEquals(longDate,longDateTest);
    
    //Long String
    String longStringFacet = getFacetXML(response, "maxn","fieldFacets", "string_sd");    
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "max");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "maxn","fieldFacets", "date_dtd");    
    Collection<Float> floatDate = (ArrayList<Float>)xmlToList(floatDateFacet, "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>)calculateStat(floatDateTestStart, "max");
    assertEquals(floatDate,floatDateTest);
    
    //Float String
    String floatStringFacet = getFacetXML(response, "maxn","fieldFacets", "string_sd");       
    Collection<Float> floatString = (ArrayList<Float>)xmlToList(floatStringFacet, "float", "float");
    ArrayList<Float> floatStringTest = (ArrayList<Float>)calculateStat(floatStringTestStart, "max");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "maxn","fieldFacets", "date_dtd");    
    Collection<Double> doubleDate = (ArrayList<Double>)xmlToList(doubleDateFacet, "double", "double");
    ArrayList<Double> doubleDateTest = (ArrayList<Double>)calculateStat(doubleDateTestStart, "max");
    assertEquals(doubleDate,doubleDateTest);
    
    //Double String
    String doubleStringFacet = getFacetXML(response, "maxn","fieldFacets", "string_sd");      
    Collection<Double> doubleString = (ArrayList<Double>)xmlToList(doubleStringFacet, "double", "double");
    ArrayList<Double> doubleStringTest = (ArrayList<Double>)calculateStat(doubleStringTestStart, "max");
    assertEquals(doubleString,doubleStringTest);
    
    //String Int
    String stringIntFacet = getFacetXML(response, "max","fieldFacets", "int_id");   
    Collection<String> stringInt = (ArrayList<String>)xmlToList(stringIntFacet, "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>)calculateStat(stringIntTestStart, "max");
    assertEquals(stringInt,stringIntTest);
    
    //String Long
    String stringLongFacet = getFacetXML(response, "max","fieldFacets", "long_ld");     
    Collection<String> stringLong = (ArrayList<String>)xmlToList(stringLongFacet, "str", "str");
    ArrayList<String> stringLongTest = (ArrayList<String>)calculateStat(stringLongTestStart, "max");
    assertEquals(stringLong,stringLongTest);

    //Date Int
    String dateIntFacet = getFacetXML(response, "max","fieldFacets", "int_id"); 
    Collection<String> dateInt = (ArrayList<String>)xmlToList(dateIntFacet, "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>)calculateStat(dateIntTestStart, "max");
    assertEquals(dateInt,dateIntTest);
    
    //Date Long
    String dateStringFacet = getFacetXML(response, "max","fieldFacets", "long_ld");       
    Collection<String> dateString = (ArrayList<String>)xmlToList(dateStringFacet, "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>)calculateStat(dateLongTestStart, "max");
    assertEquals(dateString,dateLongTest);

  }

  @SuppressWarnings("unchecked")
  @Test
  public void uniqueTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "uniquen", "fieldFacets", "date_dtd");    
    Collection<Long> intDate = (ArrayList<Long>)xmlToList(intDateFacet, "long", "int");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(intDateTestStart, "unique");
    assertEquals(intDate,intDateTest);
    //Int String
    String intStringFacet = getFacetXML(response, "uniquen", "fieldFacets", "string_sd");      
    Collection<Long> intString = (ArrayList<Long>)xmlToList(intStringFacet, "long", "int");
    ArrayList<Long> intStringTest = (ArrayList<Long>)calculateStat(intStringTestStart, "unique");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "uniquen", "fieldFacets", "date_dtd");      
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "unique");
    assertEquals(longDate,longDateTest);
    //Long String
    String longStringFacet = getFacetXML(response, "uniquen", "fieldFacets", "string_sd");   
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "unique");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "uniquen", "fieldFacets", "date_dtd");   
    Collection<Long> floatDate = (ArrayList<Long>)xmlToList(floatDateFacet, "long", "float");
    ArrayList<Long> floatDateTest = (ArrayList<Long>)calculateStat(floatDateTestStart, "unique");
    assertEquals(floatDate,floatDateTest);
    //Float String
    String floatStringFacet = getFacetXML(response, "uniquen", "fieldFacets", "string_sd");   
    Collection<Long> floatString = (ArrayList<Long>)xmlToList(floatStringFacet, "long", "float");
    ArrayList<Long> floatStringTest = (ArrayList<Long>)calculateStat(floatStringTestStart, "unique");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "uniquen", "fieldFacets", "date_dtd");   
    Collection<Long> doubleDate = (ArrayList<Long>)xmlToList(doubleDateFacet, "long", "double");
    ArrayList<Long> doubleDateTest = (ArrayList<Long>)calculateStat(doubleDateTestStart, "unique");
    assertEquals(doubleDate,doubleDateTest);
    //Double String
    String doubleStringFacet = getFacetXML(response, "uniquen", "fieldFacets", "string_sd");   
    Collection<Long> doubleString = (ArrayList<Long>)xmlToList(doubleStringFacet, "long", "double");
    ArrayList<Long> doubleStringTest = (ArrayList<Long>)calculateStat(doubleStringTestStart, "unique");
    assertEquals(doubleString,doubleStringTest);

    //Date Int
    String dateIntFacet = getFacetXML(response, "unique", "fieldFacets", "int_id");    
    Collection<Long> dateInt = (ArrayList<Long>)xmlToList(dateIntFacet, "long", "date");
    ArrayList<Long> dateIntTest = (ArrayList<Long>)calculateStat(dateIntTestStart, "unique");
    assertEquals(dateInt,dateIntTest);
    //Date Long
    String dateStringFacet = getFacetXML(response, "unique", "fieldFacets", "long_ld");     
    Collection<Long> dateString = (ArrayList<Long>)xmlToList(dateStringFacet, "long", "date");
    ArrayList<Long> dateLongTest = (ArrayList<Long>)calculateStat(dateLongTestStart, "unique");
    assertEquals(dateString,dateLongTest);

    //String Int
    String stringIntFacet = getFacetXML(response, "unique", "fieldFacets", "int_id");   
    Collection<Long> stringInt = (ArrayList<Long>)xmlToList(stringIntFacet, "long", "str");
    ArrayList<Long> stringIntTest = (ArrayList<Long>)calculateStat(stringIntTestStart, "unique");
    assertEquals(stringInt,stringIntTest);
    //String Long
    String stringLongFacet = getFacetXML(response, "unique", "fieldFacets", "long_ld");    
    Collection<Long> stringLong = (ArrayList<Long>)xmlToList(stringLongFacet, "long", "str");
    ArrayList<Long> stringLongTest = (ArrayList<Long>)calculateStat(stringLongTestStart, "unique");
    assertEquals(stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void countTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "countn", "fieldFacets", "date_dtd");    
    Collection<Long> intDate = (ArrayList<Long>)xmlToList(intDateFacet, "long", "int");
    ArrayList<Long> intDateTest = (ArrayList<Long>)calculateStat(intDateTestStart, "count");
    assertEquals(intDate,intDateTest);
    
    //Int String
    String intStringFacet = getFacetXML(response, "countn", "fieldFacets", "string_sd");      
    Collection<Long> intString = (ArrayList<Long>)xmlToList(intStringFacet, "long", "int");
    ArrayList<Long> intStringTest = (ArrayList<Long>)calculateStat(intStringTestStart, "count");
    assertEquals(intString,intStringTest);

    //Long Date
    String longDateFacet = getFacetXML(response, "countn", "fieldFacets", "date_dtd");      
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>)calculateStat(longDateTestStart, "count");
    assertEquals(longDate,longDateTest);
    
    //Long String
    String longStringFacet = getFacetXML(response, "countn", "fieldFacets", "string_sd");   
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>)calculateStat(longStringTestStart, "count");
    assertEquals(longString,longStringTest);

    //Float Date
    String floatDateFacet = getFacetXML(response, "countn", "fieldFacets", "date_dtd");   
    Collection<Long> floatDate = (ArrayList<Long>)xmlToList(floatDateFacet, "long", "float");
    ArrayList<Long> floatDateTest = (ArrayList<Long>)calculateStat(floatDateTestStart, "count");
    assertEquals(floatDate,floatDateTest);
    
    //Float String
    String floatStringFacet = getFacetXML(response, "countn", "fieldFacets", "string_sd");   
    Collection<Long> floatString = (ArrayList<Long>)xmlToList(floatStringFacet, "long", "float");
    ArrayList<Long> floatStringTest = (ArrayList<Long>)calculateStat(floatStringTestStart, "count");
    assertEquals(floatString,floatStringTest);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "countn", "fieldFacets", "date_dtd");   
    Collection<Long> doubleDate = (ArrayList<Long>)xmlToList(doubleDateFacet, "long", "double");
    ArrayList<Long> doubleDateTest = (ArrayList<Long>)calculateStat(doubleDateTestStart, "count");
    assertEquals(doubleDate,doubleDateTest);
    
    //Double String
    String doubleStringFacet = getFacetXML(response, "countn", "fieldFacets", "string_sd");   
    Collection<Long> doubleString = (ArrayList<Long>)xmlToList(doubleStringFacet, "long", "double");
    ArrayList<Long> doubleStringTest = (ArrayList<Long>)calculateStat(doubleStringTestStart, "count");
    assertEquals(doubleString,doubleStringTest);

    //Date Int
    String dateIntFacet = getFacetXML(response, "count", "fieldFacets", "int_id");    
    Collection<Long> dateInt = (ArrayList<Long>)xmlToList(dateIntFacet, "long", "date");
    ArrayList<Long> dateIntTest = (ArrayList<Long>)calculateStat(dateIntTestStart, "count");
    assertEquals(dateIntTest,dateInt);
    
    //Date Long
    String dateLongFacet = getFacetXML(response, "count", "fieldFacets", "long_ld");     
    Collection<Long> dateLong = (ArrayList<Long>)xmlToList(dateLongFacet, "long", "date");
    ArrayList<Long> dateLongTest = (ArrayList<Long>)calculateStat(dateLongTestStart, "count");
    assertEquals(dateLong,dateLongTest);

    //String Int
    String stringIntFacet = getFacetXML(response, "count", "fieldFacets", "int_id");   
    Collection<Long> stringInt = (ArrayList<Long>)xmlToList(stringIntFacet, "long", "str");
    ArrayList<Long> stringIntTest = (ArrayList<Long>)calculateStat(stringIntTestStart, "count");
    assertEquals(stringInt,stringIntTest);
    
    //String Long
    String stringLongFacet = getFacetXML(response, "count", "fieldFacets", "long_ld");    
    Collection<Long> stringLong = (ArrayList<Long>)xmlToList(stringLongFacet, "long", "str");
    ArrayList<Long> stringLongTest = (ArrayList<Long>)calculateStat(stringLongTestStart, "count");
    assertEquals(stringLong,stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void missingTest() throws Exception { 
    //Int Date
    String intDateFacet = getFacetXML(response, "missingn", "fieldFacets", "date_dtd");    
    Collection<Long> intDate = (ArrayList<Long>)xmlToList(intDateFacet, "long", "int");
    setLatestType("int");
    assertEquals(intDateTestMissing,intDate);
    
    //Int String
    String intStringFacet = getFacetXML(response, "missingn", "fieldFacets", "string_sd");      
    Collection<Long> intString = (ArrayList<Long>)xmlToList(intStringFacet, "long", "int");
    assertEquals(intStringTestMissing,intString);

    //Long Date
    String longDateFacet = getFacetXML(response, "missingn", "fieldFacets", "date_dtd");      
    Collection<Long> longDate = (ArrayList<Long>)xmlToList(longDateFacet, "long", "long");
    setLatestType("long");
    assertEquals(longDateTestMissing,longDate);
    
    //Long String
    String longStringFacet = getFacetXML(response, "missingn", "fieldFacets", "string_sd");   
    Collection<Long> longString = (ArrayList<Long>)xmlToList(longStringFacet, "long", "long");
    assertEquals(longStringTestMissing,longString);

    //Float Date
    String floatDateFacet = getFacetXML(response, "missingn", "fieldFacets", "date_dtd");   
    Collection<Long> floatDate = (ArrayList<Long>)xmlToList(floatDateFacet, "long", "float");
    setLatestType("float");
    assertEquals(floatDateTestMissing,floatDate);
    
    //Float String
    String floatStringFacet = getFacetXML(response, "missingn", "fieldFacets", "string_sd");   
    Collection<Long> floatString = (ArrayList<Long>)xmlToList(floatStringFacet, "long", "float");
    assertEquals(floatStringTestMissing,floatString);

    //Double Date
    String doubleDateFacet = getFacetXML(response, "missingn", "fieldFacets", "date_dtd");   
    Collection<Long> doubleDate = (ArrayList<Long>)xmlToList(doubleDateFacet, "long", "double");
    setLatestType("double");
    assertEquals(doubleDateTestMissing,doubleDate);
    
    //Double String
    String doubleStringFacet = getFacetXML(response, "missingn", "fieldFacets", "string_sd");   
    Collection<Long> doubleString = (ArrayList<Long>)xmlToList(doubleStringFacet, "long", "double");
    assertEquals(doubleStringTestMissing,doubleString);

    //Date Int
    String dateIntFacet = getFacetXML(response, "missing", "fieldFacets", "int_id");    
    Collection<Long> dateInt = (ArrayList<Long>)xmlToList(dateIntFacet, "long", "date");
    setLatestType("date");
    assertEquals(dateIntTestMissing,dateInt);
    
    //Date Long
    String dateStringFacet = getFacetXML(response, "missing", "fieldFacets", "long_ld");     
    Collection<Long> dateLong = (ArrayList<Long>)xmlToList(dateStringFacet, "long", "date");
    assertEquals(dateLongTestMissing,dateLong);

    //String Int
    String stringIntFacet = getFacetXML(response, "missing", "fieldFacets", "int_id");   
    Collection<Long> stringInt = (ArrayList<Long>)xmlToList(stringIntFacet, "long", "str");
    setLatestType("string");
    assertEquals(stringIntTestMissing,stringInt);
    
    //String Long
    String stringLongFacet = getFacetXML(response, "missing", "fieldFacets", "long_ld");    
    Collection<Long> stringLong = (ArrayList<Long>)xmlToList(stringLongFacet, "long", "str");
    assertEquals(stringLongTestMissing,stringLong);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTest() throws Exception { 
    //Long
    String longFacet = getFacetXML(response, "multivalued", "fieldFacets", "long_ldm");    
    Collection<Double> lon = (ArrayList<Double>)xmlToList(longFacet, "double", "mean");
    ArrayList<Double> longTest = calculateNumberStat(multiLongTestStart, "mean");
    assertEquals(lon,longTest);
    //Date
    String dateFacet = getFacetXML(response, "multivalued", "fieldFacets", "date_dtdm");    
    Collection<Double> date = (ArrayList<Double>)xmlToList(dateFacet, "double", "mean");
    ArrayList<Double> dateTest = calculateNumberStat(multiDateTestStart, "mean");
    assertEquals(date,dateTest);
    //String
    String stringFacet = getFacetXML(response, "multivalued", "fieldFacets", "string_sdm");    
    Collection<Double> string = (ArrayList<Double>)xmlToList(stringFacet, "double", "mean");
    ArrayList<Double> stringTest = calculateNumberStat(multiStringTestStart, "mean");
    assertEquals(string,stringTest);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void missingFacetTest() throws Exception { 
    //int MultiDate
    String stringFacet = getFacetXML(response, "missingf", "fieldFacets", "date_dtdm");  
    assertTrue(stringFacet.contains("<lst name=\"(MISSING)\">"));  
    ArrayList<Double> string = (ArrayList<Double>)xmlToList(stringFacet, "double", "mean");
    string.remove(0);
    ArrayList<Double> stringTest = calculateNumberStat(multiDateTestStart, "mean");
    assertEquals(string,stringTest);
    
    //Int String
    String intStringFacet = getFacetXML(response, "missingf", "fieldFacets", "string_sd"); 
    assertTrue(intStringFacet.contains("<lst name=\"(MISSING)\">")&&!intStringFacet.contains("<lst name=\"str0\">"));
    List<Double> intString = (ArrayList<Double>)xmlToList(intStringFacet, "double", "mean");
    intString.remove(0);
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "mean");
    assertEquals(intString,intStringTest);
    
    //Int Date
    String intDateFacet = getFacetXML(response, "missingf", "fieldFacets", "date_dtd");    
    Collection<Double> intDate = (ArrayList<Double>)xmlToList(intDateFacet, "double", "mean");
    ArrayList<ArrayList<Double>> intDateMissingTestStart = (ArrayList<ArrayList<Double>>) intDateTestStart.clone();
    ArrayList<Double> intDateTest = calculateNumberStat(intDateMissingTestStart, "mean");
    assertEquals(intDate,intDateTest);
    
    
  }

  private boolean checkStddevs(ArrayList<Double> list1, ArrayList<Double> list2) {
    boolean b = true;
    for (int i = 0; i<list1.size() && b; i++) {
      b = b && (Math.abs(list1.get(i)-list2.get(i))<.00000000001);
    }
    return b;
  }

}
