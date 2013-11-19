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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.PercentileCalculator;
import org.apache.solr.request.SolrQueryRequest;

import com.google.common.collect.ObjectArrays;
import org.apache.solr.util.ExternalPaths;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class AbstractAnalyticsFacetTest extends SolrTestCaseJ4 {
  protected static final HashMap<String,Object> defaults = new HashMap<String,Object>();
  
  protected String latestType = "";

  public String getFacetXML(String response, String requestName, String facetType, String facet) {
    String cat = "\n  <lst name=\""+requestName+"\">";
    String begin = "    <lst name=\""+facetType+"\">\n";
    String end = "\n    </lst>";
    int beginInt = response.indexOf(begin, response.indexOf(cat))+begin.length();
    int endInt = response.indexOf(end, beginInt);
    String fieldStr = response.substring(beginInt, endInt);
    begin = "      <lst name=\""+facet+"\">";
    end = "\n      </lst>";
    beginInt = fieldStr.indexOf(begin);
    endInt = fieldStr.indexOf(end, beginInt);
    String facetStr = "";
    if (beginInt>=0) {
      facetStr = fieldStr.substring(beginInt+begin.length(),endInt);
    }
    return facetStr+" ";
  }
  
  public static void increment(List<Long> list, int idx){
    Long i = list.remove(idx);
    list.add(idx, i+1);
  }
  
  public static String[] filter(String...args){
    List<String> l = new ArrayList<String>();
    for( int i=0; i <args.length; i+=2){
      if( args[i+1].equals("0") || args[i+1].equals("0.0") || 
          args[i+1].equals("1800-12-31T23:59:59Z") || args[i+1].equals("str0") ||
          args[i+1].equals("this is the firststr0") || 
          args[i+1].equals("this is the secondstr0") ){
        continue;
      }
      l.add(args[i]);
      l.add(args[i+1]);
    }
    return l.toArray(new String[0]);
  }
  
  protected void setLatestType(String latestType) {
    this.latestType = latestType;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public ArrayList xmlToList(String facit, String type, String name) {
    ArrayList list;
    if (type.equals("double")) {
      list = new ArrayList<Double>();
    } else if (type.equals("int")) {
      list = new ArrayList<Integer>();
    } else if (type.equals("long")) {
      list = new ArrayList<Long>();
    } else if (type.equals("float")) {
      list = new ArrayList<Float>();
    } else {
      list = new ArrayList<String>();
    }
    String find = "<"+type+" name=\""+name+"\">";
    String endS = "</"+type+">";
    int findAt = facit.indexOf(find)+find.length();
    while (findAt>find.length()) {
      int end = facit.indexOf(endS, findAt);
      if (type.equals("double")) {
        list.add(Double.parseDouble(facit.substring(findAt, end)));
      } else if (type.equals("int")) {
        list.add(Integer.parseInt(facit.substring(findAt, end)));
      } else if (type.equals("long")) {
        list.add(Long.parseLong(facit.substring(findAt, end)));
      } else if (type.equals("float")) {
        list.add(Float.parseFloat(facit.substring(findAt, end)));
      } else {
        list.add(facit.substring(findAt, end));
      }
      findAt = facit.indexOf(find, end)+find.length();
    }
    return list;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Number & Comparable<T>> ArrayList calculateNumberStat(ArrayList<ArrayList<T>> lists, String stat) {
    ArrayList result;
    if (stat.equals("median")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        result.add(MedianCalculator.getMedian(list));
      }
    } else if (stat.equals("mean")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        double d = 0;
        for (T element : list) {
          d += element.doubleValue();
        }
        result.add(d/list.size());
      }
    } else if (stat.equals("sum")) {
      result = new ArrayList<Double>();
      for (Collection<T> list : lists) {
        double d = 0;
        for (T element : list) {
          d += element.doubleValue();
        }
        result.add(d);
      }
    } else if (stat.equals("sumOfSquares")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        double d = 0;
        for (T element : list) {
          d += element.doubleValue()*element.doubleValue();
        }
        result.add(d);
      }
    } else if (stat.equals("stddev")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        double sum = 0;
        double sumSquares = 0;
        for (T element : list) {
          sum += element.doubleValue();
          sumSquares += element.doubleValue()*element.doubleValue();
        }
        String res = Double.toString(Math.sqrt(sumSquares/list.size()-sum*sum/(list.size()*list.size())));
        result.add(Double.parseDouble(res));
      }
    } else {
      throw new IllegalArgumentException();
    }
    return result;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Comparable<T>> ArrayList calculateStat(ArrayList<ArrayList<T>> lists, String stat) {
    ArrayList result;
    if (stat.contains("perc_")) {
      double[] perc = new double[]{Double.parseDouble(stat.substring(5))/100};
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        result.add(PercentileCalculator.getPercentiles(list, perc).get(0));
      }
    } else if (stat.equals("count")) {
      result = new ArrayList<Long>();
      for (List<T> list : lists) {
        //if( list.size() == 0) continue;
        result.add((long)list.size());
      }
    } else if (stat.equals("missing")) {
      result = new ArrayList<Long>();
      for (ArrayList<T> list : lists) {
        if( list.size() == 0) continue;
        result.add(calculateMissing(list,latestType));
      }
    } else if (stat.equals("unique")) {
      result = new ArrayList<Long>();
      for (List<T> list : lists) {
        HashSet<T> set = new HashSet<T>();
        set.addAll(list);
        result.add((long)set.size());
      }
    } else if (stat.equals("max")) {
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        Collections.sort(list);
        result.add(list.get(list.size()-1));
      }
    } else if (stat.equals("min")) {
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        Collections.sort((List<T>)list);
        result.add(list.get(0));
      }
    } else {
      result = null;
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Long calculateMissing(ArrayList<T> list, String type) {
    T def = (T)defaults.get(type);
    long miss = 0;
    for (T element : list) {
      if (element.compareTo(def)==0) {
        miss++;
      }
    }
    return Long.valueOf(miss);
  }
  
  public static SolrQueryRequest request(String...args){
    return SolrTestCaseJ4.req( ObjectArrays.concat(BASEPARMS, args,String.class) );
  }
  
  public static final String[] BASEPARMS = new String[]{ "q", "*:*", "indent", "true", "olap", "true", "rows", "0" };

  
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
