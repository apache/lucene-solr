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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.PercentileCalculator;
import org.apache.solr.request.SolrQueryRequest;

import com.google.common.collect.ObjectArrays;
import org.apache.solr.util.ExternalPaths;

@SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Appending","Asserting"})
public class AbstractAnalyticsStatsTest extends SolrTestCaseJ4 {
  
  protected static final String[] BASEPARMS = new String[]{ "q", "*:*", "indent", "true", "olap", "true", "rows", "0" };
  protected static final HashMap<String,Object> defaults = new HashMap<String,Object>();
  
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

  public <T extends Number & Comparable<T>> Double calculateNumberStat(ArrayList<T> list, String stat) {
    Double result;
    if (stat.equals("median")) {
      result = MedianCalculator.getMedian(list);
    } else if (stat.equals("mean")) {
      double d = 0;
      for (T element : list) {
        d += element.doubleValue();
      }
      result = Double.valueOf(d/list.size());
    } else if (stat.equals("sum")) {
      double d = 0;
      for (T element : list) {
        d += element.doubleValue();
      }
      result = Double.valueOf(d);
    } else if (stat.equals("sumOfSquares")) {
      double d = 0;
      for (T element : list) {
        d += element.doubleValue()*element.doubleValue();
      }
      result = Double.valueOf(d);
    } else if (stat.equals("stddev")) {
      double sum = 0;
      double sumSquares = 0;
      for (T element : list) {
        sum += element.doubleValue();
        sumSquares += element.doubleValue()*element.doubleValue();
      }
      result = Math.sqrt(sumSquares/list.size()-sum*sum/(list.size()*list.size()));
    } else {
      throw new IllegalArgumentException();
    }
    return result;
  }

  public <T extends Comparable<T>> Object calculateStat(ArrayList<T> list, String stat) {
    Object result;
    if (stat.contains("perc_")) {
      double[] perc = new double[]{Double.parseDouble(stat.substring(5))/100};
      result = PercentileCalculator.getPercentiles(list, perc).get(0);
    } else if (stat.equals("count")) {
      result = Long.valueOf(list.size());
    } else if (stat.equals("unique")) {
      HashSet<T> set = new HashSet<T>();
      set.addAll(list);
      result = Long.valueOf((long)set.size());
    } else if (stat.equals("max")) {
      Collections.sort(list);
      result = list.get(list.size()-1);
    } else if (stat.equals("min")) {
      Collections.sort(list);
      result = list.get(0);
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
  
  public static String[] fileToStringArr(String fileName) throws FileNotFoundException {
    Scanner file = new Scanner(new File(ExternalPaths.SOURCE_HOME, fileName), "UTF-8");
    ArrayList<String> strList = new ArrayList<String>();
    while (file.hasNextLine()) {
      String line = file.nextLine();
      line = line.trim();
      if( StringUtils.isBlank(line) || line.startsWith("#")){
        continue;
      }
      String[] param = line.split("=");
      strList.add(param[0]);
      strList.add(param[1]);
    }
    return strList.toArray(new String[0]);
  }
  

}
