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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.solr.analytics.legacy.LegacyAbstractAnalyticsCloudTest;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.OrdinalCalculator;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;

public class LegacyAbstractAnalyticsFacetCloudTest extends LegacyAbstractAnalyticsCloudTest {
  protected static final HashMap<String,Object> defaults = new HashMap<>();

  protected String latestType = "";

  @AfterClass
  public static void afterClassAbstractAnalysis() {
    defaults.clear();
  }

  @SuppressWarnings("unchecked")
  protected <T> ArrayList<T> getValueList(NamedList<Object> response, String infoName, String facetType, String facetName, String exprName, boolean includeMissing) {
    NamedList<NamedList<Object>> facetList =
        (NamedList<NamedList<Object>>)response.findRecursive(AnalyticsResponseHeadings.COMPLETED_OLD_HEADER,
                                                             infoName,
                                                             facetType,
                                                             facetName);

    ArrayList<T> results = new ArrayList<>();
    facetList.forEach( (name, expressions) -> {
      if (!includeMissing && !name.equals("(MISSING)")) {
        T result = (T)expressions.get(exprName);
        if (result != null)
          results.add(result);
      }
    });
    return results;
  }

  protected boolean responseContainsFacetValue(NamedList<Object> response, String infoName, String facetType, String facetName, String facetValue) {
    return null != response.findRecursive(AnalyticsResponseHeadings.COMPLETED_OLD_HEADER,
                                          infoName,
                                          facetType,
                                          facetName,
                                          facetValue);
  }


  public static void increment(List<Long> list, int idx){
    Long i = list.remove(idx);
    list.add(idx, i+1);
  }

  protected void setLatestType(String latestType) {
    this.latestType = latestType;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Number & Comparable<T>> ArrayList calculateFacetedNumberStat(ArrayList<ArrayList<T>> lists, String stat) {
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
  public <T extends Comparable<T>> ArrayList calculateFacetedStat(ArrayList<ArrayList<T>> lists, String stat) {
    ArrayList result;
    if (stat.contains("perc_")) {
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        int ord = (int) Math.ceil(Double.parseDouble(stat.substring(5))/100 * list.size()) - 1;
        ArrayList<Integer> percs = new ArrayList<>(1);
        percs.add(ord);
        OrdinalCalculator.putOrdinalsInPosition(list, percs);
        result.add(list.get(ord));
      }
    } else if (stat.equals("count")) {
      result = new ArrayList<Long>();
      for (List<T> list : lists) {
        result.add((long)list.size());
      }
    } else if (stat.equals("missing")) {
      result = new ArrayList<Long>();
      for (ArrayList<T> list : lists) {
        result.add(calculateMissing(list,latestType));
      }
    } else if (stat.equals("unique")) {
      result = new ArrayList<Long>();
      for (List<T> list : lists) {
        HashSet<T> set = new HashSet<>();
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
}
