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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.OrdinalCalculator;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;

public class AbstractAnalyticsFacetCloudTest extends SolrCloudTestCase {
  protected static final HashMap<String,Object> defaults = new HashMap<>();
  
  protected static final String COLLECTIONORALIAS = "collection1";
  protected static final int TIMEOUT = DEFAULT_TIMEOUT;
  protected static final String id = "id";

  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-analytics"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTIONORALIAS, "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTIONORALIAS, cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    
    cleanIndex();
  }

  public static void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }
  
  protected String latestType = "";
  
  @AfterClass
  public static void afterClassAbstractAnalysis() {
    defaults.clear();
  }

  protected NamedList<Object> queryCloudAnalytics(String[] testParams) throws SolrServerException, IOException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("indent", "true");
    params.set("olap", "true");
    params.set("rows", "0");
    for (int i = 0; i + 1 < testParams.length;) {
      params.add(testParams[i++], testParams[i++]);
    }
    cluster.waitForAllNodes(10000);
    QueryRequest qreq = new QueryRequest(params);
    QueryResponse resp = qreq.process(cluster.getSolrClient(), COLLECTIONORALIAS);
    return resp.getResponse();
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
  
  public static String[] filter(String...args){
    List<String> l = new ArrayList<>();
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

  public static ModifiableSolrParams fileToParams(Class<?> clazz, String fileName) throws FileNotFoundException {
    InputStream in = clazz.getResourceAsStream(fileName);
    if (in == null) throw new FileNotFoundException("Resource not found: " + fileName);
    Scanner file = new Scanner(in, "UTF-8");
    try { 
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("q", "*:*");
      params.set("indent", "true");
      params.set("olap", "true");
      params.set("rows", "0");
      while (file.hasNextLine()) {
        String line = file.nextLine();
        line = line.trim();
        if( StringUtils.isBlank(line) || line.startsWith("#")){
          continue;
        }
        String[] param = line.split("=");
        params.set(param[0], param[1]);
      }
      return params;
    } finally {
      IOUtils.closeWhileHandlingException(file, in);
    }
  }
}
