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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeoutException;

import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.OrdinalCalculator;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;

public class LegacyAbstractAnalyticsCloudTest extends SolrCloudTestCase {

  protected static final String COLLECTIONORALIAS = "collection1";
  protected static final int TIMEOUT = DEFAULT_TIMEOUT;
  protected static final String id = "id";

  @Before
  public void setupCollection() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-analytics"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTIONORALIAS, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTIONORALIAS, 2, 2);
  }

  @After
  public void teardownCollection() throws Exception {
    shutdownCluster();
  }

  public void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  protected static final String[] BASEPARMS = new String[]{ "q", "*:*", "indent", "true", "olap", "true", "rows", "0" };

  public static enum VAL_TYPE {
    INTEGER("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    STRING("str"),
    DATE("date");

    private VAL_TYPE (final String text) {
      this.text = text;
    }

    private final String text;

    @Override
    public String toString() {
      return text;
    }
  }

  protected NamedList<Object> queryLegacyCloudAnalytics(String[] testParams) throws SolrServerException, IOException, InterruptedException, TimeoutException {
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
  protected <T> T getValue(NamedList<Object> response, String infoName, String exprName) {
    return (T)response.findRecursive(AnalyticsResponseHeadings.COMPLETED_OLD_HEADER,
                                     infoName,
                                     exprName);
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
      ArrayList<Integer> percs = new ArrayList<>(1);
      int ord = (int) Math.ceil(Double.parseDouble(stat.substring(5))/100 * list.size()) - 1;
      percs.add(ord);
      OrdinalCalculator.putOrdinalsInPosition(list, percs);
      result = list.get(percs.get(0));
    } else if (stat.equals("count")) {
      result = Long.valueOf(list.size());
    } else if (stat.equals("unique")) {
      HashSet<T> set = new HashSet<>();
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
}
