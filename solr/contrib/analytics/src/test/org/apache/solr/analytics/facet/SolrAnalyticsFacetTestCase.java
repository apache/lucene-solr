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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.analytics.SolrAnalyticsTestCase;
import org.junit.Before;

public class SolrAnalyticsFacetTestCase extends SolrAnalyticsTestCase {

  private List<FVP> facetResults;
  private Map<String, List<FVP>> results = new HashMap<>();
  private Map<String, String> facets = new HashMap<>();
  private FVP fvp;
  private int fvp_num;

  @Before
  protected void initData() {
    results = new HashMap<>();
    facets = new HashMap<>();
  }

  protected void addFacet(String name, String json) {
    facets.put(name, json);
    facetResults = new ArrayList<>();
    results.put(name, facetResults);
    fvp_num = 0;
  }

  protected void addFacetValue(String value) {
    fvp = new FVP(fvp_num++, value, new HashMap<>());
    facetResults.add(fvp);
  }

  protected void addFacetResult(String expr, Comparable<?> result) {
    fvp.expectedResults.put(expr, result);
  }

  protected void testGrouping(Map<String,String> expressions) {
    try {
      testGrouping("grouping", expressions, facets, results);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void testGrouping(Map<String,String> expressions, boolean sortAscending) {
    try {
      testGrouping("grouping", expressions, facets, results, sortAscending);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void testGrouping(Map<String,String> expressions, String sortExpression, boolean sortAscending) {
    try {
      testGrouping("grouping", expressions, facets, results, sortExpression, sortAscending);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
