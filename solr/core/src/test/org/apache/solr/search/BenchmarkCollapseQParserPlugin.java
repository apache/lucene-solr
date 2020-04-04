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

package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4Test;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.util.LogLevel;
import org.codehaus.janino.Mod;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.core.StringContains.containsString;

@LogLevel("org.apache.solr.update.processor.LogUpdateProcessorFactory=OFF;org.apache.solr.core.SolrCore.Request=OFF")
public class BenchmarkCollapseQParserPlugin extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<String> TERMS = new ArrayList<>();
  private static final List<String> GROUP_FIELDS =  Arrays.asList("group_i", "group_ti_dv", "group_f", "group_tf_dv", "group_s", "group_s_dv");
  private static final List<String> HINTS = Arrays.asList("", CollapsingQParserPlugin.HINT_TOP_FC);
  private static final List<String> SORT_FIELDS = Arrays.asList("test_i", "test_l", "id_i", "term_s", "score");
  private static final int NUM_DOCS = 300000;
  //number of groups must be high so we can see the different between arrayBased or hashBased method
  private static final int NUM_GROUPS = 1000000;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    initCore("solrconfig-collapseqparser-benchmark.xml", "schema11.xml");
    for (int i = 0; i < 1000; i++) {
      TERMS.add(TestUtil.randomSimpleString(random(),1, 5));
    }

    for (int i = 0; i < NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", i+"",
          "test_i", random().nextInt()+"",
          "test_f", random().nextInt()+"",
          "test_l", random().nextInt()+"");
      boolean isNullGroup = random().nextInt(100) < 1;
      if (!isNullGroup) {
        for (String field : GROUP_FIELDS) {
          doc.addField(field, String.valueOf(i % NUM_GROUPS));
        }
      }
      boolean havingTerm = random().nextInt(100) < 95;
      if (havingTerm) {
        doc.addField("term_s", randomFrom(TERMS));
      }
      assertU(adoc(doc));
    }
    assertU(commit());
  }

  private static <T> T randomFrom(List<T> list) {
    return list.get(random().nextInt(list.size()));
  }

  private String randomSorts() {
    int numSort = random().nextInt(2) + 1;
    List<String> sorts = new ArrayList<>();
    for (int j = 0; j < numSort; j++) {
      sorts.add(randomFrom(SORT_FIELDS)+ " " + (random().nextBoolean()? "asc" : "desc"));
    }
    return Strings.join(sorts, ',');
  }

  private String randomDocIds(int numDocs) {
    List<Integer> docIds = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      docIds.add(random().nextInt(NUM_DOCS));
    }
    return Strings.join(docIds, ',');
  }

  private ModifiableSolrParams newBaseParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (random().nextInt(100) < 4) {
      // dense collapse
      params.add("q", "*:*");
    } else {
      // sparse collapse
      params.add("q", "term_s:" + randomFrom(TERMS));
    }

    if (random().nextInt(100) < 20) {
      params.add("qt", "/elevate");
      params.add("forceElevation", "true");
      params.add("elevateIds", randomDocIds(5));
    }
    return params;
  }

  public void testPerfOfPickGroupHeadBySort() throws Exception {
    List<ModifiableSolrParams> paramsList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      ModifiableSolrParams params = newBaseParams();
      if (random().nextInt(100) < 20) {
        params.add("fq", "{!collapse field=group_s sort=$sort}");
      } else {
        params.add("fq", "{!collapse field=group_s sort='"+ randomSorts() +"'}");
      }
      params.add("sort", randomSorts());
      paramsList.add(params);
    }
    benchmark(paramsList);
  }

  private void benchmark(List<ModifiableSolrParams> paramsList) {
    Timer timer = new Timer();
    Histogram histogram = new Histogram(new UniformReservoir());
    for (int i = 0; i < 10000; i++) {
      ModifiableSolrParams params = randomFrom(paramsList);
      try (Timer.Context context = timer.time()) {
        histogram.update(Runtime.getRuntime().totalMemory());
        assertQ(req(params));
        histogram.update(Runtime.getRuntime().totalMemory());
      }
    }
    log.info("avgTimePerRequest:{}", timer.getSnapshot().getMean());
    log.info("avgHeapSize:{}", histogram.getSnapshot().getMean());
    StatsCache cache = h.getCore().createStatsCache();
    assertEquals(0, cache.getCacheMetrics().lookups.longValue());
  }

  public void testPerfPickingGroupHeadByScore() throws Exception {
    List<ModifiableSolrParams> paramsList = new ArrayList<>();
    List<String> nullPolicies = Arrays.asList("","nullPolicy=expand", "nullPolicy=collapse");
    //min max only support int and float field
    List<String> minFields = Arrays.asList("min=test_i", "min=test_f", "min=field(test_i)", "min=cscore()","min=sum(cscore(),field(test_i))");
    List<String> maxFields = Arrays.asList("max=test_i", "max=test_f", "max=field(test_i)", "max=cscore()","max=sum(cscore(),field(test_i))");
    for (int i = 0; i < 1000; i++) {
      ModifiableSolrParams params = newBaseParams();
      String groupField = randomFrom(GROUP_FIELDS);
      String hint = "";
      if (groupField.contains("_s")) {
        hint = randomFrom(HINTS);
      }
      String minMaxHead = "";
      //TODO no commit add nullPolicies
      if (random().nextInt(100) < 20) {
        minMaxHead = randomFrom(minFields);
      } else if (random().nextInt(100) < 20) {
        minMaxHead = randomFrom(maxFields);
      }
      params.add("fq", "{!collapse field="+groupField+" "+hint+" "+minMaxHead+"}");

      if (random().nextBoolean()) {
        params.add("bf", "field(test_i)");
        params.add("defType", "edismax");
      }

      if (random().nextBoolean()) {
        params.add("sort", randomSorts());
      }

      paramsList.add(params);
    }

    benchmark(paramsList);
  }
}
