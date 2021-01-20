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
package org.apache.solr.core;

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * Test that checks that long running queries are exited by Solr using the
 * SolrQueryTimeoutImpl implementation.
 */
public class ExitableDirectoryReaderTest extends SolrTestCaseJ4 {
  
  static int NUM_DOCS = 100;
  static final String assertionString = "/response/numFound=="+ NUM_DOCS;
  static final String failureAssertionString = "/responseHeader/partialResults==true]";
  static final String longTimeout="10000";
  static final String sleep = "2";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-delaying-component.xml", "schema_latest.xml");
    createIndex();
  }

  public static void createIndex() {
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i), "name", "a" + i + " b" + i + " c" + i + " d"+i + " e" + i));
      if (random().nextInt(NUM_DOCS) == 0) {
        assertU(commit());  // sometimes make multiple segments
      }
    }
    assertU(commit());
  }

  @Test
  public void testPrefixQuery() throws Exception {
    String q = "name:a*";
    assertJQ(req("q", q,  "timeAllowed", "1", "sleep", sleep), failureAssertionString);

    // do the same query and test for both success, and that the number of documents matched (i.e. make sure no partial results were cached)
    assertJQ(req("q", q, "timeAllowed", longTimeout), assertionString);

    // this time we should get a query cache hit and hopefully no exception?  this may change in the future if time checks are put into other places.
    assertJQ(req("q", q,  "timeAllowed", "1", "sleep", sleep), assertionString);

    // now do the same for the filter cache
    assertJQ(req("q","*:*", "fq",q, "timeAllowed", "1", "sleep", sleep), failureAssertionString);

    // make sure that the result succeeds this time, and that a bad filter wasn't cached
    assertJQ(req("q","*:*", "fq",q, "timeAllowed", longTimeout), assertionString);

    // test that Long.MAX_VALUE works
    assertJQ(req("q","name:b*", "timeAllowed",Long.toString(Long.MAX_VALUE)), assertionString);

    // negative timeAllowed should disable timeouts.
    assertJQ(req("q", "name:c*", "timeAllowed", "-7"), assertionString);
  }




  // There are lots of assumptions about how/when cache entries should be changed in this method. The
  // simple case above shows the root problem without the confusion. testFilterSimpleCase should be
  // removed once it is running and this test should be un-ignored and the assumptions verified.
  // With all the weirdness, I'm not going to vouch for this test. Feel free to change it.
  @Test
  public void testCacheAssumptions() throws Exception {
    String fq= "name:d*";
    SolrCore core = h.getCore();
    MetricsMap filterCacheStats = (MetricsMap)((SolrMetricManager.GaugeWrapper)core.getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.filterCache")).getGauge();
    long fqInserts = (long) filterCacheStats.getValue().get("inserts");

    MetricsMap queryCacheStats = (MetricsMap)((SolrMetricManager.GaugeWrapper)core.getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.queryResultCache")).getGauge();
    long qrInserts = (long) queryCacheStats.getValue().get("inserts");

    // This gets 0 docs back. Use 10000 instead of 1 for timeAllowed and it gets 100 back and the for loop below
    // succeeds.
    String response = JQ(req("q", "*:*", "fq", fq, "indent", "true", "timeAllowed", "1", "sleep", sleep));
    @SuppressWarnings({"rawtypes"})
    Map res = (Map) fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    Map body = (Map) (res.get("response"));
    assertTrue("Should have fewer docs than " + NUM_DOCS, (long) (body.get("numFound")) < NUM_DOCS);

    @SuppressWarnings({"rawtypes"})
    Map header = (Map) (res.get("responseHeader"));
    assertTrue("Should have partial results", (Boolean) (header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY)));

    assertEquals("Should NOT have inserted partial results in the cache!",
        (long) queryCacheStats.getValue().get("inserts"), qrInserts);

    assertEquals("Should NOT have another insert", fqInserts, (long) filterCacheStats.getValue().get("inserts"));

    // At the end of all this, we should have no hits in the queryResultCache.
    response = JQ(req("q", "*:*", "fq", fq, "indent", "true", "timeAllowed", longTimeout));

    // Check that we did insert this one.
    assertEquals("Hits should still be 0", (long) filterCacheStats.getValue().get("hits"), 0L);
    assertEquals("Inserts should be bumped", (long) filterCacheStats.getValue().get("inserts"), fqInserts + 1);

    res = (Map) fromJSONString(response);
    body = (Map) (res.get("response"));

    assertEquals("Should have exactly " + NUM_DOCS, (long) (body.get("numFound")), NUM_DOCS);
    header = (Map) (res.get("responseHeader"));
    assertTrue("Should NOT have partial results", header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY) == null);
  }

  // When looking at a problem raised on the user's list I ran across this anomaly with timeAllowed
  // This tests for the second query NOT returning partial results, along with some other
  @Test
  public void testQueryResults() throws Exception {
    String q = "name:e*";
    SolrCore core = h.getCore();
    MetricsMap queryCacheStats = (MetricsMap)((SolrMetricManager.GaugeWrapper)core.getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.queryResultCache")).getGauge();
    Map<String,Object> nl = queryCacheStats.getValue();
    long inserts = (long) nl.get("inserts");

    String response = JQ(req("q", q, "indent", "true", "timeAllowed", "1", "sleep", sleep));

    // The queryResultCache should NOT get an entry here.
    nl = queryCacheStats.getValue();
    assertEquals("Should NOT have inserted partial results!", inserts, (long) nl.get("inserts"));

    @SuppressWarnings({"rawtypes"})
    Map res = (Map) fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    Map body = (Map) (res.get("response"));
    @SuppressWarnings({"rawtypes"})
    Map header = (Map) (res.get("responseHeader"));

    assertTrue("Should have fewer docs than " + NUM_DOCS, (long) (body.get("numFound")) < NUM_DOCS);
    assertTrue("Should have partial results", (Boolean) (header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY)));

    response = JQ(req("q", q, "indent", "true", "timeAllowed", longTimeout));

    // Check that we did insert this one.
    Map<String,Object> nl2 = queryCacheStats.getValue();
    assertEquals("Hits should still be 0", (long) nl.get("hits"), (long) nl2.get("hits"));
    assertTrue("Inserts should be bumped", inserts < (long) nl2.get("inserts"));

    res = (Map) fromJSONString(response);
    body = (Map) (res.get("response"));
    header = (Map) (res.get("responseHeader"));

    assertEquals("Should have exactly " + NUM_DOCS, NUM_DOCS, (long) (body.get("numFound")));
    Boolean test = (Boolean) (header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
    if (test != null) {
      assertFalse("Should NOT have partial results", test);
    }
  }
}


