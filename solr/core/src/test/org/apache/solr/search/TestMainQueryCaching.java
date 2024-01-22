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

import static org.apache.solr.common.util.Utils.fromJSONString;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verify caching interactions between main query and filterCache */
public class TestMainQueryCaching extends SolrTestCaseJ4 {

  private static final int MOST_DOCS = 100;
  private static final long ALL_DOCS = MOST_DOCS + 1;
  private static final String TEST_UFFSQ_PROPNAME = "solr.test.useFilterForSortedQuery";
  static String RESTORE_UFFSQ_PROP;
  private static final String TEST_QRC_WINDOW_SIZE_PROPNAME = "solr.test.queryResultWindowSize";
  static String RESTORE_QRC_WINDOW_SIZE_PROP;
  static boolean USE_FILTER_FOR_SORTED_QUERY;

  @BeforeClass
  public static void beforeClass() throws Exception {
    USE_FILTER_FOR_SORTED_QUERY = random().nextBoolean();
    RESTORE_UFFSQ_PROP =
        System.setProperty(TEST_UFFSQ_PROPNAME, Boolean.toString(USE_FILTER_FOR_SORTED_QUERY));
    RESTORE_QRC_WINDOW_SIZE_PROP = System.setProperty(TEST_QRC_WINDOW_SIZE_PROPNAME, "0");
    initCore("solrconfig-deeppaging.xml", "schema-sorts.xml");
    createIndex();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (RESTORE_UFFSQ_PROP == null) {
      System.clearProperty(TEST_UFFSQ_PROPNAME);
    } else {
      System.setProperty(TEST_UFFSQ_PROPNAME, RESTORE_UFFSQ_PROP);
    }
    if (RESTORE_QRC_WINDOW_SIZE_PROP == null) {
      System.clearProperty(TEST_QRC_WINDOW_SIZE_PROPNAME);
    } else {
      System.setProperty(TEST_QRC_WINDOW_SIZE_PROPNAME, RESTORE_QRC_WINDOW_SIZE_PROP);
    }
  }

  public static void createIndex() {
    for (int i = 0; i < MOST_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i), "str", "d" + i));
      if (random().nextInt(MOST_DOCS) == 0) {
        assertU(commit()); // sometimes make multiple segments
      }
    }
    // add an extra doc to distinguish scoring query from `*:*`
    assertU(adoc("id", Long.toString(MOST_DOCS), "str", "e" + MOST_DOCS));
    assertU(commit());
  }

  @Before
  public void beforeTest() throws Exception {
    // testing caching, it's far simpler to just reload the core every time to prevent
    // subsequent requests from affecting each other
    h.reload();
  }

  private static long coreToInserts(SolrCore core, String cacheName) {
    return (long)
        ((MetricsMap)
                ((SolrMetricManager.GaugeWrapper<?>)
                        core.getCoreMetricManager()
                            .getRegistry()
                            .getMetrics()
                            .get("CACHE.searcher.".concat(cacheName)))
                    .getGauge())
            .getValue()
            .get("inserts");
  }

  private static long coreToSortCount(SolrCore core, String skipOrFull) {
    return
        ((SolrMetricManager.GaugeWrapper<Long>)
                core.getCoreMetricManager()
                    .getRegistry()
                    .getMetrics()
                    .get("SEARCHER.searcher." + skipOrFull + "SortCount"))
            .getGauge()
            .getValue();
  }

  private static long coreToMatchAllDocsInsertCount(SolrCore core) {
    return (long) coreToLiveDocsCacheMetrics(core).get("inserts");
  }

  private static Map<String, Object> coreToLiveDocsCacheMetrics(SolrCore core) {
    return ((MetricsMap)
        (((SolrMetricManager.GaugeWrapper<?>)
                    core.getCoreMetricManager()
                        .getRegistry()
                        .getMetrics()
                        .get("SEARCHER.searcher.liveDocsCache"))
                .getGauge()).getValue()).getValue();
  }

  private static final String SCORING_QUERY = "str:d*";

  // wrapped as a ConstantScoreQuery
  private static final String CONSTANT_SCORE_QUERY = "(" + SCORING_QUERY + ")^=1.0";

  private static final String MATCH_ALL_DOCS_QUERY = "*:*";

  private static final String[] ALL_QUERIES =
      new String[] {SCORING_QUERY, CONSTANT_SCORE_QUERY, MATCH_ALL_DOCS_QUERY};

  @Test
  public void testScoringQuery() throws Exception {
    // plain request should have no caching or sorting optimization
    String response = JQ(req("q", SCORING_QUERY, "indent", "true"));
    assertMetricCounts(response, false, 0, 1, 0);
  }

  @Test
  public void testConstantScoreFlScore() throws Exception {
    // explicitly requesting scores should unconditionally disable caching and sorting optimizations
    String response =
        JQ(
            req(
                "q",
                CONSTANT_SCORE_QUERY,
                "indent",
                "true",
                "rows",
                "0",
                "fl",
                "id,score",
                "sort",
                (random().nextBoolean() ? "id asc" : "score desc")));
    assertMetricCounts(response, false, 0, 1, 0);
  }

  @Test
  public void testScoringQueryNonScoreSort() throws Exception {
    // plain request with no score in sort should consult filterCache, but need full sorting
    String response = JQ(req("q", SCORING_QUERY, "indent", "true", "sort", "id asc"));
    assertMetricCounts(response, false, USE_FILTER_FOR_SORTED_QUERY ? 1 : 0, 1, 0);
  }

  @Test
  public void testScoringQueryZeroRows() throws Exception {
    // always hit cache, optimize sort because rows=0
    String response =
        JQ(
            req(
                "q",
                SCORING_QUERY,
                "indent",
                "true",
                "rows",
                "0",
                "sort",
                (random().nextBoolean() ? "id asc" : "score desc")));
    final int insertAndSkipCount = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
    assertMetricCounts(
        response,
        false,
        insertAndSkipCount,
        USE_FILTER_FOR_SORTED_QUERY ? 0 : 1,
        insertAndSkipCount);
  }

  @Test
  public void testConstantScoreSortByScore() throws Exception {
    // hit cache and skip sort because constant score query
    String response = JQ(req("q", CONSTANT_SCORE_QUERY, "indent", "true"));
    final int insertAndSkipCount = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
    assertMetricCounts(
        response,
        false,
        insertAndSkipCount,
        USE_FILTER_FOR_SORTED_QUERY ? 0 : 1,
        insertAndSkipCount);
  }

  @Test
  public void testConstantScoreNonScoreSort() throws Exception {
    // consult filterCache because constant score query, but no skip sort (because sort-by-id)
    String response = JQ(req("q", CONSTANT_SCORE_QUERY, "indent", "true", "sort", "id asc"));
    assertMetricCounts(response, false, USE_FILTER_FOR_SORTED_QUERY ? 1 : 0, 1, 0);
  }

  /**
   * As {@link #testConstantScoreNonScoreSort} (though an analogous test could be written
   * corresponding to {@link #testConstantScoreSortByScore()}, etc...); but with an additional
   * constant-score clause that causes the associated DocSet, (if {@link
   * #USE_FILTER_FOR_SORTED_QUERY}==true) to be cached as equivalent to MatchAllDocsQuery/liveDocs,
   * _in addition to_ in the filterCache.
   *
   * <p>This is an edge case, but it's the behavior we want, and despite there being two entries,
   * the actual DocSet will be the same (`==`) in both locations (liveDocs and filterCache)
   */
  @Test
  public void testConstantScoreMatchesAllDocsNonScoreSort() throws Exception {
    String response =
        JQ(
            req(
                "q",
                CONSTANT_SCORE_QUERY + " OR (str:e*)^=4.0",
                "indent",
                "true",
                "sort",
                "id asc"));
    assertMetricCounts(
        response, USE_FILTER_FOR_SORTED_QUERY, USE_FILTER_FOR_SORTED_QUERY ? 1 : 0, 1, 0, ALL_DOCS);
  }

  @Test
  public void testMatchAllDocsPlain() throws Exception {
    // plain request with "score" sort should skip sort even if `rows` requested
    Random r = random();
    int[] counters = new int[2];
    for (int i = 1; i < ALL_DOCS + 10; i++) {
      // gradually expand the offset, otherwise we'd find that queryResultCache would intercept
      // requests, avoiding exercising the behavior we're most interested in
      String offset = Integer.toString(r.nextInt(i));
      // keep the window small (again, to avoid queryResultCache intercepting requests)
      String rows = Integer.toString(r.nextInt(2));
      String response =
          JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "start", offset, "rows", rows));
      assertMetricCounts(response, counters);
    }
  }

  private static void assertMetricCounts(String response, int[] expectCounters) {
    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    SolrCore core = h.getCore();
    assertEquals("Bad matchAllDocs insert count", 1, coreToMatchAllDocsInsertCount(core));
    assertEquals("Bad filterCache insert count", 0, coreToInserts(core, "filterCache"));
    assertEquals("Bad full sort count", 0, coreToSortCount(core, "full"));
    assertEquals("Should have exactly " + ALL_DOCS, ALL_DOCS, (body.get("numFound")));
    long queryCacheInsertCount = coreToInserts(core, "queryResultCache");
    if (queryCacheInsertCount == expectCounters[0]) {
      // should be a hit, so all insert/sort-count metrics remain unchanged.
    } else {
      assertEquals(++expectCounters[0], queryCacheInsertCount);
      expectCounters[1]++;
    }
    assertEquals("Bad skip sort count", expectCounters[1], coreToSortCount(core, "skip"));
  }

  @Test
  public void testMatchAllDocsFlScore() throws Exception {
    // explicitly requesting scores should unconditionally disable all cache consultation and sort
    // optimization
    String response =
        JQ(
            req(
                "q",
                MATCH_ALL_DOCS_QUERY,
                "indent",
                "true",
                "rows",
                "0",
                "fl",
                "id,score",
                "sort",
                (random().nextBoolean() ? "id asc" : "score desc")));
    /*
    NOTE: pretend we're not MatchAllDocs, from the perspective of `assertMetricsCounts(...)`
    We're specifically choosing `*:*` here because we want a query that would _definitely_ hit
    our various optimizations, _but_ for the fact that we explicitly requested `score` to be
    returned. This would be a bit of an anti-pattern in "real life", but it's useful (e.g., in
    tests) to have this case just disable the optimizations.
     */
    assertMetricCounts(response, false, 0, 1, 0, ALL_DOCS);
  }

  @Test
  public void testMatchAllDocsZeroRows() throws Exception {
    // plain request should _always_ skip sort when `rows=0`
    String response =
        JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "rows", "0", "sort", "id asc"));
    assertMetricCounts(response, true, 0, 0, 1);
  }

  @Test
  public void testMatchAllDocsNonScoreSort() throws Exception {
    // plain request _with_ rows and non-score sort should consult cache, but not skip sort
    String response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "sort", "id asc"));
    assertMetricCounts(response, true, 0, 1, 0);
  }

  @Test
  public void testCursorMark() throws Exception {
    String q = pickRandom(ALL_QUERIES);
    boolean includeScoreInSort = random().nextBoolean();
    String response =
        JQ(
            req(
                "q",
                q,
                "indent",
                "true",
                "cursorMark",
                "*",
                "sort",
                includeScoreInSort ? "score desc,id asc" : "id asc"));
    final long expectNumFound = MATCH_ALL_DOCS_QUERY.equals(q) ? ALL_DOCS : MOST_DOCS;
    final boolean consultMatchAllDocs;
    final boolean insertFilterCache;
    if (includeScoreInSort) {
      consultMatchAllDocs = false;
      insertFilterCache = false;
    } else if (MATCH_ALL_DOCS_QUERY.equals(q)) {
      consultMatchAllDocs = true;
      insertFilterCache = false;
    } else {
      consultMatchAllDocs = false;
      insertFilterCache = USE_FILTER_FOR_SORTED_QUERY;
    }
    assertMetricCounts(
        response, consultMatchAllDocs, insertFilterCache ? 1 : 0, 1, 0, expectNumFound);
  }

  @Test
  public void testCursorMarkZeroRows() throws Exception {
    String q = pickRandom(ALL_QUERIES);
    String response =
        JQ(
            req(
                "q",
                q,
                "indent",
                "true",
                "cursorMark",
                "*",
                "rows",
                "0",
                "sort",
                random().nextBoolean() ? "id asc" : "score desc,id asc"));
    final boolean consultMatchAllDocs;
    final boolean insertFilterCache;
    final boolean skipSort;
    if (MATCH_ALL_DOCS_QUERY.equals(q)) {
      consultMatchAllDocs = true;
      insertFilterCache = false;
      skipSort = true;
    } else {
      consultMatchAllDocs = false;
      insertFilterCache = USE_FILTER_FOR_SORTED_QUERY;
      skipSort = USE_FILTER_FOR_SORTED_QUERY;
    }
    assertMetricCounts(
        response,
        consultMatchAllDocs,
        insertFilterCache ? 1 : 0,
        skipSort ? 0 : 1,
        skipSort ? 1 : 0);
  }

  private static void assertMetricCounts(
      String response,
      boolean matchAllDocs,
      long expectFilterCacheInsertCount,
      long expectFullSortCount,
      long expectSkipSortCount) {
    assertMetricCounts(
        response,
        matchAllDocs,
        expectFilterCacheInsertCount,
        expectFullSortCount,
        expectSkipSortCount,
        matchAllDocs ? ALL_DOCS : MOST_DOCS);
  }

  private static void assertMetricCounts(
      String response,
      boolean matchAllDocs,
      long expectFilterCacheInsertCount,
      long expectFullSortCount,
      long expectSkipSortCount,
      long expectNumFound) {
    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    SolrCore core = h.getCore();
    assertEquals(
        "Bad matchAllDocs insert count",
        (matchAllDocs ? 1 : 0),
        coreToMatchAllDocsInsertCount(core));
    assertEquals(
        "Bad filterCache insert count",
        expectFilterCacheInsertCount,
        coreToInserts(core, "filterCache"));
    assertEquals("Bad full sort count", expectFullSortCount, coreToSortCount(core, "full"));
    assertEquals("Bad skip sort count", expectSkipSortCount, coreToSortCount(core, "skip"));
    assertEquals(
        "Should have exactly " + expectNumFound, expectNumFound, (body.get("numFound")));
  }

  @Test
  public void testConcurrentMatchAllDocsInitialization() throws Exception {
    final int nThreads = 20;
    final ExecutorService executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            nThreads, new SolrNamedThreadFactory(getTestName()));
    final Future<?>[] followup = new Future<?>[nThreads];
    for (int i = 0; i < nThreads; i++) {
      final int myI = i;
      followup[i] =
          executor.submit(
              () -> {
                try {
                  /*
                  NOTE: we use cursorMark=* here because it prevents consulting the
                  queryResultCache, which can interfere with DocSet fetching (which
                  is what we care about in this test).
                   */
                  String response =
                      JQ(
                          req(
                              "q",
                              MATCH_ALL_DOCS_QUERY,
                              "request_id",
                              Integer.toString(myI),
                              "cursorMark",
                              "*",
                              "sort",
                              "id asc"));
                  Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
                  Map<?, ?> body = (Map<?, ?>) (res.get("response"));
                  assertEquals(
                      "Should have exactly " + ALL_DOCS, ALL_DOCS, (body.get("numFound")));
                } catch (Exception ex) {
                  throw new RuntimeException(ex);
                }
              });
    }
    try {
      for (Future<?> f : followup) {
        f.get(); // to access exceptions/errors
      }
    } finally {
      executor.shutdown();

      // tasks should already have completed
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
    final SolrCore core = h.getCore();
    Map<String, Object> liveDocsCacheMetrics = coreToLiveDocsCacheMetrics(core);

    // the one and only liveDocs computation
    long inserts = (long) liveDocsCacheMetrics.get("inserts");

    // hits during the initial phase
    long hits = (long) liveDocsCacheMetrics.get("hits");

    long naiveHits = (long) liveDocsCacheMetrics.get("naiveHits");

    assertEquals(1, inserts);
    assertEquals(nThreads - 1, hits + naiveHits);
  }
}
