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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.FacetParams.FACET;
import static org.apache.solr.common.params.FacetParams.FACET_LIMIT;
import static org.apache.solr.common.params.FacetParams.FACET_MISSING;
import static org.apache.solr.common.params.FacetParams.FACET_OFFSET;
import static org.apache.solr.common.params.FacetParams.FACET_OVERREQUEST_COUNT;
import static org.apache.solr.common.params.FacetParams.FACET_OVERREQUEST_RATIO;
import static org.apache.solr.common.params.FacetParams.FACET_PIVOT;
import static org.apache.solr.common.params.FacetParams.FACET_PIVOT_MINCOUNT;
import static org.apache.solr.common.params.FacetParams.FACET_SORT;

/**
 * <p>
 * Randomized testing of Pivot Faceting using SolrCloud.
 * </p>
 * <p>
 * After indexing a bunch of random docs, picks some random fields to pivot facet on, 
 * and then confirms that the resulting counts match the results of filtering on those 
 * values.  This gives us strong assertions on the correctness of the total counts for 
 * each pivot value, but no assertions that the correct "top" counts were chosen.
 * </p>
 * <p>
 * NOTE: this test ignores the control collection and only deals with the 
 * CloudSolrServer - this is because the randomized field values make it very easy for 
 * the term stats to miss values even with the overrequest.
 * (because so many values will tie for "1").  What we care about here is 
 * that the counts we get back are correct and match what we get when filtering on those 
 * constraints.
 * </p>
 *
 *
 *
 */
@SuppressSSL // Too Slow
public class TestCloudPivotFacet extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // because floating point addition can depende on the order of operations, we ignore
  // any stats that can be lossy -- the purpose of testing stats here is just to sanity check
  // that the basic hooks between pivot faceting and stats.field work, and these let us do that
  private static final String USE_STATS = "count=true missing=true min=true max=true";
  
  // param used by test purely for tracing & validation
  private static String TRACE_MIN = "_test_min";
  // param used by test purely for tracing & validation
  private static String TRACE_MISS = "_test_miss";
  // param used by test purely for tracing & validation
  private static String TRACE_SORT = "_test_sort";

  public TestCloudPivotFacet() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
  }
  
  /** 
   * Controls the odds of any given doc having a value in any given field -- as this gets lower, 
   * the counts for "facet.missing" pivots should increase.
   * @see #useField()
   */
  private static int useFieldRandomizedFactor = -1;

  @BeforeClass
  public static void initUseFieldRandomizedFactor() {
    useFieldRandomizedFactor = TestUtil.nextInt(random(), 2, 30);
    log.info("init'ing useFieldRandomizedFactor = {}", useFieldRandomizedFactor);
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  public void test() throws Exception {

    waitForThingsToLevelOut(30000); // TODO: why would we have to wait?
    // 
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    final Set<String> fieldNameSet = new HashSet<>();
    
    // build up a randomized index
    final int numDocs = atLeast(500);
    log.info("numDocs: {}", numDocs);

    for (int i = 1; i <= numDocs; i++) {
      SolrInputDocument doc = buildRandomDocument(i);

      // not efficient, but it guarantees that even if people change buildRandomDocument
      // we'll always have the full list of fields w/o needing to keep code in sync
      fieldNameSet.addAll(doc.getFieldNames());

      cloudClient.add(doc);
    }
    cloudClient.commit();

    fieldNameSet.remove("id");
    assertTrue("WTF, bogus field exists?", fieldNameSet.add("bogus_not_in_any_doc_s"));

    final String[] fieldNames = fieldNameSet.toArray(new String[fieldNameSet.size()]);
    Arrays.sort(fieldNames); // need determinism when picking random fields

    for (int i = 0; i < 5; i++) {

      String q = "*:*";
      if (random().nextBoolean()) {
        q = "id:[* TO " + TestUtil.nextInt(random(),300,numDocs) + "]";
      }
      ModifiableSolrParams baseP = params("rows", "0", "q", q);
      
      if (random().nextBoolean()) {
        baseP.add("fq", "id:[* TO " + TestUtil.nextInt(random(),200,numDocs) + "]");
      }

      final boolean stats = random().nextBoolean();
      if (stats) {
        baseP.add(StatsParams.STATS, "true");
        
        // if we are doing stats, then always generated the same # of STATS_FIELD
        // params, using multiple tags from a fixed set, but with diff fieldName values.
        // later, each pivot will randomly pick a tag.
        baseP.add(StatsParams.STATS_FIELD, "{!key=sk1 tag=st1,st2 "+USE_STATS+"}" +
                  pickRandomStatsFields(fieldNames));
        baseP.add(StatsParams.STATS_FIELD, "{!key=sk2 tag=st2,st3 "+USE_STATS+"}" +
                  pickRandomStatsFields(fieldNames));
        baseP.add(StatsParams.STATS_FIELD, "{!key=sk3 tag=st3,st4 "+USE_STATS+"}" +
                  pickRandomStatsFields(fieldNames));
        // NOTE: there's a chance that some of those stats field names
        // will be the same, but if so, all the better to test that edge case
      }
      
      ModifiableSolrParams pivotP = params(FACET,"true");

      // put our FACET_PIVOT params in a set in case we just happen to pick the same one twice
      LinkedHashSet<String> pivotParamValues = new LinkedHashSet<String>();
      pivotParamValues.add(buildPivotParamValue(buildRandomPivot(fieldNames)));
                 
      if (random().nextBoolean()) {
        pivotParamValues.add(buildPivotParamValue(buildRandomPivot(fieldNames)));
      }
      pivotP.set(FACET_PIVOT, pivotParamValues.toArray(new String[pivotParamValues.size()]));

      // keep limit low - lots of unique values, and lots of depth in pivots
      pivotP.add(FACET_LIMIT, ""+TestUtil.nextInt(random(),1,17));

      // sometimes use an offset
      if (random().nextBoolean()) {
        pivotP.add(FACET_OFFSET, ""+TestUtil.nextInt(random(),0,7));
      }

      if (random().nextBoolean()) {
        String min = ""+TestUtil.nextInt(random(),0,numDocs+10);
        pivotP.add(FACET_PIVOT_MINCOUNT, min);
        // trace param for validation
        baseP.add(TRACE_MIN, min);
      }
      
      if (random().nextBoolean()) {
        String missing = ""+random().nextBoolean();
        pivotP.add(FACET_MISSING, missing);
        // trace param for validation
        baseP.add(TRACE_MISS, missing);
      }

      if (random().nextBoolean()) {
        String sort = random().nextBoolean() ? "index" : "count";
        pivotP.add(FACET_SORT, sort);
        // trace param for validation
        baseP.add(TRACE_SORT, sort);
      }

      // overrequest
      //
      // NOTE: since this test focuses on accuracy of refinement, and doesn't do 
      // control collection comparisons, there isn't a lot of need for excessive
      // overrequesting -- we focus here on trying to exercise the various edge cases
      // involved as different values are used with overrequest
      if (0 == TestUtil.nextInt(random(),0,4)) {
        // we want a decent chance of no overrequest at all
        pivotP.add(FACET_OVERREQUEST_COUNT, "0");
        pivotP.add(FACET_OVERREQUEST_RATIO, "0");
      } else {
        if (random().nextBoolean()) {
          pivotP.add(FACET_OVERREQUEST_COUNT, ""+TestUtil.nextInt(random(),0,5));
        }
        if (random().nextBoolean()) {
          // sometimes give a ratio less then 1, code should be smart enough to deal
          float ratio = 0.5F + random().nextFloat();
          // sometimes go negative
          if (random().nextBoolean()) {
            ratio *= -1;
          }
          pivotP.add(FACET_OVERREQUEST_RATIO, ""+ratio);
        }
      }
      
      assertPivotCountsAreCorrect(baseP, pivotP);
    }
  }

  /**
   * Given some query params, executes the request against the cloudClient and 
   * then walks the pivot facet values in the response, treating each one as a 
   * filter query to assert the pivot counts are correct.
   */
  private void assertPivotCountsAreCorrect(SolrParams baseParams, 
                                           SolrParams pivotParams) 
    throws SolrServerException {
    
    SolrParams initParams = SolrParams.wrapAppended(pivotParams, baseParams);

    log.info("Doing full run: {}", initParams);
    countNumFoundChecks = 0;

    NamedList<List<PivotField>> pivots = null;
    try {
      QueryResponse initResponse = cloudClient.query(initParams);
      pivots = initResponse.getFacetPivot();
      assertNotNull(initParams + " has null pivots?", pivots);
      assertEquals(initParams + " num pivots", 
                   initParams.getParams("facet.pivot").length, pivots.size());
    } catch (Exception e) {
      throw new RuntimeException("init query failed: " + initParams + ": " + 
                                 e.getMessage(), e);
    }
    try {
      for (Map.Entry<String,List<PivotField>> pivot : pivots) {
        final String pivotKey = pivot.getKey();
        // :HACK: for counting the max possible pivot depth
        final int maxDepth = 1 + pivotKey.length() - pivotKey.replace(",","").length();

        assertTraceOk(pivotKey, baseParams, pivot.getValue());

        // NOTE: we can't make any assumptions/assertions about the number of
        // constraints here because of the random data - which means if pivotting is
        // completely broken and there are no constrains this loop could be a No-Op
        // but in that case we just have to trust that DistributedFacetPivotTest
        // will catch it.
        for (PivotField constraint : pivot.getValue()) {
          int depth = assertPivotCountsAreCorrect(pivotKey, baseParams, constraint);
          
          // we can't assert that the depth reached is the same as the depth requested
          // because the fq and/or mincount may have pruned the tree too much
          assertTrue("went too deep: "+depth+": " + pivotKey + " ==> " + pivot,
                     depth <= maxDepth);

        }
      }
    } catch (AssertionError e) {
      throw new AssertionError(initParams + " ==> " + e.getMessage(), e);
    } finally {
      log.info("Ending full run (countNumFoundChecks={}): {}", 
               countNumFoundChecks, initParams);
    }
  }
  
  /**
   * Recursive Helper method for asserting that pivot constraint counts match
   * results when filtering on those constraints. Returns the recursive depth reached 
   * (for sanity checking)
   */
  private int assertPivotCountsAreCorrect(String pivotName,
                                          SolrParams baseParams, 
                                          PivotField constraint) 
    throws SolrServerException {

    SolrParams p = SolrParams.wrapAppended(baseParams,
                                           params("fq", buildFilter(constraint)));
    List<PivotField> subPivots = null;
    try {
      assertPivotData(pivotName, constraint, p); 
      subPivots = constraint.getPivot();
    } catch (Exception e) {
      throw new RuntimeException(pivotName + ": count query failed: " + p + ": " + 
                                 e.getMessage(), e);
    }
    int depth = 0;
    if (null != subPivots) {
      assertTraceOk(pivotName, baseParams, subPivots);

      for (PivotField subPivot : subPivots) {
        depth = assertPivotCountsAreCorrect(pivotName, p, subPivot);
      }
    }
    return depth + 1;
  }

  /**
   * Executes a query and compares the results with the data available in the 
   * {@link PivotField} constraint -- this method is not recursive, and doesn't 
   * check anything about the sub-pivots (if any).
   *
   * @param pivotName pivot name
   * @param constraint filters on pivot
   * @param params base solr parameters
   */
  private void assertPivotData(String pivotName, PivotField constraint, SolrParams params)
      throws SolrServerException, IOException {
    
    SolrParams p = SolrParams.wrapDefaults(params("rows","0"), params);
    QueryResponse res = cloudClient.query(p);
    String msg = pivotName + ": " + p;

    assertNumFound(msg, constraint.getCount(), res);

    if ( p.getBool(StatsParams.STATS, false) ) {
      // only check stats if stats expected
      assertPivotStats(msg, constraint, res);
    }
  }

  /**
   * Compare top level stats in response with stats from pivot constraint
   */
  private void assertPivotStats(String message, PivotField constraint, QueryResponse response) {

    if (null == constraint.getFieldStatsInfo()) {
      // no stats for this pivot, nothing to check

      // TODO: use a trace param to know if/how-many to expect ?
      log.info("No stats to check for => {}", message);
      return;
    }
    
    Map<String, FieldStatsInfo> actualFieldStatsInfoMap = response.getFieldStatsInfo();

    for (FieldStatsInfo pivotStats : constraint.getFieldStatsInfo().values()) {
      String statsKey = pivotStats.getName();

      FieldStatsInfo actualStats = actualFieldStatsInfoMap.get(statsKey);

      if (actualStats == null) {
        // handle case for not found stats (using stats query)
        //
        // these has to be a special case check due to the legacy behavior of "top level" 
        // StatsComponent results being "null" (and not even included in the 
        // getFieldStatsInfo() Map due to specila SolrJ logic) 

        log.info("Requested stats missing in verification query, pivot stats: {}", pivotStats);
        assertEquals("Special Count", 0L, pivotStats.getCount().longValue());
        assertEquals("Special Missing", 
                     constraint.getCount(), pivotStats.getMissing().longValue());

      } else {
        // regular stats, compare everything...

        assert actualStats != null;
        try {
          String msg = " of " + statsKey;
          
          // no wiggle room, these should always be exactly equals, regardless of field type
          assertEquals("Count" + msg, pivotStats.getCount(), actualStats.getCount());
          assertEquals("Missing" + msg, pivotStats.getMissing(), actualStats.getMissing());
          assertEquals("Min" + msg, pivotStats.getMin(), actualStats.getMin());
          assertEquals("Max" + msg, pivotStats.getMax(), actualStats.getMax());

        } catch (AssertionError e) {
          throw new AssertionError("Stats: Pivot[" + pivotStats + "] <==> Actual[" + actualStats + "]  => " + message, e);
        }
      }
    }

    if (constraint.getFieldStatsInfo().containsKey("sk2")) { // cheeseball hack
      // if "sk2" was one of hte stats we computed, then we must have also seen
      // sk1 or sk3 because of the way the tags are fixed
      assertEquals("had stats sk2, but not another stat?", 
                   2, constraint.getFieldStatsInfo().size());
    } else {
      // if we did not see "sk2", then 1 of the others must be alone
      assertEquals("only expected 1 stat",
                   1, constraint.getFieldStatsInfo().size());
      assertTrue("not sk1 or sk3", 
                 constraint.getFieldStatsInfo().containsKey("sk1") ||
                 constraint.getFieldStatsInfo().containsKey("sk3"));
    }

  }

  /**
   * Verify that the PivotFields we're lookin at doesn't violate any of the expected 
   * behaviors based on the <code>TRACE_*</code> params found in the base params
   */
  private void assertTraceOk(String pivotName, SolrParams baseParams, List<PivotField> constraints) {
    if (null == constraints || 0 == constraints.size()) {
      return;
    }
    final int maxIdx = constraints.size() - 1;
      
    final int min = baseParams.getInt(TRACE_MIN, -1);
    final boolean expectMissing = baseParams.getBool(TRACE_MISS, false);
    final boolean checkCount = "count".equals(baseParams.get(TRACE_SORT, "count"));

    int prevCount = Integer.MAX_VALUE;

    for (int i = 0; i <= maxIdx; i++) {
      final PivotField constraint = constraints.get(i);
      final int count = constraint.getCount();

      if (0 < min) {
        assertTrue(pivotName + ": val #"+i +" of " + maxIdx + 
                   ": count("+count+") < facet.mincount("+min+"): " + constraint,
                   min <= count);
      }
      // missing value must always come last, but only if facet.missing was used
      // and may not exist at all (mincount, none missing for this sub-facet, etc...)
      if ((i < maxIdx) || (!expectMissing)) {
        assertNotNull(pivotName + ": val #"+i +" of " + maxIdx + 
                      " has null value: " + constraint,
                      constraint.getValue());
      }
      // if we are expecting count based sort, then the count of each constraint 
      // must be lt-or-eq the count that came before -- or it must be the last value and 
      // be "missing"
      if (checkCount) {
        assertTrue(pivotName + ": val #"+i +" of" + maxIdx + 
                   ": count("+count+") > prevCount("+prevCount+"): " + constraint,
                   ((count <= prevCount)
                    || (expectMissing && i == maxIdx && null == constraint.getValue())));
        prevCount = count;
      }
    }
  }

  /**
   * Given a PivotField constraint, generate a query for the field+value
   * for use in an <code>fq</code> to verify the constraint count
   */
  private static String buildFilter(PivotField constraint) {
    Object value = constraint.getValue();
    if (null == value) {
      // facet.missing, exclude any indexed term
      return "-" + constraint.getField() + ":[* TO *]";
    }
    // otherwise, build up a term filter...
    String prefix = "{!term f=" + constraint.getField() + "}";
    if (value instanceof Date) {
      return prefix + ((Date) value).toInstant();
    } else {
      return prefix + value;
    }
  }


  /**
   * Creates a random facet.pivot param string using some of the specified fieldNames
   */
  private static String buildRandomPivot(String[] fieldNames) {
    final int depth = TestUtil.nextInt(random(), 1, 3);
    String [] fields = new String[depth];
    for (int i = 0; i < depth; i++) {
      // yes this means we might use the same field twice
      // makes it a robust test (especially for multi-valued fields)
      fields[i] = fieldNames[TestUtil.nextInt(random(),0,fieldNames.length-1)];
    }
    return String.join(",", fields);
  }

  /**
   * Picks a random field to use for Stats
   */
  private static String pickRandomStatsFields(String[] fieldNames) {
    // we need to skip boolean fields when computing stats
    String fieldName;
    do {
      fieldName = fieldNames[TestUtil.nextInt(random(),0,fieldNames.length-1)];
    }
    while(fieldName.endsWith("_b") || fieldName.endsWith("_b1")) ;
          
    return fieldName;
  }

  /**
   * Generates a random {@link FacetParams#FACET_PIVOT} value w/ local params 
   * using the specified pivotValue.
   */
  private static String buildPivotParamValue(String pivotValue) {
    // randomly decide which stat tag to use

    // if this is 0, or stats aren't enabled, we'll be asking for a tag that doesn't exist
    // ...which should be fine (just like excluding a tagged fq that doesn't exist)
    final int statTag = TestUtil.nextInt(random(), -1, 4);
      
    if (0 <= statTag) {
      // only use 1 tag name in the 'stats' localparam - see SOLR-6663
      return "{!stats=st"+statTag+"}" + pivotValue;
    } else {
      // statTag < 0 == sanity check the case of a pivot w/o any stats
      return pivotValue;
    }
  }

  /**
   * Creates a document with randomized field values, some of which be missing values, 
   * some of which will be multi-valued (per the schema) and some of which will be 
   * skewed so that small subsets of the ranges will be more common (resulting in an 
   * increased likelihood of duplicate values)
   * 
   * @see #buildRandomPivot
   */
  private static SolrInputDocument buildRandomDocument(int id) {
    SolrInputDocument doc = sdoc("id", id);
    // most fields are in most docs
    // if field is in a doc, then "skewed" chance val is from a dense range
    // (hopefully with lots of duplication)
    for (String prefix : new String[] { "pivot_i", "pivot_ti" }) {
      if (useField()) {
        doc.addField(prefix+"1", skewed(TestUtil.nextInt(random(), 20, 50),
                                        random().nextInt()));
                                        
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, skewed(TestUtil.nextInt(random(), 20, 50), 
                                      random().nextInt()));
        }
      }
    }
    for (String prefix : new String[] { "pivot_l", "pivot_tl" }) {
      if (useField()) {
        doc.addField(prefix+"1", skewed(TestUtil.nextInt(random(), 5000, 5100),
                                        random().nextLong()));
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, skewed(TestUtil.nextInt(random(), 5000, 5100), 
                                      random().nextLong()));
        }
      }
    }
    for (String prefix : new String[] { "pivot_f", "pivot_tf" }) {
      if (useField()) {
        doc.addField(prefix+"1", skewed(1.0F / random().nextInt(13),
                                        random().nextFloat() * random().nextInt()));
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, skewed(1.0F / random().nextInt(13),
                                      random().nextFloat() * random().nextInt()));
        }
      }
    }
    for (String prefix : new String[] { "pivot_d", "pivot_td" }) {
      if (useField()) {
        doc.addField(prefix+"1", skewed(1.0D / random().nextInt(19),
                                        random().nextDouble() * random().nextInt()));
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, skewed(1.0D / random().nextInt(19),
                                      random().nextDouble() * random().nextInt()));
        }
      }
    }
    for (String prefix : new String[] { "pivot_dt", "pivot_tdt" }) {
      if (useField()) {
        doc.addField(prefix+"1", skewed(randomSkewedDate(), randomDate()));
                                        
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, skewed(randomSkewedDate(), randomDate()));
                                      
        }
      }
    }
    {
      String prefix = "pivot_b";
      if (useField()) {
        doc.addField(prefix+"1", random().nextBoolean() ? "t" : "f");
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, random().nextBoolean() ? "t" : "f");
        }
      }
    }
    for (String prefix : new String[] { "pivot_x_s", "pivot_y_s", "pivot_z_s"}) {
      if (useField()) {
        doc.addField(prefix+"1", skewed(TestUtil.randomSimpleString(random(), 1, 1),
                                        randomXmlUsableUnicodeString()));
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, skewed(TestUtil.randomSimpleString(random(), 1, 1),
                                      randomXmlUsableUnicodeString()));
        }
      }
    }

    //
    // for the remaining fields, make every doc have a value in a dense range
    //

    for (String prefix : new String[] { "dense_pivot_x_s", "dense_pivot_y_s" }) {
      if (useField()) {
        doc.addField(prefix+"1", TestUtil.randomSimpleString(random(), 1, 1));
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, TestUtil.randomSimpleString(random(), 1, 1));
        }
      }
    }
    for (String prefix : new String[] { "dense_pivot_i", "dense_pivot_ti" }) {
      if (useField()) {
        doc.addField(prefix+"1", TestUtil.nextInt(random(), 20, 50));
      }
      if (useField()) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          doc.addField(prefix, TestUtil.nextInt(random(), 20, 50));
        }
      }
    }

    return doc;
  }

  /** 
   * Similar to usually() but we want it to happen just as often regardless
   * of test multiplier and nightly status
   *
   * @see #useFieldRandomizedFactor
   */
  private static boolean useField() {
    assert 0 < useFieldRandomizedFactor;
    return 0 != TestUtil.nextInt(random(), 0, useFieldRandomizedFactor);
  }
  
  /**
   * Asserts the number of docs found in the response
   */
  private void assertNumFound(String msg, int expected, QueryResponse response) {

    countNumFoundChecks++;

    assertEquals(msg, expected, response.getResults().getNumFound());
  }

  /**
   * @see #assertNumFound
   * @see #assertPivotCountsAreCorrect(SolrParams,SolrParams)
   */
  private int countNumFoundChecks = 0;

}
