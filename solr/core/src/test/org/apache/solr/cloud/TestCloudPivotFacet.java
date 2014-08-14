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

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.schema.TrieDateField;

import static org.apache.solr.common.params.FacetParams.*;

import org.apache.commons.lang.StringUtils;

import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Date;

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

  public static Logger log = LoggerFactory.getLogger(TestCloudPivotFacet.class);

  // param used by test purely for tracing & validation
  private static String TRACE_MIN = "_test_min";
  // param used by test purely for tracing & validation
  private static String TRACE_MISS = "_test_miss";
  // param used by test purely for tracing & validation
  private static String TRACE_SORT = "_test_sort";

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

  @Override
  public void doTest() throws Exception {
    waitForThingsToLevelOut(30000); // TODO: why whould we have to wait?
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

      // not efficient, but it garuntees that even if people change buildRandomDocument
      // we'll always have the full list of fields w/o needing to keep code in sync
      fieldNameSet.addAll(doc.getFieldNames());

      cloudClient.add(doc);
    }
    cloudClient.commit();

    fieldNameSet.remove("id");
    assertTrue("WTF, bogus field exists?", fieldNameSet.add("bogus_not_in_any_doc_s"));

    final String[] fieldNames = fieldNameSet.toArray(new String[fieldNameSet.size()]);
    Arrays.sort(fieldNames); // need determinism for buildRandomPivot calls


    for (int i = 0; i < 5; i++) {

      String q = "*:*";
      if (random().nextBoolean()) {
        q = "id:[* TO " + TestUtil.nextInt(random(),300,numDocs) + "]";
      }
      ModifiableSolrParams baseP = params("rows", "0", "q", q);
      
      if (random().nextBoolean()) {
        baseP.add("fq", "id:[* TO " + TestUtil.nextInt(random(),200,numDocs) + "]");
      }

      ModifiableSolrParams pivotP = params(FACET,"true",
                                           FACET_PIVOT, buildRandomPivot(fieldNames));
      if (random().nextBoolean()) {
        pivotP.add(FACET_PIVOT, buildRandomPivot(fieldNames));
      }

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
   * Recursive Helper method for asserting that pivot constraint counds match
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
      assertNumFound(pivotName, constraint.getCount(), p);
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
      return prefix + TrieDateField.formatExternal((Date)value);
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
    return StringUtils.join(fields, ",");
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
   * Asserts the number of docs matching the SolrParams aganst the cloudClient
   */
  private void assertNumFound(String msg, int expected, SolrParams p) 
    throws SolrServerException {

    countNumFoundChecks++;

    SolrParams params = SolrParams.wrapDefaults(params("rows","0"), p);
    assertEquals(msg + ": " + params, 
                 expected, cloudClient.query(params).getResults().getNumFound());
  }

  /**
   * @see #assertNumFound
   * @see #assertPivotCountsAreCorrect(SolrParams,SolrParams)
   */
  private int countNumFoundChecks = 0;
}
