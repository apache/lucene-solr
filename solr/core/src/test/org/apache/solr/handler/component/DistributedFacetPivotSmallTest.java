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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.AssertionFailedError;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class DistributedFacetPivotSmallTest extends BaseDistributedSearchTestCase {

  public DistributedFacetPivotSmallTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
  }
  
  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    
    del("*:*");

    // NOTE: we use the literal (4 character) string "null" as a company name
    // to help ensure there isn't any bugs where the literal string is treated as if it 
    // were a true NULL value.
    index(id, 19, "place_t", "cardiff dublin", "company_t", "microsoft polecat", "price_ti", "15");
    index(id, 20, "place_t", "dublin", "company_t", "polecat microsoft null", "price_ti", "19",
          // this is the only doc to have solo_* fields, therefore only 1 shard has them
          // TODO: add enum field - blocked by SOLR-6682
          "solo_i", 42, "solo_s", "lonely", "solo_dt", "1976-03-06T01:23:45Z");
    index(id, 21, "place_t", "london la dublin", "company_t",
        "microsoft fujitsu null polecat", "price_ti", "29");
    index(id, 22, "place_t", "krakow london cardiff", "company_t",
        "polecat null bbc", "price_ti", "39");
    index(id, 23, "place_t", "london", "company_t", "", "price_ti", "29");
    index(id, 24, "place_t", "la", "company_t", "");
    index(id, 25, "company_t", "microsoft polecat null fujitsu null bbc", "price_ti", "59");
    index(id, 26, "place_t", "krakow", "company_t", "null");
    index(id, 27, "place_t", "krakow cardiff dublin london la", 
          "company_t", "null microsoft polecat bbc fujitsu");
    index(id, 28, "place_t", "cork", "company_t", "fujitsu rte");
    commit();
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    
    
    final ModifiableSolrParams params = new ModifiableSolrParams();
    setDistributedParams(params);
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");
   
    
    QueryResponse rsp = queryServer(params);
    
    List<PivotField> expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    List<PivotField> expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "microsoft", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "null", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    List<PivotField> expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat", 4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft", 4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "bbc", 1, null));
    List<PivotField> expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "microsoft", 2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    List<PivotField> expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "microsoft", 2,null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "null", 2, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "bbc", 1, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,null));
    List<PivotField> expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",2, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "microsoft", 1, null));
    List<PivotField> expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    expectedCorkPivots.add(new ComparablePivotField("company_t", "rte", 1, null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4, expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,  expectedCardiffPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4, expectedLondonPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3, expectedLAPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3, expectedKrakowPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1, expectedCorkPivots));
    
    
    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    // Useful to check for errors, orders lists and does toString() equality
    // check
    testOrderedPivotsStringEquality(expectedPlacePivots, placePivots);
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test sorting by count
    
    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
    
    rsp = queryServer(params);
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    testCountSorting(placePivots);
    
    // Test limit
    
    params.set(FacetParams.FACET_LIMIT, 2);
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3,
        null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test individual facet.limit values
    params.remove(FacetParams.FACET_LIMIT);
    
    params.set("f.place_t." + FacetParams.FACET_LIMIT, 1);
    params.set("f.company_t." + FacetParams.FACET_LIMIT, 4);
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",4, null));    
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null",3, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",2, null));
    
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    
    expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    
    expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3, null));
    
    expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    
    expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4, expectedDublinPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");    
    assertEquals(expectedPlacePivots, placePivots);
    
    params.remove("f.company_t." + FacetParams.FACET_LIMIT);
    params.remove("f.place_t." + FacetParams.FACET_LIMIT);
    params.set(FacetParams.FACET_LIMIT, 2);
    
    // Test facet.missing=true with diff sorts

    index("id",777); // NOTE: id=25 has no place as well
    commit();

    SolrParams missingA = params( "q", "*:*",
                                  "rows", "0",
                                  "facet","true",
                                  "facet.pivot","place_t,company_t",
                                  "f.place_t.facet.mincount", "2",
                                  // default facet.sort
                                  FacetParams.FACET_MISSING, "true" );
    SolrParams missingB = SolrParams.wrapDefaults(missingA, 
                                                  params(FacetParams.FACET_LIMIT, "4",
                                                         "facet.sort", "index"));
    for (SolrParams p : new SolrParams[] { missingA, missingB }) {
      // in either case, the last pivot option should be the same
      rsp = query( p );
      placePivots = rsp.getFacetPivot().get("place_t,company_t");
      assertTrue("not enough values for pivot: " + p + " => " + placePivots, 
                 1 < placePivots.size());
      PivotField missing = placePivots.get(placePivots.size()-1);
      assertNull("not the missing place value: " + p, missing.getValue());
      assertEquals("wrong missing place count: " + p, 2, missing.getCount());
      assertTrue("not enough sub-pivots for missing place: "+ p +" => " + missing.getPivot(),
                 1 < missing.getPivot().size());
      missing = missing.getPivot().get(missing.getPivot().size()-1);
      assertNull("not the missing company value: " + p, missing.getValue());
      assertEquals("wrong missing company count: " + p, 1, missing.getCount());
      assertNull("company shouldn't have sub-pivots: " + p, missing.getPivot());
    }

    // sort=index + mincount + limit
    for (SolrParams variableParams : new SolrParams[] { 
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count","0",
               "facet.overrequest.ratio","0"),
        params()                                  }) {


      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.pivot","company_t",
                                                      "facet.sort", "index",
                                                      "facet.pivot.mincount", "4",
                                                      "facet.limit", "4"),
                                              variableParams );

      try {
        List<PivotField> pivots = query( p ).getFacetPivot().get("company_t");
        assertEquals(4, pivots.size());
        assertEquals("fujitsu", pivots.get(0).getValue());
        assertEquals(4, pivots.get(0).getCount());
        assertEquals("microsoft", pivots.get(1).getValue());
        assertEquals(5, pivots.get(1).getCount());
        assertEquals("null", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());
        assertEquals("polecat", pivots.get(3).getValue());
        assertEquals(6, pivots.get(3).getCount());
        
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // sort=index + mincount + limit + offset
    for (SolrParams variableParams : new SolrParams[] { 
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count","0",
               "facet.overrequest.ratio","0"),
        params()                                  }) {

      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.pivot","company_t",
                                                      "facet.sort", "index",
                                                      "facet.pivot.mincount", "4",
                                                      "facet.offset", "1",
                                                      "facet.limit", "4"),
                                              variableParams );
      try {
        List<PivotField> pivots = query( p ).getFacetPivot().get("company_t");
        assertEquals(3, pivots.size()); // asked for 4, but not enough meet the mincount
        assertEquals("microsoft", pivots.get(0).getValue());
        assertEquals(5, pivots.get(0).getCount());
        assertEquals("null", pivots.get(1).getValue());
        assertEquals(6, pivots.get(1).getCount());
        assertEquals("polecat", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }

    }
      
    // sort=index + mincount + limit + offset (more permutations)
    for (SolrParams variableParams : new SolrParams[] { 
        // all of these combinations should result in the same first value
        params("facet.pivot.mincount", "4",
               "facet.offset", "2"),
        params("facet.pivot.mincount", "5",
               "facet.offset", "1"),
        params("facet.pivot.mincount", "6",
               "facet.offset", "0" )                  }) {
      
      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.limit","1",
                                                      "facet.sort","index",
                                                      "facet.overrequest.ratio","0",
                                                      "facet.pivot", "company_t"),
                                              variableParams );

      try {
        List<PivotField> pivots = query( p ).getFacetPivot().get("company_t");
        assertEquals(1, pivots.size());
        assertEquals(pivots.toString(), "null", pivots.get(0).getValue());
        assertEquals(pivots.toString(), 6, pivots.get(0).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    doTestDeepPivotStats(false); // all price stats
    doTestDeepPivotStats(true); // just the mean price stat

    doTestPivotStatsFromOneShard();

    testFacetPivotRange();

    testFacetPivotQuery();

    testNegativeFacetQuery();

    testNegativeRangeQuery();

    testPivotFacetRangeAndQuery();
  }

  /**
   * @param justMean - only the mean stat is requested/computed
   */
  private void doTestDeepPivotStats(boolean justMean) throws Exception {
    SolrParams params = params("q", "*:*", "rows", "0", 
                               "facet", "true", "stats", "true", 
                               "facet.pivot", "{!stats=s1}place_t,company_t", 
                               "stats.field", ("{!key=avg_price tag=s1 "+
                                               (justMean ? "mean=true" : "") +"}price_ti"));
    QueryResponse rsp = query(params);

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");

    PivotField dublinPivotField = placePivots.get(0);
    assertEquals("dublin", dublinPivotField.getValue());
    assertEquals(4, dublinPivotField.getCount());

    PivotField microsoftPivotField = dublinPivotField.getPivot().get(0);
    assertEquals("microsoft", microsoftPivotField.getValue());
    assertEquals(4, microsoftPivotField.getCount());

    FieldStatsInfo dublinMicrosoftStatsInfo = microsoftPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals(21.0, (double) dublinMicrosoftStatsInfo.getMean(), 0.1E-7);
    if (justMean) {
      assertNull(dublinMicrosoftStatsInfo.getMin());
      assertNull(dublinMicrosoftStatsInfo.getMax());
      assertNull(dublinMicrosoftStatsInfo.getCount());
      assertNull(dublinMicrosoftStatsInfo.getMissing());
      assertNull(dublinMicrosoftStatsInfo.getSum());
      assertNull(dublinMicrosoftStatsInfo.getSumOfSquares());
      assertNull(dublinMicrosoftStatsInfo.getStddev());
    } else {
      assertEquals(15.0, dublinMicrosoftStatsInfo.getMin());
      assertEquals(29.0, dublinMicrosoftStatsInfo.getMax());
      assertEquals(3, (long) dublinMicrosoftStatsInfo.getCount());
      assertEquals(1, (long) dublinMicrosoftStatsInfo.getMissing());
      assertEquals(63.0, dublinMicrosoftStatsInfo.getSum());
      assertEquals(1427.0, dublinMicrosoftStatsInfo.getSumOfSquares(), 0.1E-7);
      assertEquals(7.211102550927978, dublinMicrosoftStatsInfo.getStddev(), 0.1E-7);
    }

    PivotField cardiffPivotField = placePivots.get(2);
    assertEquals("cardiff", cardiffPivotField.getValue());
    assertEquals(3, cardiffPivotField.getCount());

    PivotField polecatPivotField = cardiffPivotField.getPivot().get(0);
    assertEquals("polecat", polecatPivotField.getValue());
    assertEquals(3, polecatPivotField.getCount());

    FieldStatsInfo cardiffPolecatStatsInfo = polecatPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals(27.0, (double) cardiffPolecatStatsInfo.getMean(), 0.1E-7);
    if (justMean) {
      assertNull(cardiffPolecatStatsInfo.getMin());
      assertNull(cardiffPolecatStatsInfo.getMax());
      assertNull(cardiffPolecatStatsInfo.getCount());
      assertNull(cardiffPolecatStatsInfo.getMissing());
      assertNull(cardiffPolecatStatsInfo.getSum());
      assertNull(cardiffPolecatStatsInfo.getSumOfSquares());
      assertNull(cardiffPolecatStatsInfo.getStddev());
    } else {
      assertEquals(15.0, cardiffPolecatStatsInfo.getMin());
      assertEquals(39.0, cardiffPolecatStatsInfo.getMax());
      assertEquals(2, (long) cardiffPolecatStatsInfo.getCount());
      assertEquals(1, (long) cardiffPolecatStatsInfo.getMissing());
      assertEquals(54.0, cardiffPolecatStatsInfo.getSum());
      assertEquals(1746.0, cardiffPolecatStatsInfo.getSumOfSquares(), 0.1E-7);
      assertEquals(16.97056274847714, cardiffPolecatStatsInfo.getStddev(), 0.1E-7);
    }

    PivotField krakowPivotField = placePivots.get(3);
    assertEquals("krakow", krakowPivotField.getValue());
    assertEquals(3, krakowPivotField.getCount());

    PivotField fujitsuPivotField = krakowPivotField.getPivot().get(3);
    assertEquals("fujitsu", fujitsuPivotField.getValue());
    assertEquals(1, fujitsuPivotField.getCount());

    FieldStatsInfo krakowFujitsuStatsInfo = fujitsuPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals(Double.NaN, (double) krakowFujitsuStatsInfo.getMean(), 0.1E-7);
    if (justMean) {
      assertNull(krakowFujitsuStatsInfo.getMin());
      assertNull(krakowFujitsuStatsInfo.getMax());
      assertNull(krakowFujitsuStatsInfo.getCount());
      assertNull(krakowFujitsuStatsInfo.getMissing());
      assertNull(krakowFujitsuStatsInfo.getSum());
      assertNull(krakowFujitsuStatsInfo.getSumOfSquares());
      assertNull(krakowFujitsuStatsInfo.getStddev());
     } else {
      assertEquals(null, krakowFujitsuStatsInfo.getMin());
      assertEquals(null, krakowFujitsuStatsInfo.getMax());
      assertEquals(0, (long) krakowFujitsuStatsInfo.getCount());
      assertEquals(1, (long) krakowFujitsuStatsInfo.getMissing());
      assertEquals(0.0, krakowFujitsuStatsInfo.getSum());
      assertEquals(0.0, krakowFujitsuStatsInfo.getSumOfSquares(), 0.1E-7);
      assertEquals(Double.NaN, (double) krakowFujitsuStatsInfo.getMean(), 0.1E-7);
      assertEquals(0.0, krakowFujitsuStatsInfo.getStddev(), 0.1E-7);
    }
  }

  private void testFacetPivotRange() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    setDistributedParams(params);
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "{!range=s1}place_t,company_t");
    params.add("facet.range", "{!tag=s1 key=price}price_ti");
    params.add("facet.range.start", "0");
    params.add("facet.range.end", "100");
    params.add("facet.range.gap", "20");

    QueryResponse rsp = queryServer(params);

    List<PivotField> expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    List<PivotField> expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t",
        "microsoft", 2, null, null, createExpectedRange("price", 0, 100,
        20, 1, 0, 0, 0, 0)));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "null", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, null, createExpectedRange("price", 0, 100, 20, 1, 1, 0,
        0, 0)));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0,
        0, 0)));
    List<PivotField> expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, null, createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, null, createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 1, 1, 0, 0,
        0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "bbc", 1,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0, 0,
        0)));
    List<PivotField> expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, null, createExpectedRange("price", 0, 100, 20, 0, 2, 0,
        0, 0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "microsoft",
        2, null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0,
        0, 0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0,
        0, 0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 2, 0, 0,
        0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    List<PivotField> expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "microsoft", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedLAPivots.add(new ComparablePivotField("company_t", "null", 2, null,
        null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0, 0)));
    expectedLAPivots.add(new ComparablePivotField("company_t", "bbc", 1, null,
        null, createExpectedRange("price", 0, 100, 20, 0, 0, 0, 0, 0)));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    List<PivotField> expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",
        2, null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0,
        0, 0)));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0,
        0, 0)));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "microsoft",
        1, null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0,
        0, 0)));
    List<PivotField> expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0, 0,
        0)));
    expectedCorkPivots.add(new ComparablePivotField("company_t", "rte", 1,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0, 0,
        0)));

    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, null, createExpectedRange("price", 0, 100,
        20, 2, 1, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,
        expectedCardiffPivots, null, createExpectedRange("price", 0, 100,
        20, 1, 1, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots, null, createExpectedRange("price", 0, 100,
        20, 0, 3, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3,
        expectedLAPivots, null, createExpectedRange("price", 0, 100, 20,
        0, 1, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3,
        expectedKrakowPivots, null, createExpectedRange("price", 0, 100,
        20, 0, 1, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1,
        expectedCorkPivots, null, createExpectedRange("price", 0, 100,
        20, 0, 0, 0, 0, 0)));

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");

    // Useful to check for errors, orders lists and does toString() equality
    // check
    testOrderedPivotsStringEquality(expectedPlacePivots, placePivots);

    assertEquals(expectedPlacePivots, placePivots);

    // Test sorting by count

    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);

    rsp = queryServer(params);

    placePivots = rsp.getFacetPivot().get("place_t,company_t");

    testCountSorting(placePivots);

    // Test limit

    params.set(FacetParams.FACET_LIMIT, 2);

    rsp = queryServer(params);

    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, null, createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, null, createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 2, 0, 0,
        0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, null, createExpectedRange("price", 0, 100, 20, 0, 2, 0,
        0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, null, createExpectedRange("price", 0, 100,
        20, 2, 1, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots, null, createExpectedRange("price", 0, 100,
        20, 0, 3, 0, 0, 0)));

    placePivots = rsp.getFacetPivot().get("place_t,company_t");

    assertEquals(expectedPlacePivots, placePivots);

    // Test individual facet.limit values
    params.remove(FacetParams.FACET_LIMIT);

    params.set("f.place_t." + FacetParams.FACET_LIMIT, 1);
    params.set("f.company_t." + FacetParams.FACET_LIMIT, 4);

    rsp = queryServer(params);

    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();

    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, null, createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, null, createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 1, 1, 0, 0,
        0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0,
        0, 0)));

    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 2, 0, 0,
        0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, null, createExpectedRange("price", 0, 100, 20, 0, 2, 0,
        0, 0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0,
        0, 0)));

    expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, null, createExpectedRange("price", 0, 100, 20, 1, 1, 0,
        0, 0)));

    expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));

    expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 1, 0, 0,
        0)));

    expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1,
        null, null, createExpectedRange("price", 0, 100, 20, 0, 0, 0, 0,
        0)));

    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, null, createExpectedRange("price", 0, 100,
        20, 2, 1, 0, 0, 0)));

    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    assertEquals(expectedPlacePivots, placePivots);

    params.remove("f.company_t." + FacetParams.FACET_LIMIT);
    params.remove("f.place_t." + FacetParams.FACET_LIMIT);
    params.set(FacetParams.FACET_LIMIT, 2);

    // Test facet.missing=true with diff sorts

    index("id", 777); // NOTE: id=25 has no place as well
    commit();

    SolrParams missingA = params("q", "*:*", "rows", "0", "facet", "true",
        "facet.pivot", "place_t,company_t",
        // default facet.sort
        FacetParams.FACET_MISSING, "true");
    SolrParams missingB = SolrParams.wrapDefaults(missingA,
        params(FacetParams.FACET_LIMIT, "4", "facet.sort", "index"));
    for (SolrParams p : new SolrParams[]{missingA, missingB}) {
      // in either case, the last pivot option should be the same
      rsp = query(p);
      placePivots = rsp.getFacetPivot().get("place_t,company_t");
      assertTrue("not enough values for pivot: " + p + " => " + placePivots,
          1 < placePivots.size());
      PivotField missing = placePivots.get(placePivots.size() - 1);
      assertNull("not the missing place value: " + p, missing.getValue());
      assertEquals("wrong missing place count: " + p, 2, missing.getCount());
      assertTrue("not enough sub-pivots for missing place: " + p + " => "
          + missing.getPivot(), 1 < missing.getPivot().size());
      missing = missing.getPivot().get(missing.getPivot().size() - 1);
      assertNull("not the missing company value: " + p, missing.getValue());
      assertEquals("wrong missing company count: " + p, 1, missing.getCount());
      assertNull("company shouldn't have sub-pivots: " + p, missing.getPivot());
    }

    // sort=index + mincount + limit
    for (SolrParams variableParams : new SolrParams[]{
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count", "0", "facet.overrequest.ratio", "0"),
        params()}) {

      SolrParams p = SolrParams.wrapDefaults(
          params("q", "*:*", "rows", "0", "facet", "true", "facet.pivot",
              "company_t", "facet.sort", "index", "facet.pivot.mincount", "4",
              "facet.limit", "4"), variableParams);

      try {
        List<PivotField> pivots = query(p).getFacetPivot().get("company_t");
        assertEquals(4, pivots.size());
        assertEquals("fujitsu", pivots.get(0).getValue());
        assertEquals(4, pivots.get(0).getCount());
        assertEquals("microsoft", pivots.get(1).getValue());
        assertEquals(5, pivots.get(1).getCount());
        assertEquals("null", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());
        assertEquals("polecat", pivots.get(3).getValue());
        assertEquals(6, pivots.get(3).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // sort=index + mincount + limit + offset
    for (SolrParams variableParams : new SolrParams[]{
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count", "0", "facet.overrequest.ratio", "0"),
        params()}) {

      SolrParams p = SolrParams.wrapDefaults(
          params("q", "*:*", "rows", "0", "facet", "true", "facet.pivot",
              "company_t", "facet.sort", "index", "facet.pivot.mincount", "4",
              "facet.offset", "1", "facet.limit", "4"), variableParams);
      try {
        List<PivotField> pivots = query(p).getFacetPivot().get("company_t");
        assertEquals(3, pivots.size()); // asked for 4, but not enough meet the
        // mincount
        assertEquals("microsoft", pivots.get(0).getValue());
        assertEquals(5, pivots.get(0).getCount());
        assertEquals("null", pivots.get(1).getValue());
        assertEquals(6, pivots.get(1).getCount());
        assertEquals("polecat", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }

    }

    // sort=index + mincount + limit + offset (more permutations)
    for (SolrParams variableParams : new SolrParams[]{
        // all of these combinations should result in the same first value
        params("facet.pivot.mincount", "4", "facet.offset", "2"),
        params("facet.pivot.mincount", "5", "facet.offset", "1"),
        params("facet.pivot.mincount", "6", "facet.offset", "0")}) {

      SolrParams p = SolrParams.wrapDefaults(
          params("q", "*:*", "rows", "0", "facet", "true", "facet.limit", "1",
              "facet.sort", "index", "facet.overrequest.ratio", "0",
              "facet.pivot", "company_t"), variableParams);

      try {
        List<PivotField> pivots = query(p).getFacetPivot().get("company_t");
        assertEquals(1, pivots.size());
        assertEquals(pivots.toString(), "null", pivots.get(0).getValue());
        assertEquals(pivots.toString(), 6, pivots.get(0).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }
  }

  private void testFacetPivotQuery() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    setDistributedParams(params);
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "{!query=s1}place_t,company_t");
    params.add("facet.query", "{!tag=s1 key=highPrice}price_ti:[25 TO 100]");

    QueryResponse rsp = queryServer(params);

    List<PivotField> expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    List<PivotField> expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t",
        "microsoft", 2, null, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{0}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "null", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{0}), null));
    List<PivotField> expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "bbc", 1,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{0}), null));
    List<PivotField> expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{2}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "microsoft",
        2, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{2}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    List<PivotField> expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "microsoft", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "null", 2, null,
        createExpectedQCount(new String[]{"highPrice"}, new int[]{1}),
        null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "bbc", 1, null,
        createExpectedQCount(new String[]{"highPrice"}, new int[]{0}),
        null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    List<PivotField> expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",
        2, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{1}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{0}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "microsoft",
        1, null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{0}), null));
    List<PivotField> expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{0}), null));
    expectedCorkPivots.add(new ComparablePivotField("company_t", "rte", 1,
        null, createExpectedQCount(new String[]{"highPrice"},
        new int[]{0}), null));

    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{1}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,
        expectedCardiffPivots, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{1}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{3}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3,
        expectedLAPivots, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{1}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3,
        expectedKrakowPivots, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{1}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1,
        expectedCorkPivots, createExpectedQCount(
        new String[]{"highPrice"}, new int[]{0}), null));

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");

    // Useful to check for errors, orders lists and does toString() equality
    // check
    testOrderedPivotsStringEquality(expectedPlacePivots, placePivots);

    assertEquals(expectedPlacePivots, placePivots);

    // Add second query for low price
    params.add("facet.query", "{!tag=s1 key=lowPrice}price_ti:[0 TO 20]");
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t",
        "microsoft", 2, null, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{0, 1}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "null", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 1}), null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{0, 0}), null));
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 1}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "bbc", 1,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{0, 0}), null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{2, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "microsoft",
        2, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{2, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "microsoft", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "null", 2, null,
        createExpectedQCount(new String[]{"highPrice", "lowPrice"},
            new int[]{1, 0}), null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "bbc", 1, null,
        createExpectedQCount(new String[]{"highPrice", "lowPrice"},
            new int[]{0, 0}), null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",
        2, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{0, 0}), null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "microsoft",
        1, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{0, 0}), null));
    expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{0, 0}), null));
    expectedCorkPivots.add(new ComparablePivotField("company_t", "rte", 1,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{0, 0}), null));

    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{1, 2}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,
        expectedCardiffPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{1, 1}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{3, 0}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3,
        expectedLAPivots, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3,
        expectedKrakowPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1,
        expectedCorkPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{0, 0}), null));

    rsp = queryServer(params);

    placePivots = rsp.getFacetPivot().get("place_t,company_t");

    // Useful to check for errors, orders lists and does toString() equality
    // check
    testOrderedPivotsStringEquality(expectedPlacePivots, placePivots);
    assertEquals(expectedPlacePivots, placePivots);

    // Test sorting by count

    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);

    rsp = queryServer(params);

    placePivots = rsp.getFacetPivot().get("place_t,company_t");

    testCountSorting(placePivots);

    // Test limit

    params.set(FacetParams.FACET_LIMIT, 2);

    rsp = queryServer(params);

    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{2, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{2, 0}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{1, 2}), null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{3, 0}), null));

    placePivots = rsp.getFacetPivot().get("place_t,company_t");

    assertEquals(expectedPlacePivots, placePivots);

    // Test individual facet.limit values
    params.remove(FacetParams.FACET_LIMIT);

    params.set("f.place_t." + FacetParams.FACET_LIMIT, 1);
    params.set("f.company_t." + FacetParams.FACET_LIMIT, 4);

    rsp = queryServer(params);

    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();

    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 1}), null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));

    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{2, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{2, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 0}), null));

    expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 1}), null));

    expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));

    expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{1, 0}), null));

    expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{0, 0}), null));

    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{1, 2}), null));

    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    assertEquals(expectedPlacePivots, placePivots);

    params.remove("f.company_t." + FacetParams.FACET_LIMIT);
    params.remove("f.place_t." + FacetParams.FACET_LIMIT);
    params.set(FacetParams.FACET_LIMIT, 2);

    // Test facet.missing=true with diff sorts

    index("id", 777); // NOTE: id=25 has no place as well
    commit();

    SolrParams missingA = params("q", "*:*", "rows", "0", "facet", "true",
        "facet.pivot", "place_t,company_t",
        // default facet.sort
        FacetParams.FACET_MISSING, "true");
    SolrParams missingB = SolrParams.wrapDefaults(missingA,
        params(FacetParams.FACET_LIMIT, "4", "facet.sort", "index"));
    for (SolrParams p : new SolrParams[]{missingA, missingB}) {
      // in either case, the last pivot option should be the same
      rsp = query(p);
      placePivots = rsp.getFacetPivot().get("place_t,company_t");
      assertTrue("not enough values for pivot: " + p + " => " + placePivots,
          1 < placePivots.size());
      PivotField missing = placePivots.get(placePivots.size() - 1);
      assertNull("not the missing place value: " + p, missing.getValue());
      assertEquals("wrong missing place count: " + p, 2, missing.getCount());
      assertTrue("not enough sub-pivots for missing place: " + p + " => "
          + missing.getPivot(), 1 < missing.getPivot().size());
      missing = missing.getPivot().get(missing.getPivot().size() - 1);
      assertNull("not the missing company value: " + p, missing.getValue());
      assertEquals("wrong missing company count: " + p, 1, missing.getCount());
      assertNull("company shouldn't have sub-pivots: " + p, missing.getPivot());
    }

    // sort=index + mincount + limit
    for (SolrParams variableParams : new SolrParams[]{
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count", "0", "facet.overrequest.ratio", "0"),
        params()}) {

      SolrParams p = SolrParams.wrapDefaults(
          params("q", "*:*", "rows", "0", "facet", "true", "facet.pivot",
              "company_t", "facet.sort", "index", "facet.pivot.mincount", "4",
              "facet.limit", "4"), variableParams);

      try {
        List<PivotField> pivots = query(p).getFacetPivot().get("company_t");
        assertEquals(4, pivots.size());
        assertEquals("fujitsu", pivots.get(0).getValue());
        assertEquals(4, pivots.get(0).getCount());
        assertEquals("microsoft", pivots.get(1).getValue());
        assertEquals(5, pivots.get(1).getCount());
        assertEquals("null", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());
        assertEquals("polecat", pivots.get(3).getValue());
        assertEquals(6, pivots.get(3).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // sort=index + mincount + limit + offset
    for (SolrParams variableParams : new SolrParams[]{
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count", "0", "facet.overrequest.ratio", "0"),
        params()}) {

      SolrParams p = SolrParams.wrapDefaults(
          params("q", "*:*", "rows", "0", "facet", "true", "facet.pivot",
              "company_t", "facet.sort", "index", "facet.pivot.mincount", "4",
              "facet.offset", "1", "facet.limit", "4"), variableParams);
      try {
        List<PivotField> pivots = query(p).getFacetPivot().get("company_t");
        assertEquals(3, pivots.size()); // asked for 4, but not enough meet the
        // mincount
        assertEquals("microsoft", pivots.get(0).getValue());
        assertEquals(5, pivots.get(0).getCount());
        assertEquals("null", pivots.get(1).getValue());
        assertEquals(6, pivots.get(1).getCount());
        assertEquals("polecat", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }

    }

    // sort=index + mincount + limit + offset (more permutations)
    for (SolrParams variableParams : new SolrParams[]{
        // all of these combinations should result in the same first value
        params("facet.pivot.mincount", "4", "facet.offset", "2"),
        params("facet.pivot.mincount", "5", "facet.offset", "1"),
        params("facet.pivot.mincount", "6", "facet.offset", "0")}) {

      SolrParams p = SolrParams.wrapDefaults(
          params("q", "*:*", "rows", "0", "facet", "true", "facet.limit", "1",
              "facet.sort", "index", "facet.overrequest.ratio", "0",
              "facet.pivot", "company_t"), variableParams);

      try {
        List<PivotField> pivots = query(p).getFacetPivot().get("company_t");
        assertEquals(1, pivots.size());
        assertEquals(pivots.toString(), "null", pivots.get(0).getValue());
        assertEquals(pivots.toString(), 6, pivots.get(0).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }
  }

  private void testPivotFacetRangeAndQuery() throws Exception {
    SolrParams params = params("q", "*:*",
        "rows", "0",
        "facet", "true",
        "stats", "true",
        "facet.pivot", "{!range=s1 query=s2 stats=s3}place_t,company_t",
        "facet.range", "{!tag=s1 key=price}price_ti",
        "facet.query", "{!tag=s2 key=highPrice}price_ti:[25 TO 100]",
        "facet.query", "{!tag=s2 key=lowPrice}price_ti:[0 TO 20]",
        "stats.field", ("{!tag=s3 key=avg_price}price_ti"),
        "facet.range.start", "0",
        "facet.range.end", "100",
        "facet.range.gap", "20",
        FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT,
        FacetParams.FACET_LIMIT, "2");

    UnorderedEqualityArrayList<PivotField> expectedPlacePivots = new UnorderedEqualityArrayList<>();
    UnorderedEqualityArrayList<PivotField> expectedDublinPivots = new UnorderedEqualityArrayList<>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{1, 2}), createExpectedRange("price", 0, 100, 20, 2, 1, 0,
        0, 0)));
    UnorderedEqualityArrayList<PivotField> expectedLondonPivots = new UnorderedEqualityArrayList<>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null, createExpectedQCount(
        new String[]{"highPrice", "lowPrice"}, new int[]{2, 0}), createExpectedRange("price", 0, 100, 20, 0, 2, 0, 0,
        0)));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null, createExpectedQCount(new String[]{"highPrice",
        "lowPrice"}, new int[]{2, 0}), createExpectedRange("price", 0, 100, 20, 0, 2, 0,
        0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{1, 2}), createExpectedRange("price", 0, 100,
        20, 2, 1, 0, 0, 0)));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots, createExpectedQCount(new String[]{
        "highPrice", "lowPrice"}, new int[]{3, 0}), createExpectedRange("price", 0, 100,
        20, 0, 3, 0, 0, 0)));

    QueryResponse rsp = query(params);
    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");
    assertEquals(expectedPlacePivots, placePivots);

    PivotField dublinPivotField = placePivots.get(0);
    assertEquals("dublin", dublinPivotField.getValue());
    assertEquals(4, dublinPivotField.getCount());

    PivotField microsoftPivotField = dublinPivotField.getPivot().get(0);
    assertEquals("microsoft", microsoftPivotField.getValue());
    assertEquals(4, microsoftPivotField.getCount());

    FieldStatsInfo dublinMicrosoftStatsInfo = microsoftPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals(21.0, (double) dublinMicrosoftStatsInfo.getMean(), 0.1E-7);
    assertEquals(15.0, dublinMicrosoftStatsInfo.getMin());
    assertEquals(29.0, dublinMicrosoftStatsInfo.getMax());
    assertEquals(3, (long) dublinMicrosoftStatsInfo.getCount());
    assertEquals(1, (long) dublinMicrosoftStatsInfo.getMissing());
    assertEquals(63.0, dublinMicrosoftStatsInfo.getSum());
    assertEquals(1427.0, dublinMicrosoftStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(7.211102550927978, dublinMicrosoftStatsInfo.getStddev(), 0.1E-7);
  }

  private void testNegativeFacetQuery() throws Exception {
    // this should not hang facet.query under the pivot
    SolrParams params = params("q", "*:*",
        "rows", "0",
        "stats", "true",
        "facet.query", "{!tag=ttt}price_ti:[25 TO 100]",
        "facet", "true",
        "facet.pivot", "{!query=t}place_t,company_t");
    QueryResponse rsp = query(params);

    assertNullFacetTypeInsidePivot(FacetParams.FACET_QUERY, rsp.getFacetPivot().get("place_t,company_t"));

    params = params("q", "*:*",
        "rows", "0",
        "stats", "true",
        "facet", "true",
        "facet.pivot", "{!query=t}place_t,company_t");
    rsp = query(params);
    assertNullFacetTypeInsidePivot(FacetParams.FACET_QUERY, rsp.getFacetPivot().get("place_t,company_t"));

    params = params("q", "*:*",
        "rows", "0",
        "facet.query", "{!tag=t}price_ti:[25 TO 100]",
        "hang", "", // empty
        "facet", "true",
        "facet.pivot", "{!query=$hang}place_t,company_t");
    rsp = query(params);
    assertNullFacetTypeInsidePivot(FacetParams.FACET_QUERY, rsp.getFacetPivot().get("place_t,company_t"));

    params = params("q", "*:*",
        "rows", "0",
        "facet.query", "{!tag=t}price_ti:[25 TO 100]",
        "hang", "price_ti:[0 TO 20]", // with a query
        "facet", "true",
        "facet.pivot", "{!query=$hang}place_t,company_t");
    rsp = query(params);
    // we aren't going to start calculating facet query unless the query is specified with a 'facet.query' param
    // hence hanging an arbitrary query shouldn't work
    assertNullFacetTypeInsidePivot(FacetParams.FACET_QUERY, rsp.getFacetPivot().get("place_t,company_t"));
  }

  private void testNegativeRangeQuery() throws Exception {
    SolrParams params = params("q", "*:*",
        "rows", "0",
        "stats", "true",
        "facet.range", "{!tag=s1 key=price}price_ti",
        "facet", "true",
        "facet.pivot", "{!range=s}place_t,company_t",
        "facet.range.start", "0",
        "facet.range.end", "100",
        "facet.range.gap", "20");
    QueryResponse rsp = query(params);
    assertNullFacetTypeInsidePivot(FacetParams.FACET_RANGE, rsp.getFacetPivot().get("place_t,company_t"));

    params = params("q", "*:*",
        "rows", "0",
        "stats", "true",
        "facet.range", "{!tag=s1 key=price}price_ti",
        "facet", "true",
        "hang", "", // empty!
        "facet.pivot", "{!range=$hang}place_t,company_t",
        "facet.range.start", "0",
        "facet.range.end", "100",
        "facet.range.gap", "20");
    rsp = query(params);
    assertNullFacetTypeInsidePivot(FacetParams.FACET_RANGE, rsp.getFacetPivot().get("place_t,company_t"));

    params = params("q", "*:*",
        "rows", "0",
        "stats", "true",
        "facet.range", "{!tag=s1 key=price}price_ti",
        "facet", "true",
        "hang", "price_ti",
        "facet.pivot", "{!range=$hang}place_t,company_t",
        "facet.range.start", "0",
        "facet.range.end", "100",
        "facet.range.gap", "20");
    rsp = query(params);
    assertNullFacetTypeInsidePivot(FacetParams.FACET_RANGE, rsp.getFacetPivot().get("place_t,company_t"));
  }

  private Map<String, Integer> createExpectedQCount(String[] keys, int[] counts) {
    Map<String, Integer> expectedQCounts = new LinkedHashMap<>();
    for (int idx = 0; idx < keys.length; idx++) {
      expectedQCounts.put(keys[idx], counts[idx]);
    }
    return expectedQCounts;
  }

  private void assertNullFacetTypeInsidePivot(String facetType, List<PivotField> pivots) {
    for (PivotField pivot : pivots) {
      if (facetType == FacetParams.FACET_QUERY) {
        assertNull("pivot=" + pivot + " facetType=" + facetType
            + " should've been null. Found: " + pivot.getFacetQuery(), pivot.getFacetQuery());
      } else if (facetType == FacetParams.FACET_RANGE) {
        assertNull("pivot=" + pivot + " facetType=" + facetType
            + " should've been null. Found: " + pivot.getFacetRanges(), pivot.getFacetRanges());
      }

      if (pivot.getPivot() != null) {
        assertNullFacetTypeInsidePivot(facetType, pivot.getPivot());
      }
    }
  }

  // Useful to check for errors, orders lists and does toString() equality check
  private void testOrderedPivotsStringEquality(
      List<PivotField> expectedPlacePivots, List<PivotField> placePivots) {
    Collections.sort(expectedPlacePivots, new PivotFieldComparator());
    for (PivotField expectedPivot : expectedPlacePivots) {
      if (expectedPivot.getPivot() != null) {
        Collections.sort(expectedPivot.getPivot(), new PivotFieldComparator());
      }
    }
    Collections.sort(placePivots, new PivotFieldComparator());
    for (PivotField pivot : placePivots) {
      if (pivot.getPivot() != null) {
        Collections.sort(pivot.getPivot(), new PivotFieldComparator());
      }
    }
    assertEquals(expectedPlacePivots.toString(), placePivots.toString());
  }

  /**
   * sanity check the stat values nested under a pivot when at least one shard
   * has nothing but missing values for the stat
   */
  private void doTestPivotStatsFromOneShard() throws Exception {
    SolrParams params = params("q", "*:*", "rows", "0", 
                               "facet", "true", "stats", "true", 
                               "facet.pivot", "{!stats=s1}place_t,company_t", 
                               "stats.field", "{!tag=s1}solo_i",
                               "stats.field", "{!tag=s1}solo_s",
                               "stats.field", "{!tag=s1}solo_dt");
                               
    QueryResponse rsp = query(params);

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");

    PivotField placePivot = placePivots.get(0);
    assertEquals("dublin", placePivot.getValue());
    assertEquals(4, placePivot.getCount());

    PivotField companyPivot = placePivot.getPivot().get(2);
    assertEquals("null", companyPivot.getValue());
    assertEquals(3, companyPivot.getCount());

    for (PivotField pf : new PivotField[] { placePivot, companyPivot }) {
      assertThereCanBeOnlyOne(pf, pf.getFieldStatsInfo().get("solo_s"), "lonely");

      assertThereCanBeOnlyOne(pf, pf.getFieldStatsInfo().get("solo_i"), 42.0D);
      assertEquals(pf.getField()+":"+pf.getValue()+": int mean",
                   42.0D, pf.getFieldStatsInfo().get("solo_i").getMean());

      Object expected = new Date(194923425000L); // 1976-03-06T01:23:45Z
      assertThereCanBeOnlyOne(pf, pf.getFieldStatsInfo().get("solo_dt"), expected);
      assertEquals(pf.getField()+":"+pf.getValue()+": date mean",
                   expected, pf.getFieldStatsInfo().get("solo_dt").getMean());

      // TODO: add enum field asserts - blocked by SOLR-6682
    }
  }
  
  private void testCountSorting(List<PivotField> pivots) {
    Integer lastCount = null;
    for (PivotField pivot : pivots) {
      if (lastCount != null) {
        assertTrue(pivot.getCount() <= lastCount);
      }
      lastCount = pivot.getCount();
      if (pivot.getPivot() != null) {
        testCountSorting(pivot.getPivot());
      }
    }
  }
  
  /**
   * given a PivotField, a FieldStatsInfo, and a value; asserts that:
   * <ul>
   *  <li>stat count == 1</li>
   *  <li>stat missing == pivot count - 1</li>
   *  <li>stat min == stat max == value</li>
   * </ul>
   */
  private void assertThereCanBeOnlyOne(PivotField pf, FieldStatsInfo stats, Object val) {
    String msg = pf.getField() + ":" + pf.getValue();
    assertEquals(msg + " stats count", 1L, (long) stats.getCount());
    assertEquals(msg + " stats missing", pf.getCount()-1L, (long) stats.getMissing());
    assertEquals(msg + " stats min", val, stats.getMin());
    assertEquals(msg + " stats max", val, stats.getMax());
  }

  @SuppressWarnings({"rawtypes"})
  private List<RangeFacet> createExpectedRange(String key, int start, int end,
                                               int gap, int... values) {
    List<RangeFacet> expectedRanges = new ArrayList<>();
    RangeFacet expectedPrices = new RangeFacet.Numeric(key, start, end, gap, null, null, null);
    expectedRanges.add(expectedPrices);
    int idx = 0;
    for (int range = start; range < end; range += gap) {
      expectedPrices.addCount(String.valueOf(range), values[idx]);
      if (idx < values.length) {
        idx++;
      }
    }
    return expectedRanges;
  }

  public static class ComparablePivotField extends PivotField {

    public ComparablePivotField(String f, Object v, int count,
                                List<PivotField> pivot,  Map<String,Integer> queryCounts,
                                @SuppressWarnings({"rawtypes"})List<RangeFacet> ranges) {
      super(f, v, count, pivot, null, queryCounts, ranges);
    }

    public ComparablePivotField(String f, Object v, int count,
                                List<PivotField> pivot) {
      super(f, v, count, pivot, null, null, null);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!obj.getClass().isAssignableFrom(PivotField.class)) return false;
      PivotField other = (PivotField) obj;
      if (getCount() != other.getCount()) return false;
      if (getField() == null) {
        if (other.getField() != null) return false;
      } else if (!getField().equals(other.getField())) return false;
      if (getPivot() == null) {
        if (other.getPivot() != null) return false;
      } else if (!getPivot().equals(other.getPivot())) return false;
      if (getValue() == null) {
        if (other.getValue() != null) return false;
      } else if (!getValue().equals(other.getValue())) return false;
      if (getFacetRanges() == null) {
        if (other.getFacetRanges() != null) return false;
      } else {
        if (getFacetRanges().size() != other.getFacetRanges().size()) return false;
        for (@SuppressWarnings({"rawtypes"})RangeFacet entry : getFacetRanges()) {
          boolean found = false;
          for (@SuppressWarnings({"rawtypes"})RangeFacet otherRange : other.getFacetRanges()) {
            if (otherRange.getName().equals(entry.getName())) {
              found = true;

              if (!entry.getGap().equals(otherRange.getGap()))  return false;
              if (!entry.getStart().equals(otherRange.getStart()))  return false;
              if (!entry.getEnd().equals(otherRange.getEnd()))  return false;

              @SuppressWarnings({"unchecked"})
              List<RangeFacet.Count> myCounts = entry.getCounts();
              @SuppressWarnings({"unchecked"})
              List<RangeFacet.Count> otherRangeCounts = otherRange.getCounts();
              if ( (myCounts == null && otherRangeCounts != null)
                  || (myCounts != null && otherRangeCounts == null)
                  || (myCounts.size() != otherRangeCounts.size()))  return false;

              for (int i=0; i<myCounts.size(); i++) {
                if (!myCounts.get(i).getValue().equals(otherRangeCounts.get(i).getValue())) return false;
                if (myCounts.get(i).getCount() != otherRangeCounts.get(i).getCount())  return false;
              }
            }
          }
          if (!found) return false;
        }
      }
      if (getFacetQuery() == null) {
        if (other.getFacetQuery() != null) return false;
      } else {
        if (getFacetQuery().size() != other.getFacetQuery().size()) return false;
        for (Map.Entry<String,Integer> entry : getFacetQuery().entrySet()) {
          Integer otherQCount = other.getFacetQuery().get(entry.getKey());
          if (otherQCount == null || !otherQCount.equals(entry.getValue())) return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException("Calling hashCode in ComparablePivotField");
    }
  }
  
  public static class UnorderedEqualityArrayList<T> extends ArrayList<T> {

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException("Calling hashCode in UnorderedEqualityArrayList");
    }

    @Override
    public boolean equals(Object o) {
      boolean equal = false;
      if (o instanceof ArrayList) {
        List<?> otherList = (List<?>) o;
        if (size() == otherList.size()) {
          equal = true;
          for (Object objectInOtherList : otherList) {
            if (!contains(objectInOtherList)) {
              return false;
            }
          }
        }
      }
      return equal;
    }
    
    public int indexOf(Object o) {
      for (int i = 0; i < size(); i++) {
        if (get(i).equals(o)) {
          return i;
        }
      }
      return -1;
    }
  }
  
  public static class PivotFieldComparator implements Comparator<PivotField> {
    
    @Override
    public int compare(PivotField o1, PivotField o2) {
      int compare = Integer.compare(o2.getCount(), o1.getCount());
      if (compare == 0) {
        compare = ((String) o2.getValue()).compareTo((String) o1.getValue());
      }
      if (compare == 0) {
        for (Map.Entry<String,Integer> entry : o1.getFacetQuery().entrySet()) {
          compare = entry.getValue().compareTo(
              o2.getFacetQuery().get(entry.getKey()));
          if (compare != 0) {
            break;
          }
        }
        if (compare == 0) {
          compare = Integer.compare(o1.getFacetQuery().size(),
              o2.getFacetQuery().size());
        }
      }
      if (compare == 0) {
        for (@SuppressWarnings({"rawtypes"})RangeFacet entry : o1.getFacetRanges()) {
          boolean found = false;
          for (@SuppressWarnings({"rawtypes"})RangeFacet otherRangeFacet : o2.getFacetRanges()) {
            if (otherRangeFacet.getName().equals(entry.getName()))  {
              found = true;
            }
          }
          if (!found) {
            compare = 1;
            break;
          }
        }
      }
      return compare;
    }
    
  }
  
}
