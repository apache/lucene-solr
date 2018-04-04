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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

import java.util.List;

/**
 * tests some edge cases of pivot faceting with stats
 *
 * NOTE: This test ignores the control collection (in single node mode, there is no 
 * need for the overrequesting, all the data is local -- so comparisons with it wouldn't 
 * be valid in some cases we are testing here)
 */
public class DistributedFacetPivotSmallAdvancedTest extends BaseDistributedSearchTestCase {

  public DistributedFacetPivotSmallAdvancedTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
  }
  
  @Test
  @ShardsFixed(num = 2)
  public void test() throws Exception {

    del("*:*");
    final SolrClient shard0 = clients.get(0);
    final SolrClient shard1 = clients.get(1);

    // NOTE: we use the literal (4 character) string "null" as a company name
    // to help ensure there isn't any bugs where the literal string is treated as if it 
    // were a true NULL value.

    // shard0
    shard0.add(sdoc(id, 19, "place_t", "cardiff dublin", 
                    "company_t", "microsoft polecat", 
                    "price_ti", "15", "foo_s", "aaa", "foo_i", 10));
    shard0.add(sdoc(id, 20, "place_t", "dublin", 
                    "company_t", "polecat microsoft null", 
                    "price_ti", "19", "foo_s", "bbb", "foo_i", 4));
    shard0.add(sdoc(id, 21, "place_t", "london la dublin", 
                    "company_t", "microsoft fujitsu null polecat", 
                    "price_ti", "29", "foo_s", "bbb", "foo_i", 3));
    shard0.add(sdoc(id, 22, "place_t", "krakow london cardiff", 
                    "company_t", "polecat null bbc", 
                    "price_ti", "39", "foo_s", "bbb", "foo_i", 6));
    shard0.add(sdoc(id, 23, "place_t", "london", 
                    "company_t", "", 
                    "price_ti", "29", "foo_s", "bbb", "foo_i", 9));
    // shard1
    shard1.add(sdoc(id, 24, "place_t", "la", 
                    "company_t", "", 
                    "foo_s", "aaa", "foo_i", 21));
    shard1.add(sdoc(id, 25, 
                    "company_t", "microsoft polecat null fujitsu null bbc", 
                    "price_ti", "59", "foo_s", "aaa", "foo_i", 5));
    shard1.add(sdoc(id, 26, "place_t", "krakow", 
                    "company_t", "null", 
                    "foo_s", "aaa", "foo_i", 23));
    shard1.add(sdoc(id, 27, "place_t", "krakow cardiff dublin london la", 
                    "company_t", "null microsoft polecat bbc fujitsu", 
                    "foo_s", "aaa", "foo_i", 91));
    shard1.add(sdoc(id, 28, "place_t", "cork", 
                    "company_t", "fujitsu rte", "foo_s", "aaa", "foo_i", 76));
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    doTestDeepPivotStatsOnString();

    doTestTopStatsWithRefinement(true);
    doTestTopStatsWithRefinement(false);
  }

  /**
   * we need to ensure that stats never "overcount" the values from a single shard
   * even if we hit that shard with a refinement request 
   */
  private void doTestTopStatsWithRefinement(final boolean allStats) throws Exception {

    String stat_param = allStats ? 
      "{!tag=s1}foo_i" : "{!tag=s1 min=true max=true count=true missing=true}foo_i";

    ModifiableSolrParams coreParams = params("q", "*:*", "rows", "0",
                                             "stats", "true",
                                             "stats.field", stat_param );
    ModifiableSolrParams facetParams = new ModifiableSolrParams(coreParams);
    facetParams.add(params("facet", "true",
                           "facet.limit", "1",
                           "facet.pivot", "{!stats=s1}place_t,company_t"));
    
    ModifiableSolrParams facetForceRefineParams = new ModifiableSolrParams(facetParams);
    facetForceRefineParams.add(params(FacetParams.FACET_OVERREQUEST_COUNT, "0",
                                      FacetParams.FACET_OVERREQUEST_RATIO, "0"));

    for (ModifiableSolrParams params : new ModifiableSolrParams[] {
        coreParams, facetParams, facetForceRefineParams }) {

      // for all three sets of these params, the "top level" 
      // stats in the response of a distributed query should be the same
      ModifiableSolrParams q = new ModifiableSolrParams(params);
      q.set("shards", getShardsString());

      QueryResponse rsp = queryServer(q);
      FieldStatsInfo fieldStatsInfo = rsp.getFieldStatsInfo().get("foo_i");

      String msg = q.toString();

      assertEquals(msg, 3.0, fieldStatsInfo.getMin());
      assertEquals(msg, 91.0, fieldStatsInfo.getMax());
      assertEquals(msg, 10, (long) fieldStatsInfo.getCount());
      assertEquals(msg, 0, (long) fieldStatsInfo.getMissing());

      if (allStats) {
        assertEquals(msg, 248.0, fieldStatsInfo.getSum());
        assertEquals(msg, 15294.0, fieldStatsInfo.getSumOfSquares(), 0.1E-7);
        assertEquals(msg, 24.8, (double) fieldStatsInfo.getMean(), 0.1E-7);
        assertEquals(msg, 31.87405772027709, fieldStatsInfo.getStddev(), 0.1E-7);
      } else {
        assertNull(msg, fieldStatsInfo.getSum());
        assertNull(msg, fieldStatsInfo.getSumOfSquares());
        assertNull(msg, fieldStatsInfo.getMean());
        assertNull(msg, fieldStatsInfo.getStddev());
      }

      if (params.getBool("facet", false)) {
        // if this was a facet request, then the top pivot constraint and pivot 
        // stats should match what we expect - regardless of whether refine
        // was used, or if the query was initially satisfied by the default overrequest
        
        List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");
        assertEquals(1, placePivots.size());
        
        PivotField dublinPivotField = placePivots.get(0);
        assertEquals("dublin", dublinPivotField.getValue());
        assertEquals(4, dublinPivotField.getCount());
        assertEquals(1, dublinPivotField.getPivot().size());

        PivotField microsoftPivotField = dublinPivotField.getPivot().get(0);
        assertEquals("microsoft", microsoftPivotField.getValue());
        assertEquals(4, microsoftPivotField.getCount());
        
        FieldStatsInfo dublinMicrosoftStatsInfo = microsoftPivotField.getFieldStatsInfo().get("foo_i");
        assertEquals(3.0D, dublinMicrosoftStatsInfo.getMin());
        assertEquals(91.0D, dublinMicrosoftStatsInfo.getMax());
        assertEquals(4, (long) dublinMicrosoftStatsInfo.getCount());
        assertEquals(0, (long) dublinMicrosoftStatsInfo.getMissing());
        
        if (! allStats) {
          assertNull(msg, dublinMicrosoftStatsInfo.getSum());
          assertNull(msg, dublinMicrosoftStatsInfo.getSumOfSquares());
          assertNull(msg, dublinMicrosoftStatsInfo.getMean());
          assertNull(msg, dublinMicrosoftStatsInfo.getStddev());
        }
      }
    }

    // sanity check that the top pivot from each shard is diff, to prove to 
    // ourselves that the above queries really must have involved refinement.
    Object s0pivValue = clients.get(0)
      .query(facetParams).getFacetPivot().get("place_t,company_t").get(0).getValue();
    Object s1pivValue = clients.get(1)
      .query(facetParams).getFacetPivot().get("place_t,company_t").get(0).getValue();
    assertFalse("both shards have same top constraint, test is invalid" +
                "(did someone change the test data?) ==> " + 
                s0pivValue + "==" + s1pivValue, s0pivValue.equals(s1pivValue));
    
  }

  private void doTestDeepPivotStatsOnString() throws Exception {
    SolrParams params = params("q", "*:*", "rows", "0",
        "shards", getShardsString(),
        "facet", "true", "stats", "true",
        "facet.pivot", "{!stats=s1}place_t,company_t",
        "stats.field", "{!key=avg_price tag=s1}foo_s");
    QueryResponse rsp = queryServer(new ModifiableSolrParams(params));

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");

    PivotField dublinPivotField = placePivots.get(0);
    assertEquals("dublin", dublinPivotField.getValue());
    assertEquals(4, dublinPivotField.getCount());

    PivotField microsoftPivotField = dublinPivotField.getPivot().get(0);
    assertEquals("microsoft", microsoftPivotField.getValue());
    assertEquals(4, microsoftPivotField.getCount());

    FieldStatsInfo dublinMicrosoftStatsInfo = microsoftPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("aaa", dublinMicrosoftStatsInfo.getMin());
    assertEquals("bbb", dublinMicrosoftStatsInfo.getMax());
    assertEquals(4, (long) dublinMicrosoftStatsInfo.getCount());
    assertEquals(0, (long) dublinMicrosoftStatsInfo.getMissing());

    PivotField cardiffPivotField = placePivots.get(2);
    assertEquals("cardiff", cardiffPivotField.getValue());
    assertEquals(3, cardiffPivotField.getCount());

    PivotField polecatPivotField = cardiffPivotField.getPivot().get(0);
    assertEquals("polecat", polecatPivotField.getValue());
    assertEquals(3, polecatPivotField.getCount());

    FieldStatsInfo cardiffPolecatStatsInfo = polecatPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("aaa", cardiffPolecatStatsInfo.getMin());
    assertEquals("bbb", cardiffPolecatStatsInfo.getMax());
    assertEquals(3, (long) cardiffPolecatStatsInfo.getCount());
    assertEquals(0, (long) cardiffPolecatStatsInfo.getMissing());

    PivotField krakowPivotField = placePivots.get(3);
    assertEquals("krakow", krakowPivotField.getValue());
    assertEquals(3, krakowPivotField.getCount());

    PivotField fujitsuPivotField = krakowPivotField.getPivot().get(3);
    assertEquals("fujitsu", fujitsuPivotField.getValue());
    assertEquals(1, fujitsuPivotField.getCount());

    FieldStatsInfo krakowFujitsuStatsInfo = fujitsuPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("aaa", krakowFujitsuStatsInfo.getMin());
    assertEquals("aaa", krakowFujitsuStatsInfo.getMax());
    assertEquals(1, (long) krakowFujitsuStatsInfo.getCount());
    assertEquals(0, (long) krakowFujitsuStatsInfo.getMissing());
  }

}
