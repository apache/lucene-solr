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

import java.util.List;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.search.facet.DistributedFacetSimpleRefinementLongTailTest;

import org.junit.Test;

/**
 * <p>
 * test demonstrating how overrequesting helps finds top-terms in the "long tail" 
 * of shards that don't have even distributions of terms (something that can be common
 * in cases of custom sharding -- even if you don't know that there is a corrolation 
 * between the property you are sharding on and the property you are faceting on).
 * <p>
 * <b>NOTE:</b> This test ignores the control collection (in single node mode, there is no 
 * need for the overrequesting, all the data is local -- so comparisons with it wouldn't 
 * be valid in the cases we are testing here)
 * </p>
 * <p>
 * <b>NOTE:</b> uses the same indexed documents as {@link DistributedFacetSimpleRefinementLongTailTest} -- 
 * however the behavior of <code>refine:simple</code> is "simpler" then the refinement logic used by 
 * <code>facet.pivot</code> so the assertions in this test vary from that test.
 * </p>
 */
public class DistributedFacetPivotLongTailTest extends BaseDistributedSearchTestCase {
  
  private String STAT_FIELD = null; // will be randomized single value vs multivalued

  public DistributedFacetPivotLongTailTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");

    STAT_FIELD = random().nextBoolean() ? "stat_i1" : "stat_i";
  }
  
  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    DistributedFacetSimpleRefinementLongTailTest.buildIndexes(clients, STAT_FIELD);
    commit();

    sanityCheckIndividualShards();
    checkRefinementAndOverrequesting();
    doTestDeepPivotStats();
  }
  
  @SuppressWarnings({"rawtypes"})
  private void sanityCheckIndividualShards() throws Exception {
    assertEquals("This test assumes exactly 3 shards/clients", 3, clients.size());
    
    SolrParams req = params( "q", "*:*", 
                             "distrib", "false",
                             "facet", "true", 
                             "facet.limit", "10",
                             "facet.pivot", "foo_s,bar_s");

    // sanity check that our expectations about each shard (non-distrib) are correct

    PivotField pivot = null;
    List<PivotField> pivots = null;
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<PivotField>[] shardPivots = new List[clients.size()];
    for (int i = 0; i < clients.size(); i++) {
      shardPivots[i] = clients.get(i).query( req ).getFacetPivot().get("foo_s,bar_s");
    }

    // top 5 same on all shards
    for (int i = 0; i < 3; i++) {
      assertEquals(10, shardPivots[i].size());
      for (int j = 0; j < 5; j++) {
        pivot = shardPivots[i].get(j);
        assertEquals(pivot.toString(), "aaa"+j, pivot.getValue());
        assertEquals(pivot.toString(), 100, pivot.getCount());
      }
    }
    // top 6-10 same on shard0 & shard11
    for (int i = 0; i < 2; i++) {
      for (int j = 5; j < 10; j++) {
        pivot = shardPivots[i].get(j);
        assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("bbb"));
        assertEquals(pivot.toString(), 50, pivot.getCount());
      }
    }
    // 6-10 on shard2
    assertEquals("junkA", shardPivots[2].get(5).getValue());
    assertEquals(50, shardPivots[2].get(5).getCount());
    assertEquals("tail", shardPivots[2].get(6).getValue());
    assertEquals(45, shardPivots[2].get(6).getCount());
    for (int j = 7; j < 10; j++) {
      pivot = shardPivots[2].get(j);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("ZZZ"));
      assertEquals(pivot.toString(), 1, pivot.getCount());
    }
    // check sub-shardPivots on "tail" from shard2
    pivots = shardPivots[2].get(6).getPivot();
    assertEquals(6, pivots.size());
    for (int j = 0; j < 5; j++) {
      pivot = pivots.get(j);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("junkB"));
      assertEquals(pivot.toString(), 8, pivot.getCount());
    }
    pivot = pivots.get(5);
    assertEquals("tailB", pivot.getValue());
    assertEquals(5, pivot.getCount());
  }

  private void checkRefinementAndOverrequesting() throws Exception {
    // if we disable overrequesting, we don't find the long tail
    List<PivotField> pivots = null;
    PivotField pivot = null;
    pivots = queryServer( params( "q", "*:*",
                                  "shards", getShardsString(),
                                  FacetParams.FACET_OVERREQUEST_COUNT, "0",
                                  FacetParams.FACET_OVERREQUEST_RATIO, "0",
                                  "facet", "true",
                                  "facet.limit", "6",
                                  "facet.pivot", "{!stats=sxy}foo_s,bar_s",
                                  "stats", "true",
                                  "stats.field", "{!tag=sxy}" + STAT_FIELD)
                          ).getFacetPivot().get("foo_s,bar_s");
    assertEquals(6, pivots.size());
    for (int i = 0; i < 5; i++) {
      pivot = pivots.get(i);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("aaa"));
      assertEquals(pivot.toString(), 300, pivot.getCount());
    }
    { // even w/o the long tail, we should have still asked shard2 to refine bbb0
      pivot = pivots.get(5);
      assertTrue(pivot.toString(), pivot.getValue().equals("bbb0"));
      assertEquals(pivot.toString(), 101, pivot.getCount());
      // basic check of refined stats
      FieldStatsInfo bbb0Stats = pivot.getFieldStatsInfo().get(STAT_FIELD);
      assertEquals(STAT_FIELD, bbb0Stats.getName());
      assertEquals(-2.0, bbb0Stats.getMin());
      assertEquals(1.0, bbb0Stats.getMax());
      assertEquals(101, (long) bbb0Stats.getCount());
      assertEquals(0, (long) bbb0Stats.getMissing());
      assertEquals(48.0, bbb0Stats.getSum());
      assertEquals(0.475247524752475, (double) bbb0Stats.getMean(), 0.1E-7);
      assertEquals(54.0, bbb0Stats.getSumOfSquares(), 0.1E-7);
      assertEquals(0.55846323792, bbb0Stats.getStddev(), 0.1E-7);
    }


    // with default overrequesting, we should find the correct top 6 including 
    // long tail and top sub-pivots
    // (even if we disable overrequesting on the sub-pivot)
    for (ModifiableSolrParams q : new ModifiableSolrParams[] { 
        params(),
        params("f.bar_s.facet.overrequest.ratio","0",
               "f.bar_s.facet.overrequest.count","0")      }) {
      
      q.add( params( "q", "*:*",
                     "shards", getShardsString(),
                     "facet", "true",
                     "facet.limit", "6",
                     "facet.pivot", "foo_s,bar_s" ));
      pivots = queryServer( q ).getFacetPivot().get("foo_s,bar_s");
        
      assertEquals(6, pivots.size());
      for (int i = 0; i < 5; i++) {
        pivot = pivots.get(i);
        assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("aaa"));
        assertEquals(pivot.toString(), 300, pivot.getCount());
      }
      pivot = pivots.get(5);
      assertEquals(pivot.toString(), "tail", pivot.getValue());
      assertEquals(pivot.toString(), 135, pivot.getCount());
      // check the sub pivots
      pivots = pivot.getPivot();
      assertEquals(6, pivots.size());
      pivot = pivots.get(0);
      assertEquals(pivot.toString(), "tailB", pivot.getValue());
      assertEquals(pivot.toString(), 17, pivot.getCount());
      for (int i = 1; i < 6; i++) { // ccc(0-4)
        pivot = pivots.get(i);
        assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("ccc"));
        assertEquals(pivot.toString(), 14, pivot.getCount());
      }
    }

    // if we lower the facet.limit on the sub-pivot, overrequesting should still ensure 
    // that we get the correct top5 including "tailB"

    pivots = queryServer( params( "q", "*:*",
                                  "shards", getShardsString(),
                                  "facet", "true",
                                  "facet.limit", "6",
                                  "f.bar_s.facet.limit", "5",
                                  "facet.pivot", "foo_s,bar_s" )
                          ).getFacetPivot().get("foo_s,bar_s");
    assertEquals(6, pivots.size());
    for (int i = 0; i < 5; i++) {
      pivot = pivots.get(i);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("aaa"));
      assertEquals(pivot.toString(), 300, pivot.getCount());
    }
    pivot = pivots.get(5);
    assertEquals(pivot.toString(), "tail", pivot.getValue());
    assertEquals(pivot.toString(), 135, pivot.getCount());
    // check the sub pivots
    pivots = pivot.getPivot();
    assertEquals(5, pivots.size());
    pivot = pivots.get(0);
    assertEquals(pivot.toString(), "tailB", pivot.getValue());
    assertEquals(pivot.toString(), 17, pivot.getCount());
    for (int i = 1; i < 5; i++) { // ccc(0-3)
      pivot = pivots.get(i);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("ccc"));
      assertEquals(pivot.toString(), 14, pivot.getCount());
    }

    // however with a lower limit and overrequesting disabled, 
    // we're going to miss out on tailB

    pivots = queryServer( params( "q", "*:*",
                                  "shards", getShardsString(),
                                  "facet", "true",
                                  "facet.limit", "6",
                                  "f.bar_s.facet.overrequest.ratio", "0",
                                  "f.bar_s.facet.overrequest.count", "0",
                                  "f.bar_s.facet.limit", "5",
                                  "facet.pivot", "foo_s,bar_s" )
                          ).getFacetPivot().get("foo_s,bar_s");
    assertEquals(6, pivots.size());
    for (int i = 0; i < 5; i++) {
      pivot = pivots.get(i);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("aaa"));
      assertEquals(pivot.toString(), 300, pivot.getCount());
    }
    pivot = pivots.get(5);
    assertEquals(pivot.toString(), "tail", pivot.getValue());
    assertEquals(pivot.toString(), 135, pivot.getCount());
    // check the sub pivots
    pivots = pivot.getPivot();
    assertEquals(5, pivots.size());
    for (int i = 0; i < 5; i++) { // ccc(0-4)
      pivot = pivots.get(i);
      assertTrue(pivot.toString(), pivot.getValue().toString().startsWith("ccc"));
      assertEquals(pivot.toString(), 14, pivot.getCount());
    }

  }

  private void doTestDeepPivotStats() throws Exception {
    // Deep checking of some Facet stats - no refinement involved here

    List<PivotField> pivots = 
      query("q", "*:*",
            "shards", getShardsString(),
            "facet", "true",
            "rows" , "0",
            "facet.pivot","{!stats=s1}foo_s,bar_s",
            "stats", "true",
            "stats.field", "{!key=avg_price tag=s1}" + STAT_FIELD).getFacetPivot().get("foo_s,bar_s");
    PivotField aaa0PivotField = pivots.get(0);
    assertEquals("aaa0", aaa0PivotField.getValue());
    assertEquals(300, aaa0PivotField.getCount());

    FieldStatsInfo aaa0StatsInfo = aaa0PivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("avg_price", aaa0StatsInfo.getName());
    assertEquals(-99.0, aaa0StatsInfo.getMin());
    assertEquals(693.0, aaa0StatsInfo.getMax());
    assertEquals(300, (long) aaa0StatsInfo.getCount());
    assertEquals(0, (long) aaa0StatsInfo.getMissing());
    assertEquals(34650.0, aaa0StatsInfo.getSum());
    assertEquals(1.674585E7, aaa0StatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(115.5, (double) aaa0StatsInfo.getMean(), 0.1E-7);
    assertEquals(206.4493184076, aaa0StatsInfo.getStddev(), 0.1E-7);

    PivotField tailPivotField = pivots.get(5);
    assertEquals("tail", tailPivotField.getValue());
    assertEquals(135, tailPivotField.getCount());

    FieldStatsInfo tailPivotFieldStatsInfo = tailPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("avg_price", tailPivotFieldStatsInfo.getName());
    assertEquals(0.0, tailPivotFieldStatsInfo.getMin());
    assertEquals(44.0, tailPivotFieldStatsInfo.getMax());
    assertEquals(90, (long) tailPivotFieldStatsInfo.getCount());
    assertEquals(45, (long) tailPivotFieldStatsInfo.getMissing());
    assertEquals(1980.0, tailPivotFieldStatsInfo.getSum());
    assertEquals(22.0, (double) tailPivotFieldStatsInfo.getMean(), 0.1E-7);
    assertEquals(58740.0, tailPivotFieldStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(13.0599310011, tailPivotFieldStatsInfo.getStddev(), 0.1E-7);

    PivotField tailBPivotField = tailPivotField.getPivot().get(0);
    assertEquals("tailB", tailBPivotField.getValue());
    assertEquals(17, tailBPivotField.getCount());

    FieldStatsInfo tailBPivotFieldStatsInfo = tailBPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("avg_price", tailBPivotFieldStatsInfo.getName());
    assertEquals(35.0, tailBPivotFieldStatsInfo.getMin());
    assertEquals(40.0, tailBPivotFieldStatsInfo.getMax());
    assertEquals(12, (long) tailBPivotFieldStatsInfo.getCount());
    assertEquals(5, (long) tailBPivotFieldStatsInfo.getMissing());
    assertEquals(450.0, tailBPivotFieldStatsInfo.getSum());
    assertEquals(37.5, (double) tailBPivotFieldStatsInfo.getMean(), 0.1E-7);
    assertEquals(16910.0, tailBPivotFieldStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(1.78376517, tailBPivotFieldStatsInfo.getStddev(), 0.1E-7);
  }

}
