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
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

import java.util.List;

public class DistributedFacetPivotWhiteBoxTest extends BaseDistributedSearchTestCase {

  public DistributedFacetPivotWhiteBoxTest() {
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
    index(id, 21, "place_t", "krakow london la dublin", "company_t",
        "microsoft fujitsu null polecat", "price_ti", "29");
    index(id, 22, "place_t", "krakow london cardiff", "company_t",
        "polecat null bbc", "price_ti", "39");
    index(id, 23, "place_t", "krakow london", "company_t", "", "price_ti", "29");
    index(id, 24, "place_t", "krakow la", "company_t", "");
    index(id, 25, "company_t", "microsoft polecat null fujitsu null bbc", "price_ti", "59");
    index(id, 26, "place_t", "krakow", "company_t", "null");
    index(id, 27, "place_t", "krakow cardiff dublin london la",
        "company_t", "null microsoft polecat bbc fujitsu");
    index(id, 28, "place_t", "krakow cork", "company_t", "fujitsu rte");
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    doShardTestTopStats();
    doTestRefinementRequest();
  }

  /** 
   * recreates the initial request to a shard in a distributed query
   * confirming that both top level stats, and per-pivot stats are returned.
   */
  private void doShardTestTopStats() throws Exception {

    SolrParams params = params("facet", "true", 
                               "q", "*:*", 
                               // "wt", "javabin", 
                               "facet.pivot", "{!stats=s1}place_t,company_t", 
                               // "version", "2", 
                               "start", "0", "rows", "0", 
                               "fsv", "true", 
                               "fl", "id,score",
                               "stats", "true", 
                               "stats.field", "{!key=avg_price tag=s1}price_ti",
                               "f.place_t.facet.limit", "160", 
                               "f.place_t.facet.pivot.mincount", "0", 
                               "f.company_t.facet.limit", "160", 
                               "f.company_t.facet.pivot.mincount", "0", 
                               "isShard", "true", "distrib", "false");
    QueryResponse rsp = queryServer(new ModifiableSolrParams(params));

    assertNotNull("initial shard request should include non-null top level stats", 
                  rsp.getFieldStatsInfo());
    assertFalse("initial shard request should include top level stats", 
                rsp.getFieldStatsInfo().isEmpty());

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");
    for (PivotField pivotField : placePivots) {
      assertFalse("pivot stats should not be empty in initial request",
                  pivotField.getFieldStatsInfo().isEmpty());
    }
  }

  /** 
   * recreates a pivot refinement request to a shard in a distributed query
   * confirming that the per-pivot stats are returned, but not the top level stats
   * because they shouldn't be overcounted.
   */
  private void doTestRefinementRequest() throws Exception {
    SolrParams params = params("facet.missing", "true",
                               "facet", "true", 
                               "facet.limit", "4", 
                               "distrib", "false", 
                               // "wt", "javabin",
                               // "version", "2", 
                               "rows", "0", 
                               "facet.sort", "index",
                               "fpt0", "~krakow",
                               "facet.pivot.mincount", "-1", 
                               "isShard", "true", 
                               "facet.pivot", "{!fpt=0 stats=st1}place_t,company_t",
                               "stats", "false", 
                               "stats.field", "{!key=sk1 tag=st1,st2}price_ti");
    QueryResponse rsp = clients.get(0).query(new ModifiableSolrParams(params));

    assertNull("pivot refine request should *NOT* include top level stats", 
               rsp.getFieldStatsInfo());

    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");

    assertEquals("asked to refine exactly one place",
                 1, placePivots.size());
    assertFalse("pivot stats should not be empty in refinement request",
                placePivots.get(0).getFieldStatsInfo().isEmpty());

  }
}
