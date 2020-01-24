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

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import junit.framework.AssertionFailedError;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class DistributedFacetPivotLargeTest extends BaseDistributedSearchTestCase {
  
  public static final String SPECIAL = ""; 

  public DistributedFacetPivotLargeTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
  }
  
  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    this.stress = 0 ;
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);    
    
    setupDistributedPivotFacetDocuments();
    
    QueryResponse rsp = null;
    
    List<PivotField> pivots = null;
    PivotField firstInt = null;
    PivotField firstBool = null;
    PivotField firstDate = null;
    PivotField firstPlace = null;
    PivotField firstCompany = null;

    // basic check w/ limit & default sort (count)
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","place_s,company_t",
                 FacetParams.FACET_LIMIT, "12"); 
    pivots = rsp.getFacetPivot().get("place_s,company_t");
    assertEquals(12, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 257, firstPlace);
    assertPivot("company_t", "bbc", 101, firstPlace.getPivot().get(0));
    // Microsoft will come back wrong if refinement was not done correctly
    assertPivot("company_t", "microsoft", 56, firstPlace.getPivot().get(1));

    // trivial mincount=0 check
    rsp = query( "q", "does_not_exist_s:foo",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","company_t",
                 FacetParams.FACET_LIMIT, "10",
                 FacetParams.FACET_PIVOT_MINCOUNT,"0"); 
    pivots = rsp.getFacetPivot().get("company_t");
    assertEquals(10, pivots.size());
    for (PivotField p : pivots) {
      assertEquals(0, p.getCount());
    }

    // sanity check limit=0 w/ mincount=0 & missing=true
    rsp = query( "q", "*:*",
                  "rows", "0",
                  "facet","true",
                  "f.company_t.facet.limit", "10",
                  "facet.pivot","special_s,bogus_s,company_t",
                  "facet.missing", "true",
                  FacetParams.FACET_LIMIT, "0",
                  FacetParams.FACET_PIVOT_MINCOUNT,"0");
    pivots = rsp.getFacetPivot().get("special_s,bogus_s,company_t");
    assertEquals(1, pivots.size()); // only the missing
    assertPivot("special_s", null, docNumber - 5, pivots.get(0)); // 5 docs w/special_s
    assertEquals(pivots.toString(), 1, pivots.get(0).getPivot().size());
    assertPivot("bogus_s", null, docNumber - 5 , pivots.get(0).getPivot().get(0)); // 5 docs w/special_s
    PivotField bogus = pivots.get(0).getPivot().get(0);
    assertEquals(bogus.toString(), 11, bogus.getPivot().size());
    // last value would always be missing docs
    assertPivot("company_t", null, 2, bogus.getPivot().get(10)); // 2 docs w/company_t

    // basic check w/ default sort, limit, & mincount==0
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","place_s,company_t",
                 FacetParams.FACET_LIMIT, "50",
                 FacetParams.FACET_PIVOT_MINCOUNT,"0"); 
    pivots = rsp.getFacetPivot().get("place_s,company_t");
    assertEquals(50, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 257, firstPlace);
    assertPivot("company_t", "bbc", 101, firstPlace.getPivot().get(0));
    // Microsoft will come back wrong if refinement was not done correctly
    assertPivot("company_t", "microsoft", 56, firstPlace.getPivot().get(1));

    // sort=index + offset + limit w/ some variables
    for (SolrParams variableParams : 
           new SolrParams[] { // bother variations should kwrk just as well
             // defauts
             params(),
             // force refinement
             params(FacetParams.FACET_OVERREQUEST_RATIO, "1", 
                    FacetParams.FACET_OVERREQUEST_COUNT, "0")           }) {

      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.sort","index",
                                                      "f.place_s.facet.limit", "20",
                                                      "f.place_s.facet.offset", "40",
                                                      "facet.pivot", "place_s,company_t"),
                                              variableParams );

      try {
        rsp = query( p );
        pivots = rsp.getFacetPivot().get("place_s,company_t");
        assertEquals(20, pivots.size()); // limit
        for (int i = 0; i < 10; i++) {
          PivotField place = pivots.get(i); 
          assertTrue(place.toString(), place.getValue().toString().endsWith("placeholder"));
          assertEquals(3, place.getPivot().size());
          assertPivot("company_t", "bbc", 6, place.getPivot().get(0));
          assertPivot("company_t", "microsoft", 6, place.getPivot().get(1));
          assertPivot("company_t", "polecat", 6, place.getPivot().get(2));
        }
        assertPivot("place_s", "cardiff", 257, pivots.get(10));
        assertPivot("place_s", "krakaw", 1, pivots.get(11));
        assertPivot("place_s", "medical staffing network holdings, inc.", 51, pivots.get(12));
        for (int i = 13; i < 20; i++) {
          PivotField place = pivots.get(i); 
          assertTrue(place.toString(), place.getValue().toString().startsWith("placeholder"));
          assertEquals(1, place.getPivot().size());
          PivotField company = place.getPivot().get(0);
          assertTrue(company.toString(), company.getValue().toString().startsWith("compholder"));
          assertEquals(company.toString(), 1, company.getCount());
        }
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // sort=index + mincount=0
    //
    // SOLR-6329: facet.pivot.mincount=0 doesn't work well with distrib
    //
    // broken honda
    //
    // This is tricky, here's what i think is happening.... 
    // - "company:honda" only exists on twoShard, and only w/ "place:cardiff"
    // - twoShard has no other places in its docs
    // - twoShard can't return any other places to w/ honda as a count=0 sub-value
    // - if we refined all other companies places, would twoShard return honda==0 ?
    //   ... but there's no refinement since mincount==0
    // - would it even matter
    //
    // should we remove the refinement short circuit?
    //
    // rsp = query( params( "q", "*:*",
    //                      "rows", "0",
    //                      "facet","true",
    //                      "facet.sort","index",
    //                      "f.place_s.facet.limit", "20",
    //                      "f.place_s.facet.offset", "40",
    //                      FacetParams.FACET_PIVOT_MINCOUNT,"0",
    //                      "facet.pivot", "place_s,company_t") );
    // // TODO: more asserts
    //
    //
    // really trivial demonstration of the above problem
    // 
    // rsp = query( params( "q", "*:*",
    //                      "rows", "0",
    //                      "facet","true",
    //                      FacetParams.FACET_PIVOT_MINCOUNT,"0",
    //                      "facet.pivot", "top_s,sub_s") );

    // facet.missing=true + facet.sort=index + facet.pivot.mincount > 0 (SOLR-7829)
    final int expectedNumDocsMissingBool = 111;
    for (String facetSort : new String[] {"count", "index"}) {
      for (int mincount : new int[] { 1, 20,
                                      (expectedNumDocsMissingBool / 2) - 1,
                                      (expectedNumDocsMissingBool / 2) + 1,
                                      expectedNumDocsMissingBool }) {
             
        SolrParams p = params( "q", "*:*",
                               "fq","-real_b:true", // simplify asserts by ruling out true counts
                               "rows", "0",
                               "facet","true",
                               "facet.pivot", "real_b",
                               "facet.missing", "true",
                               "facet.pivot.mincount", ""+mincount,
                               "facet.sort", facetSort);
        
        try {
          rsp = query( p );
          pivots = rsp.getFacetPivot().get("real_b");
          assertEquals(2, pivots.size()); // false, missing - in that order, regardless of sort
          assertPivot("real_b", false, 300, pivots.get(0)); 
          assertPivot("real_b", null, expectedNumDocsMissingBool, pivots.get(1));
          
        } catch (AssertionFailedError ae) {
          throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
        }
      }
    }
    
    // basic check w/ limit & index sort
    for (SolrParams facetParams : 
           // results should be the same regardless of whether local params are used
           new SolrParams[] {
             // Broken: SOLR-6193
             // params("facet.pivot","{!facet.limit=4 facet.sort=index}place_s,company_t"),
             // params("facet.pivot","{!facet.sort=index}place_s,company_t",
             //        FacetParams.FACET_LIMIT, "4"),
             params("facet.pivot","place_s,company_t",
                    FacetParams.FACET_LIMIT, "4",
                    "facet.sort", "index")                                  }) {
      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true"), 
                                              facetParams );
      try {
        rsp = query( p );
        pivots = rsp.getFacetPivot().get("place_s,company_t");
        assertEquals(4, pivots.size());
        firstPlace = pivots.get(0);
        assertPivot("place_s", "0placeholder", 6, firstPlace);
        firstCompany = firstPlace.getPivot().get(0);
        assertPivot("company_t", "bbc", 6, firstCompany);
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // check of a single level pivot using sort=index w/mincount big enough
    // to triggers per-shard mincount > num docs on one shard
    // (beefed up test of same with nested pivot below)
    for (int limit : Arrays.asList(4, 444444, -1)) {
      SolrParams p = params("q", "*:*",
                            "rows", "0",
                            // skip place_s:Nplaceholder buckets
                            "fq","-hiredate_dt:\"2012-10-01T12:30:00Z\"", 
                            // skip company_t:compHolderN buckets from twoShard
                            "fq","-(+company_t:compHolder* +real_b:true)",
                            "facet","true",
                            "facet.pivot","place_s",
                            FacetParams.FACET_PIVOT_MINCOUNT, "50",
                            FacetParams.FACET_LIMIT, ""+limit,
                            "facet.sort", "index");
      rsp = null;
      try {
        rsp = query( p );
        assertPivot("place_s", "cardiff", 107, rsp.getFacetPivot().get("place_s").get(0));
        // - zeroShard  = 50 ... above per-shard min of 50/(numShards=4)
        // - oneShard   =  5 ... below per-shard min of 50/(numShards=4) .. should be refined
        // - twoShard   = 52 ... above per-shard min of 50/(numShards=4)
        // = threeShard =  0 ... should be refined and still match nothing
      } catch (AssertionError ae) {
        throw new AssertionError(ae.getMessage() + ": " + p.toString() + " ==> " + rsp, ae);
      }
    }
    
    // test permutations of mincount & limit with sort=index
    // (there is a per-shard optimization on mincount when sort=index is used)
    for (int limit : Arrays.asList(4, 444444, -1)) {
      SolrParams p = params("q", "*:*",
                            "rows", "0",
                            // skip place_s:Nplaceholder buckets
                            "fq","-hiredate_dt:\"2012-10-01T12:30:00Z\"", 
                            // skip company_t:compHolderN buckets from twoShard
                            "fq","-(+company_t:compHolder* +real_b:true)",
                            "facet","true",
                            "facet.pivot","place_s,company_t",
                            FacetParams.FACET_PIVOT_MINCOUNT, "50",
                            FacetParams.FACET_LIMIT, ""+limit,
                            "facet.sort", "index");
      rsp = null;
      try {
        rsp = query( p );
        pivots = rsp.getFacetPivot().get("place_s,company_t");
        firstPlace = pivots.get(0);
        assertPivot("place_s", "cardiff", 107, firstPlace);
        //
        assertPivot("company_t", "bbc",      101, firstPlace.getPivot().get(0)); 
        assertPivot("company_t", "honda",     50, firstPlace.getPivot().get(1)); 
        assertPivot("company_t", "microsoft", 56, firstPlace.getPivot().get(2)); 
        assertPivot("company_t", "polecat",   52, firstPlace.getPivot().get(3)); 
      } catch (AssertionError ae) {
        throw new AssertionError(ae.getMessage() + ": " + p.toString() + " ==> " + rsp, ae);
      }
    }

    { // similar to the test above, but now force a restriction on the over request and allow
      // terms that are early in index sort -- but don't meet the mincount overall -- to be considered
      // in the first phase. (SOLR-12954)
      SolrParams p = params("q", "*:*",
                            "rows", "0",
                            // skip company_t:compHolderN buckets from twoShard
                            "fq","-(+company_t:compHolder* +real_b:true)",
                            "facet","true",
                            "facet.pivot","place_s,company_t",
                            // the (50) Nplaceholder place_s values exist in 6 each on oneShard
                            FacetParams.FACET_PIVOT_MINCOUNT, ""+(6 * shardsArr.length),
                            FacetParams.FACET_LIMIT, "4",
                            "facet.sort", "index");
      rsp = null;
      try {
        rsp = query( p ); 
        pivots = rsp.getFacetPivot().get("place_s,company_t");
        firstPlace = pivots.get(0);
        assertPivot("place_s", "cardiff", 107, firstPlace);
        //
        assertPivot("company_t", "bbc",      101, firstPlace.getPivot().get(0)); 
        assertPivot("company_t", "honda",     50, firstPlace.getPivot().get(1)); 
        assertPivot("company_t", "microsoft", 56, firstPlace.getPivot().get(2)); 
        assertPivot("company_t", "polecat",   52, firstPlace.getPivot().get(3)); 
      } catch (AssertionError ae) {
        throw new AssertionError(ae.getMessage() + ": " + p.toString() + " ==> " + rsp, ae);
      }
    }
    
    // Pivot Faceting (combined wtih Field Faceting)
    for (SolrParams facetParams : 
           // with and w/o an excluded fq
           // (either way, facet results should be the same)
           new SolrParams[] { 
             params("facet.pivot","place_s,company_t",
                    "facet.field","place_s"),
             params("facet.pivot","{!ex=ok}place_s,company_t",
                    "facet.field","{!ex=ok}place_s",
                    "fq","{!tag=ok}place_s:cardiff"),
             params("facet.pivot","{!ex=pl,co}place_s,company_t",
                    "fq","{!tag=pl}place_s:cardiff",
                    "fq","{!tag=co}company_t:bbc")                      }) {
      
      // default order (count)
      rsp = query( SolrParams.wrapDefaults(params("q", "*:*",
                                                  "rows", "0",
                                                  "facet","true",
                                                  FacetParams.FACET_LIMIT, "4"),
                                           facetParams) );
      pivots = rsp.getFacetPivot().get("place_s,company_t");
      assertEquals(4, pivots.size());
      firstPlace = pivots.get(0);
      assertPivot("place_s", "cardiff", 257, firstPlace);
      assertEquals(4, firstPlace.getPivot().size());
      firstCompany = firstPlace.getPivot().get(0);
      assertPivot("company_t", "bbc", 101, firstCompany);

      // Index Order
      rsp = query( SolrParams.wrapDefaults(params("q", "*:*",
                                                  "rows", "0",
                                                  "facet","true",
                                                  FacetParams.FACET_LIMIT, "4",
                                                  "facet.sort", "index"),
                                           facetParams) );
      pivots = rsp.getFacetPivot().get("place_s,company_t");
      assertEquals(4, pivots.size());
      firstPlace = pivots.get(0);
      assertPivot("place_s", "0placeholder", 6, firstPlace);
      assertEquals(3, firstPlace.getPivot().size()); // num vals in data < limit==3
      firstCompany = firstPlace.getPivot().get(0);
      assertPivot("company_t", "bbc", 6, firstCompany);
      
      // Field level limits 
      rsp = query( SolrParams.wrapDefaults(params("q", "*:*",
                                                  "rows", "0",
                                                  "facet","true",
                                                  "f.place_s.facet.limit","2",
                                                  "f.company_t.facet.limit","4"),
                                           facetParams) );
      pivots = rsp.getFacetPivot().get("place_s,company_t");
      assertEquals(2, pivots.size());
      firstPlace = pivots.get(0);
      assertPivot("place_s", "cardiff", 257, firstPlace);
      assertEquals(4, firstPlace.getPivot().size());
      firstCompany = firstPlace.getPivot().get(0);
      assertPivot("company_t", "bbc", 101, firstCompany);
    }

    // Pivot Faceting Count w/fq (not excluded)
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "fq","place_s:cardiff",
                 "facet","true",
                 "facet.pivot","place_s,company_t",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("place_s,company_t");
    assertEquals(1, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 257, firstPlace);
    assertEquals(4, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "bbc", 101, firstCompany);


    // Same Pivot - one with exclusion and one w/o
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "fq","{!tag=ff}pay_i:[2000 TO *]",
                 "facet","true",
                 "facet.pivot","{!key=filt}place_s,company_t",
                 "facet.pivot","{!key=nofilt ex=ff}place_s,company_t",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("filt");
    assertEquals(4, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 105, firstPlace);
    assertEquals(4, firstPlace.getPivot().size());
    assertPivot("company_t", "bbc", 101, firstPlace.getPivot().get(0));
    assertPivot("company_t", "microsoft", 54, firstPlace.getPivot().get(1));
    //
    pivots = rsp.getFacetPivot().get("nofilt");
    assertEquals(4, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 257, firstPlace);
    assertEquals(4, firstPlace.getPivot().size());
    assertPivot("company_t", "bbc", 101, firstPlace.getPivot().get(0));
    assertPivot("company_t", "microsoft", 56, firstPlace.getPivot().get(1));

    // Same Pivot - one in default (count) order and one in index order
    //
    // Broken: SOLR-6193 - the facet.sort localparam isn't being picked up correctly
    //
    // rsp = query( "q", "*:*",
    //              "rows", "0",
    //              "facet","true",
    //              "fq","pay_i:[2000 TO *]",
    //              "facet.pivot","{!key=sc}place_s,company_t",
    //              "facet.pivot","{!key=si facet.sort=index}place_s,company_t",
    //              FacetParams.FACET_LIMIT, "4");
    // pivots = rsp.getFacetPivot().get("sc");
    // assertEquals(4, pivots.size());
    // firstPlace = pivots.get(0);
    // assertPivot("place_s", "cardiff", 105, firstPlace); 
    // assertEquals(4, firstPlace.getPivot().size());
    // assertPivot("company_t", "bbc", 101, firstPlace.getPivot().get(0));
    // assertPivot("company_t", "microsoft", 54, firstPlace.getPivot().get(1));
    // //
    // pivots = rsp.getFacetPivot().get("si");
    // assertEquals(4, pivots.size());
    // firstPlace = pivots.get(0);
    // assertPivot("place_s", "0placeholder", 6, firstPlace); 
    // assertEquals(3, firstPlace.getPivot().size()); // only 3 in the data < facet.limit
    // assertPivot("company_t", "bbc", 6, firstPlace.getPivot().get(0));
    // assertPivot("company_t", "microsoft", 6, firstPlace.getPivot().get(1));


    // Field level limits and small offset
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","place_s,company_t",
                 "f.place_s.facet.limit","2",
                 "f.company_t.facet.limit","4",
                 "facet.offset","1");
    pivots = rsp.getFacetPivot().get("place_s,company_t");
    assertEquals(2, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "medical staffing network holdings, inc.", 51, firstPlace);
    assertEquals(2, firstPlace.getPivot().size()); // num vals in data < limit==4
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "bbc", 50, firstCompany);
    
    
    // Field level offsets and limit
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "fq","{!tag=pl}place_s:cardiff",
                 "facet","true",
                 "facet.pivot","{!ex=pl}place_s,company_t",
                 "f.place_s.facet.offset","1",
                 "f.company_t.facet.offset","2",
                 FacetParams.FACET_LIMIT, "4"); 
    pivots = rsp.getFacetPivot().get("place_s,company_t");
    assertEquals(4, pivots.size());
    firstPlace = pivots.get(0);
    assertPivot("place_s", "medical staffing network holdings, inc.", 51, firstPlace);
    assertEquals(1, firstPlace.getPivot().size()); // num vals in data < limit==4
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "polecat", 50, firstCompany);
     

    // datetime
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","hiredate_dt,place_s,company_t",
                 "f.hiredate_dt.facet.limit","2",
                 "f.hiredate_dt.facet.offset","1",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("hiredate_dt,place_s,company_t");
    assertEquals(2, pivots.size());
    firstDate = pivots.get(0); // 2012-09-01T12:30:00Z
    assertPivot("hiredate_dt", new Date(1346502600000L), 200, firstDate);
    assertEquals(1, firstDate.getPivot().size()); // num vals in data < limit==4
    firstPlace = firstDate.getPivot().get(0);
    assertPivot("place_s", "cardiff", 200, firstPlace);
    assertEquals(4, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "bbc", 50, firstCompany);

    // int
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","pay_i,place_s,company_t",
                 "f.pay_i.facet.limit","2",
                 "f.pay_i.facet.offset","1",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("pay_i,place_s,company_t");
    assertEquals(2, pivots.size());
    firstInt = pivots.get(0);
    assertPivot("pay_i", 2000, 50, firstInt);
    assertEquals(4, firstInt.getPivot().size());
    firstPlace = firstInt.getPivot().get(0);
    assertPivot("place_s", "0placeholder", 1, firstPlace);
    assertEquals(3, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "bbc", 1, firstCompany);
    
    // boolean
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","real_b,place_s,company_t",
                 "f.real_b.facet.missing","true",
                 "f.real_b.facet.limit","2",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("real_b,place_s,company_t");
    assertEquals(3, pivots.size());
    firstBool = pivots.get(0);
    assertPivot("real_b", false, 300, firstBool);
    assertEquals(4, firstBool.getPivot().size());
    firstPlace = firstBool.getPivot().get(0);
    assertPivot("place_s", "0placeholder", 6, firstPlace);
    assertEquals(3, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "bbc", 6, firstCompany);
    
    // bogus fields
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","doesntexist_t,neitherdoi_i",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("doesntexist_t,neitherdoi_i");
    assertEquals(0, pivots.size());

    // bogus fields with facet.missing
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","doesntexist_t,neitherdoi_i",
                 "facet.missing", "true",
                 FacetParams.FACET_LIMIT, "4");
    pivots = rsp.getFacetPivot().get("doesntexist_t,neitherdoi_i");
    assertEquals(1, pivots.size());
    assertPivot("doesntexist_t", null, docNumber, pivots.get(0));
    assertEquals(1, pivots.get(0).getPivot().size());
    assertPivot("neitherdoi_i", null, docNumber, pivots.get(0).getPivot().get(0));

    // Negative facet limit
    for (SolrParams facetParams : 
           // results should be the same regardless of whether facet.limit is global,
           // a local param, or specified as a per-field override for both fields
           new SolrParams[] {
             params(FacetParams.FACET_LIMIT, "-1",
                    "facet.pivot","place_s,company_t"),
             // Broken: SOLR-6193
             // params("facet.pivot","{!facet.limit=-1}place_s,company_t"),    
             params("f.place_s.facet.limit", "-1",
                    "f.company_t.facet.limit", "-1",
                    "facet.pivot","place_s,company_t")                       }) {

      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.sort", "count" ),
                                              facetParams);
      try {
        rsp = query( p );
        pivots = rsp.getFacetPivot().get("place_s,company_t");
        assertEquals(103, pivots.size());
        firstPlace = pivots.get(0);
        assertPivot("place_s", "cardiff", 257, firstPlace);
        assertEquals(54, firstPlace.getPivot().size());
        firstCompany = firstPlace.getPivot().get(0);
        assertPivot("company_t","bbc", 101, firstCompany);
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // Negative per-field facet limit (outer)
    for (SolrParams facetParams : 
           // results should be the same regardless of whether per-field facet.limit is
           // a global or a local param
           new SolrParams[] {
             // Broken: SOLR-6193
             // params( "facet.pivot","{!f.id.facet.limit=-1}place_s,id" ),
             params( "facet.pivot","place_s,id",
                     "f.id.facet.limit", "-1")                            }) {
      
      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*", 
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.sort", "count" ),
                                              facetParams);
      try {
        rsp = query( p );
        pivots = rsp.getFacetPivot().get("place_s,id");
        assertEquals(100, pivots.size()); // default
        firstPlace = pivots.get(0);
        assertPivot("place_s", "cardiff", 257, firstPlace);
        assertEquals(257, firstPlace.getPivot().size());
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // Negative per-field facet limit (inner)
    for (SolrParams facetParams : 
           // results should be the same regardless of whether per-field facet.limit is
           // a global or a local param
           new SolrParams[] {
             // Broken: SOLR-6193
             // params( "facet.pivot","{!f.place_s.facet.limit=-1}place_s,id" ),
             params( "facet.pivot","place_s,id",
                     "f.place_s.facet.limit", "-1")                       }) {
      
      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*", 
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.sort", "count" ),
                                              facetParams);
      try {
        rsp = query( p );
        pivots = rsp.getFacetPivot().get("place_s,id");
        assertEquals(103, pivots.size());
        firstPlace = pivots.get(0);
        assertPivot("place_s", "cardiff", 257, firstPlace);
        assertEquals(100, firstPlace.getPivot().size()); // default
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }
    
    // Mincount + facet.pivot 2 different ways (swap field order)
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "facet.pivot","place_s,company_t",
                 "facet.pivot","company_t,place_s",
                 FacetParams.FACET_PIVOT_MINCOUNT,"6");
    pivots = rsp.getFacetPivot().get("place_s,company_t");
    assertEquals(52, pivots.size()); 
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 257, firstPlace);
    assertEquals(4, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "bbc", 101, firstCompany);
    //
    pivots = rsp.getFacetPivot().get("company_t,place_s");
    assertEquals(4, pivots.size());
    firstCompany = pivots.get(0);
    assertPivot("company_t", "bbc", 451, firstCompany);
    assertEquals(52, firstCompany.getPivot().size());
    firstPlace = firstCompany.getPivot().get(0);
    assertPivot("place_s", "cardiff", 101, firstPlace);

    // refine on SPECIAL empty string
    rsp = query( "q", "*:*",
                 "fq", "-place_s:0placeholder",
                 "rows", "0",
                 "facet","true",
                 "facet.limit","1",
                 FacetParams.FACET_OVERREQUEST_RATIO, "0", // force refinement
                 FacetParams.FACET_OVERREQUEST_COUNT, "1", // force refinement
                 "facet.pivot","special_s,company_t");
    assertEquals(docNumber - 6, rsp.getResults().getNumFound()); // all docs but 0place
    pivots = rsp.getFacetPivot().get("special_s,company_t");
    assertEquals(1, pivots.size()); 
    firstPlace = pivots.get(0);
    assertPivot("special_s", SPECIAL, 3, firstPlace);
    assertEquals(1, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "microsoft", 2, firstCompany);

    // TODO test "company_t,special_s" as well


    // refine on SPECIAL empty string & facet.missing
    // Also proves refinement on non-top elements occurs and allows them to get into the top
    rsp = query( "q", "*:*",
                 "fq", "-place_s:0placeholder",
                 "rows", "0",
                 "facet","true",
                 "facet.limit","1",
                 "facet.missing","true",
                 FacetParams.FACET_OVERREQUEST_RATIO, "0", // force refinement
                 FacetParams.FACET_OVERREQUEST_COUNT, "2", // force refinement
                 "facet.pivot","special_s,company_t");
    assertEquals(docNumber - 6, rsp.getResults().getNumFound()); // all docs but 0place
    pivots = rsp.getFacetPivot().get("special_s,company_t");
    assertEquals(2, pivots.size()); 
    firstPlace = pivots.get(0);
    assertPivot("special_s", SPECIAL, 3, firstPlace);
    assertEquals(1, firstPlace.getPivot().size());
    firstCompany = firstPlace.getPivot().get(0);
    assertPivot("company_t", "microsoft", 2, firstCompany);
    // last is "missing" val
    assertPivot("special_s", null, docNumber -6 -3 -2, pivots.get(1)); // -0place -SPECIAL -xxx

    // forced refinement on facet.missing
    rsp = query( "q", "*:*",
                 "rows", "0",
                 "facet","true",
                 "f.bogus_x_s.facet.missing","true",
                 "f.bogus_y_s.facet.missing","true",
                 "facet.pivot","bogus_x_s,place_s,bogus_y_s,company_t",
                 FacetParams.FACET_LIMIT, "12"); 
    pivots = rsp.getFacetPivot().get("bogus_x_s,place_s,bogus_y_s,company_t");
    assertEquals(1, pivots.size()); // just the missing value for bogus_x_s
    assertPivot("bogus_x_s", null, docNumber, pivots.get(0));
    pivots = pivots.get(0).getPivot();
    assertEquals(12, pivots.size()); // places
    firstPlace = pivots.get(0);
    assertPivot("place_s", "cardiff", 257, firstPlace);
    assertEquals(1, firstPlace.getPivot().size()); // just the missing value for bogus_y_s
    assertPivot("bogus_y_s", null, 257, firstPlace.getPivot().get(0));
    assertPivot("company_t", "bbc", 101, firstPlace.getPivot().get(0).getPivot().get(0));
    // Microsoft will come back wrong if refinement was not done correctly
    assertPivot("company_t", "microsoft", 56, firstPlace.getPivot().get(0).getPivot().get(1));





    // Overrequesting a lot
    this.query( "q", "*:*",
                "rows", "0",
                "facet", "true",
                "facet.pivot","place_s,company_t",
                FacetParams.FACET_OVERREQUEST_RATIO, "10",
                FacetParams.FACET_OVERREQUEST_COUNT, "100");    
    
    // Overrequesting off
    this.query( "q", "*:*",
                "rows", "0",
                "facet", "true",
                "facet.pivot","place_s,company_t",
                FacetParams.FACET_OVERREQUEST_RATIO, "0",
                FacetParams.FACET_OVERREQUEST_COUNT, "0");

    doTestDeepPivotStats();
    doTestPivotRanges();
  }

  private void doTestDeepPivotStats() throws Exception {

    QueryResponse rsp = query("q", "*:*",
                              "rows", "0",
                              "facet", "true",
                              "facet.pivot","{!stats=s1}place_s,company_t",
                              "stats", "true",
                              "stats.field", "{!key=avg_price tag=s1}pay_i");

    List<PivotField> pivots = rsp.getFacetPivot().get("place_s,company_t");

    PivotField cardiffPivotField = pivots.get(0);
    assertEquals("cardiff", cardiffPivotField.getValue());
    assertEquals(257, cardiffPivotField.getCount());

    FieldStatsInfo cardiffStatsInfo = cardiffPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("avg_price", cardiffStatsInfo.getName());
    assertEquals(0.0, cardiffStatsInfo.getMin());
    assertEquals(8742.0, cardiffStatsInfo.getMax());
    assertEquals(257, (long) cardiffStatsInfo.getCount());
    assertEquals(0, (long) cardiffStatsInfo.getMissing());
    assertEquals(347554.0, cardiffStatsInfo.getSum());
    assertEquals(8.20968772E8, cardiffStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(1352.35019455253, (double) cardiffStatsInfo.getMean(), 0.1E-7);
    assertEquals(1170.86048165857, cardiffStatsInfo.getStddev(), 0.1E-7);

    PivotField bbcCardifftPivotField = cardiffPivotField.getPivot().get(0);
    assertEquals("bbc", bbcCardifftPivotField.getValue());
    assertEquals(101, bbcCardifftPivotField.getCount());

    FieldStatsInfo bbcCardifftPivotFieldStatsInfo = bbcCardifftPivotField.getFieldStatsInfo().get("avg_price");
    assertEquals(2400.0, bbcCardifftPivotFieldStatsInfo.getMin());
    assertEquals(8742.0, bbcCardifftPivotFieldStatsInfo.getMax());
    assertEquals(101, (long) bbcCardifftPivotFieldStatsInfo.getCount());
    assertEquals(0, (long) bbcCardifftPivotFieldStatsInfo.getMissing());
    assertEquals(248742.0, bbcCardifftPivotFieldStatsInfo.getSum());
    assertEquals(6.52422564E8, bbcCardifftPivotFieldStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(2462.792079208, (double) bbcCardifftPivotFieldStatsInfo.getMean(), 0.1E-7);
    assertEquals(631.0525860312, bbcCardifftPivotFieldStatsInfo.getStddev(), 0.1E-7);


    PivotField placeholder0PivotField = pivots.get(2);
    assertEquals("0placeholder", placeholder0PivotField.getValue());
    assertEquals(6, placeholder0PivotField.getCount());

    FieldStatsInfo placeholder0PivotFieldStatsInfo = placeholder0PivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("avg_price", placeholder0PivotFieldStatsInfo.getName());
    assertEquals(2000.0, placeholder0PivotFieldStatsInfo.getMin());
    assertEquals(6400.0, placeholder0PivotFieldStatsInfo.getMax());
    assertEquals(6, (long) placeholder0PivotFieldStatsInfo.getCount());
    assertEquals(0, (long) placeholder0PivotFieldStatsInfo.getMissing());
    assertEquals(22700.0, placeholder0PivotFieldStatsInfo.getSum());
    assertEquals(1.0105E8, placeholder0PivotFieldStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(3783.333333333, (double) placeholder0PivotFieldStatsInfo.getMean(), 0.1E-7);
    assertEquals(1741.742422595, placeholder0PivotFieldStatsInfo.getStddev(), 0.1E-7);

    PivotField microsoftPlaceholder0PivotField = placeholder0PivotField.getPivot().get(1);
    assertEquals("microsoft", microsoftPlaceholder0PivotField.getValue());
    assertEquals(6, microsoftPlaceholder0PivotField.getCount());

    FieldStatsInfo microsoftPlaceholder0PivotFieldStatsInfo = microsoftPlaceholder0PivotField.getFieldStatsInfo().get("avg_price");
    assertEquals("avg_price", microsoftPlaceholder0PivotFieldStatsInfo.getName());
    assertEquals(2000.0, microsoftPlaceholder0PivotFieldStatsInfo.getMin());
    assertEquals(6400.0, microsoftPlaceholder0PivotFieldStatsInfo.getMax());
    assertEquals(6, (long) microsoftPlaceholder0PivotFieldStatsInfo.getCount());
    assertEquals(0, (long) microsoftPlaceholder0PivotFieldStatsInfo.getMissing());
    assertEquals(22700.0, microsoftPlaceholder0PivotFieldStatsInfo.getSum());
    assertEquals(1.0105E8, microsoftPlaceholder0PivotFieldStatsInfo.getSumOfSquares(), 0.1E-7);
    assertEquals(3783.333333333, (double) microsoftPlaceholder0PivotFieldStatsInfo.getMean(), 0.1E-7);
    assertEquals(1741.742422595, microsoftPlaceholder0PivotFieldStatsInfo.getStddev(), 0.1E-7);
  }

  /**
   * spot checks some pivot values and the ranges hanging on them
   */
  private void doTestPivotRanges() throws Exception {

    // note: 'p0' is only a top level range, not included in per-pivot ranges
    for (SolrParams p : new SolrParams[]{
        // results should be identical for all of these
        params("facet.range", "{!key=p0 facet.range.gap=500}pay_i",
            "facet.range", "{!key=p1 tag=t1 facet.range.gap=100}pay_i",
            "facet.range", "{!key=p2 tag=t1 facet.range.gap=200}pay_i",
            "facet.range.start", "0",
            "facet.range.end", "1000"),
        params("facet.range", "{!key=p0 facet.range.gap=500}pay_i",
            "facet.range", "{!key=p1 tag=t1 facet.range.gap=100}pay_i",
            "facet.range", "{!key=p2 tag=t1 facet.range.gap=200}pay_i",
            "f.pay_i.facet.range.start", "0",
            "facet.range.end", "1000"),
        params("facet.range", "{!key=p0 facet.range.gap=500 facet.range.start=0}pay_i",
            "facet.range", "{!key=p1 tag=t1 facet.range.gap=100 facet.range.start=0}pay_i",
            "facet.range", "{!key=p2 tag=t1 facet.range.gap=200 facet.range.start=0}pay_i",
            "facet.range.end", "1000")}) {

      QueryResponse rsp
          = query(SolrParams.wrapDefaults(p, params("q", "*:*",
          "rows", "0",
          "facet", "true",
          "facet.pivot", "{!range=t1}place_s,company_t")));

      List<PivotField> pivots = rsp.getFacetPivot().get("place_s,company_t");
      PivotField pf = null; // changes as we spot check
      List<RangeFacet.Count> rfc = null; // changes as we spot check

      // 1st sanity check top level ranges
      assertEquals(3, rsp.getFacetRanges().size());
      assertRange("p0", 0, 500, 1000, 2, rsp.getFacetRanges().get(0));
      assertRange("p1", 0, 100, 1000, 10, rsp.getFacetRanges().get(1));
      assertRange("p2", 0, 200, 1000, 5, rsp.getFacetRanges().get(2));

      // check pivots...

      // first top level pivot value
      pf = pivots.get(0);
      assertPivot("place_s", "cardiff", 257, pf);
      assertRange("p1", 0, 100, 1000, 10, pf.getFacetRanges().get(0));
      assertRange("p2", 0, 200, 1000, 5, pf.getFacetRanges().get(1));

      rfc = pf.getFacetRanges().get(0).getCounts();
      assertEquals("200", rfc.get(2).getValue());
      assertEquals(14, rfc.get(2).getCount());
      assertEquals("300", rfc.get(3).getValue());
      assertEquals(15, rfc.get(3).getCount());

      rfc = pf.getFacetRanges().get(1).getCounts();
      assertEquals("200", rfc.get(1).getValue());
      assertEquals(29, rfc.get(1).getCount());

      // drill down one level of the pivot
      pf = pf.getPivot().get(0);
      assertPivot("company_t", "bbc", 101, pf);
      assertRange("p1", 0, 100, 1000, 10, pf.getFacetRanges().get(0));
      assertRange("p2", 0, 200, 1000, 5, pf.getFacetRanges().get(1));

      rfc = pf.getFacetRanges().get(0).getCounts();
      for (RangeFacet.Count c : rfc) {

        assertEquals(0, c.getCount()); // no docs in our ranges for this pivot drill down
      }

      // pop back up and spot check a different top level pivot value
      pf = pivots.get(53);
      assertPivot("place_s", "placeholder0", 1, pf);
      assertRange("p1", 0, 100, 1000, 10, pf.getFacetRanges().get(0));
      assertRange("p2", 0, 200, 1000, 5, pf.getFacetRanges().get(1));

      rfc = pf.getFacetRanges().get(0).getCounts();
      assertEquals("0", rfc.get(0).getValue());
      assertEquals(1, rfc.get(0).getCount());
      assertEquals("100", rfc.get(1).getValue());
      assertEquals(0, rfc.get(1).getCount());

      // drill down one level of the pivot
      pf = pf.getPivot().get(0);
      assertPivot("company_t", "compholder0", 1, pf);
      assertRange("p1", 0, 100, 1000, 10, pf.getFacetRanges().get(0));
      assertRange("p2", 0, 200, 1000, 5, pf.getFacetRanges().get(1));

      rfc = pf.getFacetRanges().get(0).getCounts();
      assertEquals("0", rfc.get(0).getValue());
      assertEquals(1, rfc.get(0).getCount());
      assertEquals("100", rfc.get(1).getValue());
      assertEquals(0, rfc.get(1).getCount());

    }
  }

  /**
   * asserts that the actual PivotField matches the expected criteria
   */
  private void assertPivot(String field, Object value, int count, // int numKids,
                           PivotField actual) {
    assertEquals("FIELD: " + actual.toString(), field, actual.getField());
    assertEquals("VALUE: " + actual.toString(), value, actual.getValue());
    assertEquals("COUNT: " + actual.toString(), count, actual.getCount());
    // TODO: add arg && assert on number of kids
    //assertEquals("#KIDS: " + actual.toString(), numKids, actual.getPivot().size());
  }

  /**
   * asserts that the actual RangeFacet matches the expected criteria
   */
  private void assertRange(String name, Object start, Object gap, Object end, int numCount,
                           RangeFacet actual) {
    assertEquals("NAME: " + actual.toString(), name, actual.getName());
    assertEquals("START: " + actual.toString(), start, actual.getStart());
    assertEquals("GAP: " + actual.toString(), gap, actual.getGap());
    assertEquals("END: " + actual.toString(), end, actual.getEnd());
    assertEquals("#COUNT: " + actual.toString(), numCount, actual.getCounts().size());
  }

  private void setupDistributedPivotFacetDocuments() throws Exception{
    
    //Clear docs
    del("*:*");
    commit();

    final int maxDocs = 50;
    final SolrClient zeroShard = clients.get(0);
    final SolrClient oneShard = clients.get(1);
    final SolrClient twoShard = clients.get(2);
    final SolrClient threeShard = clients.get(3); // edge case: never gets any matching docs

    for(Integer i=0;i<maxDocs;i++){//50 entries
      addPivotDoc(zeroShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft polecat bbc","pay_i",2400,"hiredate_dt", "2012-07-01T12:30:00Z","real_b","true");
      addPivotDoc(zeroShard, "id", getDocNum(), "place_s", "medical staffing network holdings, inc.", "company_t", "microsoft polecat bbc","pay_i",2400,"hiredate_dt", "2012-07-01T12:30:00Z");
      
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", "placeholder"+i, "company_t", "compHolder"+i,"pay_i",24*i,"hiredate_dt", "2012-08-01T12:30:00Z");
      
      addPivotDoc(twoShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "bbc honda","pay_i",2400,"hiredate_dt", "2012-09-01T12:30:00Z","real_b","true");
      addPivotDoc(twoShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "compHolder"+i,"pay_i",22*i,"hiredate_dt", "2012-09-01T12:30:00Z","real_b","true");
      addPivotDoc(twoShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "compHolder"+i,"pay_i",21*i,"hiredate_dt", "2012-09-01T12:30:00Z","real_b","true");
      addPivotDoc(twoShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "compHolder"+i,"pay_i",20*i,"hiredate_dt", "2012-09-01T12:30:00Z","real_b","true");
      
      //For the filler content
      //Fifty places with 6 results each
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", i+"placeholder", "company_t", "microsoft polecat bbc","pay_i",2400,"hiredate_dt", "2012-10-01T12:30:00Z","real_b","false");
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", i+"placeholder", "company_t", "microsoft polecat bbc","pay_i",3100,"hiredate_dt", "2012-10-01T12:30:00Z","real_b","false");
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", i+"placeholder", "company_t", "microsoft polecat bbc","pay_i",3400,"hiredate_dt", "2012-10-01T12:30:00Z","real_b","false");
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", i+"placeholder", "company_t", "microsoft polecat bbc","pay_i",5400,"hiredate_dt", "2012-10-01T12:30:00Z","real_b","false");
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", i+"placeholder", "company_t", "microsoft polecat bbc","pay_i",6400,"hiredate_dt", "2012-10-01T12:30:00Z","real_b","false");
      addPivotDoc(oneShard, "id", getDocNum(), "place_s", i+"placeholder", "company_t", "microsoft polecat bbc","pay_i",2000,"hiredate_dt", "2012-10-01T12:30:00Z","real_b","false");

    }

    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft","pay_i",4367,"hiredate_dt", "2012-11-01T12:30:00Z");
    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft bbc","pay_i",8742,"hiredate_dt", "2012-11-01T12:30:00Z");
    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft polecat","pay_i",5824,"hiredate_dt", "2012-11-01T12:30:00Z");
    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft ","pay_i",6539,"hiredate_dt", "2012-11-01T12:30:00Z"); 
    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "medical staffing network holdings, inc.", "company_t", "microsoft ","pay_i",6539,"hiredate_dt", "2012-11-01T12:30:00Z", "special_s", "xxx");
    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "polecat","pay_i",4352,"hiredate_dt", "2012-01-01T12:30:00Z", "special_s", "xxx");
    addPivotDoc(oneShard, "id", getDocNum(), "place_s", "krakaw", "company_t", "polecat","pay_i",4352,"hiredate_dt", "2012-11-01T12:30:00Z", "special_s", SPECIAL); 
    
    addPivotDoc(twoShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft","pay_i",12,"hiredate_dt", "2012-11-01T12:30:00Z", "special_s", SPECIAL);
    addPivotDoc(twoShard, "id", getDocNum(), "place_s", "cardiff", "company_t", "microsoft","pay_i",543,"hiredate_dt", "2012-11-01T12:30:00Z", "special_s", SPECIAL);


    // two really trivial documents, unrelated to the rest of the tests, 
    // for the purpose of demoing the porblem with mincount=0
    addPivotDoc(oneShard, "id", getDocNum(), "top_s", "aaa", "sub_s", "bbb" );
    addPivotDoc(twoShard, "id", getDocNum(), "top_s", "xxx", "sub_s", "yyy" );

      
    commit();

    assertEquals("shard #3 should never have any docs",
                 0, threeShard.query(params("q", "*:*")).getResults().getNumFound());
  }

  /**
   * Builds up a SolrInputDocument using the specified fields, then adds it to the 
   * specified client as well as the control client 
   * @see #indexDoc(org.apache.solr.client.solrj.SolrClient,SolrParams,SolrInputDocument...)
   * @see #sdoc
   */
  private void addPivotDoc(SolrClient client, Object... fields)
    throws IOException, SolrServerException {

    indexDoc(client, params(), sdoc(fields));
  }

  private int docNumber = 0;
  
  public int getDocNum(){
    docNumber++;
    return docNumber;
  }
  
}
