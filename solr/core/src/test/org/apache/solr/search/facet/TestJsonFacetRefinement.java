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

package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.TestUtil;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

public class TestJsonFacetRefinement extends SolrTestCaseHS {

  private static SolrInstances servers;  // for distributed testing

  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog.xml", "schema_latest.xml");
  }

  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
    systemClearPropertySolrDisableShardsWhitelist();
  }


  // todo - pull up to test base class?
  public void matchJSON(String json, double delta, String... tests) throws Exception {
    for (String test : tests) {
      if (test == null) {
        assertNull(json);
        continue;
      }
      if (test.length() == 0) continue;

      String err = JSONTestUtil.match(json, test, delta);

      if (err != null) {
        throw new RuntimeException("JSON failed validation. error=" + err +
            "\n expected =" + test +
            "\n got = " + json
        );
      }
    }
  }


  public void match(Object input, double delta, String... tests) throws Exception {
    for (String test : tests) {
      String err = null;
      if (test == null) {
        if (input != null) {
          err = "expected null";
        }
      } else if (input == null) {
        err = "got null";
      } else {
        err = JSONTestUtil.matchObj(input, test, delta);
      }

      if (err != null) {
        throw new RuntimeException("JSON failed validation. error=" + err +
            "\n expected =" + test +
            "\n got = " + input
        );
      }
    }
  }


  /**
   * Use SimpleOrderedMap rather than Map to match responses from shards
   */
  public static Object fromJSON(String json) throws IOException {
    JSONParser parser = new JSONParser(json);
    ObjectBuilder ob = new ObjectBuilder(parser) {
      @Override
      @SuppressWarnings({"rawtypes"})
      public Object newObject() throws IOException {
        return new SimpleOrderedMap();
      }

      @Override
      @SuppressWarnings({"unchecked", "rawtypes"})
      public void addKeyVal(Object map, Object key, Object val) throws IOException {
        ((SimpleOrderedMap) map).add(key.toString(), val);
      }
    };

    return ob.getObject();
  }

  void doTestRefine(String facet, String... responsesAndTests) throws Exception {
    SolrQueryRequest req = req();
    try {
      int nShards = responsesAndTests.length / 2;
      Object jsonFacet = Utils.fromJSONString(facet);
      @SuppressWarnings({"rawtypes"})
      FacetParser parser = new FacetParser.FacetTopParser(req);
      FacetRequest facetRequest = parser.parse(jsonFacet);

      FacetMerger merger = null;
      FacetMerger.Context ctx = new FacetMerger.Context(nShards);
      for (int i = 0; i < nShards; i++) {
        Object response = fromJSON(responsesAndTests[i]);
        if (i == 0) {
          merger = facetRequest.createFacetMerger(response);
        }
        ctx.newShard("s" + i);
        merger.merge(response, ctx);
      }

      for (int i = 0; i < nShards; i++) {
        ctx.setShard("s" + i);
        Object refinement = merger.getRefinement(ctx);
        String tests = responsesAndTests[nShards + i];
        match(refinement, 1e-5, tests);
      }

    } finally {
      req.close();
    }

  }

  @Test
  public void testMerge() throws Exception {
    
    doTestRefine("{x : {type:terms, field:X, limit:2, refine:true} }",  // the facet request
        "{x: {buckets:[{val:x1, count:5}, {val:x2, count:3}], more:true } }",  // shard0 response
        "{x: {buckets:[{val:x2, count:4}, {val:x3, count:2}], more:true } }",  // shard1 response
        null,              // shard0 expected refinement info
        "=={x:{_l:[x1]}}"  // shard1 expected refinement info
    );

    // same test as above, but shard1 indicates it doesn't have any more results, so there shouldn't be any refinement
    doTestRefine("{x : {type:terms, field:X, limit:2, refine:true} }",  // the facet request
        "{x: {buckets:[{val:x1, count:5}, {val:x2, count:3}],more:true } }",  // shard0 response
        "{x: {buckets:[{val:x2, count:4}, {val:x3, count:2}] } }",  // shard1 response
        null,  // shard0 expected refinement info
        null   // shard1 expected refinement info  // without more:true, we should not attempt to get extra bucket
    );
    // same but with processEmpty:true we should check for refinement even if there isn't "more"
    doTestRefine("{x : {type:terms, field:X, limit:2, refine:true, facet: { processEmpty:true } } }",
        "{x: {buckets:[{val:x1, count:5}, {val:x2, count:3}],more:true } }",  // shard0 response
        "{x: {buckets:[{val:x2, count:4}] } }",  // shard1 response -- NO "more"
        null,  // shard0 expected refinement info
        "=={x:{_l:[x1]}}"  // shard1 expected refinement info
    );

    // same test w/o refinement turned on (even though shards say they have more)
    doTestRefine("{x : {type:terms, field:X, limit:2} }",  // the facet request
        "{x: {buckets:[{val:x1, count:5}, {val:x2, count:3}], more:true } }",  // shard0 response
        "{x: {buckets:[{val:x2, count:4}, {val:x3, count:2}], more:true } }",  // shard1 response
        null, // shard0 expected refinement info
        null  // shard1 expected refinement info
    );

    // same test, but nested in query facet
    doTestRefine("{top:{type:query, q:'foo_s:myquery', facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",  // the facet request
        "{top: {x: {buckets:[{val:x1, count:5}, {val:x2, count:3}], more:true } } }",  // shard0 response
        "{top: {x: {buckets:[{val:x2, count:4}, {val:x3, count:2}], more:true } } }",  // shard1 response
        null,              // shard0 expected refinement info
        "=={top:{x:{_l:[x1]}}}"  // shard1 expected refinement info
    );

    // same test w/o refinement turned on
    doTestRefine("{top:{type:query, q:'foo_s:myquery', facet:{x : {type:terms, field:X, limit:2, refine:false} } } }",
        "{top: {x: {buckets:[{val:x1, count:5}, {val:x2, count:3}] } } }",  // shard0 response
        "{top: {x: {buckets:[{val:x2, count:4}, {val:x3, count:2}] } } }",  // shard1 response
        null,
        null
    );

    // same test, but nested in a terms facet
    doTestRefine("{top:{type:terms, field:Afield, facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
        "{top: {buckets:[{val:'A', count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}], more:true} } ] } }",
        "{top: {buckets:[{val:'A', count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}], more:true} } ] } }",
        null,
        "=={top: {" +
            "_s:[  ['A' , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // same test, but nested in range facet
    doTestRefine("{top:{type:range, field:R, start:0, end:1, gap:1, facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
        "{top: {buckets:[{val:0, count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}],more:true} } ] } }",
        "{top: {buckets:[{val:0, count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}],more:true} } ] } }",
        null,
        "=={top: {" +
            "_s:[  [0 , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // same test, but now the range facet includes "other" buckets
    // (so we also verify that the "_actual_end" is echoed back)
    doTestRefine("{top:{type:range, other:all, field:R, start:0, end:1, gap:1, " +
                 "      facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
                 // phase #1
                 "{top: {buckets:[{val:0, count:2, x:{more:true,buckets:[{val:x1, count:5},{val:x2, count:3}]} } ]," +
                 "       before:{count:0},after:{count:0}," +
                 "       between:{count:2,x:{more:true,buckets:[{val:x1, count:5},{val:x2, count:3}]} }," +
                 "       '_actual_end':'does_not_matter_must_be_echoed_back' } }",
                 "{top: {buckets:[{val:0, count:1, x:{more:true,buckets:[{val:x2, count:4},{val:x3, count:2}]} } ]," +
                 "       before:{count:0},after:{count:0}," +
                 "       between:{count:1,x:{more:true,buckets:[{val:x2, count:4},{val:x3, count:2}]} }," +
                 "       '_actual_end':'does_not_matter_must_be_echoed_back' } }",
                 // refinement...
                 null,
                 "=={top: {" +
                 "    _s:[  [0 , {x:{_l:[x1]}} ]  ]," +
                 "    between:{ x:{_l : [x1]} }," +
                 "    '_actual_end':'does_not_matter_must_be_echoed_back'" +
                 "} } ");
    // imagine that all the nodes we query in phase#1 are running "old" versions of solr that
    // don't know they are suppose to compute _actual_end ... our merger should not fail or freak out
    // trust that in the phase#2 refinement request either:
    //  - the processor will re-compute it (if refine request goes to "new" version of solr)
    //  - the processor wouldn't know what to do with an _actual_end sent by the merger anyway
    doTestRefine("{top:{type:range, other:all, field:R, start:0, end:1, gap:1, " +
                 "      facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
                 // phase #1
                 "{top: {buckets:[{val:0, count:2, x:{more:true,buckets:[{val:x1, count:5},{val:x2, count:3}]} } ]," +
                 "       before:{count:0},after:{count:0}," +
                 "       between:{count:2,x:{more:true,buckets:[{val:x1, count:5},{val:x2, count:3}]} }," +
                 "       } }", // no actual_end
                 "{top: {buckets:[{val:0, count:1, x:{more:true,buckets:[{val:x2, count:4},{val:x3, count:2}]} } ]," +
                 "       before:{count:0},after:{count:0}," +
                 "       between:{count:1,x:{more:true,buckets:[{val:x2, count:4},{val:x3, count:2}]} }," +
                 "       } }", // no actual_end
                 // refinement...
                 null,
                 "=={top: {" +
                 "    _s:[  [0 , {x:{_l:[x1]}} ]  ]," +
                 "    between:{ x:{_l : [x1]} }" + 
                 "} } ");
    // a range face w/o any sub facets shouldn't require any refinement
    doTestRefine("{top:{type:range, other:all, field:R, start:0, end:3, gap:2 } }" ,
                 // phase #1
                 "{top: {buckets:[{val:0, count:2}, {val:2, count:2}]," +
                 "       before:{count:3},after:{count:47}," +
                 "       between:{count:5}," +
                 "       } }",
                 "{top: {buckets:[{val:0, count:2}, {val:2, count:19}]," +
                 "       before:{count:22},after:{count:0}," +
                 "       between:{count:21}," +
                 "       } }",
                 // refinement...
                 null,
                 null);

    // same test, but nested in range facet with ranges
    doTestRefine("{top:{type:range, field:R, ranges:[{from:0, to:1}], facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
        "{top: {buckets:[{val:\"[0,1)\", count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}],more:true} } ] } }",
        "{top: {buckets:[{val:\"[0,1)\", count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}],more:true} } ] } }",
        null,
        "=={top: {" +
            "_s:[  [\"[0,1)\" , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    doTestRefine("{top:{type:range, field:R, ranges:[{from:\"*\", to:1}], facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
        "{top: {buckets:[{val:\"[*,1)\", count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}],more:true} } ] } }",
        "{top: {buckets:[{val:\"[*,1)\", count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}],more:true} } ] } }",
        null,
        "=={top: {" +
            "_s:[  [\"[*,1)\" , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // a range facet w/o any sub facets shouldn't require any refinement
    // other and include ignored for ranges
    doTestRefine("{top:{type:range, other:all, field:R, ranges:[{from:0, to:2},{from:2, to:3}] } }",
            // phase #1
            "{top: {buckets:[{val:\"[0,2)\", count:2}, {val:\"[2,3)\", count:2}]," +
            "       } }",
        "{top: {buckets:[{val:\"[0,2)\", count:2}, {val:\"[2,3)\", count:19}]," +
            "       } }",
        // refinement...
        null,
        null);

    // for testing partial _p, we need a partial facet within a partial facet
    doTestRefine("{top:{type:terms, field:Afield, refine:true, limit:1, facet:{x : {type:terms, field:X, limit:1, refine:true} } } }",
        "{top: {buckets:[{val:'A', count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}],more:true} } ],more:true } }",
        "{top: {buckets:[{val:'B', count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}],more:true} } ],more:true } }",
        null,
        "=={top: {" +
            "_p:[  ['A' , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // test partial _p under a missing bucket
    doTestRefine("{top:{type:terms, field:Afield, refine:true, limit:1, missing:true, facet:{x : {type:terms, field:X, limit:1, refine:true} } } }",
        "{top: {buckets:[], missing:{count:12, x:{buckets:[{val:x2, count:4},{val:x3, count:2}],more:true} }  } }",
        "{top: {buckets:[], missing:{count:10, x:{buckets:[{val:x1, count:5},{val:x4, count:3}],more:true} }  } }",
        "=={top: {" +
            "missing:{x:{_l:[x1]}}" +
            "    }  " +
            "}"
        , null
    );

  }

  @Test
  public void testMergeWithOverrefine() throws Exception {
    // overrefine hueristic should use explicit overrequest as default
    doTestRefine("{x : {type:terms, field:X, limit:1, overrequest:1, sort:'count asc', refine:true} }",
                 //
                 "{x: {buckets:[{val:x1, count:3}, {val:x2, count:5}, {val:x9, count:42}, {val:x0, count:42}], more:true } }",
                 "{x: {buckets:[{val:x2, count:2}, {val:x3, count:4}, {val:x7, count:66}, {val:x8, count:66}], more:true } }",
                 //
                 "=={x:{_l:[x3]}}",
                 "=={x:{_l:[x1]}}");
    doTestRefine("{x : {type:terms, field:X, limit:1, overrequest:0, sort:'count asc', refine:true} }",
                 //
                 "{x: {buckets:[{val:x1, count:3}, {val:x2, count:5}, {val:x9, count:42}, {val:x0, count:42}], more:true } }",
                 "{x: {buckets:[{val:x2, count:2}, {val:x3, count:4}, {val:x7, count:66}, {val:x8, count:66}], more:true } }",
                 //
                 null,
                 "=={x:{_l:[x1]}}");
    
    // completely implicit hueristic when no explicit overrequest
    // limit=1 + 10% + 4 =~ 5 total, but x2 is fully populated so only the other 4 "lowest" should be refined
    doTestRefine("{x : {type:terms, field:X, limit:1, sort:'count asc', refine:true} }",
                 //
                 "{x: {buckets:[{val:x1, count:3}, {val:x2, count:5}, {val:x9, count:42}, {val:x0, count:42}], more:true } }",
                 "{x: {buckets:[{val:x2, count:2}, {val:x3, count:4}, {val:x7, count:66}, {val:x8, count:66}], more:true } }",
                 //
                 "=={x:{_l:[x3]}}",
                 "=={x:{_l:[x1,x0,x9]}}");

    // when using (default) mincount (or mincount=0) sort="count desc" should eliminate need for overrefine
    // (regardless of whether any explicit overrequest is specified)
    for (String extra : Arrays.asList("", ", mincount:0", ", mincount:1",
                                      ", overrequest:3", ", overrequest:3, mincount:0")) {
      // w/o any overrefinement, we should only need to backfill x1 & x3 (x2 already fully populated)
      doTestRefine("{x : {type:terms, field:X, limit:3, sort:'count desc', refine:true"+extra+" } }",
                   //
                   "{x: {buckets:[{val:x1, count:29}, {val:x2, count:15}, {val:x9, count:7}, {val:x0, count:7}], more:true } }",
                   "{x: {buckets:[{val:x2, count:20}, {val:x3, count:12}, {val:x7, count:7}, {val:x8, count:7}], more:true } }",
                   //
                   "=={x:{_l:[x3]}}",
                   "=={x:{_l:[x1]}}");
    }

    // with 1<mincount, even sort="count desc" should trigger hueristic overrefinement
    // limit=1 + 10% + 4 =~ 5 total, but x2 is fully populated so only the other 4 "highest" should be refined
    doTestRefine("{x : {type:terms, field:X, limit:1, mincount:5, sort:'count desc', refine:true } }",
                 //
                 "{x: {buckets:[{val:x1, count:29}, {val:x2, count:15}, {val:x9, count:5}, {val:x0, count:3}], more:true } }",
                 "{x: {buckets:[{val:x2, count:20}, {val:x3, count:12}, {val:x7, count:7}, {val:x8, count:4}], more:true } }",
                 //
                 "=={x:{_l:[x3,x7]}}",
                 "=={x:{_l:[x1,x9]}}");
    
    // explicit overrefine
    // limit=1 + overrefine=2 == 3 total, but x2 is fully populated, so only x1 & x3 need refined
    doTestRefine("{x : {type:terms, field:X, limit:1, overrequest:1, overrefine:2, sort:'count asc', refine:true} }",
                 //
                 "{x: {buckets:[{val:x1, count:3}, {val:x2, count:5}, {val:x9, count:42}, {val:x0, count:42}], more:true } }",
                 "{x: {buckets:[{val:x2, count:2}, {val:x3, count:9}, {val:x7, count:66}, {val:x8, count:66}], more:true } }",
                 //
                 "=={x:{_l:[x3]}}",
                 "=={x:{_l:[x1]}}");
    
    // explicit overrefine with 0<offset
    // offset=1 + limit=1 + overrefine=2 == 4 total, but x2 is fully populated, so only x1,x3,x9 need refined
    doTestRefine("{x : {type:terms, field:X, limit:1, offset:1, overrequest:1, overrefine:2, sort:'count asc', refine:true} }",
                 //
                 "{x: {buckets:[{val:x1, count:3}, {val:x2, count:5}, {val:x9, count:42}, {val:x0, count:43}], more:true } }",
                 "{x: {buckets:[{val:x2, count:2}, {val:x3, count:9}, {val:x7, count:66}, {val:x8, count:67}], more:true } }",
                 //
                 "=={x:{_l:[x3]}}",
                 "=={x:{_l:[x1,x9]}}");

    // hueristic refinement of nested facets
    // limit=1 + 10% + 4 =~ 5 total (at each level)
    // -> x2 is fully populated and child buckets are consistent - no refinement needed at all
    // -> x4 has counts from both shards, but child buckets don't align perfectly
    //
    // For (test) simplicity, only x3 and x4 have enough (total) y buckets to prove that the sub-facet
    // overrefine hueristic is finite...
    // -> x3 has 6 total sub-facet buckets, only "lowest 5" should be refined on missing shard
    // -> x4 also has 6 total sub-facet buckets, but only 3 need refined since 2 already fully populated
    doTestRefine("{x:{type:terms, field:X, limit:1, sort:'count asc', refine:true, " +
                 "    facet:{y : {type:terms, field:X, limit:1, sort:'count asc', refine:true} } } }",
                 //
                 "{x: {buckets:[" +
                 "       {val:'x1', count:1, y:{buckets:[{val:y11, count:1},{val:y12, count:3}], more:true} }, "+
                 "       {val:'x2', count:2, y:{buckets:[{val:y21, count:1},{val:y22, count:3}], more:true} }, "+
                 "       {val:'x4', count:3, y:{buckets:[{val:y41, count:1},{val:y4a, count:3},     "+
                 "                                       {val:y42, count:4},{val:y4d, count:5}], more:true} }, "+
                 "       {val:'x5', count:4, y:{buckets:[{val:y51, count:1},{val:y52, count:3}], more:true} }, "+
                 "    ], more:true } }",
                 "{x: {buckets:[" +
                 "       {val:'x3', count:1, y:{buckets:[{val:y31, count:1},{val:y32, count:2},     "+
                 "                                       {val:y33, count:3},{val:y34, count:4}, "+
                 "                                       {val:y35, count:5},{val:y36, count:6}], more:true} }, "+
                 "       {val:'x2', count:2, y:{buckets:[{val:y21, count:1},{val:y22, count:3}], more:true} }, "+
                 "       {val:'x4', count:3, y:{buckets:[{val:y41, count:1},{val:y4b, count:3},     "+
                 "                                       {val:y42, count:4},{val:y4c, count:9}], more:true} }, "+
                 "       {val:'x9', count:9, y:{buckets:[{val:y91, count:1},{val:y92, count:3}], more:true} }, "+
                 "    ], more:true } }",
                 // 
                 "=={x: {" +
                 "        _p:[  ['x3' , {y:{_l:[y31,y32,y33,y34,y35]}} ]  ]," +
                 "        _s:[  ['x4' , {y:{_l:[y4b]}} ]  ]," +
                 "    } }",
                 "=={x: {" +
                 "        _p:[  ['x1' , {y:{_l:[y11,y12]}} ],   " +
                 "              ['x5' , {y:{_l:[y51,y52]}} ]  ]," +
                 "        _s:[  ['x4' , {y:{_l:[y4a,y4d]}} ]  ]," +
                 "    } }");
                 
    
  }

  /** 
   * When <code>prelim_sort</code> is used, all 'top bucket' choices for refinement should still be based on
   * it, not the <code>sort</code> param, so this test is just some sanity checks that the presence of the 
   * these params doesn't break anything in the refine / logic.
   */
  @Test
  public void testRefinementMergingWithPrelimSort() throws Exception {

    doTestRefine("{x : { type:terms, field:X, limit:2, refine:true, prelim_sort:'count desc', sort:'y asc'," +
                 "       facet:{ y:'sum(y_i)' } } }",
                 // shard0 response
                 "{x: {buckets:[{val:x1, count:5, y:73}, {val:x2, count:3, y:13}], more:true } }",
                 // shard1 response
                 "{x: {buckets:[{val:x2, count:4, y:4}, {val:x3, count:2, y:22}], more:true } }",
                 // shard0 expected refinement info
                 null,
                 // shard1 expected refinement info
                 "=={x:{_l:[x1]}}");

    // same test as above, but shard1 indicates it doesn't have any more results,
    // so there shouldn't be any refinement
    doTestRefine("{x : { type:terms, field:X, limit:2, refine:true, prelim_sort:'count desc', sort:'y asc'," +
                 "       facet:{ y:'sum(y_i)' } } }",
                 // shard0 response
                 "{x: {buckets:[{val:x1, count:5, y:73}, {val:x2, count:3, y:13}], more:true } }",
                 // shard1 response
                 "{x: {buckets:[{val:x2, count:4, y:4}, {val:x3, count:2, y:22}] } }",
                 // shard0 expected refinement info
                 null,
                 // shard1 expected refinement info
                 null);
  }

  @Test
  public void testPrelimSortingWithRefinement() throws Exception {
    // NOTE: distributed prelim_sort testing in TestJsonFacets uses identical shards, so never needs
    // refinement, so here we focus on the (re)sorting of different topN refined buckets
    // after the prelim_sorting from diff shards
  
    initServers();
    final Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()));

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3); // we only use 2, but assert 3 to also test empty shard
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);

    client.deleteByQuery("*:*", null);
    int id = 0;

    // client 0 // shard1: A=1,B=1,C=2 ...
    c0.add(sdoc("id", id++, "cat_s","A", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","B", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","C", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","C", "price_i","1"));
    // ... X=3,Y=3
    c0.add(sdoc("id", id++, "cat_s","X", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","X", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","X", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","Y", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","Y", "price_i","1"));
    c0.add(sdoc("id", id++, "cat_s","Y", "price_i","1"));
    
    // client 1 // shard2: X=1,Y=2,Z=2 ...
    c1.add(sdoc("id", id++, "cat_s","X", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","Y", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","Y", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","Z", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","Z", "price_i","1"));
    // ... C=4
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1"));
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1"));
    
    // Whole Collection: A=1,B=1,Z=2,X=4,Y=5,C=6
    client.commit();
    
    // in both cases, neither C nor Z make the cut for the top3 buckets in phase#1 (due to tie breaker), 
    // so they aren't refined -- after refinement the re-sorting re-orders the buckets
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + " cat_1 : { type:terms, field:cat_s, limit:3, overrequest:0"
                         + "           , refine:true, prelim_sort:'count asc', sort:'index desc' }, "
                         + " cat_2 : { type:terms, field:cat_s, limit:3, overrequest:0"
                         + "           , refine:true, prelim_sort:'sum_p asc', sort:'count desc' "
                         + "           , facet: { sum_p: 'sum(price_i)' } }"
                         + "}")
                  , "facets=={ count: "+id+","
                  + "  cat_1:{ buckets:[ "
                  + "            {val:X,count:4}," // index desc
                  + "            {val:B,count:1}," 
                  + "            {val:A,count:1}," 
                  + "  ] },"
                  + "  cat_2:{ buckets:[ "
                  + "            {val:X,count:4,sum_p:4.0}," // count desc
                  + "            {val:A,count:1,sum_p:1.0}," // index order tie break
                  + "            {val:B,count:1,sum_p:1.0},"
                  + "  ] }"
                  + "}"
                  );

    // with some explicit overrefinement=2, we also refine C and Y, giving us those additional
    // (fully populated) buckets to consider during re-sorting...
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + " cat_1 : { type:terms, field:cat_s, limit:3, overrequest:0, overrefine:2"
                         + "           , refine:true, prelim_sort:'count asc', sort:'index desc' }, "
                         + " cat_2 : { type:terms, field:cat_s, limit:3, overrequest:0, overrefine:2"
                         + "           , refine:true, prelim_sort:'sum_p asc', sort:'count desc' "
                         + "           , facet: { sum_p: 'sum(price_i)' } }"
                         + "}")
                  , "facets=={ count: "+id+","
                  + "  cat_1:{ buckets:[ "
                  + "            {val:Y,count:5}," // index desc
                  + "            {val:X,count:4}," 
                  + "            {val:C,count:6}," 
                  + "  ] },"
                  + "  cat_2:{ buckets:[ "
                  + "            {val:C,count:6,sum_p:6.0}," // count desc
                  + "            {val:Y,count:5,sum_p:5.0},"
                  + "            {val:X,count:4,sum_p:4.0},"
                  + "  ] }"
                  + "}"
                  );
  }

  
  @Test
  public void testSortedFacetRefinementPushingNonRefinedBucketBackIntoTopN() throws Exception {
    initServers();
    final Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()));

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3); // we only use 2, but assert 3 to also test empty shard
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);

    client.deleteByQuery("*:*", null);
    int id = 0;

    // all_ss is only used for sub-faceting...
    // every doc will be in all_ss:z_all, (most c1 docs will be in all_ss:some
    // (with index order tie breaker, c1 should return "some" when limit:1
    //  but "z_all" should have a higher count from c0)
    
    // client 0 // shard1: A=1,B=1,C=2 ...
    c0.add(sdoc("id", id++, "cat_s","A", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","B", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","C", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","C", "price_i","1", "all_ss","z_all"));
    // ... X=3,Y=3
    c0.add(sdoc("id", id++, "cat_s","X", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","X", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","X", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","Y", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","Y", "price_i","1", "all_ss","z_all"));
    c0.add(sdoc("id", id++, "cat_s","Y", "price_i","1", "all_ss","z_all"));
    
    // client 1 // shard2: X=1,Y=2,Z=2 ...
    c1.add(sdoc("id", id++, "cat_s","X", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","Y", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","Y", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","Z", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","Z", "price_i","1", "all_ss","z_all","all_ss","some"));
    // ... C=4
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1", "all_ss","z_all","all_ss","some"));
    c1.add(sdoc("id", id++, "cat_s","C", "price_i","1", "all_ss","z_all","all_ss","some"));

    // the amount of overrequest shouldn't matter for demonstrating the issue...
    // it only changes how many C_fillerN & Z_fillerN terms are needed on each shard
    final int overreq = TestUtil.nextInt(random(),0,20);
    
    // for overreq=n: C_n:(x2 on client0 + x4 on client1); Z_n:(x2 on client1)
    for (int i = 0; i < overreq; i++) {
      for (int t = 0; t < 2; t++) {
        c0.add(sdoc("id", id++, "cat_s","C_filler"+i, "price_i","1", "all_ss","z_all"));
        c1.add(sdoc("id", id++, "cat_s","Z_filler"+i, "price_i","1", "all_ss","z_all","all_ss","some"));
      }
      for (int t = 0; t < 4; t++) {
        c1.add(sdoc("id", id++, "cat_s","C_filler"+i, "price_i","1", "all_ss","z_all","all_ss","some"));
        // extra c0 docs that don't contribute to the cat_s facet,...
        // just so "z_all" will win overall on parent facet
        c0.add(sdoc("id", id++, "all_ss","z_all"));
      }
    }

    
    // Whole Collection: A=1,B=1,Z=2,X=4,Y=5,C=6
    client.commit();
    
    // In an ideal world, 'Z:2' would be returned as the 3rd value,
    // but neither C or Z make the topN cut in phase#1, so only A,B,X get refined.
    // After refinement, X's increased values should *NOT* push it out of the (original) topN
    // to let "C" bubble back up into the topN, with incomplete/inaccurate count/stats
    // (NOTE: hueristic for num buckets refined is based on 'overrequest' unless explicit 'overrefine')
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + " cat_count:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                         + "             , refine:true, sort:'count asc' },"
                         + " cat_price:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                         + "             , refine:true, sort:'sum_p asc' "
                         + "             , facet: { sum_p: 'sum(price_i)' } }"
                         + "}")
                  , "facets=={ count: "+id+","
                  + "  cat_count:{ buckets:[ "
                  + "               {val:A,count:1},"
                  + "               {val:B,count:1},"
                  + "               {val:X,count:4},"
                  + "  ] },"
                  + "  cat_price:{ buckets:[ "
                  + "               {val:A,count:1,sum_p:1.0},"
                  + "               {val:B,count:1,sum_p:1.0},"
                  + "               {val:X,count:4,sum_p:4.0},"
                  + "  ] }"
                  + "}"
                  );
    
    // if we do the same query but explicitly request enough overrefinement to get past the filler
    // terms, we should get accurate counts for (C and) Z which should push X out
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + " cat_count:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                         + "             , overrefine:"+((1+overreq)*3)+", refine:true, sort:'count asc' },"
                         + " cat_price:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                         + "             , overrefine:"+((1+overreq)*3)+", refine:true, sort:'sum_p asc' "
                         + "             , facet: { sum_p: 'sum(price_i)' } }"
                         + "}")
                  , "facets=={ count: "+id+","
                  + "  cat_count:{ buckets:[ "
                  + "               {val:A,count:1},"
                  + "               {val:B,count:1},"
                  + "               {val:Z,count:2},"
                  + "  ] },"
                  + "  cat_price:{ buckets:[ "
                  + "               {val:A,count:1,sum_p:1.0},"
                  + "               {val:B,count:1,sum_p:1.0},"
                  + "               {val:Z,count:2,sum_p:2.0},"
                  + "  ] }"
                  + "}"
                  );
    
    // if we use mincount=2, such that A & B get filtered out, then we should have buckets.size() < limit
    // rather then buckets w/inaccurate counts/stats.
    // (explicitly disabling overrefine & overrequest to prevent filler terms)
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + " cat_count:{ type:terms, field:cat_s, limit:3, overrequest: 0, overrefine: 0"
                         + "             , mincount: 2, refine:true, sort:'count asc' },"
                         + " cat_price:{ type:terms, field:cat_s, limit:3, overrequest: 0, overrefine: 0"
                         + "             , mincount: 2, refine:true, sort:'sum_p asc' "
                         + "             , facet: { sum_p: 'sum(price_i)' } }"
                         + "}")
                  , "facets=={ count: "+id+","
                  + "  cat_count:{ buckets:[ "
                  + "               {val:X,count:4},"
                  + "  ] },"
                  + "  cat_price:{ buckets:[ "
                  + "               {val:X,count:4,sum_p:4.0},"
                  + "  ] }"
                  + "}"
                  );

    // When our 'cat_s' facets are nested under an 'all_ss' facet, we should likewise not get
    // any (sub) buckets with incomplete/inaccurate counts
    //
    // NOTE: parent facet limit is 1, testing with various top level overrequest/refine params to see
    // how different refinement code paths of parent effect the child refinement
    for (String top_refine : Arrays.asList("true", "false")) {
      // if our top level facet does *NO* overrequesting, then our shard1 will return "some" as it's
      // (only) top term, which will lose to "z_all" from shard0, and the (single pass) refinement
      // logic will have no choice but to choose & refine the child facet terms from shard0: A,B,C
      client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                           + " all:{ type:terms, field:all_ss, limit:1, refine:"+top_refine
                           +        ", overrequest:0"
                           + "       , facet:{"
                           + "   cat_count:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                           + "               , refine:true, sort:'count asc' },"
                           + "   cat_price:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                           + "               , refine:true, sort:'sum_p asc' "
                           + "               , facet: { sum_p: 'sum(price_i)' } }"
                           + "} } }")
                    , "facets=={ count: "+id+","
                    + "all:{ buckets:[ "
                    + "  { val:z_all, count: "+id+","
                    + "    cat_count:{ buckets:[ "
                    + "                 {val:A,count:1},"
                    + "                 {val:B,count:1},"
                    + "                 {val:C,count:6},"
                    + "    ] },"
                    + "    cat_price:{ buckets:[ "
                    + "                 {val:A,count:1,sum_p:1.0},"
                    + "                 {val:B,count:1,sum_p:1.0},"
                    + "                 {val:C,count:6,sum_p:6.0},"
                    + "    ] }"
                    + "} ] } }"
                    );

      // With any overrequest param > 0 on the parent facet, both shards will return "z_all" as a
      // viable candidate and the merge logic should recoginize that X is a better choice,
      // even though the (single shard) stats for "C" will be lower
      final int top_over = TestUtil.nextInt(random(), 1, 999);
      client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                           + " all:{ type:terms, field:all_ss, limit:1, refine:"+top_refine
                           +        ", overrequest:" + top_over
                           + "       , facet:{"
                           + "   cat_count:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                           + "               , refine:true, sort:'count asc' },"
                           + "   cat_price:{ type:terms, field:cat_s, limit:3, overrequest:"+overreq
                           + "               , refine:true, sort:'sum_p asc' "
                           + "               , facet: { sum_p: 'sum(price_i)' } }"
                           + "} } }")
                    , "facets=={ count: "+id+","
                    + "all:{ buckets:[ "
                    + "  { val:z_all, count: "+id+","
                    + "    cat_count:{ buckets:[ "
                    + "                 {val:A,count:1},"
                    + "                 {val:B,count:1},"
                    + "                 {val:X,count:4},"
                    + "    ] },"
                    + "    cat_price:{ buckets:[ "
                    + "                 {val:A,count:1,sum_p:1.0},"
                    + "                 {val:B,count:1,sum_p:1.0},"
                    + "                 {val:X,count:4,sum_p:4.0},"
                    + "    ] }"
                    + "} ] } }"
                    );

      // if we do the same query but explicitly request enough overrefinement on the child facet
      // to get past the filler terms, we should get accurate counts for (C and) Z which should push X out
      client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                           + " all:{ type:terms, field:all_ss, limit:1, refine:"+top_refine
                           +        ", overrequest:" + top_over
                           + "       , facet:{"
                           + "   cat_count:{ type:terms, field:cat_s, limit:3, overrequest:"+((1+overreq)*3)
                           + "               , refine:true, sort:'count asc' },"
                           + "   cat_price:{ type:terms, field:cat_s, limit:3, overrequest:"+((1+overreq)*3)
                           + "               , refine:true, sort:'sum_p asc' "
                           + "               , facet: { sum_p: 'sum(price_i)' } }"
                           + "} } }")
                    , "facets=={ count: "+id+","
                    + "all:{ buckets:[ "
                    + "  { val:z_all, count: "+id+","
                    + "    cat_count:{ buckets:[ "
                    + "                 {val:A,count:1},"
                    + "                 {val:B,count:1},"
                    + "                 {val:Z,count:2},"
                    + "    ] },"
                    + "    cat_price:{ buckets:[ "
                    + "                 {val:A,count:1,sum_p:1.0},"
                    + "                 {val:B,count:1,sum_p:1.0},"
                    + "                 {val:Z,count:2,sum_p:2.0},"
                    + "    ] }"
                    + "} ] } }"
                    );

    }
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12556")
  @Test
  public void testProcessEmptyRefinement() throws Exception {
    initServers();
    final Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()));

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3); // we only use 2, but assert at least 3 to also test empty shard
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);

    client.deleteByQuery("*:*", null);
    int id = 0;
    
    c0.add(sdoc("id", id++, "cat_s", "Ax"));
    c0.add(sdoc("id", id++, "cat_s", "Bx"));
    c0.add(sdoc("id", id++, "cat_s", "Cx"));
    
    c1.add(sdoc("id", id++, "cat_s", "Ay"));
    c1.add(sdoc("id", id++, "cat_s", "By"));
    c1.add(sdoc("id", id++, "cat_s", "Cy"));
    c1.add(sdoc("id", id++, "cat_s", "Dy"));
    
    client.commit();

    // regardless of how much overrequest there is, in phase#1 
    // all terms will tie on the sort criteria, and "Ax" should win the tiebreaker.
    //
    // When Ax is refined against c1, it's 'debug' sort value will increase, but regardless
    // of the value of processEmpty, no other term should be returned in it's place
    // (because if they are also correctly refined, then their 'debug' sort values will also increase
    // and Ax will stll win the tie breaker -- and if they are not refined they shouldn't be returned)
    for (int overrequest = 0; overrequest < 5; overrequest++) {
      for (boolean pe : Arrays.asList(false, true)) {
        ModifiableSolrParams p
          = params("q", "*:*", "rows", "0", "json.facet"
                   , "{"
                   + " top:{ type:terms, field:cat_s, limit:1, overrequest:"+overrequest+", "
                   + "       refine:true, sort: 'debug asc', "
                   + "       facet:{ debug:'debug(numShards)', processEmpty:"+pe+" } } }");
        try {
          client.testJQ(p
                        , "facets=={ count: "+id+","
                        + "  top:{ buckets:[ "
                        + "    { val:Ax, count: 1, "
                        + "      debug:"+(pe ? 2 : 1)
                        + "      }"
                        + "  ] } }"
                        );
        } catch (AssertionError | RuntimeException e) {
          throw new AssertionError(p + " --> " + e.getMessage(), e);
        }
      }
    }
  }

  /** Helper method used by multiple tests to look at same data diff ways */
  private int initSomeDocsWhere1ShardHasOnlyParentFacetField() throws Exception {
    initServers();
    final Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()));

    final List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 2);
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);

    client.deleteByQuery("*:*", null);
    int id = 0;
    
    // client 0 // shard1
    // only terms pX & pY (with high counts) from the parent_s facet, no child_s values
    for (int i = 0; i < 10; i++) {
      c0.add(sdoc("id", id++, "parent_s", "pX"));
      for (int j =0; j < 2; j++) {
        c0.add(sdoc("id", id++, "parent_s", "pY"));
      }
    }

    // client 1 // shard2
    // some docs with pX & pY, but on this shard, pA & pB have higher counts
    // (but not as high as pX/py on shard1)
    // all docs on this shard also have values in child_s
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3 ; j++) {
        c1.add(sdoc("id", id++, "parent_s", "pA", "child_s", "c"+i));
        c1.add(sdoc("id", id++, "parent_s", "pB", "child_s", "c"+i));
      }
      c1.add(sdoc("id", id++, "parent_s", "pX", "child_s", "c"+i));
      c1.add(sdoc("id", id++, "parent_s", "pY", "child_s", "c"+i));
    }
    c1.add(sdoc("id", id++, "parent_s", "pX", "child_s", "c0"));
    c1.add(sdoc("id", id++, "parent_s", "pY", "child_s", "c1"));
    c1.add(sdoc("id", id++, "parent_s", "pY", "child_s", "c1"));

    client.commit();
    return id;
  }

  /** @see #testSortedSubFacetRefinementWhenParentOnlyReturnedByOneShardProcessEmpty */
  @Test
  public void testSortedSubFacetRefinementWhenParentOnlyReturnedByOneShard() throws Exception {
    final int numDocs = initSomeDocsWhere1ShardHasOnlyParentFacetField();
    final Client client = servers.getClient(random().nextInt());
    final List<SolrClient> clients = client.getClientProvider().all();
    
    assertTrue(clients.size() >= 3); // we only use 2, but assert at least 3 to also test empty shard
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);
    
    // during the initial request...
    // - shard1 should return "high" count pX & pY w/o any child buckets (no "more" child)
    // - shard2 should return "lower" count pA & pB w/some child buckets
    // - any other shards should indicate they have no parent buckets (no "more" parent)
    // during refinement:
    // - shard2 should be asked to backfill any known children of pX&pY
    // - these children from shard2 will be the only (possibly) contributors to the child buckets
    //
    // - the numShards for all parent buckets should be 2, but for the child buckets it should be 1
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + "parent:{ type:terms, field:parent_s, limit:2, overrequest:0, refine:true, facet:{"
                         + "  debug:'debug(numShards)',"
                         + "  child:{ type:terms, field:child_s, limit:2, overrequest:0, refine: true,"
                         + "          facet:{ debug:'debug(numShards)' } }"
                         + "} } }")
                  , "facets=={ count: "+numDocs+","
                  + "  parent:{ buckets:[ "
                  + "    { val:pY, count: 24,"
                  + "      debug:2, "
                  + "      child:{ buckets:[ "
                  + "                   {val:c1,count:3, debug:1},"
                  + "                   {val:c0,count:1, debug:1},"
                  + "      ] } },"
                  + "    { val:pX, count: 13,"
                  + "      debug:2, "
                  + "      child:{ buckets:[ "
                  + "                   {val:c0,count:2, debug:1},"
                  + "                   {val:c1,count:1, debug:1},"
                  + "      ] } },"
                  + "  ] } }"
                  );
  }
  
  /** @see #testSortedSubFacetRefinementWhenParentOnlyReturnedByOneShard */
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12556")
  @Test
  public void testSortedSubFacetRefinementWhenParentOnlyReturnedByOneShardProcessEmpty() throws Exception {
    final int numDocs = initSomeDocsWhere1ShardHasOnlyParentFacetField();
    final Client client = servers.getClient(random().nextInt());
    final List<SolrClient> clients = client.getClientProvider().all();
    final int numClients = clients.size();
    
    assertTrue(numClients >= 3); // we only use 2, but assert at least 3 to also test empty shard
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);
    
    // if we do the same request as testSortedSubFacetRefinementWhenParentOnlyReturnedByOneShard,
    // but with processEmpty:true, then ideally we should get the same buckets & counts as before,
    // but the debug info should indicate that every shard contributed to every bucket (either initially,
    // or during refinement)
    //
    // The problem comes in with how "empty" bucket lists are dealt with...
    // - child debug counts never get higher then '2' because even with the forced "_l" refinement of
    //   the parent buckets against the "empty" shards we don't explicitly ask those shards to
    //   evaluate the child buckets
    // - perhaps we should reconsider the value of "_l" ?
    //   - why aren't we just specifying all the buckets (and child buckets) chosen in phase#1 using "_p" ?
    //   - or at the very least, if the purpose of "_l" is to give other buckets a chance to "bubble up"
    //     in phase#2, then shouldn't a "_l" refinement requests still include the buckets choosen in
    //     phase#1, and request that the shard fill them in in addition to returning its own top buckets?
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                         + "processEmpty:true,"
                         + "parent:{ type:terms, field:parent_s, limit:2, overrequest:0, refine:true, facet:{"
                         + "  processEmpty:true,"
                         + "  debug:'debug(numShards)',"
                         + "  child:{ type:terms, field:child_s, limit:2, overrequest:0, refine: true,"
                         + "          facet:{ processEmpty:true, debug:'debug(numShards)' } }"
                         + "} } }")
                  , "facets=={ count: "+numDocs+","
                  + "  parent:{ buckets:[ "
                  + "    { val:pY, count: 24,"
                  + "      debug:"+numClients+", "
                  + "      child:{ buckets:[ "
                  + "                   {val:c1,count:3, debug:"+numClients+"},"
                  + "                   {val:c0,count:1, debug:"+numClients+"},"
                  + "      ] } },"
                  + "    { val:pX, count: 13,"
                  + "      debug:"+numClients+", "
                  + "      child:{ buckets:[ "
                  + "                   {val:c0,count:2, debug:"+numClients+"},"
                  + "                   {val:c1,count:1, debug:"+numClients+"},"
                  + "      ] } },"
                  + "  ] } }"
                  );
  }

  
  @Test
  public void testBasicRefinement() throws Exception {
    ModifiableSolrParams p;
    p = params("cat_s", "cat_s", "cat_i", "cat_i", "date","cat_dt", "xy_s", "xy_s", "num_d", "num_d", "qw_s", "qw_s", "er_s", "er_s");
    doBasicRefinement(p);

    // multi-valued (except num_d)
    p = params("cat_s", "cat_ss", "cat_i", "cat_is", "date","cat_dts", "xy_s", "xy_ss", "num_d", "num_d", "qw_s", "qw_ss", "er_s", "er_ss");
    doBasicRefinement(p);

    // single valued docvalues
    p = params("cat_s", "cat_sd", "cat_i", "cat_id", "date","cat_dtd", "xy_s", "xy_sd", "num_d", "num_dd", "qw_s", "qw_sd", "er_s", "er_sd");
    doBasicRefinement(p);

    // multi valued docvalues (except num_d)
    p = params("cat_s", "cat_sds", "cat_i", "cat_ids", "date","cat_dtds", "xy_s", "xy_sds", "num_d", "num_dd", "qw_s", "qw_sds", "er_s", "er_sds");
    doBasicRefinement(p);
  }

  public void doBasicRefinement(ModifiableSolrParams p) throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()));
    
    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3);
    
    client.deleteByQuery("*:*", null);

    String cat_s = p.get("cat_s");
    String cat_i = p.get("cat_i"); // just like cat_s, but a number
    String xy_s = p.get("xy_s");
    String qw_s = p.get("qw_s");
    String er_s = p.get("er_s");  // this field is designed to test numBuckets refinement... the first phase will only have a single bucket returned for the top count bucket of cat_s
    String num_d = p.get("num_d");
    String date = p.get("date");

    clients.get(0).add(sdoc("id", "01", "all_s", "all", cat_s, "A", cat_i, 1, date, "2001-01-01T01:01:01Z", xy_s, "X", num_d, -1, qw_s, "Q", er_s, "E")); // A wins count tie
    clients.get(0).add(sdoc("id", "02", "all_s", "all", cat_s, "B", cat_i, 2, date, "2002-02-02T02:02:02Z", xy_s, "Y", num_d, 3));

    clients.get(1).add(sdoc("id", "11", "all_s", "all", cat_s, "B", cat_i, 2, date, "2002-02-02T02:02:02Z", xy_s, "X", num_d, -5, er_s, "E")); // B highest count
    clients.get(1).add(sdoc("id", "12", "all_s", "all", cat_s, "B", cat_i, 2, date, "2002-02-02T02:02:02Z", xy_s, "Y", num_d, -11, qw_s, "W"));
    clients.get(1).add(sdoc("id", "13", "all_s", "all", cat_s, "A", cat_i, 1, date, "2001-01-01T01:01:01Z", xy_s, "X", num_d, 7, er_s, "R"));       // "R" will only be picked up via refinement when parent facet is cat_s

    clients.get(2).add(sdoc("id", "21", "all_s", "all", cat_s, "A", cat_i, 1, date, "2001-01-01T01:01:01Z", xy_s, "X", num_d, 17, qw_s, "W", er_s, "E")); // A highest count
    clients.get(2).add(sdoc("id", "22", "all_s", "all", cat_s, "A", cat_i, 1, date, "2001-01-01T01:01:01Z", xy_s, "Y", num_d, -19));
    clients.get(2).add(sdoc("id", "23", "all_s", "all", cat_s, "B", cat_i, 2, date, "2002-02-02T02:02:02Z", xy_s, "X", num_d, 11));

    client.commit();

    // Shard responses should be A=1, B=2, A=2, merged should be "A=3, B=2"
    // One shard will have _facet_={"refine":{"cat0":{"_l":["A"]}}} on the second phase

    /****
     // fake a refinement request... good for development/debugging
     assertJQ(clients.get(1),
     params(p, "q", "*:*",     "_facet_","{refine:{cat0:{_l:[A]}}}", "isShard","true", "distrib","false", "shards.purpose","2097216", "ids","11,12,13",
     "json.facet", "{" +
     "cat0:{type:terms, field:cat_s, sort:'count desc', limit:1, overrequest:0, refine:true}" +
     "}"
     )
     , "facets=={foo:555}"
     );
     ****/
    for (String method : new String[]{"","dv", "dvhash","stream","uif","enum","stream","smart"}) {
      if (method.equals("")) {
        p.remove("terms");
      } else {
        p.set("terms", "method:" + method+", ");
      }
      

      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "cat0:{${terms} type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:false}" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat0:{ buckets:[ {val:A,count:3} ] }" +  // w/o overrequest and refinement, count is lower than it should be (we don't see the A from the middle shard)
              "}"
      );

      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "cat0:{${terms} type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true}" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat0:{ buckets:[ {val:A,count:4} ] }" +  // w/o overrequest, we need refining to get the correct count.
              "}"
      );

      // same as above, but with an integer field instead of a string
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "cat0:{${terms} type:terms, field:${cat_i}, sort:'count desc', limit:1, overrequest:0, refine:true}" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat0:{ buckets:[ {val:1,count:4} ] }" +  // w/o overrequest, we need refining to get the correct count.
              "}"
      );

      // same as above, but with a date field
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "cat0:{${terms} type:terms, field:${date}, sort:'count desc', limit:1, overrequest:0, refine:true}" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat0:{ buckets:[ {val:'2001-01-01T01:01:01Z',count:4} ] }" +  // w/o overrequest, we need refining to get the correct count.
              "}"
      );

      // basic refining test through/under a query facet
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "q1 : { type:query, q:'*:*', facet:{" +
              "cat0:{${terms} type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true}" +
              "}}" +
              "}"
          )
          , "facets=={ count:8" +
              ", q1:{ count:8, cat0:{ buckets:[ {val:A,count:4} ] }   }" +
              "}"
      );

      // basic refining test through/under a range facet
      for (String end : Arrays.asList(// all of these end+hardened options should produce the same buckets
                                      "end:20, hardend:true", // evenly divisible so shouldn't matter
                                      "end:20, hardend:false", "end:20", // defaults to hardened:false
                                      "end:5, hardend:false", "end:5")) {
        // since the gap divides the start/end divide eveningly, 
        // all of these hardend params should we should produce identical results
        String sub = "cat0:{${terms} type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true}";

        // single bucket, all 'other' buckets
        client.testJQ(params(p, "q", "*:*", "json.facet"
                             , "{ r1 : { type:range, field:${num_d} other:all, start:-20, gap:40, " + end
                             + "         , facet:{" + sub + "}}}")
                      , "facets=={ count:8"
                      + ", r1:{ buckets:[{val:-20.0,count:8,  cat0:{buckets:[{val:A,count:4}]}  }],"
                      + "       before:{count:0}, after:{count:0}"
                      + "       between:{count:8, cat0:{buckets:[{val:A,count:4}]}}"
                      + "}}");
        // multiple buckets, only one 'other' buckets
        client.testJQ(params(p, "q", "*:*", "json.facet"
                             , "{ r1 : { type:range, field:${num_d} other:between, start:-20, gap:20, " + end
                             + "         , facet:{" + sub + "}}}")
                      , "facets=={ count:8"
                      // NOTE: in both buckets A & B are tied, but index order should break tie
                      + ", r1:{ buckets:[{val:-20.0, count:4,  cat0:{buckets:[{val:A,count:2}]} },"
                      + "                {val:  0.0, count:4,  cat0:{buckets:[{val:A,count:2}]} } ],"
                      + "       between:{count:8, cat0:{buckets:[{val:A,count:4}]}}"
                      + "}}");
      }

      // test that basic stats work for refinement
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "cat0:{${terms} type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true, facet:{ stat1:'sum(${num_d})'}   }" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat0:{ buckets:[ {val:A,count:4, stat1:4.0} ] }" +
              "}"
      );

      // test sorting buckets by a different stat
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              " cat0:{${terms} type:terms, field:${cat_s}, sort:'min1 asc', limit:1, overrequest:0, refine:false, facet:{ min1:'min(${num_d})'}   }" +
              ",cat1:{${terms} type:terms, field:${cat_s}, sort:'min1 asc', limit:1, overrequest:0, refine:true,  facet:{ min1:'min(${num_d})'}   }" +
              ",qfacet:{type:query, q:'*:*', facet:{  cat2:{${terms} type:terms, field:${cat_s}, sort:'min1 asc', limit:1, overrequest:0, refine:true,  facet:{ min1:'min(${num_d})'}   }  }}" +  // refinement needed through a query facet
              ",allf:{${terms} type:terms, field:all_s,  facet:{  cat3:{${terms} type:terms, field:${cat_s}, sort:'min1 asc', limit:1, overrequest:0, refine:true,  facet:{ min1:'min(${num_d})'}   }  }}" +  // refinement needed through field facet
              ",sum1:'sum(${num_d})'" +  // make sure that root bucket stats aren't affected by refinement
              "}"
          )
          , "facets=={ count:8" +
              ", cat0:{ buckets:[ {val:A,count:3, min1:-19.0} ] }" +  // B wins in shard2, so we're missing the "A" count for that shard w/o refinement.
              ", cat1:{ buckets:[ {val:A,count:4, min1:-19.0} ] }" +  // with refinement, we get the right count
              ", qfacet:{ count:8,  cat2:{ buckets:[ {val:A,count:4, min1:-19.0} ] }    }" +  // just like the previous response, just nested under a query facet
              ", allf:{ buckets:[  {cat3:{ buckets:[ {val:A,count:4, min1:-19.0} ] }  ,count:8,val:all   }]  }" +  // just like the previous response, just nested under a field facet
              ", sum1:2.0" +
              "}"
      );

      // test that SKG stat reflects merged refinement
      // results shouldn't care if we use the short or long syntax, or if we have a low min_pop
      for (String s : Arrays.asList("'relatedness($fore,$back)'",
                                    "{ type:func, func:'relatedness($fore,$back)' }",
                                    "{ type:func, func:'relatedness($fore,$back)', min_popularity:0.2 }")) {
        client.testJQ(params(p, "rows", "0", "q", "*:*", "fore", "${xy_s}:X", "back", "${num_d}:[0 TO 100]",
                             "json.facet", "{"
                             + "   cat0:{ ${terms} type:terms, field: ${cat_s}, allBuckets:true, "
                             + "          sort:'count desc', limit:1, overrequest:0, refine:true, "
                             + "          facet:{ s:"+s+"} } }")
                      , "facets=={ count:8, cat0:{ "
                      // 's' key must not exist in the allBuckets bucket
                      + "   allBuckets: { count:8 }"
                      + "   buckets:[ "
                      + "   { val:A, count:4, "
                      + "     s : { relatedness: 0.00496, "
                      //+ "           foreground_count: 3, "
                      //+ "           foreground_size: 5, "
                      //+ "           background_count: 2, "
                      //+ "           background_size: 4, "
                      + "           foreground_popularity: 0.75, "
                      + "           background_popularity: 0.5, "
                      + "         } } ] }" +
                      "}"
                      );
      }
      // same query with a high min_pop should result in a -Infinity relatedness score
      client.testJQ(params(p, "rows", "0", "q", "*:*", "fore", "${xy_s}:X", "back", "${num_d}:[0 TO 100]",
                           "json.facet", "{"
                           + "   cat0:{ ${terms} type:terms, field: ${cat_s},  allBuckets:true,"
                           + "          sort:'count desc', limit:1, overrequest:0, refine:true, "
                           + "          facet:{ s:{ type:func, func:'relatedness($fore,$back)', "
                           + "                      min_popularity:0.6 } } } }")
                    , "facets=={ count:8, cat0:{ "
                    // 's' key must not exist in the allBuckets bucket
                    + "   allBuckets: { count:8 }"
                    + "   buckets:[ "
                    + "   { val:A, count:4, "
                    + "     s : { relatedness: '-Infinity', "
                    //+ "           foreground_count: 3, "
                    //+ "           foreground_size: 5, "
                    //+ "           background_count: 2, "
                    //+ "           background_size: 4, "
                    + "           foreground_popularity: 0.75, "
                    + "           background_popularity: 0.5, "
                    + "         } } ] }" +
                    "}"
                    );

      // really special case: allBuckets when there are no regular buckets...
      for (String refine : Arrays.asList("", "refine: true,", "refine:false,")) {
        client.testJQ(params(p, "rows", "0", "q", "*:*", "fore", "${xy_s}:X", "back", "${num_d}:[0 TO 100]",
                             "json.facet", "{"
                             + "   cat0:{ ${terms} type:terms, field: bogus_field_s, allBuckets:true, "
                             + refine
                             + "          facet:{ s:{ type:func, func:'relatedness($fore,$back)' } } } }")
                      , "facets=={ count:8, cat0:{ "
                      // 's' key must not exist in the allBuckets bucket
                      + "    allBuckets: { count:0 }"
                      + "    buckets:[ ]"
                      + "} }"
                      );
      }


      // SKG under nested facet where some terms only exist on one shard
      { 
        // sub-bucket order should change as sort direction changes
        final String jsonFacet = ""
          + "{ processEmpty:true, "
          + " cat0:{ ${terms} type:terms, field: ${cat_s}, "
          + "        sort:'count desc', limit:1, overrequest:0, refine:true, "
          + "        facet:{ processEmpty:true, "
          + "                qw1: { ${terms} type:terms, field: ${qw_s}, mincount:0, "
          + "                       sort:'${skg_sort}', limit:100, overrequest:0, refine:true, "
          + "                       facet:{ processEmpty:true, skg:'relatedness($fore,$back)' } } } } }";
        final String bucketQ = ""
          + "             { val:Q, count:1, "
          + "               skg : { relatedness: 1.0, "
          + "                       foreground_popularity: 0.25, "
          + "                       background_popularity: 0.0, "
          // + "                       foreground_count: 1, "
          // + "                       foreground_size: 3, "
          // + "                       background_count: 0, "
          // + "                       background_size: 4, "
          + "               } },";
        final String bucketW = ""
          + "             { val:W, count:1, "
          + "               skg : { relatedness: 0.0037, "
          + "                       foreground_popularity: 0.25, "
          + "                       background_popularity: 0.25, "
          // + "                       foreground_count: 1, "
          // + "                       foreground_size: 3, "
          // + "                       background_count: 1, "
          // + "                       background_size: 4, "
          + "               } },";
        
        client.testJQ(params(p, "rows", "0", "q", "*:*", "fore", "${xy_s}:X", "back", "${num_d}:[0 TO 100]",
                             "skg_sort", "skg desc", "json.facet", jsonFacet)
                      , "facets=={ count:8, cat0:{ buckets:[ "
                      + "   { val:A, count:4, "
                      + "     qw1 : { buckets:["
                      + bucketQ
                      + bucketW
                      + "  ] } } ] } }");
        client.testJQ(params(p, "rows", "0", "q", "*:*", "fore", "${xy_s}:X", "back", "${num_d}:[0 TO 100]",
                             "skg_sort", "skg asc", "json.facet", jsonFacet)
                      , "facets=={ count:8, cat0:{ buckets:[ "
                      + "   { val:A, count:4, "
                      + "     qw1 : { buckets:["
                      + bucketW
                      + bucketQ
                      + "  ] } } ] } }");
      }
    
      // test partial buckets (field facet within field facet)
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              " ab:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true,  facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, refine:true   }  }}" +
              ",cd:{${terms} type:terms, field:${cat_i}, limit:1, overrequest:0, refine:true,  facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, refine:true   }  }}" +
              ",ef:{${terms} type:terms, field:${date},  limit:1, overrequest:0, refine:true,  facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, refine:true   }  }}" +
              "}"
          )
          , "facets=={ count:8" +
              ", ab:{ buckets:[  {val:A, count:4, xy:{buckets:[ {val:X,count:3}]}  }]  }" +  // just like the previous response, just nested under a field facet
              ", cd:{ buckets:[  {val:1, count:4, xy:{buckets:[ {val:X,count:3}]}  }]  }" +  // just like the previous response, just nested under a field facet (int type)
              ", ef:{ buckets:[  {val:'2001-01-01T01:01:01Z', count:4, xy:{buckets:[ {val:X,count:3}]}  }]  }" +  // just like the previous response, just nested under a field facet (date type)
              "}"
      );

      // test that sibling facets and stats are included for _p buckets, but skipped for _s buckets
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              " ab :{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true,  facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, refine:true}, qq:{query:'*:*'},ww:'sum(${num_d})'  }}" +
              ",ab2:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:false, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, refine:true}, qq:{query:'*:*'},ww:'sum(${num_d})'  }}" + // top level refine=false shouldn't matter
              ",allf :{${terms} type:terms, field:all_s, limit:1, overrequest:0, refine:true,  facet:{cat:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true}, qq:{query:'*:*'},ww:'sum(${num_d})'  }}" +
              ",allf2:{${terms} type:terms, field:all_s, limit:1, overrequest:0, refine:false, facet:{cat:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true}, qq:{query:'*:*'},ww:'sum(${num_d})'  }}" + // top level refine=false shouldn't matter
              "}"
          )
          , "facets=={ count:8" +
              ", ab:{ buckets:[  {val:A, count:4, xy:{buckets:[ {val:X,count:3}]}    ,qq:{count:4}, ww:4.0 }]  }" +  // make sure qq and ww are included for _p buckets
              ", allf:{ buckets:[ {count:8, val:all, cat:{buckets:[{val:A,count:4}]} ,qq:{count:8}, ww:2.0 }]  }" +  // make sure qq and ww are excluded (not calculated again in another phase) for _s buckets
              ", ab2:{ buckets:[  {val:A, count:4, xy:{buckets:[ {val:X,count:3}]}    ,qq:{count:4}, ww:4.0 }]  }" +  // make sure qq and ww are included for _p buckets
              ", allf2:{ buckets:[ {count:8, val:all, cat:{buckets:[{val:A,count:4}]} ,qq:{count:8}, ww:2.0 }]  }" +  // make sure qq and ww are excluded (not calculated again in another phase) for _s buckets
              "}"
      );

      // test refining under the special "missing" bucket of a field facet
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "f:{${terms} type:terms, field:missing_s, limit:1, overrequest:0, missing:true, refine:true,  facet:{  cat:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true   }  }}" +
              "}"
          )
          , "facets=={ count:8" +
              ", f:{ buckets:[], missing:{count:8, cat:{buckets:[{val:A,count:4}]}  }  }" +  // just like the previous response, just nested under a field facet
              "}"
      );

      // test filling in "missing" bucket for partially refined facets
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              // test all values missing in sub-facet
              " ab :{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:false,  facet:{  zz:{${terms} type:terms, field:missing_s, limit:1, overrequest:0, refine:false, missing:true}  }}" +
              ",ab2:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true ,  facet:{  zz:{${terms} type:terms, field:missing_s, limit:1, overrequest:0, refine:true , missing:true}  }}" +
              // test some values missing in sub-facet (and test that this works with normal partial bucket refinement)
              ", cd :{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:false,  facet:{  qw:{${terms} type:terms, field:${qw_s}, limit:1, overrequest:0, refine:false, missing:true,   facet:{qq:{query:'*:*'}}   }  }}" +
              ", cd2:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true ,  facet:{  qw:{${terms} type:terms, field:${qw_s}, limit:1, overrequest:0, refine:true , missing:true,   facet:{qq:{query:'*:*'}}   }  }}" +

              "}"
          )
          , "facets=={ count:8" +
              ", ab:{ buckets:[  {val:A, count:3, zz:{buckets:[], missing:{count:3}}}]  }" +
              ",ab2:{ buckets:[  {val:A, count:4, zz:{buckets:[], missing:{count:4}}}]  }" +
              ", cd:{ buckets:[  {val:A, count:3,  qw:{buckets:[{val:Q, count:1, qq:{count:1}}], missing:{count:1,qq:{count:1}}}}]  }" +
              ",cd2:{ buckets:[  {val:A, count:4,  qw:{buckets:[{val:Q, count:1, qq:{count:1}}], missing:{count:2,qq:{count:2}}}}]  }" +
              "}"
      );

      // test filling in missing "allBuckets"
      client.testJQ(params(p, "q", "*:*", 
          "json.facet", "{" +
              "  cat0:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:false, allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:false}  }  }" +
              ", cat1:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true, allBuckets:true, sort:'min asc', facet:{  min:'min(${num_d})' }  }" +
              ", cat2:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true , allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:true }  }  }" +
              ", cat3:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true , allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:true , facet:{sum:'sum(${num_d})'}   }  }  }" +
              ", cat4:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true , allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:true , sort:'sum asc', facet:{sum:'sum(${num_d})'}   }  }  }" +
              // using overrefine only so we aren't fooled by 'local maximum' and ask all shards for 'B'
              ", cat5:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true, overrefine:2, allBuckets:true,  sort:'min desc' facet:{  min:'min(${num_d})', xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:true, facet:{sum:'sum(${num_d})'}   }  }  }" +
              "}"
          )
          , "facets=={ count:8" +
              ",cat0:{ allBuckets:{count:8}, buckets:[  {val:A, count:3, xy:{buckets:[{count:2, val:X}], allBuckets:{count:3}}}]  }" +
              ",cat1:{ allBuckets:{count:8, min:-19.0 }, buckets:[  {val:A, count:4, min:-19.0 }]  }" +
              ",cat2:{ allBuckets:{count:8}, buckets:[  {val:A, count:4, xy:{buckets:[{count:3, val:X}], allBuckets:{count:4}}}]  }" +
              ",cat3:{ allBuckets:{count:8}, buckets:[  {val:A, count:4, xy:{buckets:[{count:3, val:X, sum:23.0}], allBuckets:{count:4, sum:4.0}}}]  }" +
              ",cat4:{ allBuckets:{count:8}, buckets:[  {val:A, count:4, xy:{buckets:[{count:1, val:Y, sum:-19.0}], allBuckets:{count:4, sum:4.0}}}]  }" +
              ",cat5:{ allBuckets:{count:8, min:-19.0 }, buckets:[  {val:B, count:4, min:-11.0, xy:{buckets:[{count:2, val:X, sum:6.0}], allBuckets:{count:4, sum:-2.0}}}]  }" +
              "}"
      );

      // test filling in missing numBuckets
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "  cat :{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:false, numBuckets:true, facet:{  er:{${terms} type:terms, field:${er_s}, limit:1, overrequest:0, numBuckets:true, refine:false}  }  }" +
              ", cat2:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true , numBuckets:true, facet:{  er:{${terms} type:terms, field:${er_s}, limit:1, overrequest:0, numBuckets:true, refine:true }  }  }" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat:{ numBuckets:2, buckets:[  {val:A, count:3, er:{numBuckets:1,buckets:[{count:2, val:E}]  }}]  }" +  // the "R" bucket will not be seen w/o refinement
              ",cat2:{ numBuckets:2, buckets:[  {val:A, count:4, er:{numBuckets:2,buckets:[{count:2, val:E}]  }}]  }" +
              "}"
      );

      final String sort_limit_over = "sort:'count desc', limit:1, overrequest:0, ";
      // simplistic join domain testing: no refinement == low count
      client.testJQ(params(p, "q", "${xy_s}:Y", // query only matches one doc per shard
          "json.facet", "{" +
              "  cat0:{${terms} type:terms, field:${cat_s}, " + sort_limit_over + " refine:false," +
              // self join on all_s ensures every doc on every shard included in facets
              "        domain: { join: { from:all_s, to:all_s } } }" +
              "}"
          )
          ,
          "/response/numFound==3",
          "facets=={ count:3, " +
              // w/o overrequest and refinement, count for 'A' is lower than it should be
              // (we don't see the A from the middle shard)
              "          cat0:{ buckets:[ {val:A,count:3} ] } }");
      // simplistic join domain testing: refinement == correct count
      client.testJQ(params(p, "q", "${xy_s}:Y", // query only matches one doc per shard
          "json.facet", "{" +
              "  cat0:{${terms} type:terms, field:${cat_s}, " + sort_limit_over + " refine:true," +
              // self join on all_s ensures every doc on every shard included in facets
              "        domain: { join: { from:all_s, to:all_s } } }" +
              "}"
          )
          ,
          "/response/numFound==3",
          "facets=={ count:3," +
              // w/o overrequest, we need refining to get the correct count for 'A'.
              "          cat0:{ buckets:[ {val:A,count:4} ] } }");

      // contrived join domain + refinement (at second level) + testing
      client.testJQ(params(p, "q", "${xy_s}:Y", // query only matches one doc per shard
          "json.facet", "{" +
              // top level facet has a single term
              "  all:{${terms} type:terms, field:all_s, " + sort_limit_over + " refine:true, " +
              "       facet:{  " +
              // subfacet will facet on cat after joining on all (so all docs should be included in subfacet)
              "         cat0:{${terms} type:terms, field:${cat_s}, " + sort_limit_over + " refine:true," +
              "               domain: { join: { from:all_s, to:all_s } } } } }" +
              "}"
          )
          ,
          "/response/numFound==3",
          "facets=={ count:3," +
              // all 3 docs matching base query have same 'all' value in top facet
              "          all:{ buckets:[ { val:all, count:3, " +
              // sub facet has refinement, so count for 'A' should be correct
              "                            cat0:{ buckets: [{val:A,count:4}] } } ] } }");

    } // end method loop
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-14595")
  public void testIndexAscRefineConsistency() throws Exception {
    initServers();
    final Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()));

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3);
    final SolrClient c0 = clients.get(0);
    final SolrClient c1 = clients.get(1);
    final SolrClient c2 = clients.get(2);

    client.deleteByQuery("*:*", null);
    int id = 0;
    
    c0.add(sdoc("id", id++, "cat_s", "Z", "price_i", 10));
    
    c1.add(sdoc("id", id++, "cat_s", "Z", "price_i", -5000));
    c1.add(sdoc("id", id++, "cat_s", "X", "price_i", 2,       "child_s", "A" ));
    
    c2.add(sdoc("id", id++, "cat_s", "X", "price_i", 2,       "child_s", "B" ));
    c2.add(sdoc("id", id++, "cat_s", "X", "price_i", 2,       "child_s", "C" ));
    
    client.commit();

    // TODO once SOLR-14595 is fixed, modify test to check full EnumSet, not just these two...
    for (String m : Arrays.asList("smart", "enum")) {
      client.testJQ(params("q", "*:*", "rows", "0", "json.facet", "{"
                           + " cat : { type:terms, field:cat_s, limit:1, refine:true,"
                           + "         overrequest:0, " // to trigger parent refinement given small data set
                           + "         sort:'sum desc', "
                           + "         facet: { sum : 'sum(price_i)', "
                           + "                  child_"+m+" : { "
                           + "                     type:terms, field:child_s, limit:1, refine:true,"
                           + "                     sort:'index asc', method:" + m + " } "
                           + "       }} }"
                           )
                    , "facets=={ count:5"
                    + ", cat:{buckets:[ { val:X, count:3, sum:6.0, "
                    + "                   child_"+m+":{buckets:[{val:A, count:1}]}}]}}"
                    );
    }
  }
}
