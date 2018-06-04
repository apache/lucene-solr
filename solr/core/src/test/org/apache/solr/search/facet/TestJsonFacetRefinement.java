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
import java.util.List;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
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
      public Object newObject() throws IOException {
        return new SimpleOrderedMap();
      }

      @Override
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
      Object jsonFacet = ObjectBuilder.fromJSON(facet);
      FacetParser parser = new FacetTopParser(req);
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
        "{x: {buckets:[{val:x1, count:5}, {val:x2, count:3}] } }",  // shard0 response
        "{x: {buckets:[{val:x2, count:4}, {val:x3, count:2}] } }",  // shard1 response
        null,              // shard0 expected refinement info
        "=={x:{_l:[x1]}}"  // shard1 expected refinement info
    );

    // same test w/o refinement turned on
    doTestRefine("{x : {type:terms, field:X, limit:2} }",  // the facet request
        "{x: {buckets:[{val:x1, count:5}, {val:x2, count:3}] } }",  // shard0 response
        "{x: {buckets:[{val:x2, count:4}, {val:x3, count:2}] } }",  // shard1 response
        null, // shard0 expected refinement info
        null  // shard1 expected refinement info
    );

    // same test, but nested in query facet
    doTestRefine("{top:{type:query, q:'foo_s:myquery', facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",  // the facet request
        "{top: {x: {buckets:[{val:x1, count:5}, {val:x2, count:3}] } } }",  // shard0 response
        "{top: {x: {buckets:[{val:x2, count:4}, {val:x3, count:2}] } } }",  // shard1 response
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
        "{top: {buckets:[{val:'A', count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}]} } ] } }",
        "{top: {buckets:[{val:'A', count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}]} } ] } }",
        null,
        "=={top: {" +
            "_s:[  ['A' , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // same test, but nested in range facet
    doTestRefine("{top:{type:range, field:R, start:0, end:1, gap:1, facet:{x : {type:terms, field:X, limit:2, refine:true} } } }",
        "{top: {buckets:[{val:0, count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}]} } ] } }",
        "{top: {buckets:[{val:0, count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}]} } ] } }",
        null,
        "=={top: {" +
            "_s:[  [0 , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // for testing partial _p, we need a partial facet within a partial facet
    doTestRefine("{top:{type:terms, field:Afield, refine:true, limit:1, facet:{x : {type:terms, field:X, limit:1, refine:true} } } }",
        "{top: {buckets:[{val:'A', count:2, x:{buckets:[{val:x1, count:5},{val:x2, count:3}]} } ] } }",
        "{top: {buckets:[{val:'B', count:1, x:{buckets:[{val:x2, count:4},{val:x3, count:2}]} } ] } }",
        null,
        "=={top: {" +
            "_p:[  ['A' , {x:{_l:[x1]}} ]  ]" +
            "    }  " +
            "}"
    );

    // test partial _p under a missing bucket
    doTestRefine("{top:{type:terms, field:Afield, refine:true, limit:1, missing:true, facet:{x : {type:terms, field:X, limit:1, refine:true} } } }",
        "{top: {buckets:[], missing:{count:12, x:{buckets:[{val:x2, count:4},{val:x3, count:2}]} }  } }",
        "{top: {buckets:[], missing:{count:10, x:{buckets:[{val:x1, count:5},{val:x4, count:3}]} }  } }",
        "=={top: {" +
            "missing:{x:{_l:[x1]}}" +
            "    }  " +
            "}"
        , null
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
    client.queryDefaults().set("shards", servers.getShards(), "debugQuery", Boolean.toString(random().nextBoolean()));

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
    for (String method : new String[]{"","dvhash","stream","uif","enum","stream","smart"}) {
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
      client.testJQ(params(p, "q", "*:*",
          "json.facet", "{" +
              "r1 : { type:range, field:${num_d} start:-20, end:20, gap:40   , facet:{" +
              "cat0:{${terms} type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true}" +
              "}}" +
              "}"
          )
          , "facets=={ count:8" +
              ", r1:{ buckets:[{val:-20.0,count:8,  cat0:{buckets:[{val:A,count:4}]}  }]   }" +
              "}"
      );

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
      client.testJQ(params(p, "rows", "0", "q", "*:*", "fore", "${xy_s}:X", "back", "${num_d}:[0 TO 100]",
                           "json.facet", "{"
                           + "   cat0:{ ${terms} type:terms, field: ${cat_s}, "
                           + "          sort:'count desc', limit:1, overrequest:0, refine:true, "
                           + "          facet:{ s:'relatedness($fore,$back)'} } }")
                    , "facets=={ count:8, cat0:{ buckets:[ "
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
              "  cat :{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:false, allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:false}  }  }" +
              ", cat2:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true , allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:true }  }  }" +
              ", cat3:{${terms} type:terms, field:${cat_s}, limit:1, overrequest:0, refine:true , allBuckets:true, facet:{  xy:{${terms} type:terms, field:${xy_s}, limit:1, overrequest:0, allBuckets:true, refine:true , facet:{f:'sum(${num_d})'}   }  }  }" +
              "}"
          )
          , "facets=={ count:8" +
              ", cat:{ allBuckets:{count:8}, buckets:[  {val:A, count:3, xy:{buckets:[{count:2, val:X}], allBuckets:{count:3}}}]  }" +
              ",cat2:{ allBuckets:{count:8}, buckets:[  {val:A, count:4, xy:{buckets:[{count:3, val:X}], allBuckets:{count:4}}}]  }" +
              ",cat3:{ allBuckets:{count:8}, buckets:[  {val:A, count:4, xy:{buckets:[{count:3, val:X, f:23.0}], allBuckets:{count:4, f:4.0}}}]  }" +
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

  // Unlike solrconfig.xml this test using solrconfig-tlog.xml should not fail with too-many-exceptions (see TestSolrQueryParser.testManyClauses)
  @Test
  public void testManyClauses() throws Exception {
    String a = "1 a 2 b 3 c 10 d 11 12 "; // 10 terms
    StringBuilder sb = new StringBuilder("id:(");
    for (int i = 0; i < 1024; i++) { // historically, the max number of boolean clauses defaulted to 1024
      sb.append('z').append(i).append(' ');
    }
    sb.append(a);
    sb.append(")");

    String q = sb.toString();

    ignoreException("Too many clauses");
    assertJQ(req("q", q)
        , "/response/numFound==");
  }

}
