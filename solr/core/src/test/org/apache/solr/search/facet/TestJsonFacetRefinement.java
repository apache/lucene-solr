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
  private static int origTableSize;

  @BeforeClass
  public static void beforeTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog.xml","schema_latest.xml");
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
      if (test.length()==0) continue;

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


  /** Use SimpleOrderedMap rather than Map to match responses from shards */
  public static Object fromJSON(String json) throws IOException {
    JSONParser parser = new JSONParser(json);
    ObjectBuilder ob = new ObjectBuilder(parser) {
      @Override
      public Object newObject() throws IOException {
        return new SimpleOrderedMap();
      }

      @Override
      public void addKeyVal(Object map, Object key, Object val) throws IOException {
        ((SimpleOrderedMap)map).add(key.toString(), val);
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
      for (int i=0; i<nShards; i++) {
        Object response = fromJSON(responsesAndTests[i]);
        if (i==0) {
          merger = facetRequest.createFacetMerger(response);
        }
        ctx.newShard("s"+i);
        merger.merge(response, ctx);
      }

      for (int i=0; i<nShards; i++) {
        ctx.setShard("s"+i);
        Object refinement = merger.getRefinement(ctx);
        String tests = responsesAndTests[nShards+i];
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

  }


  @Test
  public void testBasicRefinement() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards(), "debugQuery", Boolean.toString(random().nextBoolean()) );

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3);

    client.deleteByQuery("*:*", null);

    ModifiableSolrParams p = params("cat_s", "cat_s", "num_d", "num_d");
    String cat_s = p.get("cat_s");
    String num_d = p.get("num_d");

    clients.get(0).add( sdoc("id", "01", cat_s, "A", num_d, -1) ); // A wins count tie
    clients.get(0).add( sdoc("id", "02", cat_s, "B", num_d, 3) );

    clients.get(1).add( sdoc("id", "11", cat_s, "B", num_d, -5) ); // B highest count
    clients.get(1).add( sdoc("id", "12", cat_s, "B", num_d, -11) );
    clients.get(1).add( sdoc("id", "13", cat_s, "A", num_d, 7) );

    clients.get(2).add( sdoc("id", "21", cat_s, "A", num_d, 17) ); // A highest count
    clients.get(2).add( sdoc("id", "22", cat_s, "A", num_d, -19) );
    clients.get(2).add( sdoc("id", "23", cat_s, "B", num_d, 11) );

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

    client.testJQ(params(p, "q", "*:*",
        "json.facet", "{" +
            "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:false}" +
            "}"
        )
        , "facets=={ count:8" +
            ", cat0:{ buckets:[ {val:A,count:3} ] }" +  // w/o overrequest and refinement, count is lower than it should be (we don't see the A from the middle shard)
            "}"
    );

    client.testJQ(params(p, "q", "*:*",
        "json.facet", "{" +
            "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true}" +
            "}"
        )
        , "facets=={ count:8" +
            ", cat0:{ buckets:[ {val:A,count:4} ] }" +  // w/o overrequest, we need refining to get the correct count.
            "}"
    );

    // test that basic stats work for refinement
    client.testJQ(params(p, "q", "*:*",
        "json.facet", "{" +
            "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0, refine:true, facet:{ stat1:'sum(${num_d})'}   }" +
            "}"
        )
        , "facets=={ count:8" +
            ", cat0:{ buckets:[ {val:A,count:4, stat1:4.0} ] }" +
            "}"
    );

    // test sorting buckets by a different stat
    client.testJQ(params(p, "q", "*:*",
        "json.facet", "{" +
            " cat0:{type:terms, field:${cat_s}, sort:'min1 asc', limit:1, overrequest:0, refine:false, facet:{ min1:'min(${num_d})'}   }" +
            ",cat1:{type:terms, field:${cat_s}, sort:'min1 asc', limit:1, overrequest:0, refine:true,  facet:{ min1:'min(${num_d})'}   }" +
            ",sum1:'sum(num_d)'" +  // make sure that root bucket stats aren't affected by refinement
            "}"
        )
        , "facets=={ count:8" +
            ", cat0:{ buckets:[ {val:A,count:3, min1:-19.0} ] }" +  // B wins in shard2, so we're missing the "A" count for that shar w/o refinement.
            ", cat1:{ buckets:[ {val:A,count:4, min1:-19.0} ] }" +  // with refinement, we get the right count
            ", sum1:2.0" +
            "}"
    );


  }


}
