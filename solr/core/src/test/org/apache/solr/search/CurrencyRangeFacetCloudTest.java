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
package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.CurrencyFieldTypeTest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.BeforeClass;
import org.junit.Test;

public class CurrencyRangeFacetCloudTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String COLLECTION = MethodHandles.lookup().lookupClass().getName();
  private static final String CONF = COLLECTION + "_configSet";
  
  private static String FIELD = null; // randomized

  private static final List<String> STR_VALS = Arrays.asList("x0", "x1", "x2");
  // NOTE: in our test conversions EUR uses an asynetric echange rate
  // these are the equivalent values relative to:                                USD        EUR        GBP
  private static final List<String> VALUES = Arrays.asList("10.00,USD",     // 10.00,USD  25.00,EUR   5.00,GBP
                                                           "15.00,EUR",     //  7.50,USD  15.00,EUR   7.50,GBP
                                                           "6.00,GBP",      // 12.00,USD  12.00,EUR   6.00,GBP
                                                           "7.00,EUR",      //  3.50,USD   7.00,EUR   3.50,GBP
                                                           "2,GBP");        //  4.00,USD   4.00,EUR   2.00,GBP
  private static final int NUM_DOCS = STR_VALS.size() * VALUES.size();
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    CurrencyFieldTypeTest.assumeCurrencySupport("USD", "EUR", "MXN", "GBP", "JPY", "NOK");
    FIELD = usually() ? "amount_CFT" : "amount";
    
    final int numShards = TestUtil.nextInt(random(),1,5);
    final int numReplicas = 1;
    final int maxShardsPerNode = 1;
    final int nodeCount = numShards * numReplicas;

    configureCluster(nodeCount)
      .addConfig(CONF, Paths.get(TEST_HOME(), "collection1", "conf"))
      .configure();

    assertEquals(0, (CollectionAdminRequest.createCollection(COLLECTION, CONF, numShards, numReplicas)
                      .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
                     .setMaxShardsPerNode(maxShardsPerNode)
                     .setProperties(Collections.singletonMap(CoreAdminParams.CONFIG, "solrconfig-minimal.xml"))
                     .process(cluster.getSolrClient())).getStatus());
    
    cluster.getSolrClient().setDefaultCollection(COLLECTION);
    
    for (int id = 0; id < NUM_DOCS; id++) { // we're indexing each Currency value in 3 docs, each with a diff 'x_s' field value
      // use modulo to pick the values, so we don't add the docs in strict order of either VALUES of STR_VALS
      // (that way if we want ot filter by id later, it's an independent variable)
      final String x = STR_VALS.get(id % STR_VALS.size());
      final String val = VALUES.get(id % VALUES.size());
      assertEquals(0, (new UpdateRequest().add(sdoc("id", "" + id,
                                                    "x_s", x,
                                                    FIELD, val))
                       ).process(cluster.getSolrClient()).getStatus());
      
    }
    assertEquals(0, cluster.getSolrClient().commit().getStatus());
  }

  @SuppressWarnings({"rawtypes"})
  public void testSimpleRangeFacetsOfSymetricRates() throws Exception {
    for (boolean use_mincount : Arrays.asList(true, false)) {
    
      // exchange rates relative to USD...
      //
      // for all of these permutations, the numDocs in each bucket that we get back should be the same
      // (regardless of the any asymetric echanges ranges, or the currency used for the 'gap') because the
      // start & end are always in USD.
      //
      // NOTE:
      //  - 0,1,2 are the *input* start,gap,end
      //  - 3,4,5 are the *normalized* start,gap,end expected in the response
      for (List<String> args : Arrays.asList(// default currency is USD
                                             Arrays.asList("4", "1.00", "11.0",
                                                           "4.00,USD", "1.00,USD", "11.00,USD"),
                                             // explicit USD
                                             Arrays.asList("4,USD", "1,USD", "11,USD",
                                                           "4.00,USD", "1.00,USD", "11.00,USD"),
                                             // Gap can be in diff currency (but start/end must currently match)
                                             Arrays.asList("4.00,USD", "000.50,GBP", "11,USD",
                                                           "4.00,USD", ".50,GBP", "11.00,USD"),
                                             Arrays.asList("4.00,USD", "2,EUR", "11,USD",
                                                           "4.00,USD", "2.00,EUR", "11.00,USD"))) {
        
        assertEquals(6, args.size()); // sanity check
        
        // first let's check facet.range
        SolrQuery solrQuery = new SolrQuery("q", "*:*", "rows", "0", "facet", "true", "facet.range", FIELD,
                                            "facet.mincount", (use_mincount ? "3" : "0"),
                                            "f." + FIELD + ".facet.range.start", args.get(0),
                                            "f." + FIELD + ".facet.range.gap", args.get(1),
                                            "f." + FIELD + ".facet.range.end", args.get(2),
                                            "f." + FIELD + ".facet.range.other", "all");
        QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
        try {
          assertEquals(NUM_DOCS, rsp.getResults().getNumFound());
          
          final String start = args.get(3);
          final String gap = args.get(4);
          final String end = args.get(5);
          
          final List<RangeFacet> range_facets = rsp.getFacetRanges();
          assertEquals(1, range_facets.size());
          final RangeFacet result = range_facets.get(0);
          assertEquals(FIELD, result.getName());
          assertEquals(start, result.getStart());
          assertEquals(gap, result.getGap());
          assertEquals(end, result.getEnd());
          assertEquals(3, result.getBefore());
          assertEquals(3, result.getAfter());
          assertEquals(9, result.getBetween());
          
          @SuppressWarnings({"unchecked"})
          List<RangeFacet.Count> counts = result.getCounts();
          if (use_mincount) {
            assertEquals(3, counts.size());
            for (int i = 0; i < 3; i++) {
              RangeFacet.Count bucket = counts.get(i);
              assertEquals((4 + (i * 3)) + ".00,USD", bucket.getValue());
              assertEquals("bucket #" + i, 3, bucket.getCount());
            }
          } else {
            assertEquals(7, counts.size());
            for (int i = 0; i < 7; i++) {
              RangeFacet.Count bucket = counts.get(i);
              assertEquals((4 + i) + ".00,USD", bucket.getValue());
              assertEquals("bucket #" + i, (i == 0 || i == 3 || i == 6) ? 3 : 0, bucket.getCount());
            }
          }
        } catch (AssertionError|RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }

        // same basic logic, w/json.facet
        solrQuery = new SolrQuery("q", "*:*", "rows", "0", "json.facet",
                                  "{ foo:{ type:range, field:"+FIELD+", mincount:"+(use_mincount ? 3 : 0)+", " +
                                  "        start:'"+args.get(0)+"', gap:'"+args.get(1)+"', end:'"+args.get(2)+"', other:all}}");
        rsp = cluster.getSolrClient().query(solrQuery);
        try {
          assertEquals(NUM_DOCS, rsp.getResults().getNumFound());
          
          @SuppressWarnings({"unchecked"})
          final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
          
          assertEqualsHACK("before", 3L, ((NamedList)foo.get("before")).get("count"));
          assertEqualsHACK("after", 3L, ((NamedList)foo.get("after")).get("count"));
          assertEqualsHACK("between", 9L, ((NamedList)foo.get("between")).get("count"));
          
          @SuppressWarnings({"unchecked"})
          final List<NamedList> buckets = (List<NamedList>) foo.get("buckets");
          
          if (use_mincount) {
            assertEquals(3, buckets.size());
            for (int i = 0; i < 3; i++) {
              NamedList bucket = buckets.get(i);
              assertEquals((4 + (3 * i)) + ".00,USD", bucket.get("val"));
              assertEqualsHACK("bucket #" + i, 3L, bucket.get("count"));
            }
          } else {
            assertEquals(7, buckets.size());
            for (int i = 0; i < 7; i++) {
              NamedList bucket = buckets.get(i);
              assertEquals((4 + i) + ".00,USD", bucket.get("val"));
              assertEqualsHACK("bucket #" + i, (i == 0 || i == 3 || i == 6) ? 3L : 0L, bucket.get("count"));
            }
          }
        } catch (AssertionError|RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }
      }
    }
  }
      
  public void testFacetRangeOfAsymetricRates() throws Exception {
    // facet.range: exchange rates relative to EUR...
    //
    // because of the asymetric echange rate, the counts for these buckets will be different
    // then if we just converted the EUR values to USD
    for (boolean use_mincount : Arrays.asList(true, false)) {
      final SolrQuery solrQuery = new SolrQuery("q", "*:*", "rows", "0", "facet", "true", "facet.range", FIELD,
                                                "facet.mincount", (use_mincount ? "3" : "0"),
                                                "f." + FIELD + ".facet.range.start", "8,EUR",
                                                "f." + FIELD + ".facet.range.gap", "2,EUR",
                                                "f." + FIELD + ".facet.range.end", "22,EUR",
                                                "f." + FIELD + ".facet.range.other", "all");
      final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      try {
        assertEquals(NUM_DOCS, rsp.getResults().getNumFound());
        @SuppressWarnings({"rawtypes"})
        final List<RangeFacet> range_facets = rsp.getFacetRanges();
        assertEquals(1, range_facets.size());
        @SuppressWarnings({"rawtypes"})
        final RangeFacet result = range_facets.get(0);
        assertEquals(FIELD, result.getName());
        assertEquals("8.00,EUR", result.getStart());
        assertEquals("2.00,EUR", result.getGap());
        assertEquals("22.00,EUR", result.getEnd());
        assertEquals(6, result.getBefore());
        assertEquals(3, result.getAfter());
        assertEquals(6, result.getBetween());
        
        @SuppressWarnings({"unchecked"})
        List<RangeFacet.Count> counts = result.getCounts();
        if (use_mincount) {
          assertEquals(2, counts.size());
          for (int i = 0; i < 2; i++) {
            RangeFacet.Count bucket = counts.get(i);
            assertEquals((12 + (i * 2)) + ".00,EUR", bucket.getValue());
            assertEquals("bucket #" + i, 3, bucket.getCount());
          }
        } else {
          assertEquals(7, counts.size());
          for (int i = 0; i < 7; i++) {
            RangeFacet.Count bucket = counts.get(i);
            assertEquals((8 + (i * 2)) + ".00,EUR", bucket.getValue());
            assertEquals("bucket #" + i, (i == 2 || i == 3) ? 3 : 0, bucket.getCount());
          }
        }
      } catch (AssertionError|RuntimeException ae) {
        throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
      }
    }
  }
  
  public void testJsonFacetRangeOfAsymetricRates() throws Exception {
    // json.facet: exchange rates relative to EUR (same as testFacetRangeOfAsymetricRates)
    //
    // because of the asymetric echange rate, the counts for these buckets will be different
    // then if we just converted the EUR values to USD
    for (boolean use_mincount : Arrays.asList(true, false)) {
      final SolrQuery solrQuery = new SolrQuery("q", "*:*", "rows", "0", "json.facet",
                                                "{ foo:{ type:range, field:"+FIELD+", start:'8,EUR', " +
                                                "        mincount:"+(use_mincount ? 3 : 0)+", " +
                                                "        gap:'2,EUR', end:'22,EUR', other:all}}");
      final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      try {
        assertEquals(NUM_DOCS, rsp.getResults().getNumFound());
        
        @SuppressWarnings({"unchecked"})
        final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
        
        assertEqualsHACK("before", 6L, ((NamedList)foo.get("before")).get("count"));
        assertEqualsHACK("after", 3L, ((NamedList)foo.get("after")).get("count"));
        assertEqualsHACK("between", 6L, ((NamedList)foo.get("between")).get("count"));
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        final List<NamedList> buckets = (List<NamedList>) foo.get("buckets");
        
        if (use_mincount) {
          assertEquals(2, buckets.size());
          for (int i = 0; i < 2; i++) {
            @SuppressWarnings({"rawtypes"})
            NamedList bucket = buckets.get(i);
            assertEquals((12 + (i * 2)) + ".00,EUR", bucket.get("val"));
            assertEqualsHACK("bucket #" + i, 3L, bucket.get("count"));
          }
        } else {
          assertEquals(7, buckets.size());
          for (int i = 0; i < 7; i++) {
            @SuppressWarnings({"rawtypes"})
            NamedList bucket = buckets.get(i);
            assertEquals((8 + (i * 2)) + ".00,EUR", bucket.get("val"));
            assertEqualsHACK("bucket #" + i, (i == 2 || i == 3) ? 3L : 0L, bucket.get("count"));
          }
        }
      } catch (AssertionError|RuntimeException ae) {
        throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
      }
    }
  }
    
  public void testFacetRangeCleanErrorOnMissmatchCurrency() {
    final String expected = "Cannot compare CurrencyValues when their currencies are not equal";
    ignoreException(expected);
    
    // test to check clean error when start/end have diff currency (facet.range)
    final SolrQuery solrQuery = new SolrQuery("q", "*:*", "rows", "0", "facet", "true", "facet.range", FIELD,
                                              "f." + FIELD + ".facet.range.start", "0,EUR",
                                              "f." + FIELD + ".facet.range.gap", "10,EUR",
                                              "f." + FIELD + ".facet.range.end", "100,USD");
    final SolrException ex = expectThrows(SolrException.class, () -> {
        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage(), ex.getMessage().contains(expected));
  }

  public void testJsonFacetCleanErrorOnMissmatchCurrency() {
    final String expected = "Cannot compare CurrencyValues when their currencies are not equal";
    ignoreException(expected);
    
    // test to check clean error when start/end have diff currency (json.facet)
    final SolrQuery solrQuery = new SolrQuery("q", "*:*", "json.facet",
                                              "{ x:{ type:range, field:"+FIELD+", " +
                                              "      start:'0,EUR', gap:'10,EUR', end:'100,USD' } }");
    final SolrException ex = expectThrows(SolrException.class, () -> {
        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage(), ex.getMessage().contains(expected));
  }
  
  @Test
  public void testJsonRangeFacetWithSubFacet() throws Exception {

    // range facet, with terms facet nested under using limit=2 w/overrequest disabled
    // filter out the first 5 docs (by id) which should ensure that regardless of sharding:
    //  - x2 being the top term for the 1st range bucket
    //  - x0 being the top term for the 2nd range bucket
    //  - the 2nd term in each bucket may vary based on shard/doc placement, but the count will always be '1'
    // ...and in many cases (based on the shard/doc placement) this will require refinement to backfill the top terms
    final String filter = "id_i1:["+VALUES.size()+" TO *]";

    // the *facet* results should be the same regardless of wether we filter via fq, or using a domain filter on the top facet
    for (boolean use_domain : Arrays.asList(true, false)) {
      final String domain = use_domain ? "domain: { filter:'" + filter + "'}," : "";

      // both of these options should produce same results since hardened:false is default
      final String end = random().nextBoolean() ? "end:'20,EUR'" : "end:'15,EUR'";
        
      
      final SolrQuery solrQuery = new SolrQuery("q", (use_domain ? "*:*" : filter),
                                                "rows", "0", "json.facet",
                                                "{ bar:{ type:range, field:"+FIELD+", " + domain + 
                                                "        start:'0,EUR', gap:'10,EUR', "+end+", other:all " +
                                                "        facet: { foo:{ type:terms, field:x_s, " +
                                                "                       refine:true, limit:2, overrequest:0" +
                                                " } } } }");
      final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      try {
        // this top level result count sanity check that should vary based on how we are filtering our facets...
        assertEquals(use_domain ? 15 : 10, rsp.getResults().getNumFound());

        @SuppressWarnings({"unchecked"})
        final NamedList<Object> bar = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("bar");
        @SuppressWarnings({"unchecked"})
        final List<NamedList<Object>> bar_buckets = (List<NamedList<Object>>) bar.get("buckets");
        @SuppressWarnings({"unchecked"})
        final NamedList<Object> before = (NamedList<Object>) bar.get("before");
        @SuppressWarnings({"unchecked"})
        final NamedList<Object> between = (NamedList<Object>) bar.get("between");
        @SuppressWarnings({"unchecked"})
        final NamedList<Object> after = (NamedList<Object>) bar.get("after");
        
        // sanity check our high level expectations...
        assertEquals("bar num buckets", 2, bar_buckets.size());
        assertEqualsHACK("before count", 0L, before.get("count"));
        assertEqualsHACK("between count", 8L, between.get("count"));
        assertEqualsHACK("after count", 2L, after.get("count"));
        
        // drill into the various buckets...
        
        // before should have no subfacets since it's empty...
        assertNull("before has foo???", before.get("foo"));
        
        // our 2 range buckets & their sub facets...
        for (int i = 0; i < 2; i++) {
          final NamedList<Object> bucket = bar_buckets.get(i);
          assertEquals((i * 10) + ".00,EUR", bucket.get("val"));
          assertEqualsHACK("bucket #" + i, 4L, bucket.get("count"));
          @SuppressWarnings({"unchecked"})
          final List<NamedList<Object>> foo_buckets = ((NamedList<List<NamedList<Object>>>)bucket.get("foo")).get("buckets");
          assertEquals("bucket #" + i + " foo num buckets", 2, foo_buckets.size());
          assertEquals("bucket #" + i + " foo top term", (0==i ? "x2" : "x0"), foo_buckets.get(0).get("val"));
          assertEqualsHACK("bucket #" + i + " foo top count", 2, foo_buckets.get(0).get("count"));
          // NOTE: we can't make any assertions about the 2nd val..
          // our limit + randomized sharding could result in either remaining term being picked.
          // but for eiter term, the count should be exactly the same...
          assertEqualsHACK("bucket #" + i + " foo 2nd count", 1, foo_buckets.get(1).get("count"));
        }
        
        { // between...
          @SuppressWarnings({"unchecked"})
          final List<NamedList<Object>> buckets = ((NamedList<List<NamedList<Object>>>)between.get("foo")).get("buckets");
          assertEquals("between num buckets", 2, buckets.size());
          // the counts should both be 3, and the term order should break the tie...
          assertEquals("between bucket top", "x0", buckets.get(0).get("val"));
          assertEqualsHACK("between bucket top count", 3L, buckets.get(0).get("count"));
          assertEquals("between bucket 2nd", "x2", buckets.get(1).get("val"));
          assertEqualsHACK("between bucket 2nd count", 3L, buckets.get(1).get("count"));
        }
        
        { // after...
          @SuppressWarnings({"unchecked"})
          final List<NamedList<Object>> buckets = ((NamedList<List<NamedList<Object>>>)after.get("foo")).get("buckets");
          assertEquals("after num buckets", 2, buckets.size());
          // the counts should both be 1, and the term order should break the tie...
          assertEquals("after bucket top", "x1", buckets.get(0).get("val"));
          assertEqualsHACK("after bucket top count", 1L, buckets.get(0).get("count"));
          assertEquals("after bucket 2nd", "x2", buckets.get(1).get("val"));
          assertEqualsHACK("after bucket 2nd count", 1L, buckets.get(1).get("count"));
        }
        
      } catch (AssertionError|RuntimeException ae) {
        throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
      }
    }
  }
  
  @Test
  public void testJsonRangeFacetAsSubFacet() throws Exception {

    // limit=1, overrequest=1, with refinement enabled
    // filter out the first 5 docs (by id), which should ensure that 'x2' is the top bucket overall...
    //   ...except in some rare sharding cases where it doesn't make it into the top 2 terms.
    // 
    // So the filter also explicitly accepts all 'x2' docs -- ensuring we have enough matches containing that term for it
    // to be enough of a candidate in phase#1, but for many shard arrangements it won't be returned by all shards resulting
    // in refinement being neccessary to get the x_s:x2 sub-shard ranges from shard(s) where x_s:x2 is only tied for the
    // (shard local) top term count and would lose the (index order) tie breaker with x_s:x0 or x_s:x1
    final String filter = "id_i1:["+VALUES.size()+" TO *] OR x_s:x2";

    // the *facet* results should be the same regardless of wether we filter via fq, or using a domain filter on the top facet
    for (boolean use_domain : Arrays.asList(true, false)) {
      final String domain = use_domain ? "domain: { filter:'" + filter + "'}," : "";
      final SolrQuery solrQuery = new SolrQuery("q", (use_domain ? "*:*" : filter),
                                                "rows", "0", "json.facet",
                                                "{ foo:{ type:terms, field:x_s, refine:true, limit:1, overrequest:1, " + domain +
                                                "        facet: { bar:{ type:range, field:"+FIELD+", other:all, " +
                                                "                       start:'8,EUR', gap:'2,EUR', end:'22,EUR' }} } }");
      final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      try {
        // this top level result count sanity check that should vary based on how we are filtering our facets...
        assertEquals(use_domain ? 15 : 11, rsp.getResults().getNumFound());
        
        @SuppressWarnings({"unchecked"})
        final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
        
        // sanity check...
        // because of the facet limit, foo should only have 1 bucket
        // because of the fq, the val should be "x2" and the count=5
        @SuppressWarnings({"unchecked"})
        final List<NamedList<Object>> foo_buckets = (List<NamedList<Object>>) foo.get("buckets");
        assertEquals(1, foo_buckets.size());
        assertEquals("x2", foo_buckets.get(0).get("val"));
        assertEqualsHACK("foo bucket count", 5L, foo_buckets.get(0).get("count"));
        
        @SuppressWarnings({"unchecked"})
        final NamedList<Object> bar = (NamedList<Object>)foo_buckets.get(0).get("bar");
        
        // these are the 'x2' specific counts, based on our fq...
        
        assertEqualsHACK("before", 2L, ((NamedList)bar.get("before")).get("count"));
        assertEqualsHACK("after", 1L, ((NamedList)bar.get("after")).get("count"));
        assertEqualsHACK("between", 2L, ((NamedList)bar.get("between")).get("count"));
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        final List<NamedList> buckets = (List<NamedList>) bar.get("buckets");
        assertEquals(7, buckets.size());
        for (int i = 0; i < 7; i++) {
          @SuppressWarnings({"rawtypes"})
          NamedList bucket = buckets.get(i);
          assertEquals((8 + (i * 2)) + ".00,EUR", bucket.get("val"));
          // 12,EUR & 15,EUR are the 2 values that align with x2 docs
          assertEqualsHACK("bucket #" + i, (i == 2 || i == 3) ? 1L : 0L, bucket.get("count"));
        }
      } catch (AssertionError|RuntimeException ae) {
        throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
      }
    }
  }

  /**
   * HACK to work around SOLR-11775.
   * Asserts that the 'actual' argument is a (non-null) Number, then compares it's 'longValue' to the 'expected' argument
   */
  private static void assertEqualsHACK(String msg, long expected, Object actual) {
    assertNotNull(msg, actual);
    assertTrue(msg + " ... NOT A NUMBER: " + actual.getClass(), Number.class.isInstance(actual));
    assertEquals(msg, expected, ((Number)actual).longValue());
  }

}
