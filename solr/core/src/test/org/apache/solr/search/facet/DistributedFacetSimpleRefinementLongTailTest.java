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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/**
 * A test the demonstrates some of the expected behavior fo "long tail" terms when using <code>refine:simple</code>
 * <p>
 * <b>NOTE:</b> This test ignores the control collection (in single node mode, there is no 
 * need for the overrequesting, all the data is local -- so comparisons with it wouldn't 
 * be valid in the cases we are testing here)
 * </p>
 * <p>
 * <b>NOTE:</b> This test is heavily inspired by (and uses the same indexed documents) as 
 * {@link org.apache.solr.handler.component.DistributedFacetPivotLongTailTest} -- however the behavior of 
 * <code>refine:simple</code> is "simpler" then the refinement logic used by 
 * <code>facet.pivot</code> so the assertions in this test vary from that test.
 * </p>
 */
public class DistributedFacetSimpleRefinementLongTailTest extends BaseDistributedSearchTestCase {

  private static List<String> ALL_STATS = Arrays.asList("min", "max", "sum", "stddev", "avg", "sumsq", "unique",
      "missing", "countvals", "percentile", "variance", "hll");
                                                        
  private final String STAT_FIELD;
  private String ALL_STATS_JSON = "";

  public DistributedFacetSimpleRefinementLongTailTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");

    STAT_FIELD = random().nextBoolean() ? "stat_is" : "stat_i";

    for (String stat : ALL_STATS) {
      String val = stat.equals("percentile")? STAT_FIELD+",90": STAT_FIELD;
      ALL_STATS_JSON += stat + ":'" + stat + "(" + val + ")',";
    }
  }
  
  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    buildIndexes(clients, STAT_FIELD);
    commit();
    
    sanityCheckIndividualShards();
    checkRefinementAndOverrequesting();
    checkSubFacetStats();

  }

  public static void buildIndexes(final List<SolrClient> clients, final String statField) throws Exception {

    assertEquals("This indexing code assumes exactly 3 shards/clients", 3, clients.size());
    
    final AtomicInteger docNum = new AtomicInteger();
    final SolrClient shard0 = clients.get(0);
    final SolrClient shard1 = clients.get(1);
    final SolrClient shard2 = clients.get(2);

    // the 5 top foo_s terms have 100 docs each on every shard
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 5; j++) {
        shard0.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "aaa"+j, statField, j * 13 - i));
        shard1.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "aaa"+j, statField, j * 3 + i));
        shard2.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "aaa"+j, statField, i * 7 + j));
      }
    }

    // 20 foo_s terms that come in "second" with 50 docs each
    // on both shard0 & shard1 ("bbb_")
    for (int i = 0; i < 50; i++) {
      for (int j = 0; j < 20; j++) {
        shard0.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "bbb"+j, statField, 0));
        shard1.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "bbb"+j, statField, 1));
      }
      // distracting term appears on only on shard2 50 times
      shard2.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "junkA"));
    }
    // put "bbb0" on shard2 exactly once to sanity check refinement
    shard2.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "bbb0", statField, -2));

    // long 'tail' foo_s term appears in 45 docs on every shard
    // foo_s:tail is the only term with bar_s sub-pivot terms
    for (int i = 0; i < 45; i++) {

      // for sub-pivot, shard0 & shard1 have 6 docs each for "tailB"
      // but the top 5 terms are ccc(0-4) -- 7 on each shard
      // (4 docs each have junk terms)
      String sub_term = (i < 35) ? "ccc"+(i % 5) : ((i < 41) ? "tailB" : "junkA");
      shard0.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "tail", "bar_s", sub_term, statField, i));
      shard1.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "tail", "bar_s", sub_term, statField, i));

      // shard2's top 5 sub-pivot terms are junk only it has with 8 docs each
      // and 5 docs that use "tailB"
      // NOTE: none of these get statField ! !
      sub_term = (i < 40) ? "junkB"+(i % 5) : "tailB";
      shard2.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "tail", "bar_s", sub_term));
    }

    // really long tail uncommon foo_s terms on shard2
    for (int i = 0; i < 30; i++) {
      // NOTE: using "Z" here so these sort before bbb0 when they tie for '1' instance each on shard2
      shard2.add(sdoc("id", docNum.incrementAndGet(), "foo_s", "ZZZ"+i));
    }

  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void sanityCheckIndividualShards() throws Exception {
    // sanity check that our expectations about each shard (non-distrib) are correct

    SolrParams req = params( "q", "*:*", "distrib", "false", "json.facet",
                             " { foo:{ type:terms, limit:10, field:foo_s, facet:{ bar:{ type:terms, limit:10, field:bar_s }}}}");

    List<NamedList>[] shardFooBuckets = new List[clients.size()];
    for (int i = 0; i < clients.size(); i++) {
      shardFooBuckets[i] = (List<NamedList>)
        ((NamedList<NamedList>)clients.get(i).query( req ).getResponse().get("facets")).get("foo").get("buckets");
    }

    // top 5 same on all shards
    for (int i = 0; i < 3; i++) {
      assertEquals(10, shardFooBuckets[i].size());
      for (int j = 0; j < 5; j++) {
        NamedList bucket = shardFooBuckets[i].get(j);
        assertEquals(bucket.toString(), "aaa"+j, bucket.get("val"));
        assertEquals(bucket.toString(), 100, bucket.get("count"));
      }
    }
    // top 6-10 same on shard0 & shard1
    for (int i = 0; i < 2; i++) {
      for (int j = 5; j < 10; j++) {
        NamedList bucket = shardFooBuckets[i].get(j);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("bbb"));
        assertEquals(bucket.toString(), 50, bucket.get("count"));
      }
    }

    // 6-10 on shard2
    assertEquals("junkA", shardFooBuckets[2].get(5).get("val"));
    assertEquals(50, shardFooBuckets[2].get(5).get("count"));
    assertEquals("tail", shardFooBuckets[2].get(6).get("val"));
    assertEquals(45, shardFooBuckets[2].get(6).get("count"));
    for (int j = 7; j < 10; j++) {
      NamedList bucket = shardFooBuckets[2].get(j);
      assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("ZZZ"));
      assertEquals(bucket.toString(), 1, bucket.get("count"));
    }
    
    // check 'bar' sub buckets on "tail" from shard2
    { List<NamedList> bar_buckets = (List<NamedList>)  ((NamedList<NamedList>) shardFooBuckets[2].get(6).get("bar")).get("buckets");
      assertEquals(6, bar_buckets.size());
      for (int j = 0; j < 5; j++) {
        NamedList bucket = bar_buckets.get(j);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("junkB"));
        assertEquals(bucket.toString(), 8, bucket.get("count"));
      }
      NamedList bucket = bar_buckets.get(5);
      assertEquals("tailB", bucket.get("val"));
      assertEquals(5, bucket.get("count"));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void checkRefinementAndOverrequesting() throws Exception {
    // // distributed queries // //

    { // w/o refinement, the default overrequest isn't enough to find the long 'tail' *OR* the correct count for 'bbb0'...
      List<NamedList> foo_buckets = (List<NamedList>)
        ((NamedList<NamedList>)
         queryServer( params( "q", "*:*", "shards", getShardsString(), "json.facet",
                              "{ foo: { type:terms, refine:none, limit:6, field:foo_s } }"
                              ) ).getResponse().get("facets")).get("foo").get("buckets");
      assertEquals(6, foo_buckets.size());
      for (int i = 0; i < 5; i++) {
        NamedList bucket = foo_buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("aaa"));
        assertEquals(bucket.toString(), 300L, bucket.get("count"));
      }

      // this will be short the "+1" fo the doc added to shard2...
      NamedList bucket = foo_buckets.get(5);
      assertTrue(bucket.toString(), bucket.get("val").equals("bbb0")); // 'tail' is missed
      assertEquals(bucket.toString(), 100L, bucket.get("count")); // will not include the "+1" for the doc added to shard2
    }

    // even if we enable refinement, we still won't find the long 'tail' ...
    // regardless of wether we use either the default overrequest, or disable overrequesting...
    for (String over : Arrays.asList( "", "overrequest:0,")) { 
      List<NamedList> foo_buckets = (List<NamedList>)
        ((NamedList<NamedList>)
         queryServer( params( "q", "*:*", "shards", getShardsString(), "json.facet",
                              "{ foo: { type:terms, refine:simple, limit:6, "+ over +" field:foo_s, facet:{ " + ALL_STATS_JSON + 
                              "  bar: { type:terms, refine:simple, limit:6, "+ over +" field:bar_s, facet:{"+ALL_STATS_JSON+"}}}}}"
                              ) ).getResponse().get("facets")).get("foo").get("buckets");
      assertEquals(6, foo_buckets.size());
      for (int i = 0; i < 5; i++) {
        NamedList bucket = foo_buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("aaa"));
        assertEquals(bucket.toString(), 300L, bucket.get("count"));
      }
      // ...but it should have correctly asked shard2 to refine bbb0
      NamedList bucket = foo_buckets.get(5);
      assertTrue(bucket.toString(), bucket.get("val").equals("bbb0"));
      assertEquals(bucket.toString(), 101L, bucket.get("count"));
      // ...and the status under bbb0 should be correct to include the refinement
      assertEquals(ALL_STATS.size() + 3, bucket.size()); // val,count,facet
      assertEquals(-2L, bucket.get("min"));                                         // this min only exists on shard2
      assertEquals(1L, bucket.get("max"));
      assertEquals(101L, bucket.get("countvals"));
      assertEquals(0L, bucket.get("missing"));
      assertEquals(48.0D, bucket.get("sum"));
      assertEquals(1.0D, bucket.get("percentile"));
      assertEquals(0.475247524752475D, (double) bucket.get("avg"), 0.1E-7);
      assertEquals(54.0D, (double) bucket.get("sumsq"), 0.1E-7);
      // assertEquals(0.55846323792D, (double) bucket.get("stddev"), 0.1E-7); // TODO: SOLR-11725
      // assertEquals(0.3118811881D, (double) bucket.get("variance"), 0.1E-7); // TODO: SOLR-11725
      assertEquals(0.55569169111D, (double) bucket.get("stddev"), 0.1E-7); // json.facet is using the "uncorrected stddev"
      assertEquals(0.3087932556D, (double) bucket.get("variance"), 0.1E-7); // json.facet is using the "uncorrected variance"
      assertEquals(3L, bucket.get("unique"));
      assertEquals(3L, bucket.get("hll"));
    }


    // with a limit==6, we have to "overrequest >= 20" in order to ensure that 'tail' is included in the top 6
    // this is because of how the "simple" refinement process works: the "top buckets" are determined based
    // on the info available in the first pass request.
    //
    // Even though 'tail' is returned in the top6 for shard2, the cumulative total for 'bbb0' from shard0 and shard1 is
    // high enough that the simple facet refinement ignores 'tail' because it assumes 'bbb0's final total will be greater.
    //
    // Meanwhile, for the sub-facet on 'bar', a limit==6 means we should correctly find 'tailB' as the top sub-term of 'tail',
    // regardless of how much overrequest is used (or even if we don't have any refinement) since it's always in the top6...
    for (String bar_opts : Arrays.asList( "refine:none,",
                                          "refine:simple,",
                                          "refine:none,   overrequest:0,",
                                          "refine:simple, overrequest:0," )) {


      List<NamedList> buckets = (List<NamedList>)
        ((NamedList<NamedList>)
         queryServer( params( "q", "*:*", "shards", getShardsString(), "json.facet",
                              "{ foo: { type:terms, limit:6, overrequest:20, refine:simple, field:foo_s, facet:{ " +
                              "  bar: { type:terms, limit:6, " + bar_opts + " field:bar_s }}}}"
                              ) ).getResponse().get("facets")).get("foo").get("buckets");

      assertEquals(6, buckets.size());
      for (int i = 0; i < 5; i++) {
        NamedList bucket = buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("aaa"));
        assertEquals(bucket.toString(), 300L, bucket.get("count"));
      }

      NamedList bucket = buckets.get(5);
      assertEquals(bucket.toString(), "tail", bucket.get("val"));
      assertEquals(bucket.toString(), 135L, bucket.get("count"));
      // check the sub buckets
      buckets = ((NamedList<NamedList<List<NamedList>>>) bucket).get("bar").get("buckets");
      assertEquals(6, buckets.size());
      bucket = buckets.get(0);
      assertEquals(bucket.toString(), "tailB", bucket.get("val"));
      assertEquals(bucket.toString(), 17L, bucket.get("count"));
      for (int i = 1; i < 6; i++) { // ccc(0-4)
        bucket = buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("ccc"));
        assertEquals(bucket.toString(), 14L, bucket.get("count"));
      }
    }
    
    // if we lower the limit on the sub-bucket to '5', overrequesting of at least 1 should still ensure 
    // that we get the correct top5 including "tailB" -- even w/o refinement
    for (String bar_opts : Arrays.asList( "refine:none,",
                                          "refine:simple,",
                                          "refine:none,   overrequest:1,",
                                          "refine:simple, overrequest:1," )) {
      
      List<NamedList> buckets = (List<NamedList>)
        ((NamedList<NamedList>)
         queryServer( params( "q", "*:*", "shards", getShardsString(), "json.facet",
                              "{ foo: { type:terms, limit:6, overrequest:20, refine:simple, field:foo_s, facet:{ " +
                              "  bar: { type:terms, limit:5, " + bar_opts + " field:bar_s }}}}"
                              ) ).getResponse().get("facets")).get("foo").get("buckets");
      
      assertEquals(6, buckets.size());
      for (int i = 0; i < 5; i++) {
        NamedList bucket = buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("aaa"));
        assertEquals(bucket.toString(), 300L, bucket.get("count"));
      }
      NamedList bucket = buckets.get(5);
      assertEquals(bucket.toString(), "tail", bucket.get("val"));
      assertEquals(bucket.toString(), 135L, bucket.get("count"));
      // check the sub buckets
      buckets = ((NamedList<NamedList<List<NamedList>>>) bucket).get("bar").get("buckets");
      assertEquals(5, buckets.size());
      bucket = buckets.get(0);
      assertEquals(bucket.toString(), "tailB", bucket.get("val"));
      assertEquals(bucket.toString(), 17L, bucket.get("count"));
      for (int i = 1; i < 5; i++) { // ccc(0-3)
        bucket = buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("ccc"));
        assertEquals(bucket.toString(), 14L, bucket.get("count"));
      }
    }

    // however: with a lower sub-facet limit==5, and overrequesting disabled,
    // we're going to miss out on tailB even if we have refinement
    for (String bar_opts : Arrays.asList( "refine:none,   overrequest:0,",
                                          "refine:simple, overrequest:0," )) {
      
      List<NamedList> buckets = (List<NamedList>)
        ((NamedList<NamedList>)
         queryServer( params( "q", "*:*", "shards", getShardsString(), "json.facet",
                              "{ foo: { type:terms, limit:6, overrequest:20, refine:simple, field:foo_s, facet:{ " +
                              "  bar: { type:terms, limit:5, " + bar_opts + " field:bar_s }}}}"
                              ) ).getResponse().get("facets")).get("foo").get("buckets");

      assertEquals(6, buckets.size());
      for (int i = 0; i < 5; i++) {
        NamedList bucket = buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("aaa"));
        assertEquals(bucket.toString(), 300L, bucket.get("count"));
      }
      NamedList bucket = buckets.get(5);
      assertEquals(bucket.toString(), "tail", bucket.get("val"));
      assertEquals(bucket.toString(), 135L, bucket.get("count"));
      // check the sub buckets
      buckets = ((NamedList<NamedList<List<NamedList>>>) bucket).get("bar").get("buckets");
      assertEquals(5, buckets.size());
      for (int i = 0; i < 5; i++) { // ccc(0-4)
        bucket = buckets.get(i);
        assertTrue(bucket.toString(), bucket.get("val").toString().startsWith("ccc"));
        assertEquals(bucket.toString(), 14L, bucket.get("count"));
      }
    }

  }

  private void checkSubFacetStats() throws Exception { 
    // Deep checking of some Facet stats
    
    // the assertions only care about the first 5 results of each facet, but to get the long tail more are needed
    // from the sub-shards.  results should be the same regardless of: "high limit" vs "low limit + high overrequest"
    checkSubFacetStats("refine:simple, limit: 100,");
    checkSubFacetStats("refine:simple, overrequest: 100,");

    // and the results shouldn't change if we explicitly disable refinement
    checkSubFacetStats("refine:none, limit: 100,");
    checkSubFacetStats("refine:none, overrequest: 100,");

  }
  
  private void checkSubFacetStats(String extraJson) throws Exception {
    String commonJson = "type: terms, " + extraJson;
    @SuppressWarnings({"unchecked", "rawtypes"})
    NamedList<NamedList> all_facets = (NamedList) queryServer
      ( params( "q", "*:*", "shards", getShardsString(), "rows" , "0", "json.facet",
                "{ foo : { " + commonJson + " field: foo_s, facet: { " +
                ALL_STATS_JSON + " bar: { " + commonJson + " field: bar_s, facet: { " + ALL_STATS_JSON +
                // under bar, in addition to "ALL" simple stats, we also ask for skg...
                ", skg : 'relatedness($skg_fore,$skg_back)' } } } } }",
                "skg_fore", STAT_FIELD+":[0 TO 40]", "skg_back", STAT_FIELD+":[-10000 TO 10000]"
      ) ).getResponse().get("facets");
    
    assertNotNull(all_facets);

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<NamedList> foo_buckets = (List) (all_facets.get("foo")).get("buckets");

    @SuppressWarnings({"rawtypes"})
    NamedList aaa0_Bucket = foo_buckets.get(0);
    assertEquals(ALL_STATS.size() + 3, aaa0_Bucket.size()); // val,count,facet
    assertEquals("aaa0", aaa0_Bucket.get("val"));
    assertEquals(300L, aaa0_Bucket.get("count"));
    assertEquals(-99L, aaa0_Bucket.get("min"));
    assertEquals(693L, aaa0_Bucket.get("max"));
    assertEquals(300L, aaa0_Bucket.get("countvals"));
    assertEquals(0L, aaa0_Bucket.get("missing"));
    assertEquals(34650.0D, aaa0_Bucket.get("sum"));
    assertEquals(483.70000000000016D, (double)aaa0_Bucket.get("percentile"), 0.1E-7);
    assertEquals(115.5D, (double) aaa0_Bucket.get("avg"), 0.1E-7);
    assertEquals(1.674585E7D, (double) aaa0_Bucket.get("sumsq"), 0.1E-7);
    // assertEquals(206.4493184076D, (double) aaa0_Bucket.get("stddev"), 0.1E-7); // TODO: SOLR-11725
    // assertEquals(42621.32107023412D, (double) aaa0_Bucket.get("variance"), 0.1E-7);  // TODO: SOLR-11725
    assertEquals(206.1049489944D, (double) aaa0_Bucket.get("stddev"), 0.1E-7); // json.facet is using the "uncorrected stddev"
    assertEquals(42479.25D, (double) aaa0_Bucket.get("variance"), 0.1E-7); // json.facet is using the "uncorrected variance"
    assertEquals(284L, aaa0_Bucket.get("unique"));
    assertEquals(284L, aaa0_Bucket.get("hll"));

    @SuppressWarnings({"rawtypes"})
    NamedList tail_Bucket = foo_buckets.get(5);
    assertEquals(ALL_STATS.size() + 3, tail_Bucket.size()); // val,count,facet
    assertEquals("tail", tail_Bucket.get("val"));
    assertEquals(135L, tail_Bucket.get("count"));
    assertEquals(0L, tail_Bucket.get("min"));
    assertEquals(44L, tail_Bucket.get("max"));
    assertEquals(90L, tail_Bucket.get("countvals"));
    assertEquals(40.0D, tail_Bucket.get("percentile"));
    assertEquals(45L, tail_Bucket.get("missing"));
    assertEquals(1980.0D, tail_Bucket.get("sum"));
    assertEquals(22.0D, (double) tail_Bucket.get("avg"), 0.1E-7);
    assertEquals(58740.0D, (double) tail_Bucket.get("sumsq"), 0.1E-7);
    // assertEquals(13.0599310011D, (double) tail_Bucket.get("stddev"), 0.1E-7); // TODO: SOLR-11725
    // assertEquals(170.5617977535D, (double) tail_Bucket.get("variance"), 0.1E-7); // TODO: SOLR-11725
    assertEquals(12.9871731592D, (double) tail_Bucket.get("stddev"), 0.1E-7); // json.facet is using the "uncorrected stddev"
    assertEquals(168.666666667D, (double) tail_Bucket.get("variance"), 0.1E-7); // json.facet is using the "uncorrected variance"
    assertEquals(45L, tail_Bucket.get("unique"));
    assertEquals(45L, tail_Bucket.get("hll"));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<NamedList> tail_bar_buckets = (List) ((NamedList)tail_Bucket.get("bar")).get("buckets");
   
    @SuppressWarnings({"rawtypes"})
    NamedList tailB_Bucket = tail_bar_buckets.get(0);
    assertEquals(ALL_STATS.size() + 3, tailB_Bucket.size()); // val,count,skg ... NO SUB FACETS
    assertEquals("tailB", tailB_Bucket.get("val"));
    assertEquals(17L, tailB_Bucket.get("count"));
    assertEquals(35L, tailB_Bucket.get("min"));
    assertEquals(40L, tailB_Bucket.get("max"));
    assertEquals(12L, tailB_Bucket.get("countvals"));
    assertEquals(39.9D, tailB_Bucket.get("percentile"));
    assertEquals(5L, tailB_Bucket.get("missing"));
    assertEquals(450.0D, tailB_Bucket.get("sum"));
    assertEquals(37.5D, (double) tailB_Bucket.get("avg"), 0.1E-7);
    assertEquals(16910.0D, (double) tailB_Bucket.get("sumsq"), 0.1E-7);
    // assertEquals(1.78376517D, (double) tailB_Bucket.get("stddev"), 0.1E-7); // TODO: SOLR-11725
    // assertEquals(3.1818181817D, (double) tailB_Bucket.get("variance"), 0.1E-7); // TODO: SOLR-11725
    assertEquals(1.70782513D, (double) tailB_Bucket.get("stddev"), 0.1E-7); // json.facet is using the "uncorrected stddev"
    assertEquals(2.9166666747D, (double) tailB_Bucket.get("variance"), 0.1E-7); // json.facet is using the "uncorrected variance"
    assertEquals(6L, tailB_Bucket.get("unique"));
    assertEquals(6L, tailB_Bucket.get("hll"));

    // check the SKG stats on our tailB bucket
    @SuppressWarnings({"rawtypes"})
    NamedList tailB_skg = (NamedList) tailB_Bucket.get("skg");
    assertEquals(tailB_skg.toString(),
                 3, tailB_skg.size()); 
    assertEquals(0.19990D,    tailB_skg.get("relatedness"));
    assertEquals(0.00334D,    tailB_skg.get("foreground_popularity"));
    assertEquals(0.00334D,    tailB_skg.get("background_popularity"));
    //assertEquals(12L,       tailB_skg.get("foreground_count"));
    //assertEquals(82L,       tailB_skg.get("foreground_size"));
    //assertEquals(12L,       tailB_skg.get("background_count"));
    //assertEquals(3591L,     tailB_skg.get("background_size"));
  }

}
