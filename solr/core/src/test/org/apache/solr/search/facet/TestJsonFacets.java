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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.tdunning.math.stats.AVLTreeDigest;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.util.hll.HLL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// Related tests:
//   TestCloudJSONFacetJoinDomain for random field faceting tests with domain modifications
//   TestJsonFacetRefinement for refinement tests
//   TestJsonFacetErrors for error case tests
//   TestJsonRangeFacets for range facet tests

@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Lucene45","Appending"})
public class TestJsonFacets extends SolrTestCaseHS {
  
  private static SolrInstances servers;  // for distributed testing
  private static int origTableSize;
  private static FacetField.FacetMethod origDefaultFacetMethod;

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    JSONTestUtil.failRepeatedKeys = true;

    origTableSize = FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE;
    FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE=2; // stress test resizing

    origDefaultFacetMethod = FacetField.FacetMethod.DEFAULT_METHOD;
    // instead of the following, see the constructor
    //FacetField.FacetMethod.DEFAULT_METHOD = rand(FacetField.FacetMethod.values());

    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  /**
   * Start all servers for cluster if they don't already exist
   */
  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void afterTests() throws Exception {
    systemClearPropertySolrDisableShardsWhitelist();
    JSONTestUtil.failRepeatedKeys = false;
    FacetFieldProcessorByHashDV.MAXIMUM_STARTING_TABLE_SIZE=origTableSize;
    FacetField.FacetMethod.DEFAULT_METHOD = origDefaultFacetMethod;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
  }

  // tip: when debugging failures, change this variable to DEFAULT_METHOD
  // (or if only one method is problematic, set to that explicitly)
  private static final FacetField.FacetMethod TEST_ONLY_ONE_FACET_METHOD
    = null; // FacetField.FacetMethod.DEFAULT_METHOD;

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    if (null != TEST_ONLY_ONE_FACET_METHOD) {
      return Arrays.<Object[]>asList(new Object[] { TEST_ONLY_ONE_FACET_METHOD });
    }
    
    // wrap each enum val in an Object[] and return as Iterable
    return () -> Arrays.stream(FacetField.FacetMethod.values())
      .map(it -> new Object[]{it}).iterator();
  }

  public TestJsonFacets(FacetField.FacetMethod defMethod) {
    FacetField.FacetMethod.DEFAULT_METHOD = defMethod; // note: the real default is restored in afterTests
  }

  // attempt to reproduce https://github.com/Heliosearch/heliosearch/issues/33
  @Test
  public void testComplex() throws Exception {
    Random r = random();

    Client client = Client.localClient;

    double price_low = 11000;
    double price_high = 100000;

    ModifiableSolrParams p = params("make_s","make_s", "model_s","model_s", "price_low",Double.toString(price_low), "price_high",Double.toString(price_high));


    MacroExpander m = new MacroExpander( p.getMap() );

    String make_s = m.expand("${make_s}");
    String model_s = m.expand("${model_s}");

    client.deleteByQuery("*:*", null);


    int nDocs = 99;
    String[] makes = {"honda", "toyota", "ford", null};
    Double[] prices = {10000.0, 30000.0, 50000.0, 0.0, null};
    String[] honda_models = {"accord", "civic", "fit", "pilot", null};  // make sure this is alphabetized to match tiebreaks in index
    String[] other_models = {"z1", "z2", "z3", "z4", "z5", "z6", null};

    int nHonda = 0;
    final int[] honda_model_counts = new int[honda_models.length];

    for (int i=0; i<nDocs; i++) {
      SolrInputDocument doc = sdoc("id", Integer.toString(i));

      Double price = rand(prices);
      if (price != null) {
        doc.addField("cost_f", price);
      }
      boolean matches_price = price!=null && price >= price_low && price <= price_high;

      String make = rand(makes);
      if (make != null) {
        doc.addField(make_s, make);
      }

      if ("honda".equals(make)) {
        int modelNum = r.nextInt(honda_models.length);
        String model = honda_models[modelNum];
        if (model != null) {
          doc.addField(model_s, model);
        }
        if (matches_price) {
          nHonda++;
          honda_model_counts[modelNum]++;
        }
      } else if (make == null) {
        doc.addField(model_s, rand(honda_models));  // add some docs w/ model but w/o make
      } else {
        // other makes
        doc.addField(model_s, rand(other_models));  // add some docs w/ model but w/o make
      }

      client.add(doc, null);
      if (r.nextInt(10) == 0) {
        client.add(doc, null);  // dup, causing a delete
      }
      if (r.nextInt(20) == 0) {
        client.commit();  // force new seg
      }
    }

    client.commit();

    // now figure out top counts
    List<Integer> idx = new ArrayList<>();
    for (int i=0; i<honda_model_counts.length-1; i++) {
      idx.add(i);
    }
    Collections.sort(idx, (o1, o2) -> {
      int cmp = honda_model_counts[o2] - honda_model_counts[o1];
      return cmp == 0 ? o1 - o2 : cmp;
    });



    // straight query facets
    client.testJQ(params(p, "q", "*:*", "rows","0", "fq","+${make_s}:honda +cost_f:[${price_low} TO ${price_high}]"
            , "json.facet", "{makes:{terms:{field:${make_s}, facet:{models:{terms:{field:${model_s}, limit:2, mincount:0}}}}"
                + "}}"
            , "facet","true", "facet.pivot","make_s,model_s", "facet.limit", "2"
        )
        , "facets=={count:" + nHonda + ", makes:{buckets:[{val:honda, count:" + nHonda + ", models:{buckets:["
           + "{val:" + honda_models[idx.get(0)] + ", count:" + honda_model_counts[idx.get(0)] + "},"
           + "{val:" + honda_models[idx.get(1)] + ", count:" + honda_model_counts[idx.get(1)] + "}]}"
           + "}]}}"
    );


  }


  public void indexSimple(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2",
        "num_is", "4", "num_is", "2",
        "val_b", "true", "sparse_s", "one"), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5",
        "num_is", "-9", "num_is", "-5",
        "val_b", "false"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ", "num_d", "2", "num_i", "3",
        "num_is", "2", "num_is", "3"), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7",
        "num_is", "11", "num_is", "7",
        "sparse_s", "two"),null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-5", "num_i", "-5",
        "num_is", "-5"),null);
    client.commit();
  }

  public void testMultiValuedBucketReHashing() throws Exception {
    Client client = Client.localClient();
    client.deleteByQuery("*:*", null);
    // we want a domain with a small number of documents, and more facet (point) values then docs so
    // that we force dvhash to increase the number of slots via resize...
    // (NOTE: normal resizing won't happen w/o at least 1024 slots, but test static overrides this to '2')
    client.add(sdoc("id", "1",
                    "f_sd", "qqq",
                    "f_ids", "4", "f_ids", "2", "f_ids", "999",
                    "x_ids", "3", "x_ids", "5", "x_ids", "7",
                    "z_ids", "42"), null);
    client.add(sdoc("id", "2",
                    "f_sd", "nnn",
                    "f_ids", "44", "f_ids", "22", "f_ids", "999",
                    "x_ids", "33", "x_ids", "55", "x_ids", "77",
                    "z_ids", "666"), null);
    client.add(sdoc("id", "3",
                    "f_sd", "ggg",
                    "f_ids", "444", "f_ids", "222", "f_ids", "999",
                    "x_ids", "333", "x_ids", "555", "x_ids", "777",
                    "z_ids", "1010101"), null);
    client.commit();

    // faceting on a multivalued point field sorting on a stat...
    assertJQ(req("rows", "0", "q", "id:[1 TO 2]", "json.facet"
                 , "{ f : { type: terms, field: f_ids, limit: 1, sort: 'x desc', "
                 + "        facet: { x : 'sum(x_ids)', z : 'min(z_ids)' } } }")
             , "response/numFound==2"
             , "facets/count==2"
             , "facets/f=={buckets:[{ val:999, count:2, x:180.0, z:42 }]}"
             );
  }

  public void testBehaviorEquivilenceOfUninvertibleFalse() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    // regardless of the facet method (parameterized via default at test class level)
    // faceting on an "uninvertible=false docValues=false" field is not supported.
    //
    // it should behave the same as any attempt (using any method) at faceting on
    // and "indexed=false docValues=false" field...
    for (String f : Arrays.asList("where_s_not_indexed_sS",
                                  "where_s_multi_not_uninvert",
                                  "where_s_single_not_uninvert")) {
      SolrQueryRequest request = req("rows", "0", "q", "num_i:[* TO 2]", "json.facet",
                                     "{x: {type:terms, field:'"+f+"'}}");
      if (FacetField.FacetMethod.DEFAULT_METHOD == FacetField.FacetMethod.DVHASH
          && !f.contains("multi")) {
        // DVHASH is (currently) weird...
        //
        // it's ignored for multi valued fields -- but for single valued fields, it explicitly
        // checks the *FieldInfos* on the reader to see if the DocVals type is ok.
        //
        // Which means that unlike most other facet method:xxx options, it fails hard if you try to use it
        // on a field where no docs have been indexed (yet).
        expectThrows(SolrException.class, () ->{
            assertJQ(request);
          });
        
      } else {
        // In most cases, we should just get no buckets back...
        assertJQ(request
                 , "response/numFound==3"
                 , "facets/count==3"
                 , "facets/x=={buckets:[]}"

                 );
      }
    }

    // regardless of the facet method (parameterized via default at test class level)
    // faceting on an "uninvertible=false docValues=true" field should work,
    //
    // it should behave equivilently to it's copyField source...
    for (String f : Arrays.asList("where_s",
                                  "where_s_multi_not_uninvert_dv",
                                  "where_s_single_not_uninvert_dv")) {
      assertJQ(req("rows", "0", "q", "num_i:[* TO 2]", "json.facet",
                   "{x: {type:terms, field:'"+f+"'}}")
               , "response/numFound==3"
               , "facets/count==3"
               , "facets/x=={buckets:[ {val:NY, count:2} , {val:NJ, count:1} ]}"
               );
    }
   
    // faceting on an "uninvertible=false docValues=false" field should be possible
    // when using method:enum w/sort:index
    //
    // it should behave equivilent to it's copyField source...
    for (String f : Arrays.asList("where_s",
                                  "where_s_multi_not_uninvert",
                                  "where_s_single_not_uninvert")) {
      assertJQ(req("rows", "0", "q", "num_i:[* TO 2]", "json.facet",
                                     "{x: {type:terms, sort:'index asc', method:enum, field:'"+f+"'}}")
               , "response/numFound==3"
               , "facets/count==3"
               , "facets/x=={buckets:[ {val:NJ, count:1} , {val:NY, count:2} ]}"
               );
    }
  }


  
  @Test
  public void testExplicitQueryDomain() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    { // simple 'query' domain
      
      // the facet buckets for all of the requests below should be identical
      // only the numFound & top level facet count should differ
      final String expectedFacets
        = "facets/w=={ buckets:["
        + "  { val:'NJ', count:2}, "
        + "  { val:'NY', count:1} ] }";
      
      assertJQ(req("rows", "0", "q", "cat_s:B", "json.facet",
                   "{w: {type:terms, field:'where_s'}}"),
               "response/numFound==3",
               "facets/count==3",
               expectedFacets);
      assertJQ(req("rows", "0", "q", "id:3", "json.facet",
                   "{w: {type:terms, field:'where_s', domain: { query:'cat_s:B' }}}"),
               "response/numFound==1",
               "facets/count==1",
               expectedFacets);
      assertJQ(req("rows", "0", "q", "*:*", "fq", "-*:*", "json.facet",
                   "{w: {type:terms, field:'where_s', domain: { query:'cat_s:B' }}}"),
               "response/numFound==0",
               "facets/count==0",
               expectedFacets);
      assertJQ(req("rows", "0", "q", "*:*", "fq", "-*:*", "domain_q", "cat_s:B", "json.facet",
                   "{w: {type:terms, field:'where_s', domain: { query:{param:domain_q} }}}"),
               "response/numFound==0",
               "facets/count==0",
               expectedFacets);
    }
    
    { // a nested explicit query domain

      // for all of the "top" buckets, the subfacet should have identical sub-buckets
      final String expectedSubBuckets = "{ buckets:[ { val:'B', count:3}, { val:'A', count:2} ] }";
      assertJQ(req("rows", "0", "q", "num_i:[0 TO *]", "json.facet",
                   "{w: {type:terms, field:'where_s', " + 
                   "     facet: { c: { type:terms, field:'cat_s', domain: { query:'*:*' }}}}}")
               , "facets/w=={ buckets:["
               + "  { val:'NJ', count:2, c: " + expectedSubBuckets + "}, "
               + "  { val:'NY', count:1, c: " + expectedSubBuckets + "} "
               + "] }"
               );
    }

    { // an (effectively) empty query should produce an error
      ignoreException("'query' domain can not be null");
      ignoreException("'query' domain must not evaluate to an empty list");
      for (String raw : Arrays.asList("null", "[ ]", "{param:bogus}")) {
        expectThrows(SolrException.class, () -> {
            assertJQ(req("rows", "0", "q", "num_i:[0 TO *]", "json.facet",
                         "{w: {type:terms, field:'where_s', " + 
                         "     facet: { c: { type:terms, field:'cat_s', domain: { query: "+raw+" }}}}}"));
          });
      }
    }
  }

  
  @Test
  public void testSimpleSKG() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    // using relatedness() as a top level stat, not nested under any facet
    // (not particularly useful, but shouldn't error either)
    assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                 "fore", "where_s:NY", "back", "*:*",
                 "json.facet", " { skg: 'relatedness($fore,$back)' }")
             , "facets=={"
             + "   count:5, "
             + "   skg : { relatedness: 0.00699,"
             + "           foreground_popularity: 0.33333,"
             + "           background_popularity: 0.83333,"
             + "   } }"
             );
    
    // simple single level facet w/skg stat & (re)sorting
    for (String sort : Arrays.asList("sort:'index asc'",
                                     "sort:'y desc'",
                                     "sort:'z desc'",
                                     "sort:'skg desc'",
                                     "prelim_sort:'count desc', sort:'index asc'",
                                     "prelim_sort:'count desc', sort:'y desc'",
                                     "prelim_sort:'count desc', sort:'z desc'",
                                     "prelim_sort:'count desc', sort:'skg desc'")) {
      // the relatedness score of each of our cat_s values is (conviniently) also alphabetical order,
      // (and the same order as 'sum(num_i) desc' & 'min(num_i) desc')
      //
      // So all of these re/sort options should produce identical output (since the num buckets is < limit)
      // - Testing "index" sort allows the randomized use of "stream" processor as default to be tested.
      // - Testing (re)sorts on other stats sanity checks code paths where relatedness() is a "defered" Agg
      for (String limit : Arrays.asList(", ", ", limit:5, ", ", limit:-1, ")) {
        // results shouldn't change regardless of our limit param"
        assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                     "fore", "where_s:NY", "back", "*:*",
                     "json.facet", ""
                     + "{x: { type: terms, field: 'cat_s', "+sort + limit
                     + "      facet: { skg: 'relatedness($fore,$back)', y:'sum(num_i)', z:'min(num_i)' } } }")
                 , "facets=={count:5, x:{ buckets:["
                 + "   { val:'A', count:2, y:5.0, z:2, "
                 + "     skg : { relatedness: 0.00554, "
                 //+ "             foreground_count: 1, "
                 //+ "             foreground_size: 2, "
                 //+ "             background_count: 2, "
                 //+ "             background_size: 6,"
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.33333, },"
                 + "   }, "
                 + "   { val:'B', count:3, y:-3.0, z:-5, "
                 + "     skg : { relatedness: 0.0, " // perfectly average and uncorrolated
                 //+ "             foreground_count: 1, "
                 //+ "             foreground_size: 2, "
                 //+ "             background_count: 3, "
                 //+ "             background_size: 6,"
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.5 },"
                 + "   } ] } } "
                 );
        // same query with a prefix of 'B' should produce only a single bucket with exact same results
        assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                     "fore", "where_s:NY", "back", "*:*",
                     "json.facet", ""
                     + "{x: { type: terms, field: 'cat_s', prefix:'B', "+sort + limit
                     + "      facet: { skg: 'relatedness($fore,$back)', y:'sum(num_i)', z:'min(num_i)' } } }")
                 , "facets=={count:5, x:{ buckets:["
                 + "   { val:'B', count:3, y:-3.0, z:-5, "
                 + "     skg : { relatedness: 0.0, " // perfectly average and uncorrolated
                 //+ "             foreground_count: 1, "
                 //+ "             foreground_size: 2, "
                 //+ "             background_count: 3, "
                 //+ "             background_size: 6,"
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.5 },"
                 + "   } ] } } "
                 );
      }
    }

    // relatedness shouldn't be computed for allBuckets, but it also shouldn't cause any problems
    for (String sort : Arrays.asList("sort:'y desc'",
                                     "sort:'z desc'",
                                     "sort:'skg desc'",
                                     "sort:'index asc'",
                                     "prelim_sort:'count desc', sort:'skg desc'")) {
      // the relatedness score of each of our cat_s values is (conveniently) also alphabetical order,
      // (and the same order as 'sum(num_i) desc' & 'min(num_i) desc')
      //
      // So all of these re/sort options should produce identical output (since the num buckets is < limit)
      // - Testing "index" sort allows the randomized use of "stream" processor as default to be tested.
      // - Testing (re)sorts on other stats sanity checks code paths where relatedness() is a "deferred" Agg
      for (String limit : Arrays.asList(", ", ", limit:5, ", ", limit:-1, ")) {
        // results shouldn't change regardless of our limit param"
        assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                     "fore", "where_s:NY", "back", "*:*",
                     "json.facet", ""
                     + "{x: { type: terms, field: 'cat_s', allBuckets:true, "+sort + limit
                     + "      facet: { skg: 'relatedness($fore,$back)', y:'sum(num_i)', z:'min(num_i)' } } }")
                 , "facets=={count:5, x:{ "
                 // 'skg' key must not exist in th allBuckets bucket
                 + "                      allBuckets: { count:5, y:2.0, z:-5 },"
                 + "buckets:["
                 + "   { val:'A', count:2, y:5.0, z:2, "
                 + "     skg : { relatedness: 0.00554, "
                 //+ "             foreground_count: 1, "
                 //+ "             foreground_size: 2, "
                 //+ "             background_count: 2, "
                 //+ "             background_size: 6,"
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.33333, },"
                 + "   }, "
                 + "   { val:'B', count:3, y:-3.0, z:-5, "
                 + "     skg : { relatedness: 0.0, " // perfectly average and uncorrelated
                 //+ "             foreground_count: 1, "
                 //+ "             foreground_size: 2, "
                 //+ "             background_count: 3, "
                 //+ "             background_size: 6,"
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.5 },"
                 + "   } ] } } "
                 );
        
        // really special case: allBuckets when there are no regular buckets...
        assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                     "fore", "where_s:NY", "back", "*:*",
                     "json.facet", ""
                     + "{x: { type: terms, field: 'bogus_field_s', allBuckets:true, "+sort + limit
                     + "      facet: { skg: 'relatedness($fore,$back)', y:'sum(num_i)', z:'min(num_i)' } } }")
                 , "facets=={count:5, x:{ "
                 // 'skg' key (as well as 'z' since it's a min) must not exist in the allBuckets bucket
                 + "                      allBuckets: { count:0, y:0.0 },"
                 + "buckets:[ ]"
                 + "   } } "
                 );

        
      }
    }

    
    // trivial sanity check that we can (re)sort on SKG after pre-sorting on count...
    // ...and it's only computed for the top N buckets (based on our pre-sort)
    for (int overrequest : Arrays.asList(0, 1, 42)) {
      // based on our counts & relatedness values, the blackbox output should be the same for both
      // overrequest values ... only DebugAgg stats should change...
      DebugAgg.Acc.collectDocs.set(0);
      DebugAgg.Acc.collectDocSets.set(0);
      
      assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                   "fore", "where_s:NJ", "back", "*:*",
                   "json.facet", ""
                   + "{x: { type: terms, field: 'cat_s', prelim_sort: 'count desc', sort:'skg desc', "
                   + "      limit: 1, overrequest: " + overrequest + ", "
                   + "      facet: { skg: 'debug(wrap,relatedness($fore,$back))' } } }")
               , "facets=={count:5, x:{ buckets:["
               + "   { val:'B', count:3, "
               + "     skg : { relatedness: 0.00638, " 
               //+ "             foreground_count: 2, "
               //+ "             foreground_size: 3, "
               //+ "             background_count: 3, "
               //+ "             background_size: 6,"
               + "             foreground_popularity: 0.33333,"
               + "             background_popularity: 0.5 },"
               + "   }, "
               + " ] } } "
               );
      // at most 2 buckets, regardless of overrequest...
      assertEqualsAndReset(0 < overrequest ? 2 : 1, DebugAgg.Acc.collectDocSets);
      assertEqualsAndReset(0, DebugAgg.Acc.collectDocs);
    }
      
    // SKG used in multiple nested facets
    //
    // we'll re-use these params in 2 requests, one will simulate a shard request
    final SolrParams nestedSKG = params
      ("q", "cat_s:[* TO *]", "rows", "0", "fore", "num_i:[-1000 TO 0]", "back", "*:*", "json.facet"
       , "{x: { type: terms, field: 'cat_s', sort: 'skg desc', "
       + "      facet: { skg: 'relatedness($fore,$back)', "
       + "               y:   { type: terms, field: 'where_s', sort: 'skg desc', "
       + "                      facet: { skg: 'relatedness($fore,$back)' } } } } }");
       
    // plain old request
    assertJQ(req(nestedSKG)
             , "facets=={count:5, x:{ buckets:["
             + "   { val:'B', count:3, "
             + "     skg : { relatedness: 0.01539, "
             //+ "             foreground_count: 2, "
             //+ "             foreground_size: 2, "
             //+ "             background_count: 3, "
             //+ "             background_size: 6, "
             + "             foreground_popularity: 0.33333,"
             + "             background_popularity: 0.5 },"
             + "     y : { buckets:["
             + "            {  val:'NY', count: 1, "
             + "               skg : { relatedness: 0.00554, " 
             //+ "                       foreground_count: 1, "
             //+ "                       foreground_size: 2, "
             //+ "                       background_count: 2, "
             //+ "                       background_size: 6, "
             + "                       foreground_popularity: 0.16667, "
             + "                       background_popularity: 0.33333, "
             + "            } }, "
             + "            {  val:'NJ', count: 2, "
             + "               skg : { relatedness: 0.0, " // perfectly average and uncorrolated
             //+ "                       foreground_count: 1, "
             //+ "                       foreground_size: 2, "
             //+ "                       background_count: 3, "
             //+ "                       background_size: 6, "
             + "                       foreground_popularity: 0.16667, "
             + "                       background_popularity: 0.5, "
             + "            } }, "
             + "     ] } "
             + "   }, "
             + "   { val:'A', count:2, "
             + "     skg : { relatedness:-0.01097, "
             //+ "             foreground_count: 0, "
             //+ "             foreground_size: 2, "
             //+ "             background_count: 2, "
             //+ "             background_size: 6,"
             + "             foreground_popularity: 0.0,"
             + "             background_popularity: 0.33333 },"
             + "     y : { buckets:["
             + "            {  val:'NJ', count: 1, "
             + "               skg : { relatedness: 0.0, " // perfectly average and uncorrolated
             //+ "                       foreground_count: 0, "
             //+ "                       foreground_size: 0, "
             //+ "                       background_count: 3, "
             //+ "                       background_size: 6, "
             + "                       foreground_popularity: 0.0, "
             + "                       background_popularity: 0.5, "
             + "            } }, "
             + "            {  val:'NY', count: 1, "
             + "               skg : { relatedness: 0.0, " // perfectly average and uncorrolated
             //+ "                       foreground_count: 0, "
             //+ "                       foreground_size: 0, "
             //+ "                       background_count: 2, "
             //+ "                       background_size: 6, "
             + "                       foreground_popularity: 0.0, "
             + "                       background_popularity: 0.33333, "
             + "            } }, "
             + "   ] } } ] } } ");

    // same request, but with whitebox params testing isShard
    // to verify the raw counts/sizes
    assertJQ(req(nestedSKG,
                 // fake an initial shard request
                 "distrib", "false", "isShard", "true", "_facet_", "{}",
                 "shards.purpose", ""+FacetModule.PURPOSE_GET_JSON_FACETS)
             , "facets=={count:5, x:{ buckets:["
             + "   { val:'B', count:3, "
             + "     skg : { "
             + "             foreground_count: 2, "
             + "             foreground_size: 2, "
             + "             background_count: 3, "
             + "             background_size: 6 }, "
             + "     y : { buckets:["
             + "            {  val:'NY', count: 1, "
             + "               skg : { " 
             + "                       foreground_count: 1, "
             + "                       foreground_size: 2, "
             + "                       background_count: 2, "
             + "                       background_size: 6, "
             + "            } }, "
             + "            {  val:'NJ', count: 2, "
             + "               skg : { " 
             + "                       foreground_count: 1, "
             + "                       foreground_size: 2, "
             + "                       background_count: 3, "
             + "                       background_size: 6, "
             + "            } }, "
             + "     ] } "
             + "   }, "
             + "   { val:'A', count:2, "
             + "     skg : { " 
             + "             foreground_count: 0, "
             + "             foreground_size: 2, "
             + "             background_count: 2, "
             + "             background_size: 6 },"
             + "     y : { buckets:["
             + "            {  val:'NJ', count: 1, "
             + "               skg : { " 
             + "                       foreground_count: 0, "
             + "                       foreground_size: 0, "
             + "                       background_count: 3, "
             + "                       background_size: 6, "
             + "            } }, "
             + "            {  val:'NY', count: 1, "
             + "               skg : { " 
             + "                       foreground_count: 0, "
             + "                       foreground_size: 0, "
             + "                       background_count: 2, "
             + "                       background_size: 6, "
             + "            } }, "
             + "   ] } } ] } } ");

    
    // SKG w/min_pop (NOTE: incredibly contrived and not-useful fore/back for testing min_pop w/shard sorting)
    //
    // we'll re-use these params in 2 requests, one will simulate a shard request
    final SolrParams minPopSKG = params
      ("q", "cat_s:[* TO *]", "rows", "0", "fore", "num_i:[0 TO 1000]", "back", "cat_s:B", "json.facet"
       , "{x: { type: terms, field: 'cat_s', sort: 'skg desc', "
       + "      facet: { skg: { type:func, func:'relatedness($fore,$back)', "
       + "                      min_popularity: 0.001 }" 
       + "             } } }");

    // plain old request
    assertJQ(req(minPopSKG)
             , "facets=={count:5, x:{ buckets:["
             + "   { val:'B', count:3, "
             + "     skg : { relatedness: -1.0, "
             //+ "             foreground_count: 1, "
             //+ "             foreground_size: 3, "
             //+ "             background_count: 3, "
             //+ "             background_size: 3, "
             + "             foreground_popularity: 0.33333," 
             + "             background_popularity: 1.0," 
             + "   } }, "
             + "   { val:'A', count:2, "
             + "     skg : { relatedness:'-Infinity', " // bg_pop is below min_pop (otherwise 1.0)
             //+ "             foreground_count: 2, "
             //+ "             foreground_size: 3, "
             //+ "             background_count: 0, "
             //+ "             background_size: 3, "
             + "             foreground_popularity: 0.66667,"
             + "             background_popularity: 0.0,"
             + "   } } ] } } ");

    // same request, but with whitebox params testing isShard
    // to verify the raw counts/sizes and that per-shard sorting doesn't pre-emptively sort "A" to the bottom
    assertJQ(req(minPopSKG,
                 // fake an initial shard request
                 "distrib", "false", "isShard", "true", "_facet_", "{}",
                 "shards.purpose", ""+FacetModule.PURPOSE_GET_JSON_FACETS)
             , "facets=={count:5, x:{ buckets:["
             + "   { val:'A', count:2, "
             + "     skg : { " 
             + "             foreground_count: 2, "
             + "             foreground_size: 3, "
             + "             background_count: 0, "
             + "             background_size: 3, "
             + "   } }, "
             + "   { val:'B', count:3, "
             + "     skg : { "
             + "             foreground_count: 1, "
             + "             foreground_size: 3, "
             + "             background_count: 3, "
             + "             background_size: 3, "
             + "   } } ] } }");
  }

  @Test
  public void testSKGSweepMultiAcc() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);
    
    // simple single level facet w/skg & trivial non-sweeping stat using various sorts & (re)sorting
    for (String sort : Arrays.asList("sort:'index asc'",
                                     "sort:'y desc'",
                                     "sort:'z desc'",
                                     "sort:'skg desc'",
                                     "prelim_sort:'count desc', sort:'index asc'",
                                     "prelim_sort:'count desc', sort:'y desc'",
                                     "prelim_sort:'count desc', sort:'z desc'",
                                     "prelim_sort:'count desc', sort:'skg desc'")) {
      // the relatedness score of each of our cat_s values is (conviniently) also alphabetical order,
      // (and the same order as 'sum(num_i) desc' & 'min(num_i) desc')
      //
      // So all of these re/sort options should produce identical output
      // - Testing "index" sort allows the randomized use of "stream" processor as default to be tested.
      // - Testing (re)sorts on other stats sanity checks code paths where relatedness() is a "defered" Agg

      for (String sweep : Arrays.asList("true", "false")) {
        //  results should be the same even if we disable sweeping...
        assertJQ(req("q", "cat_s:[* TO *]", "rows", "0",
                     "fore", "where_s:NY", "back", "*:*",
                     "json.facet", ""
                     + "{x: { type: terms, field: 'cat_s', "+sort+", limit:-1, "
                     + "      facet: { skg: { type: 'func', func:'relatedness($fore,$back)', "
                     +"                       "+RelatednessAgg.SWEEP_COLLECTION+": "+sweep+" },"
                     + "               y:'sum(num_i)', "
                     +"                z:'min(num_i)' } } }")
                 , "facets=={count:5, x:{ buckets:["
                 + "   { val:'A', count:2, y:5.0, z:2, "
                 + "     skg : { relatedness: 0.00554, "
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.33333, },"
                 + "   }, "
                 + "   { val:'B', count:3, y:-3.0, z:-5, "
                 + "     skg : { relatedness: 0.0, " // perfectly average and uncorrolated
                 + "             foreground_popularity: 0.16667,"
                 + "             background_popularity: 0.5 },"
                 + "   } ] } } "
                 );
      }
    }
  }

  
  @Test
  public void testRepeatedNumerics() throws Exception {
    Client client = Client.localClient();
    String field = "num_is"; // docValues of multi-valued points field can contain duplicate values... make sure they don't mess up our counts.
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2", "val_b", "true", "sparse_s", "one", field,"0", field,"0"), null);
    client.commit();

    client.testJQ(params("q", "id:1", "field", field
        , "json.facet", "{" +
            "f1:{terms:${field}}" +
            ",f2:'hll(${field})'" +
            ",f3:{type:range, field:${field}, start:0, end:1, gap:1}" +
            "}"
        )
        , "facets=={count:1, " +
            "f1:{buckets:[{val:0, count:1}]}" +
            ",f2:1" +
            ",f3:{buckets:[{val:0, count:1}]}" +
            "}"
    );
  }

  public void testDomainJoinSelf() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    // self join domain switch at the second level of faceting
    assertJQ(req("q", "*:*", "rows", "0",
                 "json.facet", ""
                 + "{x: { type: terms, field: 'num_i', "
                 + "      facet: { y: { domain: { join: { from: 'cat_s', to: 'cat_s' } }, "
                 + "                    type: terms, field: 'where_s' "
                 + "                  } } } }")
             , "facets=={count:6, x:{ buckets:["
             + "   { val:-5, count:2, "
             + "     y : { buckets:[{ val:'NJ', count:2 }, { val:'NY', count:1 } ] } }, "
             + "   { val:2, count:1, "
             + "     y : { buckets:[{ val:'NJ', count:1 }, { val:'NY', count:1 } ] } }, "
             + "   { val:3, count:1, "
             + "     y : { buckets:[{ val:'NJ', count:1 }, { val:'NY', count:1 } ] } }, "
             + "   { val:7, count:1, "
             + "     y : { buckets:[{ val:'NJ', count:2 }, { val:'NY', count:1 } ] } } ] } }"
             );
  }
  
  public void testDomainGraph() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    // should be the same as join self
    assertJQ(req("q", "*:*", "rows", "0",
        "json.facet", ""
            + "{x: { type: terms, field: 'num_i', "
            + "      facet: { y: { domain: { graph: { from: 'cat_s', to: 'cat_s' } }, "
            + "                    type: terms, field: 'where_s' "
            + "                  } } } }")
        , "facets=={count:6, x:{ buckets:["
            + "   { val:-5, count:2, "
            + "     y : { buckets:[{ val:'NJ', count:2 }, { val:'NY', count:1 } ] } }, "
            + "   { val:2, count:1, "
            + "     y : { buckets:[{ val:'NJ', count:1 }, { val:'NY', count:1 } ] } }, "
            + "   { val:3, count:1, "
            + "     y : { buckets:[{ val:'NJ', count:1 }, { val:'NY', count:1 } ] } }, "
            + "   { val:7, count:1, "
            + "     y : { buckets:[{ val:'NJ', count:2 }, { val:'NY', count:1 } ] } } ] } }"
    );

    // This time, test with a traversalFilter
    // should be the same as join self
    assertJQ(req("q", "*:*", "rows", "0",
        "json.facet", ""
            + "{x: { type: terms, field: 'num_i', "
            + "      facet: { y: { domain: { graph: { from: 'cat_s', to: 'cat_s', traversalFilter: 'where_s:NY' } }, "
            + "                    type: terms, field: 'where_s' "
            + "                  } } } }")
        , "facets=={count:6, x:{ buckets:["
            + "   { val:-5, count:2, "
            + "     y : { buckets:[{ val:'NJ', count:1 }, { val:'NY', count:1 } ] } }, "
            + "   { val:2, count:1, "
            + "     y : { buckets:[{ val:'NY', count:1 } ] } }, "
            + "   { val:3, count:1, "
            + "     y : { buckets:[{ val:'NJ', count:1 }, { val:'NY', count:1 } ] } }, "
            + "   { val:7, count:1, "
            + "     y : { buckets:[{ val:'NJ', count:1  }, { val:'NY', count:1 } ] } } ] } }"
    );
  }


  public void testNestedJoinDomain() throws Exception {
    Client client = Client.localClient();

    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "1_s", "A", "2_s", "A", "3_s", "C", "y_s", "B", "x_t", "x   z", "z_t", "  2 3"), null);
    client.add(sdoc("id", "2", "1_s", "B", "2_s", "A", "3_s", "B", "y_s", "B", "x_t", "x y  ", "z_t", "1   3"), null);
    client.add(sdoc("id", "3", "1_s", "C", "2_s", "A", "3_s", "#", "y_s", "A", "x_t", "  y z", "z_t", "1 2  "), null);
    client.add(sdoc("id", "4", "1_s", "A", "2_s", "B", "3_s", "C", "y_s", "A", "x_t", "    z", "z_t", "    3"), null);
    client.add(sdoc("id", "5", "1_s", "B", "2_s", "_", "3_s", "B", "y_s", "C", "x_t", "x    ", "z_t", "1   3"), null);
    client.add(sdoc("id", "6", "1_s", "C", "2_s", "B", "3_s", "A", "y_s", "C", "x_t", "x y z", "z_t", "1    "), null);
    client.commit();

    assertJQ(req("q", "x_t:x", "rows", "0", // NOTE q - only x=x in base set (1,2,5,6)
                 "json.facet", ""
                 + "{x: { type: terms, field: 'x_t', "
                 + "      domain: { join: { from:'1_s', to:'2_s' } },"
                 //                y1 & y2 are the same facet, with *similar* child facet z1/z2 ...
                 + "      facet: { y1: { type: terms, field: 'y_s', "
                 //                               z1 & z2 are same field, diff join...
                 + "                     facet: { z1: { type: terms, field: 'z_t', "
                 + "                                    domain: { join: { from:'2_s', to:'3_s' } } } } },"
                 + "               y2: { type: terms, field: 'y_s', "
                 //                               z1 & z2 are same field, diff join...
                 + "                     facet: { z2: { type: terms, field: 'z_t', "
                 + "                                    domain: { join: { from:'3_s', to:'1_s' } } } } } } } }")
             , "facets=={count:4, "
             + "x:{ buckets:[" // joined 1->2: doc5 drops out, counts: z=4, x=3, y=3 
             + "   { val:z, count:4, " // x=z (docs 1,3,4,6) y terms: A=2, B=1, C=1
             + "     y1 : { buckets:[ " // z1 joins 2->3...
             + "             { val:A, count:2, " // A in docs(3,4), joins (A,B) -> docs(2,5,6)
             + "               z1: { buckets:[{ val:'1', count:3 }, { val:'3', count:2 }] } }, "
             + "             { val:B, count:1, " // B in doc1, joins A -> doc6
             + "               z1: { buckets:[{ val:'1', count:1 }] } }, "
             + "             { val:C, count:1, " // C in doc6, joins B -> docs(2,5)
             + "               z1: { buckets:[{ val:'1', count:2 }, { val:'3', count:2 }] } } "
             + "          ] }, "
             + "     y2 : { buckets:[ " // z2 joins 3->1...
             + "             { val:A, count:2, " // A in docs(3,4), joins C -> docs(3,6)
             + "               z2: { buckets:[{ val:'1', count:2 }, { val:'2', count:1 }] } }, "
             + "             { val:B, count:1, " // B in doc1, joins C -> docs(3,6)
             + "               z2: { buckets:[{ val:'1', count:2 }, { val:'2', count:1 }] } }, "
             + "             { val:C, count:1, " // C in doc6, joins A -> docs(1,4)
             + "               z2: { buckets:[{ val:'3', count:2 }, { val:'2', count:1 }] } } "
             + "          ] } }, "
             + "   { val:x, count:3, " // x=x (docs 1,2,!5,6) y terms: B=2, C=1 
             + "     y1 : { buckets:[ " // z1 joins 2->3...
             + "             { val:B, count:2, " // B in docs(1,2), joins A -> doc6
             + "               z1: { buckets:[{ val:'1', count:1 }] } }, "
             + "             { val:C, count:1, " // C in doc6, joins B -> docs(2,5)
             + "               z1: { buckets:[{ val:'1', count:2 }, { val:'3', count:2 }] } } "
             + "          ] }, "
             + "     y2 : { buckets:[ " // z2 joins 3->1...
             + "             { val:B, count:2, " // B in docs(1,2), joins C,B -> docs(2,3,5,6)
             + "               z2: { buckets:[{ val:'1', count:4 }, { val:'3', count:2 }, { val:'2', count:1 }] } }, "
             + "             { val:C, count:1, " // C in doc6, joins A -> docs(1,4)
             + "               z2: { buckets:[{ val:'3', count:2 }, { val:'2', count:1 }] } } "
             + "          ] } }, "
             + "   { val:y, count:3, " // x=y (docs 2,3,6) y terms: A=1, B=1, C=1 
             + "     y1 : { buckets:[ " // z1 joins 2->3...
             + "             { val:A, count:1, " // A in doc3, joins A -> doc6
             + "               z1: { buckets:[{ val:'1', count:1 }] } }, "
             + "             { val:B, count:1, " // B in doc2, joins A -> doc6
             + "               z1: { buckets:[{ val:'1', count:1 }] } }, "
             + "             { val:C, count:1, " // C in doc6, joins B -> docs(2,5)
             + "               z1: { buckets:[{ val:'1', count:2 }, { val:'3', count:2 }] } } "
             + "          ] }, "
             + "     y2 : { buckets:[ " // z2 joins 3->1...
             + "             { val:A, count:1, " // A in doc3, joins # -> empty set
             + "               z2: { buckets:[ ] } }, "
             + "             { val:B, count:1, " // B in doc2, joins B -> docs(2,5)
             + "               z2: { buckets:[{ val:'1', count:2 }, { val:'3', count:2 }] } }, "
             + "             { val:C, count:1, " // C in doc6, joins A -> docs(1,4)
             + "               z2: { buckets:[{ val:'3', count:2 }, { val:'2', count:1 }] } } "
             + "          ]}  }"
             + "   ]}}"
             );
  }

  
  @Test
  public void testMethodStream() throws Exception {
    Client client = Client.localClient();
    indexSimple(client);

    assertJQ(req("q", "*:*", "rows", "0", "json.facet", "{x:'sum(num_is)'}")
        , "facets=={count:6 , x:,10.0}"
    );
    assertJQ(req("q", "*:*", "rows", "0", "json.facet", "{x:'min(num_is)'}")
        , "facets=={count:6 , x:,-9}"
    );

    // test multiple json.facet commands
    assertJQ(req("q", "*:*", "rows", "0"
        , "json.facet", "{x:'sum(num_d)'}"
        , "json.facet", "{y:'min(num_d)'}"
        , "json.facet", "{z:'min(num_is)'}"
        )
        , "facets=={count:6 , x:3.0, y:-9.0, z:-9 }"
    );


    // test streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream }}" + // won't stream; need sort:index asc
            ", cat2:{terms:{field:'cat_s', method:stream, sort:'index asc' }}" +
            ", cat3:{terms:{field:'cat_s', method:stream, sort:'index asc', mincount:3 }}" + // mincount
            ", cat4:{terms:{field:'cat_s', method:stream, sort:'index asc', prefix:B }}" + // prefix
            ", cat5:{terms:{field:'cat_s', method:stream, sort:'index asc', offset:1 }}" + // offset
            ", cat6:{terms:{field:'cat_s', method:stream, sort:'index asc', missing:true }}" + // missing
            ", cat7:{terms:{field:'cat_s', method:stream, sort:'index asc', numBuckets:true }}" + // numBuckets
            ", cat8:{terms:{field:'cat_s', method:stream, sort:'index asc', allBuckets:true }}" + // allBuckets
            " }"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:B, count:3},{val:A, count:2}]}" +
            ", cat2:{buckets:[{val:A, count:2},{val:B, count:3}]}" +
            ", cat3:{buckets:[{val:B, count:3}]}" +
            ", cat4:{buckets:[{val:B, count:3}]}" +
            ", cat5:{buckets:[{val:B, count:3}]}" +
            ", cat6:{missing:{count:1}, buckets:[{val:A, count:2},{val:B, count:3}]}" +
            ", cat7:{numBuckets:2, buckets:[{val:A, count:2},{val:B, count:3}]}" +
            ", cat8:{allBuckets:{count:5}, buckets:[{val:A, count:2},{val:B, count:3}]}" +
            " }"
    );


    // test nested streaming under non-streaming
    assertJQ(req("q", "*:*", "rows", "0"
        , "json.facet", "{   cat:{terms:{field:'cat_s', sort:'index asc', facet:{where:{terms:{field:where_s,method:stream,sort:'index asc'}}}   }}}"
        )
        , "facets=={count:6 " +
        ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1},{val:NY,count:1}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2},{val:NY,count:1}]}    }]}"
        + "}"
    );

    // test nested streaming under streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream,sort:'index asc', facet:{where:{terms:{field:where_s,method:stream,sort:'index asc'}}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1},{val:NY,count:1}]}   },{val:B, count:3, where:{buckets:[{val:NJ,count:2},{val:NY,count:1}]}    }]}"
            + "}"
    );

    // test nested streaming with stats under streaming
    assertJQ(req("q", "*:*", "rows", "0"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream,sort:'index asc', facet:{  where:{terms:{field:where_s,method:stream,sort:'index asc',sort:'index asc', facet:{x:'max(num_d)', y:'sum(num_is)'}     }}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, where:{buckets:[{val:NJ,count:1,x:2.0,y:5.0},{val:NY,count:1,x:4.0,y:6.0}]}   }," +
            "{val:B, count:3, where:{buckets:[{val:NJ,count:2,x:11.0,y:4.0},{val:NY,count:1,x:-5.0,y:-5.0}]}    }]}"
            + "}"
    );

    // test nested streaming with stats under streaming with stats
    assertJQ(req("q", "*:*", "rows", "0",
            "facet","true"
            , "json.facet", "{   cat:{terms:{field:'cat_s', method:stream,sort:'index asc', facet:{ y:'min(num_d)',  where:{terms:{field:where_s,method:stream,sort:'index asc', facet:{x:'max(num_d)'}     }}}   }}}"
        )
        , "facets=={count:6 " +
            ", cat :{buckets:[{val:A, count:2, y:2.0, where:{buckets:[{val:NJ,count:1,x:2.0},{val:NY,count:1,x:4.0}]}   },{val:B, count:3, y:-9.0, where:{buckets:[{val:NJ,count:2,x:11.0},{val:NY,count:1,x:-5.0}]}    }]}"
            + "}"
    );


    assertJQ(req("q", "*:*", "fq","cat_s:A")
        , "response/numFound==2"
    );
  }

  Map<String,String[]> suffixMap = new HashMap<>();
  {
    suffixMap.put("_s", new String[]{"_s","_ss","_sd","_sds"} );
    suffixMap.put("_ss", new String[]{"_ss","_sds"} );
    suffixMap.put("_l", new String[]{"_l","_ls","_ld","_lds"} );
    suffixMap.put("_ls", new String[]{"_ls","_lds"} );
    suffixMap.put("_i", new String[]{"_i","_is","_id","_ids", "_l","_ls","_ld","_lds"} );
    suffixMap.put("_is", new String[]{"_is","_ids", "_ls","_lds"} );
    suffixMap.put("_d", new String[]{"_d","_ds","_dd","_dds"} );
    suffixMap.put("_ds", new String[]{"_ds","_dds"} );
    suffixMap.put("_f", new String[]{"_f","_fs","_fd","_fds", "_d","_ds","_dd","_dds"} );
    suffixMap.put("_fs", new String[]{"_fs","_fds","_ds","_dds"} );
    suffixMap.put("_dt", new String[]{"_dt","_dts","_dtd","_dtds"} );
    suffixMap.put("_dts", new String[]{"_dts","_dtds"} );
    suffixMap.put("_b", new String[]{"_b"} );
  }

  List<String> getAlternatives(String field) {
    int idx = field.lastIndexOf("_");
    if (idx<=0 || idx>=field.length()) return Collections.singletonList(field);
    String suffix = field.substring(idx);
    String[] alternativeSuffixes = suffixMap.get(suffix);
    if (alternativeSuffixes == null) return Collections.singletonList(field);
    String base = field.substring(0, idx);
    List<String> out = new ArrayList<>(alternativeSuffixes.length);
    for (String altS : alternativeSuffixes) {
      out.add( base + altS );
    }
    Collections.shuffle(out, random());
    return out;
  }

  @Test
  public void testStats() throws Exception {
    doStats(Client.localClient, params("debugQuery", Boolean.toString(random().nextBoolean()) ));
  }

  @Test
  public void testStatsDistrib() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()) );
    doStats( client, params() );
  }

  public void doStats(Client client, ModifiableSolrParams p) throws Exception {
    Map<String, List<String>> fieldLists = new HashMap<>();
    fieldLists.put("noexist", getAlternatives("noexist_s"));
    fieldLists.put("cat_s", getAlternatives("cat_s"));
    fieldLists.put("where_s", getAlternatives("where_s"));
    fieldLists.put("num_d", getAlternatives("num_f")); // num_d name is historical, which is why we map it to num_f alternatives so we can include floats as well
    fieldLists.put("num_i", getAlternatives("num_i"));
    fieldLists.put("super_s", getAlternatives("super_s"));
    fieldLists.put("val_b", getAlternatives("val_b"));
    fieldLists.put("date", getAlternatives("date_dt"));
    fieldLists.put("sparse_s", getAlternatives("sparse_s"));
    fieldLists.put("multi_ss", getAlternatives("multi_ss"));

    int maxAlt = 0;
    for (List<String> fieldList : fieldLists.values()) {
      maxAlt = Math.max(fieldList.size(), maxAlt);
    }

    // take the field with the maximum number of alternative types and loop through our variants that many times
    for (int i=0; i<maxAlt; i++) {
      ModifiableSolrParams args = params(p);
      for (String field : fieldLists.keySet()) {
        List<String> alts = fieldLists.get(field);
        String alt = alts.get( i % alts.size() );
        args.add(field, alt);
      }

      args.set("rows","0");
      // doStatsTemplated(client, args);
    }


    // single valued strings
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_s",  "cat_s","cat_s", "where_s","where_s", "num_d","num_d", "num_i","num_i", "num_l","long_l", "super_s","super_s", "val_b","val_b", "date","date_dt", "sparse_s","sparse_s"    ,"multi_ss","multi_ss") );

    // multi-valued strings, long/float substitute for int/double
    doStatsTemplated(client, params(p, "facet","true",       "rows","0", "noexist","noexist_ss", "cat_s","cat_ss", "where_s","where_ss", "num_d","num_f", "num_i","num_l", "num_l","long_l", "num_is","num_ls", "num_fs", "num_ds", "super_s","super_ss", "val_b","val_b", "date","date_dt", "sparse_s","sparse_ss", "multi_ss","multi_ss") );

    // multi-valued strings, method=dv for terms facets
    doStatsTemplated(client, params(p, "terms_method", "method:dv,", "rows", "0", "noexist", "noexist_ss", "cat_s", "cat_ss", "where_s", "where_ss", "num_d", "num_f", "num_i", "num_l", "num_l","long_l","super_s", "super_ss", "val_b", "val_b", "date", "date_dt", "sparse_s", "sparse_ss", "multi_ss", "multi_ss"));

    // single valued docvalues for strings, and single valued numeric doc values for numeric fields
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_sd",  "cat_s","cat_sd", "where_s","where_sd", "num_d","num_dd", "num_i","num_id", "num_is","num_lds", "num_l","long_ld", "num_fs","num_dds", "super_s","super_sd", "val_b","val_b", "date","date_dtd", "sparse_s","sparse_sd"    ,"multi_ss","multi_sds") );

    // multi-valued docvalues
    FacetFieldProcessorByArrayDV.unwrap_singleValued_multiDv = false;  // better multi-valued coverage
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_sds",  "cat_s","cat_sds", "where_s","where_sds", "num_d","num_d", "num_i","num_i", "num_is","num_ids", "num_l","long_ld", "num_fs","num_fds",    "super_s","super_sds", "val_b","val_b", "date","date_dtds", "sparse_s","sparse_sds"    ,"multi_ss","multi_sds") );

    // multi-valued docvalues
    FacetFieldProcessorByArrayDV.unwrap_singleValued_multiDv = true;
    doStatsTemplated(client, params(p,                "rows","0", "noexist","noexist_sds",  "cat_s","cat_sds", "where_s","where_sds", "num_d","num_d", "num_i","num_i", "num_is","num_ids", "num_l","long_ld", "num_fs","num_fds",   "super_s","super_sds", "val_b","val_b", "date","date_dtds", "sparse_s","sparse_sds"    ,"multi_ss","multi_sds") );
  }

  public static void doStatsTemplated(Client client, ModifiableSolrParams p) throws Exception {
    p.set("Z_num_i", "Z_" + p.get("num_i") );
    p.set("Z_num_l", "Z_" + p.get("num_l") );
    p.set("sparse_num_d", "sparse_" + p.get("num_d") );
    if (p.get("num_is") == null) p.add("num_is","num_is");
    if (p.get("num_fs") == null) p.add("num_fs","num_fs");

    String terms = p.get("terms");
    if (terms == null) terms="";
    int limit=0;
    switch (random().nextInt(4)) {
      case 0: limit=-1; break;
      case 1: limit=1000000; break;
      case 2: // fallthrough
      case 3: // fallthrough
    }
    if (limit != 0) {
      terms=terms+"limit:"+limit+",";
    }
    String terms_method = p.get("terms_method");
    if (terms_method != null) {
      terms=terms+terms_method;
    }
    String refine_method = p.get("refine_method");
    if (refine_method == null && random().nextBoolean()) {
      refine_method = "refine:true,";
    }
    if (refine_method != null) terms = terms + refine_method;

    p.set("terms", terms);
    // "${terms}" should be put at the beginning of generic terms facets.
    // It may specify "method=..." or "limit:-1", so should not be used if the facet explicitly specifies.

    MacroExpander m = new MacroExpander( p.getMap() );

    String cat_s = m.expand("${cat_s}");
    String where_s = m.expand("${where_s}");
    String num_d = m.expand("${num_d}");
    String num_i = m.expand("${num_i}");
    String num_is = m.expand("${num_is}");
    String num_fs = m.expand("${num_fs}");
    String Z_num_i = m.expand("${Z_num_i}");
    String Z_num_l = m.expand("${Z_num_l}");
    String val_b = m.expand("${val_b}");
    String date = m.expand("${date}");
    String super_s = m.expand("${super_s}");
    String sparse_s = m.expand("${sparse_s}");
    String multi_ss = m.expand("${multi_ss}");
    String sparse_num_d = m.expand("${sparse_num_d}");


    client.deleteByQuery("*:*", null);

    Client iclient = client;

    /*** This code was not needed yet, but may be needed if we want to force empty shard results more often.
    // create a new indexing client that doesn't use one shard to better test for empty or non-existent results
    if (!client.local()) {
      List<SolrClient> shards = client.getClientProvider().all();
      iclient = new Client(shards.subList(0, shards.size()-1), client.getClientProvider().getSeed());
     }
     ***/

    SolrInputDocument doc =
               sdoc("id", "1", cat_s, "A", where_s, "NY", num_d, "4", sparse_num_d, "6", num_i, "2",   num_is,"2",num_is,"-5", num_fs,"2",num_fs,"-5",   super_s, "zodiac", date, "2001-01-01T01:01:01Z", val_b, "true", sparse_s, "one");
    iclient.add(doc, null);
    iclient.add(doc, null);
    iclient.add(doc, null);  // a couple of deleted docs
    iclient.add(sdoc("id", "2", cat_s, "B", where_s, "NJ", num_d, "-9",                  num_i, "-5", num_is,"3",num_is,"-1", num_fs,"3",num_fs,"-1.5", super_s,"superman", date,"2002-02-02T02:02:02Z", val_b, "false"         , multi_ss,"a", multi_ss,"b" , Z_num_i, "0", Z_num_l,"0"), null);
    iclient.add(sdoc("id", "3"), null);
    iclient.commit();
    iclient.add(sdoc("id", "4", cat_s, "A", where_s, "NJ", num_d, "2", sparse_num_d,"-4",num_i, "3",   num_is,"0",num_is,"3", num_fs,"0", num_fs,"3",   super_s,"spiderman", date,"2003-03-03T03:03:03Z"                         , multi_ss, "b", Z_num_i, ""+Integer.MIN_VALUE, Z_num_l,Long.MIN_VALUE), null);
    iclient.add(sdoc("id", "5", cat_s, "B", where_s, "NJ", num_d, "11",                  num_i, "7",  num_is,"0",            num_fs,"0",               super_s,"batman"   , date,"2001-02-03T01:02:03Z"          ,sparse_s,"two", multi_ss, "a"), null);
    iclient.commit();
    iclient.add(sdoc("id", "6", cat_s, "B", where_s, "NY", num_d, "-5",                  num_i, "-5", num_is,"-1",           num_fs,"-1.5",            super_s,"hulk"     , date,"2002-03-01T03:02:01Z"                         , multi_ss, "b", multi_ss, "a", Z_num_i, ""+Integer.MAX_VALUE, Z_num_l,Long.MAX_VALUE), null);
    iclient.commit();
    client.commit();


    // test for presence of debugging info
    ModifiableSolrParams debugP = params(p);
    debugP.set("debugQuery","true");
    client.testJQ(params(debugP, "q", "*:*"
          , "json.facet", "{catA:{query:{q:'${cat_s}:A'}},  catA2:{query:{query:'${cat_s}:A'}},  catA3:{query:'${cat_s}:A'}    }"
      )
        , "facets=={ 'count':6, 'catA':{ 'count':2}, 'catA2':{ 'count':2}, 'catA3':{ 'count':2}}"
        , "debug/facet-trace=="  // just test for presence, not exact structure / values
    );


    // straight query facets
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{catA:{query:{q:'${cat_s}:A'}},  catA2:{query:{query:'${cat_s}:A'}},  catA3:{query:'${cat_s}:A'}    }"
        )
        , "facets=={ 'count':6, 'catA':{ 'count':2}, 'catA2':{ 'count':2}, 'catA3':{ 'count':2}}"
    );

    // nested query facets
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ catB:{type:query, q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} }}"
        )
        , "facets=={ 'count':6, 'catB':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}}"
    );

    // nested query facets on subset
    client.testJQ(params(p, "q", "id:(2 3)"
            , "json.facet", "{ catB:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} }}}"
        )
        , "facets=={ 'count':2, 'catB':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}}"
    );

    // nested query facets with stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ catB:{query:{q:'${cat_s}:B', facet:{nj:{query:{q:'${where_s}:NJ'}}, ny:{query:'${where_s}:NY'}} }}}"
        )
        , "facets=={ 'count':6, 'catB':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}}"
    );


    // field/terms facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{c1:{field:'${cat_s}'}, c2:{field:{field:'${cat_s}'}}, c3:{${terms} type:terms, field:'${cat_s}'}  }"
        )
        , "facets=={ 'count':6, " +
            "'c1':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c2':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}, " +
            "'c3':{ 'buckets':[{ 'val':'B', 'count':3}, { 'val':'A', 'count':2}]}} "
    );

    // test mincount
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{  'buckets':[{ 'val':'B', 'count':3}]} } "
    );

    // test default mincount of 1
    client.testJQ(params(p, "q", "id:1"
            , "json.facet", "{f1:{terms:'${cat_s}'}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{  'buckets':[{ 'val':'A', 'count':1}]} } "
    );

    // test  mincount of 0 - need processEmpty for distrib to match up
    client.testJQ(params(p, "q", "id:1"
            , "json.facet", "{processEmpty:true, f1:{terms:{${terms} field:'${cat_s}', mincount:0}}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{  'buckets':[{ 'val':'A', 'count':1}, { 'val':'B', 'count':0}]} } "
    );

    // test  mincount of 0 with stats, need processEmpty for distrib to match up
    client.testJQ(params(p, "q", "id:1"
            , "json.facet", "{processEmpty:true, f1:{terms:{${terms} field:'${cat_s}', mincount:0, allBuckets:true, facet:{n1:'sum(${num_d})'}  }}}"
        )
        , "facets=={ 'count':1, " +
            "'f1':{ allBuckets:{ 'count':1, n1:4.0}, 'buckets':[{ 'val':'A', 'count':1, n1:4.0}, { 'val':'B', 'count':0 /*, n1:0.0 */ }]} } "
    );

    // test sorting by other stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'sum(${num_d})'}  }}" +
                " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'sum(${num_d})'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-3.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:6.0 }]} }"
    );

    // test trivial re-sorting by stats
    // (there are other more indepth tests of this in doTestPrelimSorting, but this let's us sanity check
    // small responses with multiple templatized params of diff real types)
    client.testJQ(params(p, "q", "*:*", "json.facet" // num_d
                         , "{f1:{terms:{${terms} field:'${cat_s}', "
                         + "     prelim_sort:'count desc', sort:'n1 desc', facet:{n1:'sum(${num_d})'}  }},"
                         + " f2:{terms:{${terms} field:'${cat_s}', "
                         + "     prelim_sort:'count asc', sort:'n1 asc', facet:{n1:'sum(${num_d})'}  }} }"
                         )
                  , "facets=={ 'count':6 "
                  + ", f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-3.0}]}"
                  + ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:6.0 }]} }"
    );
    client.testJQ(params(p, "q", "*:*", "json.facet" // num_i
                         , "{f1:{terms:{${terms} field:'${cat_s}', "
                         + "     prelim_sort:'count desc', sort:'n1 desc', facet:{n1:'sum(${num_i})'}  }},"
                         + " f2:{terms:{${terms} field:'${cat_s}', "
                         + "     prelim_sort:'count asc', sort:'n1 asc', facet:{n1:'sum(${num_i})'}  }} }"
                         )
                  , "facets=={ 'count':6 "
                  + ", f1:{  'buckets':[{ val:'A', count:2, n1:5.0 }, { val:'B', count:3, n1:-3.0}]}"
                  + ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:5.0 }]} }"
    );

    
    // test sorting by other stats and more than one facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'sum(${num_d})', n2:'avg(${num_d})'}  }}" +
                          " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc' , facet:{n1:'sum(${num_d})', n2:'avg(${num_d})'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 , n2:3.0 }, { val:'B', count:3, n1:-3.0, n2:-1.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0, n2:-1.0}, { val:'A', count:2, n1:6.0 , n2:3.0 }]} }"
    );

    // test sorting by other stats
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f1:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'min(${num_d})'}  }" +
            " , f2:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'max(${num_d})'}  } " +
            " , f3:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'unique(${where_s})'}  } " +
            " , f4:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'hll(${where_s})'}  } " +
            " , f5:{${terms} type:terms, field:'${cat_s}', sort:'x desc', facet:{x:'variance(${num_d})'}  } " +
            " , f6:{type:terms, field:${num_d}, limit:1, sort:'x desc', facet:{x:'hll(${num_i})'}  } " +  // facet on a field that will cause hashing and exercise hll.resize on numeric field
            " , f7:{type:terms, field:${cat_s}, limit:2, sort:'x desc', facet:{x:'missing(${sparse_num_d})'}  } " +
            " , f8:{type:terms, field:${cat_s}, limit:2, sort:'x desc', facet:{x:'countvals(${sparse_num_d})'}  } " +
            "}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, x:2.0 },  { val:'B', count:3, x:-9.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, x:11.0 }, { val:'A', count:2, x:4.0 }]} " +
            ", f3:{  'buckets':[{ val:'A', count:2, x:2 },    { val:'B', count:3, x:2 }]} " +
            ", f4:{  'buckets':[{ val:'A', count:2, x:2 },    { val:'B', count:3, x:2 }]} " +
            ", f5:{  'buckets':[{ val:'B', count:3, x:74.6666666666666 },    { val:'A', count:2, x:1.0 }]} " +
            ", f6:{  buckets:[{ val:-9.0, count:1, x:1 }]} " +
            ", f7:{  buckets:[{ val:B, count:3, x:3 },{ val:A, count:2, x:0 }]} " +
            ", f8:{  buckets:[{ val:A, count:2, x:2 },{ val:B, count:3, x:0 }]} " +
            "}"
    );

    // test sorting by stat with function
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'avg(add(${num_d},${num_d}))'}  }}" +
                " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'avg(add(${num_d},${num_d}))'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-2.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-2.0}, { val:'A', count:2, n1:6.0 }]} }"
    );

    // test sorting by missing stat with function
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'missing(field(${sparse_num_d}))'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'missing(field(${sparse_num_d}))'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1:3 }, { val:'A', count:2, n1:0}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:0}, { val:'B', count:3, n1:3 }]} }"
    );

    // test sorting by missing stat with domain query
    client.testJQ(params(p, "q", "-id:*"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', domain:{query:'*:*'},  sort:'n1 desc', facet:{n1:'missing(field(${sparse_num_d}))'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', domain:{query:'*:*'}, sort:'n1 asc', facet:{n1:'missing(field(${sparse_num_d}))'}  }} }"
        )
        , "facets=={ 'count':0, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1:3 }, { val:'A', count:2, n1:0}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:0}, { val:'B', count:3, n1:3 }]} }"
    );

    // test with sub-facet aggregation with stat on field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", " {f1:{terms:{${terms}, field:'${cat_s}', " +
            "facet:{f2:{terms:{${terms}, field:${where_s}, sort:'index asc', " +
            "facet:{n1:'missing(${sparse_num_d})'}}}}}}}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, f2:{'buckets':[{val:'NJ', count:2, n1:2},{val:'NY', count:1, n1:1}]} }," +
            " { val:'A', count:2, f2:{'buckets':[{val:'NJ', count:1, n1:0},{val:'NY', count:1, n1:0}]}}]}" +
            "}"
    );

    // test with sub-facet aggregation with stat on func
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", " {f1:{terms:{${terms}, field:'${cat_s}', " +
            "facet:{f2:{terms:{${terms}, field:${where_s}, sort:'index asc', " +
            "facet:{n1:'missing(field(${sparse_num_d}))'}}}}}}}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, f2:{'buckets':[{val:'NJ', count:2, n1:2},{val:'NY', count:1, n1:1}]} }," +
            " { val:'A', count:2, f2:{'buckets':[{val:'NJ', count:1, n1:0},{val:'NY', count:1, n1:0}]}}]}" +
            "}"
    );

    // test sorting by countvals stat with function
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'countvals(field(${sparse_num_d}))'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'countvals(field(${sparse_num_d}))'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1:0 }, { val:'A', count:2, n1:2}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:2}, { val:'B', count:3, n1:0 }]} }"
    );

    // test sorting by countvals stat with domain query
    client.testJQ(params(p, "q", "-id:*"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', domain:{query:'*:*'},  sort:'n1 asc', facet:{n1:'countvals(field(${sparse_num_d}))'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', domain:{query:'*:*'}, sort:'n1 desc', facet:{n1:'countvals(field(${sparse_num_d}))'}  }} }"
        )
        , "facets=={ 'count':0, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1:0 }, { val:'A', count:2, n1:2}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:2}, { val:'B', count:3, n1:0 }]} }"
    );

    // test with sub-facet aggregation with stat on field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", " {f1:{terms:{${terms}, field:'${cat_s}', " +
            "facet:{f2:{terms:{${terms}, field:${where_s}, sort:'index asc', " +
            "facet:{n1:'countvals(${sparse_num_d})'}}}}}}}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, f2:{'buckets':[{val:'NJ', count:2, n1:0},{val:'NY', count:1, n1:0}]} }," +
            " { val:'A', count:2, f2:{'buckets':[{val:'NJ', count:1, n1:1},{val:'NY', count:1, n1:1}]}}]}" +
            "}"
    );

    // test with sub-facet aggregation with stat on func
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", " {f1:{terms:{${terms}, field:'${cat_s}', " +
            "facet:{f2:{terms:{${terms}, field:${where_s}, sort:'index asc', " +
            "facet:{n1:'countvals(field(${sparse_num_d}))'}}}}}}}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, f2:{'buckets':[{val:'NJ', count:2, n1:0},{val:'NY', count:1, n1:0}]} }," +
            " { val:'A', count:2, f2:{'buckets':[{val:'NJ', count:1, n1:1},{val:'NY', count:1, n1:1}]}}]}" +
            "}"
    );

    // facet on numbers to test resize from hashing (may need to be sorting by the metric to test that)
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f1:{${terms}  type:field, field:${num_is}, facet:{a:'min(${num_i})'}, sort:'a asc' }" +
            ",f2:{${terms}  type:field, field:${num_is}, facet:{a:'max(${num_i})'}, sort:'a desc' }" +
            "}"
        )
        , "facets=={count:6 " +
            ",f1:{ buckets:[{val:-1,count:2,a:-5},{val:3,count:2,a:-5},{val:-5,count:1,a:2},{val:2,count:1,a:2},{val:0,count:2,a:3} ] } " +
            ",f2:{ buckets:[{val:0,count:2,a:7},{val:3,count:2,a:3},{val:-5,count:1,a:2},{val:2,count:1,a:2},{val:-1,count:2,a:-5} ] } " +
            "}"
    );


    // Same thing for dates
    // test min/max of string field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f3:{${terms}  type:field, field:${num_is}, facet:{a:'min(${date})'}, sort:'a desc' }" +
            ",f4:{${terms}  type:field, field:${num_is}, facet:{a:'max(${date})'}, sort:'a asc' }" +
            "}"
        )
        , "facets=={count:6 " +
            ",f3:{ buckets:[{val:-1,count:2,a:'2002-02-02T02:02:02Z'},{val:3,count:2,a:'2002-02-02T02:02:02Z'},{val:0,count:2,a:'2001-02-03T01:02:03Z'},{val:-5,count:1,a:'2001-01-01T01:01:01Z'},{val:2,count:1,a:'2001-01-01T01:01:01Z'} ] } " +
            ",f4:{ buckets:[{val:-5,count:1,a:'2001-01-01T01:01:01Z'},{val:2,count:1,a:'2001-01-01T01:01:01Z'},{val:-1,count:2,a:'2002-03-01T03:02:01Z'},{val:0,count:2,a:'2003-03-03T03:03:03Z'},{val:3,count:2,a:'2003-03-03T03:03:03Z'} ] } " +
            "}"
    );

    // test field faceting on date field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f1:{${terms}  type:field, field:${date}}" +
            ",f2:{${terms}  type:field, field:${date} sort:'index asc'}" +
            ",f3:{${terms}  type:field, field:${date} sort:'index desc'}" +
            // ",f4:{${terms}  type:field, field:${date}, facet:{x:{type:field,field:${num_is},limit:1}}     }" +
            "}"
        )
        , "facets=={count:6 " +
            ",f1:{ buckets:[ {val:'2001-01-01T01:01:01Z', count:1},{val:'2001-02-03T01:02:03Z', count:1},{val:'2002-02-02T02:02:02Z', count:1},{val:'2002-03-01T03:02:01Z', count:1},{val:'2003-03-03T03:03:03Z', count:1} ] }" +
            ",f2:{ buckets:[ {val:'2001-01-01T01:01:01Z', count:1},{val:'2001-02-03T01:02:03Z', count:1},{val:'2002-02-02T02:02:02Z', count:1},{val:'2002-03-01T03:02:01Z', count:1},{val:'2003-03-03T03:03:03Z', count:1} ] }" +
            ",f3:{ buckets:[ {val:'2003-03-03T03:03:03Z', count:1},{val:'2002-03-01T03:02:01Z', count:1},{val:'2002-02-02T02:02:02Z', count:1},{val:'2001-02-03T01:02:03Z', count:1},{val:'2001-01-01T01:01:01Z', count:1} ] }" +
            "}"
    );


    // percentiles 0,10,50,90,100
    // catA: 2.0 2.2 3.0 3.8 4.0
    // catB: -9.0 -8.2 -5.0 7.800000000000001 11.0
    // all: -9.0 -7.3999999999999995 2.0 8.200000000000001 11.0
    // test sorting by single percentile
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'percentile(${num_d},50)'}  }}" +
                " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'percentile(${num_d},50)'}  }} " +
                " , f3:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'percentile(${sparse_num_d},50)'}  }} " +
            "}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:3.0 }, { val:'B', count:3, n1:-5.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-5.0}, { val:'A', count:2, n1:3.0 }]}" +
            ", f3:{  'buckets':[{ val:'A', count:2, n1:1.0}, { val:'B', count:3}]}" +
            "}"
    );

    // test sorting by multiple percentiles (sort is by first)
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, sort:'n1 desc', facet:{n1:'percentile(${num_d},50,0,100)'}  }}" +
                " , f2:{terms:{${terms} field:${cat_s}, sort:'n1 asc', facet:{n1:'percentile(${num_d},50,0,100)'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:[3.0,2.0,4.0] }, { val:'B', count:3, n1:[-5.0,-9.0,11.0] }]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:[-5.0,-9.0,11.0]}, { val:'A', count:2, n1:[3.0,2.0,4.0] }]} }"
    );

    // test sorting by count/index order
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'count desc' }  }" +
                "           , f2:{terms:{${terms} field:'${cat_s}', sort:'count asc'  }  }" +
                "           , f3:{terms:{${terms} field:'${cat_s}', sort:'index asc'  }  }" +
                "           , f4:{terms:{${terms} field:'${cat_s}', sort:'index desc' }  }" +
                "}"
        )
        , "facets=={ count:6 " +
            " ,f1:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            " ,f2:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f3:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f4:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            "}"
    );

    // test sorting by default count/index order
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'count' }  }" +
                "           , f2:{terms:{${terms} field:'${cat_s}', sort:'count asc'  }  }" +
                "           , f3:{terms:{${terms} field:'${cat_s}', sort:'index'  }  }" +
                "           , f4:{terms:{${terms} field:'${cat_s}', sort:'index desc' }  }" +
                "}"
        )
        , "facets=={ count:6 " +
            " ,f1:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            " ,f2:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f3:{buckets:[ {val:A,count:2}, {val:B,count:3} ] }" +
            " ,f4:{buckets:[ {val:B,count:3}, {val:A,count:2} ] }" +
            "}"
    );


    // test tiebreaks when sorting by count
    client.testJQ(params(p, "q", "id:1 id:6"
            , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'count desc' }  }" +
                "           , f2:{terms:{${terms} field:'${cat_s}', sort:'count asc'  }  }" +
                "}"
        )
        , "facets=={ count:2 " +
            " ,f1:{buckets:[ {val:A,count:1}, {val:B,count:1} ] }" +
            " ,f2:{buckets:[ {val:A,count:1}, {val:B,count:1} ] }" +
            "}"
    );

    // terms facet with nested query facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{${terms} field:'${cat_s}', facet:{nj:{query:'${where_s}:NJ'}}    }   }}"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ 'buckets':[{ 'val':'B', 'count':3, 'nj':{ 'count':2}}, { 'val':'A', 'count':2, 'nj':{ 'count':1}}]} }"
    );

    // terms facet with nested query facet on subset
    client.testJQ(params(p, "q", "id:(2 5 4)"
            , "json.facet", "{cat:{terms:{${terms} field:'${cat_s}', facet:{nj:{query:'${where_s}:NJ'}}    }   }}"
        )
        , "facets=={ 'count':3, " +
            "'cat':{ 'buckets':[{ 'val':'B', 'count':2, 'nj':{ 'count':2}}, { 'val':'A', 'count':1, 'nj':{ 'count':1}}]} }"
    );

    // test prefix
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:s, mincount:0 }}}"  // even with mincount=0, we should only see buckets with the prefix
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:spiderman, count:1}, {val:superman, count:1}]} } "
    );

    // test prefix that doesn't exist
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:ttt, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix that doesn't exist at start
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:aaaaaa, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix that doesn't exist at end
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${super_s}, prefix:zzzzzz, mincount:0 }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[]} } "
    );

    // test prefix on where field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f1:{${terms} type:terms, field:${where_s}, prefix:N  }" +
            ",f2:{${terms} type:terms, field:${where_s}, prefix:NY }" +
            ",f3:{${terms} type:terms, field:${where_s}, prefix:NJ }" +
            "}"
        )
        , "facets=={ 'count':6 " +
            ",f1:{ 'buckets':[ {val:NJ,count:3}, {val:NY,count:2} ]}" +
            ",f2:{ 'buckets':[ {val:NY,count:2} ]}" +
            ",f3:{ 'buckets':[ {val:NJ,count:3} ]}" +
            " } "
    );

    // test prefix on real multi-valued field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            " f1:{${terms} type:terms, field:${multi_ss}, prefix:A  }" +
            ",f2:{${terms} type:terms, field:${multi_ss}, prefix:z }" +
            ",f3:{${terms} type:terms, field:${multi_ss}, prefix:aa }" +
            ",f4:{${terms} type:terms, field:${multi_ss}, prefix:bb }" +
            ",f5:{${terms} type:terms, field:${multi_ss}, prefix:a }" +
            ",f6:{${terms} type:terms, field:${multi_ss}, prefix:b }" +
            "}"
        )
        , "facets=={ 'count':6 " +
            ",f1:{buckets:[]}" +
            ",f2:{buckets:[]}" +
            ",f3:{buckets:[]}" +
            ",f4:{buckets:[]}" +
            ",f5:{buckets:[ {val:a,count:3} ]}" +
            ",f6:{buckets:[ {val:b,count:3} ]}" +
            " } "
    );

    //
    // missing
    //

    // test missing w/ non-existent field
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${noexist}, missing:true}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[], missing:{count:6} } } "
    );

    // test missing
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1}, {val:two, count:1}], missing:{count:4} } } "
    );

    // test missing with stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true, facet:{x:'sum(${num_d})'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1, x:4.0}, {val:two, count:1, x:11.0}], missing:{count:4, x:-12.0}   } } "
    );

    // test that the missing bucket is not affected by any prefix
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true, prefix:on, facet:{x:'sum(${num_d})'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:one, count:1, x:4.0}], missing:{count:4, x:-12.0}   } } "
    );

    // test missing with prefix that doesn't exist
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{terms:{${terms} field:${sparse_s}, missing:true, prefix:ppp, facet:{x:'sum(${num_d})'}   }}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[], missing:{count:4, x:-12.0}   } } "
    );

    // test numBuckets
    client.testJQ(params(p, "q", "*:*", "rows", "0", "facet", "true"
            , "json.facet", "{f1:{terms:{${terms_method} field:${cat_s}, numBuckets:true, limit:1}}}" // TODO: limit:0 produced an error
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:2, buckets:[{val:B, count:3}]} } "
    );

    // prefix should lower numBuckets
    client.testJQ(params(p, "q", "*:*", "rows", "0", "facet", "true"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, numBuckets:true, prefix:B}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:1, buckets:[{val:B, count:3}]} } "
    );

    // mincount should not lower numBuckets (since SOLR-10552)
    client.testJQ(params(p, "q", "*:*", "rows", "0", "facet", "true"
            , "json.facet", "{f1:{terms:{${terms} field:${cat_s}, numBuckets:true, mincount:3}}}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ numBuckets:2, buckets:[{val:B, count:3}]} } "
    );

    // basic range facet
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${num_d}, start:-5, end:10, gap:5}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );

    // basic range facet on dates
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${date}, start:'2001-01-01T00:00:00Z', end:'2003-01-01T00:00:00Z', gap:'+1YEAR'}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:'2001-01-01T00:00:00Z',count:2}, {val:'2002-01-01T00:00:00Z',count:2}] } }"
    );

    // range facet on dates w/ stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${date}, start:'2002-01-01T00:00:00Z', end:'2005-01-01T00:00:00Z', gap:'+1YEAR',   other:all, facet:{ x:'avg(${num_d})' } } }"
        )
        , "facets=={count:6, f:{buckets:[ {val:'2002-01-01T00:00:00Z',count:2,x:-7.0}, {val:'2003-01-01T00:00:00Z',count:1,x:2.0}, {val:'2004-01-01T00:00:00Z',count:0}], before:{count:2,x:7.5}, after:{count:0}, between:{count:3,x:-4.0}  } }"
    );

    // basic range facet with "include" params
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, include:upper}}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:0}, {val:0.0,count:2}, {val:5.0,count:0} ] } }"
    );

    // range facet with sub facets and stats
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */ } ] } }"
    );

    // range facet with sub facets and stats, with "other:all"
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */} ]" +
            ",before: {count:1,x:-5.0,ny:{count:0}}" +
            ",after:  {count:1,x:7.0, ny:{count:0}}" +
            ",between:{count:3,x:0.0, ny:{count:2}}" +
            " } }"
    );

    // range facet with mincount
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all, mincount:2,    facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}"
        )
        , "facets=={count:6, f:{buckets:[  {val:0.0,count:2,x:5.0,ny:{count:1}} ]" +
            ",before: {count:1,x:-5.0,ny:{count:0}}" +
            ",after:  {count:1,x:7.0, ny:{count:0}}" +
            ",between:{count:3,x:0.0, ny:{count:2}}" +
            " } }"
    );
    
    // sparse range facet (with sub facets and stats), with "other:all"
    client.testJQ(params(p, "q", "*:*", "json.facet",
                         "{f:{range:{field:${num_d}, start:-5, end:10, gap:1, other:all,   "+
                         "           facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
                         )
                  , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1, x:-5.0,ny:{count:1}}, "+
                  "                                 {val:-4.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val:-3.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val:-2.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val:-1.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 0.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 1.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 2.0,count:1, x:3.0,ny:{count:0}} , "+
                  "                                 {val: 3.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 4.0,count:1, x:2.0,ny:{count:1}} , "+
                  "                                 {val: 5.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 6.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 7.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 8.0,count:0 /* ,x:0.0,ny:{count:0} */} ,"+
                  "                                 {val: 9.0,count:0 /* ,x:0.0,ny:{count:0} */}"+
                  "                               ]" +
                  "                       ,before: {count:1,x:-5.0,ny:{count:0}}" +
                  "                       ,after:  {count:1,x:7.0, ny:{count:0}}" +
                  "                       ,between:{count:3,x:0.0, ny:{count:2}}" +
                  " } }"
    );
    
    // sparse range facet (with sub facets and stats), with "other:all" & mincount==1
    client.testJQ(params(p, "q", "*:*", "json.facet",
                         "{f:{range:{field:${num_d}, start:-5, end:10, gap:1, other:all, mincount:1,   "+
                         "           facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
                         )
                  , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1, x:-5.0,ny:{count:1}}, "+
                  "                                 {val: 2.0,count:1, x:3.0,ny:{count:0}} , "+
                  "                                 {val: 4.0,count:1, x:2.0,ny:{count:1}} "+
                  "                               ]" +
                  "                       ,before: {count:1,x:-5.0,ny:{count:0}}" +
                  "                       ,after:  {count:1,x:7.0, ny:{count:0}}" +
                  "                       ,between:{count:3,x:0.0, ny:{count:2}}" +
                  " } }"
    );

    // range facet with sub facets and stats, with "other:all", on subset
    client.testJQ(params(p, "q", "id:(3 4 6)"
            , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
        )
        , "facets=={count:3, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:1,x:3.0,ny:{count:0}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */} ]" +
            ",before: {count:0 /* ,x:0.0,ny:{count:0} */ }" +
            ",after:  {count:0 /* ,x:0.0,ny:{count:0} */}" +
            ",between:{count:2,x:-2.0, ny:{count:1}}" +
            " } }"
    );

    // range facet with stats on string fields
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ wn:'unique(${where_s})',wh:'hll(${where_s})'     }   }}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,wn:1,wh:1}, {val:0.0,count:2,wn:2,wh:2}, {val:5.0,count:0}]" +
            " ,before:{count:1,wn:1,wh:1}" +
            " ,after:{count:1,wn:1,wh:1} " +
            " ,between:{count:3,wn:2,wh:2} " +
            " } }"
    );

    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ wmin:'min(${where_s})', wmax:'max(${where_s})'    }   }}"
        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,wmin:NY,wmax:NY}, {val:0.0,count:2,wmin:NJ,wmax:NY}, {val:5.0,count:0}]" +
            " ,before:{count:1,wmin:NJ,wmax:NJ}" +
            " ,after:{count:1,wmin:NJ,wmax:NJ} " +
            " ,between:{count:3,wmin:NJ,wmax:NY} " +
            " } }"
    );

    // stats at top level
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', avg2:'avg(def(${num_d},0))', mind:'min(${num_d})', maxd:'max(${num_d})'" +
                ", numwhere:'unique(${where_s})', unique_num_i:'unique(${num_i})', unique_num_d:'unique(${num_d})', unique_date:'unique(${date})'" +
                ", where_hll:'hll(${where_s})', hll_num_i:'hll(${num_i})', hll_num_d:'hll(${num_d})', hll_date:'hll(${date})'" +
                ", med:'percentile(${num_d},50)', perc:'percentile(${num_d},0,50.0,100)', variance:'variance(${num_d})', stddev:'stddev(${num_d})'" +
                ", mini:'min(${num_i})', maxi:'max(${num_i})', missing:'missing(${sparse_num_d})', vals:'countvals(${sparse_num_d})'" +
            " }"
        )
        , "facets=={ 'count':6, " +
            "sum1:3.0, sumsq1:247.0, avg1:0.6, avg2:0.5, mind:-9.0, maxd:11.0" +
            ", numwhere:2, unique_num_i:4, unique_num_d:5, unique_date:5" +
            ", where_hll:2, hll_num_i:4, hll_num_d:5, hll_date:5" +
            ", med:2.0, perc:[-9.0,2.0,11.0], variance:49.04, stddev:7.002856560004639" +
            ", mini:-5, maxi:7, missing:4, vals:2" +
            "}"
    );

    // stats at top level on multi-valued fields
    client.testJQ(params(p, "q", "*:*", "myfield", "${multi_ss}"
        , "json.facet", "{ sum1:'sum(${num_fs})', sumsq1:'sumsq(${num_fs})', avg1:'avg(${num_fs})', mind:'min(${num_fs})', maxd:'max(${num_fs})'" +
            ", mini:'min(${num_is})', maxi:'max(${num_is})', mins:'min(${multi_ss})', maxs:'max(${multi_ss})'" +
            ", stddev:'stddev(${num_fs})', variance:'variance(${num_fs})', median:'percentile(${num_fs}, 50)'" +
            ", perc:'percentile(${num_fs}, 0,75,100)', maxss:'max($multi_ss)'" +
            " }"
        )
        , "facets=={ 'count':6, " +
            "sum1:0.0, sumsq1:51.5, avg1:0.0, mind:-5.0, maxd:3.0" +
            ", mini:-5, maxi:3, mins:'a', maxs:'b'" +
            ", stddev:2.537222891273055, variance:6.4375, median:0.0, perc:[-5.0,2.25,3.0], maxss:'b'" +
            "}"
    );

    // test sorting by multi-valued
    client.testJQ(params(p, "q", "*:*", "my_field", "${num_is}"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'avg($my_field)'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'avg($my_field)'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1: 0.25}, { val:'A', count:2, n1:0.0}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:0.0}, { val:'B', count:3, n1:0.25 }]} }"
    );

    // test sorting by percentile
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', sort:'n1 asc', facet:{n1:'percentile(${num_is}, 50)'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', sort:'n1 desc', facet:{n1:'percentile(${num_is}, 50)'}  }} }"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1: -0.50}, { val:'A', count:2, n1:1.0}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:1.0}, { val:'B', count:3, n1:-0.50 }]} }"
    );

    // test sorting by multi-valued field with domain query
    client.testJQ(params(p, "q", "-id:*"
        , "json.facet", "{f1:{terms:{${terms} field:'${cat_s}', domain:{query:'*:*'},  sort:'n1 desc', facet:{n1:'sum(${num_is})'}  }}" +
            " , f2:{terms:{${terms} field:'${cat_s}', domain:{query:'*:*'}, sort:'n1 asc', facet:{n1:'sum(${num_is})'}  }} }"
        )
        , "facets=={ 'count':0, " +
            "  f1:{  'buckets':[{ val:'B', count:3, n1:1.0 }, { val:'A', count:2, n1:0.0}]}" +
            ", f2:{  'buckets':[{ val:'A', count:2, n1:0.0}, { val:'B', count:3, n1:1.0 }]} }"
    );

    client.testJQ(params(p, "q", "*:*"
        , "json.facet", " {f1:{terms:{${terms}, field:'${cat_s}', " +
            "facet:{f2:{terms:{${terms}, field:${where_s}, sort:'index asc', " +
            "facet:{n1:'min(${multi_ss})'}}}}}}}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, f2:{'buckets':[{val:'NJ', count:2, n1:'a'},{val:'NY', count:1, n1:'a'}]} }," +
            " { val:'A', count:2, f2:{'buckets':[{val:'NJ', count:1, n1:'b'},{val:'NY', count:1}]}}]}" +
            "}"
    );

    client.testJQ(params(p, "q", "*:*"
        , "json.facet", " {f1:{terms:{${terms}, field:'${cat_s}', " +
            "facet:{f2:{terms:{${terms}, field:${where_s}, sort:'index asc', " +
            "facet:{n1:'max(${multi_ss})'}}}}}}}"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'B', count:3, f2:{'buckets':[{val:'NJ', count:2, n1:'b'},{val:'NY', count:1, n1:'b'}]} }," +
            " { val:'A', count:2, f2:{'buckets':[{val:'NJ', count:1, n1:'b'},{val:'NY', count:1}]}}]}" +
            "}"
    );

    // stats at top level, no matches
    client.testJQ(params(p, "q", "id:DOESNOTEXIST"
            , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', min1:'min(${num_d})', max1:'max(${num_d})'" +
                ", numwhere:'unique(${where_s})', unique_num_i:'unique(${num_i})', unique_num_d:'unique(${num_d})', unique_date:'unique(${date})'" +
                ", where_hll:'hll(${where_s})', hll_num_i:'hll(${num_i})', hll_num_d:'hll(${num_d})', hll_date:'hll(${date})'" +
                ", med:'percentile(${num_d},50)', perc:'percentile(${num_d},0,50.0,100)', variance:'variance(${num_d})', stddev:'stddev(${num_d})' }"
        )
        , "facets=={count:0 " +
            "\n//  ,sum1:0.0, sumsq1:0.0, avg1:0.0, min1:'NaN', max1:'NaN', numwhere:0 \n" +
            " }"
    );

    // stats at top level, matching documents, but no values in the field
    // NOTE: this represents the current state of what is returned, not the ultimate desired state.
    client.testJQ(params(p, "q", "id:3"
        , "json.facet", "{ sum1:'sum(${num_d})', sumsq1:'sumsq(${num_d})', avg1:'avg(${num_d})', min1:'min(${num_d})', max1:'max(${num_d})'" +
            ", numwhere:'unique(${where_s})', unique_num_i:'unique(${num_i})', unique_num_d:'unique(${num_d})', unique_date:'unique(${date})'" +
            ", where_hll:'hll(${where_s})', hll_num_i:'hll(${num_i})', hll_num_d:'hll(${num_d})', hll_date:'hll(${date})'" +
            ", med:'percentile(${num_d},50)', perc:'percentile(${num_d},0,50.0,100)', variance:'variance(${num_d})', stddev:'stddev(${num_d})' }"
        )
        , "facets=={count:1 " +
            ",sum1:0.0," +
            " sumsq1:0.0," +
            " avg1:0.0," +   // TODO: undesirable. omit?
            // " min1:'NaN'," +
            // " max1:'NaN'," +
            " numwhere:0," +
            " unique_num_i:0," +
            " unique_num_d:0," +
            " unique_date:0," +
            " where_hll:0," +
            " hll_num_i:0," +
            " hll_num_d:0," +
            " hll_date:0," +
            " variance:0.0," +
            " stddev:0.0" +
            " }"
    );

    //
    // tests on a multi-valued field with actual multiple values, just to ensure that we are
    // using a multi-valued method for the rest of the tests when appropriate.
    //

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{cat:{terms:{${terms} field:'${multi_ss}', facet:{nj:{query:'${where_s}:NJ'}}    }   }}"
        )
        , "facets=={ 'count':6, " +
            "'cat':{ 'buckets':[{ 'val':'a', 'count':3, 'nj':{ 'count':2}}, { 'val':'b', 'count':3, 'nj':{ 'count':2}}]} }"
    );

    // test unique on multi-valued field
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            "x:'unique(${multi_ss})'" +
            ",z:'missing(${multi_ss})'" +
            ",z1:'missing(${num_is})'" +
            ",v:'countvals(${multi_ss})'" +
            ",v1:'countvals(${num_is})'" +
            ",y:{query:{q:'id:2', facet:{x:'unique(${multi_ss})'} }}  " +
            ",x2:'hll(${multi_ss})'" +
            ",y2:{query:{q:'id:2', facet:{x:'hll(${multi_ss})'} }}  " +
            " }"
        )
        , "facets=={count:6 " +
            ",x:2" +
            ",z:2" +
            ",z1:1" +
            ",v:6" +
            ",v1:8" +
            ",y:{count:1, x:2}" +  // single document should yield 2 unique values
            ",x2:2" +
            ",y2:{count:1, x:2}" +  // single document should yield 2 unique values
            " }"
    );

    // test allBucket multi-valued
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{x:{terms:{${terms} field:'${multi_ss}',allBuckets:true}}}"
        )
        , "facets=={ count:6, " +
            "x:{ buckets:[{val:a, count:3}, {val:b, count:3}] , allBuckets:{count:6} } }"
    );

    // allBuckets for multi-valued field with stats.  This can sometimes take a different path of adding complete DocSets to the Acc
    // also test limit:0
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f0:{${terms_method} type:terms, field:${multi_ss}, allBuckets:true, limit:0} " +
                ",f1:{${terms_method} type:terms, field:${multi_ss}, allBuckets:true, limit:0, offset:1} " +  // offset with 0 limit
                ",f2:{${terms_method} type:terms, field:${multi_ss}, allBuckets:true, limit:0, facet:{x:'sum(${num_d})'}, sort:'x desc' } " +
                ",f3:{${terms_method} type:terms, field:${multi_ss}, allBuckets:true, limit:0, missing:true, facet:{x:'sum(${num_d})', y:'avg(${num_d})'}, sort:'x desc' } " +
                "}"
        )
        , "facets=={ 'count':6, " +
            " f0:{allBuckets:{count:6}, buckets:[]}" +
            ",f1:{allBuckets:{count:6}, buckets:[]}" +
            ",f2:{allBuckets:{count:6, x:-15.0}, buckets:[]} " +
            ",f3:{allBuckets:{count:6, x:-15.0, y:-2.5}, buckets:[], missing:{count:2, x:4.0, y:4.0} }} " +
            "}"
    );

    // allBuckets with numeric field with stats.
    // also test limit:0
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f0:{${terms_method} type:terms, field:${num_i}, allBuckets:true, limit:0} " +
                ",f1:{${terms_method} type:terms, field:${num_i}, allBuckets:true, limit:0, offset:1} " +  // offset with 0 limit
                ",f2:{${terms_method} type:terms, field:${num_i}, allBuckets:true, limit:0, facet:{x:'sum(${num_d})'}, sort:'x desc' } " +
                "}"
        )
        , "facets=={ 'count':6, " +
            " f0:{allBuckets:{count:5}, buckets:[]}" +
            ",f1:{allBuckets:{count:5}, buckets:[]}" +
            ",f2:{allBuckets:{count:5, x:3.0}, buckets:[]} " +
            "}"
    );


    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    // test converting legacy facets

    // test mincount
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f1:{terms:{field:'${cat_s}', mincount:3}}}"
            , "facet","true", "facet.version", "2", "facet.field","{!key=f1}${cat_s}", "facet.mincount","3"
        )
        , "facets=={ 'count':6, " +
            "'f1':{  'buckets':[{ 'val':'B', 'count':3}]} } "
    );

    // test prefix
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f1:{terms:{field:${super_s}, prefix:s, mincount:0 }}}"  // even with mincount=0, we should only see buckets with the prefix
            , "facet","true", "facet.version", "2", "facet.field","{!key=f1}${super_s}", "facet.prefix","s", "facet.mincount","0"
        )
        , "facets=={ 'count':6, " +
            "'f1':{ 'buckets':[{val:spiderman, count:1}, {val:superman, count:1}]} } "
    );

    // range facet with sub facets and stats
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
            , "facet","true", "facet.version", "2", "facet.range","{!key=f}${num_d}", "facet.range.start","-5", "facet.range.end","10", "facet.range.gap","5"
            , "f.f.facet.stat","x:sum(${num_i})", "subfacet.f.query","{!key=ny}${where_s}:NY"

        )
        , "facets=={count:6, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */ } ] } }"
    );

    // test sorting by stat
    client.testJQ(params(p, "q", "*:*"
            // , "json.facet", "{f1:{terms:{field:'${cat_s}', sort:'n1 desc', facet:{n1:'sum(${num_d})'}  }}" +
            //    " , f2:{terms:{field:'${cat_s}', sort:'n1 asc', facet:{n1:'sum(${num_d})'}  }} }"
            , "facet","true", "facet.version", "2", "facet.field","{!key=f1}${cat_s}", "f.f1.facet.sort","n1 desc", "facet.stat","n1:sum(${num_d})"
            , "facet.field","{!key=f2}${cat_s}", "f.f1.facet.sort","n1 asc"
        )
        , "facets=={ 'count':6, " +
            "  f1:{  'buckets':[{ val:'A', count:2, n1:6.0 }, { val:'B', count:3, n1:-3.0}]}" +
            ", f2:{  'buckets':[{ val:'B', count:3, n1:-3.0}, { val:'A', count:2, n1:6.0 }]} }"
    );

    // range facet with sub facets and stats, with "other:all", on subset
    client.testJQ(params(p, "q", "id:(3 4 6)"
            //, "json.facet", "{f:{range:{field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}   }}}"
            , "facet","true", "facet.version", "2", "facet.range","{!key=f}${num_d}", "facet.range.start","-5", "facet.range.end","10", "facet.range.gap","5"
            , "f.f.facet.stat","x:sum(${num_i})", "subfacet.f.query","{!key=ny}${where_s}:NY", "facet.range.other","all"
        )
        , "facets=={count:3, f:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:1,x:3.0,ny:{count:0}}, {val:5.0,count:0 /* ,x:0.0,ny:{count:0} */} ]" +
            ",before: {count:0 /* ,x:0.0,ny:{count:0} */ }" +
            ",after:  {count:0 /* ,x:0.0,ny:{count:0} */}" +
            ",between:{count:2,x:-2.0, ny:{count:1}}" +
            " } }"
    );


    ////////////////////////////////////////////////////////////////////////////////////////////
    // multi-select / exclude tagged filters via excludeTags
    ////////////////////////////////////////////////////////////////////////////////////////////

    // test uncached multi-select (see SOLR-8496)
    client.testJQ(params(p, "q", "{!cache=false}*:*", "fq","{!tag=doc3,allfilt}-id:3"

            , "json.facet", "{" +
                "f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} }  " +
                "}"
        )
        , "facets=={ count:5, " +
            " f1:{ buckets:[ {val:B, count:3}, {val:A, count:2} ]  }" +
            "}"
    );

    // test sub-facets of  empty buckets with domain filter exclusions (canProduceFromEmpty) (see SOLR-9519)
    client.testJQ(params(p, "q", "*:*", "fq","{!tag=doc3}id:non-exist", "fq","{!tag=CATA}${cat_s}:A"

        , "json.facet", "{" +
            "f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} }  " +
            ",q1 :{type:query, q:'*:*', facet:{ f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} } }  }  " +  // nested under query
            ",q1a:{type:query, q:'id:4', facet:{ f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} } }  }  " +  // nested under query, make sure id:4 filter still applies
            ",r1 :{type:range, field:${num_d}, start:0, gap:3, end:5,  facet:{ f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3} } }  }  " +  // nested under range, make sure range constraints still apply
            ",f2:{${terms} type:terms, field:${cat_s}, domain:{filter:'*:*'} }  " + // domain filter doesn't widen, so f2 should not appear.
            "}"
    )
    , "facets=={ count:0, " +
        " f1:{ buckets:[ {val:A, count:2} ]  }" +
        ",q1:{ count:0, f1:{buckets:[{val:A, count:2}]} }" +
        ",q1a:{ count:0, f1:{buckets:[{val:A, count:1}]} }" +
        ",r1:{ buckets:[ {val:0.0,count:0,f1:{buckets:[{val:A, count:1}]}}, {val:3.0,count:0,f1:{buckets:[{val:A, count:1}]}} ]  }" +
        "}"
    );

    // nested query facets on subset (with excludeTags)
    client.testJQ(params(p, "q", "*:*", "fq","{!tag=abc}id:(2 3)"
            , "json.facet", "{ processEmpty:true," +
                " f1:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:[xyz,qaz]}}" +
                ",f2:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:abc }}" +
                ",f3:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:'xyz ,abc ,qaz' }}" +
                ",f4:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:[xyz , abc , qaz] }}" +
                ",f5:{query:{q:'${cat_s}:B', facet:{nj:{query:'${where_s}:NJ'}, ny:{query:'${where_s}:NY'}} , excludeTags:[xyz,qaz]}}" +    // this is repeated, but it did fail when a single context was shared among sub-facets
                ",f6:{query:{q:'${cat_s}:B', facet:{processEmpty:true, nj:{query:'${where_s}:NJ'}, ny:{ type:query, q:'${where_s}:NY', excludeTags:abc}}  }}" +  // exclude in a sub-facet
                ",f7:{query:{q:'${cat_s}:B', facet:{processEmpty:true, nj:{query:'${where_s}:NJ'}, ny:{ type:query, q:'${where_s}:NY', excludeTags:xyz}}  }}" +  // exclude in a sub-facet that doesn't match
                "}"
        )
        , "facets=={ 'count':2, " +
            " 'f1':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}" +
            ",'f2':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}" +
            ",'f3':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}" +
            ",'f4':{'count':3, 'nj':{'count':2}, 'ny':{'count':1}}" +
            ",'f5':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}" +
            ",'f6':{'count':1, 'nj':{'count':1}, 'ny':{'count':1}}" +
            ",'f7':{'count':1, 'nj':{'count':1}, 'ny':{'count':0}}" +
            "}"
    );

    // terms facet with nested query facet (with excludeTags, using new format inside domain:{})
    client.testJQ(params(p, "q", "{!cache=false}*:*", "fq", "{!tag=doc6,allfilt}-id:6", "fq","{!tag=doc3,allfilt}-id:3"

            , "json.facet", "{processEmpty:true, " +
                " f0:{${terms} type:terms, field:${cat_s},                                    facet:{nj:{query:'${where_s}:NJ'}} }  " +
                ",f1:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc3},   missing:true,  facet:{nj:{query:'${where_s}:NJ'}} }  " +
                ",f2:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:allfilt},missing:true,  facet:{nj:{query:'${where_s}:NJ'}} }  " +
                ",f3:{${terms} type:terms, field:${cat_s}, domain:{excludeTags:doc6},   missing:true,  facet:{nj:{query:'${where_s}:NJ'}} }  " +
                "}"
        )
        , "facets=={ count:4, " +
            " f0:{ buckets:[ {val:A, count:2, nj:{ count:1}}, {val:B, count:2, nj:{count:2}} ] }" +
            ",f1:{ buckets:[ {val:A, count:2, nj:{ count:1}}, {val:B, count:2, nj:{count:2}} ] , missing:{count:1,nj:{count:0}} }" +
            ",f2:{ buckets:[ {val:B, count:3, nj:{ count:2}}, {val:A, count:2, nj:{count:1}} ] , missing:{count:1,nj:{count:0}} }" +
            ",f3:{ buckets:[ {val:B, count:3, nj:{ count:2}}, {val:A, count:2, nj:{count:1}} ] , missing:{count:0} }" +
            "}"
    );

    // range facet with sub facets and stats, with "other:all" (with excludeTags)
    client.testJQ(params(p, "q", "*:*", "fq", "{!tag=doc6,allfilt}-id:6", "fq","{!tag=doc3,allfilt}-id:3"
            , "json.facet", "{processEmpty:true " +
                ", f1:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}} , domain:{excludeTags:allfilt} }" +
                ", f2:{type:range, field:${num_d}, start:-5, end:10, gap:5, other:all,   facet:{ x:'sum(${num_i})', ny:{query:'${where_s}:NY'}}  }" +
                "}"
        )
        , "facets=={count:4" +
            ",f1:{buckets:[ {val:-5.0,count:1,x:-5.0,ny:{count:1}}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0} ]" +
              ",before: {count:1,x:-5.0,ny:{count:0}}" +
              ",after:  {count:1,x:7.0, ny:{count:0}}" +
              ",between:{count:3,x:0.0, ny:{count:2}} }" +
            ",f2:{buckets:[ {val:-5.0,count:0}, {val:0.0,count:2,x:5.0,ny:{count:1}}, {val:5.0,count:0} ]" +
              ",before: {count:1,x:-5.0,ny:{count:0}}" +
              ",after:  {count:1,x:7.0, ny:{count:0}}" +
              ",between:{count:2,x:5.0, ny:{count:1}} }" +
            "}"
    );


    //
    // facet on numbers
    //
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f1:{${terms}  type:field, field:${num_i} }" +
                ",f2:{${terms}  type:field, field:${num_i}, sort:'count asc' }" +
                ",f3:{${terms}  type:field, field:${num_i}, sort:'index asc' }" +
                ",f4:{${terms}  type:field, field:${num_i}, sort:'index desc' }" +
                ",f5:{${terms}  type:field, field:${num_i}, sort:'index desc', limit:1, missing:true, allBuckets:true, numBuckets:true }" +
                ",f6:{${terms}  type:field, field:${num_i}, sort:'index desc', mincount:2, numBuckets:true }" +   // mincount should not lower numbuckets (since SOLR-10552)
                ",f7:{${terms}  type:field, field:${num_i}, sort:'index desc', offset:2, numBuckets:true }" +     // test offset
                ",f8:{${terms}  type:field, field:${num_i}, sort:'index desc', offset:100, numBuckets:true }" +   // test high offset
                ",f9:{${terms}  type:field, field:${num_i}, sort:'x desc', facet:{x:'avg(${num_d})'}, missing:true, allBuckets:true, numBuckets:true }" +            // test stats
                ",f10:{${terms}  type:field, field:${num_i}, facet:{a:{query:'${cat_s}:A'}}, missing:true, allBuckets:true, numBuckets:true }" +     // test subfacets
                ",f11:{${terms}  type:field, field:${num_i}, facet:{a:'unique(${num_d})'} ,missing:true, allBuckets:true, sort:'a desc' }" +     // test subfacet using unique on numeric field (this previously triggered a resizing bug)
                "}"
        )
        , "facets=={count:6 " +
            ",f1:{ buckets:[{val:-5,count:2},{val:2,count:1},{val:3,count:1},{val:7,count:1} ] } " +
            ",f2:{ buckets:[{val:2,count:1},{val:3,count:1},{val:7,count:1},{val:-5,count:2} ] } " +
            ",f3:{ buckets:[{val:-5,count:2},{val:2,count:1},{val:3,count:1},{val:7,count:1} ] } " +
            ",f4:{ buckets:[{val:7,count:1},{val:3,count:1},{val:2,count:1},{val:-5,count:2} ] } " +
            ",f5:{ buckets:[{val:7,count:1}]   , numBuckets:4, allBuckets:{count:5}, missing:{count:1}  } " +
            ",f6:{ buckets:[{val:-5,count:2}]  , numBuckets:4  } " +
            ",f7:{ buckets:[{val:2,count:1},{val:-5,count:2}] , numBuckets:4 } " +
            ",f8:{ buckets:[] , numBuckets:4 } " +
            ",f9:{ buckets:[{val:7,count:1,x:11.0},{val:2,count:1,x:4.0},{val:3,count:1,x:2.0},{val:-5,count:2,x:-7.0} ],  numBuckets:4, allBuckets:{count:5,x:0.6},missing:{count:1,x:0.0} } " +  // TODO: should missing exclude "x" because no values were collected?
            ",f10:{ buckets:[{val:-5,count:2,a:{count:0}},{val:2,count:1,a:{count:1}},{val:3,count:1,a:{count:1}},{val:7,count:1,a:{count:0}} ],  numBuckets:4, allBuckets:{count:5},missing:{count:1,a:{count:0}} } " +
            ",f11:{ buckets:[{val:-5,count:2,a:2},{val:2,count:1,a:1},{val:3,count:1,a:1},{val:7,count:1,a:1} ] , missing:{count:1,a:0} , allBuckets:{count:5,a:5}  } " +
            "}"
    );


    // facet on a float field - shares same code with integers/longs currently, so we only need to test labels/sorting
    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{" +
                " f1:{${terms}  type:field, field:${num_d} }" +
                ",f2:{${terms}  type:field, field:${num_d}, sort:'index desc' }" +
                "}"
        )
        , "facets=={count:6 " +
            ",f1:{ buckets:[{val:-9.0,count:1},{val:-5.0,count:1},{val:2.0,count:1},{val:4.0,count:1},{val:11.0,count:1} ] } " +
            ",f2:{ buckets:[{val:11.0,count:1},{val:4.0,count:1},{val:2.0,count:1},{val:-5.0,count:1},{val:-9.0,count:1} ] } " +
            "}"
    );

    // test 0, min/max int/long
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
                "  u : 'unique(${Z_num_i})'" +
                ", u2 : 'unique(${Z_num_l})'" +
                ", min1 : 'min(${Z_num_i})', max1 : 'max(${Z_num_i})'" +
                ", min2 : 'min(${Z_num_l})', max2 : 'max(${Z_num_l})'" +
                ", f1:{${terms}  type:field, field:${Z_num_i} }" +
                ", f2:{${terms}  type:field, field:${Z_num_l} }" +
        "}"
        )
        , "facets=={count:6 " +
            ",u:3" +
            ",u2:3" +
            ",min1:" + Integer.MIN_VALUE +
            ",max1:" + Integer.MAX_VALUE +
            ",min2:" + Long.MIN_VALUE +
            ",max2:" + Long.MAX_VALUE +
            ",f1:{ buckets:[{val:" + Integer.MIN_VALUE + ",count:1},{val:0,count:1},{val:" + Integer.MAX_VALUE+",count:1}]} " +
            ",f2:{ buckets:[{val:" + Long.MIN_VALUE + ",count:1},{val:0,count:1},{val:" + Long.MAX_VALUE+",count:1}]} " +
            "}"
    );



    // multi-valued integer
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            " c1:'unique(${num_is})', c2:'hll(${num_is})', c3:'missing(${num_is})'" +
            ", c4:'countvals(${num_is})', c5:'agg(countvals(${num_is}))'" +
            ",f1:{${terms} type:terms, field:${num_is} }  " +
            "}"
        )
        , "facets=={ count:6 " +
            ", c1:5, c2:5, c3:1, c4:8, c5:8" +
            ", f1:{ buckets:[ {val:-1,count:2},{val:0,count:2},{val:3,count:2},{val:-5,count:1},{val:2,count:1}  ] } " +
            "} "
    );

    // multi-valued float
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            " c1:'unique(${num_fs})', c2:'hll(${num_fs})', c3:'missing(${num_fs})', c4:'agg(missing(${num_fs}))', c5:'countvals(${num_fs})'" +
            ",f1:{${terms} type:terms, field:${num_fs} }  " +
            "}"
        )
        , "facets=={ count:6 " +
            ", c1:5, c2:5, c3:1, c4:1, c5:8" +
            ", f1:{ buckets:[ {val:-1.5,count:2},{val:0.0,count:2},{val:3.0,count:2},{val:-5.0,count:1},{val:2.0,count:1}  ] } " +
            "} "
    );

    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{" +
            // "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0}" +  // overrequest=0 test needs predictable layout
            "cat1:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:1}" +
            ",catDef:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:-1}" +  // -1 is default overrequest
            ",catBig:{type:terms, field:${cat_s}, sort:'count desc', offset:1, limit:2147483647, overrequest:2147483647}" +  // make sure overflows don't mess us up
            "}"
        )
        , "facets=={ count:6" +
            // ", cat0:{ buckets:[ {val:B,count:3} ] }"
            ", cat1:{ buckets:[ {val:B,count:3} ] }" +
            ", catDef:{ buckets:[ {val:B,count:3} ] }" +
            ", catBig:{ buckets:[ {val:A,count:2} ] }" +
            "}"
    );


    // test filter
    client.testJQ(params(p, "q", "*:*", "myfilt","${cat_s}:A", "ff","-id:1", "ff","-id:2"
        , "json.facet", "{" +
            "t:{${terms} type:terms, field:${cat_s}, domain:{filter:[]} }" + // empty filter list
            ",t_filt:{${terms} type:terms, field:${cat_s}, domain:{filter:'${cat_s}:B'} }" +
            ",t_filt2 :{${terms} type:terms, field:${cat_s}, domain:{filter:'{!query v=$myfilt}'} }" +  // test access to qparser and other query parameters
            ",t_filt2a:{${terms} type:terms, field:${cat_s}, domain:{filter:{param:myfilt} } }" +  // test filter via "param" type
            ",t_filt3: {${terms} type:terms, field:${cat_s}, domain:{filter:['-id:1','-id:2']} }" +
            ",t_filt3a:{${terms} type:terms, field:${cat_s}, domain:{filter:{param:ff}} }" +  // test multi-valued query parameter
            ",q:{type:query, q:'${cat_s}:B', domain:{filter:['-id:5']} }" + // also tests a top-level negative filter
            ",r:{type:range, field:${num_d}, start:-5, end:10, gap:5, domain:{filter:'-id:4'} }" +
            "}"
        )
        , "facets=={ count:6, " +
            "t        :{ buckets:[ {val:B, count:3}, {val:A, count:2} ] }" +
            ",t_filt  :{ buckets:[ {val:B, count:3}] } " +
            ",t_filt2 :{ buckets:[ {val:A, count:2}] } " +
            ",t_filt2a:{ buckets:[ {val:A, count:2}] } " +
            ",t_filt3 :{ buckets:[ {val:B, count:2}, {val:A, count:1}] } " +
            ",t_filt3a:{ buckets:[ {val:B, count:2}, {val:A, count:1}] } " +
            ",q:{count:2}" +
            ",r:{buckets:[ {val:-5.0,count:1}, {val:0.0,count:1}, {val:5.0,count:0} ] }" +
            "}"
    );

    //test filter using queries from json.queries
    client.testJQ(params(p, "q", "*:*"
        , "json.queries", "{catS:{'#cat_sA': '${cat_s}:A'}, ff:[{'#id_1':'-id:1'},{'#id_2':'-id:2'}]}"
        , "json.facet", "{" +
            ",t_filt1:{${terms} type:terms, field:${cat_s}, domain:{filter:{param:catS} } }" + // test filter via "param" type from .queries
            ",t_filt2:{${terms} type:terms, field:${cat_s}, domain:{filter:{param:ff}} }" +  // test multi-valued query parameter from .queries
            "}"
        )
        , "facets=={ count:6, " +
            ",t_filt1:{ buckets:[ {val:A, count:2}] } " +
            ",t_filt2:{ buckets:[ {val:B, count:2}, {val:A, count:1}] } " +
            "}"
    );

    // test acc reuse (i.e. reset() method).  This is normally used for stats that are not calculated in the first phase,
    // currently non-sorting stats.
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f1:{type:terms, field:'${cat_s}', facet:{h:'hll(${where_s})' , u:'unique(${where_s})', mind:'min(${num_d})', maxd:'max(${num_d})', mini:'min(${num_i})', maxi:'max(${num_i})'" +
            ", sumd:'sum(${num_d})', avgd:'avg(${num_d})', variance:'variance(${num_d})', stddev:'stddev(${num_d})', missing:'missing(${multi_ss})', vals:'countvals(${multi_ss})'}   }}"
        )
        , "facets=={ 'count':6, " +
            "'f1':{  buckets:[{val:B, count:3, h:2, u:2, mind:-9.0, maxd:11.0, mini:-5, maxi:7,  sumd:-3.0, avgd:-1.0, variance:74.66666666666667, stddev:8.640987597877148, missing:0, vals:5}," +
            "                 {val:A, count:2, h:2, u:2, mind:2.0, maxd:4.0,  mini:2, maxi:3, sumd:6.0, avgd:3.0, variance:1.0, stddev:1.0, missing:1, vals:1}] } } "

    );


    // test min/max of string field
    if (where_s.equals("where_s") || where_s.equals("where_sd")) {  // supports only single valued currently...
      client.testJQ(params(p, "q", "*:* -(+${cat_s}:A +${where_s}:NJ)"  // make NY the only value in bucket A
          , "json.facet", "{" +
              "  f1:{type:terms, field:'${cat_s}', facet:{min:'min(${where_s})', max:'max(${where_s})'}   }" +
              ", f2:{type:terms, field:'${cat_s}', facet:{min:'min(${where_s})', max:'max(${where_s})'} , sort:'min desc'}" +
              ", f3:{type:terms, field:'${cat_s}', facet:{min:'min(${where_s})', max:'max(${where_s})'} , sort:'min asc'}" +
              ", f4:{type:terms, field:'${cat_s}', facet:{min:'min(${super_s})', max:'max(${super_s})'} , sort:'max asc'}" +
              ", f5:{type:terms, field:'${cat_s}', facet:{min:'min(${super_s})', max:'max(${super_s})'} , sort:'max desc'}" +
              "}"
          )
          , "facets=={ count:5, " +
              " f1:{ buckets:[{val:B, count:3, min:NJ, max:NY}, {val:A, count:1, min:NY, max:NY}]}" +
              ",f2:{ buckets:[{val:A, count:1, min:NY, max:NY}, {val:B, count:3, min:NJ, max:NY}]}" +
              ",f3:{ buckets:[{val:B, count:3, min:NJ, max:NY}, {val:A, count:1, min:NY, max:NY}]}" +
              ",f4:{ buckets:[{val:B, count:3, min:batman, max:superman}, {val:A, count:1, min:zodiac, max:zodiac}]}" +
              ",f5:{ buckets:[{val:A, count:1, min:zodiac, max:zodiac}, {val:B, count:3, min:batman, max:superman}]}" +
              " } "
      );


    }


    ////////////////////////////////////////////////////////////////
    // test which phase stats are calculated in
    ////////////////////////////////////////////////////////////////
    if (client.local()) {
      long creates, resets;
      // NOTE: these test the current implementation and may need to be adjusted to match future optimizations (such as calculating N buckets in parallel in the second phase)

      creates = DebugAgg.Acc.creates.get();
      resets = DebugAgg.Acc.resets.get();
      client.testJQ(params(p, "q", "*:*"
          , "json.facet", "{f1:{terms:{${terms_method} field:${super_s}, limit:1, facet:{x:'debug()'}   }}}"  // x should be deferred to 2nd phase
          )
          , "facets=={ 'count':6, " +
              "f1:{  buckets:[{ val:batman, count:1, x:1}]} } "
      );

      assertEquals(1, DebugAgg.Acc.creates.get() - creates);
      assertTrue( DebugAgg.Acc.resets.get() - resets <= 1);
      assertTrue( DebugAgg.Acc.last.numSlots <= 2 ); // probably "1", but may be special slot for something.  As long as it's not cardinality of the field


      creates = DebugAgg.Acc.creates.get();
      resets = DebugAgg.Acc.resets.get();
      client.testJQ(params(p, "q", "*:*"
          , "json.facet", "{f1:{terms:{${terms_method} field:${super_s}, limit:1, facet:{ x:'debug()'} , sort:'x asc'  }}}"  // sorting by x... must be done all at once in first phase
          )
          , "facets=={ 'count':6, " +
              "f1:{  buckets:[{ val:batman, count:1, x:1}]}" +
              " } "
      );

      assertEquals(1, DebugAgg.Acc.creates.get() - creates);
      assertTrue( DebugAgg.Acc.resets.get() - resets == 0);
      assertTrue( DebugAgg.Acc.last.numSlots >= 5 ); // all slots should be done in a single shot. there may be more than 5 due to special slots or hashing.


      // When limit:-1, we should do most stats in first phase (SOLR-10634)
      creates = DebugAgg.Acc.creates.get();
      resets = DebugAgg.Acc.resets.get();
      client.testJQ(params(p, "q", "*:*"
          , "json.facet", "{f1:{terms:{${terms_method} field:${super_s}, limit:-1, facet:{x:'debug()'}  }}}"
          )
          , "facets=="
      );

      assertEquals(1, DebugAgg.Acc.creates.get() - creates);
      assertTrue( DebugAgg.Acc.resets.get() - resets == 0);
      assertTrue( DebugAgg.Acc.last.numSlots >= 5 ); // all slots should be done in a single shot. there may be more than 5 due to special slots or hashing.

      // Now for a numeric field
      // When limit:-1, we should do most stats in first phase (SOLR-10634)
      creates = DebugAgg.Acc.creates.get();
      resets = DebugAgg.Acc.resets.get();
      client.testJQ(params(p, "q", "*:*"
          , "json.facet", "{f1:{terms:{${terms_method} field:${num_d}, limit:-1, facet:{x:'debug()'}  }}}"
          )
          , "facets=="
      );

      assertEquals(1, DebugAgg.Acc.creates.get() - creates);
      assertTrue( DebugAgg.Acc.resets.get() - resets == 0);
      assertTrue( DebugAgg.Acc.last.numSlots >= 5 ); // all slots should be done in a single shot. there may be more than 5 due to special slots or hashing.


      // But if we need to calculate domains anyway, it probably makes sense to calculate most stats in the 2nd phase (along with sub-facets)
      creates = DebugAgg.Acc.creates.get();
      resets = DebugAgg.Acc.resets.get();
      client.testJQ(params(p, "q", "*:*"
          , "json.facet", "{f1:{terms:{${terms_method} field:${super_s}, limit:-1, facet:{ x:'debug()' , y:{terms:${where_s}}   }  }}}"
          )
          , "facets=="
      );

      assertEquals(1, DebugAgg.Acc.creates.get() - creates);
      assertTrue( DebugAgg.Acc.resets.get() - resets >=4);
      assertTrue( DebugAgg.Acc.last.numSlots <= 2 ); // probably 1, but could be higher

      // Now with a numeric field
      // But if we need to calculate domains anyway, it probably makes sense to calculate most stats in the 2nd phase (along with sub-facets)
      creates = DebugAgg.Acc.creates.get();
      resets = DebugAgg.Acc.resets.get();
      client.testJQ(params(p, "q", "*:*"
          , "json.facet", "{f1:{terms:{${terms_method} field:${num_d}, limit:-1, facet:{ x:'debug()' , y:{terms:${where_s}}   }  }}}"
          )
          , "facets=="
      );

      assertEquals(1, DebugAgg.Acc.creates.get() - creates);
      assertTrue( DebugAgg.Acc.resets.get() - resets >=4);
      assertTrue( DebugAgg.Acc.last.numSlots <= 2 ); // probably 1, but could be higher
    }
    //////////////////////////////////////////////////////////////// end phase testing

    //
    // Refinement should not be needed to get exact results here, so this tests that
    // extra refinement requests are not sent out.  This currently relies on counting the number of times
    // debug() aggregation is parsed... which is somewhat fragile.  Please replace this with something
    // better in the future - perhaps debug level info about number of refinements or additional facet phases.
    //
    for (String facet_field : new String[]{cat_s,where_s,num_d,num_i,num_is,num_fs,super_s,date,val_b,multi_ss}) {
      ModifiableSolrParams test = params(p, "q", "id:(1 2)", "facet_field",facet_field, "debug", "true"
          , "json.facet", "{ " +
              " f1:{type:terms, field:'${facet_field}',  refine:${refine},  facet:{x:'debug()'}   }" +
              ",f2:{type:terms, method:dvhash, field:'${facet_field}',  refine:${refine},  facet:{x:'debug()'}   }" +
              ",f3:{type:terms, field:'${facet_field}',  refine:${refine},  facet:{x:'debug()',  y:{type:terms,field:'${facet_field}',refine:${refine}}}   }" +  // facet within facet
              " }"
      );
      long startParses = DebugAgg.parses.get();
      client.testJQ(params(test, "refine", "false")
          , "facets==" + ""
      );
      long noRefineParses = DebugAgg.parses.get() - startParses;

      startParses = DebugAgg.parses.get();
      client.testJQ(params(test, "refine", "true")
          , "facets==" + ""
      );
      long refineParses = DebugAgg.parses.get() - startParses;
      assertEquals(noRefineParses, refineParses);
    }
  }

  public void testPrelimSortingSingleNode() throws Exception {
    doTestPrelimSortingSingleNode(false, false);
  }
  
  public void testPrelimSortingSingleNodeExtraStat() throws Exception {
    doTestPrelimSortingSingleNode(true, false);
  }
  
  public void testPrelimSortingSingleNodeExtraFacet() throws Exception {
    doTestPrelimSortingSingleNode(false, true);
  }
  
  public void testPrelimSortingSingleNodeExtraStatAndFacet() throws Exception {
    doTestPrelimSortingSingleNode(true, true);
  }
  
  /** @see #doTestPrelimSorting */
  public void doTestPrelimSortingSingleNode(final boolean extraAgg, final boolean extraSubFacet) throws Exception {
    // we're not using Client.localClient because it doesn't provide a SolrClient to
    // use in doTestPrelimSorting -- so instead we make a single node, and don't use any shards param...
    final SolrInstances nodes = new SolrInstances(1, "solrconfig-tlog.xml", "schema_latest.xml");
    try {
      final Client client = nodes.getClient(random().nextInt());
      client.queryDefaults().set("debugQuery", Boolean.toString(random().nextBoolean()) );
      doTestPrelimSorting(client, extraAgg, extraSubFacet);
    } finally {
      nodes.stop();
    }
  }
  
  public void testPrelimSortingDistrib() throws Exception {
    doTestPrelimSortingDistrib(false, false);
  }
  
  public void testPrelimSortingDistribExtraStat() throws Exception {
    doTestPrelimSortingDistrib(true, false);
  }
  
  public void testPrelimSortingDistribExtraFacet() throws Exception {
    doTestPrelimSortingDistrib(false, true);
  }
  
  public void testPrelimSortingDistribExtraStatAndFacet() throws Exception {
    doTestPrelimSortingDistrib(true, true);
  }

  /** @see #doTestPrelimSorting */
  public void doTestPrelimSortingDistrib(final boolean extraAgg, final boolean extraSubFacet) throws Exception {
    // we only use 2 shards, but we also want to to sanity check code paths if one (additional) shard is empty
    final int totalShards = random().nextBoolean() ? 2 : 3;
    
    final SolrInstances nodes = new SolrInstances(totalShards, "solrconfig-tlog.xml", "schema_latest.xml");
    try {
      final Client client = nodes.getClient(random().nextInt());
      client.queryDefaults().set( "shards", nodes.getShards(),
                                  "debugQuery", Boolean.toString(random().nextBoolean()) );
      doTestPrelimSorting(client, extraAgg, extraSubFacet);
    } finally {
      nodes.stop();
    }
  }
  
  /**
   * Helper method that indexes a fixed set of docs to exactly <em>two</em> of the SolrClients 
   * involved in the current Client such that each shard is identical for the purposes of simplified 
   * doc/facet counting/assertions -- if there is only one SolrClient (Client.local) then it sends that 
   * single shard twice as many docs so the counts/assertions will be consistent.
   *
   * Note: this test doesn't demonstrate practical uses of prelim_sort.
   * The scenerios it tests are actualy fairly absurd, but help to ensure that edge cases are covered.
   *
   * @param client client to use -- may be local or multishard
   * @param extraAgg if an extra aggregation function should be included, this hits slightly diff code paths
   * @param extraSubFacet if an extra sub facet should be included, this hits slightly diff code paths
   */
  public void doTestPrelimSorting(final Client client,
                                  final boolean extraAgg,
                                  final boolean extraSubFacet) throws Exception {
    
    client.deleteByQuery("*:*", null);
    
    List<SolrClient> clients = client.getClientProvider().all();
    
    // carefully craft two balanced shards (assuming we have at least two) and leave any other shards
    // empty to help check the code paths of some shards returning no buckets.
    //
    // if we are in a single node sitaution, these clients will be the same, and we'll have the same
    // total docs in our collection, but the numShardsWithData will be diff
    // (which will affect some assertions)
    final SolrClient shardA = clients.get(0);
    final SolrClient shardB = clients.get(clients.size()-1);
    final int numShardsWithData = (shardA == shardB) ? 1 : 2;

    // for simplicity, each foo_s "term" exists on each shard in the same number of docs as it's numeric 
    // value (so count should be double the term) and bar_i is always 1 per doc (so sum(bar_i)
    // should always be the same as count)
    int id = 0;
    for (int i = 1; i <= 20; i++) {
      for (int j = 1; j <= i; j++) {
        shardA.add(new SolrInputDocument("id", ""+(++id), "foo_s", "foo_" + i, "bar_i", "1"));
        shardB.add(new SolrInputDocument("id", ""+(++id), "foo_s", "foo_" + i, "bar_i", "1"));
      }
    }
    assertEquals(420, id); // sanity check
    client.commit();
    DebugAgg.Acc.collectDocs.set(0);
    DebugAgg.Acc.collectDocSets.set(0);

    // NOTE: sorting by index can cause some optimizations when using type=enum|stream
    // that cause our stat to be collected differently, so we have to account for that when
    // looking at DebugAdd collect stats if/when the test framework picks those
    // ...BUT... this only affects cloud, for single node prelim_sort overrides streaming
    final boolean indexSortDebugAggFudge = ( 1 < numShardsWithData ) &&
      (FacetField.FacetMethod.DEFAULT_METHOD.equals(FacetField.FacetMethod.STREAM) ||
       FacetField.FacetMethod.DEFAULT_METHOD.equals(FacetField.FacetMethod.ENUM));
    
    
    final String common = "refine:true, type:field, field:'foo_s', facet: { "
      + "x: 'debug(wrap,sum(bar_i))' "
      + (extraAgg ? ", y:'min(bar_i)'" : "")
      + (extraSubFacet ? ", z:{type:query, q:'bar_i:0'}" : "")
      + "}";
    final String yz = (extraAgg ? "y:1, " : "") + (extraSubFacet ? "z:{count:0}, " : "");
    
    // really basic: top 5 by (prelim_sort) count, (re)sorted by a stat
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                         , "{ foo_a:{ "+ common+", limit:5, overrequest:0, "
                         + "          prelim_sort:'count desc', sort:'x asc' }"
                         + "  foo_b:{ "+ common+", limit:5, overrequest:0, "
                         + "          prelim_sort:'count asc', sort:'x desc' } }")
                  , "facets=={ 'count':420, "
                  + "  'foo_a':{ 'buckets':[" 
                  + "    { val:foo_16, count:32, " + yz + "x:32.0},"
                  + "    { val:foo_17, count:34, " + yz + "x:34.0},"
                  + "    { val:foo_18, count:36, " + yz + "x:36.0},"
                  + "    { val:foo_19, count:38, " + yz + "x:38.0},"
                  + "    { val:foo_20, count:40, " + yz + "x:40.0},"
                  + "] },"
                  + "  'foo_b':{ 'buckets':[" 
                  + "    { val:foo_5, count:10, " + yz + "x:10.0},"
                  + "    { val:foo_4, count:8,  " + yz + "x:8.0},"
                  + "    { val:foo_3, count:6,  " + yz + "x:6.0},"
                  + "    { val:foo_2, count:4,  " + yz + "x:4.0},"
                  + "    { val:foo_1, count:2,  " + yz + "x:2.0},"
                  + "] },"
                  + "}"
                  );
    // (re)sorting should prevent 'sum(bar_i)' from being computed for every doc
    // only the choosen buckets should be collected (as a set) once per node...
    assertEqualsAndReset(0, DebugAgg.Acc.collectDocs);
    // 2 facets, 5 bucket, on each shard
    assertEqualsAndReset(numShardsWithData * 2 * 5, DebugAgg.Acc.collectDocSets);

    { // same really basic top 5 by (prelim_sort) count, (re)sorted by a stat -- w/allBuckets:true
      // check code paths with and w/o allBuckets
      // NOTE: allBuckets includes stats, but not other sub-facets...
      final String aout = "allBuckets:{ count:420, "+ (extraAgg ? "y:1, " : "") + "x:420.0 }";
      client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                           , "{ foo_a:{ " + common+", allBuckets:true, limit:5, overrequest:0, "
                           + "          prelim_sort:'count desc', sort:'x asc' }"
                           + "  foo_b:{ " + common+", allBuckets:true, limit:5, overrequest:0, "
                           + "          prelim_sort:'count asc', sort:'x desc' } }")
                    , "facets=={ 'count':420, "
                    + "  'foo_a':{ " + aout + " 'buckets':[" 
                    + "    { val:foo_16, count:32, " + yz + "x:32.0},"
                    + "    { val:foo_17, count:34, " + yz + "x:34.0},"
                    + "    { val:foo_18, count:36, " + yz + "x:36.0},"
                    + "    { val:foo_19, count:38, " + yz + "x:38.0},"
                    + "    { val:foo_20, count:40, " + yz + "x:40.0},"
                    + "] },"
                    + "  'foo_b':{ " + aout + " 'buckets':[" 
                    + "    { val:foo_5, count:10, " + yz + "x:10.0},"
                    + "    { val:foo_4, count:8,  " + yz + "x:8.0},"
                    + "    { val:foo_3, count:6,  " + yz + "x:6.0},"
                    + "    { val:foo_2, count:4,  " + yz + "x:4.0},"
                    + "    { val:foo_1, count:2,  " + yz + "x:2.0},"
                    + "] },"
                    + "}"
                    );
      // because of allBuckets, we collect every doc on everyshard (x2 facets) in a single "all" slot...
      assertEqualsAndReset(2 * 420, DebugAgg.Acc.collectDocs);
      // ... in addition to collecting each of the choosen buckets (as sets) once per node...
      // 2 facets, 5 bucket, on each shard
      assertEqualsAndReset(numShardsWithData * 2 * 5, DebugAgg.Acc.collectDocSets);
    }
    
    // pagination (with offset) should happen against the re-sorted list (up to the effective limit)
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                         , "{ foo_a:{ "+common+", offset:2, limit:3, overrequest:0, "
                         + "          prelim_sort:'count desc', sort:'x asc' }"
                         + "  foo_b:{ "+common+", offset:2, limit:3, overrequest:0, "
                         + "          prelim_sort:'count asc', sort:'x desc' } }")
                  , "facets=={ 'count':420, "
                  + "  'foo_a':{ 'buckets':[" 
                  + "    { val:foo_18, count:36, " + yz + "x:36.0},"
                  + "    { val:foo_19, count:38, " + yz + "x:38.0},"
                  + "    { val:foo_20, count:40, " + yz + "x:40.0},"
                  + "] },"
                  + "  'foo_b':{ 'buckets':[" 
                  + "    { val:foo_3, count:6,  " + yz + "x:6.0},"
                  + "    { val:foo_2, count:4,  " + yz + "x:4.0},"
                  + "    { val:foo_1, count:2,  " + yz + "x:2.0},"
                  + "] },"
                  + "}"
                  );
    assertEqualsAndReset(0, DebugAgg.Acc.collectDocs);
    // 2 facets, 5 buckets (including offset), on each shard
    assertEqualsAndReset(numShardsWithData * 2 * 5, DebugAgg.Acc.collectDocSets);
    
    // when overrequesting is used, the full list of candidate buckets should be considered
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                         , "{ foo_a:{ "+common+", limit:5, overrequest:5, "
                         + "          prelim_sort:'count desc', sort:'x asc' }"
                         + "  foo_b:{ "+common+", limit:5, overrequest:5, "
                         + "          prelim_sort:'count asc', sort:'x desc' } }")
                  , "facets=={ 'count':420, "
                  + "  'foo_a':{ 'buckets':[" 
                  + "    { val:foo_11, count:22, " + yz + "x:22.0},"
                  + "    { val:foo_12, count:24, " + yz + "x:24.0},"
                  + "    { val:foo_13, count:26, " + yz + "x:26.0},"
                  + "    { val:foo_14, count:28, " + yz + "x:28.0},"
                  + "    { val:foo_15, count:30, " + yz + "x:30.0},"
                  + "] },"
                  + "  'foo_b':{ 'buckets':[" 
                  + "    { val:foo_10, count:20, " + yz + "x:20.0},"
                  + "    { val:foo_9, count:18,  " + yz + "x:18.0},"
                  + "    { val:foo_8, count:16,  " + yz + "x:16.0},"
                  + "    { val:foo_7, count:14,  " + yz + "x:14.0},"
                  + "    { val:foo_6, count:12,  " + yz + "x:12.0},"
                  + "] },"
                  + "}"
                  );
    assertEqualsAndReset(0, DebugAgg.Acc.collectDocs);
    // 2 facets, 10 buckets (including overrequest), on each shard
    assertEqualsAndReset(numShardsWithData * 2 * 10, DebugAgg.Acc.collectDocSets);

    { // for an (effectively) unlimited facet, then from the black box perspective of the client,
      // preliminary sorting should be completely ignored...
      final StringBuilder expected = new StringBuilder("facets=={ 'count':420, 'foo_a':{ 'buckets':[\n");
      for (int i = 20; 0 < i; i--) {
        final int x = i * 2;
        expected.append("{ val:foo_"+i+", count:"+x+", " + yz + "x:"+x+".0},\n");
      }
      expected.append("] } }");
      for (int limit : Arrays.asList(-1, 100000)) {
        for (String sortOpts : Arrays.asList("sort:'x desc'",
                                             "prelim_sort:'count asc', sort:'x desc'",
                                             "prelim_sort:'index asc', sort:'x desc'")) {
          final String snippet = "limit: " + limit + ", " + sortOpts;
          client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                               , "{ foo_a:{ "+common+", " + snippet + "}}")
                        , expected.toString());

          // the only difference from a white box perspective, is when/if we are 
          // optimized to use the sort SlotAcc during collection instead of the prelim_sort SlotAcc..
          // (ie: sub facet preventing single pass (re)sort in single node mode)
          if (((0 < limit || extraSubFacet) && snippet.contains("prelim_sort")) &&
              ! (indexSortDebugAggFudge && snippet.contains("index asc"))) {
            // by-pass single pass collection, do everything as sets...
            assertEqualsAndReset(snippet, numShardsWithData * 20, DebugAgg.Acc.collectDocSets);
            assertEqualsAndReset(snippet, 0, DebugAgg.Acc.collectDocs);
          } else { // simple sort on x, or optimized single pass (re)sort, or indexSortDebugAggFudge
            // no sets should have been (post) collected for our stat
            assertEqualsAndReset(snippet, 0, DebugAgg.Acc.collectDocSets);
            // every doc should be collected...
            assertEqualsAndReset(snippet, 420, DebugAgg.Acc.collectDocs);
          }
        }
      }
    }

    // test all permutations of (prelim_sort | sort) on (index | count | stat) since there are
    // custom sort codepaths for index & count that work differnetly then general stats
    //
    // NOTE: there's very little value in re-sort by count/index after prelim_sort on something more complex,
    // typically better to just ignore the prelim_sort, but we're testing it for completeness
    // (and because you *might* want to prelim_sort by some function, for the purpose of "sampling" the
    // top results and then (re)sorting by count/index)
    for (String numSort : Arrays.asList("count", "x")) { // equivilent ordering
      client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                           , "{ foo_a:{ "+common+", limit:10, overrequest:0, "
                           + "          prelim_sort:'"+numSort+" asc', sort:'index desc' }"
                           + "  foo_b:{ "+common+", limit:10, overrequest:0, "
                           + "          prelim_sort:'index asc', sort:'"+numSort+" desc' } }")
                    , "facets=={ 'count':420, "
                    + "  'foo_a':{ 'buckets':[" 
                    + "    { val:foo_9,  count:18, " + yz + "x:18.0},"
                    + "    { val:foo_8,  count:16, " + yz + "x:16.0},"
                    + "    { val:foo_7,  count:14, " + yz + "x:14.0},"
                    + "    { val:foo_6,  count:12, " + yz + "x:12.0},"
                    + "    { val:foo_5,  count:10, " + yz + "x:10.0},"
                    + "    { val:foo_4,  count:8,  " + yz + "x:8.0},"
                    + "    { val:foo_3,  count:6,  " + yz + "x:6.0},"
                    + "    { val:foo_2,  count:4,  " + yz + "x:4.0},"
                    + "    { val:foo_10, count:20, " + yz + "x:20.0},"
                    + "    { val:foo_1,  count:2,  " + yz + "x:2.0},"
                    + "] },"
                    + "  'foo_b':{ 'buckets':[" 
                    + "    { val:foo_18, count:36, " + yz + "x:36.0},"
                    + "    { val:foo_17, count:34, " + yz + "x:34.0},"
                    + "    { val:foo_16, count:32, " + yz + "x:32.0},"
                    + "    { val:foo_15, count:30, " + yz + "x:30.0},"
                    + "    { val:foo_14, count:28, " + yz + "x:28.0},"
                    + "    { val:foo_13, count:26, " + yz + "x:26.0},"
                    + "    { val:foo_12, count:24, " + yz + "x:24.0},"
                    + "    { val:foo_11, count:22, " + yz + "x:22.0},"
                    + "    { val:foo_10, count:20, " + yz + "x:20.0},"
                    + "    { val:foo_1,  count:2,  " + yz + "x:2.0},"
                    + "] },"
                    + "}"
                    );
      // since these behave differently, defer DebugAgg counter checks until all are done...
    }
    // These 3 permutations defer the compuation of x as docsets,
    // so it's 3 x (10 buckets on each shard) (but 0 direct docs)
    //      prelim_sort:count, sort:index
    //      prelim_sort:index, sort:x
    //      prelim_sort:index, sort:count
    // ...except when streaming, prelim_sort:index does no docsets.
    assertEqualsAndReset((indexSortDebugAggFudge ? 1 : 3) * numShardsWithData * 10,
                         DebugAgg.Acc.collectDocSets);
    // This is the only situation that should (always) result in every doc being collected (but 0 docsets)...
    //      prelim_sort:x,     sort:index
    // ...but the (2) prelim_sort:index streaming situations above will also cause all the docs in the first
    // 10+1 buckets to be collected (enum checks limit+1 to know if there are "more"...
    assertEqualsAndReset(420 + (indexSortDebugAggFudge ?
                                2 * numShardsWithData * (1+10+11+12+13+14+15+16+17+18+19) : 0),
                         DebugAgg.Acc.collectDocs);

    // sanity check of prelim_sorting in a sub facet
    client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                         , "{ bar:{ type:query, query:'foo_s:[foo_10 TO foo_19]', facet: {"
                         + "        foo:{ "+ common+", limit:5, overrequest:0, "
                         + "              prelim_sort:'count desc', sort:'x asc' } } } }")
                  , "facets=={ 'count':420, "
                  + " 'bar':{ 'count':290, "
                  + "    'foo':{ 'buckets':[" 
                  + "      { val:foo_15, count:30, " + yz + "x:30.0},"
                  + "      { val:foo_16, count:32, " + yz + "x:32.0},"
                  + "      { val:foo_17, count:34, " + yz + "x:34.0},"
                  + "      { val:foo_18, count:36, " + yz + "x:36.0},"
                  + "      { val:foo_19, count:38, " + yz + "x:38.0},"
                  + "    ] },"
                  + "  },"
                  + "}"
                  );
    // the prelim_sort should prevent 'sum(bar_i)' from being computed for every doc
    // only the choosen buckets should be collected (as a set) once per node...
    assertEqualsAndReset(0, DebugAgg.Acc.collectDocs);
    // 5 bucket, on each shard
    assertEqualsAndReset(numShardsWithData * 5, DebugAgg.Acc.collectDocSets);

    { // sanity check how defered stats are handled
      
      // here we'll prelim_sort & sort on things that are both "not x" and using the debug() counters
      // (wrapping x) to assert that 'x' is correctly defered and only collected for the final top buckets
      final List<String> sorts = new ArrayList<String>(Arrays.asList("index asc", "count asc"));
      if (extraAgg) {
        sorts.add("y asc"); // same for every bucket, but index order tie breaker should kick in
      }
      for (String s : sorts) {
        client.testJQ(params("q", "*:*", "rows", "0", "json.facet"
                             , "{ foo:{ "+ common+", limit:5, overrequest:0, "
                             + "          prelim_sort:'count desc', sort:'"+s+"' } }")
                      , "facets=={ 'count':420, "
                      + "  'foo':{ 'buckets':[" 
                      + "    { val:foo_16, count:32, " + yz + "x:32.0},"
                      + "    { val:foo_17, count:34, " + yz + "x:34.0},"
                      + "    { val:foo_18, count:36, " + yz + "x:36.0},"
                      + "    { val:foo_19, count:38, " + yz + "x:38.0},"
                      + "    { val:foo_20, count:40, " + yz + "x:40.0},"
                      + "] } }"
                      );
        // Neither prelim_sort nor sort should need 'sum(bar_i)' to be computed for every doc
        // only the choosen buckets should be collected (as a set) once per node...
        assertEqualsAndReset(0, DebugAgg.Acc.collectDocs);
        // 5 bucket, on each shard
        assertEqualsAndReset(numShardsWithData * 5, DebugAgg.Acc.collectDocSets);
      }
    }
  }

  
  @Test
  public void testOverrequest() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()) );

    List<SolrClient> clients = client.getClientProvider().all();
    assertTrue(clients.size() >= 3);

    client.deleteByQuery("*:*", null);

    ModifiableSolrParams p = params("cat_s", "cat_s");
    String cat_s = p.get("cat_s");

    clients.get(0).add( sdoc("id", "1", cat_s, "A") ); // A will win tiebreak
    clients.get(0).add( sdoc("id", "2", cat_s, "B") );

    clients.get(1).add( sdoc("id", "3", cat_s, "B") );
    clients.get(1).add( sdoc("id", "4", cat_s, "A") ); // A will win tiebreak

    clients.get(2).add( sdoc("id", "5", cat_s, "B") );
    clients.get(2).add( sdoc("id", "6", cat_s, "B") );

    client.commit();

    // Shard responses should be A=1, A=1, B=2, merged should be "A=2, B=2" hence A wins tiebreak

    client.testJQ(params(p, "q", "*:*",
        "json.facet", "{" +
            "cat0:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:0}" +
            ",cat1:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:1}" +
            ",catDef:{type:terms, field:${cat_s}, sort:'count desc', limit:1, overrequest:-1}" +  // -1 is default overrequest
            ",catBig:{type:terms, field:${cat_s}, sort:'count desc', offset:1, limit:2147483647, overrequest:2147483647}" +  // make sure overflows don't mess us up
            "}"
        )
        , "facets=={ count:6" +
            ", cat0:{ buckets:[ {val:A,count:2} ] }" +  // with no overrequest, we incorrectly conclude that A is the top bucket
            ", cat1:{ buckets:[ {val:B,count:4} ] }" +
            ", catDef:{ buckets:[ {val:B,count:4} ] }" +
            ", catBig:{ buckets:[ {val:A,count:2} ] }" +
            "}"
    );
  }


  @Test
  public void testBigger() throws Exception {
    ModifiableSolrParams p = params("rows", "0", "cat_s", "cat_ss", "where_s", "where_ss");
    //    doBigger(Client.localClient, p);

    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards() );
    doBigger( client, p );
  }

  private String getId(int id) {
    return String.format(Locale.US, "%05d", id);
  }

  public void doBigger(Client client, ModifiableSolrParams p) throws Exception {
    MacroExpander m = new MacroExpander(p.getMap());

    String cat_s = m.expand("${cat_s}");
    String where_s = m.expand("${where_s}");

    client.deleteByQuery("*:*", null);

    Random r = new Random(0);  // make deterministic
    int numCat = 1;
    int numWhere = 2000000000;
    int commitPercent = 10;
    int ndocs=1000;

    Map<Integer, Map<Integer, List<Integer>>> model = new HashMap<>();  // cat->where->list<ids>
    for (int i=0; i<ndocs; i++) {
      Integer cat = r.nextInt(numCat);
      Integer where = r.nextInt(numWhere);
      client.add( sdoc("id", getId(i), cat_s,cat, where_s, where) , null );
      Map<Integer,List<Integer>> sub = model.get(cat);
      if (sub == null) {
        sub = new HashMap<>();
        model.put(cat, sub);
      }
      List<Integer> ids = sub.get(where);
      if (ids == null) {
        ids = new ArrayList<>();
        sub.put(where, ids);
      }
      ids.add(i);

      if (r.nextInt(100) < commitPercent) {
        client.commit();
      }
    }

    client.commit();

    int sz = model.get(0).size();

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{f1:{type:terms, field:${cat_s}, limit:2, facet:{x:'unique($where_s)'}  }}"
        )
        , "facets=={ 'count':" + ndocs + "," +
            "'f1':{  'buckets':[{ 'val':'0', 'count':" + ndocs + ", x:" + sz + " }]} } "
    );

    if (client.local()) {
      // distrib estimation prob won't match
      client.testJQ(params(p, "q", "*:*"
              , "json.facet", "{f1:{type:terms, field:${cat_s}, limit:2, facet:{x:'hll($where_s)'}  }}"
          )
          , "facets=={ 'count':" + ndocs + "," +
              "'f1':{  'buckets':[{ 'val':'0', 'count':" + ndocs + ", x:" + sz + " }]} } "
      );
    }

    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{f1:{type:terms, field:id, limit:1, offset:990}}"
        )
        , "facets=={ 'count':" + ndocs + "," +
            "'f1':{buckets:[{val:'00990',count:1}]}} "
    );


    for (int i=0; i<20; i++) {
      int off = random().nextInt(ndocs);
      client.testJQ(params(p, "q", "*:*", "off",Integer.toString(off)
          , "json.facet", "{f1:{type:terms, field:id, limit:1, offset:${off}}}"
          )
          , "facets=={ 'count':" + ndocs + "," +
              "'f1':{buckets:[{val:'"  + getId(off)  + "',count:1}]}} "
      );
    }
  }

  public void testTolerant() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards() + "," + DEAD_HOST_1 + "/ignore_exception");
    indexSimple(client);

    try {
      client.testJQ(params("ignore_exception", "true", "shards.tolerant", "false", "q", "*:*"
              , "json.facet", "{f:{type:terms, field:cat_s}}"
          )
          , "facets=={ count:6," +
              "f:{ buckets:[{val:B,count:3},{val:A,count:2}] }" +
              "}"
      );
      fail("we should have failed");
    } catch (Exception e) {
      // ok
    }

    client.testJQ(params("ignore_exception", "true", "shards.tolerant", "true", "q", "*:*"
            , "json.facet", "{f:{type:terms, field:cat_s}}"
        )
        , "facets=={ count:6," +
            "f:{ buckets:[{val:B,count:3},{val:A,count:2}] }" +
            "}"
    );
  }

  @Test
  public void testBlockJoin() throws Exception {
    doBlockJoin(Client.localClient());
  }

  public void doBlockJoin(Client client) throws Exception {
    ModifiableSolrParams p = params("rows","0");

    client.deleteByQuery("*:*", null);

    SolrInputDocument parent;
    parent = sdoc("id", "1", "type_s","book", "book_s","A", "v_t","q");
    client.add(parent, null);

    parent = sdoc("id", "2", "type_s","book", "book_s","B", "v_t","q w");
    parent.addChildDocument( sdoc("id","2.1", "type_s","page", "page_s","a", "v_t","x y z")  );
    parent.addChildDocument( sdoc("id","2.2", "type_s","page", "page_s","b", "v_t","x y  ") );
    parent.addChildDocument( sdoc("id","2.3", "type_s","page", "page_s","c", "v_t","  y z" )  );
    client.add(parent, null);

    parent = sdoc("id", "3", "type_s","book", "book_s","C", "v_t","q w e");
    parent.addChildDocument( sdoc("id","3.1", "type_s","page", "page_s","d", "v_t","x    ")  );
    parent.addChildDocument( sdoc("id","3.2", "type_s","page", "page_s","e", "v_t","  y  ")  );
    parent.addChildDocument( sdoc("id","3.3", "type_s","page", "page_s","f", "v_t","    z")  );
    client.add(parent, null);

    parent = sdoc("id", "4", "type_s","book", "book_s","D", "v_t","e");
    client.add(parent, null);

    client.commit();

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ " +
                "pages:{ type:query, domain:{blockChildren:'type_s:book'} , facet:{ x:{field:v_t} } }" +
                ",pages2:{type:terms, field:v_t, domain:{blockChildren:'type_s:book'} }" +
                ",books:{ type:query, domain:{blockParent:'type_s:book'}  , facet:{ x:{field:v_t} } }" +
                ",books2:{type:terms, field:v_t, domain:{blockParent:'type_s:book'} }" +
                ",pageof3:{ type:query, q:'id:3', facet : { x : { type:terms, field:page_s, domain:{blockChildren:'type_s:book'}}} }" +
                ",bookof22:{ type:query, q:'id:2.2', facet : { x : { type:terms, field:book_s, domain:{blockParent:'type_s:book'}}} }" +
                ",missing_blockParent:{ type:query, domain:{blockParent:'type_s:does_not_exist'} }" +
                ",missing_blockChildren:{ type:query, domain:{blockChildren:'type_s:does_not_exist'} }" +
                "}"
        )
        , "facets=={ count:10" +
            ", pages:{count:6 , x:{buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ]}  }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", books:{count:4 , x:{buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ]}  }" +
            ", books2:{ buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ] }" +
            ", pageof3:{count:1 , x:{buckets:[ {val:d,count:1},{val:e,count:1},{val:f,count:1} ]}  }" +
            ", bookof22:{count:1 , x:{buckets:[ {val:B,count:1} ]}  }" +
            ", missing_blockParent:{count:0}" +
            ", missing_blockChildren:{count:0}" +
            "}"
    );

    // no matches in base query
    client.testJQ(params("q", "no_match_s:NO_MATCHES"
            , "json.facet", "{ processEmpty:true," +
                "pages:{ type:query, domain:{blockChildren:'type_s:book'} }" +
                ",books:{ type:query, domain:{blockParent:'type_s:book'} }" +
                "}"
        )
        , "facets=={ count:0" +
            ", pages:{count:0}" +
            ", books:{count:0}" +
            "}"
    );


    // test facet on children nested under terms facet on parents
    client.testJQ(params("q", "*:*"
            , "json.facet", "{" +
                "books:{ type:terms, field:book_s, facet:{ pages:{type:terms, field:v_t, domain:{blockChildren:'type_s:book'}} } }" +
                "}"
        )
        , "facets=={ count:10" +
            ", books:{buckets:[{val:A,count:1,pages:{buckets:[]}}" +
            "                 ,{val:B,count:1,pages:{buckets:[{val:y,count:3},{val:x,count:2},{val:z,count:2}]}}" +
            "                 ,{val:C,count:1,pages:{buckets:[{val:x,count:1},{val:y,count:1},{val:z,count:1}]}}" +
            "                 ,{val:D,count:1,pages:{buckets:[]}}"+
            "] }" +
            "}"
    );

    // test filter after block join
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            "pages1:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:'*:*'} }" +
            ",pages2:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:'-id:3.1'} }" +
            ",books:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:'*:*'} }" +
            ",books2:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:'id:1'} }" +
            "}"
        )
        , "facets=={ count:10" +
            ", pages1:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:z,count:3},{val:x,count:2} ] }" +
            ", books:{ buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ] }" +
            ", books2:{ buckets:[ {val:q,count:1} ] }" +
            "}"
    );


    // test other various ways to get filters
    client.testJQ(params(p, "q", "*:*", "f1","-id:3.1", "f2","id:1"
        , "json.facet", "{ " +
            "pages1:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:[]} }" +
            ",pages2:{type:terms, field:v_t, domain:{blockChildren:'type_s:book', filter:{param:f1} } }" +
            ",books:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:[{param:q},{param:missing_param}]} }" +
            ",books2:{type:terms, field:v_t, domain:{blockParent:'type_s:book', filter:[{param:f2}] } }" +
            "}"
        )
        , "facets=={ count:10" +
            ", pages1:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:z,count:3},{val:x,count:2} ] }" +
            ", books:{ buckets:[ {val:q,count:3},{val:e,count:2},{val:w,count:2} ] }" +
            ", books2:{ buckets:[ {val:q,count:1} ] }" +
            "}"
    );
  }

  /**
   * An explicit test for unique*(_root_) across all methods
   */
  public void testUniquesForMethod() throws Exception {
    final Client client = Client.localClient();

    final SolrParams p = params("rows","0");

    client.deleteByQuery("*:*", null);

    SolrInputDocument parent;
    parent = sdoc("id", "1", "type_s","book", "book_s","A", "v_t","q");
    client.add(parent, null);

    parent = sdoc("id", "2", "type_s","book", "book_s","B", "v_t","q w");
    parent.addChildDocument( sdoc("id","2.1", "type_s","page", "page_s","a", "v_t","x y z")  );
    parent.addChildDocument( sdoc("id","2.2", "type_s","page", "page_s","a", "v_t","x1   z")  );
    parent.addChildDocument( sdoc("id","2.3", "type_s","page", "page_s","a", "v_t","x2   z")  );
    parent.addChildDocument( sdoc("id","2.4", "type_s","page", "page_s","b", "v_t","x y  ") );
    parent.addChildDocument( sdoc("id","2.5", "type_s","page", "page_s","c", "v_t","  y z" )  );
    parent.addChildDocument( sdoc("id","2.6", "type_s","page", "page_s","c", "v_t","    z" )  );
    client.add(parent, null);

    parent = sdoc("id", "3", "type_s","book", "book_s","C", "v_t","q w e");
    parent.addChildDocument( sdoc("id","3.1", "type_s","page", "page_s","b", "v_t","x y  ") );
    parent.addChildDocument( sdoc("id","3.2", "type_s","page", "page_s","d", "v_t","x    ")  );
    parent.addChildDocument( sdoc("id","3.3", "type_s","page", "page_s","e", "v_t","  y  ")  );
    parent.addChildDocument( sdoc("id","3.4", "type_s","page", "page_s","f", "v_t","    z")  );
    client.add(parent, null);

    parent = sdoc("id", "4", "type_s","book", "book_s","D", "v_t","e");
    client.add(parent, null);

    client.commit();

    client.testJQ(params(p, "q", "type_s:page"
        , "json.facet", "{" +
            "  types: {" +
            "    type:terms," +
            "    field:type_s," +
            "    limit:-1," +
            "    facet: {" +
            "           in_books: \"unique(_root_)\"," +
            "           via_field:\"uniqueBlock(_root_)\","+
            "           via_query:\"uniqueBlock({!v=type_s:book})\" }"+
            "  }," +
            "  pages: {" +
            "    type:terms," +
            "    field:page_s," +
            "    limit:-1," +
            "    facet: {" +
            "           in_books: \"unique(_root_)\"," +
            "           via_field:\"uniqueBlock(_root_)\","+
            "           via_query:\"uniqueBlock({!v=type_s:book})\" }"+
            "  }" +
            "}" )

        , "response=={numFound:10,start:0,numFoundExact:true,docs:[]}"
        , "facets=={ count:10," +
            "types:{" +
            "    buckets:[ {val:page, count:10, in_books:2, via_field:2, via_query:2 } ]}" +
            "pages:{" +
            "    buckets:[ " +
            "     {val:a, count:3, in_books:1, via_field:1, via_query:1}," +
            "     {val:b, count:2, in_books:2, via_field:2, via_query:2}," +
            "     {val:c, count:2, in_books:1, via_field:1, via_query:1}," +
            "     {val:d, count:1, in_books:1, via_field:1, via_query:1}," +
            "     {val:e, count:1, in_books:1, via_field:1, via_query:1}," +
            "     {val:f, count:1, in_books:1, via_field:1, via_query:1}" +
            "    ]}" +
            "}"
    );
  }

  /**
   * Similar to {@link #testBlockJoin} but uses query time joining.
   * <p>
   * (asserts are slightly diff because if a query matches multiple types of documents, blockJoin domain switches
   * to parent/child domains preserve any existing parent/children from the original domain - eg: when q=*:*)
   * </p>
   */
  public void testQueryJoinBooksAndPages() throws Exception {

    final Client client = Client.localClient();

    final SolrParams p = params("rows","0");

    client.deleteByQuery("*:*", null);


    // build up a list of the docs we want to test with
    List<SolrInputDocument> docsToAdd = new ArrayList<>(10);
    docsToAdd.add(sdoc("id", "1", "type_s","book", "book_s","A", "v_t","q"));
    
    docsToAdd.add( sdoc("id", "2", "type_s","book", "book_s","B", "v_t","q w") );
    docsToAdd.add( sdoc("book_id_s", "2", "id", "2.1", "type_s","page", "page_s","a", "v_t","x y z") );
    docsToAdd.add( sdoc("book_id_s", "2", "id", "2.2", "type_s","page", "page_s","b", "v_t","x y  ") );
    docsToAdd.add( sdoc("book_id_s", "2", "id","2.3", "type_s","page", "page_s","c", "v_t","  y z" ) );

    docsToAdd.add( sdoc("id", "3", "type_s","book", "book_s","C", "v_t","q w e") );
    docsToAdd.add( sdoc("book_id_s", "3", "id","3.1", "type_s","page", "page_s","d", "v_t","x    ") );
    docsToAdd.add( sdoc("book_id_s", "3", "id","3.2", "type_s","page", "page_s","e", "v_t","  y  ") );
    docsToAdd.add( sdoc("book_id_s", "3", "id","3.3", "type_s","page", "page_s","f", "v_t","    z") );

    docsToAdd.add( sdoc("id", "4", "type_s","book", "book_s","D", "v_t","e") );
    
    // shuffle the docs since order shouldn't matter
    Collections.shuffle(docsToAdd, random());
    for (SolrInputDocument doc : docsToAdd) {
      client.add(doc, null);
    }
    client.commit();

    // the domains we'll be testing, initially setup for block join
    final String toChildren = "join: { from:'id', to:'book_id_s' }";
    final String toParents = "join: { from:'book_id_s', to:'id' }";
    final String toBogusChildren = "join: { from:'id', to:'does_not_exist_s' }";
    final String toBogusParents = "join: { from:'book_id_s', to:'does_not_exist_s' }";

    client.testJQ(params(p, "q", "*:*"
            , "json.facet", "{ " +
                "pages:{ type:query, domain:{"+toChildren+"} , facet:{ x:{field:v_t} } }" +
                ",pages2:{type:terms, field:v_t, domain:{"+toChildren+"} }" +
                ",books:{ type:query, domain:{"+toParents+"}  , facet:{ x:{field:v_t} } }" +
                ",books2:{type:terms, field:v_t, domain:{"+toParents+"} }" +
                ",pageof3:{ type:query, q:'id:3', facet : { x : { type:terms, field:page_s, domain:{"+toChildren+"}}} }" +
                ",bookof22:{ type:query, q:'id:2.2', facet : { x : { type:terms, field:book_s, domain:{"+toParents+"}}} }" +
                ",missing_Parents:{ type:query, domain:{"+toBogusParents+"} }" +
                ",missing_Children:{ type:query, domain:{"+toBogusChildren+"} }" +
                "}"
        )
        , "facets=={ count:10" +
            ", pages:{count:6 , x:{buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ]}  }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", books:{count:2 , x:{buckets:[ {val:q,count:2},{val:w,count:2},{val:e,count:1} ]}  }" +
            ", books2:{ buckets:[ {val:q,count:2},{val:w,count:2},{val:e,count:1} ] }" +
            ", pageof3:{count:1 , x:{buckets:[ {val:d,count:1},{val:e,count:1},{val:f,count:1} ]}  }" +
            ", bookof22:{count:1 , x:{buckets:[ {val:B,count:1} ]}  }" +
            ", missing_Parents:{count:0}" + 
            ", missing_Children:{count:0}" +
            "}"
    );

    // no matches in base query
    client.testJQ(params("q", "no_match_s:NO_MATCHES"
            , "json.facet", "{ processEmpty:true," +
                "pages:{ type:query, domain:{"+toChildren+"} }" +
                ",books:{ type:query, domain:{"+toParents+"} }" +
                "}"
        )
        , "facets=={ count:0" +
            ", pages:{count:0}" +
            ", books:{count:0}" +
            "}"
    );


    // test facet on children nested under terms facet on parents
    client.testJQ(params("q", "*:*"
            , "json.facet", "{" +
                "books:{ type:terms, field:book_s, facet:{ pages:{type:terms, field:v_t, domain:{"+toChildren+"}} } }" +
                "}"
        )
        , "facets=={ count:10" +
            ", books:{buckets:[{val:A,count:1,pages:{buckets:[]}}" +
            "                 ,{val:B,count:1,pages:{buckets:[{val:y,count:3},{val:x,count:2},{val:z,count:2}]}}" +
            "                 ,{val:C,count:1,pages:{buckets:[{val:x,count:1},{val:y,count:1},{val:z,count:1}]}}" +
            "                 ,{val:D,count:1,pages:{buckets:[]}}"+
            "] }" +
            "}"
    );

    // test filter after join
    client.testJQ(params(p, "q", "*:*"
        , "json.facet", "{ " +
            "pages1:{type:terms, field:v_t, domain:{"+toChildren+", filter:'*:*'} }" +
            ",pages2:{type:terms, field:v_t, domain:{"+toChildren+", filter:'-id:3.1'} }" +
            ",books:{type:terms, field:v_t, domain:{"+toParents+", filter:'*:*'} }" +
            ",books2:{type:terms, field:v_t, domain:{"+toParents+", filter:'id:2'} }" +
            "}"
        )
        , "facets=={ count:10" +
            ", pages1:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:z,count:3},{val:x,count:2} ] }" +
            ", books:{ buckets:[ {val:q,count:2},{val:w,count:2},{val:e,count:1} ] }" +
            ", books2:{ buckets:[ {val:q,count:1}, {val:w,count:1} ] }" +
            "}"
    );


    // test other various ways to get filters
    client.testJQ(params(p, "q", "*:*", "f1","-id:3.1", "f2","id:2"
        , "json.facet", "{ " +
            "pages1:{type:terms, field:v_t, domain:{"+toChildren+", filter:[]} }" +
            ",pages2:{type:terms, field:v_t, domain:{"+toChildren+", filter:{param:f1} } }" +
            ",books:{type:terms, field:v_t, domain:{"+toParents+", filter:[{param:q},{param:missing_param}]} }" +
            ",books2:{type:terms, field:v_t, domain:{"+toParents+", filter:[{param:f2}] } }" +
            "}"
        )
        , "facets=={ count:10" +
            ", pages1:{ buckets:[ {val:y,count:4},{val:x,count:3},{val:z,count:3} ] }" +
            ", pages2:{ buckets:[ {val:y,count:4},{val:z,count:3},{val:x,count:2} ] }" +
            ", books:{ buckets:[ {val:q,count:2},{val:w,count:2},{val:e,count:1} ] }" +
            ", books2:{ buckets:[ {val:q,count:1}, {val:w,count:1} ] }" +
            "}"
    );

  }

  public void XtestPercentiles() {
    AVLTreeDigest catA = new AVLTreeDigest(100);
    catA.add(4);
    catA.add(2);

    AVLTreeDigest catB = new AVLTreeDigest(100);
    catB.add(-9);
    catB.add(11);
    catB.add(-5);

    AVLTreeDigest all = new AVLTreeDigest(100);
    all.add(catA);
    all.add(catB);

    System.out.println(str(catA));
    System.out.println(str(catB));
    System.out.println(str(all));

    // 2.0 2.2 3.0 3.8 4.0
    // -9.0 -8.2 -5.0 7.800000000000001 11.0
    // -9.0 -7.3999999999999995 2.0 8.200000000000001 11.0
  }

  private static String str(AVLTreeDigest digest) {
    StringBuilder sb = new StringBuilder();
    for (double d : new double[] {0,.1,.5,.9,1}) {
      sb.append(" ").append(digest.quantile(d));
    }
    return sb.toString();
  }

  /*** test code to ensure TDigest is working as we expect. */

  public void XtestTDigest() throws Exception {
    AVLTreeDigest t1 = new AVLTreeDigest(100);
    t1.add(10, 1);
    t1.add(90, 1);
    t1.add(50, 1);

    System.out.println(t1.quantile(0.1));
    System.out.println(t1.quantile(0.5));
    System.out.println(t1.quantile(0.9));

    assertEquals(t1.quantile(0.5), 50.0, 0.01);

    AVLTreeDigest t2 = new AVLTreeDigest(100);
    t2.add(130, 1);
    t2.add(170, 1);
    t2.add(90, 1);

    System.out.println(t2.quantile(0.1));
    System.out.println(t2.quantile(0.5));
    System.out.println(t2.quantile(0.9));

    AVLTreeDigest top = new AVLTreeDigest(100);

    t1.compress();
    ByteBuffer buf = ByteBuffer.allocate(t1.byteSize()); // upper bound
    t1.asSmallBytes(buf);
    byte[] arr1 = Arrays.copyOf(buf.array(), buf.position());

    ByteBuffer rbuf = ByteBuffer.wrap(arr1);
    top.add(AVLTreeDigest.fromBytes(rbuf));

    System.out.println(top.quantile(0.1));
    System.out.println(top.quantile(0.5));
    System.out.println(top.quantile(0.9));

    t2.compress();
    ByteBuffer buf2 = ByteBuffer.allocate(t2.byteSize()); // upper bound
    t2.asSmallBytes(buf2);
    byte[] arr2 = Arrays.copyOf(buf2.array(), buf2.position());

    ByteBuffer rbuf2 = ByteBuffer.wrap(arr2);
    top.add(AVLTreeDigest.fromBytes(rbuf2));

    System.out.println(top.quantile(0.1));
    System.out.println(top.quantile(0.5));
    System.out.println(top.quantile(0.9));
  }

  public void XtestHLL() {
    HLLAgg.HLLFactory fac = new HLLAgg.HLLFactory();
    HLL hll = fac.getHLL();
    hll.addRaw(123456789);
    hll.addRaw(987654321);
  }


  /** atomicly resets the acctual AtomicLong value matches the expected and resets it to  0 */
  private static final void assertEqualsAndReset(String msg, long expected, AtomicLong actual) {
    final long current = actual.getAndSet(0);
    assertEquals(msg, expected, current);
  }
  /** atomicly resets the acctual AtomicLong value matches the expected and resets it to  0 */
  private static final void assertEqualsAndReset(long expected, AtomicLong actual) {
    final long current = actual.getAndSet(0);
    assertEquals(expected, current);
  }
  
}
