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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.noggit.JSONUtil;
import org.noggit.JSONWriter;
import org.noggit.JSONWriter.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.search.facet.RelatednessAgg.computeRelatedness;
import static org.apache.solr.search.facet.RelatednessAgg.roundTo5Digits;

/** 
 * <p>
 * A randomized test of nested facets using the <code>relatedness()</code> function, that asserts the 
 * accuracy the results for all the buckets returned using verification queries of the (expected) 
 * foreground &amp; background queries based on the nested facet terms.
 * <p>
 * Note that unlike normal facet "count" verification, using a high limit + overrequest isn't a substitute 
 * for refinement in order to ensure accurate "skg" computation across shards.  For that reason, this 
 * tests forces <code>refine: true</code> (unlike {@link TestCloudJSONFacetJoinDomain}) and specifies a
 * <code>domain: { 'query':'*:*' }</code> for every facet, in order to guarantee that all shards
 * participate in all facets, so that the popularity &amp; relatedness values returned can be proven 
 * with validation requests.
 * </p>
 * <p>
 * (Refinement alone is not enough. Using the '*:*' query as the facet domain is necessary to
 * prevent situations where a single shardX may return candidate bucket with no child-buckets due to 
 * the normal facet intersections, but when refined on other shardY(s), can produce "high scoring" 
 * SKG child-buckets, which would then be missing the foreground/background "size" contributions from 
 * shardX.
 * </p>
 * 
 * @see TestCloudJSONFacetJoinDomain
 * @see TestCloudJSONFacetSKGEquiv
 */
@Slow
public class TestCloudJSONFacetSKG extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  private static final int DEFAULT_LIMIT = FacetField.DEFAULT_FACET_LIMIT;
  private static final int MAX_FIELD_NUM = 15;
  private static final int UNIQUE_FIELD_VALS = 50;

  /** Multivalued string field suffixes that can be randomized for testing diff facet/join code paths */
  private static final String[] MULTI_STR_FIELD_SUFFIXES = new String[]
    { "_multi_ss", "_multi_sds", "_multi_sdsS" };
  /** Multivalued int field suffixes that can be randomized for testing diff facet/join code paths */
  private static final String[] MULTI_INT_FIELD_SUFFIXES = new String[]
    { "_multi_is", "_multi_ids", "_multi_idsS" };
  
  /** Single Valued string field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] SOLO_STR_FIELD_SUFFIXES = new String[]
    { "_solo_s", "_solo_sd", "_solo_sdS" };
  /** Single Valued int field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] SOLO_INT_FIELD_SUFFIXES = new String[]
    { "_solo_i", "_solo_id", "_solo_idS" };

  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static final ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    // sanity check constants
    assertTrue("bad test constants: some suffixes will never be tested",
               (MULTI_STR_FIELD_SUFFIXES.length < MAX_FIELD_NUM) &&
               (MULTI_INT_FIELD_SUFFIXES.length < MAX_FIELD_NUM) &&
               (SOLO_STR_FIELD_SUFFIXES.length < MAX_FIELD_NUM) &&
               (SOLO_INT_FIELD_SUFFIXES.length < MAX_FIELD_NUM));
    
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    // multi replicas should not matter...
    final int repFactor = usually() ? 1 : 2;
    // ... but we definitely want to test multiple shards
    final int numShards = TestUtil.nextInt(random(), 1, (usually() ? 2 :3));
    final int numNodes = (numShards * repFactor);
   
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");
    
    configureCluster(numNodes).addConfig(configName, configDir).configure();
    
    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig-tlog.xml");
    collectionProperties.put("schema", "schema_latest.xml");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    waitForRecoveriesToFinish(CLOUD_CLIENT);

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      CLIENTS.add(getHttpSolrClient(jetty.getBaseUrl() + "/" + COLLECTION_NAME + "/"));
    }

    final int numDocs = atLeast(100);
    for (int id = 0; id < numDocs; id++) {
      SolrInputDocument doc = sdoc("id", ""+id);
      for (int fieldNum = 0; fieldNum < MAX_FIELD_NUM; fieldNum++) {
        // NOTE: we ensure every doc has at least one value in each field
        // that way, if a term is returned for a parent there there is guaranteed to be at least one
        // one term in the child facet as well.
        //
        // otherwise, we'd face the risk of a single shardX returning parentTermX as a top term for
        // the parent facet, but having no child terms -- meanwhile on refinement another shardY that
        // did *not* returned parentTermX in phase#1, could return some *new* child terms under
        // parentTermX, but their stats would not include the bgCount from shardX.
        //
        // in normal operation, this is an edge case that isn't a big deal because the ratios &
        // relatedness scores are statistically approximate, but for the purpose of this test where
        // we verify correctness via exactness we need all shards to contribute to the SKG statistics
        final int numValsThisDoc = TestUtil.nextInt(random(), 1, (usually() ? 5 : 10));
        for (int v = 0; v < numValsThisDoc; v++) {
          final String fieldValue = randFieldValue(fieldNum);
          
          // multi valued: one string, and one integer
          doc.addField(multiStrField(fieldNum), fieldValue);
          doc.addField(multiIntField(fieldNum), fieldValue);
        }
        { // single valued: one string, and one integer
          final String fieldValue = randFieldValue(fieldNum);
          doc.addField(soloStrField(fieldNum), fieldValue);
          doc.addField(soloIntField(fieldNum), fieldValue);
        }
      }
      CLOUD_CLIENT.add(doc);
      if (random().nextInt(100) < 1) {
        CLOUD_CLIENT.commit();  // commit 1% of the time to create new segments
      }
      if (random().nextInt(100) < 5) {
        CLOUD_CLIENT.add(doc);  // duplicate the doc 5% of the time to create deleted docs
      }
    }
    CLOUD_CLIENT.commit();
  }

  /**
   * Given a (random) number, and a (static) array of possible suffixes returns a consistent field name that 
   * uses that number and one of hte specified suffixes in it's name.
   *
   * @see #MULTI_STR_FIELD_SUFFIXES
   * @see #MULTI_INT_FIELD_SUFFIXES
   * @see #MAX_FIELD_NUM
   * @see #randFieldValue
   */
  private static String field(final String[] suffixes, final int fieldNum) {
    assert fieldNum < MAX_FIELD_NUM;
    
    final String suffix = suffixes[fieldNum % suffixes.length];
    return "field_" + fieldNum + suffix;
  }
  /** Given a (random) number, returns a consistent field name for a multi valued string field */
  private static String multiStrField(final int fieldNum) {
    return field(MULTI_STR_FIELD_SUFFIXES, fieldNum);
  }
  /** Given a (random) number, returns a consistent field name for a multi valued int field */
  private static String multiIntField(final int fieldNum) {
    return field(MULTI_INT_FIELD_SUFFIXES, fieldNum);
  }
  /** Given a (random) number, returns a consistent field name for a single valued string field */
  private static String soloStrField(final int fieldNum) {
    return field(SOLO_STR_FIELD_SUFFIXES, fieldNum);
  }
  /** Given a (random) number, returns a consistent field name for a single valued int field */
  private static String soloIntField(final int fieldNum) {
    return field(SOLO_INT_FIELD_SUFFIXES, fieldNum);
  }

  /**
   * Given a (random) field number, returns a random (integer based) value for that field.
   * NOTE: The number of unique values in each field is constant according to {@link #UNIQUE_FIELD_VALS}
   * but the precise <em>range</em> of values will vary for each unique field number, such that cross field joins 
   * will match fewer documents based on how far apart the field numbers are.
   *
   * @see #UNIQUE_FIELD_VALS
   * @see #field
   */
  private static String randFieldValue(final int fieldNum) {
    return "" + (fieldNum + TestUtil.nextInt(random(), 1, UNIQUE_FIELD_VALS));
  }

  
  @AfterClass
  private static void afterClass() throws Exception {
    if (null != CLOUD_CLIENT) {
      CLOUD_CLIENT.close();
      CLOUD_CLIENT = null;
    }
    for (HttpSolrClient client : CLIENTS) {
      client.close();
    }
    CLIENTS.clear();
  }
  
  /** 
   * Test some small, hand crafted, but non-trivial queries that are
   * easier to trace/debug then a pure random monstrosity.
   * (ie: if something obvious gets broken, this test may fail faster and in a more obvious way then testRandom)
   */
  public void testBespoke() throws Exception {
    { // trivial single level facet
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(multiStrField(9), UNIQUE_FIELD_VALS, 0, null);
      facets.put("top1", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
      assertFacetSKGsAreCorrect(maxBuckets, facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS);
    }
    
    { // trivial single level facet w/sorting on skg
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(multiStrField(9), UNIQUE_FIELD_VALS, 0, "skg desc");
      facets.put("top2", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
      assertFacetSKGsAreCorrect(maxBuckets, facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS);
    }

    { // trivial single level facet w/ 2 diff ways to request "limit = (effectively) Infinite"
      // to sanity check refinement of buckets missing from other shard in both cases
      
      // NOTE that these two queries & facets *should* effectively identical given that the
      // very large limit value is big enough no shard will ever return that may terms,
      // but the "limit=-1" case it actually triggers slightly different code paths
      // because it causes FacetField.returnsPartial() to be "true"
      for (int limit : new int[] { 999999999, -1 }) {
        Map<String,TermFacet> facets = new LinkedHashMap<>();
        facets.put("top_facet_limit__" + limit, new TermFacet(multiStrField(9), limit, 0, "skg desc"));
        final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
        assertFacetSKGsAreCorrect(maxBuckets, facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
        assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS);
      }
    }
    { // allBuckets should have no impact...
      for (Boolean allBuckets : Arrays.asList( null, false, true )) {
        Map<String,TermFacet> facets = new LinkedHashMap<>();
        facets.put("allb__" + allBuckets, new TermFacet(multiStrField(9),
                                                        map("allBuckets", allBuckets,
                                                            "sort", "skg desc"))); 
        final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
        assertFacetSKGsAreCorrect(maxBuckets, facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
        assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS);
      }
    }
  }
  
  public void testRandom() throws Exception {

    // since the "cost" of verifying the stats for each bucket is so high (see TODO in verifySKGResults())
    // we put a safety valve in place on the maximum number of buckets that we are willing to verify
    // across *all* the queries that we do.
    // that way if the randomized queries we build all have relatively small facets, so be it, but if
    // we get a really big one early on, we can test as much as possible, skip other iterations.
    //
    // (deeply nested facets may contain more buckets then the max, but we won't *check* all of them)
    final int maxBucketsAllowed = atLeast(2000);
    final AtomicInteger maxBucketsToCheck = new AtomicInteger(maxBucketsAllowed);
    
    final int numIters = atLeast(10);
    for (int iter = 0; iter < numIters && 0 < maxBucketsToCheck.get(); iter++) {
      assertFacetSKGsAreCorrect(maxBucketsToCheck, TermFacet.buildRandomFacets(),
                                buildRandomQuery(), buildRandomQuery(), buildRandomQuery());
    }
    assertTrue("Didn't check a single bucket???", maxBucketsToCheck.get() < maxBucketsAllowed);
           

  }

  /**
   * Generates a random query string across the randomized fields/values in the index
   *
   * @see #randFieldValue
   * @see #field
   */
  private static String buildRandomQuery() {
    if (0 == TestUtil.nextInt(random(), 0,10)) {
      return "*:*";
    }
    final int numClauses = TestUtil.nextInt(random(), 3, 10);
    final String[] clauses = new String[numClauses];
    for (int c = 0; c < numClauses; c++) {
      final int fieldNum = random().nextInt(MAX_FIELD_NUM);
      // keep queries simple, just use str fields - not point of test
      clauses[c] = multiStrField(fieldNum) + ":" + randFieldValue(fieldNum);
    }
    return buildORQuery(clauses);
  }

  private static String buildORQuery(String... clauses) {
    assert 0 < clauses.length;
    return "(" + String.join(" OR ", clauses) + ")";
  }
  
  /**
   * Given a set of term facets, and top level query strings, asserts that 
   * the SKG stats for each facet term returned when executing that query with those foreground/background
   * queries match the expected results of executing the equivalent queries in isolation.
   *
   * @see #verifySKGResults
   */
  private void assertFacetSKGsAreCorrect(final AtomicInteger maxBucketsToCheck,
                                         Map<String,TermFacet> expected,
                                         final String query,
                                         final String foreQ,
                                         final String backQ) throws SolrServerException, IOException {
    final SolrParams baseParams = params("rows","0", "fore", foreQ, "back", backQ);
    
    final SolrParams facetParams = params("q", query,
                                          "json.facet", ""+TermFacet.toJSONFacetParamValue(expected));
    final SolrParams initParams = SolrParams.wrapAppended(facetParams, baseParams);
    
    log.info("Doing full run: {}", initParams);

    QueryResponse rsp = null;
    // JSON Facets not (currently) available from QueryResponse...
    @SuppressWarnings({"rawtypes"})
    NamedList topNamedList = null;
    try {
      rsp = (new QueryRequest(initParams)).process(getRandClient(random()));
      assertNotNull(initParams + " is null rsp?", rsp);
      topNamedList = rsp.getResponse();
      assertNotNull(initParams + " is null topNamedList?", topNamedList);
    } catch (Exception e) {
      throw new RuntimeException("init query failed: " + initParams + ": " + 
                                 e.getMessage(), e);
    }
    try {
      @SuppressWarnings({"rawtypes"})
      final NamedList facetResponse = (NamedList) topNamedList.get("facets");
      assertNotNull("null facet results?", facetResponse);
      assertEquals("numFound mismatch with top count?",
                   rsp.getResults().getNumFound(), ((Number)facetResponse.get("count")).longValue());

      // Note: even if the query has numFound=0, our explicit background query domain should
      // still force facet results
      // (even if the background query matches nothing, that just means there will be no
      // buckets in those facets)
      assertFacetSKGsAreCorrect(maxBucketsToCheck, expected, baseParams, facetResponse);
      
    } catch (AssertionError e) {
      throw new AssertionError(initParams + " ===> " + topNamedList + " --> " + e.getMessage(), e);
    } finally {
      log.info("Ending full run"); 
    }
  }

  /** 
   * Recursive helper method that walks the actual facet response, comparing the SKG results to 
   * the expected output based on the equivalent filters generated from the original TermFacet.
   */
  @SuppressWarnings({"unchecked"})
  private void assertFacetSKGsAreCorrect(final AtomicInteger maxBucketsToCheck,
                                         final Map<String,TermFacet> expected,
                                         final SolrParams baseParams,
                                         @SuppressWarnings({"rawtypes"})final NamedList actualFacetResponse) throws SolrServerException, IOException {

    for (Map.Entry<String,TermFacet> entry : expected.entrySet()) {
      final String facetKey = entry.getKey();
      final TermFacet facet = entry.getValue();
      
      @SuppressWarnings({"rawtypes"})
      final NamedList results = (NamedList) actualFacetResponse.get(facetKey);
      assertNotNull(facetKey + " key missing from: " + actualFacetResponse, results);

      if (null != results.get("allBuckets")) {
        // if the response includes an allBuckets bucket, then there must not be an skg value
        
        // 'skg' key must not exist in th allBuckets bucket
        assertEquals(facetKey + " has skg in allBuckets: " + results.get("allBuckets"),
                     Collections.emptyList(),
                     ((NamedList)results.get("allBuckets")).getAll("skg"));
      }
      @SuppressWarnings({"rawtypes"})
      final List<NamedList> buckets = (List<NamedList>) results.get("buckets");
      assertNotNull(facetKey + " has null buckets: " + actualFacetResponse, buckets);

      if (buckets.isEmpty()) {
        // should only happen if the background query does not match any docs with field X
        final long docsWithField = getNumFound(params("_trace", "noBuckets",
                                                      "rows", "0",
                                                      "q", facet.field+":[* TO *]",
                                                      "fq", baseParams.get("back")));

        assertEquals(facetKey + " has no buckets, but docs in background exist with field: " + facet.field,
                     0, docsWithField);
      }

      // NOTE: it's important that we do this depth first -- not just because it's the easiest way to do it,
      // but because it means that our maxBucketsToCheck will ensure we do a lot of deep sub-bucket checking,
      // not just all the buckets of the top level(s) facet(s)
      for (@SuppressWarnings({"rawtypes"})NamedList bucket : buckets) {
        final String fieldVal = bucket.get("val").toString(); // int or stringified int

        verifySKGResults(facetKey, facet, baseParams, fieldVal, bucket);
        if (maxBucketsToCheck.decrementAndGet() <= 0) {
          return;
        }
        
        final SolrParams verifyParams = SolrParams.wrapAppended(baseParams,
                                                                params("fq", facet.field + ":" + fieldVal));
        
        // recursively check subFacets
        if (! facet.subFacets.isEmpty()) {
          assertFacetSKGsAreCorrect(maxBucketsToCheck, facet.subFacets, verifyParams, bucket);
        }
      }
    }
    
    { // make sure we don't have any facet keys we don't expect
      // a little hackish because subfacets have extra keys...
      @SuppressWarnings({"rawtypes"})
      final LinkedHashSet expectedKeys = new LinkedHashSet(expected.keySet());
      expectedKeys.add("count");
      if (0 <= actualFacetResponse.indexOf("val",0)) {
        expectedKeys.add("val");
        expectedKeys.add("skg");
      }
      assertEquals("Unexpected keys in facet response",
                   expectedKeys, actualFacetResponse.asShallowMap().keySet());
    }
  }

  /**
   * Verifies that the popularity &amp; relatedness values containined in a single SKG bucket 
   * match the expected values based on the facet field &amp; bucket value, as well the existing 
   * filterParams.
   * 
   * @see #assertFacetSKGsAreCorrect
   */
  private void verifySKGResults(String facetKey, TermFacet facet, SolrParams filterParams,
                                String fieldVal, NamedList<Object> bucket)
    throws SolrServerException, IOException {

    final String bucketQ = facet.field+":"+fieldVal;
    @SuppressWarnings({"unchecked"})
    final NamedList<Object> skgBucket = (NamedList<Object>) bucket.get("skg");
    assertNotNull(facetKey + "/bucket:" + bucket.toString(), skgBucket);

    // TODO: make this more efficient?
    // ideally we'd do a single query w/4 facet.queries, one for each count
    // but formatting the queries is a pain, currently we leverage the accumulated fq's
    final long fgSize = getNumFound(SolrParams.wrapAppended(params("_trace", "fgSize",
                                                                   "rows","0",
                                                                   "q","{!query v=$fore}"),
                                                            filterParams));
    final long bgSize = getNumFound(params("_trace", "bgSize",
                                           "rows","0",
                                           "q", filterParams.get("back")));
    
    final long fgCount = getNumFound(SolrParams.wrapAppended(params("_trace", "fgCount",
                                                                   "rows","0",
                                                                    "q","{!query v=$fore}",
                                                                    "fq", bucketQ),
                                                             filterParams));
    final long bgCount = getNumFound(params("_trace", "bgCount",
                                            "rows","0",
                                            "q", bucketQ,
                                            "fq", filterParams.get("back")));

    assertEquals(facetKey + "/bucket:" + bucket + " => fgPop should be: " + fgCount + " / " + bgSize,
                 roundTo5Digits((double) fgCount / bgSize),
                 skgBucket.get("foreground_popularity"));
    assertEquals(facetKey + "/bucket:" + bucket + " => bgPop should be: " + bgCount + " / " + bgSize,
                 roundTo5Digits((double) bgCount / bgSize),
                 skgBucket.get("background_popularity"));
    assertEquals(facetKey + "/bucket:" + bucket + " => relatedness is wrong",
                 roundTo5Digits(computeRelatedness(fgCount, fgSize, bgCount, bgSize)),
                 skgBucket.get("relatedness"));
    
  }

  /**
   * Trivial data structure for modeling a simple terms facet that can be written out as a json.facet param.
   *
   * Doesn't do any string escaping or quoting, so don't use whitespace or reserved json characters
   */
  private static final class TermFacet implements Writable {

    /** non-skg subfacets for use in verification */
    public final Map<String,TermFacet> subFacets = new LinkedHashMap<>();
    
    private final Map<String,Object> jsonData = new LinkedHashMap<>();

    public final String field;
    /** 
     * @param field must be non null
     * @param options can set any of options used in a term facet other then field or (sub) facets
     */
    public TermFacet(final String field, final Map<String,Object> options) {
      assert null != field;
      this.field = field;
      
      jsonData.putAll(options);
      
      // we don't allow these to be overridden by options, so set them now...
      jsonData.put("type", "terms");
      jsonData.put("field", field);
      // see class javadocs for why we always use refine:true & the query:'*:*' domain for this test.
      jsonData.put("refine", true);
      jsonData.put("domain", map("query","*:*"));
      
    }

    /** all params except field can be null */
    public TermFacet(String field, Integer limit, Integer overrequest, String sort) {
      this(field, map("limit", limit, "overrequest", overrequest, "sort", sort));
    }
    
    /** Simplified constructor asks for limit = # unique vals */
    public TermFacet(String field) {
      this(field, UNIQUE_FIELD_VALS, 0, "skg desc"); 
      
    }
    @Override
    public void write(JSONWriter writer) {
      // we need to include both our "real" subfacets, along with our SKG stat and 'processEmpty'
      // (we don't put these in 'subFacets' to help keep the verification code simpler
      final Map<String,Object> sub = map("processEmpty", true,
                                         "skg", "relatedness($fore,$back)");
      sub.putAll(subFacets);
      
      final Map<String,Object> out = map("facet", sub);
      out.putAll(jsonData);
      
      writer.write(out);
    }

    /**
     * Given a set of (possibly nested) facets, generates a suitable <code>json.facet</code> param value to 
     * use for testing them against in a solr request.
     */
    public static String toJSONFacetParamValue(final Map<String,TermFacet> facets) {
      assert null != facets;
      assert ! facets.isEmpty();
      
      // see class javadocs for why we always want processEmpty
      final Map<String,Object> jsonData = map("processEmpty", true);
      jsonData.putAll(facets);
      
      return JSONUtil.toJSON(jsonData, -1); // no newlines
    }

    /**
     * Factory method for generating some random facets.  
     *
     * For simplicity, each facet will have a unique key name.
     */
    public static Map<String,TermFacet> buildRandomFacets() {
      // for simplicity, use a unique facet key regardless of depth - simplifies verification
      // and le's us enforce a hard limit on the total number of facets in a request
      AtomicInteger keyCounter = new AtomicInteger(0);
      
      final int maxDepth = TestUtil.nextInt(random(), 0, (usually() ? 2 : 3));
      return buildRandomFacets(keyCounter, maxDepth);
    }
    
    /**
     * picks a random field to facet on.
     *
     * @see #field
     * @return field name, never null
     */
    public static String randomFacetField(final Random r) {
      final int fieldNum = r.nextInt(MAX_FIELD_NUM);
      switch(r.nextInt(4)) {
        case 0: return multiStrField(fieldNum);
        case 1: return multiIntField(fieldNum);
        case 2: return soloStrField(fieldNum);
        case 3: return soloIntField(fieldNum);
        default: throw new RuntimeException("Broken case statement");
      }
    }

    /**
     * picks a random value for the "perSeg" param, biased in favor of interesting test cases
     *
     * @return a Boolean, may be null
     */
    public static Boolean randomPerSegParam(final Random r) {

      switch(r.nextInt(4)) {
        case 0: return true;
        case 1: return false;
        case 2: 
        case 3: return null;
        default: throw new RuntimeException("Broken case statement");
      }
    }
    
    /**
     * picks a random value for the "prefix" param, biased in favor of interesting test cases
     *
     * @return a valid prefix value, may be null
     */
    public static String randomPrefixParam(final Random r, final String facetField) {
      
      if (facetField.contains("multi_i") || facetField.contains("solo_i")) {
        // never used a prefix on a numeric field
        return null;
      }
      assert (facetField.contains("multi_s") || facetField.contains("solo_s"))
        : "possible facet fields have changed, breaking test";
      
      switch(r.nextInt(5)) {
        case 0: return "2";
        case 1: return "3";
        case 2: 
        case 3: 
        case 4: return null;
        default: throw new RuntimeException("Broken case statement");
      }
    }
    
    /**
     * picks a random value for the "prelim_sort" param, biased in favor of interesting test cases.  
     *
     * @return a sort string (w/direction), or null to specify nothing (trigger default behavior)
     * @see #randomSortParam
     */
    public static String randomPrelimSortParam(final Random r, final String sort) {

      if (null != sort && sort.startsWith("skg") && 1 == TestUtil.nextInt(random(), 0, 3)) {
        return "count desc";
      }
      return null;
    }
    /**
     * picks a random value for the "sort" param, biased in favor of interesting test cases
     *
     * @return a sort string (w/direction), or null to specify nothing (trigger default behavior)
     * @see #randomLimitParam
     * @see #randomAllBucketsParam
     * @see #randomPrelimSortParam
     */
    public static String randomSortParam(Random r) {

      // IMPORTANT!!!
      // if this method is modified to produce new sorts, make sure to update
      // randomLimitParam to account for them if they are impacted by SOLR-12556
      final String dir = random().nextBoolean() ? "asc" : "desc";
      switch(r.nextInt(4)) {
        case 0: return null;
        case 1: return "count " + dir;
        case 2: return "skg " + dir;
        case 3: return "index " + dir;
        default: throw new RuntimeException("Broken case statement");
      }
    }
    /**
     * picks a random value for the "limit" param, biased in favor of interesting test cases
     *
     * <p>
     * <b>NOTE:</b> Due to SOLR-12556, we have to force an overrequest of "all" possible terms for 
     * some sort values.
     * </p>
     *
     * @return a number to specify in the request, or null to specify nothing (trigger default behavior)
     * @see #UNIQUE_FIELD_VALS
     * @see #randomSortParam
     */
    public static Integer randomLimitParam(Random r, final String sort) {
      if (null != sort) {
        if (sort.equals("count asc") || sort.startsWith("skg")) {
          // of the known types of sorts produced, these are at risk of SOLR-12556
          // so request (effectively) unlimited num buckets
          return r.nextBoolean() ? UNIQUE_FIELD_VALS : -1;
        }
      }
      final int limit = 1 + r.nextInt((int) (UNIQUE_FIELD_VALS * 1.5F));
      if (limit >= UNIQUE_FIELD_VALS && r.nextBoolean()) {
        return -1; // unlimited
      } else if (limit == DEFAULT_LIMIT && r.nextBoolean()) { 
        return null; // sometimes, don't specify limit if it's the default
      }
      return limit;
    }
    
    /**
     * picks a random value for the "overrequest" param, biased in favor of interesting test cases.
     *
     * @return a number to specify in the request, or null to specify nothing (trigger default behavior)
     * @see #UNIQUE_FIELD_VALS
     */
    public static Integer randomOverrequestParam(Random r) {
      switch(r.nextInt(10)) {
        case 0:
        case 1:
        case 2:
        case 3:
          return 0; // 40% of the time, disable overrequest to better stress refinement
        case 4:
        case 5:
          return r.nextInt(UNIQUE_FIELD_VALS); // 20% ask for less them what's needed
        case 6:
          return r.nextInt(Integer.MAX_VALUE); // 10%: completley random value, statisticaly more then enough
        default: break;
      }
      // else.... either leave param unspecified (or redundently specify the -1 default)
      return r.nextBoolean() ? null : -1;
    }
    
    /**
     * picks a random value for the "allBuckets" param, biased in favor of interesting test cases.  
     * This bucket should be ignored by relatedness, but inclusion should not cause any problems 
     * (or change the results)
     *
     * <p>
     * <b>NOTE:</b> allBuckets is meaningless in conjunction with the <code>STREAM</code> processor, so
     * this method always returns null if sort is <code>index asc</code>.
     * </p>
     *
     *
     * @return a Boolean, may be null
     */
    public static Boolean randomAllBucketsParam(final Random r, final String sort) {
      switch(r.nextInt(4)) {
        case 0: return true;
        case 1: return false;
        case 2: 
        case 3: return null;
        default: throw new RuntimeException("Broken case statement");
      }
    }

    /** 
     * recursive helper method for building random facets
     *
     * @param keyCounter used to ensure every generated facet has a unique key name
     * @param maxDepth max possible depth allowed for the recusion, a lower value may be used depending on how many facets are returned at the current level. 
     */
    private static Map<String,TermFacet> buildRandomFacets(AtomicInteger keyCounter, int maxDepth) {
      final int numFacets = Math.max(1, TestUtil.nextInt(random(), -1, 3)); // 3/5th chance of being '1'
      Map<String,TermFacet> results = new LinkedHashMap<>();
      for (int i = 0; i < numFacets; i++) {
        if (keyCounter.get() < 3) { // a hard limit on the total number of facets (regardless of depth) to reduce OOM risk
          
          final String sort = randomSortParam(random());
          final String facetField = randomFacetField(random());
          final TermFacet facet =  new TermFacet(facetField,
                                                 map("sort", sort,
                                                     "prelim_sort", randomPrelimSortParam(random(), sort),
                                                     "limit", randomLimitParam(random(), sort),
                                                     "overrequest", randomOverrequestParam(random()),
                                                     "prefix", randomPrefixParam(random(), facetField),
                                                     "allBuckets", randomAllBucketsParam(random(), sort),
                                                     "perSeg", randomPerSegParam(random())));
                                                     

                                                 
          results.put("facet_" + keyCounter.incrementAndGet(), facet);
          if (0 < maxDepth) {
            // if we're going wide, don't go deep
            final int nextMaxDepth = Math.max(0, maxDepth - numFacets);
            facet.subFacets.putAll(buildRandomFacets(keyCounter, TestUtil.nextInt(random(), 0, nextMaxDepth)));
          }
        }
      }
      return results;
    }
  }

  /** 
   * returns a random SolrClient -- either a CloudSolrClient, or an HttpSolrClient pointed 
   * at a node in our cluster 
   */
  public static SolrClient getRandClient(Random rand) {
    int numClients = CLIENTS.size();
    int idx = TestUtil.nextInt(rand, 0, numClients);

    return (idx == numClients) ? CLOUD_CLIENT : CLIENTS.get(idx);
  }

  /**
   * Uses a random SolrClient to execture a request and returns only the numFound
   * @see #getRandClient
   */
  public static long getNumFound(final SolrParams req) throws SolrServerException, IOException {
    return getRandClient(random()).query(req).getResults().getNumFound();
  }
  
  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assert null != client.getDefaultCollection();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(client.getDefaultCollection(),
                                                        client.getZkStateReader(),
                                                        true, true, 330);
  }
  
  /** helper macro: fails on null keys, skips pairs with null values  */
  public static Map<String,Object> map(Object... pairs) {
    if (0 != pairs.length % 2) throw new IllegalArgumentException("uneven number of arguments");
    final Map<String,Object> map = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i+=2) {
      final Object key = pairs[i];
      final Object val = pairs[i+1];
      if (null == key) throw new NullPointerException("arguemnt " + i);
      if (null == val) continue;
      
      map.put(key.toString(), val);
    }
    return map;
  }

}
