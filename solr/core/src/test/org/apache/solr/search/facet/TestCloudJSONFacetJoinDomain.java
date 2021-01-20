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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.solr.cloud.TestCloudPivotFacet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * <p>
 * Tests randomized JSON Facets, sometimes using query 'join' domain transfers and/or domain 'filter' options
 * </p>
 * <p>
 * The results of each facet constraint count will be compared with a verification query using an equivalent filter
 * </p>
 * 
 * @see TestCloudPivotFacet
 */
public class TestCloudJSONFacetJoinDomain extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  private static final int DEFAULT_LIMIT = FacetField.DEFAULT_FACET_LIMIT;
  private static final int MAX_FIELD_NUM = 15;
  private static final int UNIQUE_FIELD_VALS = 20;

  // NOTE: set to 'true' to see if refinement testing is adequate (should get fails occasionally)
  private static final boolean FORCE_DISABLE_REFINEMENT = false;
  
  /** Multivalued string field suffixes that can be randomized for testing diff facet/join code paths */
  private static final String[] STR_FIELD_SUFFIXES = new String[] { "_ss", "_sds", "_sdsS" };
  /** Multivalued int field suffixes that can be randomized for testing diff facet/join code paths */
  private static final String[] INT_FIELD_SUFFIXES = new String[] { "_is", "_ids", "_idsS" };
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static final ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    // sanity check constants
    assertTrue("bad test constants: some suffixes will never be tested",
               (STR_FIELD_SUFFIXES.length < MAX_FIELD_NUM) && (INT_FIELD_SUFFIXES.length < MAX_FIELD_NUM));
    
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
        // NOTE: some docs may have no value in a field
        final int numValsThisDoc = TestUtil.nextInt(random(), 0, (usually() ? 3 : 6));
        for (int v = 0; v < numValsThisDoc; v++) {
          final String fieldValue = randFieldValue(fieldNum);
          
          // for each fieldNum, there are actaully two fields: one string, and one integer
          doc.addField(field(STR_FIELD_SUFFIXES, fieldNum), fieldValue);
          doc.addField(field(INT_FIELD_SUFFIXES, fieldNum), fieldValue);
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
   * @see #STR_FIELD_SUFFIXES
   * @see #INT_FIELD_SUFFIXES
   * @see #MAX_FIELD_NUM
   * @see #randFieldValue
   */
  private static String field(final String[] suffixes, final int fieldNum) {
    assert fieldNum < MAX_FIELD_NUM;
    
    final String suffix = suffixes[fieldNum % suffixes.length];
    return "field_" + fieldNum + suffix;
  }
  private static String strfield(final int fieldNum) {
    return field(STR_FIELD_SUFFIXES, fieldNum);
  }
  private static String intfield(final int fieldNum) {
    return field(INT_FIELD_SUFFIXES, fieldNum);
  }

  /**
   * Given a (random) field number, returns a random (integer based) value for that field.
   * NOTE: The number of unique values in each field is constant acording to {@link #UNIQUE_FIELD_VALS}
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

  /** Sanity check that malformed requests produce errors */
  public void testMalformedGivesError() throws Exception {

    ignoreException(".*'join' domain change.*");
    
    for (String join : Arrays.asList("bogus",
                                     "{ }",
                                     "{ from:null, to:foo_s }",
                                     "{ from:foo_s }",
                                     "{ from:foo_s, to:foo_s, bogus:'what what?' }",
                                     "{ to:foo_s, bogus:'what what?' }")) {
      SolrException e = expectThrows(SolrException.class, () -> {
          final SolrParams req = params("q", "*:*", "json.facet",
                                        "{ x : { type:terms, field:x_s, domain: { join:"+join+" } } }");
          getRandClient(random()).request(new QueryRequest(req));
        });
      assertEquals(join + " -> " + e, SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(join + " -> " + e, e.getMessage().contains("'join' domain change"));
    }
  }

  public void testJoinMethodSyntax() throws Exception {
    // 'method' value that doesn't exist at all
    {
      final String joinJson = "{from:foo, to:bar, method:invalidValue}";
      SolrException e = expectThrows(SolrException.class, () -> {
        final SolrParams req = params("q", "*:*", "json.facet",
            "{ x : { type:terms, field:x_s, domain: { join:"+joinJson+" } } }");
        getRandClient(random()).request(new QueryRequest(req));
      });
      assertEquals(joinJson + " -> " + e, SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(joinJson + " -> " + e, e.getMessage().contains("join method 'invalidValue' not supported"));
    }

    // 'method' value that exists on joins generally but isn't supported for join domain transforms
    {
      final String joinJson = "{from:foo, to:bar, method:crossCollection}";
      SolrException e = expectThrows(SolrException.class, () -> {
        final SolrParams req = params("q", "*:*", "json.facet",
            "{ x : { type:terms, field:x_s, domain: { join:"+joinJson+" } } }");
        getRandClient(random()).request(new QueryRequest(req));
      });
      assertEquals(joinJson + " -> " + e, SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(joinJson + " -> " + e, e.getMessage().contains("Join method crossCollection not supported"));
    }


    // Valid, supported method value
    {
      final String joinJson = "{from:" +strfield(1)+ ", to:"+strfield(1)+", method:index}";
        final SolrParams req = params("q", "*:*", "json.facet", "{ x : { type:terms, field:x_s, domain: { join:"+joinJson+" } } }");
        getRandClient(random()).request(new QueryRequest(req));
        // For the purposes of this test, we're not interested in the response so much as that Solr will accept a valid 'method' value
    }
  }

  public void testSanityCheckDomainMethods() throws Exception {
    { 
      final JoinDomain empty = new JoinDomain(null, null, null);
      assertEquals(null, empty.toJSONFacetParamValue());
      final SolrParams out = empty.applyDomainToQuery("safe_key", params("q","qqq"));
      assertNotNull(out);
      assertEquals(null, out.get("safe_key"));
      assertEquals("qqq", out.get("q"));
    }
    {
      final JoinDomain join = new JoinDomain("xxx", "yyy", null);
      assertEquals("domain:{join:{from:xxx,to:yyy}}", join.toJSONFacetParamValue().toString());
      final SolrParams out = join.applyDomainToQuery("safe_key", params("q","qqq"));
      assertNotNull(out);
      assertEquals("qqq", out.get("safe_key"));
      assertEquals("{!join from=xxx to=yyy v=$safe_key}", out.get("q"));
      
    }
    {
      final JoinDomain filter = new JoinDomain(null, null, "zzz");
      assertEquals("domain:{filter:'zzz'}", filter.toJSONFacetParamValue().toString());
      final SolrParams out = filter.applyDomainToQuery("safe_key", params("q","qqq"));
      assertNotNull(out);
      assertEquals(null, out.get("safe_key"));
      assertEquals("zzz AND qqq", out.get("q"));
    }
    {
      final JoinDomain both = new JoinDomain("xxx", "yyy", "zzz");
      assertEquals("domain:{join:{from:xxx,to:yyy},filter:'zzz'}", both.toJSONFacetParamValue().toString());
      final SolrParams out = both.applyDomainToQuery("safe_key", params("q","qqq"));
      assertNotNull(out);
      assertEquals("qqq", out.get("safe_key"));
      assertEquals("zzz AND {!join from=xxx to=yyy v=$safe_key}", out.get("q"));
    }
  }

  /** 
   * Test some small, hand crafted, but non-trivial queries that are
   * easier to trace/debug then a pure random monstrosity.
   * (ie: if something obvious gets broken, this test may fail faster and in a more obvious way then testRandom)
   */
  public void testBespoke() throws Exception {

    { // sanity check our test methods can handle a query matching no docs
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(strfield(5), strfield(9), strfield(9)+":[* TO *]"));
      top.subFacets.put("sub", new TermFacet(strfield(11), new JoinDomain(strfield(8), strfield(8), null)));
      facets.put("empty_top", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
      assertFacetCountsAreCorrect(maxBuckets, facets, strfield(7) + ":bogus");
      assertEquals("Empty search result shouldn't have found a single bucket",
                   UNIQUE_FIELD_VALS, maxBuckets.get());
    }
    
    { // sanity check our test methods can handle a query where a facet filter prevents any doc from having terms
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(null, null, "-*:*"));
      top.subFacets.put("sub", new TermFacet(strfield(11), new JoinDomain(strfield(8), strfield(8), null)));
      facets.put("filtered_top", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
      assertFacetCountsAreCorrect(maxBuckets, facets, "*:*");
      assertEquals("Empty join filter shouldn't have found a single bucket",
                   UNIQUE_FIELD_VALS, maxBuckets.get());
    }
    
    { // sanity check our test methods can handle a query where a facet filter prevents any doc from having sub-terms
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(strfield(8), strfield(8), null));
      top.subFacets.put("sub", new TermFacet(strfield(11), new JoinDomain(null, null, "-*:*")));
      facets.put("filtered_top", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS);
      assertFacetCountsAreCorrect(maxBuckets, facets, "*:*");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS);
    }
  
    { // strings
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(strfield(5), strfield(9), strfield(9)+":[* TO *]"));
      top.subFacets.put("facet_5", new TermFacet(strfield(11), new JoinDomain(strfield(8), strfield(8), null)));
      facets.put("facet_4", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS * UNIQUE_FIELD_VALS);
      assertFacetCountsAreCorrect(maxBuckets, facets, "("+strfield(7)+":16 OR "+strfield(9)+":16 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS * UNIQUE_FIELD_VALS);
    }

    { // ints
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(intfield(9), new JoinDomain(intfield(5), intfield(9), null));
      facets.put("top", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS * UNIQUE_FIELD_VALS);
      assertFacetCountsAreCorrect(maxBuckets, facets, "("+intfield(7)+":16 OR "+intfield(3)+":13)");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS * UNIQUE_FIELD_VALS);
    }

    { // some domains with filter only, no actual join
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(null, null, strfield(9)+":[* TO *]"));
      top.subFacets.put("facet_5", new TermFacet(strfield(11), new JoinDomain(null, null, strfield(3)+":[* TO 5]")));
      facets.put("top", top);
      final AtomicInteger maxBuckets = new AtomicInteger(UNIQUE_FIELD_VALS * UNIQUE_FIELD_VALS);
      assertFacetCountsAreCorrect(maxBuckets, facets, "("+strfield(7)+":16 OR "+strfield(9)+":16 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < UNIQUE_FIELD_VALS * UNIQUE_FIELD_VALS);

    }

    { // low limits, explicit refinement
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9),
                                    new JoinDomain(strfield(5), strfield(9), strfield(9)+":[* TO *]"),
                                    5, 0, true);
      top.subFacets.put("facet_5", new TermFacet(strfield(11),
                                                 new JoinDomain(strfield(8), strfield(8), null),
                                                 10, 0, true));
      facets.put("facet_4", top);
      final AtomicInteger maxBuckets = new AtomicInteger(5 * 10);
      assertFacetCountsAreCorrect(maxBuckets, facets, "("+strfield(7)+":6 OR "+strfield(9)+":6 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < 5 * 10);
    }
    
    { // low limit, high overrequest
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9),
                                    new JoinDomain(strfield(5), strfield(9), strfield(9)+":[* TO *]"),
                                    5, UNIQUE_FIELD_VALS + 10, false);
      top.subFacets.put("facet_5", new TermFacet(strfield(11),
                                                 new JoinDomain(strfield(8), strfield(8), null),
                                                 10, UNIQUE_FIELD_VALS + 10, false));
      facets.put("facet_4", top);
      final AtomicInteger maxBuckets = new AtomicInteger(5 * 10);
      assertFacetCountsAreCorrect(maxBuckets, facets, "("+strfield(7)+":6 OR "+strfield(9)+":6 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < 5 * 10);
    }
    
    { // low limit, low overrequest, explicit refinement
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9),
                                    new JoinDomain(strfield(5), strfield(9), strfield(9)+":[* TO *]"),
                                    5, 7, true);
      top.subFacets.put("facet_5", new TermFacet(strfield(11),
                                                 new JoinDomain(strfield(8), strfield(8), null),
                                                 10, 7, true));
      facets.put("facet_4", top);
      final AtomicInteger maxBuckets = new AtomicInteger(5 * 10);
      assertFacetCountsAreCorrect(maxBuckets, facets, "("+strfield(7)+":6 OR "+strfield(9)+":6 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");
      assertTrue("Didn't check a single bucket???", maxBuckets.get() < 5 * 10);
    }
    
  }

  public void testTheTestRandomRefineParam() {
    // sanity check that randomRefineParam never violates isRefinementNeeded
    // (should be imposisble ... unless someone changes/breaks the randomization logic in the future)
    final int numIters = atLeast(100);
    for (int iter = 0; iter < numIters; iter++) {
      final Integer limit = TermFacet.randomLimitParam(random());
      final Integer overrequest = TermFacet.randomOverrequestParam(random());
      final Boolean refine = TermFacet.randomRefineParam(random(), limit, overrequest);
      if (TermFacet.isRefinementNeeded(limit, overrequest)) {
        assertEquals("limit: " + limit + ", overrequest: " + overrequest + ", refine: " + refine,
                     Boolean.TRUE, refine);
      }
    }
  }
  
  public void testTheTestTermFacetShouldFreakOutOnBadRefineOptions() {
    expectThrows(AssertionError.class, () -> {
        final TermFacet bogus = new TermFacet("foo", null, 5, 0, false);
      });
  }

  public void testRandom() throws Exception {

    // we put a safety valve in place on the maximum number of buckets that we are willing to verify
    // across *all* the queries that we do.
    // that way if the randomized queries we build all have relatively small facets, so be it, but if
    // we get a really big one early on, we can test as much as possible, skip other iterations.
    //
    // (deeply nested facets may contain more buckets then the max, but we won't *check* all of them)
    final int maxBucketsAllowed = atLeast(2000);
    final AtomicInteger maxBucketsToCheck = new AtomicInteger(maxBucketsAllowed);
    
    final int numIters = atLeast(20);
    for (int iter = 0; iter < numIters && 0 < maxBucketsToCheck.get(); iter++) {
      assertFacetCountsAreCorrect(maxBucketsToCheck, TermFacet.buildRandomFacets(), buildRandomQuery());
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
    List<String> clauses = new ArrayList<String>(numClauses);
    for (int c = 0; c < numClauses; c++) {
      final int fieldNum = random().nextInt(MAX_FIELD_NUM);
      // keep queries simple, just use str fields - not point of test
      clauses.add(strfield(fieldNum) + ":" + randFieldValue(fieldNum));
    }
    return "(" + String.join(" OR ", clauses) + ")";
  }
  
  /**
   * Given a set of (potentially nested) term facets, and a base query string, asserts that 
   * the actual counts returned when executing that query with those facets match the expected results
   * of filtering on the equivalent facet terms+domain
   */
  private void assertFacetCountsAreCorrect(final AtomicInteger maxBucketsToCheck,
                                           Map<String,TermFacet> expected,
                                           final String query) throws SolrServerException, IOException {

    final SolrParams baseParams = params("q", query, "rows","0");
    final SolrParams facetParams = params("json.facet", ""+TermFacet.toJSONFacetParamValue(expected));
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
      if (0 == rsp.getResults().getNumFound()) {
        // when the query matches nothing, we should expect no top level facets
        expected = Collections.emptyMap();
      }
      assertFacetCountsAreCorrect(maxBucketsToCheck, expected, baseParams, facetResponse);
    } catch (AssertionError e) {
      throw new AssertionError(initParams + " ===> " + topNamedList + " --> " + e.getMessage(), e);
    } finally {
      log.info("Ending full run"); 
    }
  }

  /** 
   * Recursive Helper method that walks the actual facet response, comparing the counts to the expected output 
   * based on the equivalent filters generated from the original TermFacet.
   */
  private void assertFacetCountsAreCorrect(final AtomicInteger maxBucketsToCheck,
                                           final Map<String,TermFacet> expected,
                                           final SolrParams baseParams,
                                           @SuppressWarnings({"rawtypes"})final NamedList actualFacetResponse) throws SolrServerException, IOException {

    for (Map.Entry<String,TermFacet> entry : expected.entrySet()) {
      final String facetKey = entry.getKey();
      final TermFacet facet = entry.getValue();
      @SuppressWarnings({"rawtypes"})
      final NamedList results = (NamedList) actualFacetResponse.get(facetKey);
      assertNotNull(facetKey + " key missing from: " + actualFacetResponse, results);
      @SuppressWarnings({"unchecked", "rawtypes"})
      final List<NamedList> buckets = (List<NamedList>) results.get("buckets");
      assertNotNull(facetKey + " has null buckets: " + actualFacetResponse, buckets);

      if (buckets.isEmpty()) {
        // should only happen if the baseParams query does not match any docs with our field X
        final long docsWithField = getRandClient(random()).query
          (facet.applyValueConstraintAndDomain(baseParams, facetKey, "[* TO *]")).getResults().getNumFound();
        assertEquals(facetKey + " has no buckets, but docs in query exist with field: " + facet.field,
                     0, docsWithField);
      }
      
      for (@SuppressWarnings({"rawtypes"})NamedList bucket : buckets) {
        final long count = ((Number) bucket.get("count")).longValue();
        final String fieldVal = bucket.get("val").toString(); // int or stringified int

        // change our query to filter on the fieldVal, and wrap in the facet domain (if any)
        final SolrParams verifyParams = facet.applyValueConstraintAndDomain(baseParams, facetKey, fieldVal);

        // check the count for this bucket
        assertEquals(facetKey + ": " + verifyParams,
                     count, getRandClient(random()).query(verifyParams).getResults().getNumFound());

        if (maxBucketsToCheck.decrementAndGet() <= 0) {
          return;
        }
        
        // recursively check subFacets
        if (! facet.subFacets.isEmpty()) {
          assertFacetCountsAreCorrect(maxBucketsToCheck, facet.subFacets, verifyParams, bucket);
        }
      }
    }
    assertTrue("facets have unexpected keys left over: " + actualFacetResponse,
               // should alwasy be a count, maybe a 'val' if we're a subfacet
               (actualFacetResponse.size() == expected.size() + 1) ||
               (actualFacetResponse.size() == expected.size() + 2));
  }

  
  /**
   * Trivial data structure for modeling a simple terms facet that can be written out as a json.facet param.
   *
   * Doesn't do any string escaping or quoting, so don't use whitespace or reserved json characters
   */
  private static final class TermFacet {
    public final String field;
    public final Map<String,TermFacet> subFacets = new LinkedHashMap<>();
    public final JoinDomain domain; // may be null
    public final Integer limit; // may be null
    public final Integer overrequest; // may be null
    public final Boolean refine; // may be null

    /** Simplified constructor asks for limit = # unique vals */
    public TermFacet(String field, JoinDomain domain) {
      this(field, domain, UNIQUE_FIELD_VALS, 0, false);
    }
    public TermFacet(String field, JoinDomain domain, Integer limit, Integer overrequest, Boolean refine) {
      assert null != field;
      this.field = field;
      this.domain = domain;
      this.limit = limit;
      this.overrequest = overrequest;
      this.refine = refine;
      if (isRefinementNeeded(limit, overrequest)) {
        assertEquals("Invalid refine param based on limit & overrequest: " + this.toString(),
                     Boolean.TRUE, refine);
      }
    }

    /** 
     * Returns new SolrParams that:
     * <ul>
     *  <li>copy the original SolrParams</li>
     *  <li>modify/wrap the original "q" param to capture the domain change for this facet (if any)</li>
     *  <li>add a filter query against this field with the specified value</li>
     * </ul>
     * 
     * @see JoinDomain#applyDomainToQuery
     */
    public SolrParams applyValueConstraintAndDomain(SolrParams orig, String facetKey, String facetVal) {
      // first wrap our original query in the domain if there is one...
      if (null != domain) {
        orig = domain.applyDomainToQuery(facetKey + "_q", orig);
      }
      // then filter by the facet value we need to test...
      final ModifiableSolrParams out = new ModifiableSolrParams(orig);
      out.set("q", field + ":" + facetVal + " AND " + orig.get("q"));

      return out;
    }
    
    /**
     * recursively generates the <code>json.facet</code> param value to use for testing this facet
     */
    private CharSequence toJSONFacetParamValue() {
      final String limitStr = (null == limit) ? "" : (", limit:" + limit);
      final String overrequestStr = (null == overrequest) ? "" : (", overrequest:" + overrequest);
      final String refineStr = (null == refine) ? "" : ", refine:" + refine;
      final StringBuilder sb = new StringBuilder("{ type:terms, field:" + field + limitStr + overrequestStr + refineStr);
      if (! subFacets.isEmpty()) {
        sb.append(", facet:");
        sb.append(toJSONFacetParamValue(subFacets));
      }
      if (null != domain) {
        CharSequence ds = domain.toJSONFacetParamValue();
        if (null != ds) {
          sb.append(", ").append(ds);
        }
      }
      sb.append("}");
      return sb;
    }
    
    /**
     * Given a set of (possibly nested) facets, generates a suitable <code>json.facet</code> param value to 
     * use for testing them against in a solr request.
     */
    public static CharSequence toJSONFacetParamValue(Map<String,TermFacet> facets) {
      assert null != facets;
      assert 0 < facets.size();
      StringBuilder sb = new StringBuilder("{");
      for (String key : facets.keySet()) {
        sb.append(key).append(" : ").append(facets.get(key).toJSONFacetParamValue());
        sb.append(" ,");
      }
      sb.setLength(sb.length() - 1);
      sb.append("}");
      return sb;
    }
    
    /**
     * Factory method for generating some random (nested) facets.  
     *
     * For simplicity, each facet will have a unique key name, regardless of it's depth under other facets 
     *
     * @see JoinDomain
     */
    public static Map<String,TermFacet> buildRandomFacets() {
      // for simplicity, use a unique facet key regardless of depth - simplifies verification
      AtomicInteger keyCounter = new AtomicInteger(0);
      final int maxDepth = TestUtil.nextInt(random(), 0, (usually() ? 2 : 3));
      return buildRandomFacets(keyCounter, maxDepth);
    }

    /**
     * picks a random value for the "limit" param, biased in favor of interesting test cases
     *
     * @return a number to specify in the request, or null to specify nothing (trigger default behavior)
     * @see #UNIQUE_FIELD_VALS
     */
    public static Integer randomLimitParam(Random r) {
      final int limit = 1 + r.nextInt(UNIQUE_FIELD_VALS * 2);
      if (limit >= UNIQUE_FIELD_VALS && r.nextBoolean()) {
        return -1; // unlimited
      } else if (limit == DEFAULT_LIMIT && r.nextBoolean()) { 
        return null; // sometimes, don't specify limit if it's the default
      }
      return limit;
    }
    
    /**
     * picks a random value for the "overrequest" param, biased in favor of interesting test cases
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
          return 0; // 40% of the time, no overrequest to better stress refinement
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
     * picks a random value for the "refine" param, that is garunteed to be suitable for
     * the specified limit &amp; overrequest params.
     *
     * @return a value to specify in the request, or null to specify nothing (trigger default behavior)
     * @see #randomLimitParam
     * @see #randomOverrequestParam
     * @see #UNIQUE_FIELD_VALS
     */
    public static Boolean randomRefineParam(Random r, Integer limitParam, Integer overrequestParam) {
      if (isRefinementNeeded(limitParam, overrequestParam)) {
        return true;
      }

      // refinement is not required
      if (0 == r.nextInt(10)) { // once in a while, turn on refinement even if it isn't needed.
        return true;
      }
      // explicitly or implicitly indicate refinement is not needed
      return r.nextBoolean() ? false : null;
    }
    
    /**
     * Deterministicly identifies if the specified limit &amp; overrequest params <b>require</b> 
     * a "refine:true" param be used in the the request, in order for the counts to be 100% accurate.
     * 
     * @see #UNIQUE_FIELD_VALS
     */
    public static boolean isRefinementNeeded(Integer limitParam, Integer overrequestParam) {

      if (FORCE_DISABLE_REFINEMENT) {
        return false;
      }
      
      // use the "effective" values if the params are null
      final int limit = null == limitParam ? DEFAULT_LIMIT : limitParam;
      final int overrequest = null == overrequestParam ? 0 : overrequestParam;

      return
        // don't presume how much overrequest will be done by default, just check the limit
        (overrequest < 0 && limit < UNIQUE_FIELD_VALS)
        // if the user specified overrequest is not "enough" to get all unique values 
        || (overrequest >= 0 && (long)limit + overrequest < UNIQUE_FIELD_VALS);
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
        final JoinDomain domain = JoinDomain.buildRandomDomain();
        assert null != domain;
        final Integer limit = randomLimitParam(random());
        final Integer overrequest = randomOverrequestParam(random());
        final TermFacet facet =  new TermFacet(field(random().nextBoolean() ? STR_FIELD_SUFFIXES : INT_FIELD_SUFFIXES,
                                                     random().nextInt(MAX_FIELD_NUM)),
                                               domain, limit, overrequest,
                                               randomRefineParam(random(), limit, overrequest));
        results.put("facet_" + keyCounter.incrementAndGet(), facet);
        if (0 < maxDepth) {
          // if we're going wide, don't go deep
          final int nextMaxDepth = Math.max(0, maxDepth - numFacets);
          facet.subFacets.putAll(buildRandomFacets(keyCounter, TestUtil.nextInt(random(), 0, nextMaxDepth)));
        }
      }
      return results;
    }
  }


  /**
   * Models a Domain Change which includes either a 'join' or a 'filter' or both
   */
  private static final class JoinDomain { 
    public final String from;
    public final String to;
    public final String filter; // not bothering with more then 1 filter, not the point of the test

    /** 
     * @param from left side of join field name, null if domain involves no joining
     * @param to right side of join field name, null if domain involves no joining
     * @param filter filter to apply to domain, null if domain involves no filtering
     */
    public JoinDomain(String from, String to, String filter) { 
      assert ! ((null ==  from) ^ (null == to)) : "if from is null, to must be null";
      this.from = from;
      this.to = to;
      this.filter = filter;
    }

    /** 
     * @return the JSON string representing this domain for use in a facet param, or null if no domain should be used
     * */
    public CharSequence toJSONFacetParamValue() {
      if (null == from && null == filter) {
        return null;
      }
      StringBuilder sb = new StringBuilder("domain:{");
      if (null != from) {
        assert null != to;
        sb. append("join:{from:").append(from).append(",to:").append(to).append("}");
        if (null != filter){
          sb.append(",");
        }
        
      }
      if (null != filter) {
        sb.append("filter:'").append(filter).append("'");
      }
      sb.append("}");
      return sb;
    }

    /** 
     * Given some original SolrParams, returns new SolrParams where the original "q" param is wrapped
     * as needed to apply the equivalent transformation to a query as this domain would to a facet
     */
    public SolrParams applyDomainToQuery(String safeKey, SolrParams in) {
      assert null == in.get(safeKey); // shouldn't be possible if every facet uses a unique key string
      
      String q = in.get("q");
      final ModifiableSolrParams out = new ModifiableSolrParams(in);
      if (null != from) {
        out.set(safeKey, in.get("q"));
        q =  "{!join from="+from+" to="+to+" v=$"+safeKey+"}";
      }
      if (null != filter) {
        q = filter + " AND " + q;
      }
      out.set("q", q);
      return out;
    }

    /**
     * Factory method for creating a random domain change to use with a facet - may return an 'noop' JoinDomain,
     * but will never return null.
     */
    public static JoinDomain buildRandomDomain() { 

      // use consistent type on both sides of join
      final String[] suffixes = random().nextBoolean() ? STR_FIELD_SUFFIXES : INT_FIELD_SUFFIXES;
      
      final boolean noJoin = random().nextBoolean();

      String from = null;
      String to = null;
      for (;;) {
        if (noJoin) break;
        from = field(suffixes, random().nextInt(MAX_FIELD_NUM));
        to = field(suffixes, random().nextInt(MAX_FIELD_NUM));
        // HACK: joined numeric point fields need docValues.. for now just skip _is fields if we are dealing with points.
        if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP) && (from.endsWith("_is") || to.endsWith("_is")))
        {
            continue;
        }
        break;
      }

      // keep it simple, only filter on string fields - not point of test
      final String filterField = strfield(random().nextInt(MAX_FIELD_NUM));
      
      final String filter = random().nextBoolean() ? null : filterField+":[* TO *]";
      return new JoinDomain(from, to, filter);
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

  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assert null != client.getDefaultCollection();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(client.getDefaultCollection(),
                                                        client.getZkStateReader(),
                                                        true, true, 330);
  }

}
