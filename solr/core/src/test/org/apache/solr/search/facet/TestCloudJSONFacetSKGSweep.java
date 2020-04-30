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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.BaseDistributedSearchTestCase;
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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import static org.apache.solr.search.facet.FacetField.FacetMethod;
import static org.apache.solr.search.facet.RelatednessAgg.computeRelatedness;
import static org.apache.solr.search.facet.RelatednessAgg.roundTo5Digits;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// nocommit: jdocs and code currently assume option is named 'sweep_collection' - adjust as impl changes
// nocommit: ... really: just make the constant in RelatednessAgg public and refer to it directly here

/** 
 * <p>
 * A randomized test of nested facets using the <code>relatedness()</code> function, that asserts the 
 * results are consistent regardless of wether the <code>sweep</code> option is used.
 * </p>
 * <p>
 * This test is based on {@link TestCloudJSONFacetSKG} but does <em>not</em> 
 * force <code>refine: true</code> nor specify a <code>domain: { 'query':'*:*' }</code> for every facet, 
 * because this test does not attempt to prove the results with validation requests.
 * </p>
 * <p>
 * This test only concerns itself with the equivilency of results, regardless of the value of the 
 * <code>sweep_collection</code> option.
 * </p>
 * 
 * @see TestCloudJSONFacetSKG
 */
public class TestCloudJSONFacetSKGSweep extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  private static final int DEFAULT_LIMIT = FacetField.DEFAULT_FACET_LIMIT;
  private static final int MAX_FIELD_NUM = 15;
  private static final int UNIQUE_FIELD_VALS = 50;

  /** Multi-Valued string field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] MULTI_STR_FIELD_SUFFIXES = new String[] { "_ss", "_sds", "_sdsS" };
  /** Multi-Valued int field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] MULTI_INT_FIELD_SUFFIXES = new String[] { "_is", "_ids", "_idsS" };

  /** Single Valued string field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] SOLO_STR_FIELD_SUFFIXES = new String[] { "_s", "_sd", "_sdS" };
  /** Single Valued int field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] SOLO_INT_FIELD_SUFFIXES = new String[] { "_i", "_id", "_idS" };

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

      // NOTE: for each fieldNum, there are actaully 4 fields: multi(str+int) + solo(str+int)
      for (int fieldNum = 0; fieldNum < MAX_FIELD_NUM; fieldNum++) {
        // NOTE: Some docs may not have any value in some fields
        final int numValsThisDoc = TestUtil.nextInt(random(), 0, (usually() ? 5 : 10));
        for (int v = 0; v < numValsThisDoc; v++) {
          final String fieldValue = randFieldValue(fieldNum);
          
          // multi valued: one string, and one integer
          doc.addField(multiStrField(fieldNum), fieldValue);
          doc.addField(multiIntField(fieldNum), fieldValue);
        }
        if (3 <= numValsThisDoc) { // use num values in multivalue to inform sparseness of single value
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
  
  /** 
   * Sanity check that our method of varying the <code>sweep_collection</code> and <code>method</code> params 
   * works and can be verified by inspecting the debug output of basic requests.
   */
  public void testWhiteboxSanitySweepDebug() throws Exception {
    final SolrParams baseParams = params("rows","0",
                                         "debug","query",
                                         "q", multiStrField(5)+":9",
                                         "fore", multiStrField(7)+":11",
                                         "back", "*:*");
    
    { // simple single facet with a single skg that is sorted on...
      
      final Map<String,TermFacet> facets = new LinkedHashMap<>();
      // choose a single value string so we know both 'dv' and 'dvhash' can be specified
      facets.put("str", new TermFacet(soloStrField(9), 10, 0, "skg desc", null));
      final SolrParams facetParams
        = SolrParams.wrapDefaults(params("method_val", "dv",
                                         "json.facet", ""+TermFacet.toJSONFacetParamValue(facets)),
                                  baseParams);
      
      // both default sweep option and explicit sweep should give same results...
      for (SolrParams sweepParams : Arrays.asList(params(),
                                                  params("sweep_key", "sweep_collection",
                                                         "sweep_val", "true"))) {
        final SolrParams params = SolrParams.wrapDefaults(sweepParams, facetParams);
        
        final List<NamedList<Object>> facetDebug = getFacetDebug(params);
        assertEquals(1, facetDebug.size());
        assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), facetDebug.get(0).get("processor"));
        final NamedList<Object> sweep_debug = (NamedList<Object>) facetDebug.get(0).get("sweep_collection");
        assertNotNull(sweep_debug);
        assertEquals("count", sweep_debug.get("base"));
        assertEquals(Arrays.asList("skg!fg","skg!bg"), sweep_debug.get("accs"));
        assertEquals(Arrays.asList("skg"), sweep_debug.get("mapped"));
      }
      { // UIF will always *try* to sweep, but disabling on stat should mean debug is mostly empty...
        final SolrParams params = SolrParams.wrapDefaults(params("sweep_key", "sweep_collection",
                                                                 "sweep_val", "false"),
                                                          facetParams);
        final List<NamedList<Object>> facetDebug = getFacetDebug(params);
        assertEquals(1, facetDebug.size());
        assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), facetDebug.get(0).get("processor"));
        final NamedList<Object> sweep_debug = (NamedList<Object>) facetDebug.get(0).get("sweep_collection");
        assertNotNull(sweep_debug);
        assertEquals("count", sweep_debug.get("base"));
        assertEquals(Collections.emptyList(), sweep_debug.get("accs"));
        assertEquals(Collections.emptyList(), sweep_debug.get("mapped"));
      }
      { // if we override 'dv' with 'hashdv' which doesn't sweep, our sweep debug should be empty,
        // even if the skg stat does ask for sweeping explicitly...
        final SolrParams params = SolrParams.wrapDefaults(params("method_val", "dvhash",
                                                                 "sweep_key", "sweep_collection",
                                                                 "sweep_val", "true"),
                                                          facetParams);
        final List<NamedList<Object>> facetDebug = getFacetDebug(params);
        assertEquals(1, facetDebug.size());
        assertEquals(FacetFieldProcessorByHashDV.class.getSimpleName(), facetDebug.get(0).get("processor"));

        // nocommit: this currently fails because even non-sweeping processors call
        // nocommit: FacetFieldProcessor.fillBucketFromSlot which calls countAcc.getBaseSweepingAcc()
        // nocommit: so we get a SweepingAcc (which populates the debug) even though we don't need/want/use it
        //
        // assertNull(facetDebug.get(0).get("sweep_collection"));
      }

      // nocommit: TODO test multiple relatedness() functions, confirm expected sweep accs based on sort
      
      // nocommit: TODO test nested facets and inspect nested debug
      
      // nocommit: TODO test multiacc situation w/multi relatedness(), confirm multiple sweep accs

      // nocommit: TODO test prelim_sort=count + sort=relatedness(), confirm no sweep accs
    }
  }

  private List<NamedList<Object>> getFacetDebug(final SolrParams params) {
    try {
      final QueryResponse rsp = (new QueryRequest(params)).process(getRandClient(random()));
      assertNotNull(params + " is null rsp?", rsp);
      final NamedList topNamedList = rsp.getResponse();
      assertNotNull(params + " is null topNamedList?", topNamedList);
      
      // skip past the "top level" (implicit) Facet query to get it's "sub-facets" (the real facets)...
      final List<NamedList<Object>> facetDebug =
        (List<NamedList<Object>>) topNamedList.findRecursive("debug", "facet-trace", "sub-facet");
      assertNotNull(topNamedList + " ... null facet debug?", facetDebug);
      return facetDebug;
    } catch (Exception e) {
      throw new RuntimeException("query failed: " + params + ": " + 
                                 e.getMessage(), e);
    } 

  }
  
  /** 
   * Test some small, hand crafted, but non-trivial queries that are
   * easier to trace/debug then a pure random monstrosity.
   * (ie: if something obvious gets broken, this test may fail faster and in a more obvious way then testRandom)
   */
  public void testBespoke() throws Exception {
    
    { // two trivial single level facets
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("str", new TermFacet(multiStrField(9), UNIQUE_FIELD_VALS, 0, null, null));
      facets.put("int", new TermFacet(multiIntField(9), UNIQUE_FIELD_VALS, 0, null, null));
      assertFacetSKGsAreConsistent(facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
    }
    
    { // trivial single level facet w/sorting on skg and refinement explicitly disabled
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("xxx", new TermFacet(multiStrField(9), UNIQUE_FIELD_VALS, 0, "skg desc", false));
      assertFacetSKGsAreConsistent(facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
    }

    { // trivial single level facet w/ 2 diff ways to request "limit = (effectively) Infinite"
      // to sanity check refinement of buckets missing from other shard in both cases
      
      // NOTE that these two queries & facets *should* effectively identical given that the
      // very large limit value is big enough no shard will ever return that may terms,
      // but the "limit=-1" case it actaully triggers slightly different code paths
      // because it causes FacetField.returnsPartial() to be "true"
      for (int limit : new int[] { 999999999, -1 }) {
        Map<String,TermFacet> facets = new LinkedHashMap<>();
        facets.put("top_facet_limit__" + limit, new TermFacet(multiStrField(9), limit, 0, "skg desc", true));
        assertFacetSKGsAreConsistent(facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
      }
    }

  }
  
  public void testRandom() throws Exception {

    final int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      assertFacetSKGsAreConsistent(TermFacet.buildRandomFacets(),
                                   buildRandomQuery(), buildRandomQuery(), buildRandomQuery());
    }
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
    return buildRandomORQuery(numClauses);
  }
  /** The more clauses, the more docs it's likely to match */
  private static String buildRandomORQuery(final int numClauses) {
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
   * the results of these queries are identical even when varying the <code>sweep_collection</code> param; 
   * either by explicitly setting to <code>true</code> or <code>false</code> or by changing the param 
   * key to not set it at all.
   */
  private void assertFacetSKGsAreConsistent(final Map<String,TermFacet> facets,
                                            final String query,
                                            final String foreQ,
                                            final String backQ) throws SolrServerException, IOException {
    final SolrParams basicParams = params("rows","0",
                                          "q", query, "fore", foreQ, "back", backQ,
                                          "json.facet", ""+TermFacet.toJSONFacetParamValue(facets));
    
    log.info("Doing full run: {}", basicParams);
    try {

      // start by recording the results of the purely "default" behavior...
      final NamedList expected = getFacetResponse(basicParams);

      // now loop over all permutations of processors and sweep values and compare them
      for (FacetMethod method : EnumSet.allOf(FacetMethod.class)) {
        for (Boolean sweep : Arrays.asList(true, false, null)) {
          ModifiableSolrParams options = params("method_val", method.toString().toLowerCase(Locale.ROOT));
          if (null != sweep) {
            options.add("sweep_key", "sweep_collection");
            options.add("sweep_val", sweep.toString());
          }
          
          final NamedList actual = getFacetResponse(SolrParams.wrapAppended(options, basicParams));

          // we can't rely on a trivial assertEquals() comparison...
          // 
          // even w/ any sweeping, the order of the sub-facet keys can change between
          // processors.  (notably: method:enum vs method:smart when sort:"index asc")
          // 
          // NOTE: this doesn't ignore the order of the buckets,
          // it ignores the order of the keys in each bucket...
          final String pathToMismatch = BaseDistributedSearchTestCase.compare
            (expected, actual, 0,
             Collections.singletonMap("buckets", BaseDistributedSearchTestCase.UNORDERED));
          if (null != pathToMismatch) {
            log.error("{}: expected = {}", options, expected);
            log.error("{}: actual = {}", options, actual);
            fail("Mismatch: " + pathToMismatch + " using " + options);
          }
        }
      }
    } catch (AssertionError e) {
      throw new AssertionError(basicParams + " ===> " + e.getMessage(), e);
    } finally {
      log.info("Ending full run"); 
    }
  }

  /**     
   * We ignore {@link QueryResponse#getJsonFacetingResponse()} because it isn't as useful for
   * doing a "deep equals" comparison across requests
   */
  private NamedList getFacetResponse(final SolrParams params) {
    try {
      final QueryResponse rsp = (new QueryRequest(params)).process(getRandClient(random()));
      assertNotNull(params + " is null rsp?", rsp);
      final NamedList topNamedList = rsp.getResponse();
      assertNotNull(params + " is null topNamedList?", topNamedList);
      final NamedList facetResponse = (NamedList) topNamedList.get("facets");
      assertNotNull("null facet results?", facetResponse);
      assertEquals("numFound mismatch with top count?",
                   rsp.getResults().getNumFound(), ((Number)facetResponse.get("count")).longValue());
      
      return facetResponse;
      
    } catch (Exception e) {
      throw new RuntimeException("query failed: " + params + ": " + 
                                 e.getMessage(), e);
    }
  }

  private static interface Facet {
    /**
     * recursively generates the <code>json.facet</code> param value to use for testing this facet
     */
    public CharSequence toJSONFacetParamValue();
  }

  /**
   * Trivial data structure for modeling a simple <code>relatedness()</code> facet that can be written out as a json.facet param.
   *
   * Doesn't do any string escaping or quoting, so don't use whitespace or reserved json characters
   *
   * The JSON for all of these facets includes a <code>${sweep_key:xxx}</code> (which will be ignored 
   * by default) and <code>${sweep_val:yyy}</code> which may be set as params on each request to override the 
   * implicit default sweeping behavior of the underlying SKGAcc.
   *
   * The specified fore/back queries will be wrapped in localparam syntax in the resulting json, 
   * unless they are 'null' in which case <code>$fore</code> and <code>$back</code> refs will be used 
   * in their place, and must be set as request params (this allows "random" facets to still easily 
   * trigger the "nested facets re-using the same fore/back set for SKG situation)
   */
  private static final class RelatednessFacet implements Facet {
    private final String foreQ;
    private final String backQ;
    /** Assumes null for fore/back queries */
    public RelatednessFacet() {
      this(null, null);
    }
    public RelatednessFacet(final String foreQ, final String backQ) {
      this.foreQ = foreQ;
      this.backQ = backQ;
    }
    public CharSequence toJSONFacetParamValue() {
      final String f = null == foreQ ? "$fore" : "{!v='"+foreQ+"'}";
      final String b = null == backQ ? "$back" : "{!v='"+backQ+"'}";

      // nocommit: can we remove min_popularity ?
      return"{ \"type\": \"func\", "
        + "  \"func\": \"relatedness("+f+","+b+")\""
        + "  \"min_popularity\": 0.001, "
        // sweep_val param can be set to true|false to test explicit values, but to do so
        // sweep_key param must be changed to 'sweep'XZ
        + "  ${sweep_key:xxx}: ${sweep_val:yyy} }";
    }
    
    public static RelatednessFacet buildRandom() {
      // nocommit: want to bias this in favor of null fore/back since
      // nocommit: that's most realistic for typical nested facets
      return new RelatednessFacet(buildRandomORQuery(TestUtil.nextInt(random(), 1, 5)),
                                  buildRandomORQuery(TestUtil.nextInt(random(), 3, 9)));
    }
  }
  
  /**
   * Trivial data structure for modeling a simple terms facet that can be written out as a json.facet param.
   * Since the point of this test is SKG, every TermFacet implicitly has one fixed "skg" subFacet, but that 
   * can be overridden by the caller
   *
   * Doesn't do any string escaping or quoting, so don't use whitespace or reserved json characters
   *
   * The resulting facets all specify a <code>method</code> of <code>${method_val:smart}</code> which may be 
   * overridden via request params. 
   */
  private static final class TermFacet implements Facet {
    public final String field;
    public final Map<String,Facet> subFacets = new LinkedHashMap<>();
    public final Integer limit; // may be null
    public final Integer overrequest; // may be null
    public final String sort; // may be null
    public final Boolean refine; // may be null

    // nocommit: we need to add some 'allBuckets' testing options as well
    // nocommit: since that makes interesting changes to the processor/collection paths
    
    // nocommit: we need to add some 'prelim_sort' testing options as well
    // nocommit: since that makes interesting changes to the processor/collection paths
    
    /** Simplified constructor asks for limit = # unique vals */
    public TermFacet(String field) {
      this(field, UNIQUE_FIELD_VALS, 0, null, null); 
    }
    
    public TermFacet(String field, Integer limit, Integer overrequest, String sort, Boolean refine) {
      assert null != field;
      this.field = field;
      this.limit = limit;
      this.overrequest = overrequest;
      this.sort = sort;
      this.refine = refine;
      this.subFacets.put("skg", new RelatednessFacet());
    }

    public CharSequence toJSONFacetParamValue() {
      assert ! subFacets.isEmpty() : "TermFacet w/o at least one SKG stat defeats point of test";
      
      final String limitStr = (null == limit) ? "" : (", limit:" + limit);
      final String overrequestStr = (null == overrequest) ? "" : (", overrequest:" + overrequest);
      final String sortStr = (null == sort) ? "" : (", sort: '" + sort + "'");
      final String refineStr = (null == refine) ? "" : (", refine: " + refine);
      final StringBuilder sb
        = new StringBuilder("{ type:terms, method:${method_val:smart}, field:" + field +
                            limitStr + overrequestStr + sortStr + refineStr);
      
      if ( ! subFacets.isEmpty()) {
        sb.append(", facet:");
        sb.append(toJSONFacetParamValue(subFacets));
      }
      sb.append("}");
      return sb;
    }
    
    /**
     * Given a set of (possibly nested) facets, generates a suitable <code>json.facet</code> param value to 
     * use for testing them against in a solr request.
     */
    public static CharSequence toJSONFacetParamValue(final Map<String,? extends Facet> facets) {
      assert null != facets;
      assert ! facets.isEmpty();

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
     * Factory method for generating some random facets.  
     *
     * For simplicity, each facet will have a unique key name.
     */
    public static Map<String,TermFacet> buildRandomFacets() {
      // for simplicity, use a unique facet key regardless of depth - simplifies verification
      // and let's us enforce a hard limit on the total number of facets in a request
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
        case 0: return field(MULTI_STR_FIELD_SUFFIXES, fieldNum);
        case 1: return field(MULTI_INT_FIELD_SUFFIXES, fieldNum);
        case 2: return field(SOLO_STR_FIELD_SUFFIXES, fieldNum);
        case 3: return field(SOLO_INT_FIELD_SUFFIXES, fieldNum);
        default: throw new RuntimeException("Broken case statement");
      }
    }
  

    /**
     * picks a random value for the "refine" param, biased in favor of interesting test cases
     *
     * @return a Boolean, may be null
     */
    public static Boolean randomRefineParam(Random r) {

      switch(r.nextInt(3)) {
        case 0: return null;
        case 1: return true;
        case 2: return false;
        default: throw new RuntimeException("Broken case statement");
      }
    }
    
    /**
     * picks a random value for the "sort" param, biased in favor of interesting test cases.  
     * Assumes every TermFacet will have at least one "skg0" stat
     *
     * @return a sort string (w/direction), or null to specify nothing (trigger default behavior)
     * @see #randomLimitParam
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
        
        // nocommit: since we're not using processEmpty for this test
        // nocommit: (because we're only comparing equivilent requests, not issuing verification queries)
        // nocommit: we should be able to ignore SOLR-12556 and not do anything special on these sorts...
        
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

          final String field = randomFacetField(random());
          final Boolean refine = randomRefineParam(random());
          final String sort = randomSortParam(random());
          final Integer limit = randomLimitParam(random(), sort);
          final Integer overrequest = randomOverrequestParam(random());
          
          // nocommit: we need to add some 'allBuckets' testing options as well
          // nocommit: since that makes interesting changes to the processor/collection paths
          
          // nocommit: we need to add some 'prelim_sort' testing options as well
          // nocommit: since that makes interesting changes to the processor/collection paths
    
          final TermFacet facet =  new TermFacet(field, limit, overrequest, sort, refine);
          
          results.put("facet_" + keyCounter.incrementAndGet(), facet);
          if (0 < maxDepth) {
            // if we're going wide, don't go deep
            final int nextMaxDepth = Math.max(0, maxDepth - numFacets);
            facet.subFacets.putAll(buildRandomFacets(keyCounter, TestUtil.nextInt(random(), 0, nextMaxDepth)));
          }
          
          // we get one implicit RelatednessFacet automatically,
          // randomly add 1 or 2 more ... 3/5th chance of being '0'
          final int numExtraSKGStats = Math.max(0, TestUtil.nextInt(random(), -2, 2)); 
          for (int skgId = 0; skgId < numExtraSKGStats; skgId++) {
            // sometimes we overwrite the trivial defualt "skg" with this one...
            final String key = (0 == skgId && 0 == TestUtil.nextInt(random(), 0, 5)) ? "skg" : "skg" + skgId;
            facet.subFacets.put(key, RelatednessFacet.buildRandom());
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

}
