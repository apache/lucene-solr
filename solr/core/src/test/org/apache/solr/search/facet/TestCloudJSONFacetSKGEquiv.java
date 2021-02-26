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
import java.util.EnumSet;
import java.util.LinkedHashMap;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.noggit.JSONUtil;
import org.noggit.JSONWriter;
import org.noggit.JSONWriter.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.search.facet.FacetField.FacetMethod;
import static org.apache.solr.search.facet.SlotAcc.SweepingCountSlotAcc.SWEEP_COLLECTION_DEBUG_KEY;

/** 
 * <p>
 * A randomized test of nested facets using the <code>relatedness()</code> function, that asserts the 
 * results are consistent and equivalent regardless of what <code>method</code> (ie: FacetFieldProcessor)
 * and/or <code>{@value RelatednessAgg#SWEEP_COLLECTION}</code> option is requested.
 * </p>
 * <p>
 * This test is based on {@link TestCloudJSONFacetSKG} but does <em>not</em> 
 * force <code>refine: true</code> nor specify a <code>domain: { 'query':'*:*' }</code> for every facet, 
 * because this test does not attempt to prove the results with validation requests.
 * </p>
 * <p>
 * This test only concerns itself with the equivalency of results
 * </p>
 * 
 * @see TestCloudJSONFacetSKG
 */
public class TestCloudJSONFacetSKGEquiv extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  private static final int DEFAULT_LIMIT = FacetField.DEFAULT_FACET_LIMIT;
  private static final int MAX_FIELD_NUM = 15;
  private static final int UNIQUE_FIELD_VALS = 50;

  /** Multi-Valued string field suffixes that can be randomized for testing diff facet code paths */
  private static final String[] MULTI_STR_FIELD_SUFFIXES = new String[]
    { "_multi_ss", "_multi_sds", "_multi_sdsS" };
  /** Multi-Valued int field suffixes that can be randomized for testing diff facet code paths */
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

    log.info("Created {} using numNodes={}, numShards={}, repFactor={}, numDocs={}",
             COLLECTION_NAME, numNodes, numShards, repFactor, numDocs);
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
   * Sanity check that our method of varying the <code>method</code> param
   * works and can be verified by inspecting the debug output of basic requests.
   */
  public void testWhiteboxSanityMethodProcessorDebug() throws Exception {
    // NOTE: json.facet debugging output can be wonky, particularly when dealing with cloud
    // so for these queries we keep it simple:
    // - only one "top" facet per request
    // - no refinement
    // even with those constraints in place, a single facet can (may/sometimes?) produce multiple debug
    // blocks - aparently due to shard merging? So...
    // - only inspect the "first" debug NamedList in the results
    //
    
    // simple individual facet that sorts on an skg stat...
    final TermFacet f = new TermFacet(soloStrField(9), 10, 0, "skg desc", null);
    final Map<String,TermFacet> facets = new LinkedHashMap<>();
    facets.put("str", f);
    
    final SolrParams facetParams = params("rows","0",
                                          "debug","true", // SOLR-14451
                                          // *:* is the only "safe" query for this test,
                                          // to ensure we always have at least one bucket for every facet
                                          // so we can be confident in getting the debug we expect...
                                          "q", "*:*",
                                          "fore", multiStrField(7)+":11",
                                          "back", "*:*",
                                          "json.facet", Facet.toJSONFacetParamValue(facets));
    
    { // dv 
      final SolrParams params = SolrParams.wrapDefaults(params("method_val", "dv"),
                                                        facetParams);
      final NamedList<Object> debug = getFacetDebug(params);
      assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), debug.get("processor"));
    }
    { // dvhash
      final SolrParams params = SolrParams.wrapDefaults(params("method_val", "dvhash"),
                                                        facetParams);
      final NamedList<Object> debug = getFacetDebug(params);
      assertEquals(FacetFieldProcessorByHashDV.class.getSimpleName(), debug.get("processor"));
    }
  }
  
  /** 
   * Sanity check that our method of varying the <code>{@value RelatednessAgg#SWEEP_COLLECTION}</code> in conjunction with the
   * <code>method</code> params works and can be verified by inspecting the debug output of basic requests.
   */
  public void testWhiteboxSanitySweepDebug() throws Exception {
    // NOTE: json.facet debugging output can be wonky, particularly when dealing with cloud
    // so for these queries we keep it simple:
    // - only one "top" facet per request
    // - no refinement
    // even with those constraints in place, a single facet can (may/sometimes?) produce multiple debug
    // blocks - aparently due to shard merging? So...
    // - only inspect the "first" debug NamedList in the results
    //
    
    final SolrParams baseParams = params("rows","0",
                                         "debug","true", // SOLR-14451
                                         // *:* is the only "safe" query for this test,
                                         // to ensure we always have at least one bucket for every facet
                                         // so we can be confident in getting the debug we expect...
                                         "q", "*:*",
                                         "fore", multiStrField(7)+":11",
                                         "back", "*:*");
    
    // simple individual facet that sorts on an skg stat...
    //
    // all results we test should be the same even if there is another 'skg_extra' stat,
    // it shouldn't be involved in the sweeping at all.
    for (Facet extra : Arrays.asList(null,  new RelatednessFacet(multiStrField(2)+":9", null))) {
      // choose a single value string so we know both 'dv' (sweep) and 'dvhash' (no sweep) can be specified
      final TermFacet f = new TermFacet(soloStrField(9), 10, 0, "skg desc", null);
      if (null != extra) {
        f.subFacets.put("skg_extra", extra);
      }
      final Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("str", f);
        
      final SolrParams facetParams
        = SolrParams.wrapDefaults(params("method_val", "dv",
                                         "json.facet", Facet.toJSONFacetParamValue(facets)),
                                  baseParams);
      
      // both default sweep option and explicit sweep should give same results...
      for (SolrParams sweepParams : Arrays.asList(params(),
                                                  params("sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                         "sweep_val", "true"))) {
        final SolrParams params = SolrParams.wrapDefaults(sweepParams, facetParams);
        
        final NamedList<Object> debug = getFacetDebug(params);
        assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), debug.get("processor"));
        @SuppressWarnings("unchecked")
        final NamedList<Object> sweep_debug = (NamedList<Object>) debug.get(SWEEP_COLLECTION_DEBUG_KEY);
        assertNotNull(sweep_debug);
        assertEquals("count", sweep_debug.get("base"));
        assertEquals(Arrays.asList("skg!fg","skg!bg"), sweep_debug.get("accs"));
        assertEquals(Arrays.asList("skg"), sweep_debug.get("mapped"));
      }
      { // 'dv' will always *try* to sweep, but disabling on stat should mean debug is mostly empty...
        final SolrParams params = SolrParams.wrapDefaults(params("sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                                 "sweep_val", "false"),
                                                          facetParams);
        final NamedList<Object> debug = getFacetDebug(params);
        assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), debug.get("processor"));
        @SuppressWarnings("unchecked")
        final NamedList<Object> sweep_debug = (NamedList<Object>) debug.get(SWEEP_COLLECTION_DEBUG_KEY);
        assertNotNull(sweep_debug);
        assertEquals("count", sweep_debug.get("base"));
        assertEquals(Collections.emptyList(), sweep_debug.get("accs"));
        assertEquals(Collections.emptyList(), sweep_debug.get("mapped"));
      }
      { // if we override 'dv' with 'hashdv' which doesn't sweep, our sweep debug should be empty,
        // even if the skg stat does ask for sweeping explicitly...
        final SolrParams params = SolrParams.wrapDefaults(params("method_val", "dvhash",
                                                                 "sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                                 "sweep_val", "true"),
                                                          facetParams);
        final NamedList<Object> debug = getFacetDebug(params);
        assertEquals(FacetFieldProcessorByHashDV.class.getSimpleName(), debug.get("processor"));
        assertNull(debug.get(SWEEP_COLLECTION_DEBUG_KEY));
      }
    }

    // simple facet that sorts on an skg stat but uses prelim_sort on count
    //
    // all results we test should be the same even if there is another 'skg_extra' stat,
    // neither skg should be involved in the sweeping at all.
    for (Facet extra : Arrays.asList(null,  new RelatednessFacet(multiStrField(2)+":9", null))) {
      // choose a single value string so we know both 'dv' (sweep) and 'dvhash' (no sweep) can be specified
      final TermFacet f = new TermFacet(soloStrField(9), map("limit", 3, "overrequest", 0,
                                                             "sort", "skg desc",
                                                             "prelim_sort", "count asc"));
      if (null != extra) {
        f.subFacets.put("skg_extra", extra);
      }
      final Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("str", f);
        
      final SolrParams facetParams
        = SolrParams.wrapDefaults(params("method_val", "dv",
                                         "json.facet", Facet.toJSONFacetParamValue(facets)),
                                  baseParams);

      // default sweep as well as any explicit sweep=true/false values should give same results: no sweeping
      for (SolrParams sweepParams : Arrays.asList(params(),
                                                  params("sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                         "sweep_val", "false"),
                                                  params("sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                         "sweep_val", "true"))) {
        final SolrParams params = SolrParams.wrapDefaults(sweepParams, facetParams);
        
        final NamedList<Object> debug = getFacetDebug(params);
        assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), debug.get("processor"));
        @SuppressWarnings("unchecked")
        final NamedList<Object> sweep_debug = (NamedList<Object>) debug.get(SWEEP_COLLECTION_DEBUG_KEY);
        assertNotNull(sweep_debug);
        assertEquals("count", sweep_debug.get("base"));
        assertEquals(Collections.emptyList(), sweep_debug.get("accs"));
        assertEquals(Collections.emptyList(), sweep_debug.get("mapped"));
      }
    }
    
    { // single facet with infinite limit + multiple skgs...
      // this should trigger MultiAcc collection, causing sweeping on both skg functions
      //
      // all results we test should be the same even if there is another 'min' stat,
      // in each term facet.  it shouldn't affect the sweeping/MultiAcc at all.
      for (Facet extra : Arrays.asList(null,  new SumFacet(multiIntField(2)))) {
        final Map<String,TermFacet> facets = new LinkedHashMap<>();
        final TermFacet facet = new TermFacet(soloStrField(9), -1, 0, "skg2 desc", null);
        facet.subFacets.put("skg2", new RelatednessFacet(multiStrField(2)+":9", null));
        if (null != extra) {
          facet.subFacets.put("sum", extra);
        }
        facets.put("str", facet);
        final SolrParams facetParams
          = SolrParams.wrapDefaults(params("method_val", "dv",
                                           "json.facet", Facet.toJSONFacetParamValue(facets)),
                                    baseParams);
        
        // both default sweep option and explicit sweep should give same results...
        for (SolrParams sweepParams : Arrays.asList(params(),
                                                    params("sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                           "sweep_val", "true"))) {
          final SolrParams params = SolrParams.wrapDefaults(sweepParams, facetParams);
          
          final NamedList<Object> debug = getFacetDebug(params);
          assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), debug.get("processor"));
          @SuppressWarnings("unchecked")
          final NamedList<Object> sweep_debug = (NamedList<Object>) debug.get(SWEEP_COLLECTION_DEBUG_KEY);
          assertNotNull(sweep_debug);
          assertEquals("count", sweep_debug.get("base"));
          assertEquals(Arrays.asList("skg!fg","skg!bg","skg2!fg","skg2!bg"), sweep_debug.get("accs"));
          assertEquals(Arrays.asList("skg","skg2"), sweep_debug.get("mapped"));
        }
      }
    }
    
    // nested facets that both sort on an skg stat
    // (set limit + overrequest tiny to keep multishard response managable)
    //
    // all results we test should be the same even if there is another 'skg_extra' stat,
    // in each term facet.  they shouldn't be involved in the sweeping at all.
    for (Facet extra : Arrays.asList(null,  new RelatednessFacet(multiStrField(2)+":9", null))) {
      // choose single value strings so we know both 'dv' (sweep) and 'dvhash' (no sweep) can be specified
      // choose 'id' for the parent facet so we are garunteed some child facets
      final TermFacet parent = new TermFacet("id", 1, 0, "skg desc", false);
      final TermFacet child = new TermFacet(soloStrField(7), 1, 0, "skg desc", false);
      parent.subFacets.put("child", child);
      if (null != extra) {
        parent.subFacets.put("skg_extra", extra);
        child.subFacets.put("skg_extra", extra);
      }
      final Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("parent", parent);
        
      final SolrParams facetParams
        = SolrParams.wrapDefaults(params("method_val", "dv",
                                         "json.facet", Facet.toJSONFacetParamValue(facets)),
                                  baseParams);
      // both default sweep option and explicit sweep should give same results...
      for (SolrParams sweepParams : Arrays.asList(params(),
                                                  params("sweep_key", RelatednessAgg.SWEEP_COLLECTION,
                                                         "sweep_val", "true"))) {
        final SolrParams params = SolrParams.wrapDefaults(sweepParams, facetParams);
        
        final NamedList<Object> parentDebug = getFacetDebug(params);
        assertEquals("id", parentDebug.get("field"));
        assertNotNull(parentDebug.get("sub-facet"));
        // may be multiples from diff shards, just use first one
        @SuppressWarnings("unchecked")
        final NamedList<Object> childDebug = ((List<NamedList<Object>>)parentDebug.get("sub-facet")).get(0);
        assertEquals(soloStrField(7), childDebug.get("field"));

        // these should all be true for both the parent and the child debug..
        for (NamedList<Object> debug : Arrays.asList(parentDebug, childDebug)) {
          assertEquals(FacetFieldProcessorByArrayDV.class.getSimpleName(), debug.get("processor"));
          @SuppressWarnings("unchecked")
          final NamedList<Object> sweep_debug = (NamedList<Object>) debug.get(SWEEP_COLLECTION_DEBUG_KEY);
          assertNotNull(sweep_debug);
          assertEquals("count", sweep_debug.get("base"));
          assertEquals(Arrays.asList("skg!fg","skg!bg"), sweep_debug.get("accs"));
          assertEquals(Arrays.asList("skg"), sweep_debug.get("mapped"));
        }
      }
    }
  }

  /**
   * returns the <b>FIRST</b> NamedList (under the implicit 'null' FacetQuery) in the "facet-trace" output 
   * of the request.  Should not be used with multiple "top level" facets 
   * (the output is too confusing in cloud mode to be confident where/qhy each NamedList comes from)
   */
  private NamedList<Object> getFacetDebug(final SolrParams params) {
    try {
      final QueryResponse rsp = (new QueryRequest(params)).process(getRandClient(random()));
      assertNotNull(params + " is null rsp?", rsp);
      @SuppressWarnings({"rawtypes"})
      final NamedList topNamedList = rsp.getResponse();
      assertNotNull(params + " is null topNamedList?", topNamedList);
      
      // skip past the (implicit) top Facet query to get it's "sub-facets" (the real facets)...
      @SuppressWarnings({"unchecked"})
      final List<NamedList<Object>> facetDebug =
        (List<NamedList<Object>>) topNamedList.findRecursive("debug", "facet-trace", "sub-facet");
      assertNotNull(topNamedList + " ... null facet debug?", facetDebug);
      assertFalse(topNamedList + " ... not even one facet debug?", facetDebug.isEmpty());
      return facetDebug.get(0);
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
    
    { // trivial single level facet w/ perSeg
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("xxx", new TermFacet(multiStrField(9),
                                      map("perSeg", true)));
      
      assertFacetSKGsAreConsistent(facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
    }
    
    { // trivial single level facet w/ prefix 
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("xxx", new TermFacet(multiStrField(9),
                                      map("prefix", "2")));
      
      
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
    
    { // multi-valued facet field w/infinite limit and an extra (non-SKG / non-sweeping) stat
      final TermFacet xxx = new TermFacet(multiStrField(12), -1, 0, "count asc", false);
      xxx.subFacets.put("sum", new SumFacet(multiIntField(4)));
      final Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("xxx", xxx);
      assertFacetSKGsAreConsistent(facets,
                                   buildORQuery(multiStrField(13) + ":26",
                                                multiStrField(6) + ":33",
                                                multiStrField(9) + ":24"),
                                   buildORQuery(multiStrField(4) + ":27",
                                                multiStrField(12) + ":18",
                                                multiStrField(2) + ":28",
                                                multiStrField(13) + ":50"),
                                   "*:*");
    }
  }
  
  public void testBespokeAllBuckets() throws Exception {
    { // single level facet w/sorting on skg and allBuckets
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("xxx", new TermFacet(multiStrField(9), map("sort", "skg desc",
                                                            "allBuckets", true)));
      
      assertFacetSKGsAreConsistent(facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
    }
  }
  
  public void testBespokePrefix() throws Exception {
    { // trivial single level facet w/ prefix 
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      facets.put("xxx", new TermFacet(multiStrField(9),
                                      map("sort", "skg desc",
                                          "limit", -1,
                                          "prefix", "2")));
      
      assertFacetSKGsAreConsistent(facets, multiStrField(7)+":11", multiStrField(5)+":9", "*:*");
    }
  }
  
  /** 
   * Given a few explicit "structures" of requests, test many permutations of various params/options.
   * This is more complex then {@link #testBespoke} but should still be easier to trace/debug then 
   * a pure random monstrosity.
   */
  public void testBespokeStructures() throws Exception {
    // we don't need to test every field, just make sure we test enough fields to hit every suffix..
    final int maxFacetFieldNum = Collections.max(Arrays.asList(MULTI_STR_FIELD_SUFFIXES.length,
                                                               MULTI_INT_FIELD_SUFFIXES.length,
                                                               SOLO_STR_FIELD_SUFFIXES.length,
                                                               SOLO_INT_FIELD_SUFFIXES.length));
    
    for (int facetFieldNum = 0; facetFieldNum < maxFacetFieldNum; facetFieldNum++) {
      for (String facetFieldName : Arrays.asList(soloStrField(facetFieldNum), multiStrField(facetFieldNum))) {
        for (int limit : Arrays.asList(10, -1)) {
          for (String sort : Arrays.asList("count desc", "skg desc", "index asc")) {
            for (Boolean refine : Arrays.asList(false, true)) {
              { // 1 additional (non-SKG / non-sweeping) stat
                final TermFacet xxx = new TermFacet(facetFieldName, map("limit", limit,
                                                                        "overrequest", 0,
                                                                        "sort", sort,
                                                                        "refine", refine));
                xxx.subFacets.put("sum", new SumFacet(soloIntField(3)));
                final Map<String,TermFacet> facets = new LinkedHashMap<>();
                facets.put("xxx1", xxx);
                assertFacetSKGsAreConsistent(facets,
                                             buildORQuery(multiStrField(11) + ":55",
                                                          multiStrField(0) + ":46"),
                                             multiStrField(5)+":9", "*:*");
              }
              { // multiple SKGs
                final TermFacet xxx = new TermFacet(facetFieldName, map("limit", limit,
                                                                        "overrequest", 0,
                                                                        "sort", sort,
                                                                        "refine", refine));
                xxx.subFacets.put("skg2", new RelatednessFacet(multiStrField(2)+":9", "*:*"));
                final Map<String,TermFacet> facets = new LinkedHashMap<>();
                facets.put("xxx2", xxx);
                assertFacetSKGsAreConsistent(facets,
                                             buildORQuery(multiStrField(11) + ":55",
                                                          multiStrField(0) + ":46"),
                                             multiStrField(5)+":9", "*:*");
              }
              { // multiple SKGs and a multiple non-SKG / non-sweeping stats
                final TermFacet xxx = new TermFacet(facetFieldName, map("limit", limit,
                                                                        "overrequest", 0,
                                                                        "sort", sort,
                                                                        "refine", refine));
                xxx.subFacets.put("minAAA", new SumFacet(soloIntField(3)));
                xxx.subFacets.put("skg2", new RelatednessFacet(multiStrField(2)+":9", "*:*"));
                xxx.subFacets.put("minBBB", new SumFacet(soloIntField(2)));
                final Map<String,TermFacet> facets = new LinkedHashMap<>();
                facets.put("xxx3", xxx);
                assertFacetSKGsAreConsistent(facets,
                                             buildORQuery(multiStrField(11) + ":55",
                                                          multiStrField(0) + ":46"),
                                             multiStrField(5)+":9", "*:*");
              }
            }
          }
        }
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
   * the results of these queries are identical even when varying the <code>method_val</code> param
   * and when varying the <code>{@value RelatednessAgg#SWEEP_COLLECTION}</code> param; either by explicitly setting to 
   * <code>true</code> or <code>false</code> or by changing the param key to not set it at all.
   */
  private void assertFacetSKGsAreConsistent(final Map<String,TermFacet> facets,
                                            final String query,
                                            final String foreQ,
                                            final String backQ) throws SolrServerException, IOException {
    final SolrParams basicParams = params("rows","0",
                                          "q", query, "fore", foreQ, "back", backQ,
                                          "json.facet", Facet.toJSONFacetParamValue(facets));
    
    log.info("Doing full run: {}", basicParams);
    try {

      // start by recording the results of the purely "default" behavior...
      @SuppressWarnings({"rawtypes"})
      final NamedList expected = getFacetResponse(basicParams);

      // now loop over all permutations of processors and sweep values and and compare them to the "default"...
      for (FacetMethod method : EnumSet.allOf(FacetMethod.class)) {
        for (Boolean sweep : Arrays.asList(true, false, null)) {
          final ModifiableSolrParams options = params("method_val", method.toString().toLowerCase(Locale.ROOT));
          if (null != sweep) {
            options.add("sweep_key", RelatednessAgg.SWEEP_COLLECTION);
            options.add("sweep_val", sweep.toString());
          }
          
          @SuppressWarnings({"rawtypes"})
          final NamedList actual = getFacetResponse(SolrParams.wrapAppended(options, basicParams));
          
          // we can't rely on a trivial assertEquals() comparison...
          // 
          // the order of the sub-facet keys can change between
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
  @SuppressWarnings({"rawtypes"})
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

  private static interface Facet { // Mainly just a Marker Interface
    
    /**
     * Given a set of (possibly nested) facets, generates a suitable <code>json.facet</code> param value to 
     * use for testing them against in a solr request.
     */
    public static String toJSONFacetParamValue(final Map<String,? extends Facet> facets) {
      assert null != facets;
      assert ! facets.isEmpty();

      return JSONUtil.toJSON(facets, -1); // no newlines
    }
  }

  /** 
   * trivial facet that is not SKG (and doesn't have any of it's special behavior) for the purposes 
   * of testing how TermFacet behaves with a mix of sub-facets.
   */
  private static final class SumFacet implements Facet {
    private final String field;
    public SumFacet(final String field) {
      this.field = field;
    }
    @Override
    public String toString() { // used in JSON by default
      return "sum(" + field + ")";
    }
    public static SumFacet buildRandom() {
      final int fieldNum = random().nextInt(MAX_FIELD_NUM);
      final boolean multi = random().nextBoolean();
      return new SumFacet(multi ? multiIntField(fieldNum) : soloIntField(fieldNum));
    }
  }
  
  /**
   * Trivial data structure for modeling a simple <code>relatedness()</code> facet that can be written out as a json.facet param.
   *
   * Doesn't do any string escaping or quoting, so don't use whitespace or reserved json characters
   *
   * The specified fore/back queries will be wrapped in localparam syntax in the resulting json, 
   * unless they are 'null' in which case <code>$fore</code> and <code>$back</code> refs will be used 
   * in their place, and must be set as request params (this allows "random" facets to still easily 
   * trigger the "nested facets re-using the same fore/back set for SKG situation)
   *
   * The JSON for all of these facets includes a <code>${sweep_key:xxx}</code> (which will be ignored 
   * by default) and <code>${sweep_val:yyy}</code> which may be set as params on each request to override the 
   * implicit default sweeping behavior of the underlying SKGAcc.
   */
  private static final class RelatednessFacet implements Facet, Writable {
    public final Map<String,Object> jsonData = new LinkedHashMap<>();
    
    /** Assumes null for fore/back queries w/no options */
    public RelatednessFacet() {
      this(null, null, map());
    }
    /** Assumes no options */
    public RelatednessFacet(final String foreQ, final String backQ) {
      this(foreQ, backQ, map());
    }
    public RelatednessFacet(final String foreQ, final String backQ,
                            final Map<String,Object> options) {
      assert null != options;
      
      final String f = null == foreQ ? "$fore" : "{!v='"+foreQ+"'}";
      final String b = null == backQ ? "$back" : "{!v='"+backQ+"'}";

      jsonData.putAll(options);
      
      // we don't allow these to be overridden by options, so set them now...
      jsonData.put("type", "func");
      jsonData.put("func", "relatedness("+f+","+b+")");
      jsonData.put("${sweep_key:xxx}","${sweep_val:yyy}");
    }
    @Override
    public void write(JSONWriter writer) {
      writer.write(jsonData);
    }
    
    public static RelatednessFacet buildRandom() {

      final Map<String,Object> options = new LinkedHashMap<>();
      if (random().nextBoolean()) {
        options.put("min_popularity", "0.001");
      }
      
      // bias this in favor of null fore/back since that's most realistic for typical nested facets
      final boolean simple = random().nextBoolean();
      final String fore = simple ? null : buildRandomORQuery(TestUtil.nextInt(random(), 1, 5));
      final String back = simple ? null : buildRandomORQuery(TestUtil.nextInt(random(), 1, 9));
      
      return new RelatednessFacet(fore, back, options);
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
  private static final class TermFacet implements Facet, Writable {

    public final Map<String,Object> jsonData = new LinkedHashMap<>();
    public final Map<String,Facet> subFacets = new LinkedHashMap<>();

    /** 
     * @param field must be non null
     * @param options can set any of options used in a term facet other then field or (sub) facets
     */
    public TermFacet(final String field, final Map<String,Object> options) {
      assert null != field;
      
      jsonData.put("method", "${method_val:smart}");
      
      jsonData.putAll(options);

      // we don't allow these to be overridden by options, so set them now...
      jsonData.put("type", "terms");
      jsonData.put("field",field);
      jsonData.put("facet", subFacets);
      
      subFacets.put("skg", new RelatednessFacet());
    }

    /** all params except field can be null */
    public TermFacet(String field, Integer limit, Integer overrequest, String sort, Boolean refine) {
      this(field, map("limit", limit, "overrequest", overrequest, "sort", sort, "refine", refine));
    }
    
    @Override
    public void write(JSONWriter writer) {
      writer.write(jsonData);
    }

    /** 
     * Generates a random TermFacet that does not contai nany random sub-facets 
     * beyond a single consistent "skg" stat) 
     */
    public static TermFacet buildRandom() {
      final String sort = randomSortParam(random());
      final String facetField = randomFacetField(random());
      return new TermFacet(facetField,
                           map("limit", randomLimitParam(random()),
                               "overrequest", randomOverrequestParam(random(), sort),
                               "prefix", randomPrefixParam(random(), facetField),
                               "perSeg", randomPerSegParam(random()),
                               "sort", sort,
                               "prelim_sort", randomPrelimSortParam(random(), sort),
                               "allBuckets", randomAllBucketsParam(random(), sort),
                               "refine", randomRefineParam(random())));
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
        case 0: return multiStrField(fieldNum);
        case 1: return multiIntField(fieldNum);
        case 2: return soloStrField(fieldNum);
        case 3: return soloIntField(fieldNum);
        default: throw new RuntimeException("Broken case statement");
      }
    }
  
    /**
     * picks a random value for the "allBuckets" param, biased in favor of interesting test cases
     * This bucket should be ignored by relatedness, but inclusion should not cause any problems 
     * (or change the results)
     *
     * <p>
     * <b>NOTE:</b> allBuckets is meaningless in conjunction with the <code>STREAM</code> processor, so
     * this method always returns null if sort is <code>index asc</code>.
     * </p>
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
     * picks a random value for the "refine" param, biased in favor of interesting test cases
     *
     * @return a Boolean, may be null
     */
    public static Boolean randomRefineParam(final Random r) {

      switch(r.nextInt(3)) {
        case 0: return null;
        case 1: return true;
        case 2: return false;
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
     * picks a random value for the "sort" param, biased in favor of interesting test cases.  
     * Assumes every TermFacet will have at least one "skg" stat
     *
     * @return a sort string (w/direction), or null to specify nothing (trigger default behavior)
     * @see #randomAllBucketsParam
     * @see #randomPrelimSortParam
     */
    public static String randomSortParam(final Random r) {

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
     * picks a random value for the "limit" param, biased in favor of interesting test cases
     *
     * @return a number to specify in the request, or null to specify nothing (trigger default behavior)
     * @see #UNIQUE_FIELD_VALS
     */
    public static Integer randomLimitParam(final Random r) {

      final int limit = 1 + r.nextInt((int) (UNIQUE_FIELD_VALS * 1.5F));
      
      if (1 == TestUtil.nextInt(random(), 0, 3)) {
        // bias in favor of just using default
        return null;
      }
      
      if (limit >= UNIQUE_FIELD_VALS && r.nextBoolean()) {
        return -1; // unlimited
      }
      
      return limit;
    }
    
    /**
     * picks a random value for the "overrequest" param, biased in favor of interesting test cases.
     * <p>
     * <b>NOTE:</b> due to variations in overrequest behavior betewen <code>metod:enum<code> and other 
     * processors (see <a href="https://issues.apache.org/jira/browse/SOLR-14595">SOLR-14595</a>) this 
     * method takes in the "sort" param and returns a constant value of <code>0</code> if the sort is 
     * <code>index asc</code> to ensure that the set of candidate buckets considered during merging 
     * (and refinement) is consistent regardless of what processor is used (and/or what sort is used 
     * on the parent facet)
     * </p>
     *
     * @return a number to specify in the request, or null to specify nothing (trigger default behavior)
     * @see #UNIQUE_FIELD_VALS
     * @see <a href="https://issues.apache.org/jira/browse/SOLR-14595">SOLR-14595</a>
     */
    public static Integer randomOverrequestParam(final Random r, final String sort) {

      if ("index asc".equals(sort)) {
        return 0; // test work around for SOLR-14595
      }
      
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

          final TermFacet facet = TermFacet.buildRandom();
          
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

          if (1 == TestUtil.nextInt(random(), 0, 4)) {
            // occasionally add in a non-SKG related stat...
            facet.subFacets.put("sum", SumFacet.buildRandom());
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
