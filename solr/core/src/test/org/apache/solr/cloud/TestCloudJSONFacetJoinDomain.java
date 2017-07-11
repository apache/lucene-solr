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
package org.apache.solr.cloud;

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
  
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressPointFields;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
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
 * The results of each facet constraint count will be compared with a verification query using an equivilent filter
 * </p>
 * 
 * @see TestCloudPivotFacet
 */
@SuppressPointFields(bugUrl="https://issues.apache.org/jira/browse/SOLR-10939")
public class TestCloudJSONFacetJoinDomain extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  private static final int MAX_FIELD_NUM = 15;
  private static final int UNIQUE_FIELD_VALS = 20;
  private static final int FACET_LIMIT = UNIQUE_FIELD_VALS + 1;
  
  /** Multivalued string field suffixes that can be randomized for testing diff facet/join code paths */
  private static final String[] STR_FIELD_SUFFIXES = new String[] { "_ss", "_sds", "_sdsS" };
  /** Multivalued int field suffixes that can be randomized for testing diff facet/join code paths */
  private static final String[] INT_FIELD_SUFFIXES = new String[] { "_is", "_ids", "_idsS" };
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  public TestCloudJSONFacetJoinDomain() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
  }
  
  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    // sanity check constants
    assertTrue("bad test constants: must have UNIQUE_FIELD_VALS < FACET_LIMIT since refinement not currently supported",
               UNIQUE_FIELD_VALS < FACET_LIMIT);
    assertTrue("bad test constants: some suffixes will never be tested",
               (STR_FIELD_SUFFIXES.length < MAX_FIELD_NUM) && (INT_FIELD_SUFFIXES.length < MAX_FIELD_NUM));
    
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
   * but the cise pr<em>range</em> of values will vary for each unique field number, such that cross field joins 
   * will match fewer documents based on how far apart the field numbers are.
   *
   * @see #UNIQUE_FIELD_VALS
   * @see #field
   */
  private static String randFieldValue(final int fieldNum) {
    return "" + (fieldNum + TestUtil.nextInt(random(), 0, UNIQUE_FIELD_VALS));
  }

  
  @AfterClass
  private static void afterClass() throws Exception {
    CLOUD_CLIENT.close(); CLOUD_CLIENT = null;
    for (HttpSolrClient client : CLIENTS) {
      client.close();
    }
    CLIENTS = null;
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
          final NamedList trash = getRandClient(random()).request(new QueryRequest(req));
        });
      assertEquals(join + " -> " + e, SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(join + " -> " + e, e.getMessage().contains("'join' domain change"));
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
      assertFacetCountsAreCorrect(facets, strfield(7) + ":bogus");
    }
    
    { // sanity check our test methods can handle a query where a facet filter prevents any doc from having terms
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(null, null, "-*:*"));
      top.subFacets.put("sub", new TermFacet(strfield(11), new JoinDomain(strfield(8), strfield(8), null)));
      facets.put("filtered_top", top);
      assertFacetCountsAreCorrect(facets, "*:*");
    }
    
    { // sanity check our test methods can handle a query where a facet filter prevents any doc from having sub-terms
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(strfield(8), strfield(8), null));
      top.subFacets.put("sub", new TermFacet(strfield(11), new JoinDomain(null, null, "-*:*")));
      facets.put("filtered_top", top);
      assertFacetCountsAreCorrect(facets, "*:*");
    }
  
    { // strings
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(strfield(5), strfield(9), strfield(9)+":[* TO *]"));
      top.subFacets.put("facet_5", new TermFacet(strfield(11), new JoinDomain(strfield(8), strfield(8), null)));
      facets.put("facet_4", top);
      assertFacetCountsAreCorrect(facets, "("+strfield(7)+":6 OR "+strfield(9)+":6 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");
    }

    { // ints
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(intfield(9), new JoinDomain(intfield(5), intfield(9), null));
      facets.put("top", top);
      assertFacetCountsAreCorrect(facets, "("+intfield(7)+":6 OR "+intfield(3)+":3)");
    }

    { // some domains with filter only, no actual join
      Map<String,TermFacet> facets = new LinkedHashMap<>();
      TermFacet top = new TermFacet(strfield(9), new JoinDomain(null, null, strfield(9)+":[* TO *]"));
      top.subFacets.put("facet_5", new TermFacet(strfield(11), new JoinDomain(null, null, strfield(3)+":[* TO 5]")));
      facets.put("top", top);
      assertFacetCountsAreCorrect(facets, "("+strfield(7)+":6 OR "+strfield(9)+":6 OR "+strfield(6)+":19 OR "+strfield(0)+":11)");

    }
  }

  public void testRandom() throws Exception {

    final int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; iter++) {
      assertFacetCountsAreCorrect(TermFacet.buildRandomFacets(), buildRandomQuery());
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
    List<String> clauses = new ArrayList<String>(numClauses);
    for (int c = 0; c < numClauses; c++) {
      final int fieldNum = random().nextInt(MAX_FIELD_NUM);
      // keep queries simple, just use str fields - not point of test
      clauses.add(strfield(fieldNum) + ":" + randFieldValue(fieldNum));
    }
    return "(" + StringUtils.join(clauses, " OR ") + ")";
  }
  
  /**
   * Given a set of (potentially nested) term facets, and a base query string, asserts that 
   * the actual counts returned when executing that query with those facets match the expected results
   * of filtering on the equivilent facet terms+domain
   */
  private void assertFacetCountsAreCorrect(Map<String,TermFacet> expected,
                                           final String query) throws SolrServerException, IOException {

    final SolrParams baseParams = params("q", query, "rows","0");
    final SolrParams facetParams = params("json.facet", ""+TermFacet.toJSONFacetParamValue(expected));
    final SolrParams initParams = SolrParams.wrapAppended(facetParams, baseParams);
    
    log.info("Doing full run: {}", initParams);

    QueryResponse rsp = null;
    // JSON Facets not (currently) available from QueryResponse...
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
      final NamedList facetResponse = (NamedList) topNamedList.get("facets");
      assertNotNull("null facet results?", facetResponse);
      assertEquals("numFound mismatch with top count?",
                   rsp.getResults().getNumFound(), ((Number)facetResponse.get("count")).longValue());
      if (0 == rsp.getResults().getNumFound()) {
        // when the query matches nothing, we should expect no top level facets
        expected = Collections.emptyMap();
      }
      assertFacetCountsAreCorrect(expected, baseParams, facetResponse);
    } catch (AssertionError e) {
      throw new AssertionError(initParams + " ===> " + topNamedList + " --> " + e.getMessage(), e);
    } finally {
      log.info("Ending full run"); 
    }
  }

  /** 
   * Recursive Helper method that walks the actual facet response, comparing the counts to the expected output 
   * based on the equivilent filters generated from the original TermFacet.
   */
  private void assertFacetCountsAreCorrect(Map<String,TermFacet> expected,
                                           SolrParams baseParams,
                                           NamedList actualFacetResponse) throws SolrServerException, IOException {

    for (Map.Entry<String,TermFacet> entry : expected.entrySet()) {
      final String facetKey = entry.getKey();
      final TermFacet facet = entry.getValue();
      final NamedList results = (NamedList) actualFacetResponse.get(facetKey);
      assertNotNull(facetKey + " key missing from: " + actualFacetResponse, results);
      final List<NamedList> buckets = (List<NamedList>) results.get("buckets");
      assertNotNull(facetKey + " has null buckets: " + actualFacetResponse, buckets);
      for (NamedList bucket : buckets) {
        final long count = ((Number) bucket.get("count")).longValue();
        final String fieldVal = bucket.get("val").toString(); // int or stringified int

        // change our query to filter on the fieldVal, and wrap in the facet domain (if any)
        final SolrParams verifyParams = facet.applyValueConstraintAndDomain(baseParams, facetKey, fieldVal);

        // check the count for this bucket
        assertEquals(facetKey + ": " + verifyParams,
                     count, getRandClient(random()).query(verifyParams).getResults().getNumFound());

        // recursively check subFacets
        if (! facet.subFacets.isEmpty()) {
          assertFacetCountsAreCorrect(facet.subFacets,
                                      verifyParams, bucket);
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
    public TermFacet(String field, JoinDomain domain) {
      assert null != field;
      this.field = field;
      this.domain = domain;
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
      // NOTE: since refinement isn't supported, we have to use the max cardinality of the field as limit
      StringBuilder sb = new StringBuilder("{ type:terms, field:" + field + ", limit: " + FACET_LIMIT);
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
        final TermFacet facet =  new TermFacet(field(random().nextBoolean() ? STR_FIELD_SUFFIXES : INT_FIELD_SUFFIXES,
                                                     random().nextInt(MAX_FIELD_NUM)),
                                               domain);
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
     * as needed to apply the equivilent transformation to a query as this domain would to a facet
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
      final String from = noJoin ? null : field(suffixes, random().nextInt(MAX_FIELD_NUM));
      final String to = noJoin ? null : field(suffixes, random().nextInt(MAX_FIELD_NUM));
      
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
