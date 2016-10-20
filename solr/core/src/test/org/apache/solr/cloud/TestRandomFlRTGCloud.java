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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.RawValueTransformerFactory;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.util.RandomizeSSL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @see TestCloudPseudoReturnFields */
@RandomizeSSL(clientAuth=0.0,reason="client auth uses too much RAM")
public class TestRandomFlRTGCloud extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  /** Always included in fl so we can vet what doc we're looking at */
  private static final FlValidator ID_VALIDATOR = new SimpleFieldValueValidator("id");
  
  /** 
   * Types of things we will randomly ask for in fl param, and validate in response docs.
   *
   * @see #addRandomFlValidators
   */
  private static final List<FlValidator> FL_VALIDATORS = Collections.unmodifiableList
    (Arrays.<FlValidator>asList(
      new GlobValidator("*"),
      new GlobValidator("*_i"),
      new GlobValidator("*_s"),
      new GlobValidator("a*"),
      new DocIdValidator(),
      new DocIdValidator("my_docid_alias"),
      new ShardValidator(),
      new ShardValidator("my_shard_alias"),
      new ValueAugmenterValidator(42),
      new ValueAugmenterValidator(1976, "val_alias"),
      //
      new RenameFieldValueValidator("id", "my_id_alias"),
      new SimpleFieldValueValidator("aaa_i"),
      new RenameFieldValueValidator("bbb_i", "my_int_field_alias"),
      new SimpleFieldValueValidator("ccc_s"),
      new RenameFieldValueValidator("ddd_s", "my_str_field_alias"),
      //
      // SOLR-9376: RawValueTransformerFactory doesn't work in cloud mode 
      //
      // new RawFieldValueValidator("json", "eee_s", "my_json_field_alias"),
      // new RawFieldValueValidator("json", "fff_s"),
      // new RawFieldValueValidator("xml", "ggg_s", "my_xml_field_alias"),
      // new RawFieldValueValidator("xml", "hhh_s"),
      //
      new NotIncludedValidator("bogus_unused_field_ss"),
      new NotIncludedValidator("bogus_alias","bogus_alias:other_bogus_field_i"),
      new NotIncludedValidator("bogus_raw_alias","bogus_raw_alias:[xml f=bogus_raw_field_ss]"),
      //
      new FunctionValidator("aaa_i"), // fq field
      new FunctionValidator("aaa_i", "func_aaa_alias"),
      new GeoTransformerValidator("geo_1_srpt"),
      new GeoTransformerValidator("geo_2_srpt","my_geo_alias"),
      new ExplainValidator(),
      new ExplainValidator("explain_alias"),
      new SubQueryValidator(),
      new NotIncludedValidator("score"),
      new NotIncludedValidator("score","score_alias:score")));
  
  @BeforeClass
  public static void createMiniSolrCloudCluster() throws Exception {

    // 50% runs use single node/shard a FL_VALIDATORS with all validators known to work on single node
    // 50% runs use multi node/shard with FL_VALIDATORS only containing stuff that works in cloud
    final boolean singleCoreMode = random().nextBoolean();

    // (asuming multi core multi replicas shouldn't matter (assuming multi node) ...
    final int repFactor = singleCoreMode ? 1 : (usually() ? 1 : 2);
    // ... but we definitely want to ensure forwarded requests to other shards work ...
    final int numShards = singleCoreMode ? 1 : 2;
    // ... including some forwarded requests from nodes not hosting a shard
    final int numNodes = 1 + (singleCoreMode ? 0 : (numShards * repFactor));
    
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");
    
    configureCluster(numNodes).addConfig(configName, configDir).configure();

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .withProperty("config", "solrconfig-tlog.xml")
        .withProperty("schema", "schema-psuedo-fields.xml")
        .process(CLOUD_CLIENT);

    waitForRecoveriesToFinish(CLOUD_CLIENT);

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      CLIENTS.add(getHttpSolrClient(jetty.getBaseUrl() + "/" + COLLECTION_NAME + "/"));
    }
  }

  @AfterClass
  private static void afterClass() throws Exception {
    CLOUD_CLIENT.close(); CLOUD_CLIENT = null;
    for (HttpSolrClient client : CLIENTS) {
      client.close();
    }
    CLIENTS = null;
  }

  /** 
   * Tests thta all TransformerFactories that are implicitly provided by Solr are tested in this class
   *
   * @see FlValidator#getDefaultTransformerFactoryName
   * @see #FL_VALIDATORS
   * @see TransformerFactory#defaultFactories
   */
  public void testCoverage() throws Exception {
    final Set<String> implicit = new LinkedHashSet<>();
    for (String t :  TransformerFactory.defaultFactories.keySet()) {
      implicit.add(t);
    }
    
    final Set<String> covered = new LinkedHashSet<>();
    for (FlValidator v : FL_VALIDATORS) {
      String t = v.getDefaultTransformerFactoryName();
      if (null != t) {
        covered.add(t);
      }
    }

    // items should only be added to this list if it's known that they do not work with RTG
    // and a specific Jira for fixing this is listed as a comment
    final List<String> knownBugs = Arrays.asList
      ( "xml","json", // SOLR-9376
        "child" // way to complicatd to vet with this test, see SOLR-9379 instead
      );

    for (String buggy : knownBugs) {
      assertFalse(buggy + " is listed as a being a known bug, " +
                  "but it exists in the set of 'covered' TransformerFactories",
                  covered.contains(buggy));
      assertTrue(buggy + " is listed as a known bug, " +
                 "but it does not even exist in the set of 'implicit' TransformerFactories",
                  implicit.remove(buggy));
    }
    
    implicit.removeAll(covered);
    assertEquals("Some implicit TransformerFactories are not yet tested by this class: " + implicit,
                 0, implicit.size());
  }


  public void testRandomizedUpdatesAndRTGs() throws Exception {

    final int maxNumDocs = atLeast(100);
    final int numSeedDocs = random().nextInt(maxNumDocs / 10); // at most ~10% of the max possible docs
    final int numIters = atLeast(maxNumDocs * 10);
    final SolrInputDocument[] knownDocs = new SolrInputDocument[maxNumDocs];

    log.info("Starting {} iters by seeding {} of {} max docs",
             numIters, numSeedDocs, maxNumDocs);

    int itersSinceLastCommit = 0;
    for (int i = 0; i < numIters; i++) {
      itersSinceLastCommit = maybeCommit(random(), itersSinceLastCommit, numIters);

      if (i < numSeedDocs) {
        // first N iters all we worry about is seeding
        knownDocs[i] = addRandomDocument(i);
      } else {
        assertOneIter(knownDocs);
      }
    }
  }

  /** 
   * Randomly chooses to do a commit, where the probability of doing so increases the longer it's been since 
   * a commit was done.
   *
   * @returns <code>0</code> if a commit was done, else <code>itersSinceLastCommit + 1</code>
   */
  private static int maybeCommit(final Random rand, final int itersSinceLastCommit, final int numIters) throws IOException, SolrServerException {
    final float threshold = itersSinceLastCommit / numIters;
    if (rand.nextFloat() < threshold) {
      log.info("COMMIT");
      assertEquals(0, getRandClient(rand).commit().getStatus());
      return 0;
    }
    return itersSinceLastCommit + 1;
  }
  
  private void assertOneIter(final SolrInputDocument[] knownDocs) throws IOException, SolrServerException {
    // we want to occasionally test more then one doc per RTG
    final int numDocsThisIter = TestUtil.nextInt(random(), 1, atLeast(2));
    int numDocsThisIterThatExist = 0;
    
    // pick some random docIds for this iteration and ...
    final int[] docIds = new int[numDocsThisIter];
    for (int i = 0; i < numDocsThisIter; i++) {
      docIds[i] = random().nextInt(knownDocs.length);
      if (null != knownDocs[docIds[i]]) {
        // ...check how many already exist
        numDocsThisIterThatExist++;
      }
    }

    // we want our RTG requests to occasionally include missing/deleted docs,
    // but that's not the primary focus of the test, so weight the odds accordingly
    if (random().nextInt(numDocsThisIter + 2) <= numDocsThisIterThatExist) {

      if (0 < TestUtil.nextInt(random(), 0, 13)) {
        log.info("RTG: numDocsThisIter={} numDocsThisIterThatExist={}, docIds={}",
                 numDocsThisIter, numDocsThisIterThatExist, docIds);
        assertRTG(knownDocs, docIds);
      } else {
        // sporadically delete some docs instead of doing an RTG
        log.info("DEL: numDocsThisIter={} numDocsThisIterThatExist={}, docIds={}",
                 numDocsThisIter, numDocsThisIterThatExist, docIds);
        assertDelete(knownDocs, docIds);
      }
    } else {
      log.info("UPD: numDocsThisIter={} numDocsThisIterThatExist={}, docIds={}",
               numDocsThisIter, numDocsThisIterThatExist, docIds);
      assertUpdate(knownDocs, docIds);
    }
  }

  /**
   * Does some random indexing of the specified docIds and adds them to knownDocs
   */
  private void assertUpdate(final SolrInputDocument[] knownDocs, final int[] docIds) throws IOException, SolrServerException {
    
    for (final int docId : docIds) {
      // TODO: this method should also do some atomic update operations (ie: "inc" and "set")
      // (but make sure to eval the updates locally as well before modifying knownDocs)
      knownDocs[docId] = addRandomDocument(docId);
    }
  }
  
  /**
   * Deletes the docIds specified and asserts the results are valid, updateing knownDocs accordingly
   */
  private void assertDelete(final SolrInputDocument[] knownDocs, final int[] docIds) throws IOException, SolrServerException {
    List<String> ids = new ArrayList<>(docIds.length);
    for (final int docId : docIds) {
      ids.add("" + docId);
      knownDocs[docId] = null;
    }
    assertEquals("Failed delete: " + docIds, 0, getRandClient(random()).deleteById(ids).getStatus());
  }
  
  /**
   * Adds one randomly generated document with the specified docId, asserting success, and returns 
   * the document added
   */
  private SolrInputDocument addRandomDocument(final int docId) throws IOException, SolrServerException {
    final SolrClient client = getRandClient(random());

    final SolrInputDocument doc = sdoc("id", "" + docId,
                                       "aaa_i", random().nextInt(),
                                       "bbb_i", random().nextInt(),
                                       //
                                       "ccc_s", TestUtil.randomSimpleString(random()),
                                       "ddd_s", TestUtil.randomSimpleString(random()),
                                       "eee_s", TestUtil.randomSimpleString(random()),
                                       "fff_s", TestUtil.randomSimpleString(random()),
                                       "ggg_s", TestUtil.randomSimpleString(random()),
                                       "hhh_s", TestUtil.randomSimpleString(random()),
                                       //
                                       "geo_1_srpt", GeoTransformerValidator.getValueForIndexing(random()),
                                       "geo_2_srpt", GeoTransformerValidator.getValueForIndexing(random()),
                                       // for testing subqueries
                                       "next_2_ids_ss", String.valueOf(docId + 1),
                                       "next_2_ids_ss", String.valueOf(docId + 2),
                                       // for testing prefix globbing
                                       "axx_i", random().nextInt(),
                                       "ayy_i", random().nextInt(),
                                       "azz_s", TestUtil.randomSimpleString(random()));
    
    log.info("ADD: {} = {}", docId, doc);
    assertEquals(0, client.add(doc).getStatus());
    return doc;
  }

  
  /**
   * Does one or more RTG request for the specified docIds with a randomized fl &amp; fq params, asserting
   * that the returned document (if any) makes sense given the expected SolrInputDocuments
   */
  private void assertRTG(final SolrInputDocument[] knownDocs, final int[] docIds) throws IOException, SolrServerException {
    final SolrClient client = getRandClient(random());
    // NOTE: not using SolrClient.getById or getByIds because we want to force choice of "id" vs "ids" params
    final ModifiableSolrParams params = params("qt","/get");
    
    // random fq -- nothing fancy, secondary concern for our test
    final Integer FQ_MAX = usually() ? null : random().nextInt();
    if (null != FQ_MAX) {
      params.add("fq", "aaa_i:[* TO " + FQ_MAX + "]");
    }
    
    final Set<FlValidator> validators = new LinkedHashSet<>();
    validators.add(ID_VALIDATOR); // always include id so we can be confident which doc we're looking at
    addRandomFlValidators(random(), validators);
    FlValidator.addParams(validators, params);

    final List<String> idsToRequest = new ArrayList<>(docIds.length);
    final List<SolrInputDocument> docsToExpect = new ArrayList<>(docIds.length);
    for (int docId : docIds) {
      // every docId will be included in the request
      idsToRequest.add("" + docId);
      
      // only docs that should actually exist and match our (optional) filter will be expected in response
      if (null != knownDocs[docId]) {
        Integer filterVal = (Integer) knownDocs[docId].getFieldValue("aaa_i");
        if (null == FQ_MAX || ((null != filterVal) && filterVal.intValue() <= FQ_MAX.intValue())) {
          docsToExpect.add(knownDocs[docId]);
        }
      }
    }

    // even w/only 1 docId requested, the response format can vary depending on how we request it
    final boolean askForList = random().nextBoolean() || (1 != idsToRequest.size());
    if (askForList) {
      if (1 == idsToRequest.size()) {
        // have to be careful not to try to use "multi" 'id' params with only 1 docId
        // with a single docId, the only way to ask for a list is with the "ids" param
        params.add("ids", idsToRequest.get(0));
      } else {
        if (random().nextBoolean()) {
          // each id in it's own param
          for (String id : idsToRequest) {
            params.add("id",id);
          }
        } else {
          // add one or more comma seperated ids params
          params.add(buildCommaSepParams(random(), "ids", idsToRequest));
        }
      }
    } else {
      assert 1 == idsToRequest.size();
      params.add("id",idsToRequest.get(0));
    }
    
    final QueryResponse rsp = client.query(params);
    assertNotNull(params.toString(), rsp);

    final SolrDocumentList docs = getDocsFromRTGResponse(askForList, rsp);
    assertNotNull(params + " => " + rsp, docs);
    
    assertEquals("num docs mismatch: " + params + " => " + docsToExpect + " vs " + docs,
                 docsToExpect.size(), docs.size());
    
    // NOTE: RTG makes no garuntees about the order docs will be returned in when multi requested
    for (SolrDocument actual : docs) {
      try {
        int actualId = assertParseInt("id", actual.getFirstValue("id"));
        final SolrInputDocument expected = knownDocs[actualId];
        assertNotNull("expected null doc but RTG returned: " + actual, expected);
        
        Set<String> expectedFieldNames = new TreeSet<>();
        for (FlValidator v : validators) {
          expectedFieldNames.addAll(v.assertRTGResults(validators, expected, actual));
        }
        // ensure only expected field names are in the actual document
        Set<String> actualFieldNames = new TreeSet<>(actual.getFieldNames());
        assertEquals("Actual field names returned differs from expected", expectedFieldNames, actualFieldNames);
      } catch (AssertionError ae) {
        throw new AssertionError(params + " => " + actual + ": " + ae.getMessage(), ae);
      }
    }
  }

  /** 
   * trivial helper method to deal with diff response structure between using a single 'id' param vs
   * 2 or more 'id' params (or 1 or more 'ids' params).
   *
   * @return List from response, or a synthetic one created from single response doc if 
   * <code>expectList</code> was false; May be empty; May be null if response included null list.
   */
  private static SolrDocumentList getDocsFromRTGResponse(final boolean expectList, final QueryResponse rsp) {
    if (expectList) {
      return rsp.getResults();
    }
    
    // else: expect single doc, make our own list...
    
    final SolrDocumentList result = new SolrDocumentList();
    NamedList<Object> raw = rsp.getResponse();
    Object doc = raw.get("doc");
    if (null != doc) {
      result.add((SolrDocument) doc);
      result.setNumFound(1);
    }
    return result;
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

  /** 
   * Abstraction for diff types of things that can be added to an 'fl' param that can validate
   * the results are correct compared to an expected SolrInputDocument
   */
  private interface FlValidator {
    
    /** 
     * Given a list of FlValidators, adds one or more fl params that corrispond to the entire set, 
     * as well as any other special case top level params required by the validators.
     */
    public static void addParams(final Collection<FlValidator> validators, final ModifiableSolrParams params) {
      final List<String> fls = new ArrayList<>(validators.size());
      for (FlValidator v : validators) {
        params.add(v.getExtraRequestParams());
        fls.add(v.getFlParam());
      }
      params.add(buildCommaSepParams(random(), "fl", fls));
    }

    /**
     * Indicates if this validator is for a transformer that returns true from 
     * {@link DocTransformer#needsSolrIndexSearcher}.  Other validators for transformers that 
     * do <em>not</em> require a re-opened searcher (but may have slightly diff behavior depending 
     * on wether a doc comesfrom the index or from the update log) may use this information to 
     * decide wether they wish to enforce stricter assertions on the resulting document.
     *
     * The default implementation always returns <code>false</code>
     *
     * @see DocIdValidator
     */
    public default boolean requiresRealtimeSearcherReOpen() {
      return false;
    }

    /**
     * the name of a transformer listed in {@link TransformerFactory#defaultFactories} that this validator
     * corrisponds to, or null if not applicable.  Used for testing coverage of 
     * Solr's implicitly supported transformers.
     *
     * Default behavior is to return null
     * @see #testCoverage
     */
    public default String getDefaultTransformerFactoryName() { return null; }
    
    /**
     * Any special case params that must be added to the request for this validator
     */
    public default SolrParams getExtraRequestParams() { return params(); }
    
    /** 
     * Must return a non null String that can be used in an fl param -- either by itself, 
     * or with other items separated by commas
     */
    public String getFlParam();

    /** 
     * Given the expected document and the actual document returned from an RTG, this method
     * should assert that relative to what {@link #getFlParam} returns, the actual document contained
     * what it should relative to the expected document.
     *
     * @param validators all validators in use for this request, including the current one
     * @param expected a document containing the expected fields &amp; values that should be in the index
     * @param actual A document that was returned by an RTG request
     * @return A set of "field names" in the actual document that this validator expected.
     */
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual);
  }
  
  /** 
   * Some validators behave in a way that "suppresses" real fields even when they would otherwise match a glob
   * @see GlobValidator
   */
  private interface SuppressRealFields {
    public Set<String> getSuppressedFields();
  }

  private abstract static class FieldValueValidator implements FlValidator {
    protected final String expectedFieldName;
    protected final String actualFieldName;
    public FieldValueValidator(final String expectedFieldName, final String actualFieldName) {
      this.expectedFieldName = expectedFieldName;
      this.actualFieldName = actualFieldName;
    }
    public abstract String getFlParam();
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      assertEquals(expectedFieldName + " vs " + actualFieldName,
                   expected.getFieldValue(expectedFieldName), actual.getFirstValue(actualFieldName));
      return Collections.<String>singleton(actualFieldName);
    }
  }
  
  private static class SimpleFieldValueValidator extends FieldValueValidator {
    public SimpleFieldValueValidator(final String fieldName) {
      super(fieldName, fieldName);
    }
    public String getFlParam() { return expectedFieldName; }
  }
  
  private static class RenameFieldValueValidator extends FieldValueValidator implements SuppressRealFields {
    public RenameFieldValueValidator(final String origFieldName, final String alias) {
      super(origFieldName, alias);
    }
    public String getFlParam() { return actualFieldName + ":" + expectedFieldName; }
    public Set<String> getSuppressedFields() { return Collections.singleton(expectedFieldName); }
  }

  /**
   * Validator for {@link RawValueTransformerFactory}
   *
   * This validator is fairly weak, because it doesn't do anything to verify the conditional logic
   * in RawValueTransformerFactory realted to the output format -- but that's out of the scope of 
   * this randomized testing.  
   * 
   * What we're primarily concerned with is that the transformer does it's job and puts the string 
   * in the response, regardless of cloud/RTG/uncommited state of the document.
   */
  private static class RawFieldValueValidator extends RenameFieldValueValidator {
    final String type;
    final String alias;
    public RawFieldValueValidator(final String type, final String fieldName, final String alias) {
      // transformer is weird, default result key doesn't care what params are used...
      super(fieldName, null == alias ? "["+type+"]" : alias);
      this.type = type;
      this.alias = alias;
    }
    public RawFieldValueValidator(final String type, final String fieldName) {
      this(type, fieldName, null);
    }
    public String getFlParam() {
      return (null == alias ? "" : (alias + ":")) + "[" + type + " f=" + expectedFieldName + "]";
    }
    public String getDefaultTransformerFactoryName() {
      return type;
    }
  }
 

  /** 
   * enforces that a valid <code>[docid]</code> is present in the response, possibly using a 
   * resultKey alias.  By default the only validation of docId values is that they are an integer 
   * greater than or equal to <code>-1</code> -- but if any other validator in use returns true 
   * from {@link #requiresRealtimeSearcherReOpen} then the constraint is tightened and values must 
   * be greater than or equal to <code>0</code> 
   */
  private static class DocIdValidator implements FlValidator {
    private static final String NAME = "docid";
    private static final String USAGE = "["+NAME+"]";
    private final String resultKey;
    public DocIdValidator(final String resultKey) {
      this.resultKey = resultKey;
    }
    public DocIdValidator() {
      this(USAGE);
    }
    public String getDefaultTransformerFactoryName() { return NAME; }
    public String getFlParam() { return USAGE.equals(resultKey) ? resultKey : resultKey+":"+USAGE; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      final Object value =  actual.getFirstValue(resultKey);
      assertNotNull(getFlParam() + " => no value in actual doc", value);
      assertTrue(USAGE + " must be an Integer: " + value, value instanceof Integer);

      int minValidDocId = -1; // if it comes from update log
      for (FlValidator other : validators) {
        if (other.requiresRealtimeSearcherReOpen()) {
          minValidDocId = 0;
          break;
        }
      }
      assertTrue(USAGE + " must be >= " + minValidDocId + ": " + value,
                 minValidDocId <= ((Integer)value).intValue());
      return Collections.<String>singleton(resultKey);
    }
  }

  /** Trivial validator of ShardAugmenterFactory */
  private static class ShardValidator implements FlValidator {
    private static final String NAME = "shard";
    private static final String USAGE = "["+NAME+"]";
    private final String resultKey;
    public ShardValidator(final String resultKey) {
      this.resultKey = resultKey;
    }
    public ShardValidator() {
      this(USAGE);
    }
    public String getDefaultTransformerFactoryName() { return NAME; }
    public String getFlParam() { return USAGE.equals(resultKey) ? resultKey : resultKey+":"+USAGE; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      final Object value =  actual.getFirstValue(resultKey);
      assertNotNull(getFlParam() + " => no value in actual doc", value);
      assertTrue(USAGE + " must be an String: " + value, value instanceof String);

      // trivial sanity check
      assertFalse(USAGE + " => blank string", value.toString().trim().isEmpty());
      return Collections.<String>singleton(resultKey);
    }
  }

  /** Trivial validator of ValueAugmenter */
  private static class ValueAugmenterValidator implements FlValidator {
    private static final String NAME = "value";
    private static String trans(final int value) { return "[" + NAME + " v=" + value + " t=int]"; }
    
    private final String resultKey;
    private final String fl;
    private final Integer expectedVal;
    private ValueAugmenterValidator(final String fl, final int expectedVal, final String resultKey) {
      this.resultKey = resultKey;
      this.expectedVal = expectedVal;
      this.fl = fl;
    }
    public ValueAugmenterValidator(final int expectedVal, final String resultKey) {
      this(resultKey + ":" +trans(expectedVal), expectedVal, resultKey);
    }
    public ValueAugmenterValidator(final int expectedVal) {
      // value transformer is weird, default result key doesn't care what params are used...
      this(trans(expectedVal), expectedVal, "["+NAME+"]");
    }
    public String getDefaultTransformerFactoryName() { return NAME; }
    public String getFlParam() { return fl; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      final Object actualVal =  actual.getFirstValue(resultKey);
      assertNotNull(getFlParam() + " => no value in actual doc", actualVal);
      assertEquals(getFlParam(), expectedVal, actualVal);
      return Collections.<String>singleton(resultKey);
    }
  }

  
  /** Trivial validator of a ValueSourceAugmenter */
  private static class FunctionValidator implements FlValidator {
    private static String func(String fieldName) {
      return "add(1.3,sub("+fieldName+","+fieldName+"))";
    }
    protected final String fl;
    protected final String resultKey;
    protected final String fieldName;
    public FunctionValidator(final String fieldName) {
      this(func(fieldName), fieldName, func(fieldName));
    }
    public FunctionValidator(final String fieldName, final String resultKey) {
      this(resultKey + ":" + func(fieldName), fieldName, resultKey);
    }
    private FunctionValidator(final String fl, final String fieldName, final String resultKey) {
      this.fl = fl;
      this.resultKey = resultKey;
      this.fieldName = fieldName;
    }
    /** always returns true */
    public boolean requiresRealtimeSearcherReOpen() { return true; }
    public String getFlParam() { return fl; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      final Object origVal = expected.getFieldValue(fieldName);
      assertTrue("this validator only works on numeric fields: " + origVal, origVal instanceof Number);
      
      assertEquals(fl, 1.3F, actual.getFirstValue(resultKey));
      return Collections.<String>singleton(resultKey);
    }
  }

  
  /** 
   * Trivial validator of a SubQueryAugmenter.  
   *
   * This validator ignores 90% of the features/complexity
   * of SubQueryAugmenter, and instead just focuses on the basics of:
   * <ul>
   *  <li>do a subquery for docs where SUBQ_FIELD contains the id of the top level doc</li>
   *  <li>verify that any subquery match is expected based on indexing pattern</li>
   * </ul>
   */
  private static class SubQueryValidator implements FlValidator {

    // HACK to work around SOLR-9396...
    // 
    // we're using "id" (and only "id") in the subquery.q as a workarround limitation in
    // "$rows.foo" parsing -- it only works reliably if "foo" is in fl, so we only use "$rows.id",
    // which we know is in every request (and is a valid integer)
    
    public final static String NAME = "subquery";
    public final static String SUBQ_KEY = "subq";
    public final static String SUBQ_FIELD = "next_2_ids_i";
    public String getFlParam() { return SUBQ_KEY+":["+NAME+"]"; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      final int compVal = assertParseInt("expected id", expected.getFieldValue("id"));
      
      final Object actualVal = actual.getFieldValue(SUBQ_KEY);
      assertTrue("Expected a doclist: " + actualVal,
                 actualVal instanceof SolrDocumentList);
      assertTrue("should be at most 2 docs in doc list: " + actualVal,
                 ((SolrDocumentList) actualVal).getNumFound() <= 2);
      
      for (SolrDocument subDoc : (SolrDocumentList) actualVal) {
        final int subDocIdVal = assertParseInt("subquery id", subDoc.getFirstValue("id"));
        assertTrue("subDocId="+subDocIdVal+" not in valid range for id="+compVal+" (expected "
                   + (compVal-1) + " or " + (compVal-2) + ")",
                   ((subDocIdVal < compVal) && ((compVal-2) <= subDocIdVal)));
        
      }
    
      return Collections.<String>singleton(SUBQ_KEY);
    }
    public String getDefaultTransformerFactoryName() { return NAME; }
    public SolrParams getExtraRequestParams() {
      return params(SubQueryValidator.SUBQ_KEY + ".q",
                    "{!field f=" + SubQueryValidator.SUBQ_FIELD + " v=$row.id}");
    }
  }
  
  /** Trivial validator of a GeoTransformer */
  private static class GeoTransformerValidator implements FlValidator, SuppressRealFields{
    private static final String NAME = "geo";
    /** 
     * we're not worried about testing the actual geo parsing/formatting of values,
     * just that the transformer gets called with the expected field value.
     * so have a small set of fixed input values we use when indexing docs,
     * and the expected output for each
     */
    private static final Map<String,String> VALUES = new HashMap<>();
    /** 
     * The set of legal field values this validator is willing to test as a list so we can
     * reliably index into it with random ints
     */
    private static final List<String> ALLOWED_FIELD_VALUES;
    static {
      for (int i = -42; i < 66; i+=13) {
        VALUES.put("POINT( 42 "+i+" )", "{\"type\":\"Point\",\"coordinates\":[42,"+i+"]}");
      }
      ALLOWED_FIELD_VALUES = Collections.unmodifiableList(new ArrayList<>(VALUES.keySet()));
    }
    /** 
     * returns a random field value usable when indexing a document that this validator will
     * be able to handle.
     */
    public static String getValueForIndexing(final Random rand) {
      return ALLOWED_FIELD_VALUES.get(rand.nextInt(ALLOWED_FIELD_VALUES.size()));
    }
    private static String trans(String fieldName) {
      return "["+NAME+" f="+fieldName+"]";
    }
    protected final String fl;
    protected final String resultKey;
    protected final String fieldName;
    public GeoTransformerValidator(final String fieldName) {
      // geo transformer is weird, default result key doesn't care what params are used...
      this(trans(fieldName), fieldName, "["+NAME+"]");
    }
    public GeoTransformerValidator(final String fieldName, final String resultKey) {
      this(resultKey + ":" + trans(fieldName), fieldName, resultKey);
    }
    private GeoTransformerValidator(final String fl, final String fieldName, final String resultKey) {
      this.fl = fl;
      this.resultKey = resultKey;
      this.fieldName = fieldName;
    }
    public String getDefaultTransformerFactoryName() { return NAME; }
    public String getFlParam() { return fl; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      final Object origVal = expected.getFieldValue(fieldName);
      assertTrue(fl + ": orig field value is not supported: " + origVal, VALUES.containsKey(origVal));
      
      assertEquals(fl, VALUES.get(origVal), actual.getFirstValue(resultKey));
      return Collections.<String>singleton(resultKey);
    }
    public Set<String> getSuppressedFields() { return Collections.singleton(fieldName); }
  }

  /** 
   * Glob based validator.
   * This class checks that every field in the expected doc exists in the actual doc with the expected 
   * value -- with special exceptions for fields that are "suppressed" (usually via an alias)
   *
   * By design, fields that are aliased are "moved" unless the original field name was explicitly included 
   * in the fl, globs don't count.
   *
   * @see RenameFieldValueValidator
   */
  private static class GlobValidator implements FlValidator {
    private final String glob;
    public GlobValidator(final String glob) {
      this.glob = glob;
    }
    private final Set<String> matchingFieldsCache = new LinkedHashSet<>();
    
    public String getFlParam() { return glob; }
    
    private boolean matchesGlob(final String fieldName) {
      if ( FilenameUtils.wildcardMatch(fieldName, glob) ) {
        matchingFieldsCache.add(fieldName); // Don't calculate it again
        return true;
      }
      return false;
    }
                                
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {

      final Set<String> renamed = new LinkedHashSet<>(validators.size());
      for (FlValidator v : validators) {
        if (v instanceof SuppressRealFields) {
          renamed.addAll(((SuppressRealFields)v).getSuppressedFields());
        }
      }
      
      // every real field name matching the glob that is not renamed should be in the results
      Set<String> result = new LinkedHashSet<>(expected.getFieldNames().size());
      for (String f : expected.getFieldNames()) {
        if ( matchesGlob(f) && (! renamed.contains(f) ) ) {
          result.add(f);
          assertEquals(glob + " => " + f, expected.getFieldValue(f), actual.getFirstValue(f));
        }
      }
      return result;
    }
  }
  
  /** 
   * for things like "score" and "[explain]" where we explicitly expect what we ask for in the fl
   * to <b>not</b> be returned when using RTG.
   */
  private static class NotIncludedValidator implements FlValidator {
    private final String fieldName;
    private final String fl;
    public NotIncludedValidator(final String fl) {
      this(fl, fl);
    }
    public NotIncludedValidator(final String fieldName, final String fl) {
      this.fieldName = fieldName;
      this.fl = fl;
    }
    public String getFlParam() { return fl; }
    public Collection<String> assertRTGResults(final Collection<FlValidator> validators,
                                               final SolrInputDocument expected,
                                               final SolrDocument actual) {
      assertEquals(fl, null, actual.getFirstValue(fieldName));
      return Collections.emptySet();
    }
  }

  /** explain should always be ignored when using RTG */
  private static class ExplainValidator extends NotIncludedValidator {
    private final static String NAME = "explain";
    private final static String USAGE = "[" + NAME + "]";
    public ExplainValidator() {
      super(USAGE);
    }
    public ExplainValidator(final String resultKey) {
      super(USAGE, resultKey + ":" + USAGE);
    }
    public String getDefaultTransformerFactoryName() { return NAME; }
  }

  /** helper method for adding a random number (may be 0) of items from {@link #FL_VALIDATORS} */
  private static void addRandomFlValidators(final Random r, final Set<FlValidator> validators) {
    List<FlValidator> copyToShuffle = new ArrayList<>(FL_VALIDATORS);
    Collections.shuffle(copyToShuffle, r);
    final int numToReturn = r.nextInt(copyToShuffle.size());
    validators.addAll(copyToShuffle.subList(0, numToReturn + 1));
  }

  /**
   * Given an ordered list of values to include in a (key) param, randomly groups them (ie: comma seperated) 
   * into actual param key=values which are returned as a new SolrParams instance
   */
  private static SolrParams buildCommaSepParams(final Random rand, final String key, Collection<String> values) {
    ModifiableSolrParams result = new ModifiableSolrParams();
    List<String> copy = new ArrayList<>(values);
    while (! copy.isEmpty()) {
      List<String> slice = copy.subList(0, random().nextInt(1 + copy.size()));
      result.add(key,String.join(",",slice));
      slice.clear();
    }
    return result;
  }

  /** helper method for asserting an object is a non-null String can be parsed as an int */
  public static int assertParseInt(String msg, Object orig) {
    assertNotNull(msg + ": is null", orig);
    assertTrue(msg + ": is not a string: " + orig, orig instanceof String);
    try {
      return Integer.parseInt(orig.toString());
    } catch (NumberFormatException nfe) {
      throw new AssertionError(msg + ": can't be parsed as a number: " + orig, nfe);
    }
  }
}
