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

import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.util.NamedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.BeforeClass;

/**
 * Builds a random index of a few simple fields, maintaining an in-memory model of the expected
 * doc counts so that we can verify the results of range facets w/ nested field facets that need refinement.
 *
 * The focus here is on stressing the cases where the document values fall direct only on the
 * range boundaries, and how the various "include" options affects refinement.
 */
public class RangeFacetCloudTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String COLLECTION = MethodHandles.lookup().lookupClass().getName();
  private static final String CONF = COLLECTION + "_configSet";

  private static final String INT_FIELD = "range_i";
  private static final String STR_FIELD = "facet_s";
  private static final int NUM_RANGE_VALUES = 6;
  private static final int TERM_VALUES_RANDOMIZER = 100;

  private static final List<String> SORTS = Arrays.asList("count desc", "count asc", "index asc", "index desc");
  
  private static final List<EnumSet<FacetRangeOther>> OTHERS = buildListOfFacetRangeOtherOptions();
  private static final List<FacetRangeOther> BEFORE_AFTER_BETWEEN
    = Arrays.asList(FacetRangeOther.BEFORE, FacetRangeOther.AFTER, FacetRangeOther.BETWEEN);
    
  /**
   * the array indexes represent values in our numeric field, while the array values
   * track the number of docs that will have that value.
   */
  private static final int[] RANGE_MODEL = new int[NUM_RANGE_VALUES];
  /**
   * the array indexes represent values in our numeric field, while the array values
   * track the mapping from string field terms to facet counts for docs that have that numeric value
   */
  private static final Map<String,Integer>[] TERM_MODEL = new Map[NUM_RANGE_VALUES];
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    final int numShards = TestUtil.nextInt(random(),1,5);
    final int numReplicas = 1;
    final int maxShardsPerNode = 1;
    final int nodeCount = numShards * numReplicas;

    configureCluster(nodeCount)
      .addConfig(CONF, Paths.get(TEST_HOME(), "collection1", "conf"))
      .configure();

    assertEquals(0, (CollectionAdminRequest.createCollection(COLLECTION, CONF, numShards, numReplicas)
                     .setMaxShardsPerNode(maxShardsPerNode)
                     .setProperties(Collections.singletonMap(CoreAdminParams.CONFIG, "solrconfig-minimal.xml"))
                     .process(cluster.getSolrClient())).getStatus());
    
    cluster.getSolrClient().setDefaultCollection(COLLECTION);

    final int numDocs = atLeast(1000);
    final int maxTermId = atLeast(TERM_VALUES_RANDOMIZER);

    // clear the RANGE_MODEL
    Arrays.fill(RANGE_MODEL, 0);
    // seed the TERM_MODEL Maps so we don't have null check later
    for (int i = 0; i < NUM_RANGE_VALUES; i++) {
      TERM_MODEL[i] = new LinkedHashMap<>();
    }

    // build our index & our models
    for (int id = 0; id < numDocs; id++) {
      final int rangeVal = random().nextInt(NUM_RANGE_VALUES);
      final String termVal = "x" + random().nextInt(maxTermId);
      final SolrInputDocument doc = sdoc("id", ""+id,
                                         INT_FIELD, ""+rangeVal,
                                         STR_FIELD, termVal);
      RANGE_MODEL[rangeVal]++;
      TERM_MODEL[rangeVal].merge(termVal, 1, Integer::sum);

      assertEquals(0, (new UpdateRequest().add(doc)).process(cluster.getSolrClient()).getStatus());
    }
    assertEquals(0, cluster.getSolrClient().commit().getStatus());
    
  }

  public void testInclude_Lower() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:lower", "")) { // same behavior
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:5, gap:1"+otherStr+include+subFacet+" } }");

          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");

            assertEquals("num buckets", 4, buckets.size());
            for (int i = 0; i < 4; i++) {
              int expectedVal = i+1;
              assertBucket("bucket#" + i, expectedVal, modelVals(expectedVal), subFacetLimit, buckets.get(i));
            }

            assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);

          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }

  public void testInclude_Lower_Gap2() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:lower", "")) { // same behavior
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             "{ foo:{ type:range, field:"+INT_FIELD+" start:0, end:5, gap:2"+otherStr+include+subFacet+" } }");
        
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 3, buckets.size());
            assertBucket("bucket#0", 0, modelVals(0,1), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 2, modelVals(2,3), subFacetLimit, buckets.get(1));
            assertBucket("bucket#2", 4, modelVals(4,5), subFacetLimit, buckets.get(2));
            
            assertBeforeAfterBetween(other, emptyVals(), emptyVals(), modelVals(0,5), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }
  public void testInclude_Lower_Gap2_hardend() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:lower", "")) { // same behavior
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             "{ foo:{ type:range, field:"+INT_FIELD+" start:0, end:5, gap:2, hardend:true"
             +        otherStr+include+subFacet+" } }");
        
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 3, buckets.size());
            assertBucket("bucket#0", 0, modelVals(0,1), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 2, modelVals(2,3), subFacetLimit, buckets.get(1));
            assertBucket("bucket#2", 4, modelVals(4), subFacetLimit, buckets.get(2));
            
            assertBeforeAfterBetween(other, emptyVals(), modelVals(5), modelVals(0,4), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }

  public void testStatsWithOmitHeader() throws Exception {
    // SOLR-13509: no NPE should be thrown when only stats are specified with omitHeader=true
    SolrQuery solrQuery = new SolrQuery("q", "*:*", "omitHeader", "true",
        "json.facet", "{unique_foo:\"unique(" + STR_FIELD+ ")\"}");
    final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
    // response shouldn't contain header as omitHeader is set to true
    assertNull(rsp.getResponseHeader());
  }
  
  public void testInclude_Upper() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        final SolrQuery solrQuery = new SolrQuery
          ("q", "*:*", "rows", "0", "json.facet",
           // exclude a single high value from our ranges
           "{ foo:{ type:range, field:"+INT_FIELD+" start:0, end:4, gap:1, include:upper"+otherStr+subFacet+" } }");
    
        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
        try {
          final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
          final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
          
          assertEquals("num buckets", 4, buckets.size());
          for (int i = 0; i < 4; i++) {
            assertBucket("bucket#" + i, i, modelVals(i+1), subFacetLimit, buckets.get(i));
          }
          assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);
          
        } catch (AssertionError|RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }
      }
    }
  }
  public void testInclude_Upper_Gap2() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        final SolrQuery solrQuery = new SolrQuery
          ("q", "*:*", "rows", "0", "json.facet",
           // exclude a single high value from our ranges
           "{ foo:{ type:range, field:"+INT_FIELD+" start:0, end:4, gap:2, include:upper"+otherStr+subFacet+" } }");
    
        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
        try {
          final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
          final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
          
          assertEquals("num buckets", 2, buckets.size());
          assertBucket("bucket#0", 0, modelVals(1,2), subFacetLimit, buckets.get(0));
          assertBucket("bucket#1", 2, modelVals(3,4), subFacetLimit, buckets.get(1));
          
          assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);
          
        } catch (AssertionError|RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }
      }
    }
  }
  
  public void testInclude_Edge() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        final SolrQuery solrQuery = new SolrQuery
          ("q", "*:*", "rows", "0", "json.facet",
           // exclude a single low/high value from our ranges
           "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:4, gap:1, include:edge"+otherStr+subFacet+" } }");
        
        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
        try {
          final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
          final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
          
          assertEquals("num buckets", 3, buckets.size());
          
          assertBucket("bucket#0", 1, modelVals(1), subFacetLimit, buckets.get(0));
          
          // middle bucket doesn't include lower or upper so it's empty
          assertBucket("bucket#1", 2, emptyVals(), subFacetLimit, buckets.get(1));
          
          assertBucket("bucket#2", 3, modelVals(4), subFacetLimit, buckets.get(2));
          
          assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);
          
        } catch (AssertionError|RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }
      }
    }
  }

  public void testInclude_EdgeLower() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:'edge,lower'", ", include:[edge,lower]")) { // same
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:4, gap:1"+otherStr+include+subFacet+" } }");
          
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 3, buckets.size());
            
            assertBucket("bucket#0", 1, modelVals(1), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 2, modelVals(2), subFacetLimit, buckets.get(1));
            assertBucket("bucket#2", 3, modelVals(3,4), subFacetLimit, buckets.get(2));
            
            assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }
  
  public void testInclude_EdgeUpper() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:'edge,upper'", ", include:[edge,upper]")) { // same
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:4, gap:1"+otherStr+include+subFacet+" } }");
          
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 3, buckets.size());
            
            assertBucket("bucket#0", 1, modelVals(1,2), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 2, modelVals(3), subFacetLimit, buckets.get(1));
            assertBucket("bucket#2", 3, modelVals(4), subFacetLimit, buckets.get(2));
            
            assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }
  
  public void testInclude_EdgeLowerUpper() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:'edge,lower,upper'", ", include:[edge,lower,upper]")) { // same
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:4, gap:1"+otherStr+include+subFacet+" } }");
          
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 3, buckets.size());
            
            assertBucket("bucket#0", 1, modelVals(1,2), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 2, modelVals(2,3), subFacetLimit, buckets.get(1));
            assertBucket("bucket#2", 3, modelVals(3,4), subFacetLimit, buckets.get(2));
            
            assertBeforeAfterBetween(other, modelVals(0), modelVals(5), modelVals(1,4), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }
  
  public void testInclude_All() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:'edge,lower,upper,outer'",
                                            ", include:[edge,lower,upper,outer]",
                                            ", include:all")) { // same
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:4, gap:1"+otherStr+include+subFacet+" } }");
          
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 3, buckets.size());
            
            assertBucket("bucket#0", 1, modelVals(1,2), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 2, modelVals(2,3), subFacetLimit, buckets.get(1));
            assertBucket("bucket#2", 3, modelVals(3,4), subFacetLimit, buckets.get(2));
            
            assertBeforeAfterBetween(other, modelVals(0,1), modelVals(4,5), modelVals(1,4), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }

  /**
   * This test will also sanity check that mincount is working properly 
   */
  public void testInclude_All_Gap2() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:'edge,lower,upper,outer'",
                                            ", include:[edge,lower,upper,outer]",
                                            ", include:all")) { // same

          // we also want to sanity check that mincount doesn't bork anything,
          // so we're going to do the query twice:
          // 1) no mincount, keep track of which bucket has the highest count & what it was
          // 2) use that value as the mincount, assert that the other bucket isn't returned
          long mincount_to_use = -1;
          Object expected_mincount_bucket_val = null; // HACK: use null to mean neither in case of tie

          // initial query, no mincount...
          SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+", start:1, end:4, gap:2"+otherStr+include+subFacet+" } }");
          
          QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 2, buckets.size());
            
            assertBucket("bucket#0", 1, modelVals(1,3), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 3, modelVals(3,5), subFacetLimit, buckets.get(1));
            
            assertBeforeAfterBetween(other, modelVals(0,1), modelVals(5), modelVals(1,5), subFacetLimit, foo);

            // if we've made it this far, then our buckets match the model
            // now use our buckets to pick a mincount to use based on the MIN(+1) count seen
            long count0 = ((Number)buckets.get(0).get("count")).longValue();
            long count1 = ((Number)buckets.get(1).get("count")).longValue();
            
            mincount_to_use = 1 + Math.min(count0, count1);
            if (count0 > count1) {
              expected_mincount_bucket_val = buckets.get(0).get("val");
            } else if (count1 > count0) {
              expected_mincount_bucket_val = buckets.get(1).get("val");
            }

          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }

          // second query, using mincount...
          solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges, 
             "{ foo:{ type:range, field:"+INT_FIELD+", mincount:" + mincount_to_use +
             ", start:1, end:4, gap:2"+otherStr+include+subFacet+" } }");
          
          rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");

            if (null == expected_mincount_bucket_val) {
              assertEquals("num buckets", 0, buckets.size());
            } else {
              assertEquals("num buckets", 1, buckets.size());
              final Object actualBucket = buckets.get(0);
              if (expected_mincount_bucket_val.equals(1)) {
                assertBucket("bucket#0(0)", 1, modelVals(1,3), subFacetLimit, actualBucket);
              } else {
                assertBucket("bucket#0(1)", 3, modelVals(3,5), subFacetLimit, actualBucket);
              }
            }
            
            // regardless of mincount, the before/after/between special buckets should always be returned
            assertBeforeAfterBetween(other, modelVals(0,1), modelVals(5), modelVals(1,5), subFacetLimit, foo);

          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }
  
  public void testInclude_All_Gap2_hardend() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (EnumSet<FacetRangeOther> other : OTHERS) {
        final String otherStr = formatFacetRangeOther(other);
        for (String include : Arrays.asList(", include:'edge,lower,upper,outer'",
                                            ", include:[edge,lower,upper,outer]",
                                            ", include:all")) { // same
          final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
             // exclude a single low/high value from our ranges
             "{ foo:{ type:range, field:"+INT_FIELD+" start:1, end:4, gap:2, hardend:true"
             +         otherStr+include+subFacet+" } }");
          
          final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
          try {
            final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
            final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");
            
            assertEquals("num buckets", 2, buckets.size());
            
            assertBucket("bucket#0", 1, modelVals(1,3), subFacetLimit, buckets.get(0));
            assertBucket("bucket#1", 3, modelVals(3,4), subFacetLimit, buckets.get(1));
            
            assertBeforeAfterBetween(other, modelVals(0,1), modelVals(4,5), modelVals(1,4), subFacetLimit, foo);
            
          } catch (AssertionError|RuntimeException ae) {
            throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
          }
        }
      }
    }
  }

  public void testRangeWithInterval() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (boolean incUpper : Arrays.asList(false, true)) {
        String incUpperStr = ",inclusive_to:"+incUpper;
        final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
                "{ foo:{ type:range, field:" + INT_FIELD + " ranges:[{from:1, to:2"+ incUpperStr+ "}," +
                    "{from:2, to:3"+ incUpperStr +"},{from:3, to:4"+ incUpperStr +"},{from:4, to:5"+ incUpperStr+"}]"
                    + subFacet + " } }");

        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
        try {
          final NamedList<Object> foo = ((NamedList<NamedList<Object>>) rsp.getResponse().get("facets")).get("foo");
          final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");

          assertEquals("num buckets", 4, buckets.size());
          for (int i = 0; i < 4; i++) {
            String expectedVal = "[" + (i + 1) + "," + (i + 2) + (incUpper? "]": ")");
            ModelRange modelVals = incUpper? modelVals(i+1, i+2) : modelVals(i+1);
            assertBucket("bucket#" + i, expectedVal, modelVals, subFacetLimit, buckets.get(i));
          }
        } catch (AssertionError | RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }
      }
    }
  }

  public void testRangeWithOldIntervalFormat() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);
      for (boolean incUpper : Arrays.asList(false, true)) {
        String incUpperStr = incUpper? "]\"":")\"";
        final SolrQuery solrQuery = new SolrQuery
            ("q", "*:*", "rows", "0", "json.facet",
                "{ foo:{ type:range, field:" + INT_FIELD + " ranges:[{range:\"[1,2"+ incUpperStr+ "}," +
                    "{range:\"[2,3"+ incUpperStr +"},{range:\"[3,4"+ incUpperStr +"},{range:\"[4,5"+ incUpperStr+"}]"
                    + subFacet + " } }");

        final QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
        try {
          final NamedList<Object> foo = ((NamedList<NamedList<Object>>) rsp.getResponse().get("facets")).get("foo");
          final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");

          assertEquals("num buckets", 4, buckets.size());
          for (int i = 0; i < 4; i++) {
            String expectedVal = "[" + (i + 1) + "," + (i + 2) + (incUpper? "]": ")");
            ModelRange modelVals = incUpper? modelVals(i+1, i+2) : modelVals(i+1);
            assertBucket("bucket#" + i, expectedVal, modelVals, subFacetLimit, buckets.get(i));
          }
        } catch (AssertionError | RuntimeException ae) {
          throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
        }
      }
    }
  }

  public void testIntervalWithMincount() throws Exception {
    for (boolean doSubFacet : Arrays.asList(false, true)) {
      final Integer subFacetLimit = pickSubFacetLimit(doSubFacet);
      final CharSequence subFacet = makeSubFacet(subFacetLimit);

      long mincount_to_use = -1;
      Object expected_mincount_bucket_val = null;

      // without mincount
      SolrQuery solrQuery = new SolrQuery(
          "q", "*:*", "rows", "0", "json.facet",
          "{ foo:{ type:range, field:" + INT_FIELD + " ranges:[{from:1, to:3},{from:3, to:5}]" +
              subFacet + " } }"
      );

      QueryResponse rsp = cluster.getSolrClient().query(solrQuery);
      try {
        final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
        final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");

        assertEquals("num buckets", 2, buckets.size());

        // upper is not included
        assertBucket("bucket#0", "[1,3)", modelVals(1,2), subFacetLimit, buckets.get(0));
        assertBucket("bucket#1", "[3,5)", modelVals(3,4), subFacetLimit, buckets.get(1));

        // if we've made it this far, then our buckets match the model
        // now use our buckets to pick a mincount to use based on the MIN(+1) count seen
        long count0 = ((Number)buckets.get(0).get("count")).longValue();
        long count1 = ((Number)buckets.get(1).get("count")).longValue();

        mincount_to_use = 1 + Math.min(count0, count1);
        if (count0 > count1) {
          expected_mincount_bucket_val = buckets.get(0).get("val");
        } else if (count1 > count0) {
          expected_mincount_bucket_val = buckets.get(1).get("val");
        }

      } catch (AssertionError|RuntimeException ae) {
        throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
      }

      // with mincount
      solrQuery = new SolrQuery(
          "q", "*:*", "rows", "0", "json.facet",
          "{ foo:{ type:range, field:" + INT_FIELD + " ranges:[{from:1, to:3},{from:3, to:5}]" +
              ",mincount:" + mincount_to_use + subFacet + " } }"
      );

      rsp = cluster.getSolrClient().query(solrQuery);
      try {
        final NamedList<Object> foo = ((NamedList<NamedList<Object>>)rsp.getResponse().get("facets")).get("foo");
        final List<NamedList<Object>> buckets = (List<NamedList<Object>>) foo.get("buckets");

        if (null == expected_mincount_bucket_val) {
          assertEquals("num buckets", 0, buckets.size());
        } else {
          assertEquals("num buckets", 1, buckets.size());
          final Object actualBucket = buckets.get(0);
          if (expected_mincount_bucket_val.equals("[1,3)")) {
            assertBucket("bucket#0(0)", "[1,3)", modelVals(1,2), subFacetLimit, actualBucket);
          } else {
            assertBucket("bucket#0(1)", "[3,5)", modelVals(3,4), subFacetLimit, actualBucket);
          }
        }
      } catch (AssertionError|RuntimeException ae) {
        throw new AssertionError(solrQuery.toString() + " -> " + rsp.toString() + " ===> " + ae.getMessage(), ae);
      }
    }
  }

  /**
   * Helper method for validating a single 'bucket' from a Range facet.
   *
   * @param label to use in assertions
   * @param expectedVal <code>"val"</code> to assert for this bucket, use <code>null</code> for special "buckets" like before, after, between.
   * @param expectedRangeValues a range of the expected values in the numeric field whose cumulative counts should match this buckets <code>"count"</code>
   * @param subFacetLimitUsed if null, then assert this bucket has no  <code>"bar"</code> subfacet, otherwise assert expected term counts for each actual term, and sanity check the number terms returnd against the model and/or this limit.
   * @param actualBucket the actual bucket returned from a query for all assertions to be conducted against.
   */
  private static void assertBucket(final String label,
                                   final Object expectedVal,
                                   final ModelRange expectedRangeValues,
                                   final Integer subFacetLimitUsed,
                                   final Object actualBucket) {
    try {
      
      assertNotNull("null bucket", actualBucket);
      assertNotNull("expectedRangeValues", expectedRangeValues);
      assertTrue("bucket is not a NamedList", actualBucket instanceof NamedList);
      final NamedList<Object> bucket = (NamedList<Object>) actualBucket;

      if (null != expectedVal) {
        assertEquals("val", expectedVal, bucket.get("val"));
      }
      
      // figure out the model from our range of values...
      long expectedCount = 0;
      List<Map<String,Integer>> toMerge = new ArrayList<>(NUM_RANGE_VALUES);
      for (int i = expectedRangeValues.lower; i <= expectedRangeValues.upper; i++) {
        expectedCount += RANGE_MODEL[i];
        toMerge.add(TERM_MODEL[i]);
      }

      assertEqualsHACK("count", expectedCount, bucket.get("count"));
      
      // merge the maps of our range values by summing the (int) values on key collisions
      final Map<String,Long> expectedTermCounts = toMerge.stream()
        .flatMap(m -> m.entrySet().stream())
        .collect(Collectors.toMap(Entry::getKey, (e -> e.getValue().longValue()), Long::sum));

      if (null == subFacetLimitUsed || 0 == expectedCount) {
        assertNull("unexpected subfacets", bucket.get("bar"));
      } else {
        NamedList<Object> bar = ((NamedList<Object>)bucket.get("bar"));
        assertNotNull("can't find subfacet 'bar'", bar);

        final int numBucketsExpected = subFacetLimitUsed < 0
          ? expectedTermCounts.size() : Math.min(subFacetLimitUsed, expectedTermCounts.size());
        final List<NamedList<Object>> subBuckets = (List<NamedList<Object>>) bar.get("buckets");
        // we should either have filled out the expected limit, or 
        assertEquals("num subfacet buckets", numBucketsExpected, subBuckets.size());

        // assert sub-facet term counts for the subBuckets that do exist
        for (NamedList<Object> subBucket : subBuckets) {
          final Object term = subBucket.get("val");
          assertNotNull("subfacet bucket with null term: " + subBucket, term);
          final Long expectedTermCount = expectedTermCounts.get(term.toString());
          assertNotNull("unexpected subfacet bucket: " + subBucket, expectedTermCount);
          assertEqualsHACK("subfacet count for term: " + term, expectedTermCount, subBucket.get("count"));
        }
      }
        
    } catch (AssertionError|RuntimeException ae) {
      throw new AssertionError(label + ": " + ae.getMessage(), ae);
    }
  }
  
  /**
   * A convenience method for calling {@link #assertBucket} on the before/after/between buckets
   * of a facet result, based on the {@link FacetRangeOther} specified for this facet.
   * 
   * @see #assertBucket
   * @see #buildListOfFacetRangeOtherOptions 
   */
  private static void assertBeforeAfterBetween(final EnumSet<FacetRangeOther> other,
                                               final ModelRange before,
                                               final ModelRange after,
                                               final ModelRange between,
                                               final Integer subFacetLimitUsed,
                                               final NamedList<Object> facet) {
    //final String[] names = new String[] { "before", "after", "between" };
    assertEquals(3, BEFORE_AFTER_BETWEEN.size());
    final ModelRange[] expected = new ModelRange[] { before, after, between };
    for (int i = 0; i < 3; i++) {
      FacetRangeOther key = BEFORE_AFTER_BETWEEN.get(i);
      String name = key.toString();
      if (other.contains(key) || other.contains(FacetRangeOther.ALL)) {
        assertBucket(name, null, expected[i], subFacetLimitUsed, facet.get(name));
      } else {
        assertNull("unexpected other=" + name, facet.get(name));
      }
    }
  }

  /** 
   * A little helper struct to make the method sig of {@link #assertBucket} more readable.
   * If lower (or upper) is negative, then both must be negative and upper must be less then 
   * lower -- this indicate that the bucket should be empty.
   * @see #modelVals
   * @see #emptyVals
   */
  private static final class ModelRange {
    public final int lower;
    public final int upper;
    /** Don't use, use the convenience methods */
    public ModelRange(int lower, int upper) {
      if (lower < 0 || upper < 0) {
        assert(lower < 0 && upper < lower);
      } else {
        assert(lower <= upper);
      }
      this.lower = lower;
      this.upper = upper;
    }
  }
  private static final ModelRange emptyVals() {
    return new ModelRange(-1, -100);
  }
  private static final ModelRange modelVals(int value) {
    return modelVals(value, value);
  }
  private static final ModelRange modelVals(int lower, int upper) {
    assertTrue(upper + " < " + lower, lower <= upper);
    assertTrue("negative lower", 0 <= lower);
    assertTrue("negative upper", 0 <= upper);
    return new ModelRange(lower, upper);
  }

  /** randomized helper */
  private static final Integer pickSubFacetLimit(final boolean doSubFacet) {
    if (! doSubFacet) { return null; }
    int result = TestUtil.nextInt(random(), -10, atLeast(TERM_VALUES_RANDOMIZER));
    return (result <= 0) ? -1 : result;
  }
  /** randomized helper */
  private static final CharSequence makeSubFacet(final Integer subFacetLimit) {
    if (null == subFacetLimit) {
      return "";
    }
    final StringBuilder result = new StringBuilder(", facet:{ bar:{ type:terms, refine:true, field:"+STR_FIELD);
    // constrain overrequesting to stress refiement, but still test those codepaths
    final String overrequest = random().nextBoolean() ? "0" : "1";
      
    result.append(", overrequest:").append(overrequest).append(", limit:").append(subFacetLimit);
    
    // order should have no affect on our testing
    if (random().nextBoolean()) {
      result.append(", sort:'").append(SORTS.get(random().nextInt(SORTS.size()))).append("'");
    }
    result.append("} }");
    return result;
  }

  /** 
   * Helper for seeding the re-used static struct, and asserting no one changes the Enum w/o updating this test
   *
   * @see #assertBeforeAfterBetween 
   * @see #formatFacetRangeOther
   * @see #OTHERS
   */
  private static final List<EnumSet<FacetRangeOther>> buildListOfFacetRangeOtherOptions() {
    assertEquals("If someone adds to FacetRangeOther this method (and bulk of test) needs updated",
                 5, EnumSet.allOf(FacetRangeOther.class).size());
    
    // we're not overly concerned about testing *EVERY* permutation,
    // we just want to make sure we test multiple code paths (some, all, "ALL", none)
    //
    // NOTE: Don't mix "ALL" or "NONE" with other options so we don't have to make assertBeforeAfterBetween
    // overly complicated
    ArrayList<EnumSet<FacetRangeOther>> results = new ArrayList(5);
    results.add(EnumSet.of(FacetRangeOther.ALL));
    results.add(EnumSet.of(FacetRangeOther.BEFORE, FacetRangeOther.AFTER, FacetRangeOther.BETWEEN));
    results.add(EnumSet.of(FacetRangeOther.BEFORE, FacetRangeOther.AFTER));
    results.add(EnumSet.of(FacetRangeOther.BETWEEN));
    results.add(EnumSet.of(FacetRangeOther.NONE));
    return results;
  }
  
  /** 
   * @see #assertBeforeAfterBetween 
   * @see #buildListOfFacetRangeOtherOptions
   */
  private static final String formatFacetRangeOther(EnumSet<FacetRangeOther> other) {
    if (other.contains(FacetRangeOther.NONE) && random().nextBoolean()) {
      return ""; // sometimes don't output a param at all when we're dealing with the default NONE
    }
    String val = other.toString();
    if (random().nextBoolean()) {
      // two valid syntaxes to randomize between:
      // - a JSON list of items (conveniently the default toString of EnumSet),
      // - a single quoted string containing the comma separated list
      val = val.replaceAll("\\[|\\]","'");

      // HACK: work around SOLR-12539...
      //
      // when sending a single string containing a comma separated list of values, JSON Facets 'other'
      // parsing can't handle any leading (or trailing?) whitespace
      val = val.replaceAll("\\s","");
    }
    return ", other:" + val;
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
