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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.CursorPagingTest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.hamcrest.core.StringContains.containsString;

public class TestRandomCollapseQParserPlugin extends SolrTestCaseJ4 {

  /** Full SolrServer instance for arbitrary introspection of response data and adding fqs */
  public static SolrClient SOLR;
  public static List<String> ALL_SORT_FIELD_NAMES;
  public static List<String> ALL_COLLAPSE_FIELD_NAMES;

  private static String[] NULL_POLICIES
    = new String[] {CollapsingQParserPlugin.NullPolicy.IGNORE.getName(),
                    CollapsingQParserPlugin.NullPolicy.COLLAPSE.getName(),
                    CollapsingQParserPlugin.NullPolicy.EXPAND.getName()};
  
  @BeforeClass
  public static void buildIndexAndClient() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-sorts.xml");
    
    final int totalDocs = atLeast(500);
    for (int i = 1; i <= totalDocs; i++) {
      SolrInputDocument doc = CursorPagingTest.buildRandomDocument(i);
      // every doc will be in the same group for this (string) field
      doc.addField("same_for_all_docs", "xxx");
      assertU(adoc(doc));
    }
    assertU(commit());
    
    // Don't close this client, it would shutdown the CoreContainer
    SOLR = new EmbeddedSolrServer(h.getCoreContainer(), h.coreName);
    
    ALL_SORT_FIELD_NAMES = CursorPagingTest.pruneAndDeterministicallySort
      (h.getCore().getLatestSchema().getFields().keySet());
    
    ALL_COLLAPSE_FIELD_NAMES = new ArrayList<String>(ALL_SORT_FIELD_NAMES.size());
    for (String candidate : ALL_SORT_FIELD_NAMES) {
      if (candidate.startsWith("str")
          || candidate.startsWith("float")
          || candidate.startsWith("int") ) {
        ALL_COLLAPSE_FIELD_NAMES.add(candidate);
      }
    }
  }
  
  @AfterClass
  public static void cleanupStatics() throws Exception {
    deleteCore();
    SOLR = null;
    ALL_SORT_FIELD_NAMES = ALL_COLLAPSE_FIELD_NAMES = null;
  }

  public void testEveryIsolatedSortFieldOnSingleGroup() throws Exception {
    
    for (String sortField : ALL_SORT_FIELD_NAMES) {
      for (String dir : Arrays.asList(" asc", " desc")) {
        
        final String sort = sortField + dir + ", id" + dir; // need id for tie breaker
        final String q = random().nextBoolean() ? "*:*" : CursorPagingTest.buildRandomQuery();

        final SolrParams sortedP = params("q", q, "rows", "1",
                                          "sort", sort);
                                        
        final QueryResponse sortedRsp = SOLR.query(sortedP);

        // random data -- might be no docs matching our query
        if (0 != sortedRsp.getResults().getNumFound()) {
          final SolrDocument firstDoc = sortedRsp.getResults().get(0);

          // check forced array resizing starting from 1
          for (String p : Arrays.asList("{!collapse field=", "{!collapse size='1' field=")) {
            for (String fq : Arrays.asList
                   (p + "same_for_all_docs sort='"+sort+"'}",
                    // nullPolicy=expand shouldn't change anything since every doc has field
                    p + "same_for_all_docs sort='"+sort+"' nullPolicy=expand}",
                    // a field in no docs with nullPolicy=collapse should have same effect as
                    // collapsing on a field in every doc
                    p + "not_in_any_docs sort='"+sort+"' nullPolicy=collapse}")) {
              final SolrParams collapseP = params("q", q, "rows", "1", "fq", fq);
              
              // since every doc is in the same group, collapse query should return exactly one doc
              final QueryResponse collapseRsp = SOLR.query(collapseP);
              assertEquals("collapse should have produced exactly one doc: " + collapseP,
                           1, collapseRsp.getResults().getNumFound());
              final SolrDocument groupHead = collapseRsp.getResults().get(0);
              
              // the group head from the collapse query should match the first doc of a simple sort
              assertEquals(sortedP + " => " + firstDoc + " :VS: " + collapseP + " => " + groupHead,
                           firstDoc.getFieldValue("id"), groupHead.getFieldValue("id"));
            }
          }
        }
      }
    }
  }
  
  public void testRandomCollpaseWithSort() throws Exception {
    
    final int numMainQueriesPerCollapseField = atLeast(5);
    
    for (String collapseField : ALL_COLLAPSE_FIELD_NAMES) {
      for (int i = 0; i < numMainQueriesPerCollapseField; i++) {

        final String topSort = CursorPagingTest.buildRandomSort(ALL_SORT_FIELD_NAMES);
        final String collapseSort = CursorPagingTest.buildRandomSort(ALL_SORT_FIELD_NAMES);
        
        final String q = random().nextBoolean() ? "*:*" : CursorPagingTest.buildRandomQuery();
        
        final SolrParams mainP = params("q", q, "fl", "id,"+collapseField);

        final String csize = random().nextBoolean() ?
          "" : " size=" + TestUtil.nextInt(random(),1,10000);

        final String nullPolicy = randomNullPolicy();
        final String nullPs =  nullPolicy.equals(CollapsingQParserPlugin.NullPolicy.IGNORE.getName())
          // ignore is default, randomly be explicit about it
          ? (random().nextBoolean() ? "" : " nullPolicy=ignore")
          : (" nullPolicy=" + nullPolicy);
        
        final SolrParams collapseP
          = params("sort", topSort,
                   "rows", "200",
                   "fq", ("{!collapse" + csize + nullPs +
                          " field="+collapseField+" sort='"+collapseSort+"'}"));

        try {
          final QueryResponse mainRsp = SOLR.query(SolrParams.wrapDefaults(collapseP, mainP));

          for (SolrDocument doc : mainRsp.getResults()) {
            final Object groupHeadId = doc.getFieldValue("id");
            final Object collapseVal = doc.getFieldValue(collapseField);
            
            if (null == collapseVal) {
              if (nullPolicy.equals(CollapsingQParserPlugin.NullPolicy.EXPAND.getName())) {
                // nothing to check for this doc, it's in its own group
                continue;
              }
              
              assertFalse(groupHeadId + " has null collapseVal but nullPolicy==ignore; " + 
                          "mainP: " + mainP + ", collapseP: " + collapseP,
                          nullPolicy.equals(CollapsingQParserPlugin.NullPolicy.IGNORE.getName()));
            }
            
            // workaround for SOLR-8082...
            //
            // what's important is that we already did the collapsing on the *real* collapseField
            // to verify the groupHead returned is really the best our verification filter
            // on docs with that value in a different field containing the exact same values
            final String checkField = collapseField.replace("float_dv", "float");
            
            final String checkFQ = ((null == collapseVal)
                                    ? ("-" + checkField + ":[* TO *]")
                                    : ("{!field f="+checkField+"}" + collapseVal.toString()));
            
            final SolrParams checkP = params("fq", checkFQ,
                                             "rows", "1",
                                             "sort", collapseSort);
            
            final QueryResponse checkRsp = SOLR.query(SolrParams.wrapDefaults(checkP, mainP));
            
            assertTrue("not even 1 match for sanity check query? expected: " + doc,
                       ! checkRsp.getResults().isEmpty());
            final SolrDocument firstMatch = checkRsp.getResults().get(0);
            final Object firstMatchId = firstMatch.getFieldValue("id");
            assertEquals("first match for filtered group '"+ collapseVal +
                         "' not matching expected group head ... " +
                         "mainP: " + mainP + ", collapseP: " + collapseP + ", checkP: " + checkP,
                         groupHeadId, firstMatchId);
          }
        } catch (Exception e) {
          throw new RuntimeException("BUG using params: " + collapseP + " + " + mainP, e);
        }
      }
    }
  }
  
  public void testParsedFilterQueryResponse() throws Exception {
    String nullPolicy = randomNullPolicy();
    String groupHeadSort = "'_version_ asc'";
    String collapseSize = "5000";
    String collapseHint = "top_fc";
    String filterQuery = "{!collapse field=id sort=" + groupHeadSort + " nullPolicy=" + nullPolicy + " size=" +
                                collapseSize + " hint=" + collapseHint + "}";
    SolrParams solrParams = params("q", "*:*", "rows", "0", "debug", "true", "fq", filterQuery);

    QueryResponse response = SOLR.query(solrParams);
    // Query name is occurring twice, this should be handled in QueryParsing.toString
    String expectedParsedFilterString = "CollapsingPostFilter(CollapsingPostFilter(field=id, " +
        "nullPolicy=" + nullPolicy + ", GroupHeadSelector(selectorText=" + groupHeadSort.substring(1,
        groupHeadSort.length() - 1) + ", type=SORT" +
        "), hint=" + collapseHint + ", size=" + collapseSize + "))";
    List<String> expectedParsedFilterQuery = Collections.singletonList(expectedParsedFilterString);
    assertEquals(expectedParsedFilterQuery, response.getDebugMap().get("parsed_filter_queries"));
    assertEquals(Collections.singletonList(filterQuery), response.getDebugMap().get("filter_queries"));
  }

  public void testNullPolicy() {
    String nullPolicy = "xyz";
    String groupHeadSort = "'_version_ asc'";
    String filterQuery = "{!collapse field=id sort=" + groupHeadSort + " nullPolicy=" + nullPolicy + "}";
    SolrParams solrParams = params("q", "*:*", "fq", filterQuery);

    SolrException e = expectThrows(SolrException.class, () -> SOLR.query(solrParams));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("Invalid nullPolicy: " + nullPolicy));

    // valid nullPolicy
    assertQ(req("q", "*:*", "fq", "{!collapse field=id nullPolicy=" + randomNullPolicy() + "}"));
  }

  private String randomNullPolicy() {
    return NULL_POLICIES[ TestUtil.nextInt(random(), 0, NULL_POLICIES.length-1) ];
  }
  
}
