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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.apache.lucene.search.Query;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;

import org.junit.After;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.instanceOf;

/** Test collapse functionality with hierarchical documents using 'block collapse' */
public class TestBlockCollapse extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    initCore("solrconfig-collapseqparser.xml", "schema15.xml");
  }

  @After
  public void cleanup() throws Exception {
    clearIndex();
    assertU(commit());
  }

  public void testPostFilterIntrospection() throws Exception {
    final List<String> fieldValueSelectors = Arrays.asList("sort='bar_i asc'",
                                                           "min=bar_i",
                                                           "max=bar_i",
                                                           "min='sum(bar_i, 42)'",
                                                           "max='sum(bar_i, 42)'");
    for (SolrParams p : Arrays.asList(params(),
                                      // QEC boosting shouldn't impact what impl we get in any situation
                                      params("qt", "/elevate", "elevateIds", "42"))) {
                                             
      try (SolrQueryRequest req = req()) {
        // non-block based collapse sitautions, regardless of nullPolicy...
        for (String np : Arrays.asList("", " nullPolicy=ignore", " nullPolicy=expand", " nullPolicy=collapse",
                                       // when policy is 'collapse' hint should be ignored...
                                       " nullPolicy=collapse hint=block")) {
          assertThat(parseAndBuildCollector("{!collapse field=foo_s1"+np+"}", req), 
                     instanceOf(CollapsingQParserPlugin.OrdScoreCollector.class));
          assertThat(parseAndBuildCollector("{!collapse field=foo_i"+np+"}", req), 
                     instanceOf(CollapsingQParserPlugin.IntScoreCollector.class));
          for (String selector : fieldValueSelectors) {
            assertThat(parseAndBuildCollector("{!collapse field=foo_s1 " + selector + np + "}", req), 
                       instanceOf(CollapsingQParserPlugin.OrdFieldValueCollector.class));
          }
          for (String selector : fieldValueSelectors) {
            assertThat(parseAndBuildCollector("{!collapse field=foo_i " + selector + np + "}", req), 
                       instanceOf(CollapsingQParserPlugin.IntFieldValueCollector.class));
          }
          
          // anything with cscore() is (currently) off limits regardless of null policy or hint...
          for (String selector : Arrays.asList(" min=sum(42,cscore())",
                                               " max=cscore()")) {
            for (String hint : Arrays.asList("", " hint=block")) {
              assertThat(parseAndBuildCollector("{!collapse field=_root_" + selector + np + hint + "}", req), 
                         instanceOf(CollapsingQParserPlugin.OrdFieldValueCollector.class));
              assertThat(parseAndBuildCollector("{!collapse field=foo_s1" + selector + np + hint + "}", req), 
                         instanceOf(CollapsingQParserPlugin.OrdFieldValueCollector.class));
              assertThat(parseAndBuildCollector("{!collapse field=foo_i" + selector + np + hint + "}", req), 
                         instanceOf(CollapsingQParserPlugin.IntFieldValueCollector.class));
            }
          }
        }
        
        // block based collectors as long as nullPolicy isn't collapse...
        for (String np : Arrays.asList("", " nullPolicy=ignore", " nullPolicy=expand")) {
          assertThat(parseAndBuildCollector("{!collapse field=_root_"+np+"}", req),             // implicit block collection on _root_
                     instanceOf(CollapsingQParserPlugin.BlockOrdScoreCollector.class));
          assertThat(parseAndBuildCollector("{!collapse field=_root_ hint=top_fc"+np+"}", req), // top_fc shouldn't stop implicit block collection
                     instanceOf(CollapsingQParserPlugin.BlockOrdScoreCollector.class));
          assertThat(parseAndBuildCollector("{!collapse field=foo_s1 hint=block"+np+"}", req),
                     instanceOf(CollapsingQParserPlugin.BlockOrdScoreCollector.class));
          assertThat(parseAndBuildCollector("{!collapse field=foo_i hint=block"+np+"}", req),
                     instanceOf(CollapsingQParserPlugin.BlockIntScoreCollector.class));
          for (String selector : fieldValueSelectors) {
            assertThat(parseAndBuildCollector("{!collapse field=foo_s1 hint=block " + selector + np + "}", req), 
                       instanceOf(CollapsingQParserPlugin.BlockOrdSortSpecCollector.class));
          }
          for (String selector : fieldValueSelectors) {
            assertThat(parseAndBuildCollector("{!collapse field=foo_i hint=block " + selector + np + "}", req), 
                       instanceOf(CollapsingQParserPlugin.BlockIntSortSpecCollector.class));
          }
        }
      
      }
    }
    
  }
  
  /** 
   * Helper method for introspection testing 
   * @see #testPostFilterIntrospection
   */
  private DelegatingCollector parseAndBuildCollector(final String input, final SolrQueryRequest req) throws Exception {
    try {
      final SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req,rsp));
      
      final Query q = QParser.getParser(input, "lucene", true, req).getQuery();
      assertTrue("Not a PostFilter: " + input, q instanceof PostFilter);
      return ((PostFilter)q).getFilterCollector(req.getSearcher());
    } finally {
      SolrRequestInfo.clearRequestInfo();
    }
  }


  public void testEmptyIndex() throws Exception {
    // some simple sanity checks that collapse queries against empty indexes don't match any docs
    // (or throw any errors)
    
    doTestEmptyIndex();
    
    assertU(adoc(dupFields(sdoc("id", "p1",
                                "block_i", 1, 
                                "skus", sdocs(dupFields(sdoc("id", "p1s1", "block_i", 1, "txt_t", "a  b  c  d  e ", "num_i", 42)),
                                              dupFields(sdoc("id", "p1s2", "block_i", 1, "txt_t", "a  XX c  d  e ", "num_i", 10)),
                                              dupFields(sdoc("id", "p1s3", "block_i", 1, "txt_t", "XX b  XX XX e ", "num_i", 777)),
                                              dupFields(sdoc("id", "p1s4", "block_i", 1, "txt_t", "a  XX c  d  XX", "num_i", 6))
                                              )))));
    assertU(commit());
    assertU(delQ("_root_:p1")); // avoid *:* so we don't get low level deleteAll optimization
    assertU(commit());
    
    doTestEmptyIndex();
    
    clearIndex();
    assertU(commit());

    doTestEmptyIndex();
  }
  
  /** @see #testEmptyIndex */
  private void doTestEmptyIndex() throws Exception {
    for (String opt : Arrays.asList(// no block collapse logic used (sanity checks)
                                    "field=block_s1",            
                                    "field=block_i",             
                                    // block collapse used implicitly (ord)
                                    "field=_root_",
                                    "field=_root_ hint=top_fc",             // top_fc hint shouldn't matter
                                    // block collapse used explicitly (ord)
                                    "field=_root_ hint=block",
                                    "field=block_s1 hint=block",
                                    // block collapse used explicitly (int)
                                    "field=block_i  hint=block"  
                                    )) {
      for (String nullPolicy : Arrays.asList("", // ignore is default
                                             " nullPolicy=ignore",
                                             " nullPolicy=expand")) {

        for (String suffix : SELECTOR_FIELD_SUFFIXES) {
          for (String headSelector : Arrays.asList("", // score is default
                                                   " max=asc" + suffix,
                                                   " min=desc" + suffix,
                                                   " sort='asc" + suffix + " desc'",
                                                   " sort='desc" +suffix + " asc'",
                                                   " max=sum(42,asc" + suffix + ")",
                                                   " min=sum(42,desc" + suffix + ")",
                                                   " max=sub(0,desc" + suffix + ")",
                                                   " min=sub(0,asc" + suffix + ")")) {
            
            if (headSelector.endsWith("_l") && opt.endsWith("_i")) {
              // NOTE: this limitation doesn't apply to block collapse on int,
              // so we only check 'opt.endsWith' (if ends with block hint we're ok)
              assertQEx("expected known limitation of using long for min/max selector when doing numeric collapse",
                        "min/max must be Int or Float",
                        req("q", "*:*",
                            "fq", "{!collapse " + opt + nullPolicy + headSelector + "}"),
                        SolrException.ErrorCode.BAD_REQUEST);
              continue;
            }

            assertQ(req("q", "*:*",
                        "fq", "{!collapse " + opt + nullPolicy + headSelector + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=0]"
                    );
          }
        }
      }
    }
  }
  
  
  public void testSimple() throws Exception {
    
    { // convert our docs to update commands, along with some commits, in a shuffled order and process all of them...
      final List<String> updates = Stream.concat(Stream.of(commit()),
                                                 makeBlockDocs().stream().map(doc -> adoc(doc))).collect(Collectors.toList());
      Collections.shuffle(updates, random());
      for (String u : updates) {
        assertU(u);
      }
      assertU(commit());
    }
    
    for (String opt : Arrays.asList(// no block collapse logic used (sanity checks)
                                    "field=block_s1",            
                                    "field=block_i",             
                                    // block collapse used implicitly (ord)
                                    "field=_root_",
                                    "field=_root_ hint=top_fc",             // top_fc hint shouldn't matter
                                    // block collapse used explicitly (ord)
                                    "field=_root_ hint=block",
                                    "field=block_s1 hint=block",
                                    // block collapse used explicitly (int)
                                    "field=block_i  hint=block"  
                                    )) {
      
      { // score based group head selection (default)
      
        // these permutations should all give the same results, since the queries don't match any docs in 'null' groups
        // (because we don't have any in our index)...
        for (String nullPolicy : Arrays.asList("", // ignore is default
                                               " nullPolicy=ignore",
                                               " nullPolicy=expand")) { 
          for (String q : Arrays.asList("txt_t:XX",             // only child docs with XX match
                                        "txt_t:* txt_t:XX",     // all child docs match
                                        "*:* txt_t:XX")) {      // all docs match

            // single score based collapse...
            assertQ(req("q", q,
                        "fq", "{!collapse " + opt + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1]/str[@name='id'][.='p2s4']"
                    , "//result/doc[2]/str[@name='id'][.='p3s1']"
                    , "//result/doc[3]/str[@name='id'][.='p1s3']"
                    );

            // same query, but boosting a diff p1 sku to change group head (and result order)
            assertQ(req("q", q,
                        "qt", "/elevate",
                        "elevateIds", "p1s1",
                        "fq", "{!collapse " + opt + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1]/str[@name='id'][.='p1s1']"
                    , "//result/doc[2]/str[@name='id'][.='p2s4']"
                    , "//result/doc[3]/str[@name='id'][.='p3s1']"
                    );
            
            // same query, but boosting multiple skus from p1
            assertQ(req("q", q,
                        "qt", "/elevate",
                        "elevateIds", "p1s1,p1s2",
                        "fq", "{!collapse " + opt + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=4]"
                    , "//result/doc[1]/str[@name='id'][.='p1s1']"
                    , "//result/doc[2]/str[@name='id'][.='p1s2']"
                    , "//result/doc[3]/str[@name='id'][.='p2s4']"
                    , "//result/doc[4]/str[@name='id'][.='p3s1']"
                    );
          }

          { // use func query to assert expected scores
            assertQ(req("q", "{!func}sum(42, num_i)",
                        "fq", "{!collapse " + opt + nullPolicy + "}",
                        "fl","score,id",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                    , "//result/doc[2][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                    , "//result/doc[3][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                    );
            // same query, but boosting a diff child to change group head (and result order)
            assertQ(req("q", "{!func}sum(42, num_i)",
                        "qt", "/elevate",
                        "elevateIds", "p1s1",
                        "fq", "{!collapse " + opt + nullPolicy + "}",
                        "fl","score,id",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1][str[@name='id'][.='p1s1'] and float[@name='score'][.=84.0]]"
                    , "//result/doc[2][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                    , "//result/doc[3][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                    );
            // same query, but boosting multiple skus from p1
            assertQ(req("q", "{!func}sum(42, num_i)",
                        "qt", "/elevate",
                        "elevateIds", "p1s2,p1s1",
                        "fq", "{!collapse " + opt + nullPolicy + "}",
                        "fl","score,id",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=4]"
                    , "//result/doc[1][str[@name='id'][.='p1s2'] and float[@name='score'][.=52.0]]"
                    , "//result/doc[2][str[@name='id'][.='p1s1'] and float[@name='score'][.=84.0]]"
                    , "//result/doc[3][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                    , "//result/doc[4][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                    );
          }
        }
        
      } // score 


      // sort and min/max  based group head selection
      for (String suffix : SELECTOR_FIELD_SUFFIXES) {

        // these permutations should all give the same results, since the queries don't match any docs in 'null' groups
        // (because we don't have any in our index)...
        for (String nullPolicy : Arrays.asList("", // ignore is default
                                               " nullPolicy=ignore",
                                               " nullPolicy=expand")) {
          
          // queries that are relevancy based...
          for (String selector : Arrays.asList(" sort='asc" + suffix + " asc'",
                                               " sort='sum(asc" + suffix + ",42) asc'",
                                               " max=desc" + suffix,
                                               " min=asc" + suffix,
                                               " min='sum(asc" + suffix + ", 42)'")) {
            
            if (selector.endsWith("_l") && opt.endsWith("_i")) {
              // NOTE: this limitation doesn't apply to block collapse on int,
              // so we only check 'opt.endsWith' (if ends with block hint we're ok)
              assertQEx("expected known limitation of using long for min/max selector when doing numeric collapse",
                        "min/max must be Int or Float",
                        req("q", "*:*",
                            "fq", "{!collapse " + opt + nullPolicy + selector + "}"),
                        SolrException.ErrorCode.BAD_REQUEST);
              continue;
            }

            assertQ(req("q","txt_t:XX",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1]/str[@name='id'][.='p2s4']"
                    , "//result/doc[2]/str[@name='id'][.='p3s4']"
                    , "//result/doc[3]/str[@name='id'][.='p1s4']"
                    );
            assertQ(req("q","txt_t:* txt_t:XX",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1]/str[@name='id'][.='p3s4']"
                    , "//result/doc[2]/str[@name='id'][.='p1s4']"
                    , "//result/doc[3]/str[@name='id'][.='p2s2']"
                    );
            // same query, but boosting skus to change group head (and result order)
            assertQ(req("q","txt_t:* txt_t:XX",
                        "qt", "/elevate",
                        "elevateIds", "p2s3,p1s1",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1]/str[@name='id'][.='p2s3']"
                    , "//result/doc[2]/str[@name='id'][.='p1s1']"
                    , "//result/doc[3]/str[@name='id'][.='p3s4']"
                    );
            // same query, but boosting multiple skus from p1
            assertQ(req("q","txt_t:* txt_t:XX",
                        "qt", "/elevate",
                        "elevateIds", "p2s3,p1s4,p1s3",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "sort", "score desc, num_i asc")
                    , "*[count(//doc)=4]"
                    , "//result/doc[1]/str[@name='id'][.='p2s3']"
                    , "//result/doc[2]/str[@name='id'][.='p1s4']"
                    , "//result/doc[3]/str[@name='id'][.='p1s3']"
                    , "//result/doc[4]/str[@name='id'][.='p3s4']"
                    );

            
          }
          
          // query use {!func} so we can assert expected scores
          for (String selector : Arrays.asList(" sort='asc" + suffix + " desc'",
                                               " sort='sum(asc" + suffix + ",42) desc'",
                                               " min=desc" + suffix,
                                               " max=asc" + suffix,
                                               " min='sum(desc" + suffix + ", 42)'",
                                               " max='sum(asc" + suffix + ", 42)'")) {
            
            if (selector.endsWith("_l") && opt.endsWith("_i")) {
              // NOTE: this limitation doesn't apply to block collapse on int,
              // so we only check 'opt.endsWith' (if ends with block hint we're ok)
              assertQEx("expected known limitation of using long for min/max selector when doing numeric collapse",
                        "min/max must be Int or Float",
                        req("q", "*:*",
                            "fq", "{!collapse " + opt + nullPolicy + selector + "}"),
                        SolrException.ErrorCode.BAD_REQUEST);
              continue;
            }
            
            assertQ(req("q", "{!func}sum(42, num_i)",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "fl","score,id",
                        "sort", "num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                    , "//result/doc[2][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                    , "//result/doc[3][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                    );
            // same query, but boosting multiple skus from p1
            assertQ(req("q", "{!func}sum(42, num_i)",
                        "qt", "/elevate",
                        "elevateIds", "p1s2,p1s1",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "fl","score,id",
                        "sort", "num_i asc")
                    , "*[count(//doc)=4]"
                    , "//result/doc[1][str[@name='id'][.='p1s2'] and float[@name='score'][.=52.0]]"
                    , "//result/doc[2][str[@name='id'][.='p1s1'] and float[@name='score'][.=84.0]]"
                    , "//result/doc[3][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                    , "//result/doc[4][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                    );

            
          }
          
          // queries are relevancy based, and score is used in collapse local param sort -- but not in top fl/sort
          // (ie: help prove we setup 'needScores' correctly for collapse, even though top level query doesn't care)
          for (String selector : Arrays.asList("", // implicit score ranking as sanity check
                                               " sort='score desc'",
                                               // unused tie breaker after score
                                               " sort='score desc, sum(num_i,42) desc'",
                                               // force score to be a tie breaker
                                               " sort='sum(1.5,2.5) asc, score desc'")) {
            assertQ(req("q", "*:* txt_t:XX",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "fl", "id",
                        "sort", "num_i asc")
                    , "*[count(//doc)=3]"
                    , "//result/doc[1][str[@name='id'][.='p2s4']]" // 13
                    , "//result/doc[2][str[@name='id'][.='p3s1']]" // 15
                    , "//result/doc[3][str[@name='id'][.='p1s3']]" // 777
                    );
            // same query, but boosting multiple skus from p3
            // NOTE: this causes each boosted doc to be returned, but top level sort is not score, so QEC doesn't hijak order
            assertQ(req("q", "*:* txt_t:XX",
                        "qt", "/elevate",
                        "elevateIds", "p3s3,p3s2",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "fl", "id",
                        "sort", "num_i asc")
                    , "*[count(//doc)=4]"
                    , "//result/doc[1][str[@name='id'][.='p2s4']]" // 13
                    , "//result/doc[2][str[@name='id'][.='p3s2']]" // 100 (boosted so treated as own group)
                    , "//result/doc[3][str[@name='id'][.='p1s3']]" // 777
                    , "//result/doc[4][str[@name='id'][.='p3s3']]" // 1234 (boosted so treated as own group)
                    );
            // same query, w/forceElevation to change top level order
            assertQ(req("q", "*:* txt_t:XX",
                        "qt", "/elevate",
                        "elevateIds", "p3s3,p3s2",
                        "forceElevation", "true",
                        "fq", "{!collapse " + opt + selector + nullPolicy + "}",
                        "fl", "id",
                        "sort", "num_i asc")
                    , "*[count(//doc)=4]"
                    , "//result/doc[1][str[@name='id'][.='p3s3']]" // 1234 (boosted so treated as own group)
                    , "//result/doc[2][str[@name='id'][.='p3s2']]" // 100 (boosted so treated as own group)
                    , "//result/doc[3][str[@name='id'][.='p2s4']]" // 13
                    , "//result/doc[4][str[@name='id'][.='p1s3']]" // 777
                    );

            
          }
        }
      }
    } // sort
  }

  public void testNullPolicyExpand() throws Exception {
    
    { // convert our docs + some docs w/o collapse fields, along with some commits, to update commands
      // in a shuffled order and process all of them...
      final List<String> updates = Stream.concat(Stream.of(commit(), commit()),
                                                 Stream.concat(makeBlockDocs().stream(),
                                                               sdocs(dupFields(sdoc("id","z1",   "num_i", 1)),
                                                                     dupFields(sdoc("id","z2",   "num_i", 2)),
                                                                     dupFields(sdoc("id","z3",   "num_i", 3)),
                                                                     dupFields(sdoc("id","z100", "num_i", 100))).stream()
                                                               ).map(doc -> adoc(doc))).collect(Collectors.toList());
      Collections.shuffle(updates, random());
      for (String u : updates) {
        assertU(u);
      }
      assertU(commit());
    }
    
    // NOTE: we don't try to collapse on '_root_' in this test, because then we'll get different results
    // compared to our other collapse fields (because every doc has a _root_ field)
    for (String opt : Arrays.asList(// no block collapse logic used (sanity checks)
                                    "field=block_s1",            
                                    "field=block_i",             
                                    // block collapse used explicitly (ord)
                                    "field=block_s1 hint=block",
                                    // block collapse used explicitly (int)
                                    "field=block_i  hint=block"  
                                    )) {
      
      { // score based group head selection (default)
        assertQ(req("q", "*:* txt_t:XX",
                    "fq", "{!collapse " + opt + " nullPolicy=expand}",
                    "sort", "score desc, num_i asc")
                , "*[count(//doc)=7]"
                , "//result/doc[1]/str[@name='id'][.='p2s4']"
                , "//result/doc[2]/str[@name='id'][.='p3s1']"
                , "//result/doc[3]/str[@name='id'][.='p1s3']"
                , "//result/doc[4]/str[@name='id'][.='z1']"
                , "//result/doc[5]/str[@name='id'][.='z2']"
                , "//result/doc[6]/str[@name='id'][.='z3']"
                , "//result/doc[7]/str[@name='id'][.='z100']"
                );
        // same query, but boosting docs to change group heads (and result order)
        assertQ(req("q", "*:* txt_t:XX",
                    "qt", "/elevate",
                    "elevateIds", "z2,p3s3",
                    "fq", "{!collapse " + opt + " nullPolicy=expand}",
                    "sort", "score desc, num_i asc")
                , "*[count(//doc)=7]"
                , "//result/doc[1]/str[@name='id'][.='z2']"
                , "//result/doc[2]/str[@name='id'][.='p3s3']"
                , "//result/doc[3]/str[@name='id'][.='p2s4']"
                , "//result/doc[4]/str[@name='id'][.='p1s3']"
                , "//result/doc[5]/str[@name='id'][.='z1']"
                , "//result/doc[6]/str[@name='id'][.='z3']"
                , "//result/doc[7]/str[@name='id'][.='z100']"
                );

        // use func query to assert expected scores
        assertQ(req("q", "{!func}sum(42, num_i)",
                    "fq", "{!collapse " + opt + " nullPolicy=expand}",
                    "fl","score,id",
                    "sort", "score desc, num_i asc")
                , "*[count(//doc)=7]"
                , "//result/doc[1][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                , "//result/doc[2][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                , "//result/doc[3][str[@name='id'][.='z100'] and float[@name='score'][.=142.0]]"
                , "//result/doc[4][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                , "//result/doc[5][str[@name='id'][.='z3']   and float[@name='score'][.=45.0]]"
                , "//result/doc[6][str[@name='id'][.='z2']   and float[@name='score'][.=44.0]]"
                , "//result/doc[7][str[@name='id'][.='z1']   and float[@name='score'][.=43.0]]"
                );
        // same query, but boosting docs to change group heads (and result order)
        assertQ(req("q", "{!func}sum(42, num_i)",
                    "qt", "/elevate",
                    "elevateIds", "p2s4,z2,p2s1",
                    "fq", "{!collapse " + opt + " nullPolicy=expand}",
                    "fl","score,id",
                    "sort", "score desc, num_i asc")
                , "*[count(//doc)=8]"
                , "//result/doc[1][str[@name='id'][.='p2s4'] and float[@name='score'][.=55.0]]"
                , "//result/doc[2][str[@name='id'][.='z2']   and float[@name='score'][.=44.0]]"
                , "//result/doc[3][str[@name='id'][.='p2s1'] and float[@name='score'][.=97.0]]"
                , "//result/doc[4][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                , "//result/doc[5][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                , "//result/doc[6][str[@name='id'][.='z100'] and float[@name='score'][.=142.0]]"
                , "//result/doc[7][str[@name='id'][.='z3']   and float[@name='score'][.=45.0]]"
                , "//result/doc[8][str[@name='id'][.='z1']   and float[@name='score'][.=43.0]]"
                );
        
      } // score 
      
      // sort and min/max based group head selection
      for (String suffix : SELECTOR_FIELD_SUFFIXES) {
        
        // queries that are relevancy based...
        for (String selector : Arrays.asList(" sort='asc" + suffix + " asc'",
                                             " sort='sum(asc" + suffix + ",42) asc'",
                                             " min=asc" + suffix,
                                             " max=desc" + suffix,
                                             " min='sum(asc" + suffix + ", 42)'",
                                             " max='sum(desc" + suffix + ", 42)'")) {
          
          if (selector.endsWith("_l") && opt.endsWith("_i")) {
            // NOTE: this limitation doesn't apply to block collapse on int,
            // so we only check 'opt.endsWith' (if ends with block hint we're ok)
            assertQEx("expected known limitation of using long for min/max selector when doing numeric collapse",
                      "min/max must be Int or Float",
                      req("q", "*:*",
                          "fq", "{!collapse " + opt + selector + " nullPolicy=expand}"),
                      SolrException.ErrorCode.BAD_REQUEST);
            continue;
          }
          
          assertQ(req("q","num_i:* txt_t:* txt_t:XX",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "sort", "score desc, num_i asc")
                  , "*[count(//doc)=7]"
                  , "//result/doc[1]/str[@name='id'][.='p3s4']"
                  , "//result/doc[2]/str[@name='id'][.='p1s4']"
                  , "//result/doc[3]/str[@name='id'][.='p2s2']"
                  , "//result/doc[4]/str[@name='id'][.='z1']"
                  , "//result/doc[5]/str[@name='id'][.='z2']"
                  , "//result/doc[6]/str[@name='id'][.='z3']"
                  , "//result/doc[7]/str[@name='id'][.='z100']"
                  );
          assertQ(req("q","num_i:* txt_t:XX",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "sort", "num_i asc")
                  , "*[count(//doc)=7]"
                  , "//result/doc[1]/str[@name='id'][.='z1']"
                  , "//result/doc[2]/str[@name='id'][.='z2']"
                  , "//result/doc[3]/str[@name='id'][.='z3']"
                  , "//result/doc[4]/str[@name='id'][.='p3s4']"
                  , "//result/doc[5]/str[@name='id'][.='p1s4']"
                  , "//result/doc[6]/str[@name='id'][.='p2s2']"
                  , "//result/doc[7]/str[@name='id'][.='z100']"
                  );
          // same query, but boosting multiple docs
          // NOTE: this causes each boosted doc to be returned, but top level sort is not score, so QEC doesn't hijak order
          assertQ(req("q","num_i:* txt_t:XX",
                      "qt", "/elevate",
                      "elevateIds", "p3s3,z3,p3s1",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "sort", "num_i asc")
                  , "*[count(//doc)=8]"
                  , "//result/doc[1]/str[@name='id'][.='z1']"
                  , "//result/doc[2]/str[@name='id'][.='z2']"
                  , "//result/doc[3]/str[@name='id'][.='z3']"
                  , "//result/doc[4]/str[@name='id'][.='p1s4']"
                  , "//result/doc[5]/str[@name='id'][.='p2s2']"
                  , "//result/doc[6]/str[@name='id'][.='p3s1']"
                  , "//result/doc[7]/str[@name='id'][.='z100']"
                  , "//result/doc[8]/str[@name='id'][.='p3s3']"
                  );
          // same query, w/forceElevation to change top level order
          assertQ(req("q","num_i:* txt_t:XX",
                      "qt", "/elevate",
                      "elevateIds", "p3s3,z3,p3s1",
                      "forceElevation", "true",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "sort", "num_i asc")
                  , "*[count(//doc)=8]"
                  , "//result/doc[1]/str[@name='id'][.='p3s3']"
                  , "//result/doc[2]/str[@name='id'][.='z3']"
                  , "//result/doc[3]/str[@name='id'][.='p3s1']"
                  , "//result/doc[4]/str[@name='id'][.='z1']"
                  , "//result/doc[5]/str[@name='id'][.='z2']"
                  , "//result/doc[6]/str[@name='id'][.='p1s4']"
                  , "//result/doc[7]/str[@name='id'][.='p2s2']"
                  , "//result/doc[8]/str[@name='id'][.='z100']"
                  );

        }

        // query uses {!func} so we can assert expected scores
        for (String selector : Arrays.asList(" sort='asc" + suffix + " desc'",
                                             " sort='sum(asc" + suffix + ",42) desc'",
                                             " min=desc" + suffix,
                                             " max=asc" + suffix,
                                             " min='sum(desc" + suffix + ", 42)'",
                                             " max='sum(asc" + suffix + ", 42)'")) {

          if (selector.endsWith("_l") && opt.endsWith("_i")) {
            // NOTE: this limitation doesn't apply to block collapse on int,
            // so we only check 'opt.endsWith' (if ends with block hint we're ok)
            assertQEx("expected known limitation of using long for min/max selector when doing numeric collapse",
                      "min/max must be Int or Float",
                      req("q", "*:*",
                          "fq", "{!collapse " + opt + selector + " nullPolicy=expand}"),
                      SolrException.ErrorCode.BAD_REQUEST);
            continue;
          }
          
          assertQ(req("q", "{!func}sum(42, num_i)",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "fl","score,id",
                      "sort", "num_i asc")
                  , "*[count(//doc)=7]"
                  , "//result/doc[1][str[@name='id'][.='z1']   and float[@name='score'][.=43.0]]"
                  , "//result/doc[2][str[@name='id'][.='z2']   and float[@name='score'][.=44.0]]"
                  , "//result/doc[3][str[@name='id'][.='z3']   and float[@name='score'][.=45.0]]"
                  , "//result/doc[4][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                  , "//result/doc[5][str[@name='id'][.='z100'] and float[@name='score'][.=142.0]]"
                  , "//result/doc[6][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                  , "//result/doc[7][str[@name='id'][.='p3s3'] and float[@name='score'][.=1276.0]]"
                  );
          // same query, but boosting multiple docs
          // NOTE: this causes each boosted doc to be returned, but top level sort is not score, so QEC doesn't hijak order
          assertQ(req("q", "{!func}sum(42, num_i)",
                      "qt", "/elevate",
                      "elevateIds", "p3s1,z3,p3s4",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "fl","score,id",
                      "sort", "num_i asc")
                  , "*[count(//doc)=8]"
                  , "//result/doc[1][str[@name='id'][.='z1']   and float[@name='score'][.=43.0]]"
                  , "//result/doc[2][str[@name='id'][.='z2']   and float[@name='score'][.=44.0]]"
                  , "//result/doc[3][str[@name='id'][.='z3']   and float[@name='score'][.=45.0]]"
                  , "//result/doc[4][str[@name='id'][.='p3s4'] and float[@name='score'][.=46.0]]"
                  , "//result/doc[5][str[@name='id'][.='p3s1'] and float[@name='score'][.=57.0]]"
                  , "//result/doc[6][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                  , "//result/doc[7][str[@name='id'][.='z100'] and float[@name='score'][.=142.0]]"
                  , "//result/doc[8][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                  );
          // same query, w/forceElevation to change top level order
          assertQ(req("q", "{!func}sum(42, num_i)",
                      "qt", "/elevate",
                      "elevateIds", "p3s1,z3,p3s4",
                      "forceElevation", "true",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "fl","score,id",
                      "sort", "num_i asc")
                  , "*[count(//doc)=8]"
                  , "//result/doc[1][str[@name='id'][.='p3s1'] and float[@name='score'][.=57.0]]"
                  , "//result/doc[2][str[@name='id'][.='z3']   and float[@name='score'][.=45.0]]"
                  , "//result/doc[3][str[@name='id'][.='p3s4'] and float[@name='score'][.=46.0]]"
                  , "//result/doc[4][str[@name='id'][.='z1']   and float[@name='score'][.=43.0]]"
                  , "//result/doc[5][str[@name='id'][.='z2']   and float[@name='score'][.=44.0]]"
                  , "//result/doc[6][str[@name='id'][.='p2s3'] and float[@name='score'][.=141.0]]"
                  , "//result/doc[7][str[@name='id'][.='z100'] and float[@name='score'][.=142.0]]"
                  , "//result/doc[8][str[@name='id'][.='p1s3'] and float[@name='score'][.=819.0]]"
                  );
          
        }
        
        // queries are relevancy based, and score is used in collapse local param sort -- but not in top fl/sort
        // (ie: help prove we setup 'needScores' correctly for collapse, even though top level query doesn't care)
        for (String selector : Arrays.asList("", // implicit score ranking as sanity check
                                             " sort='score desc'",
                                             // unused tie breaker after score
                                             " sort='score desc, sum(num_i,42) desc'",
                                             // force score to be a tie breaker
                                             " sort='sum(1.5,2.5) asc, score desc'")) {
          
          assertQ(req("q", "*:* txt_t:XX",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "fl", "id",
                      "sort", "num_i asc")
                  , "*[count(//doc)=7]"
                  , "//result/doc[1][str[@name='id'][.='z1']]"
                  , "//result/doc[2][str[@name='id'][.='z2']]"
                  , "//result/doc[3][str[@name='id'][.='z3']]"
                  , "//result/doc[4][str[@name='id'][.='p2s4']]" // 13
                  , "//result/doc[5][str[@name='id'][.='p3s1']]" // 15
                  , "//result/doc[6][str[@name='id'][.='z100']]"
                  , "//result/doc[7][str[@name='id'][.='p1s3']]" // 777
                  );
          // same query, but boosting multiple docs
          // NOTE: this causes each boosted doc to be returned, but top level sort is not score, so QEC doesn't hijak order
          assertQ(req("q", "*:* txt_t:XX",
                      "qt", "/elevate",
                      "elevateIds", "p3s3,z3,p3s4",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "fl", "id",
                      "sort", "num_i asc")
                  , "*[count(//doc)=8]"
                  , "//result/doc[1][str[@name='id'][.='z1']]"
                  , "//result/doc[2][str[@name='id'][.='z2']]"
                  , "//result/doc[3][str[@name='id'][.='z3']]"
                  , "//result/doc[4][str[@name='id'][.='p3s4']]" // 4
                  , "//result/doc[5][str[@name='id'][.='p2s4']]" // 13
                  , "//result/doc[6][str[@name='id'][.='z100']]"
                  , "//result/doc[7][str[@name='id'][.='p1s3']]" // 777
                  , "//result/doc[8][str[@name='id'][.='p3s3']]" // 1234
                  );
          // same query, w/forceElevation to change top level order
          assertQ(req("q", "*:* txt_t:XX",
                      "qt", "/elevate",
                      "elevateIds", "p3s3,z3,p3s4",
                      "forceElevation", "true",
                      "fq", "{!collapse " + opt + selector + " nullPolicy=expand}",
                      "fl", "id",
                      "sort", "num_i asc")
                  , "*[count(//doc)=8]"
                  , "//result/doc[1][str[@name='id'][.='p3s3']]" // 1234
                  , "//result/doc[2][str[@name='id'][.='z3']]"
                  , "//result/doc[3][str[@name='id'][.='p3s4']]" // 4
                  , "//result/doc[4][str[@name='id'][.='z1']]"
                  , "//result/doc[5][str[@name='id'][.='z2']]"
                  , "//result/doc[6][str[@name='id'][.='p2s4']]" // 13
                  , "//result/doc[7][str[@name='id'][.='z100']]"
                  , "//result/doc[8][str[@name='id'][.='p1s3']]" // 777
                  );


        }
        
      } // sort
    }
  }

  /**
   * There is no reason why ExpandComponent should care if/when block collapse is used,
   * this test just serves as a "future proofing" against the possibility that someone adds new expectations
   * to ExpandComponent of some side effect state that CollapseQParser should produce.
   *
   * We don't bother testing _root_ field collapsing in this test, since it contains different field values 
   * then our other collapse fields.
   * (and the other tests should adequeately prove that the block hueristics for _root_ collapsing work)
   */
  public void testBlockCollapseWithExpandComponent() throws Exception {

    { // convert our docs + some docs w/o collapse fields, along with some commits, to update commands
      // in a shuffled order and process all of them...
      final List<String> updates = Stream.concat(Stream.of(commit(), commit()),
                                                 Stream.concat(makeBlockDocs().stream(),
                                                               sdocs(dupFields(sdoc("id","z1", "num_i", 1)),
                                                                     dupFields(sdoc("id","z2", "num_i", 2)),
                                                                     dupFields(sdoc("id","z3", "num_i", 3))).stream()
                                                               ).map(doc -> adoc(doc))).collect(Collectors.toList());
      Collections.shuffle(updates, random());
      for (String u : updates) {
        assertU(u);
      }
      assertU(commit());
    }

    final String EX = "/response/lst[@name='expanded']/result";
    // we don't bother testing _root_ field collapsing, since it contains different field values then block_s1
    for (String opt : Arrays.asList(// no block collapse logic used (sanity checks)
                                    "field=block_s1",            
                                    "field=block_i",

                                    // block collapse used explicitly (int)
                                    "field=block_i  hint=block",
                                    
                                    // block collapse used explicitly (ord)
                                    "field=block_s1 hint=block"
                                    )) {

      // these permutations should all give the same results, since the queries don't match any docs in 'null' groups
      for (String nullPolicy : Arrays.asList("", // ignore is default
                                             " nullPolicy=ignore",
                                             " nullPolicy=expand")) {
        
        // score based collapse with boost to change p1 group head
        assertQ(req("q", "txt_t:XX", // only child docs with XX match
                    "expand", "true",
                    "qt", "/elevate",
                    "elevateIds", "p1s1",
                    "fl", "id",
                    "fq", "{!collapse " + opt + nullPolicy + "}",
                    "sort", "score desc, num_i asc")
                , "*[count(/response/result/doc)=3]"
                , "/response/result/doc[1]/str[@name='id'][.='p1s1']"
                , "/response/result/doc[2]/str[@name='id'][.='p2s4']"
                , "/response/result/doc[3]/str[@name='id'][.='p3s1']"
                //
                ,"*[count("+EX+")=count(/response/result/doc)]" // group per doc
                //
                ,"*[count("+EX+"[@name='-1']/doc)=3]"
                ,EX+"[@name='-1']/doc[1]/str[@name='id'][.='p1s3']"
                ,EX+"[@name='-1']/doc[2]/str[@name='id'][.='p1s4']"
                ,EX+"[@name='-1']/doc[3]/str[@name='id'][.='p1s2']"
                //
                ,"*[count("+EX+"[@name='0']/doc)=2]"
                ,EX+"[@name='0']/doc[1]/str[@name='id'][.='p2s3']"
                ,EX+"[@name='0']/doc[2]/str[@name='id'][.='p2s1']"
                //
                ,"*[count("+EX+"[@name='1']/doc)=2]"
                ,EX+"[@name='1']/doc[1]/str[@name='id'][.='p3s4']"
                ,EX+"[@name='1']/doc[2]/str[@name='id'][.='p3s3']"
                );
      }

      // nullPolicy=expand w/ func query to assert expected scores
      for (String suffix : SELECTOR_FIELD_SUFFIXES) {
        for (String selector : Arrays.asList(" sort='asc" + suffix + " desc'",
                                             " sort='sum(asc" + suffix + ",42) desc'",
                                             " min=desc" + suffix,
                                             " max=asc" + suffix,
                                             " min='sum(desc" + suffix + ", 42)'",
                                             " max='sum(asc" + suffix + ", 42)'")) {
          assertQ(req("q", "{!func}sum(42, num_i)",
                      "expand", "true",
                      "fq", "{!collapse " + opt + " nullPolicy=expand}",
                      "fq", "num_i:[2 TO 13]",                                   // NOTE: FQ!!!!
                      "fl","score,id",
                      "sort", "score desc, num_i asc")
                  , "*[count(/response/result/doc)=5]"
                  , "/response/result/doc[1][str[@name='id'][.='p2s4'] and float[@name='score'][.=55.0]]"
                  , "/response/result/doc[2][str[@name='id'][.='p1s2'] and float[@name='score'][.=52.0]]"
                  , "/response/result/doc[3][str[@name='id'][.='p3s4'] and float[@name='score'][.=46.0]]"
                  , "/response/result/doc[4][str[@name='id'][.='z3']   and float[@name='score'][.=45.0]]"
                  , "/response/result/doc[5][str[@name='id'][.='z2']   and float[@name='score'][.=44.0]]"
                  //
                  ,"*[count("+EX+")=2]" // groups w/o any other docs don't expand
                  //
                  ,"*[count("+EX+"[@name='-1']/doc)=1]"
                  ,EX+"[@name='-1']/doc[1][str[@name='id'][.='p1s4'] and float[@name='score'][.=48.0]]"
                  //
                  ,"*[count("+EX+"[@name='0']/doc)=1]"
                  ,EX+"[@name='0']/doc[1][str[@name='id'][.='p2s2'] and float[@name='score'][.=52.0]]"
                  //
                  // no "expand" docs for group '1' because no other docs match query
                  // no "expand" docs for nulls unless/until SOLR-14330 is implemented
                  );
        }
      }
    }
  }

  /**
   * returns a (new) list of the block based documents used in our test methods
   */
  protected static final List<SolrInputDocument> makeBlockDocs() {
    // NOTE: block_i and block_s1 will contain identical content so these need to be "numbers"...
    // The specific numbers shouldn't matter (and we explicitly test '0' to confirm legacy bug/behavior
    // of treating 0 as null is no longer a problem) ...
    final String A = "-1";
    final String B = "0"; 
    final String C = "1";

    return sdocs(dupFields(sdoc("id", "p1",
                                "block_i", A, 
                                "skus", sdocs(dupFields(sdoc("id", "p1s1", "block_i", A, "txt_t", "a  b  c  d  e ", "num_i", 42)),
                                              dupFields(sdoc("id", "p1s2", "block_i", A, "txt_t", "a  XX c  d  e ", "num_i", 10)),
                                              dupFields(sdoc("id", "p1s3", "block_i", A, "txt_t", "XX b  XX XX e ", "num_i", 777)),
                                              dupFields(sdoc("id", "p1s4", "block_i", A, "txt_t", "a  XX c  d  XX", "num_i", 6))
                                              ))),
                 dupFields(sdoc("id", "p2",
                                "block_i", B, 
                                "skus", sdocs(dupFields(sdoc("id", "p2s1", "block_i", B, "txt_t", "a  XX c  d  e ", "num_i", 55)),
                                              dupFields(sdoc("id", "p2s2", "block_i", B, "txt_t", "a  b  c  d  e ", "num_i", 10)),
                                              dupFields(sdoc("id", "p2s3", "block_i", B, "txt_t", "XX b  c  XX e ", "num_i", 99)),
                                              dupFields(sdoc("id", "p2s4", "block_i", B, "txt_t", "a  XX XX d  XX", "num_i", 13))
                                              ))),
                 dupFields(sdoc("id", "p3",
                                "block_i", C,
                                "skus", sdocs(dupFields(sdoc("id", "p3s1", "block_i", C, "txt_t", "a  XX XX XX e ", "num_i", 15)),
                                              dupFields(sdoc("id", "p3s2", "block_i", C, "txt_t", "a  b  c  d  e ", "num_i", 100)),
                                              dupFields(sdoc("id", "p3s3", "block_i", C, "txt_t", "XX b  c  d  e ", "num_i", 1234)),
                                              dupFields(sdoc("id", "p3s4", "block_i", C, "txt_t", "a  b  XX d  XX", "num_i", 4))
                                              ))));
  }
  protected final static List<String> SELECTOR_FIELD_SUFFIXES = Arrays.asList("_i", "_l", "_f");
  protected static SolrInputDocument dupFields(final SolrInputDocument doc) {
    if (doc.getFieldNames().contains("block_i")) {
      doc.setField("block_s1", doc.getFieldValue("block_i"));
    }
    // as num_i value increases, the asc_* fields increase
    // as num_i value increases, the desc_* fields decrease
    if (doc.getFieldNames().contains("num_i")) {
      final int val = ((Integer)doc.getFieldValue("num_i")).intValue();
      for (String suffix : SELECTOR_FIELD_SUFFIXES) {
        doc.setField("asc" + suffix, val);
        doc.setField("desc" + suffix, 0 - val);
      }
    }
    return doc;
  }
}
