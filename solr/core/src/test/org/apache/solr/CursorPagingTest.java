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

package org.apache.solr;

import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_PARAM;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_NEXT;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CursorMark; //jdoc

import org.noggit.ObjectBuilder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.After;

/**
 * Tests of deep paging using {@link CursorMark} and {@link #CURSOR_MARK_PARAM}.
 */
@SuppressCodecs("Lucene3x")
public class CursorPagingTest extends SolrTestCaseJ4 {

  /** solrconfig.xml file name, shared with other cursor related tests */
  public final static String TEST_SOLRCONFIG_NAME = "solrconfig-deeppaging.xml";
  /** schema.xml file name, shared with other cursor related tests */
  public final static String TEST_SCHEMAXML_NAME = "schema-sorts.xml";

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("solr.test.useFilterForSortedQuery", Boolean.toString(random().nextBoolean()));
    initCore(TEST_SOLRCONFIG_NAME, TEST_SCHEMAXML_NAME);
  }
  @After
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  /** verify expected error msgs from bad client behavior */
  public void testBadInputs() throws Exception {
    // sometimes seed some data, othertimes use an empty index
    if (random().nextBoolean()) {
      assertU(adoc("id", "42", "str", "z", "float", "99.99", "int", "42"));
      assertU(adoc("id", "66", "str", "x", "float", "22.00", "int", "-66"));
    } else {
      assertU(commit());
    }
      assertU(commit());

    // empty, blank, or bogus cursor
    for (String c : new String[] { "", "   ", "all the docs please!"}) {
      assertFail(params("q", "*:*", 
                        "sort", "id desc", 
                        CURSOR_MARK_PARAM, c),
                 ErrorCode.BAD_REQUEST, "Unable to parse");
    }

    // no id in sort
    assertFail(params("q", "*:*", 
                      "sort", "score desc", 
                      CURSOR_MARK_PARAM, CURSOR_MARK_START),
               ErrorCode.BAD_REQUEST, "uniqueKey field");
    // _docid_
    assertFail(params("q", "*:*", 
                      "sort", "_docid_ asc, id desc", 
                      CURSOR_MARK_PARAM, CURSOR_MARK_START),
               ErrorCode.BAD_REQUEST, "_docid_");

    // using cursor w/ timeAllowed
    assertFail(params("q", "*:*", 
                      "sort", "id desc", 
                      CommonParams.TIME_ALLOWED, "1000",
                      CURSOR_MARK_PARAM, CURSOR_MARK_START),
               ErrorCode.BAD_REQUEST, CommonParams.TIME_ALLOWED);

    // using cursor w/ grouping
    assertFail(params("q", "*:*", 
                      "sort", "id desc", 
                      GroupParams.GROUP, "true",
                      GroupParams.GROUP_FIELD, "str",
                      CURSOR_MARK_PARAM, CURSOR_MARK_START),
               ErrorCode.BAD_REQUEST, "Grouping");
  }


  /** simple static test of some carefully crafted docs */
  public void testSimple() throws Exception {
    String cursorMark;
    SolrParams params = null;
    
    final String intsort = "int" + (random().nextBoolean() ? "" : "_dv");
    final String intmissingsort = defaultCodecSupportsMissingDocValues() ? intsort : "int";

    // trivial base case: ensure cursorMark against an empty index doesn't blow up
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","4",
                    "fl", "id",
                    "sort", "id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==0"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              );
    assertEquals(CURSOR_MARK_START, cursorMark);


    // don't add in order of any field to ensure we aren't inadvertantly 
    // counting on internal docid ordering
    assertU(adoc("id", "9", "str", "c", "float", "-3.2", "int", "42"));
    assertU(adoc("id", "7", "str", "c", "float", "-3.2", "int", "-1976"));
    assertU(adoc("id", "2", "str", "c", "float", "-3.2", "int", "666"));
    assertU(adoc("id", "0", "str", "b", "float", "64.5", "int", "-42"));
    assertU(adoc("id", "5", "str", "b", "float", "64.5", "int", "2001"));
    assertU(adoc("id", "8", "str", "b", "float", "64.5", "int", "4055"));
    assertU(adoc("id", "6", "str", "a", "float", "64.5", "int", "7"));
    assertU(adoc("id", "1", "str", "a", "float", "64.5", "int", "7"));
    assertU(adoc("id", "4", "str", "a", "float", "11.1", "int", "6"));
    assertU(adoc("id", "3", "str", "a", "float", "11.1")); // int is missing
    assertU(commit());

    // base case: ensure cursorMark that matches no docs doesn't blow up
    cursorMark = CURSOR_MARK_START;
    params = params("q", "id:9999999", 
                    "rows","4",
                    "fl", "id",
                    "sort", "id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==0"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              );
    assertEquals(CURSOR_MARK_START, cursorMark);

    // edge case: ensure rows=0 doesn't blow up and gives back same cursor for next
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","0",
                    "fl", "id",
                    "sort", "id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              );
    assertEquals(CURSOR_MARK_START, cursorMark);

    // simple id sort w/some faceting
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:6", 
                    "rows","4",
                    "fl", "id",
                    "sort", "id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':9},{'id':8},{'id':7},{'id':6}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9" 
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':5},{'id':3},{'id':2},{'id':1}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9" 
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':0}]"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark, 
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));

    // simple score sort w/some faceting
    cursorMark = CURSOR_MARK_START;
    params = params("q", "float:[0 TO *] int:7 id:6", 
                    "rows","4",
                    "fl", "id",
                    "facet", "true",
                    "facet.field", "str",
                    "json.nl", "map",
                    "sort", "score desc, id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==7"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':6},{'id':1},{'id':8},{'id':5}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':3,'c':0}"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==7"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':4},{'id':3},{'id':0}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':3,'c':0}"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==7"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':3,'c':0}"
                              ));

    // int sort with dups, id tie breaker ... and some faceting
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:2001 -int:4055", 
                    "rows","3",
                    "fl", "id",
                    "facet", "true",
                    "facet.field", "str",
                    "json.nl", "map",
                    "sort", intsort + " asc, id asc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':7},{'id':0},{'id':3}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':1,'c':3}"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':4},{'id':1},{'id':6}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':1,'c':3}"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':9},{'id':2}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':1,'c':3}"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8" 
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':1,'c':3}"
                              ));

    // int missing first sort with dups, id tie breaker
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:2001 -int:4055", 
                    "rows","3",
                    "fl", "id",
                    "json.nl", "map",
                    "sort", intmissingsort + "_first asc, id asc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':3},{'id':7},{'id':0}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':4},{'id':1},{'id':6}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':9},{'id':2}]"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8" 
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));

    // int missing last sort with dups, id tie breaker
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:2001 -int:4055", 
                    "rows","3",
                    "fl", "id",
                    "json.nl", "map",
                    "sort", intmissingsort + "_last asc, id asc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':7},{'id':0},{'id':4}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':1},{'id':6},{'id':9}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':2},{'id':3}]"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8" 
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));

    // string sort with dups, id tie breaker
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","6",
                    "fl", "id",
                    "sort", "str asc, id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':6},{'id':4},{'id':3},{'id':1},{'id':8},{'id':5}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':0},{'id':9},{'id':7},{'id':2}]"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));

    // tri-level sort with more dups of primary then fit on a page
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","2",
                    "fl", "id",
                    "sort", "float asc, "+intsort+" desc, id desc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':2},{'id':9}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':7},{'id':4}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10" 
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':3},{'id':8}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':5},{'id':6}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':1},{'id':0}]"
                              );
    // we've exactly exhausted all the results, but solr had no way of know that
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));

    // trivial base case: rows bigger then number of matches
    cursorMark = CURSOR_MARK_START;
    params = params("q", "id:3 id:7", 
                    "rows","111",
                    "fl", "id",
                    "sort", intsort + " asc, id asc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==2"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':7},{'id':3}]"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==2"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));

    // sanity check our full walk method
    SentinelIntSet ids;
    ids = assertFullWalkNoDups(10, params("q", "*:*", 
                                          "rows", "4",
                                          "sort", "id desc"));
    assertEquals(10, ids.size());
    ids = assertFullWalkNoDups(9, params("q", "*:*", 
                                         "rows", "1",
                                         "fq", "-id:4",
                                         "sort", "id asc"));
    assertEquals(9, ids.size());
    assertFalse("matched on id:4 unexpectedly", ids.exists(4));
    ids = assertFullWalkNoDups(9, params("q", "*:*", 
                                         "rows", "3",
                                         "fq", "-id:6",
                                         "sort", "float desc, id asc, "+intsort+" asc"));
    assertEquals(9, ids.size());
    assertFalse("matched on id:6 unexpectedly", ids.exists(6));
    ids = assertFullWalkNoDups(9, params("q", "float:[0 TO *] int:7 id:6", 
                                         "rows", "3",
                                         "sort", "score desc, id desc"));
    assertEquals(7, ids.size());
    assertFalse("matched on id:9 unexpectedly", ids.exists(9));
    assertFalse("matched on id:7 unexpectedly", ids.exists(7));
    assertFalse("matched on id:2 unexpectedly", ids.exists(2));

    // strategically delete/add some docs in the middle of walking the cursor
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","2",
                    "fl", "id",
                    "sort", "str asc, id asc");
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':1},{'id':3}]"
                              );
    // delete the last guy we got
    assertU(delI("3")); 
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':4},{'id':6}]"
                              );
    // delete the next guy we expect
    assertU(delI("0")); 
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':5},{'id':8}]"
                              );
    // update a doc we've already seen so it repeats
    assertU(adoc("id", "5", "str", "c"));
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':2},{'id':5}]"
                              );
    // update the next doc we expect so it's now in the past
    assertU(adoc("id", "7", "str", "a"));
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':9}]"
                              );
    // no more, so no change to cursorMark, and no new docs
    assertEquals(cursorMark,
                 assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[]"
                              ));
  }

  /**
   * test that our assumptions about how caches are affected hold true
   */
  public void testCacheImpacts() throws Exception {
    // cursor queryies can't live in the queryResultCache, but independent filters
    // should still be cached & reused

    // don't add in order of any field to ensure we aren't inadvertantly 
    // counting on internal docid ordering
    assertU(adoc("id", "9", "str", "c", "float", "-3.2", "int", "42"));
    assertU(adoc("id", "7", "str", "c", "float", "-3.2", "int", "-1976"));
    assertU(adoc("id", "2", "str", "c", "float", "-3.2", "int", "666"));
    assertU(adoc("id", "0", "str", "b", "float", "64.5", "int", "-42"));
    assertU(adoc("id", "5", "str", "b", "float", "64.5", "int", "2001"));
    assertU(adoc("id", "8", "str", "b", "float", "64.5", "int", "4055"));
    assertU(adoc("id", "6", "str", "a", "float", "64.5", "int", "7"));
    assertU(adoc("id", "1", "str", "a", "float", "64.5", "int", "7"));
    assertU(adoc("id", "4", "str", "a", "float", "11.1", "int", "6"));
    assertU(adoc("id", "3", "str", "a", "float", "11.1", "int", "3"));
    assertU(commit());

    final Collection<String> allFieldNames = getAllSortFieldNames();

    final SolrInfoMBean filterCacheStats 
      = h.getCore().getInfoRegistry().get("filterCache");
    assertNotNull(filterCacheStats);
    final SolrInfoMBean queryCacheStats 
      = h.getCore().getInfoRegistry().get("queryResultCache");
    assertNotNull(queryCacheStats);

    final long preQcIn = (Long) queryCacheStats.getStatistics().get("inserts");
    final long preFcIn = (Long) filterCacheStats.getStatistics().get("inserts");
    final long preFcHits = (Long) filterCacheStats.getStatistics().get("hits");

    SentinelIntSet ids = assertFullWalkNoDups
      (10, params("q", "*:*",
                  "rows",""+_TestUtil.nextInt(random(),1,11),
                  "fq", "-id:[1 TO 2]",
                  "fq", "-id:[6 TO 7]",
                  "fl", "id",
                  "sort", buildRandomSort(allFieldNames)));
    
    assertEquals(6, ids.size());

    final long postQcIn = (Long) queryCacheStats.getStatistics().get("inserts");
    final long postFcIn = (Long) filterCacheStats.getStatistics().get("inserts");
    final long postFcHits = (Long) filterCacheStats.getStatistics().get("hits");
    
    assertEquals("query cache inserts changed", preQcIn, postQcIn);
    // NOTE: use of pure negative filters causees "*:* to be tracked in filterCache
    assertEquals("filter cache did not grow correctly", 3, postFcIn-preFcIn);
    assertTrue("filter cache did not have any new cache hits", 0 < postFcHits-preFcHits);

  }

  /** randomized testing of a non-trivial number of docs using assertFullWalkNoDups 
   */
  public void testRandomSortsOnLargeIndex() throws Exception {
    final Collection<String> allFieldNames = getAllSortFieldNames();

    final int initialDocs = _TestUtil.nextInt(random(),100,200);
    final int totalDocs = atLeast(5000);

    // start with a smallish number of documents, and test that we can do a full walk using a 
    // sort on *every* field in the schema...

    for (int i = 1; i <= initialDocs; i++) {
      SolrInputDocument doc = buildRandomDocument(i);
      assertU(adoc(doc));
    }
    assertU(commit());

    for (String f : allFieldNames) {
      for (String order : new String[] {" asc", " desc"}) {
        String sort = f + order + ("id".equals(f) ? "" : ", id" + order);
        String rows = "" + _TestUtil.nextInt(random(),13,50);
        SentinelIntSet ids = assertFullWalkNoDups(totalDocs, 
                                                  params("q", "*:*",
                                                         "fl","id",
                                                         "rows",rows,
                                                         "sort",sort));
        assertEquals(initialDocs, ids.size());
      }
    }

    // now add a lot more docs, and test a handful of randomized sorts
    for (int i = initialDocs+1; i <= totalDocs; i++) {
      SolrInputDocument doc = buildRandomDocument(i);
      assertU(adoc(doc));
    }
    assertU(commit());

    final int numRandomSorts = atLeast(5);
    for (int i = 0; i < numRandomSorts; i++) {
      final String sort = buildRandomSort(allFieldNames);
      final String rows = "" + _TestUtil.nextInt(random(),63,113);
      final String fl = random().nextBoolean() ? "id" : "id,score";
      final boolean matchAll = random().nextBoolean();
      final String q = matchAll ? "*:*" : buildRandomQuery();

      SentinelIntSet ids = assertFullWalkNoDups(totalDocs, 
                                                params("q", q,
                                                       "fl",fl,
                                                       "rows",rows,
                                                       "sort",sort));
      if (matchAll) {
        assertEquals(totalDocs, ids.size());
      }

    }
  }

  /** Similar to usually() but we want it to happen just as often regardless
   * of test multiplier and nightly status 
   */
  private static boolean useField() {
    return 0 != _TestUtil.nextInt(random(), 0, 30);
  }
  
  /** returns likely most (1/10) of the time, otherwise unlikely */
  private static Object skewed(Object likely, Object unlikely) {
    return (0 == _TestUtil.nextInt(random(), 0, 9)) ? unlikely : likely;
  }
  
  /**
   * An immutable list of the fields in the schema that can be used for sorting,
   * deterministically random order.
   */
  private List<String> getAllSortFieldNames() {
    return pruneAndDeterministicallySort
      (h.getCore().getLatestSchema().getFields().keySet());
  }

  
  /**
   * <p>
   * Given a list of field names in the schema, returns an immutable list in 
   * deterministically random order with the following things removed:
   * </p>
   * <ul>
   *  <li><code>_version_</code> is removed</li>
   *  <li><code>dv_last</code> and <code>dv_first</code> fields are removed 
   *      if the codec doesn't support them</li>
   * </ul>
   * @see #defaultCodecSupportsMissingDocValues
   */
  public static List<String> pruneAndDeterministicallySort(Collection<String> raw) {

    final boolean prune_dv_missing = ! defaultCodecSupportsMissingDocValues();

    ArrayList<String> names = new ArrayList<String>(37);
    for (String f : raw) {
      if (f.equals("_version_")) {
        continue;
      }
      if (prune_dv_missing && (f.endsWith("_dv_last") || f.endsWith("_dv_first")) ) {
        continue;
      }
      names.add(f);
    }

    Collections.sort(names);
    Collections.shuffle(names,random());
    return Collections.<String>unmodifiableList(names);
  }

  /**
   * Given a set of params, executes a cursor query using {@link #CURSOR_MARK_START}
   * and then continuously walks the results using {@link #CURSOR_MARK_START} as long
   * as a non-0 number of docs ar returned.  This method records the the set of all id's
   * (must be positive ints) encountered and throws an assertion failure if any id is
   * encountered more than once, or if the set grows above maxSize
   */
  public SentinelIntSet assertFullWalkNoDups(int maxSize, SolrParams params) 
    throws Exception {

    SentinelIntSet ids = new SentinelIntSet(maxSize, -1);
    String cursorMark = CURSOR_MARK_START;
    int docsOnThisPage = Integer.MAX_VALUE;
    while (0 < docsOnThisPage) {
      String json = assertJQ(req(params, 
                                 CURSOR_MARK_PARAM, cursorMark));
      Map rsp = (Map) ObjectBuilder.fromJSON(json);
      assertTrue("response doesn't contain " + CURSOR_MARK_NEXT + ": " + json,
                 rsp.containsKey(CURSOR_MARK_NEXT));
      String nextCursorMark = (String)rsp.get(CURSOR_MARK_NEXT);
      assertNotNull(CURSOR_MARK_NEXT + " is null", nextCursorMark);
      List<Map<Object,Object>> docs = (List) (((Map)rsp.get("response")).get("docs"));
      docsOnThisPage = docs.size();
      if (null != params.getInt(CommonParams.ROWS)) {
        int rows = params.getInt(CommonParams.ROWS);
        assertTrue("Too many docs on this page: " + rows + " < " + docsOnThisPage,
                   docsOnThisPage <= rows);
      }
      if (0 == docsOnThisPage) {
        assertEquals("no more docs, but "+CURSOR_MARK_NEXT+" isn't same",
                     cursorMark, nextCursorMark);
      }
      for (Map<Object,Object> doc : docs) {
        int id = ((Long)doc.get("id")).intValue();
        assertFalse("walk already seen: " + id, ids.exists(id));
        ids.put(id);
        assertFalse("id set bigger then max allowed ("+maxSize+"): " + ids.size(),
                    maxSize < ids.size());
      }
      cursorMark = nextCursorMark;
    }
    return ids;
  }

  /**
   * test faceting with deep paging
   */
  public void testFacetingWithRandomSorts() throws Exception {
    final int numDocs = _TestUtil.nextInt(random(), 1000, 3000);
    String[] fieldsToFacetOn = { "int", "long", "str" };
    String[] facetMethods = { "enum", "fc", "fcs" };

    for (int i = 1; i <= numDocs; i++) {
      SolrInputDocument doc = buildRandomDocument(i);
      assertU(adoc(doc));
    }
    assertU(commit());

    Collection<String> allFieldNames = getAllSortFieldNames();
    String[] fieldNames = new String[allFieldNames.size()];
    allFieldNames.toArray(fieldNames);
    String f = fieldNames[_TestUtil.nextInt(random(), 0, fieldNames.length - 1)];
    String order = 0 == _TestUtil.nextInt(random(), 0, 1) ? " asc" : " desc";
    String sort = f + order + (f.equals("id") ? "" : ", id" + order);
    String rows = "" + _TestUtil.nextInt(random(),13,50);
    String facetField = fieldsToFacetOn
        [_TestUtil.nextInt(random(), 0, fieldsToFacetOn.length - 1)];
    String facetMethod = facetMethods
        [_TestUtil.nextInt(random(), 0, facetMethods.length - 1)];
    SentinelIntSet ids = assertFullWalkNoDupsWithFacets
        (numDocs, params("q", "*:*",
            "fl", "id," + facetField,
            "facet", "true",
            "facet.field", facetField,
            "facet.method", facetMethod,
            "facet.missing", "true",
            "facet.limit", "-1", // unlimited
            "rows", rows,
            "sort", sort));
    assertEquals(numDocs, ids.size());
  }

  /**
   * Given a set of params, executes a cursor query using {@link #CURSOR_MARK_START}
   * and then continuously walks the results using {@link #CURSOR_MARK_START} as long
   * as a non-0 number of docs ar returned.  This method records the the set of all id's
   * (must be positive ints) encountered and throws an assertion failure if any id is
   * encountered more than once, or if the set grows above maxSize.
   *
   * Also checks that facets are the same with each page, and that they are correct.
   */
  public SentinelIntSet assertFullWalkNoDupsWithFacets(int maxSize, SolrParams params)
      throws Exception {

    final String facetField = params.get("facet.field");
    assertNotNull("facet.field param not specified", facetField);
    assertFalse("facet.field param contains multiple values", facetField.contains(","));
    assertEquals("facet.limit param not set to -1", "-1", params.get("facet.limit"));
    final Map<String,MutableValueInt> facetCounts = new HashMap<String,MutableValueInt>();
    SentinelIntSet ids = new SentinelIntSet(maxSize, -1);
    String cursorMark = CURSOR_MARK_START;
    int docsOnThisPage = Integer.MAX_VALUE;
    List previousFacets = null;
    while (0 < docsOnThisPage) {
      String json = assertJQ(req(params, CURSOR_MARK_PARAM, cursorMark));
      Map rsp = (Map) ObjectBuilder.fromJSON(json);
      assertTrue("response doesn't contain " + CURSOR_MARK_NEXT + ": " + json,
                 rsp.containsKey(CURSOR_MARK_NEXT));
      String nextCursorMark = (String)rsp.get(CURSOR_MARK_NEXT);
      assertNotNull(CURSOR_MARK_NEXT + " is null", nextCursorMark);
      List<Map<Object,Object>> docs = (List)(((Map)rsp.get("response")).get("docs"));
      docsOnThisPage = docs.size();
      if (null != params.getInt(CommonParams.ROWS)) {
        int rows = params.getInt(CommonParams.ROWS);
        assertTrue("Too many docs on this page: " + rows + " < " + docsOnThisPage,
                   docsOnThisPage <= rows);
      }
      if (0 == docsOnThisPage) {
        assertEquals("no more docs, but "+CURSOR_MARK_NEXT+" isn't same",
                     cursorMark, nextCursorMark);
      }
      for (Map<Object,Object> doc : docs) {
        int id = ((Long)doc.get("id")).intValue();
        assertFalse("walk already seen: " + id, ids.exists(id));
        ids.put(id);
        assertFalse("id set bigger then max allowed ("+maxSize+"): " + ids.size(),
                    maxSize < ids.size());
        Object facet = doc.get(facetField);
        String facetString = null == facet ? null : facet.toString(); // null: missing facet value
        MutableValueInt count = facetCounts.get(facetString);
        if (null == count) {
          count = new MutableValueInt();
          facetCounts.put(facetString, count);
        }
        ++count.value;
      }
      cursorMark = nextCursorMark;

      Map facetFields = (Map)((Map)rsp.get("facet_counts")).get("facet_fields");
      List facets = (List)facetFields.get(facetField);
      if (null != previousFacets) {
        assertEquals("Facets not the same as on previous page:\nprevious page facets: "
            + Arrays.toString(facets.toArray(new Object[facets.size()]))
            + "\ncurrent page facets: "
            + Arrays.toString(previousFacets.toArray(new Object[previousFacets.size()])),
            previousFacets, facets);
      }
      previousFacets = facets;
    }

    assertNotNull("previousFacets is null", previousFacets);
    assertEquals("Mismatch in number of facets: ", facetCounts.size(), previousFacets.size() / 2);
    int pos;
    for (pos = 0 ; pos < previousFacets.size() ; pos += 2) {
      String label = (String)previousFacets.get(pos);
      int expectedCount = ((Number)previousFacets.get(pos + 1)).intValue();
      MutableValueInt count = facetCounts.get(label);
      assertNotNull("Expected facet label #" + (pos / 2) + " not found: '" + label + "'", count);
      assertEquals("Facet count mismatch for label #" + (pos / 2) + " '" + label + "'", expectedCount,
                   facetCounts.get(label).value);
      pos += 2;
    }
    return ids;
  }

  /**
   * Asserts that the query matches the specified JSON patterns and then returns the
   * {@link #CURSOR_MARK_NEXT} value from the response
   *
   * @see #assertJQ
   */
  public String assertCursor(SolrQueryRequest req, String... tests) throws Exception {
    String json = assertJQ(req, tests);
    Map rsp = (Map) ObjectBuilder.fromJSON(json);
    assertTrue("response doesn't contain "+CURSOR_MARK_NEXT + ": " + json,
               rsp.containsKey(CURSOR_MARK_NEXT));
    String next = (String)rsp.get(CURSOR_MARK_NEXT);
    assertNotNull(CURSOR_MARK_NEXT + " is null", next);
    return next;
  }

  /**
   * execute a local request, verify that we get an expected error
   */
  public void assertFail(SolrParams p, ErrorCode expCode, String expSubstr) 
    throws Exception {

    try {
      ignoreException(expSubstr);
      assertJQ(req(p));
      fail("no exception matching expected: " + expCode.code + ": " + expSubstr);
    } catch (SolrException e) {
      assertEquals(expCode.code, e.code());
      assertTrue("Expected substr not found: " + expSubstr + " <!< " + e.getMessage(),
                 e.getMessage().contains(expSubstr));
    } finally {
      unIgnoreException(expSubstr);
    }
  }

  /**
   * Creates a document with randomized field values, some of which be missing values, 
   * and some of which will be skewed so that small subsets of the ranges will be 
   * more common (resulting in an increased likelihood of duplicate values)
   * 
   * @see #buildRandomQuery
   */
  public static SolrInputDocument buildRandomDocument(int id) {
    SolrInputDocument doc = sdoc("id", id);
    // most fields are in most docs
    // if field is in a doc, then "skewed" chance val is from a dense range
    // (hopefully with lots of duplication)
    if (useField()) {
      doc.addField("int", skewed(random().nextInt(), 
                                 _TestUtil.nextInt(random(), 20, 50)));
    }
    if (useField()) {
      doc.addField("long", skewed(random().nextLong(), 
                                  _TestUtil.nextInt(random(), 5000, 5100)));
    }
    if (useField()) {
      doc.addField("float", skewed(random().nextFloat() * random().nextInt(), 
                                   1.0F / random().nextInt(23)));
    }
    if (useField()) {
      doc.addField("double", skewed(random().nextDouble() * random().nextInt(), 
                                    1.0D / random().nextInt(37)));
    }
    if (useField()) {
      doc.addField("str", skewed(randomUsableUnicodeString(),
                                 _TestUtil.randomSimpleString(random(),1,1)));

    }
    if (useField()) {
      int numBytes = (Integer) skewed(_TestUtil.nextInt(random(), 20, 50), 2);
      byte[] randBytes = new byte[numBytes];
      random().nextBytes(randBytes);
      doc.addField("bin", ByteBuffer.wrap(randBytes));
    }
    return doc;
  }

  /**
   * Generates a random query using the fields populated by 
   * {@link #buildRandomDocument}.  Queries will typically be fairly simple, but 
   * won't be so trivial that the scores are completely constant.
   */
  public static String buildRandomQuery() {
    List<String> numericFields = Arrays.asList("int","long","float","double");
    Collections.shuffle(numericFields, random());
    if (random().nextBoolean()) {
      // simple function query across one field.
      return "{!func}" + numericFields.get(0);
    } else {
      // several SHOULD clauses on range queries
      int low = _TestUtil.nextInt(random(),-2379,2);
      int high = _TestUtil.nextInt(random(),4,5713);
      return 
        numericFields.get(0) + ":[* TO 0] " +
        numericFields.get(1) + ":[0 TO *] " +
        numericFields.get(2) + ":[" + low + " TO " + high + "]";
    }
  }

  /**
   * We want "realistic" unicode strings beyond simple ascii, but because our
   * updates use XML we need to ensure we don't get "special" code block.
   */
  private static String randomUsableUnicodeString() {
    String result = _TestUtil.randomRealisticUnicodeString(random());
    if (result.matches(".*\\p{InSpecials}.*")) {
      // oh well
      result = _TestUtil.randomSimpleString(random());
    }
    return result;
  }

  /**
   * Given a list of fieldNames, builds up a random sort string which is guaranteed to
   * have at least 3 clauses, ending with the "id" field for tie breaking
   */
  public static String buildRandomSort(final Collection<String> fieldNames) {

    ArrayList<String> shuffledNames = new ArrayList<String>(fieldNames);
    Collections.replaceAll(shuffledNames, "id", "score");
    Collections.shuffle(shuffledNames, random());

    final StringBuilder result = new StringBuilder();
    final int numClauses = atLeast(2);

    for (int i = 0; i < numClauses; i++) {
      String field = shuffledNames.get(i);

      // wrap in a function sometimes
      if ( (!"score".equals(field))
           && 
           (0 == _TestUtil.nextInt(random(), 0, 7)) ) {
        // specific function doesn't matter, just proving that we can handle the concept.
        // but we do have to be careful with non numeric fields
        if (field.startsWith("str") || field.startsWith("bin")) {
          field = "if(exists(" + field + "),47,83)";
        } else {
          field = "abs(" + field + ")";
        }
      }
      result.append(field).append(random().nextBoolean() ? " asc, " : " desc, ");
    }
    result.append("id").append(random().nextBoolean() ? " asc" : " desc");
    return result.toString();
  }

}
