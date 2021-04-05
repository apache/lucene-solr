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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CursorMark;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.BeforeClass;

import org.apache.commons.lang3.StringUtils;

import static org.apache.solr.common.params.SolrParams.wrapDefaults;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_NEXT;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_PARAM;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;
import static org.apache.solr.common.params.CommonParams.TIME_ALLOWED;
import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * Tests of deep paging using {@link CursorMark} and {@link CursorMarkParams#CURSOR_MARK_PARAM}.
 */
public class CursorPagingTest extends SolrTestCaseJ4 {

  /** solrconfig.xml file name, shared with other cursor related tests */

  public final static String TEST_SOLRCONFIG_NAME = "solrconfig-deeppaging.xml";
  /** schema.xml file name, shared with other cursor related tests */
  public final static String TEST_SCHEMAXML_NAME = "schema-sorts.xml";
  /** values from enumConfig.xml */
  public static final String[] SEVERITY_ENUM_VALUES =
      { "Not Available", "Low", "Medium", "High", "Critical" };

  @BeforeClass
  public static void beforeTests() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
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

    // using cursor w/ grouping
    assertFail(params("q", "*:*", 
                      "sort", "id desc", 
                      GroupParams.GROUP, "true",
                      GroupParams.GROUP_FIELD, "str",
                      CURSOR_MARK_PARAM, CURSOR_MARK_START),
               ErrorCode.BAD_REQUEST, "Grouping");

    // if a user specifies a 'bogus' cursorMark param, this should error *only* if some other component
    // cares about (and parses) a SortSpec in it's prepare() method.
    // (the existence of a 'sort' param shouldn't make a diff ... unless it makes a diff to a component being used,
    // which it doesn't for RTG)
    assertU(adoc("id", "yyy", "str", "y", "float", "3", "int", "-3"));
    if (random().nextBoolean()) {
      assertU(commit());
    }
    for (SolrParams p : Arrays.asList(params(),
                                      params(CURSOR_MARK_PARAM, "gibberish"),
                                      params(CURSOR_MARK_PARAM, "gibberish",
                                             "sort", "id asc"))) {
      assertJQ(req(p,
                   "qt","/get",
                   "fl", "id",
                   "id","yyy") 
               , "=={'doc':{'id':'yyy'}}");
      assertJQ(req(p,
                   "qt","/get",
                   "fl", "id",
                   "id","xxx") // doesn't exist in our collection
               , "=={'doc':null}");
    }
  }


  /** simple static test of some carefully crafted docs */
  public void testSimple() throws Exception {
    String cursorMark;
    SolrParams params = null;
    
    final String intsort = "int" + (random().nextBoolean() ? "" : "_dv");
    final String intmissingsort = intsort;

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


    // don't add in order of any field to ensure we aren't inadvertently
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
                              ,"/response/docs==[{'id':'9'},{'id':'8'},{'id':'7'},{'id':'6'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9" 
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'5'},{'id':'3'},{'id':'2'},{'id':'1'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9" 
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'0'}]"
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
                              ,"/response/docs==[{'id':'6'},{'id':'1'},{'id':'8'},{'id':'5'}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':3,'c':0}"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==7"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'4'},{'id':'3'},{'id':'0'}]"
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
                              ,"/response/docs==[{'id':'7'},{'id':'0'},{'id':'3'}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':1,'c':3}"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'4'},{'id':'1'},{'id':'6'}]"
                              ,"/facet_counts/facet_fields/str=={'a':4,'b':1,'c':3}"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'9'},{'id':'2'}]"
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
                              ,"/response/docs==[{'id':'3'},{'id':'7'},{'id':'0'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'4'},{'id':'1'},{'id':'6'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'9'},{'id':'2'}]"
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
                              ,"/response/docs==[{'id':'7'},{'id':'0'},{'id':'4'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'1'},{'id':'6'},{'id':'9'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'2'},{'id':'3'}]"
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
                              ,"/response/docs==[{'id':'6'},{'id':'4'},{'id':'3'},{'id':'1'},{'id':'8'},{'id':'5'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'0'},{'id':'9'},{'id':'7'},{'id':'2'}]"
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
                              ,"/response/docs==[{'id':'2'},{'id':'9'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'7'},{'id':'4'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10" 
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'3'},{'id':'8'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'5'},{'id':'6'}]"
                              );
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==10"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'1'},{'id':'0'}]"
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
                              ,"/response/docs==[{'id':'7'},{'id':'3'}]"
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
                              ,"/response/docs==[{'id':'1'},{'id':'3'}]"
                              );
    // delete the last guy we got
    assertU(delI("3")); 
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==9"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'4'},{'id':'6'}]"
                              );
    // delete the next guy we expect
    assertU(delI("0")); 
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'5'},{'id':'8'}]"
                              );
    // update a doc we've already seen so it repeats
    assertU(adoc("id", "5", "str", "c"));
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'2'},{'id':'5'}]"
                              );
    // update the next doc we expect so it's now in the past
    assertU(adoc("id", "7", "str", "a"));
    assertU(commit());
    cursorMark = assertCursor(req(params, CURSOR_MARK_PARAM, cursorMark)
                              ,"/response/numFound==8"
                              ,"/response/start==0"
                              ,"/response/docs==[{'id':'9'}]"
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
   * test that timeAllowed parameter can be used with cursors
   * uses DelayingSearchComponent in solrconfig-deeppaging.xml
   */
  @LogLevel("org.apache.solr.search.SolrIndexSearcher=ERROR;org.apache.solr.handler.component.SearchHandler=ERROR")
  public void testTimeAllowed() throws Exception {
    String wontExceedTimeout = "10000";
    int numDocs = 1000;
    List<String> ids = IntStream.range(0, 1000).mapToObj(String::valueOf).collect(Collectors.toList());
    // Shuffle to test ordering
    Collections.shuffle(ids, random());
    for (String id : ids) {
      assertU(adoc("id", id, "name", "a" + id));
      if (random().nextInt(numDocs) == 0) {
        assertU(commit());  // sometimes make multiple segments
      }
    }
    assertU(commit());

    Collections.sort(ids);

    String cursorMark, nextCursorMark = CURSOR_MARK_START;

    SolrParams params = params("q", "name:a*",
        "fl", "id",
        "sort", "id asc",
        "rows", "50",
        "qt", "/delayed",
        "sleep", "10");

    List<String> foundDocIds = new ArrayList<>();
    String[] timeAllowedVariants = {"1", "50", wontExceedTimeout};
    int partialCount = 0;
    do {
      cursorMark = nextCursorMark;
      for (String timeAllowed : timeAllowedVariants) {

        // execute the query
        String json = assertJQ(req(params, CURSOR_MARK_PARAM, cursorMark, TIME_ALLOWED, timeAllowed));

        Map<?, ?> response = (Map<?, ?>) fromJSONString(json);
        Map<?, ?> responseHeader = (Map<?, ?>) response.get("responseHeader");
        Map<?, ?> responseBody = (Map<?, ?>) response.get("response");
        nextCursorMark = (String) response.get(CURSOR_MARK_NEXT);

        // count occurrence of partialResults (confirm at the end at least one)
        if (responseHeader.containsKey("partialResults")) {
          partialCount++;
        }

        // add the ids found (confirm we found all at the end in correct order)
        @SuppressWarnings({"unchecked"})
        List<Map<Object, Object>> docs = (List<Map<Object, Object>>) (responseBody.get("docs"));
        for (Map<Object, Object> doc : docs) {
          foundDocIds.add(doc.get("id").toString());
        }

        // break out of the timeAllowed variants as soon as we progress
        if (!cursorMark.equals(nextCursorMark)) {
          break;
        }
      }
    } while (!cursorMark.equals(nextCursorMark));

    ArrayList<String> sortedFoundDocIds = new ArrayList<>(foundDocIds);
    sortedFoundDocIds.sort(null);
    // Note: it is not guaranteed that all docs will be found, because a query may time out
    // before reaching all segments, this causes documents in the skipped segments to be skipped
    // in the overall result set as the cursor pages through.
    assertEquals("Should have found last doc id eventually", ids.get(ids.size() -1), foundDocIds.get(foundDocIds.size() -1));
    assertEquals("Documents arrived in sorted order within and between pages", sortedFoundDocIds, foundDocIds);
    assertTrue("Should have experienced at least one partialResult", partialCount > 0);
  }


  /**
   * test that our assumptions about how caches are affected hold true
   */
  public void testCacheImpacts() throws Exception {
    // cursor queries can't live in the queryResultCache, but independent filters
    // should still be cached & reused

    // don't add in order of any field to ensure we aren't inadvertently
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

    final MetricsMap filterCacheStats =
        (MetricsMap)((SolrMetricManager.GaugeWrapper)h.getCore().getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.filterCache")).getGauge();
    assertNotNull(filterCacheStats);
    final MetricsMap queryCacheStats =
        (MetricsMap)((SolrMetricManager.GaugeWrapper)h.getCore().getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.queryResultCache")).getGauge();
    assertNotNull(queryCacheStats);

    final long preQcIn = (Long) queryCacheStats.getValue().get("inserts");
    final long preFcIn = (Long) filterCacheStats.getValue().get("inserts");
    final long preFcHits = (Long) filterCacheStats.getValue().get("hits");

    SentinelIntSet ids = assertFullWalkNoDups
      (10, params("q", "*:*",
                  "rows",""+ TestUtil.nextInt(random(), 1, 11),
                  "fq", "-id:[1 TO 2]",
                  "fq", "-id:[6 TO 7]",
                  "fl", "id",
                  "sort", buildRandomSort(allFieldNames)));
    
    assertEquals(6, ids.size());

    final long postQcIn = (Long) queryCacheStats.getValue().get("inserts");
    final long postFcIn = (Long) filterCacheStats.getValue().get("inserts");
    final long postFcHits = (Long) filterCacheStats.getValue().get("hits");
    
    assertEquals("query cache inserts changed", preQcIn, postQcIn);
    // NOTE: use of pure negative filters clauses "*:* to be tracked in filterCache
    assertEquals("filter cache did not grow correctly", 3, postFcIn-preFcIn);
    assertTrue("filter cache did not have any new cache hits", 0 < postFcHits-preFcHits);

  }

  /** randomized testing of a non-trivial number of docs using assertFullWalkNoDups 
   */
  public void testRandomSortsOnLargeIndex() throws Exception {
    final Collection<String> allFieldNames = getAllSortFieldNames();

    final int initialDocs = TestUtil.nextInt(random(), 100, 200);
    final int totalDocs = atLeast(500);

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
        String rows = "" + TestUtil.nextInt(random(), 13, 50);
        final SolrParams main = params("q", "*:*",
                                       "fl","id",
                                       "rows",rows,
                                       "sort",sort);
        final SentinelIntSet ids = assertFullWalkNoDups(totalDocs, main);
        assertEquals(initialDocs, ids.size());

        // same query, now with QEC ... verify we get all the same docs, but the (expected) elevated docs are first...
        final SentinelIntSet elevated = assertFullWalkNoDupsElevated(wrapDefaults(params("qt", "/elevate",
                                                                                         "fl","id,[elevated]",
                                                                                         "forceElevation","true",
                                                                                         "elevateIds", "50,20,80"),
                                                                                  main),
                                                                     ids);
        assertTrue(elevated.exists(50));
        assertTrue(elevated.exists(20));
        assertTrue(elevated.exists(80));
        assertEquals(3, elevated.size());
      }
    }

    // now add a lot more docs, and test a handful of randomized sorts
    for (int i = initialDocs+1; i <= totalDocs; i++) {
      SolrInputDocument doc = buildRandomDocument(i);
      assertU(adoc(doc));
    }
    assertU(commit());

    final int numRandomSorts = atLeast(3);
    for (int i = 0; i < numRandomSorts; i++) {
      final String sort = buildRandomSort(allFieldNames);
      final String rows = "" + TestUtil.nextInt(random(), 63, 113);
      final String fl = random().nextBoolean() ? "id" : "id,score";
      final boolean matchAll = random().nextBoolean();
      final String q = matchAll ? "*:*" : buildRandomQuery();
      final SolrParams main = params("q", q,
                                     "fl",fl,
                                     "rows",rows,
                                     "sort",sort);
      final SentinelIntSet ids = assertFullWalkNoDups(totalDocs, main);
      if (matchAll) {
        assertEquals(totalDocs, ids.size());
      }

      // same query, now with QEC ... verify we get all the same docs, but the (expected) elevated docs are first...
      // first we have to build a set of ids to elevate, from the set of ids known to match query...
      final int[] expectedElevated = pickElevations(TestUtil.nextInt(random(), 3, 33), ids);
      final SentinelIntSet elevated = assertFullWalkNoDupsElevated
        (wrapDefaults(params("qt", "/elevate",
                             "fl", fl + ",[elevated]",
                             // HACK: work around SOLR-15307... same results should match, just not same order
                             "sort", (sort.startsWith("score asc") ? "score desc, " + sort : sort),
                             "forceElevation","true",
                             "elevateIds", StringUtils.join(expectedElevated,',')),
                      main),
         ids);
      for (int expected : expectedElevated) {
        assertTrue(expected + " wasn't elevated even though it should have been",
                   elevated.exists(expected));
      }
      assertEquals(expectedElevated.length, elevated.size());
    }
  }

  /** Similar to usually() but we want it to happen just as often regardless
   * of test multiplier and nightly status 
   */
  private static boolean useField() {
    return 0 != TestUtil.nextInt(random(), 0, 30);
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
   * </ul>
   */
  public static List<String> pruneAndDeterministicallySort(Collection<String> raw) {

    ArrayList<String> names = new ArrayList<>(37);
    for (String f : raw) {
      if (f.equals("_version_")) {
        continue;
      }
      names.add(f);
    }

    Collections.sort(names);
    Collections.shuffle(names,random());
    return Collections.<String>unmodifiableList(names);
  }

  /**
   * Given a set of params, executes a cursor query using {@link CursorMarkParams#CURSOR_MARK_START}
   * and then continuously walks the results using {@link CursorMarkParams#CURSOR_MARK_START} as long
   * as a non-0 number of docs ar returned.  This method records the the set of all id's
   * (must be positive ints) encountered and throws an assertion failure if any id is
   * encountered more than once, or if an id is encountered which is not expected, 
   * or if an id is <code>[elevated]</code> and comes "after" any ids which were not <code>[elevated]</code>
   * 
   *
   * @returns set of all elevated ids encountered in the walk
   * @see #assertFullWalkNoDups(SolrParams,Consumer)
   */
  public SentinelIntSet assertFullWalkNoDupsElevated(final SolrParams params, final SentinelIntSet allExpected)
    throws Exception {

    final SentinelIntSet ids = new SentinelIntSet(allExpected.size(), -1);
    final SentinelIntSet idsElevated = new SentinelIntSet(32, -1);

    assertFullWalkNoDups(params, (doc) -> {
        final int id = Integer.parseInt(doc.get("id").toString());
        final boolean elevated = Boolean.parseBoolean(doc.getOrDefault("[elevated]","false").toString());
        assertTrue(id + " is not expected to match query",
                   allExpected.exists(id));
        assertFalse("walk already seen: " + id,
                    ids.exists(id));
        if (elevated) {
          assertEquals("id is elevated, but we've already seen non elevated ids: " + id,
                       idsElevated.size(), ids.size());
          idsElevated.put(id);
        }
        ids.put(id);
      });
    assertEquals("total number of ids seen did not match expected",
                 allExpected.size(), ids.size());
    
    return idsElevated;
  }

  
  /**
   * Given a set of params, executes a cursor query using {@link CursorMarkParams#CURSOR_MARK_START}
   * and then continuously walks the results using {@link CursorMarkParams#CURSOR_MARK_START} as long
   * as a non-0 number of docs ar returned.  This method records the the set of all id's
   * (must be positive ints) encountered and throws an assertion failure if any id is
   * encountered more than once, or if the set grows above maxSize
   *
   * @returns set of all ids encountered in the walk
   * @see #assertFullWalkNoDups(SolrParams,Consumer)
   */
  public SentinelIntSet assertFullWalkNoDups(int maxSize, SolrParams params) 
    throws Exception {
    final SentinelIntSet ids = new SentinelIntSet(maxSize, -1);
    assertFullWalkNoDups(params, (doc) -> {
        int id = Integer.parseInt(doc.get("id").toString());
        assertFalse("walk already seen: " + id, ids.exists(id));
        ids.put(id);
        assertFalse("id set bigger then max allowed ("+maxSize+"): " + ids.size(),
                    maxSize < ids.size());
        
      });
    return ids;
  }
  
  /**
   * Given a set of params, executes a cursor query using {@link CursorMarkParams#CURSOR_MARK_START}
   * and then continuously walks the results using {@link CursorMarkParams#CURSOR_MARK_START} as long
   * as a non-0 number of docs ar returned.  This method does some basic validation of each response, and then 
   * passes each doc encountered (in order returned) to the specified Consumer, which may throw an assertion if 
   * there is a problem. 
   */
  public void assertFullWalkNoDups(SolrParams params, Consumer<Map<Object,Object>> consumer)
    throws Exception {
    
    String cursorMark = CURSOR_MARK_START;
    int docsOnThisPage = Integer.MAX_VALUE;
    while (0 < docsOnThisPage) {
      String json = assertJQ(req(params,
                                 CURSOR_MARK_PARAM, cursorMark));
      @SuppressWarnings({"rawtypes"})
      Map rsp = (Map) fromJSONString(json);
      assertTrue("response doesn't contain " + CURSOR_MARK_NEXT + ": " + json,
                 rsp.containsKey(CURSOR_MARK_NEXT));
      String nextCursorMark = (String)rsp.get(CURSOR_MARK_NEXT);
      assertNotNull(CURSOR_MARK_NEXT + " is null", nextCursorMark);
      @SuppressWarnings({"unchecked"})
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
        consumer.accept(doc);
      }
      cursorMark = nextCursorMark;
    }
  }

  /**
   * test faceting with deep paging
   */
  public void testFacetingWithRandomSorts() throws Exception {
    final int numDocs = TestUtil.nextInt(random(), 1000, 3000);
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
    String f = fieldNames[TestUtil.nextInt(random(), 0, fieldNames.length - 1)];
    String order = 0 == TestUtil.nextInt(random(), 0, 1) ? " asc" : " desc";
    String sort = f + order + (f.equals("id") ? "" : ", id" + order);
    String rows = "" + TestUtil.nextInt(random(), 13, 50);
    String facetField = fieldsToFacetOn
        [TestUtil.nextInt(random(), 0, fieldsToFacetOn.length - 1)];
    String facetMethod = facetMethods
        [TestUtil.nextInt(random(), 0, facetMethods.length - 1)];
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
   * Given a set of params, executes a cursor query using {@link CursorMarkParams#CURSOR_MARK_START}
   * and then continuously walks the results using {@link CursorMarkParams#CURSOR_MARK_START} as long
   * as a non-0 number of docs ar returned.  This method records the the set of all id's
   * (must be positive ints) encountered and throws an assertion failure if any id is
   * encountered more than once, or if the set grows above maxSize.
   *
   * Also checks that facets are the same with each page, and that they are correct.
   */
  @SuppressWarnings({"unchecked"})
  public SentinelIntSet assertFullWalkNoDupsWithFacets(int maxSize, SolrParams params)
      throws Exception {

    final String facetField = params.get("facet.field");
    assertNotNull("facet.field param not specified", facetField);
    assertFalse("facet.field param contains multiple values", facetField.contains(","));
    assertEquals("facet.limit param not set to -1", "-1", params.get("facet.limit"));
    final Map<String,MutableValueInt> facetCounts = new HashMap<>();
    SentinelIntSet ids = new SentinelIntSet(maxSize, -1);
    String cursorMark = CURSOR_MARK_START;
    int docsOnThisPage = Integer.MAX_VALUE;
    @SuppressWarnings({"rawtypes"})
    List previousFacets = null;
    while (0 < docsOnThisPage) {
      String json = assertJQ(req(params, CURSOR_MARK_PARAM, cursorMark));
      @SuppressWarnings({"rawtypes"})
      Map rsp = (Map) fromJSONString(json);
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
        int id = Integer.parseInt(doc.get("id").toString());
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

      @SuppressWarnings({"rawtypes"})
      Map facetFields = (Map)((Map)rsp.get("facet_counts")).get("facet_fields");
      @SuppressWarnings({"rawtypes"})
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
   * {@link CursorMarkParams#CURSOR_MARK_NEXT} value from the response
   *
   * @see #assertJQ
   */
  public String assertCursor(SolrQueryRequest req, String... tests) throws Exception {
    String json = assertJQ(req, tests);
    @SuppressWarnings({"rawtypes"})
    Map rsp = (Map) fromJSONString(json);
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
      SolrException e = expectThrows(SolrException.class, () -> {
        ignoreException(expSubstr);
        assertJQ(req(p));
      });
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
                                 TestUtil.nextInt(random(), 20, 50)));
    }
    if (useField()) {
      doc.addField("long", skewed(random().nextLong(), 
                                  TestUtil.nextInt(random(), 5000, 5100)));
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
      doc.addField("str", skewed(randomXmlUsableUnicodeString(),
                                 TestUtil.randomSimpleString(random(), 1, 1)));
    }
    if (useField()) {
      int numBytes = (int) skewed(TestUtil.nextInt(random(), 20, 50), 2);
      byte[] randBytes = new byte[numBytes];
      random().nextBytes(randBytes);
      doc.addField("bin", ByteBuffer.wrap(randBytes));
    }
    if (useField()) {
      doc.addField("date", skewed(randomDate(), randomSkewedDate()));
    }
    if (useField()) {
      doc.addField("uuid", UUID.randomUUID().toString());
    }
    if (useField()) {
      doc.addField("currency", skewed("" + (random().nextInt() / 100.) + "," + randomCurrency(),
                                      "" + TestUtil.nextInt(random(), 250, 320) + ",USD"));
    }
    if (useField()) {
      doc.addField("bool", random().nextBoolean() ? "t" : "f");
    }
    if (useField()) {
      doc.addField("enum", randomEnumValue());
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
      int low = TestUtil.nextInt(random(), -2379, 2);
      int high = TestUtil.nextInt(random(), 4, 5713);
      return 
        numericFields.get(0) + ":[* TO 0] " +
        numericFields.get(1) + ":[0 TO *] " +
        numericFields.get(2) + ":[" + low + " TO " + high + "]";
    }
  }

  private static final String[] currencies = { "USD", "EUR", "NOK" };

  public static String randomCurrency() {
    return currencies[random().nextInt(currencies.length)];
  }

  private static String randomEnumValue() {
    return SEVERITY_ENUM_VALUES[random().nextInt(SEVERITY_ENUM_VALUES.length)];
  }

  /**
   * Given a list of fieldNames, builds up a random sort string which is guaranteed to
   * have at least 3 clauses, ending with the "id" field for tie breaking
   */
  public static String buildRandomSort(final Collection<String> fieldNames) {

    ArrayList<String> shuffledNames = new ArrayList<>(fieldNames);
    Collections.replaceAll(shuffledNames, "id", "score");
    Collections.shuffle(shuffledNames, random());

    final StringBuilder result = new StringBuilder();
    final int numClauses = TestUtil.nextInt(random(), 2, 5);

    for (int i = 0; i < numClauses; i++) {
      String field = shuffledNames.get(i);

      // wrap in a function sometimes
      if ( ! "score".equals(field) && 0 == TestUtil.nextInt(random(), 0, 7)) {
        // specific function doesn't matter, just proving that we can handle the concept.
        // but we do have to be careful with non numeric fields
        if (field.contains("float") || field.contains("double")
            || field.contains("int") || field.contains("long")) {
          field = "abs(" + field + ")";
        } else {
          field = "if(exists(" + field + "),47,83)";
        }
      }
      result.append(field).append(random().nextBoolean() ? " asc, " : " desc, ");
    }
    result.append("id").append(random().nextBoolean() ? " asc" : " desc");
    return result.toString();
  }

  /** 
   * Given a set of id, picks some, semi-randomly, to use for elevation
   */
  public static int[] pickElevations(final int numToElevate, final SentinelIntSet ids) {
    assert numToElevate < ids.size();
    final int[] results = new int[numToElevate];
    int slot = 0;
    for (int key : ids.keys) {
      if (key != ids.emptyVal) {
        if (slot < results.length) {
          // until results is full, take any value we can get in the 'next' slot...
          results[slot] = key;
        } else if (numToElevate * 2 < slot) {
          // once we've done enough (extra) iters, break out with what we've got
          break;
        } else {
          // otherwise, pick a random slot to overwrite .. maybe
          if (random().nextBoolean()) {
            results[random().nextInt(results.length)] = key;
          }
        }
        slot++;
      }
    }
    return results;
  }
  
}
