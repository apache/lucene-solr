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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.CursorPagingTest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.CursorMark;

import org.apache.commons.lang3.StringUtils;

import static org.apache.solr.common.params.SolrParams.wrapDefaults;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_PARAM;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_NEXT;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Distributed tests of deep paging using {@link CursorMark} and {@link CursorMarkParams#CURSOR_MARK_PARAM}.
 * 
 * NOTE: this class Reuses some utilities from {@link CursorPagingTest} that assume the same schema and configs.
 *
 * @see CursorPagingTest 
 */
@Slow
@SuppressSSL(bugUrl="https://issues.apache.org/jira/browse/SOLR-9182 - causes OOM")
public class DistribCursorPagingTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DistribCursorPagingTest() {
    System.setProperty("solr.test.useFilterForSortedQuery", Boolean.toString(random().nextBoolean()));
    configString = CursorPagingTest.TEST_SOLRCONFIG_NAME;
    schemaString = CursorPagingTest.TEST_SCHEMAXML_NAME;
  }

  @Override
  protected String getCloudSolrConfig() {
    return configString;
  }

  /** 
   * A really obnoxious hack needed to get our elevate.xml into zk ... 
   * But simpler for now then re-writing the whole test case using SolrCloudTestCase.
   */
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    ZkTestServer.putConfig("conf1", zkServer.getZkClient(), ZkTestServer.SOLRHOME, "elevate.xml");
  }
  
  @Test
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      doBadInputTest();
      del("*:*");
      commit();

      doSimpleTest();
      del("*:*");
      commit();

      doRandomSortsOnLargeIndex();
      del("*:*");
      commit();

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }

  private void doBadInputTest() throws Exception {
    // sometimes seed some data, other times use an empty index
    if (random().nextBoolean()) {
      indexDoc(sdoc("id", "42", "str", "z", "float", "99.99", "int", "42"));
      indexDoc(sdoc("id", "66", "str", "x", "float", "22.00", "int", "-66"));
    } else {
      del("*:*");
    }
    commit();

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

  }

  private void doSimpleTest() throws Exception {
    String cursorMark = CURSOR_MARK_START;
    SolrParams params = null;
    QueryResponse rsp = null;

    final String intsort = "int" + (random().nextBoolean() ? "" : "_dv");
    final String intmissingsort = intsort;

    // trivial base case: ensure cursorMark against an empty index doesn't blow up
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","4",
                    "fl", "id",
                    "sort", "id desc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(0, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals(cursorMark, assertHashNextCursorMark(rsp));

    // don't add in order of either field to ensure we aren't inadvertantly 
    // counting on internal docid ordering
    indexDoc(sdoc("id", "9", "str", "c", "float", "-3.2", "int", "42"));
    indexDoc(sdoc("id", "7", "str", "c", "float", "-3.2", "int", "-1976"));
    indexDoc(sdoc("id", "2", "str", "c", "float", "-3.2", "int", "666"));
    indexDoc(sdoc("id", "0", "str", "b", "float", "64.5", "int", "-42"));
    indexDoc(sdoc("id", "5", "str", "b", "float", "64.5", "int", "2001"));
    indexDoc(sdoc("id", "8", "str", "b", "float", "64.5", "int", "4055"));
    indexDoc(sdoc("id", "6", "str", "a", "float", "64.5", "int", "7"));
    indexDoc(sdoc("id", "1", "str", "a", "float", "64.5", "int", "7"));
    indexDoc(sdoc("id", "4", "str", "a", "float", "11.1", "int", "6"));
    indexDoc(sdoc("id", "3", "str", "a", "float", "11.1")); // int is missing
    commit();

    // base case: ensure cursorMark that matches no docs doesn't blow up
    cursorMark = CURSOR_MARK_START;
    params = params("q", "id:9999999", 
                    "rows","4",
                    "fl", "id",
                    "sort", "id desc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(0, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals(cursorMark, assertHashNextCursorMark(rsp));

    // edge case: ensure rows=0 doesn't blow up and gives back same cursor for next
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","0",
                    "fl", "id",
                    "sort", "id desc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(10, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals(cursorMark, assertHashNextCursorMark(rsp));

    // simple id sort
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:6", 
                    "rows","4",
                    "fl", "id",
                    "sort", "id desc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(9, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 9, 8, 7, 6);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(9, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 5, 3, 2, 1);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(9, rsp); 
    assertStartsAt(0, rsp);
    assertDocList(rsp, 0);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(9, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));
    
    // NOTE: because field stats and queryNorms can vary amongst shards,
    //       not all "obvious" score based sorts can be iterated cleanly.
    //       queries that seem like they should result in an obvious "tie" score 
    //       between two documents (and would tie in a single node case) may actually
    //       get diff scores for diff docs if they are on diff shards
    //
    //       so here, in this test, we can't assert a hardcoded score ordering -- we trust 
    //       the full walk testing (below)

    // int sort with dups, id tie breaker ... and some faceting
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:2001 -int:4055", 
                    "rows","3",
                    "fl", "id",
                    "facet", "true",
                    "facet.field", "str",
                    "facet.mincount", "1",
                    "json.nl", "map",
                    "sort", intsort + " asc, id asc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 7, 0, 3);
    assertEquals(3, rsp.getFacetField("str").getValues().size());
    assertEquals("a", rsp.getFacetField("str").getValues().get(0).getName());
    assertEquals(4, rsp.getFacetField("str").getValues().get(0).getCount());
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 4, 1, 6);
    assertEquals("a", rsp.getFacetField("str").getValues().get(0).getName());
    assertEquals(4, rsp.getFacetField("str").getValues().get(0).getCount());
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 9, 2);
    assertEquals("a", rsp.getFacetField("str").getValues().get(0).getName());
    assertEquals(4, rsp.getFacetField("str").getValues().get(0).getCount());
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals("a", rsp.getFacetField("str").getValues().get(0).getName());
    assertEquals(4, rsp.getFacetField("str").getValues().get(0).getCount());
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));
  
    // int missing first sort with dups, id tie breaker
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:2001 -int:4055", 
                    "rows","3",
                    "fl", "id",
                    "json.nl", "map",
                    "sort", intmissingsort + "_first  asc, id asc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 3, 7, 0);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 4, 1, 6);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 9, 2);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));

    // int missing last sort with dups, id tie breaker
    cursorMark = CURSOR_MARK_START;
    params = params("q", "-int:2001 -int:4055", 
                    "rows","3",
                    "fl", "id",
                    "json.nl", "map",
                    "sort", intmissingsort + "_last asc, id asc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 7, 0, 4);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 1, 6, 9);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 2, 3);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));

    // string sort with dups, id tie breaker
    cursorMark = CURSOR_MARK_START;
    params = params("q", "*:*", 
                    "rows","6",
                    "fl", "id",
                    "sort", "str asc, id desc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(10, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 6, 4, 3, 1, 8, 5);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(10, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 0, 9, 7, 2);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(10, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));

    // tri-level sort with more dups of primary then fit on a page.
    // also a function based sort using a simple function(s) on same field
    // (order should be the same in all cases)
    for (String primarysort : new String[] { "float", "field('float')", "sum(float,42)" }) {
      cursorMark = CURSOR_MARK_START;
      params = params("q", "*:*", 
                      "rows","2",
                      "fl", "id",
                      "sort", primarysort + " asc, "+intsort+" desc, id desc");
      rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
      assertNumFound(10, rsp);
      assertStartsAt(0, rsp);
      assertDocList(rsp, 2, 9);
      cursorMark = assertHashNextCursorMark(rsp);
      //
      rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
      assertNumFound(10, rsp); 
      assertStartsAt(0, rsp);
      assertDocList(rsp, 7, 4);
      cursorMark = assertHashNextCursorMark(rsp);
      //
      rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
      assertNumFound(10, rsp); 
      assertStartsAt(0, rsp);
      assertDocList(rsp, 3, 8);
      cursorMark = assertHashNextCursorMark(rsp);
      //
      rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
      assertNumFound(10, rsp); 
      assertStartsAt(0, rsp);
      assertDocList(rsp, 5, 6);
      cursorMark = assertHashNextCursorMark(rsp);
      //
      rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
      assertNumFound(10, rsp);
      assertStartsAt(0, rsp);
      assertDocList(rsp, 1, 0);
      cursorMark = assertHashNextCursorMark(rsp);
      // we've exactly exhausted all the results, but solr had no way of know that
      //
      rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
      assertNumFound(10, rsp); 
      assertStartsAt(0, rsp);
      assertDocList(rsp);
      assertEquals("no more docs, but cursorMark has changed", 
                   cursorMark, assertHashNextCursorMark(rsp));
    }
    
    // trivial base case: rows bigger then number of matches
    cursorMark = CURSOR_MARK_START;
    params = params("q", "id:3 id:7", 
                    "rows","111",
                    "fl", "id",
                    "sort", intsort + " asc, id asc");
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(2, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 7, 3);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(2, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp);
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));

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
                                         "sort", "float desc, id asc, int asc"));
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
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(10, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 1, 3);
    cursorMark = assertHashNextCursorMark(rsp);
    // delete the last guy we got
    del("id:3");
    commit();
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(9, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 4, 6);
    cursorMark = assertHashNextCursorMark(rsp);
    // delete the next guy we expect
    del("id:0"); 
    commit();
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 5, 8);
    cursorMark = assertHashNextCursorMark(rsp);
    // update a doc we've already seen so it repeats
    indexDoc(sdoc("id", "5", "str", "c"));
    commit();
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertNumFound(8, rsp);
    assertStartsAt(0, rsp);
    assertDocList(rsp, 2, 5);
    cursorMark = assertHashNextCursorMark(rsp);
    // update the next doc we expect so it's now in the past
    indexDoc(sdoc("id", "7", "str", "a"));
    commit();
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertDocList(rsp, 9);
    cursorMark = assertHashNextCursorMark(rsp);
    //
    rsp = query(p(params, CURSOR_MARK_PARAM, cursorMark));
    assertDocList(rsp);
    assertEquals("no more docs, but cursorMark has changed", 
                 cursorMark, assertHashNextCursorMark(rsp));


  }

  /** randomized testing of a non-trivial number of docs using assertFullWalkNoDups 
   */
  public void doRandomSortsOnLargeIndex() throws Exception {
    final Collection<String> allFieldNames = getAllSortFieldNames();

    final int numInitialDocs = TestUtil.nextInt(random(), 100, 200);
    final int totalDocs = atLeast(500);

    // start with a smallish number of documents, and test that we can do a full walk using a 
    // sort on *every* field in the schema...

    List<SolrInputDocument> initialDocs = new ArrayList<>();
    for (int i = 1; i <= numInitialDocs; i++) {
      SolrInputDocument doc = CursorPagingTest.buildRandomDocument(i);
      initialDocs.add(doc);
      indexDoc(doc);
    }
    commit();

    for (String f : allFieldNames) {
      for (String order : new String[] {" asc", " desc"}) {
        String sort = f + order + ("id".equals(f) ? "" : ", id" + order);
        String rows = "" + TestUtil.nextInt(random(), 13, 50);
        final SolrParams main = params("q", "*:*",
                                       "fl","id,"+f,
                                       "rows",rows,
                                       "sort",sort);
        final SentinelIntSet ids = assertFullWalkNoDups(numInitialDocs, main);
        assertEquals(numInitialDocs, ids.size());

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

    // now add a lot more docs, and test a handful of randomized multi-level sorts
    for (int i = numInitialDocs+1; i <= totalDocs; i++) {
      SolrInputDocument doc = CursorPagingTest.buildRandomDocument(i);
      indexDoc(doc);
    }
    commit();

    final int numRandomSorts = atLeast(3);
    for (int i = 0; i < numRandomSorts; i++) {
      final String sort = CursorPagingTest.buildRandomSort(allFieldNames);
      final String rows = "" + TestUtil.nextInt(random(), 63, 113);
      final String fl = random().nextBoolean() ? "id" : "id,score";
      final boolean matchAll = random().nextBoolean();
      final String q = matchAll ? "*:*" : CursorPagingTest.buildRandomQuery();
      final SolrParams main = params("q", q,
                                     "fl",fl,
                                     "rows",rows,
                                     "sort",sort);
      final SentinelIntSet ids = assertFullWalkNoDups(totalDocs,
                                                      params("q", q,
                                                             "fl",fl,
                                                             "rows",rows,
                                                             "sort",sort));
      if (matchAll) {
        assertEquals(totalDocs, ids.size());
      }
      
      // same query, now with QEC ... verify we get all the same docs, but the (expected) elevated docs are first...
      // first we have to build a set of ids to elevate, from the set of ids known to match query...
      final int[] expectedElevated = CursorPagingTest.pickElevations(TestUtil.nextInt(random(), 3, 33), ids);
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
  
  /**
   * Asks the LukeRequestHandler on the control client for a list of the fields in the 
   * schema and then prunes that list down to just the fields that can be used for sorting,
   * and returns them as an immutable list in a deterministically random order.
   */
  private List<String> getAllSortFieldNames() throws SolrServerException, IOException {
    LukeRequest req = new LukeRequest("/admin/luke");
    req.setShowSchema(true); 
    NamedList<Object> rsp = controlClient.request(req);
    @SuppressWarnings({"unchecked"})
    NamedList<Object> fields = (NamedList) ((NamedList)rsp.get("schema")).get("fields");
    ArrayList<String> names = new ArrayList<>(fields.size());
    for (Map.Entry<String,Object> item : fields) {
      names.add(item.getKey());
    }
    
    return CursorPagingTest.pruneAndDeterministicallySort(names);
  }

  /**
   * execute a request, verify that we get an expected error
   */
  public void assertFail(SolrParams p, ErrorCode expCode, String expSubstr) 
    throws Exception {

    try {
      ignoreException(expSubstr);
      query(p);
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
   * Given a QueryResponse returned by SolrServer.query, asserts that the
   * numFound on the doc list matches the expectation
   * @see org.apache.solr.client.solrj.SolrClient#query
   */
  private void assertNumFound(int expected, QueryResponse rsp) {
    assertEquals(expected, extractDocList(rsp).getNumFound());
  }

  /**
   * Given a QueryResponse returned by SolrServer.query, asserts that the
   * start on the doc list matches the expectation
   * @see org.apache.solr.client.solrj.SolrClient#query
   */
  private void assertStartsAt(int expected, QueryResponse rsp) {
    assertEquals(expected, extractDocList(rsp).getStart());
  }

  /**
   * Given a QueryResponse returned by SolrServer.query, asserts that the
   * "id" of the list of documents returned matches the expected list
   * @see org.apache.solr.client.solrj.SolrClient#query
   */
  private void assertDocList(QueryResponse rsp, int... ids) {
    SolrDocumentList docs = extractDocList(rsp);
    assertEquals("Wrong number of docs in response", ids.length, docs.size());
    int i = 0;
    for (int id : ids) {
      assertEquals(rsp.toString(), ""+id, docs.get(i).get("id"));
      i++;
    }
  }

  /**
   * Given a QueryResponse returned by SolrServer.query, asserts that the
   * response does include {@link CursorMarkParams#CURSOR_MARK_NEXT} key and returns it
   * @see org.apache.solr.client.solrj.SolrClient#query
   */
  private String assertHashNextCursorMark(QueryResponse rsp) {
    String r = rsp.getNextCursorMark();
    assertNotNull(CURSOR_MARK_NEXT+" is null/missing", r);
    return r;
  }

  private SolrDocumentList extractDocList(QueryResponse rsp) {
    SolrDocumentList docs = rsp.getResults();
    assertNotNull("docList is null", docs);
    return docs;
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
        
        if (ids.exists(id)) {
          String msg = "walk already seen: " + id;
          try {
            try {
              queryAndCompareShards(params("distrib","false",
                                           "q","id:"+id));
            } catch (AssertionError ae) {
              throw new AssertionError(msg + ", found shard inconsistency that would explain it...", ae);
            }
            final QueryResponse rsp = cloudClient.query(params("q","id:"+id));
            throw new AssertionError(msg + ", don't know why; q=id:"+id+" gives: " + rsp.toString());
          } catch (Exception e) {
            throw new AssertionError(msg + ", exception trying to fiture out why...", e);
          }
        }
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
   * <p>
   * Given a set of params, executes a cursor query using {@link CursorMarkParams#CURSOR_MARK_START} 
   * and then continuously walks the results using {@link CursorMarkParams#CURSOR_MARK_START} as long 
   * as a non-0 number of docs ar returned.  This method records the the set of all id's
   * (must be positive ints) encountered and throws an assertion failure if any id is 
   * encountered more then once, or if the set grows above maxSize
   * </p>
   *
   * <p>
   * Note that this method explicitly uses the "cloudClient" for executing the queries, 
   * instead of relying on the test infrastructure to execute the queries redundently
   * against both the cloud client as well as a control client.  This is because term stat 
   * differences in a sharded setup can result in different scores for documents compared 
   * to the control index -- which can affect the sorting in some cases and cause false 
   * negatives in the response comparisons (even if we don't include "score" in the "fl")
   * </p>
   *
   * @returns set of all ids encountered in the walk
   * @see #assertFullWalkNoDups(SolrParams,Consumer)
   */
  public SentinelIntSet assertFullWalkNoDups(int maxSize, SolrParams params) throws Exception {
    final SentinelIntSet ids = new SentinelIntSet(maxSize, -1);
    assertFullWalkNoDups(params, (doc) -> {
        int id = Integer.parseInt(doc.getFieldValue("id").toString());
        if (ids.exists(id)) {
          String msg = "walk already seen: " + id;
          try {
            try {
              queryAndCompareShards(params("distrib","false",
                                           "q","id:"+id));
            } catch (AssertionError ae) {
              throw new AssertionError(msg + ", found shard inconsistency that would explain it...", ae);
            }
            final QueryResponse rsp = cloudClient.query(params("q","id:"+id));
            throw new AssertionError(msg + ", don't know why; q=id:"+id+" gives: " + rsp.toString());
          } catch (Exception e) {
            throw new AssertionError(msg + ", exception trying to fiture out why...", e);
          }
        }
        ids.put(id);
        assertFalse("id set bigger then max allowed ("+maxSize+"): " + ids.size(),
                    maxSize < ids.size());
      });
    return ids;
  }

  
  /**
   * <p>
   * Given a set of params, executes a cursor query using {@link CursorMarkParams#CURSOR_MARK_START} 
   * and then continuously walks the results using {@link CursorMarkParams#CURSOR_MARK_START} as long 
   * as a non-0 number of docs ar returned.  This method does some basic validation of each response, and then 
   * passes each doc encountered (in order returned) to the specified Consumer, which may throw an assertion if 
   * there is a problem. 
   * </p>
   *
   * <p>
   * Note that this method explicitly uses the "cloudClient" for executing the queries, 
   * instead of relying on the test infrastructure to execute the queries redundently
   * against both the cloud client as well as a control client.  This is because term stat 
   * differences in a sharded setup can result in different scores for documents compared 
   * to the control index -- which can affect the sorting in some cases and cause false 
   * negatives in the response comparisons (even if we don't include "score" in the "fl")
   * </p>
   */
  public void assertFullWalkNoDups(SolrParams params, Consumer<SolrDocument> consumer) throws Exception {
  
    String cursorMark = CURSOR_MARK_START;
    int docsOnThisPage = Integer.MAX_VALUE;
    while (0 < docsOnThisPage) {
      final SolrParams p = p(params, CURSOR_MARK_PARAM, cursorMark);
      QueryResponse rsp = cloudClient.query(p);
      String nextCursorMark = assertHashNextCursorMark(rsp);
      SolrDocumentList docs = extractDocList(rsp);
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

      for (SolrDocument doc : docs) {
        consumer.accept(doc);
      }
      cursorMark = nextCursorMark;
    }
  }

  private SolrParams p(SolrParams params, String... other) {
    SolrParams extras = params(other);
    return SolrParams.wrapDefaults(params, extras);
  }

}
