package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;

import org.apache.lucene.facet.FacetTestBase;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.facet.search.ScoredDocIdCollector;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.ScoreFacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

/** Test ScoredDocIdCollector. */
public class TestScoredDocIdCollector extends FacetTestBase {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    initIndex();
  }

  @Override
  public void tearDown() throws Exception {
    closeAll();
    super.tearDown();
  }

  @Test
  public void testConstantScore() throws Exception {
    // test that constant score works well

    Query q = new TermQuery(new Term(CONTENT_FIELD, "white"));
    if (VERBOSE) {
      System.out.println("Query: " + q);
    }
    float constScore = 17.0f;
    ScoredDocIdCollector dCollector = ScoredDocIdCollector.create(indexReader
        .maxDoc(), false); // scoring is disabled
    dCollector.setDefaultScore(constScore);
    searcher.search(q, dCollector);

    // verify by doc scores at the level of doc-id-iterator
    ScoredDocIDs scoredDocIDs = dCollector.getScoredDocIDs();
    assertEquals("Wrong number of matching documents!", 2, scoredDocIDs.size());
    ScoredDocIDsIterator docItr = scoredDocIDs.iterator();
    while (docItr.next()) {
      assertEquals("Wrong score for doc " + docItr.getDocID(), constScore,
          docItr.getScore(), Double.MIN_VALUE);
    }

    // verify by facet values
    List<FacetResult> countRes = findFacets(scoredDocIDs, getFacetedSearchParams());
    List<FacetResult> scoreRes = findFacets(scoredDocIDs, sumScoreSearchParams());

    assertEquals("Wrong number of facet count results!", 1, countRes.size());
    assertEquals("Wrong number of facet score results!", 1, scoreRes.size());

    FacetResultNode parentCountRes = countRes.get(0).getFacetResultNode();
    FacetResultNode parentScoreRes = scoreRes.get(0).getFacetResultNode();

    assertEquals("Wrong number of top count aggregated categories!", 3,
        parentCountRes.getNumSubResults());
    assertEquals("Wrong number of top score aggregated categories!", 3,
        parentScoreRes.getNumSubResults());

    // rely on that facet value is computed as doc-score, and
    // accordingly compare values of the two top-category results.

    FacetResultNode[] countResNodes = resultNodesAsArray(parentCountRes);
    FacetResultNode[] scoreResNodes = resultNodesAsArray(parentScoreRes);

    for (int i = 0; i < scoreResNodes.length; i++) {
      assertEquals("Ordinals differ!", 
          countResNodes[i].getOrdinal(), scoreResNodes[i].getOrdinal());
      assertEquals("Wrong scores!", 
          constScore * countResNodes[i].getValue(),
          scoreResNodes[i].getValue(), 
          Double.MIN_VALUE);
    }
  }

  // compute facets with certain facet requests and docs
  private List<FacetResult> findFacets(ScoredDocIDs sDocids,
      FacetSearchParams facetSearchParams) throws IOException {
    FacetsAccumulator fAccumulator = new StandardFacetsAccumulator(
        facetSearchParams, indexReader, taxoReader);
    List<FacetResult> res = fAccumulator.accumulate(sDocids);

    // Results are ready, printing them...
    int i = 0;
    for (FacetResult facetResult : res) {
      if (VERBOSE) {
        System.out.println("Res " + (i++) + ": " + facetResult);
      }
    }

    return res;
  }

  @Test
  public void testOutOfOrderCollectionScoringEnabled() throws Exception {
    assertFalse(
        "when scoring enabled, out-of-order collection should not be supported",
        ScoredDocIdCollector.create(1, true).acceptsDocsOutOfOrder());
  }

  @Test
  public void testOutOfOrderCollectionScoringDisabled() throws Exception {
    // This used to fail, because ScoredDocIdCollector.acceptDocsOutOfOrder
    // returned true, even when scoring was enabled.
    final int[] docs = new int[] { 1, 0, 2 }; // out of order on purpose

    ScoredDocIdCollector sdic = ScoredDocIdCollector.create(docs.length, false);
    assertTrue(
        "when scoring disabled, out-of-order collection should be supported",
        sdic.acceptsDocsOutOfOrder());
    for (int i = 0; i < docs.length; i++) {
      sdic.collect(docs[i]);
    }

    assertEquals("expected 3 documents but got " + sdic.getScoredDocIDs().size(), 3, sdic.getScoredDocIDs().size());
    ScoredDocIDsIterator iter = sdic.getScoredDocIDs().iterator();
    Arrays.sort(docs);
    for (int i = 0; iter.next(); i++) {
      assertEquals("expected doc " + docs[i], docs[i], iter.getDocID());
    }
  }

  /* use a scoring aggregator */
  private FacetSearchParams sumScoreSearchParams() {
    // this will use default faceted indexing params, not altering anything about indexing
    FacetSearchParams res = super.getFacetedSearchParams();
    res.addFacetRequest(new ScoreFacetRequest(new CategoryPath("root", "a"), 10));
    return res;
  }

  @Override
  protected FacetSearchParams getFacetedSearchParams() {
    FacetSearchParams res = super.getFacetedSearchParams();
    res.addFacetRequest(new CountFacetRequest(new CategoryPath("root","a"), 10));
    return res;
  }

}