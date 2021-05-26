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
package org.apache.solr.ltr;

import java.io.IOException;
import java.nio.file.Paths;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.TestLinearModel;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLTRReRankingPipeline extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final SolrResourceLoader solrResourceLoader = new SolrResourceLoader(Paths.get("").toAbsolutePath());

  @BeforeClass
  public static void setup() throws Exception {
    initCore("solrconfig-ltr.xml", "schema.xml");
  }

  private static List<Feature> makeFieldValueFeatures(int[] featureIds,
      String field) {
    final List<Feature> features = new ArrayList<>();
    for (final int i : featureIds) {
      final Map<String,Object> params = new HashMap<String,Object>();
      params.put("field", field);
      final Feature f = Feature.getInstance(solrResourceLoader,
          FieldValueFeature.class.getName(),
          "f" + i, params);
      f.setIndex(i);
      features.add(f);
    }
    return features;
  }

  private static class MockModel extends LTRScoringModel {

    public MockModel(String name, List<Feature> features,
        List<Normalizer> norms,
        String featureStoreName, List<Feature> allFeatures,
        Map<String,Object> params) {
      super(name, features, norms, featureStoreName, allFeatures, params);
    }

    @Override
    public float score(float[] modelFeatureValuesNormalized) {
      return modelFeatureValuesNormalized[2];
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc,
        float finalScore, List<Explanation> featureExplanations) {
      return null;
    }

  }

  @Test
  public void testRescorer() throws Exception {
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "field", "wizard the the the the the oz", "finalScore", "F"));
    assertU(adoc("id", "1", "field", "wizard oz the the the the the the", "finalScore", "T"));
    assertU(commit());

    try (SolrQueryRequest solrQueryRequest = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams())) {

      final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
      bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
      bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
      final SolrIndexSearcher searcher = solrQueryRequest.getSearcher();
      // first run the standard query
      TopDocs hits = searcher.search(bqBuilder.build(), 10);
      assertEquals(2, hits.totalHits.value);
      assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
      assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

      final List<Feature> features = makeFieldValueFeatures(new int[] {0, 1, 2},
              "finalScore");
      final List<Normalizer> norms =
              new ArrayList<Normalizer>(
                      Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
      final List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0, 1,
              2, 3, 4, 5, 6, 7, 8, 9}, "finalScore");
      final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
              features, norms, "test", allFeatures, TestLinearModel.makeFeatureWeights(features));

      LTRScoringQuery ltrScoringQuery = new LTRScoringQuery(ltrScoringModel);
      ltrScoringQuery.setRequest(solrQueryRequest);
      final LTRRescorer rescorer = new LTRRescorer(ltrScoringQuery);
      hits = rescorer.rescore(searcher, hits, 2);

      // rerank using the field finalScore
      assertEquals("1", searcher.doc(hits.scoreDocs[0].doc).get("id"));
      assertEquals("0", searcher.doc(hits.scoreDocs[1].doc).get("id"));
    }
  }

  @Test
  public void testDifferentTopN() throws IOException {
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "field", "wizard oz oz oz oz oz", "finalScoreFloat", "1.0"));
    assertU(adoc("id", "1", "field", "wizard oz oz oz oz the", "finalScoreFloat", "2.0"));
    assertU(adoc("id", "2", "field", "wizard oz oz oz the the ", "finalScoreFloat", "3.0"));
    assertU(adoc("id", "3", "field", "wizard oz oz the the the the ", "finalScoreFloat", "4.0"));
    assertU(adoc("id", "4", "field", "wizard oz the the the the the the", "finalScoreFloat", "5.0"));
    assertU(commit());

    try (SolrQueryRequest solrQueryRequest = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams())) {
      // Do ordinary BooleanQuery:
      final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
      bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
      bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
      final SolrIndexSearcher searcher = solrQueryRequest.getSearcher();

      // first run the standard query
      TopDocs hits = searcher.search(bqBuilder.build(), 10);
      assertEquals(5, hits.totalHits.value);

      assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
      assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));
      assertEquals("2", searcher.doc(hits.scoreDocs[2].doc).get("id"));
      assertEquals("3", searcher.doc(hits.scoreDocs[3].doc).get("id"));
      assertEquals("4", searcher.doc(hits.scoreDocs[4].doc).get("id"));

      final List<Feature> features = makeFieldValueFeatures(new int[] {0, 1, 2},
              "finalScoreFloat");
      final List<Normalizer> norms =
              new ArrayList<Normalizer>(
                      Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
      final List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0, 1,
              2, 3, 4, 5, 6, 7, 8, 9}, "finalScoreFloat");
      final Double featureWeight = 0.1;
      final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
              features, norms, "test", allFeatures, TestLinearModel.makeFeatureWeights(features, featureWeight));

      LTRScoringQuery scoringQuery = new LTRScoringQuery(ltrScoringModel);
      scoringQuery.setRequest(solrQueryRequest);
      final LTRRescorer rescorer = new LTRRescorer(scoringQuery);

      // rerank @ 0 should not change the order
      hits = rescorer.rescore(searcher, hits, 0);
      assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
      assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));
      assertEquals("2", searcher.doc(hits.scoreDocs[2].doc).get("id"));
      assertEquals("3", searcher.doc(hits.scoreDocs[3].doc).get("id"));
      assertEquals("4", searcher.doc(hits.scoreDocs[4].doc).get("id"));

      // test rerank with different topN cuts

      for (int topN = 1; topN <= 5; topN++) {
        log.info("rerank {} documents ", topN);
        hits = searcher.search(bqBuilder.build(), 10);

        final ScoreDoc[] slice = new ScoreDoc[topN];
        System.arraycopy(hits.scoreDocs, 0, slice, 0, topN);
        hits = new TopDocs(hits.totalHits, slice);
        hits = rescorer.rescore(searcher, hits, topN);
        for (int i = topN - 1, j = 0; i >= 0; i--, j++) {
          if (log.isInfoEnabled()) {
            log.info("doc {} in pos {}", searcher.doc(hits.scoreDocs[j].doc)
                .get("id"), j);
          }
          assertEquals(i,
                  Integer.parseInt(searcher.doc(hits.scoreDocs[j].doc).get("id")));
          assertEquals((i + 1) * features.size()*featureWeight, hits.scoreDocs[j].score, 0.00001);

        }
      }
    }
  }

  @Test
  public void testDocParam() throws Exception {
    try (SolrQueryRequest solrQueryRequest = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams())) {
      List<Feature> features = makeFieldValueFeatures(new int[] {0},
              "finalScore");
      List<Normalizer> norms =
              new ArrayList<Normalizer>(
                      Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
      List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0},
              "finalScore");
      MockModel ltrScoringModel = new MockModel("test",
              features, norms, "test", allFeatures, null);
      LTRScoringQuery query = new LTRScoringQuery(ltrScoringModel);
      query.setRequest(solrQueryRequest);
      LTRScoringQuery.ModelWeight wgt = query.createWeight(null, ScoreMode.COMPLETE, 1f);
      LTRScoringQuery.ModelWeight.ModelScorer modelScr = wgt.scorer(null);
      modelScr.getDocInfo().setOriginalDocScore(1f);
      for (final Scorable.ChildScorable feat : modelScr.getChildren()) {
        assertNotNull(((Feature.FeatureWeight.FeatureScorer) feat.child).getDocInfo().getOriginalDocScore());
      }

      features = makeFieldValueFeatures(new int[] {0, 1, 2}, "finalScore");
      norms =
              new ArrayList<Normalizer>(
                      Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
      allFeatures = makeFieldValueFeatures(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8,
              9}, "finalScore");
      ltrScoringModel = new MockModel("test", features, norms,
              "test", allFeatures, null);
      query = new LTRScoringQuery(ltrScoringModel);
      query.setRequest(solrQueryRequest);
      wgt = query.createWeight(null, ScoreMode.COMPLETE, 1f);
      modelScr = wgt.scorer(null);
      modelScr.getDocInfo().setOriginalDocScore(1f);
      for (final Scorable.ChildScorable feat : modelScr.getChildren()) {
        assertNotNull(((Feature.FeatureWeight.FeatureScorer) feat.child).getDocInfo().getOriginalDocScore());
      }
    }
  }
}
