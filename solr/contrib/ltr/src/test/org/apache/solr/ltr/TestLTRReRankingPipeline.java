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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.TestLinearModel;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLTRReRankingPipeline extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

  private IndexSearcher getSearcher(IndexReader r) {
    final IndexSearcher searcher = newSearcher(r);

    return searcher;
  }

  private static List<Feature> makeFieldValueFeatures(int[] featureIds,
      String field) {
    final List<Feature> features = new ArrayList<>();
    for (final int i : featureIds) {
      final Map<String,Object> params = new HashMap<String,Object>();
      params.put("field", field);
      final Feature f = Feature.getInstance(solrResourceLoader,
          FieldValueFeature.class.getCanonicalName(),
          "f" + i, params);
      f.setIndex(i);
      features.add(f);
    }
    return features;
  }

  private class MockModel extends LTRScoringModel {

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

  @Ignore
  @Test
  public void testRescorer() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz",
        Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 1.0f));

    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the",
        Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 2.0f));
    w.addDocument(doc);

    final IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
    bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
    final IndexSearcher searcher = getSearcher(r);
    // first run the standard query
    TopDocs hits = searcher.search(bqBuilder.build(), 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    final List<Feature> features = makeFieldValueFeatures(new int[] {0, 1, 2},
        "final-score");
    final List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    final List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0, 1,
        2, 3, 4, 5, 6, 7, 8, 9}, "final-score");
    final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
        features, norms, "test", allFeatures, null);

    final LTRRescorer rescorer = new LTRRescorer(new LTRScoringQuery(ltrScoringModel));
    hits = rescorer.rescore(searcher, hits, 2);

    // rerank using the field final-score
    assertEquals("1", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    r.close();
    dir.close();

  }

  @Ignore
  @Test
  public void testDifferentTopN() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard oz oz oz oz oz", Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 1.0f));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    doc.add(newTextField("field", "wizard oz oz oz oz the", Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 2.0f));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    doc.add(newTextField("field", "wizard oz oz oz the the ", Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 3.0f));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    doc.add(newTextField("field", "wizard oz oz the the the the ",
        Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 4.0f));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "4", Field.Store.YES));
    doc.add(newTextField("field", "wizard oz the the the the the the",
        Field.Store.NO));
    doc.add(new FloatDocValuesField("final-score", 5.0f));
    w.addDocument(doc);

    final IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
    bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
    final IndexSearcher searcher = getSearcher(r);

    // first run the standard query
    TopDocs hits = searcher.search(bqBuilder.build(), 10);
    assertEquals(5, hits.totalHits);

    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(hits.scoreDocs[2].doc).get("id"));
    assertEquals("3", searcher.doc(hits.scoreDocs[3].doc).get("id"));
    assertEquals("4", searcher.doc(hits.scoreDocs[4].doc).get("id"));

    final List<Feature> features = makeFieldValueFeatures(new int[] {0, 1, 2},
        "final-score");
    final List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    final List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0, 1,
        2, 3, 4, 5, 6, 7, 8, 9}, "final-score");
    final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
        features, norms, "test", allFeatures, null);

    final LTRRescorer rescorer = new LTRRescorer(new LTRScoringQuery(ltrScoringModel));

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
      hits = new TopDocs(hits.totalHits, slice, hits.getMaxScore());
      hits = rescorer.rescore(searcher, hits, topN);
      for (int i = topN - 1, j = 0; i >= 0; i--, j++) {
        log.info("doc {} in pos {}", searcher.doc(hits.scoreDocs[j].doc)
            .get("id"), j);

        assertEquals(i,
            Integer.parseInt(searcher.doc(hits.scoreDocs[j].doc).get("id")));
        assertEquals(i + 1, hits.scoreDocs[j].score, 0.00001);

      }
    }

    r.close();
    dir.close();

  }

  @Test
  public void testDocParam() throws Exception {
    final Map<String,Object> test = new HashMap<String,Object>();
    test.put("fake", 2);
    List<Feature> features = makeFieldValueFeatures(new int[] {0},
        "final-score");
    List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0},
        "final-score");
    MockModel ltrScoringModel = new MockModel("test",
        features, norms, "test", allFeatures, null);
    LTRScoringQuery query = new LTRScoringQuery(ltrScoringModel);
    LTRScoringQuery.ModelWeight wgt = query.createWeight(null, true, 1f);
    LTRScoringQuery.ModelWeight.ModelScorer modelScr = wgt.scorer(null);
    modelScr.getDocInfo().setOriginalDocScore(new Float(1f));
    for (final Scorer.ChildScorer feat : modelScr.getChildren()) {
      assertNotNull(((Feature.FeatureWeight.FeatureScorer) feat.child).getDocInfo().getOriginalDocScore());
    }

    features = makeFieldValueFeatures(new int[] {0, 1, 2}, "final-score");
    norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    allFeatures = makeFieldValueFeatures(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8,
        9}, "final-score");
    ltrScoringModel = new MockModel("test", features, norms,
        "test", allFeatures, null);
    query = new LTRScoringQuery(ltrScoringModel);
    wgt = query.createWeight(null, true, 1f);
    modelScr = wgt.scorer(null);
    modelScr.getDocInfo().setOriginalDocScore(new Float(1f));
    for (final Scorer.ChildScorer feat : modelScr.getChildren()) {
      assertNotNull(((Feature.FeatureWeight.FeatureScorer) feat.child).getDocInfo().getOriginalDocScore());
    }
  }

}
