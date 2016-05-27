package org.apache.solr.ltr.ranking;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.ltr.feature.ModelMetadata;
import org.apache.solr.ltr.feature.impl.FieldValueFeature;
import org.apache.solr.ltr.ranking.ModelQuery.ModelWeight;
import org.apache.solr.ltr.ranking.ModelQuery.ModelWeight.ModelScorer;
import org.apache.solr.ltr.util.NamedParams;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressCodecs("Lucene3x")
public class TestReRankingPipeline extends LuceneTestCase {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private IndexSearcher getSearcher(IndexReader r) {
    IndexSearcher searcher = newSearcher(r);

    return searcher;
  }

  private static List<Feature> makeFieldValueFeatures(int[] featureIds,
      String field) {
    List<Feature> features = new ArrayList<>();
    for (int i : featureIds) {
      FieldValueFeature f = new FieldValueFeature();
      f.name = "f" + i;
      f.params = new NamedParams().add("field", field);
      features.add(f);
    }
    return features;
  }

  private class MockModel extends ModelMetadata {

    public MockModel(String name, String type, List<Feature> features,
        String featureStoreName, Collection<Feature> allFeatures,
        NamedParams params) {
      super(name, type, features, featureStoreName, allFeatures, params);
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
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

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

    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    Builder bqBuilder = new Builder();
    bqBuilder.add(new TermQuery(new Term("field", "wizard")), Occur.SHOULD);
    bqBuilder.add(new TermQuery(new Term("field", "oz")), Occur.SHOULD);
    IndexSearcher searcher = getSearcher(r);
    // first run the standard query
    TopDocs hits = searcher.search(bqBuilder.build(), 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    List<Feature> features = makeFieldValueFeatures(new int[] {0, 1, 2},
        "final-score");
    List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0, 1, 2, 3,
        4, 5, 6, 7, 8, 9}, "final-score");
    RankSVMModel meta = new RankSVMModel("test",
        MockModel.class.getCanonicalName(), features, "test", allFeatures, null);

    LTRRescorer rescorer = new LTRRescorer(new ModelQuery(meta));
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
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

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

    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    Builder bqBuilder = new Builder();
    bqBuilder.add(new TermQuery(new Term("field", "wizard")), Occur.SHOULD);
    bqBuilder.add(new TermQuery(new Term("field", "oz")), Occur.SHOULD);
    IndexSearcher searcher = getSearcher(r);

    // first run the standard query
    TopDocs hits = searcher.search(bqBuilder.build(), 10);
    assertEquals(5, hits.totalHits);
    for (int i = 0; i < 5; i++) {
      System.out.print(hits.scoreDocs[i].doc + " -> ");
      System.out.println(searcher.doc(hits.scoreDocs[i].doc).get("id"));
    }

    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(hits.scoreDocs[2].doc).get("id"));
    assertEquals("3", searcher.doc(hits.scoreDocs[3].doc).get("id"));
    assertEquals("4", searcher.doc(hits.scoreDocs[4].doc).get("id"));

    List<Feature> features = makeFieldValueFeatures(new int[] {0, 1, 2},
        "final-score");
    List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0, 1, 2, 3,
        4, 5, 6, 7, 8, 9}, "final-score");
    RankSVMModel meta = new RankSVMModel("test",
        MockModel.class.getCanonicalName(), features, "test", allFeatures, null);

    LTRRescorer rescorer = new LTRRescorer(new ModelQuery(meta));

    // rerank @ 0 should not change the order
    hits = rescorer.rescore(searcher, hits, 0);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.doc(hits.scoreDocs[2].doc).get("id"));
    assertEquals("3", searcher.doc(hits.scoreDocs[3].doc).get("id"));
    assertEquals("4", searcher.doc(hits.scoreDocs[4].doc).get("id"));

    // test rerank with different topN cuts

    for (int topN = 1; topN <= 5; topN++) {
      logger.info("rerank {} documents ", topN);
      hits = searcher.search(bqBuilder.build(), 10);
      // meta = new MockModel();
      // rescorer = new LTRRescorer(new ModelQuery(meta));
      ScoreDoc[] slice = new ScoreDoc[topN];
      System.arraycopy(hits.scoreDocs, 0, slice, 0, topN);
      hits = new TopDocs(hits.totalHits, slice, hits.getMaxScore());
      hits = rescorer.rescore(searcher, hits, topN);
      for (int i = topN - 1, j = 0; i >= 0; i--, j++) {
        logger.info("doc {} in pos {}", searcher.doc(hits.scoreDocs[j].doc)
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
    NamedParams test = new NamedParams();
    test.add("fake", 2);
    List<Feature> features = makeFieldValueFeatures(new int[] {0},
        "final-score");
    List<Feature> allFeatures = makeFieldValueFeatures(new int[] {0},
        "final-score");
    MockModel meta = new MockModel("test", MockModel.class.getCanonicalName(),
        features, "test", allFeatures, null);
    ModelQuery query = new ModelQuery(meta);
    ModelWeight wgt = query.createWeight(null, true);
    ModelScorer modelScr = wgt.scorer(null);
    modelScr.setDocInfoParam("ORIGINAL_SCORE", 1);
    for (ChildScorer feat : modelScr.getChildren()) {
      assert (((FeatureScorer) feat.child).hasDocParam("ORIGINAL_SCORE"));
    }

    features = makeFieldValueFeatures(new int[] {0, 1, 2}, "final-score");
    allFeatures = makeFieldValueFeatures(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8,
        9}, "final-score");
    meta = new MockModel("test", MockModel.class.getCanonicalName(), features,
        "test", allFeatures, null);
    query = new ModelQuery(meta);
    wgt = query.createWeight(null, true);
    modelScr = wgt.scorer(null);
    modelScr.setDocInfoParam("ORIGINAL_SCORE", 1);
    for (ChildScorer feat : modelScr.getChildren()) {
      assert (((FeatureScorer) feat.child).hasDocParam("ORIGINAL_SCORE"));
    }
  }

}
