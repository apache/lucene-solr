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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.ltr.feature.impl.ValueFeature;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.ltr.util.NamedParams;
import org.junit.Test;

@SuppressCodecs("Lucene3x")
public class TestModelQuery extends LuceneTestCase {

  private IndexSearcher getSearcher(IndexReader r) {
    IndexSearcher searcher = newSearcher(r, false, false);
    return searcher;
  }

  private static List<Feature> makeFeatures(int[] featureIds) {
    List<Feature> features = new ArrayList<>();
    for (int i : featureIds) {
      ValueFeature f = new ValueFeature();
      try {
        f.init("f" + i, new NamedParams().add("value", i), i);
      } catch (FeatureException e) {
        e.printStackTrace();
      }
      features.add(f);
    }
    return features;
  }

  private static List<Feature> makeNormalizedFeatures(int[] featureIds) {
    List<Feature> features = new ArrayList<>();
    for (int i : featureIds) {
      ValueFeature f = new ValueFeature();
      f.name = "f" + i;
      f.params = new NamedParams().add("value", i);
      f.id = i;
      f.norm = new Normalizer() {

        @Override
        public float normalize(float value) {
          return 42.42f;
        }
      };
      features.add(f);
    }
    return features;
  }

  private static NamedParams makeFeatureWeights(List<Feature> features) {
    NamedParams nameParams = new NamedParams();
    HashMap<String,Double> modelWeights = new HashMap<String,Double>();
    for (Feature feat : features) {
      modelWeights.put(feat.name, 0.1);
    }
    if (modelWeights.isEmpty()) modelWeights.put("", 0.0);
    nameParams.add("weights", modelWeights);
    return nameParams;
  }

  private ModelQuery.ModelWeight performQuery(TopDocs hits,
      IndexSearcher searcher, int docid, ModelQuery model) throws IOException,
      ModelException {
    List<LeafReaderContext> leafContexts = searcher.getTopReaderContext()
        .leaves();
    int n = ReaderUtil.subIndex(hits.scoreDocs[0].doc, leafContexts);
    final LeafReaderContext context = leafContexts.get(n);
    int deBasedDoc = hits.scoreDocs[0].doc - context.docBase;

    Weight weight = searcher.createNormalizedWeight(model, true);
    Scorer scorer = weight.scorer(context);

    // rerank using the field final-score
    scorer.iterator().advance(deBasedDoc);
    float score = scorer.score();

    // assertEquals(42.0f, score, 0.0001);
    // assertTrue(weight instanceof AssertingWeight);
    // (AssertingIndexSearcher)
    assertTrue(weight instanceof ModelQuery.ModelWeight);
    ModelQuery.ModelWeight modelWeight = (ModelQuery.ModelWeight) weight;
    return modelWeight;

  }

  @Test
  public void testModelQuery() throws IOException, ModelException {
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

    List<Feature> features = makeFeatures(new int[] {0, 1, 2});
    List<Feature> allFeatures = makeFeatures(new int[] {0, 1, 2, 3, 4, 5, 6, 7,
        8, 9});
    RankSVMModel meta = new RankSVMModel("test",
        RankSVMModel.class.getCanonicalName(), features, "test", allFeatures,
        makeFeatureWeights(features));

    ModelQuery.ModelWeight modelWeight = performQuery(hits, searcher,
        hits.scoreDocs[0].doc, new ModelQuery(meta));
    assertEquals(3, modelWeight.modelFeatureValuesNormalized.length);
    assertEquals(10, modelWeight.allFeatureValues.length);

    for (int i = 0; i < 3; i++) {
      assertEquals((float) i, modelWeight.modelFeatureValuesNormalized[i],
          0.0001);
    }
    for (int i = 0; i < 10; i++) {
      assertEquals((float) i, modelWeight.allFeatureValues[i], 0.0001);
    }

    for (int i = 0; i < 10; i++) {
      assertEquals("f" + i, modelWeight.allFeatureNames[i]);

    }

    int[] mixPositions = new int[] {8, 2, 4, 9, 0};
    features = makeFeatures(mixPositions);
    meta = new RankSVMModel("test", RankSVMModel.class.getCanonicalName(),
        features, "test", allFeatures, makeFeatureWeights(features));

    modelWeight = performQuery(hits, searcher, hits.scoreDocs[0].doc,
        new ModelQuery(meta));
    assertEquals(mixPositions.length,
        modelWeight.modelFeatureValuesNormalized.length);

    for (int i = 0; i < mixPositions.length; i++) {
      assertEquals((float) mixPositions[i],
          modelWeight.modelFeatureValuesNormalized[i], 0.0001);
    }
    for (int i = 0; i < 10; i++) {
      assertEquals((float) i, modelWeight.allFeatureValues[i], 0.0001);
    }

    int[] noPositions = new int[] {};
    features = makeFeatures(noPositions);
    meta = new RankSVMModel("test", RankSVMModel.class.getCanonicalName(),
        features, "test", allFeatures, makeFeatureWeights(features));

    modelWeight = performQuery(hits, searcher, hits.scoreDocs[0].doc,
        new ModelQuery(meta));
    assertEquals(0, modelWeight.modelFeatureValuesNormalized.length);

    // test normalizers
    features = makeNormalizedFeatures(mixPositions);
    RankSVMModel normMeta = new RankSVMModel("test",
        RankSVMModel.class.getCanonicalName(), features, "test", allFeatures,
        makeFeatureWeights(features));

    modelWeight = performQuery(hits, searcher, hits.scoreDocs[0].doc,
        new ModelQuery(normMeta));
    assertEquals(mixPositions.length,
        modelWeight.modelFeatureValuesNormalized.length);
    for (int i = 0; i < mixPositions.length; i++) {
      assertEquals(42.42f, modelWeight.modelFeatureValuesNormalized[i], 0.0001);
    }
    for (int i = 0; i < 10; i++) {
      assertEquals((float) i, modelWeight.allFeatureValues[i], 0.0001);
    }
    r.close();
    dir.close();

  }

}
