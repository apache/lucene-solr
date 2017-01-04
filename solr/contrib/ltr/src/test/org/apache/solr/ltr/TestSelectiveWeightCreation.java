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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.ModelException;
import org.apache.solr.ltr.model.TestLinearModel;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSelectiveWeightCreation extends TestRerankBase {
  private IndexSearcher getSearcher(IndexReader r) {
    final IndexSearcher searcher = newSearcher(r, false, false);
    return searcher;
  }

  private static List<Feature> makeFeatures(int[] featureIds) {
    final List<Feature> features = new ArrayList<>();
    for (final int i : featureIds) {
      Map<String,Object> params = new HashMap<String,Object>();
      params.put("value", i);
      final Feature f = Feature.getInstance(solrResourceLoader,
          ValueFeature.class.getCanonicalName(),
          "f" + i, params);
      f.setIndex(i);
      features.add(f);
    }
    return features;
  }

  private static Map<String,Object> makeFeatureWeights(List<Feature> features) {
    final Map<String,Object> nameParams = new HashMap<String,Object>();
    final HashMap<String,Double> modelWeights = new HashMap<String,Double>();
    for (final Feature feat : features) {
      modelWeights.put(feat.getName(), 0.1);
    }
    nameParams.put("weights", modelWeights);
    return nameParams;
  }

  private LTRScoringQuery.ModelWeight performQuery(TopDocs hits,
      IndexSearcher searcher, int docid, LTRScoringQuery model) throws IOException,
      ModelException {
    final List<LeafReaderContext> leafContexts = searcher.getTopReaderContext()
        .leaves();
    final int n = ReaderUtil.subIndex(hits.scoreDocs[0].doc, leafContexts);
    final LeafReaderContext context = leafContexts.get(n);
    final int deBasedDoc = hits.scoreDocs[0].doc - context.docBase;

    final Weight weight = searcher.createNormalizedWeight(model, true);
    final Scorer scorer = weight.scorer(context);

    // rerank using the field final-score
    scorer.iterator().advance(deBasedDoc);
    scorer.score();
    assertTrue(weight instanceof LTRScoringQuery.ModelWeight);
    final LTRScoringQuery.ModelWeight modelWeight = (LTRScoringQuery.ModelWeight) weight;
    return modelWeight;

  }


  @BeforeClass
  public static void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1 w3", "description", "w1", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity",
        "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w4 w3", "description", "w4", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(commit());

    loadFeatures("external_features.json");
    loadModels("external_model.json");
    loadModels("external_model_store.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testScoringQueryWeightCreation() throws IOException, ModelException {
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
    final TopDocs hits = searcher.search(bqBuilder.build(), 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    List<Feature> features = makeFeatures(new int[] {0, 1, 2});
    final List<Feature> allFeatures = makeFeatures(new int[] {0, 1, 2, 3, 4, 5,
        6, 7, 8, 9});
    final List<Normalizer> norms = new ArrayList<>();
    for (int k=0; k < features.size(); ++k){
        norms.add(IdentityNormalizer.INSTANCE);
    }

    // when features are NOT requested in the response, only the modelFeature weights should be created
    final LTRScoringModel ltrScoringModel1 = TestLinearModel.createLinearModel("test",
        features, norms, "test", allFeatures,
        makeFeatureWeights(features));
    LTRScoringQuery.ModelWeight modelWeight = performQuery(hits, searcher,
        hits.scoreDocs[0].doc, new LTRScoringQuery(ltrScoringModel1, false)); // features not requested in response
    LTRScoringQuery.FeatureInfo[] featuresInfo = modelWeight.getFeaturesInfo();

    assertEquals(features.size(), modelWeight.getModelFeatureValuesNormalized().length);
    int validFeatures = 0;
    for (int i=0; i < featuresInfo.length; ++i){
      if (featuresInfo[i] != null && featuresInfo[i].isUsed()){
        validFeatures += 1;
      }
    }
    assertEquals(validFeatures, features.size());

    // when features are requested in the response, weights should be created for all features
    final LTRScoringModel ltrScoringModel2 = TestLinearModel.createLinearModel("test",
        features, norms, "test", allFeatures,
        makeFeatureWeights(features));
    modelWeight = performQuery(hits, searcher,
        hits.scoreDocs[0].doc, new LTRScoringQuery(ltrScoringModel2, true)); // features requested in response
    featuresInfo = modelWeight.getFeaturesInfo();

    assertEquals(features.size(), modelWeight.getModelFeatureValuesNormalized().length);
    assertEquals(allFeatures.size(), modelWeight.getExtractedFeatureWeights().length);

    validFeatures = 0;
    for (int i=0; i < featuresInfo.length; ++i){
      if (featuresInfo[i] != null && featuresInfo[i].isUsed()){
        validFeatures += 1;
      }
    }
    assertEquals(validFeatures, allFeatures.size());

    assertU(delI("0"));assertU(delI("1"));
    r.close();
    dir.close();
  }


  @Test
  public void testSelectiveWeightsRequestFeaturesFromDifferentStore() throws Exception {

    final String docs0fv = FeatureLoggerTestUtils.toFeatureVector(
        "matchedTitle","1.0", "titlePhraseMatch","0.40254828");
    final String docs0fv_fstore4= FeatureLoggerTestUtils.toFeatureVector(
        "popularity","3.0", "originalScore","1.0");

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score");
    query.add("rows", "4");
    query.add("rq", "{!ltr reRankDocs=4 model=externalmodel efi.user_query=w3}");
    query.add("fl", "fv:[fv]");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='4'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv=='"+docs0fv+"'"); // extract all features in default store

    query.remove("fl");
    query.remove("rq");
    query.add("fl", "*,score");
    query.add("rq", "{!ltr reRankDocs=4 model=externalmodel efi.user_query=w3}");
    query.add("fl", "fv:[fv store=fstore4 efi.myPop=3]");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.999");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv=='"+docs0fv_fstore4+"'"); // extract all features from fstore4


    query.remove("fl");
    query.remove("rq");
    query.add("fl", "*,score");
    query.add("rq", "{!ltr reRankDocs=4 model=externalmodelstore efi.user_query=w3 efi.myconf=0.8}");
    query.add("fl", "fv:[fv store=fstore4 efi.myPop=3]");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'"); // score using fstore2 used by externalmodelstore
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.7992");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv=='"+docs0fv_fstore4+"'"); // extract all features from fstore4
  }
}

