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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.TestLinearModel;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


public class TestLTRExitingReaderException extends SolrTestCase {

  private Directory directory1;
  private Directory directory2;

  private IndexReader reader;

  private Query query;
  private LTRScoringQuery ltrScoringQuery;

  static boolean scorerExitingReaderException = false;
  static boolean scoreExitingReaderException = false;
  static int leafTotal = 0;

  /**
   * initializes
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();

    directory1 = newDirectory();
    final RandomIndexWriter w1 = new RandomIndexWriter(random(), directory1);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz",
        Field.Store.NO));
    doc.add(new StoredField("final-score", 1));
    w1.addDocument(doc);

    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard the the oz the the the the",
        Field.Store.NO));
    doc.add(new StoredField("final-score", 2));
    w1.addDocument(doc);


    directory2 = newDirectory();
    final RandomIndexWriter w2 = new RandomIndexWriter(random(), directory2);

    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "oz wizard the the the the the the",
        Field.Store.NO));
    doc.add(new StoredField("final-score", 3));
    w2.addDocument(doc);

//        reader = w1.getReader();

    reader = new MultiReader(w1.getReader(), w2.getReader());

    w1.close();
    w2.close();


    // Do ordinary BooleanQuery:
    final BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    bqBuilder.add(new TermQuery(new Term("field", "wizard")), BooleanClause.Occur.SHOULD);
    bqBuilder.add(new TermQuery(new Term("field", "oz")), BooleanClause.Occur.SHOULD);
    query = bqBuilder.build();

    final List<Feature> features = makeFieldValueFeatures(new int[]{0, 1, 2},
        "final-score");
    final List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(), IdentityNormalizer.INSTANCE));
    final List<Feature> allFeatures = makeFieldValueFeatures(new int[]{0, 1,
        2, 3, 4, 5, 6, 7, 8, 9}, "final-score");
    final LTRScoringModel ltrScoringModel = TestLinearModel.createLinearModel("test",
        features, norms, "test", allFeatures, TestLinearModel.makeFeatureWeights(features));

    ltrScoringQuery = new LTRScoringQuery(ltrScoringModel);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory1.close();
    directory2.close();
    super.tearDown();
  }

  private IndexSearcher getSearcher(IndexReader r) {
    final IndexSearcher searcher = newSearcher(r, false, false);
    return searcher;
  }

  private static List<Feature> makeFieldValueFeatures(int[] featureIds,
                                                      String field) {
    final List<Feature> features = new ArrayList<>();
    for (final int i : featureIds) {
      final Map<String, Object> params = new HashMap<String, Object>();
      params.put("field", field);
      final Feature f = new FieldValueFeature("f" + i, params, field);
      f.setIndex(i);
      features.add(f);
    }
    return features;
  }


  @Test
  public void testRescorer() throws IOException {
    scorerExitingReaderException = false;
    scoreExitingReaderException = false;

    IndexSearcher searcher = getSearcher(reader);
    TopDocs hits = searcher.search(query, 10);

    final LTRRescorer rescorer = new LTRRescorer(ltrScoringQuery);
    hits = rescorer.rescore(searcher, hits, 3);

    // rerank using the field final-score
    assertEquals("2", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals("0", searcher.doc(hits.scoreDocs[2].doc).get("id"));
  }

  @Test
  public void testRescorerPartialScore() throws IOException {
    scorerExitingReaderException = false;
    scoreExitingReaderException = true;

    IndexSearcher searcher = getSearcher(reader);
    TopDocs hits = searcher.search(query, 10);

    final LTRRescorer rescorer = new LTRRescorer(ltrScoringQuery);
    hits = rescorer.rescore(searcher, hits, 3);

    // rerank using the field final-score
    assertEquals("1", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits.scoreDocs[1].doc).get("id"));
  }

  @Test
  public void testRescorerPartialScorer() throws IOException {
    scorerExitingReaderException = true;
    scoreExitingReaderException = false;

    IndexSearcher searcher = getSearcher(reader);
    TopDocs hits = searcher.search(query, 10);

    final LTRRescorer rescorer = new LTRRescorer(ltrScoringQuery);
    hits = rescorer.rescore(searcher, hits, 3);

    // rerank using the field final-score
    assertEquals("1", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits.scoreDocs[1].doc).get("id"));
  }

  public static class FieldValueFeature extends Feature {

    private String field;

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    @Override
    public LinkedHashMap<String, Object> paramsToMap() {
      final LinkedHashMap<String, Object> params = defaultParamsToMap();
      params.put("field", field);
      return params;
    }

    @Override
    protected void validate() throws FeatureException {
    }

    public FieldValueFeature(String name, Map<String, Object> params, String field) {
      super(name, params);
      this.field = field;
    }

    @Override
    public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
                                      SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi)
        throws IOException {
      return new FieldValueFeatureWeight(searcher, request, originalQuery, efi);
    }

    public class FieldValueFeatureWeight extends FeatureWeight {

      public FieldValueFeatureWeight(IndexSearcher searcher,
                                     SolrQueryRequest request, Query originalQuery, Map<String, String[]> efi) {
        super(FieldValueFeature.this, searcher, request, originalQuery, efi);
      }

      @Override
      public FeatureScorer scorer(LeafReaderContext context) throws IOException {
        leafTotal++;
        // An ExitingReaderException occurs during the mock scorer process
        // This usually happens when the term is loaded by the LeafReaderContext loop
        if (leafTotal > 3 && scorerExitingReaderException) {
          throw new ExitableDirectoryReader.ExitingReaderException("The request took too long to iterate over doc values");
        }
        return new FieldValueFeatureScorer(this, context,
            DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
      }

      public class FieldValueFeatureScorer extends FeatureScorer {
        LeafReaderContext context = null;

        public FieldValueFeatureScorer(FeatureWeight weight,
                                       LeafReaderContext context, DocIdSetIterator itr) {
          super(weight, itr);
          this.context = context;
        }

        @Override
        public float score() throws IOException {
          final Document document = context.reader().document(itr.docID());
          final IndexableField indexableField = document.getField(field);
          if (indexableField == null) {
            return getDefaultValue();
          }
          final Number number = indexableField.numericValue();
          if (number != null) {
            // An ExitingReaderException occurs during the mock score process
            // The function calculation loads the term during the Score procedure, causing an ExitingReaderException to occur
            if (number.floatValue() == 3 && scoreExitingReaderException) {
              throw new ExitableDirectoryReader.ExitingReaderException("The request took too long to iterate over doc values");
            }
            return number.floatValue();
          }
          return getDefaultValue();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
          return Float.POSITIVE_INFINITY;
        }
      }
    }
  }

}
