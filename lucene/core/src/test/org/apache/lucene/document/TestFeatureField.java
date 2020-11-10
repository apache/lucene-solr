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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestFeatureField extends LuceneTestCase {

  /** Round a float value the same way that {@link FeatureField} rounds feature values. */
  private static float round(float f) {
    int bits = Float.floatToIntBits(f);
    bits &= ~0 << 15; // clear last 15 bits
    return Float.intBitsToFloat(bits);
  }

  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    FeatureField pagerank = new FeatureField("features", "pagerank", 1);
    FeatureField urlLength = new FeatureField("features", "urlLen", 1);
    doc.add(pagerank);
    doc.add(urlLength);

    pagerank.setFeatureValue(10);
    urlLength.setFeatureValue(1f / 24);
    writer.addDocument(doc);

    pagerank.setFeatureValue(100);
    urlLength.setFeatureValue(1f / 20);
    writer.addDocument(doc);

    writer.addDocument(new Document()); // gap

    pagerank.setFeatureValue(1);
    urlLength.setFeatureValue(1f / 100);
    writer.addDocument(doc);

    pagerank.setFeatureValue(42);
    urlLength.setFeatureValue(1f / 23);
    writer.addDocument(doc);

    writer.forceMerge(1);
    DirectoryReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    LeafReaderContext context = reader.leaves().get(0);

    Query q = FeatureField.newLogQuery("features", "pagerank", 3f, 4.5f);
    Weight w = q.createWeight(searcher, ScoreMode.TOP_SCORES, 2);
    Scorer s = w.scorer(context);

    assertEquals(0, s.iterator().nextDoc());
    assertEquals((float) (6.0 * Math.log(4.5f + 10)), s.score(), 0f);

    assertEquals(1, s.iterator().nextDoc());
    assertEquals((float) (6.0 * Math.log(4.5f + 100)), s.score(), 0f);

    assertEquals(3, s.iterator().nextDoc());
    assertEquals((float) (6.0 * Math.log(4.5f + 1)), s.score(), 0f);

    assertEquals(4, s.iterator().nextDoc());
    assertEquals((float) (6.0 * Math.log(4.5f + 42)), s.score(), 0f);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, s.iterator().nextDoc());

    q = FeatureField.newLinearQuery("features", "pagerank", 3f);
    w = q.createWeight(searcher, ScoreMode.TOP_SCORES, 2);
    s = w.scorer(context);

    assertEquals(0, s.iterator().nextDoc());
    assertEquals((float) (6.0 * 10), s.score(), 0f);

    assertEquals(1, s.iterator().nextDoc());
    assertEquals((float) (6.0 * 100), s.score(), 0f);

    assertEquals(3, s.iterator().nextDoc());
    assertEquals((float) (6.0 * 1), s.score(), 0f);

    assertEquals(4, s.iterator().nextDoc());
    assertEquals((float) (6.0 * 42), s.score(), 0f);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, s.iterator().nextDoc());

    q = FeatureField.newSaturationQuery("features", "pagerank", 3f, 4.5f);
    w = q.createWeight(searcher, ScoreMode.TOP_SCORES, 2);
    s = w.scorer(context);

    assertEquals(0, s.iterator().nextDoc());
    assertEquals(6f * (1 - 4.5f / (4.5f + 10)), s.score(), 0f);

    assertEquals(1, s.iterator().nextDoc());
    assertEquals(6f * (1 - 4.5f / (4.5f + 100)), s.score(), 0f);

    assertEquals(3, s.iterator().nextDoc());
    assertEquals(6f * (1 - 4.5f / (4.5f + 1)), s.score(), 0f);

    assertEquals(4, s.iterator().nextDoc());
    assertEquals(6f * (1 - 4.5f / (4.5f + 42)), s.score(), 0f);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, s.iterator().nextDoc());

    q = FeatureField.newSigmoidQuery("features", "pagerank", 3f, 4.5f, 0.6f);
    w = q.createWeight(searcher, ScoreMode.TOP_SCORES, 2);
    s = w.scorer(context);
    double kPa = Math.pow(4.5f, 0.6f);

    assertEquals(0, s.iterator().nextDoc());
    assertEquals((float) (6 * (1 - kPa / (kPa + Math.pow(10, 0.6f)))), s.score(), 0f);

    assertEquals(1, s.iterator().nextDoc());
    assertEquals((float) (6 * (1 - kPa / (kPa + Math.pow(100, 0.6f)))), s.score(), 0f);

    assertEquals(3, s.iterator().nextDoc());
    assertEquals((float) (6 * (1 - kPa / (kPa + Math.pow(1, 0.6f)))), s.score(), 0f);

    assertEquals(4, s.iterator().nextDoc());
    assertEquals((float) (6 * (1 - kPa / (kPa + Math.pow(42, 0.6f)))), s.score(), 0f);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, s.iterator().nextDoc());

    q = FeatureField.newSaturationQuery("features", "urlLen", 3f, 1f/24);
    w = q.createWeight(searcher, ScoreMode.TOP_SCORES, 2);
    s = w.scorer(context);

    assertEquals(0, s.iterator().nextDoc());
    assertEquals(6f * (1 - (1f/24) / (1f/24 + round(1f/24))), s.score(), 0f);

    assertEquals(1, s.iterator().nextDoc());
    assertEquals(6f * (1 - 1f/24 / (1f/24 + round(1f/20))), s.score(), 0f);

    assertEquals(3, s.iterator().nextDoc());
    assertEquals(6f * (1 - 1f/24 / (1f/24 + round(1f/100))), s.score(), 0f);

    assertEquals(4, s.iterator().nextDoc());
    assertEquals(6f * (1 - 1f/24 / (1f/24 + round(1f/23))), s.score(), 0f);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, s.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testExplanations() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    FeatureField pagerank = new FeatureField("features", "pagerank", 1);
    doc.add(pagerank);

    pagerank.setFeatureValue(10);
    writer.addDocument(doc);

    pagerank.setFeatureValue(100);
    writer.addDocument(doc);

    writer.addDocument(new Document()); // gap

    pagerank.setFeatureValue(1);
    writer.addDocument(doc);

    pagerank.setFeatureValue(42);
    writer.addDocument(doc);

    DirectoryReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);

    QueryUtils.check(random(), FeatureField.newLogQuery("features", "pagerank", 1f, 4.5f), searcher);
    QueryUtils.check(random(), FeatureField.newLinearQuery("features", "pagerank", 1f), searcher);
    QueryUtils.check(random(), FeatureField.newSaturationQuery("features", "pagerank", 1f, 12f), searcher);
    QueryUtils.check(random(), FeatureField.newSigmoidQuery("features", "pagerank", 1f, 12f, 0.6f), searcher);

    // Test boosts that are > 1
    QueryUtils.check(random(), FeatureField.newLogQuery("features", "pagerank", 3f, 4.5f), searcher);
    QueryUtils.check(random(), FeatureField.newLinearQuery("features", "pagerank", 3f), searcher);
    QueryUtils.check(random(), FeatureField.newSaturationQuery("features", "pagerank", 3f, 12f), searcher);
    QueryUtils.check(random(), FeatureField.newSigmoidQuery("features", "pagerank", 3f, 12f, 0.6f), searcher);

    // Test boosts that are < 1
    QueryUtils.check(random(), FeatureField.newLogQuery("features", "pagerank", .2f, 4.5f), searcher);
    QueryUtils.check(random(), FeatureField.newLinearQuery("features", "pagerank", .2f), searcher);
    QueryUtils.check(random(), FeatureField.newSaturationQuery("features", "pagerank", .2f, 12f), searcher);
    QueryUtils.check(random(), FeatureField.newSigmoidQuery("features", "pagerank", .2f, 12f, 0.6f), searcher);

    reader.close();
    dir.close();
  }

  public void testLogSimScorer() {
    doTestSimScorer(new FeatureField.LogFunction(4.5f).scorer(3f));
  }

  public void testLinearSimScorer() {
    doTestSimScorer(new FeatureField.LinearFunction().scorer(1f));
  }

  public void testSatuSimScorer() {
    doTestSimScorer(new FeatureField.SaturationFunction("foo", "bar", 20f).scorer(3f));
  }

  public void testSigmSimScorer() {
    doTestSimScorer(new FeatureField.SigmoidFunction(20f, 0.6f).scorer(3f));
  }

  private void doTestSimScorer(SimScorer s) {
    float maxScore = s.score(Float.MAX_VALUE, 1);
    assertTrue(Float.isFinite(maxScore)); // used to compute max scores
    // Test that the score doesn't decrease with freq
    for (int freq = 2; freq < 65536; ++freq) {
      assertTrue(s.score(freq - 1, 1L) <= s.score(freq, 1L));
    }
    assertTrue(s.score(65535, 1L) <= maxScore);
  }

  public void testComputePivotFeatureValue() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());

    // Make sure that we create a legal pivot on missing features
    DirectoryReader reader = writer.getReader();
    float pivot = FeatureField.computePivotFeatureValue(reader, "features", "pagerank");
    assertTrue(Float.isFinite(pivot));
    assertTrue(pivot > 0);
    reader.close();

    Document doc = new Document();
    FeatureField pagerank = new FeatureField("features", "pagerank", 1);
    doc.add(pagerank);

    pagerank.setFeatureValue(10);
    writer.addDocument(doc);

    pagerank.setFeatureValue(100);
    writer.addDocument(doc);

    writer.addDocument(new Document()); // gap

    pagerank.setFeatureValue(1);
    writer.addDocument(doc);

    pagerank.setFeatureValue(42);
    writer.addDocument(doc);

    reader = writer.getReader();
    writer.close();

    pivot = FeatureField.computePivotFeatureValue(reader, "features", "pagerank");
    double expected = Math.pow(10 * 100 * 1 * 42, 1/4.); // geometric mean
    assertEquals(expected, pivot, 0.1);

    reader.close();
    dir.close();
  }

  public void testExtractTerms() throws IOException {
    IndexReader reader = new MultiReader();
    IndexSearcher searcher = newSearcher(reader);
    Query query = FeatureField.newLogQuery("field", "term", 2f, 42);

    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
    Set<Term> terms = new HashSet<>();
    weight.extractTerms(terms);
    assertEquals(Collections.emptySet(), terms);

    terms = new HashSet<>();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    weight.extractTerms(terms);
    assertEquals(Collections.singleton(new Term("field", "term")), terms);

    terms = new HashSet<>();
    weight = searcher.createWeight(query, ScoreMode.TOP_SCORES, 1f);
    weight.extractTerms(terms);
    assertEquals(Collections.singleton(new Term("field", "term")), terms);
  }

  public void testDemo() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    FeatureField pagerank = new FeatureField("features", "pagerank", 1);
    doc.add(pagerank);
    TextField body = new TextField("body", "", Store.NO);
    doc.add(body);

    pagerank.setFeatureValue(10);
    body.setStringValue("Apache Lucene");
    writer.addDocument(doc);

    pagerank.setFeatureValue(1000);
    body.setStringValue("Apache Web HTTP server");
    writer.addDocument(doc);

    pagerank.setFeatureValue(1);
    body.setStringValue("Lucene is a search engine");
    writer.addDocument(doc);

    pagerank.setFeatureValue(42);
    body.setStringValue("Lucene in the sky with diamonds");
    writer.addDocument(doc);

    DirectoryReader reader = writer.getReader();
    writer.close();

    // NOTE: If you need to make changes below, then you likely also need to
    // update javadocs of FeatureField.

    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new BM25Similarity());
    Query query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "apache")), Occur.SHOULD)
        .add(new TermQuery(new Term("body", "lucene")), Occur.SHOULD)
        .build();
    Query boost = FeatureField.newSaturationQuery("features", "pagerank");
    Query boostedQuery = new BooleanQuery.Builder()
        .add(query, Occur.MUST)
        .add(boost, Occur.SHOULD)
        .build();
    TopDocs topDocs = searcher.search(boostedQuery, 10);
    assertEquals(4, topDocs.scoreDocs.length);
    assertEquals(1, topDocs.scoreDocs[0].doc);
    assertEquals(0, topDocs.scoreDocs[1].doc);
    assertEquals(3, topDocs.scoreDocs[2].doc);
    assertEquals(2, topDocs.scoreDocs[3].doc);

    reader.close();
    dir.close();
  }

}
