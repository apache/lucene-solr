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
package org.apache.lucene.queries.function;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.NormValueSource;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestLongNormValueSource extends LuceneTestCase {

  static Directory dir;
  static IndexReader reader;
  static IndexSearcher searcher;
  static Analyzer analyzer;
  
  private static Similarity sim = new PreciseClassicSimilarity();

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwConfig = newIndexWriterConfig(analyzer);
    iwConfig.setMergePolicy(newLogMergePolicy());
    iwConfig.setSimilarity(sim);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);

    Document doc = new Document();
    doc.add(new TextField("text", "this is a test test test", Field.Store.NO));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new TextField("text", "second test", Field.Store.NO));
    iw.addDocument(doc);

    reader = iw.getReader();
    searcher = newSearcher(reader);
    iw.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    analyzer.close();
    analyzer = null;
  }

  public void testNorm() throws Exception {
    Similarity saved = searcher.getSimilarity(true);
    try {
      // no norm field (so agnostic to indexed similarity)
      searcher.setSimilarity(sim);
      assertHits(new FunctionQuery(
          new NormValueSource("text")),
          new float[] { 0f, 0f });
    } finally {
      searcher.setSimilarity(saved);
    }
  }

  void assertHits(Query q, float scores[]) throws Exception {
    ScoreDoc expected[] = new ScoreDoc[scores.length];
    int expectedDocs[] = new int[scores.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = i;
      expected[i] = new ScoreDoc(i, scores[i]);
    }
    TopDocs docs = searcher.search(q, 2, new Sort(new SortField("id", SortField.Type.STRING)));

    /*
    for (int i=0;i<docs.scoreDocs.length;i++) {
      System.out.println(searcher.explain(q, docs.scoreDocs[i].doc));
    }
    */

    CheckHits.checkHits(random(), q, "", searcher, expectedDocs);
    CheckHits.checkHitsQuery(q, expected, docs.scoreDocs, expectedDocs);
    CheckHits.checkExplanations(q, "", searcher);
  }
}


/** Encodes norm as 4-byte float. */
class PreciseClassicSimilarity extends TFIDFSimilarity {

  /** Sole constructor: parameter-free */
  public PreciseClassicSimilarity() {}

  /** Implemented as <code>overlap / maxOverlap</code>. */
  @Override
  public float coord(int overlap, int maxOverlap) {
    return overlap / (float)maxOverlap;
  }

  /** Implemented as <code>1/sqrt(sumOfSquaredWeights)</code>. */
  @Override
  public float queryNorm(float sumOfSquaredWeights) {
    return (float)(1.0 / Math.sqrt(sumOfSquaredWeights));
  }

  /**
   * Encodes a normalization factor for storage in an index.
   * <p>
   * The encoding uses a three-bit mantissa, a five-bit exponent, and the
   * zero-exponent point at 15, thus representing values from around 7x10^9 to
   * 2x10^-9 with about one significant decimal digit of accuracy. Zero is also
   * represented. Negative numbers are rounded up to zero. Values too large to
   * represent are rounded down to the largest representable value. Positive
   * values too small to represent are rounded up to the smallest positive
   * representable value.
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   * @see org.apache.lucene.util.SmallFloat
   */
  @Override
  public final long encodeNormValue(float f) {
    return Float.floatToIntBits(f);
  }

  /**
   * Decodes the norm value, assuming it is a single byte.
   *
   * @see #encodeNormValue(float)
   */
  @Override
  public final float decodeNormValue(long norm) {
    return Float.intBitsToFloat((int)norm);
  }

  /** Implemented as
   *  <code>state.getBoost()*lengthNorm(numTerms)</code>, where
   *  <code>numTerms</code> is {@link org.apache.lucene.index.FieldInvertState#getLength()} if {@link
   *  #setDiscountOverlaps} is false, else it's {@link
   *  org.apache.lucene.index.FieldInvertState#getLength()} - {@link
   *  org.apache.lucene.index.FieldInvertState#getNumOverlap()}.
   *
   *  @lucene.experimental */
  @Override
  public float lengthNorm(FieldInvertState state) {
    final int numTerms;
    if (discountOverlaps) {
      numTerms = state.getLength() - state.getNumOverlap();
    } else {
      numTerms = state.getLength();
    }
    return state.getBoost() * ((float) (1.0 / Math.sqrt(numTerms)));
  }

  /** Implemented as <code>sqrt(freq)</code>. */
  @Override
  public float tf(float freq) {
    return (float)Math.sqrt(freq);
  }

  /** Implemented as <code>1 / (distance + 1)</code>. */
  @Override
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }

  /** The default implementation returns <code>1</code> */
  @Override
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    return 1;
  }

  /** Implemented as <code>log(docCount/(docFreq+1)) + 1</code>. */
  @Override
  public float idf(long docFreq, long docCount) {
    return (float)(Math.log(docCount/(double)(docFreq+1)) + 1.0);
  }

  /**
   * True if overlap tokens (tokens with a position of increment of zero) are
   * discounted from the document's length.
   */
  protected boolean discountOverlaps = true;

  /** Determines whether overlap tokens (Tokens with
   *  0 position increment) are ignored when computing
   *  norm.  By default this is true, meaning overlap
   *  tokens do not count when computing norms.
   *
   *  @lucene.experimental
   *
   *  @see #computeNorm
   */
  public void setDiscountOverlaps(boolean v) {
    discountOverlaps = v;
  }

  /**
   * Returns true if overlap tokens are discounted from the document's length.
   * @see #setDiscountOverlaps
   */
  public boolean getDiscountOverlaps() {
    return discountOverlaps;
  }

  @Override
  public String toString() {
    return "DefaultSimilarity";
  }
}
