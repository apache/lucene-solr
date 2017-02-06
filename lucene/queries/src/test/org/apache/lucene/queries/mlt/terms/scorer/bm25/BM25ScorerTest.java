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

package org.apache.lucene.queries.mlt.terms.scorer.bm25;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.mlt.terms.scorer.BM25Scorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

public class BM25ScorerTest {
  public static final String SAMPLE_FIELD = "field1";
  public static final String SAMPLE_TERM = "term1";

  private BM25Scorer scorerToTest = new BM25Scorer();

  @Test
  public void indexedNorms_shouldCalculateBM25SimilarityScore() throws IOException {
    Map<String, NumericDocValues> field2normsFromIndex = new HashMap<>();
    NumericDocValues field1Norms = new TestNumericDocValues(10);
    field2normsFromIndex.put(SAMPLE_FIELD, field1Norms);
    scorerToTest.setField2normsFromIndex(field2normsFromIndex);

    CollectionStatistics field1Stats = new CollectionStatistics(SAMPLE_FIELD, 100, 90, 1000, 1000);

    BytesRef term1ByteRef = new BytesRef(SAMPLE_TERM);
    TermStatistics term1Stat = new TermStatistics(term1ByteRef, 20, 60);

    float score = scorerToTest.score(SAMPLE_FIELD, field1Stats, term1Stat, 20);
    Assert.assertEquals(0.19, score, 0.01);
  }

  @Test
  public void normsFromDocument_shouldCalculateBM25SimilarityScore() throws IOException {
    Map<String, Float> fieldToNorm = new HashMap<>();
    fieldToNorm.put(SAMPLE_FIELD, 332f);
    scorerToTest.setField2norm(fieldToNorm);

    CollectionStatistics field1Stats = new CollectionStatistics(SAMPLE_FIELD, 100, 90, 1000, 1000);

    BytesRef term1ByteRef = new BytesRef(SAMPLE_TERM);
    TermStatistics term1Stat = new TermStatistics(term1ByteRef, 20, 60);

    float score = scorerToTest.score(SAMPLE_FIELD, field1Stats, term1Stat, 20);
    Assert.assertEquals(0.19, score, 0.01);
  }

  @Test
  public void normFromFreeText_shouldCalculateBM25SimilarityScore() throws IOException {
    float norm = 332f;
    scorerToTest.setTextNorm(norm);

    CollectionStatistics field1Stats = new CollectionStatistics(SAMPLE_FIELD, 100, 90, 1000, 1000);

    BytesRef term1ByteRef = new BytesRef(SAMPLE_TERM);
    TermStatistics term1Stat = new TermStatistics(term1ByteRef, 20, 60);

    float score = scorerToTest.score(SAMPLE_FIELD, field1Stats, term1Stat, 20);
    Assert.assertEquals(0.19, score, 0.01);
  }

  //testare se ho direttamente la norma constante

  private class TestNumericDocValues extends NumericDocValues {
    long normIndex = 100; // precomputer value in cache for this position is 332
    final int maxDoc;
    int doc = -1;

    TestNumericDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public long longValue() throws IOException {
      return normIndex;
    }
  }
}
