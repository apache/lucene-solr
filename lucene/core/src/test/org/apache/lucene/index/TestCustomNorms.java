package org.apache.lucene.index;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;

/**
 * 
 */
@SuppressCodecs("Lucene3x")
public class TestCustomNorms extends LuceneTestCase {
  final String floatTestField = "normsTestFloat";
  final String exceptionTestField = "normsTestExcp";

  public void testFloatNorms() throws IOException {

    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    Similarity provider = new MySimProvider();
    config.setSimilarity(provider);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    final LineFileDocs docs = new LineFileDocs(random());
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      Document doc = docs.nextDoc();
      float nextFloat = random().nextFloat();
      Field f = new TextField(floatTestField, "" + nextFloat, Field.Store.YES);
      f.setBoost(nextFloat);

      doc.add(f);
      writer.addDocument(doc);
      doc.removeField(floatTestField);
      if (rarely()) {
        writer.commit();
      }
    }
    writer.commit();
    writer.close();
    AtomicReader open = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    NumericDocValues norms = open.getNormValues(floatTestField);
    assertNotNull(norms);
    for (int i = 0; i < open.maxDoc(); i++) {
      Document document = open.document(i);
      float expected = Float.parseFloat(document.get(floatTestField));
      assertEquals(expected, Float.intBitsToFloat((int)norms.get(i)), 0.0f);
    }
    open.close();
    dir.close();
    docs.close();
  }

  public class MySimProvider extends PerFieldSimilarityWrapper {
    Similarity delegate = new DefaultSimilarity();

    @Override
    public float queryNorm(float sumOfSquaredWeights) {
      return delegate.queryNorm(sumOfSquaredWeights);
    }

    @Override
    public Similarity get(String field) {
      if (floatTestField.equals(field)) {
        return new FloatEncodingBoostSimilarity();
      } else {
        return delegate;
      }
    }

    @Override
    public float coord(int overlap, int maxOverlap) {
      return delegate.coord(overlap, maxOverlap);
    }
  }

  public static class FloatEncodingBoostSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return Float.floatToIntBits(state.getBoost());
    }
    
    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
