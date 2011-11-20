package org.apache.lucene.search;

/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;

/** Document boost unit test.
 *
 *
 */
public class TestSetNorm extends LuceneTestCase {

  public void testSetNorm() throws Exception {
    Directory store = newDirectory();
    IndexWriter writer = new IndexWriter(store, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    // add the same document four times
    Field f1 = newField("field", "word", TextField.TYPE_STORED);
    Document d1 = new Document();
    d1.add(f1);
    writer.addDocument(d1);
    writer.addDocument(d1);
    writer.addDocument(d1);
    writer.addDocument(d1);
    writer.close();

    // reset the boost of each instance of this document
    IndexReader reader = IndexReader.open(store, false);
    DefaultSimilarity similarity = new DefaultSimilarity();
    reader.setNorm(0, "field", similarity.encodeNormValue(1.0f));
    reader.setNorm(1, "field", similarity.encodeNormValue(2.0f));
    reader.setNorm(2, "field", similarity.encodeNormValue(4.0f));
    reader.setNorm(3, "field", similarity.encodeNormValue(16.0f));
    reader.close();

    // check that searches are ordered by this boost
    final float[] scores = new float[4];

    IndexReader ir = IndexReader.open(store);
    IndexSearcher is = new IndexSearcher(ir);
    is.search
      (new TermQuery(new Term("field", "word")),
       new Collector() {
         private int base = 0;
         private Scorer scorer;
         @Override
         public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer;
         }
         @Override
         public final void collect(int doc) throws IOException {
           scores[doc + base] = scorer.score();
         }
         @Override
         public void setNextReader(AtomicReaderContext context) {
           base = context.docBase;
         }
         @Override
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });
    is.close();
    ir.close();
    float lastScore = 0.0f;

    for (int i = 0; i < 4; i++) {
      assertTrue(scores[i] > lastScore);
      lastScore = scores[i];
    }
    store.close();
  }
}
