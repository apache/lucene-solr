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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Document boost unit test.
 *
 *
 */
public class TestDocBoost extends LuceneTestCase {

  public void testDocBoost() throws Exception {
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), store, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    Field f1 = newField("field", "word", TextField.TYPE_STORED);
    Field f2 = newField("field", "word", TextField.TYPE_STORED);
    f2.setBoost(2.0f);

    Document d1 = new Document();
    Document d2 = new Document();

    d1.add(f1);                                 // boost = 1
    d2.add(f2);                                 // boost = 2

    writer.addDocument(d1);
    writer.addDocument(d2);

    IndexReader reader = writer.getReader();
    writer.close();

    final float[] scores = new float[4];

    IndexSearcher searcher = newSearcher(reader);
    searcher.search
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

    float lastScore = 0.0f;

    for (int i = 0; i < 2; i++) {
      if (VERBOSE) {
        System.out.println(searcher.explain(new TermQuery(new Term("field", "word")), i));
      }
      assertTrue("score: " + scores[i] + " should be > lastScore: " + lastScore, scores[i] > lastScore);
      lastScore = scores[i];
    }
    
    reader.close();
    store.close();
  }
}
