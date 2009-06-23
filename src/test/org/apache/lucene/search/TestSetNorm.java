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
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

/** Document boost unit test.
 *
 *
 * @version $Revision$
 */
public class TestSetNorm extends LuceneTestCase {
  public TestSetNorm(String name) {
    super(name);
  }

  public void testSetNorm() throws Exception {
    RAMDirectory store = new RAMDirectory();
    IndexWriter writer = new IndexWriter(store, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

    // add the same document four times
    Fieldable f1 = new Field("field", "word", Field.Store.YES, Field.Index.ANALYZED);
    Document d1 = new Document();
    d1.add(f1);
    writer.addDocument(d1);
    writer.addDocument(d1);
    writer.addDocument(d1);
    writer.addDocument(d1);
    writer.close();

    // reset the boost of each instance of this document
    IndexReader reader = IndexReader.open(store);
    reader.setNorm(0, "field", 1.0f);
    reader.setNorm(1, "field", 2.0f);
    reader.setNorm(2, "field", 4.0f);
    reader.setNorm(3, "field", 16.0f);
    reader.close();

    // check that searches are ordered by this boost
    final float[] scores = new float[4];

    new IndexSearcher(store).search
      (new TermQuery(new Term("field", "word")),
       new Collector() {
         private int base = 0;
         private Scorer scorer;
         public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer;
         }
         public final void collect(int doc) throws IOException {
           scores[doc + base] = scorer.score();
         }
         public void setNextReader(IndexReader reader, int docBase) {
           base = docBase;
         }
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    float lastScore = 0.0f;

    for (int i = 0; i < 4; i++) {
      assertTrue(scores[i] > lastScore);
      lastScore = scores[i];
    }
  }
}
