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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestIndexOrDocValuesQuery extends LuceneTestCase {

  public void testUseIndexForSelectiveQueries() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig()
        // relies on costs and PointValues.estimateCost so we need the default codec
        .setCodec(TestUtil.getDefaultCodec()));
    for (int i = 0; i < 2000; ++i) {
      Document doc = new Document();
      if (i == 42) {
        doc.add(new StringField("f1", "bar", Store.NO));
        doc.add(new LongPoint("f2", 42L));
        doc.add(new NumericDocValuesField("f2", 42L));
      } else if (i == 100) {
        doc.add(new StringField("f1", "foo", Store.NO));
        doc.add(new LongPoint("f2", 2L));
        doc.add(new NumericDocValuesField("f2", 2L));
      } else {
        doc.add(new StringField("f1", "bar", Store.NO));
        doc.add(new LongPoint("f2", 2L));
        doc.add(new NumericDocValuesField("f2", 2L));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null);

    // The term query is more selective, so the IndexOrDocValuesQuery should use doc values
    final Query q1 = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("f1", "foo")), Occur.MUST)
        .add(new IndexOrDocValuesQuery(LongPoint.newExactQuery("f2", 2), NumericDocValuesField.newRangeQuery("f2", 2L, 2L)), Occur.MUST)
        .build();

    final Weight w1 = searcher.createNormalizedWeight(q1, random().nextBoolean());
    final Scorer s1 = w1.scorer(searcher.getIndexReader().leaves().get(0));
    assertNotNull(s1.twoPhaseIterator()); // means we use doc values

    // The term query is less selective, so the IndexOrDocValuesQuery should use points
    final Query q2 = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("f1", "bar")), Occur.MUST)
        .add(new IndexOrDocValuesQuery(LongPoint.newExactQuery("f2", 42), NumericDocValuesField.newRangeQuery("f2", 42L, 42L)), Occur.MUST)
        .build();

    final Weight w2 = searcher.createNormalizedWeight(q2, random().nextBoolean());
    final Scorer s2 = w2.scorer(searcher.getIndexReader().leaves().get(0));
    assertNull(s2.twoPhaseIterator()); // means we use points

    reader.close();
    w.close();
    dir.close();
  }

}
