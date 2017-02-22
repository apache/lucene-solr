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
package org.apache.lucene.search.similarities;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBooleanSimilarity extends LuceneTestCase {

  public void testTermScoreIsEqualToBoost() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    doc.add(new StringField("foo", "baz", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    doc.add(new StringField("foo", "bar", Store.NO));
    w.addDocument(doc);

    DirectoryReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new BooleanSimilarity());
    TopDocs topDocs = searcher.search(new TermQuery(new Term("foo", "bar")), 2);
    assertEquals(2, topDocs.totalHits);
    assertEquals(1f, topDocs.scoreDocs[0].score, 0f);
    assertEquals(1f, topDocs.scoreDocs[1].score, 0f);

    topDocs = searcher.search(new TermQuery(new Term("foo", "baz")), 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1f, topDocs.scoreDocs[0].score, 0f);

    topDocs = searcher.search(new BoostQuery(new TermQuery(new Term("foo", "baz")), 3f), 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(3f, topDocs.scoreDocs[0].score, 0f);

    reader.close();
    dir.close();
  }

  public void testPhraseScoreIsEqualToBoost() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig().setSimilarity(new BooleanSimilarity()));
    Document doc = new Document();
    doc.add(new TextField("foo", "bar baz quux", Store.NO));
    w.addDocument(doc);

    DirectoryReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new BooleanSimilarity());

    PhraseQuery query = new PhraseQuery(2, "foo", "bar", "quux");

    TopDocs topDocs = searcher.search(query, 2);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1f, topDocs.scoreDocs[0].score, 0f);

    topDocs = searcher.search(new BoostQuery(query, 7), 2);
    assertEquals(1, topDocs.totalHits);
    assertEquals(7f, topDocs.scoreDocs[0].score, 0f);

    reader.close();
    dir.close();
  }

  public void testSameNormsAsBM25() {
    BooleanSimilarity sim1 = new BooleanSimilarity();
    BM25Similarity sim2 = new BM25Similarity();
    sim2.setDiscountOverlaps(true);
    for (int iter = 0; iter < 100; ++iter) {
      final int length = TestUtil.nextInt(random(), 1, 100);
      final int position = random().nextInt(length);
      final int numOverlaps = random().nextInt(50);
      final float boost = random().nextFloat() * 10;
      FieldInvertState state = new FieldInvertState("foo", position, length, numOverlaps, 100, boost);
      assertEquals(
          sim2.computeNorm(state),
          sim1.computeNorm(state),
          0f);
    }
  }
}
