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
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestNormsFieldExistsQuery extends LuceneTestCase {

  public void testRandom() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new TextField("text1", "value", Store.NO));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      assertSameMatches(searcher, new TermQuery(new Term("has_value", "yes")), new NormsFieldExistsQuery("text1"), false);

      reader.close();
      dir.close();
    }
  }

  public void testApproximation() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new TextField("text1", "value", Store.NO));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      BooleanQuery.Builder ref = new BooleanQuery.Builder();
      ref.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      ref.add(new TermQuery(new Term("has_value", "yes")), Occur.FILTER);

      BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
      bq1.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      bq1.add(new NormsFieldExistsQuery("text1"), Occur.FILTER);
      assertSameMatches(searcher, ref.build(), bq1.build(), true);

      reader.close();
      dir.close();
    }
  }

  public void testScore() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new TextField("text1", "value", Store.NO));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      final float boost = random().nextFloat() * 10;
      final Query ref = new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("has_value", "yes"))), boost);

      final Query q1 = new BoostQuery(new NormsFieldExistsQuery("text1"), boost);
      assertSameMatches(searcher, ref, q1, true);

      reader.close();
      dir.close();
    }
  }

  public void testMissingField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(0, searcher.count(new NormsFieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testAllDocsHaveField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new TextField("f", "value", Store.NO));
    iw.addDocument(doc);
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(1, searcher.count(new NormsFieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testFieldExistsButNoDocsHaveField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    // 1st segment has the field, but 2nd one does not
    Document doc = new Document();
    doc.add(new TextField("f", "value", Store.NO));
    iw.addDocument(doc);
    iw.commit();
    iw.addDocument(new Document());
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(1, searcher.count(new NormsFieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  private void assertSameMatches(IndexSearcher searcher, Query q1, Query q2, boolean scores) throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits.value, td2.totalHits.value);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }
}
