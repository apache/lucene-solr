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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDocValuesTermsQuery extends LuceneTestCase {

  public void testEquals() {
    assertEquals(new DocValuesTermsQuery("foo", "bar"), new DocValuesTermsQuery("foo", "bar"));
    assertEquals(new DocValuesTermsQuery("foo", "bar"), new DocValuesTermsQuery("foo", "bar", "bar"));
    assertEquals(new DocValuesTermsQuery("foo", "bar", "baz"), new DocValuesTermsQuery("foo", "baz", "bar"));
    assertFalse(new DocValuesTermsQuery("foo", "bar").equals(new DocValuesTermsQuery("foo2", "bar")));
    assertFalse(new DocValuesTermsQuery("foo", "bar").equals(new DocValuesTermsQuery("foo", "baz")));
  }

  public void testDuelTermsQuery() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Term> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(new Term("f", value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Term term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(term.field(), term.text(), Store.NO));
        doc.add(new SortedDocValuesField(term.field(), new BytesRef(term.text())));
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(allTerms.get(0)));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      if (reader.numDocs() == 0) {
        // may occasionally happen if all documents got the same term
        IOUtils.close(reader, dir);
        continue;
      }

      for (int i = 0; i < 100; ++i) {
        final float boost = random().nextFloat() * 10;
        final int numQueryTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 8));
        List<Term> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          queryTerms.add(allTerms.get(random().nextInt(allTerms.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Term term : queryTerms) {
          bq.add(new TermQuery(term), Occur.SHOULD);
        }
        Query q1 = new BoostQuery(new ConstantScoreQuery(bq.build()), boost);
        List<String> bytesTerms = new ArrayList<>();
        for (Term term : queryTerms) {
          bytesTerms.add(term.text());
        }
        final Query q2 = new BoostQuery(new DocValuesTermsQuery("f", bytesTerms.toArray(new String[0])), boost);
        assertSameMatches(searcher, q1, q2, true);
      }

      reader.close();
      dir.close();
    }
  }

  public void testApproximation() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Term> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(new Term("f", value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Term term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(term.field(), term.text(), Store.NO));
        doc.add(new SortedDocValuesField(term.field(), new BytesRef(term.text())));
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(allTerms.get(0)));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      if (reader.numDocs() == 0) {
        // may occasionally happen if all documents got the same term
        IOUtils.close(reader, dir);
        continue;
      }

      for (int i = 0; i < 100; ++i) {
        final float boost = random().nextFloat() * 10;
        final int numQueryTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 8));
        List<Term> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          queryTerms.add(allTerms.get(random().nextInt(allTerms.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Term term : queryTerms) {
          bq.add(new TermQuery(term), Occur.SHOULD);
        }
        Query q1 = new BoostQuery(new ConstantScoreQuery(bq.build()), boost);
        List<String> bytesTerms = new ArrayList<>();
        for (Term term : queryTerms) {
          bytesTerms.add(term.text());
        }
        final Query q2 = new BoostQuery(new DocValuesTermsQuery("f", bytesTerms.toArray(new String[0])), boost);

        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(q1, Occur.MUST);
        bq1.add(new TermQuery(allTerms.get(0)), Occur.FILTER);

        BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
        bq2.add(q2, Occur.MUST);
        bq2.add(new TermQuery(allTerms.get(0)), Occur.FILTER);

        assertSameMatches(searcher, bq1.build(), bq2.build(), true);
      }

      reader.close();
      dir.close();
    }
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
