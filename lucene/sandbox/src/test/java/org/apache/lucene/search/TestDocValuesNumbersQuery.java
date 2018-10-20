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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestDocValuesNumbersQuery extends LuceneTestCase {

  public void testEquals() {
    assertEquals(new DocValuesNumbersQuery("field", 17L, 42L), new DocValuesNumbersQuery("field", 17L, 42L));
    assertEquals(new DocValuesNumbersQuery("field", 17L, 42L, 32416190071L), new DocValuesNumbersQuery("field", 17L, 32416190071L, 42L));
    assertFalse(new DocValuesNumbersQuery("field", 42L).equals(new DocValuesNumbersQuery("field2", 42L)));
    assertFalse(new DocValuesNumbersQuery("field", 17L, 42L).equals(new DocValuesNumbersQuery("field", 17L, 32416190071L)));
  }

  public void testDuelTermsQuery() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Long> allNumbers = new ArrayList<>();
      final int numNumbers = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numNumbers; ++i) {
        allNumbers.add(random().nextLong());
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Long number = allNumbers.get(random().nextInt(allNumbers.size()));
        doc.add(new StringField("text", number.toString(), Store.NO));
        doc.add(new NumericDocValuesField("long", number));
        doc.add(new SortedNumericDocValuesField("twolongs", number));
        doc.add(new SortedNumericDocValuesField("twolongs", number*2));
        iw.addDocument(doc);
      }
      if (numNumbers > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("text", allNumbers.get(0).toString())));
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
        final int numQueryNumbers = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 8));
        Set<Long> queryNumbers = new HashSet<>();
        Set<Long> queryNumbersX2 = new HashSet<>();
        for (int j = 0; j < numQueryNumbers; ++j) {
          Long number = allNumbers.get(random().nextInt(allNumbers.size()));
          queryNumbers.add(number);
          queryNumbersX2.add(2*number);
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Long number : queryNumbers) {
          bq.add(new TermQuery(new Term("text", number.toString())), Occur.SHOULD);
        }
        Query q1 = new BoostQuery(new ConstantScoreQuery(bq.build()), boost);

        Query q2 = new BoostQuery(new DocValuesNumbersQuery("long", queryNumbers), boost);
        assertSameMatches(searcher, q1, q2, true);

        Query q3 = new BoostQuery(new DocValuesNumbersQuery("twolongs", queryNumbers), boost);
        assertSameMatches(searcher, q1, q3, true);

        Query q4 = new BoostQuery(new DocValuesNumbersQuery("twolongs", queryNumbersX2), boost);
        assertSameMatches(searcher, q1, q4, true);
      }

      reader.close();
      dir.close();
    }
  }

  public void testApproximation() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Long> allNumbers = new ArrayList<>();
      final int numNumbers = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numNumbers; ++i) {
        allNumbers.add(random().nextLong());
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Long number = allNumbers.get(random().nextInt(allNumbers.size()));
        doc.add(new StringField("text", number.toString(), Store.NO));
        doc.add(new NumericDocValuesField("long", number));
        iw.addDocument(doc);
      }
      if (numNumbers > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("text", allNumbers.get(0).toString())));
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
        final int numQueryNumbers = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 8));
        Set<Long> queryNumbers = new HashSet<>();
        for (int j = 0; j < numQueryNumbers; ++j) {
          queryNumbers.add(allNumbers.get(random().nextInt(allNumbers.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Long number : queryNumbers) {
          bq.add(new TermQuery(new Term("text", number.toString())), Occur.SHOULD);
        }
        Query q1 = new BoostQuery(new ConstantScoreQuery(bq.build()), boost);
        final Query q2 = new BoostQuery(new DocValuesNumbersQuery("long", queryNumbers), boost);

        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(q1, Occur.MUST);
        bq1.add(new TermQuery(new Term("text", allNumbers.get(0).toString())), Occur.FILTER);

        BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
        bq2.add(q2, Occur.MUST);
        bq2.add(new TermQuery(new Term("text", allNumbers.get(0).toString())), Occur.FILTER);

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
