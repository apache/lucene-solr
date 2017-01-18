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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;

public class TermInSetQueryTest extends LuceneTestCase {

  public void testDuel() throws IOException {
    final int iters = atLeast(2);
    final String field = "f";
    for (int iter = 0; iter < iters; ++iter) {
      final List<BytesRef> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(new BytesRef(value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final BytesRef term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(field, term, Store.NO));
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term(field, allTerms.get(0))));
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
        List<BytesRef> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          queryTerms.add(allTerms.get(random().nextInt(allTerms.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef t : queryTerms) {
          bq.add(new TermQuery(new Term(field, t)), Occur.SHOULD);
        }
        final Query q1 = new ConstantScoreQuery(bq.build());
        final Query q2 = new TermInSetQuery(field, queryTerms);
        assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);
      }

      reader.close();
      dir.close();
    }
  }

  private void assertSameMatches(IndexSearcher searcher, Query q1, Query q2, boolean scores) throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits, td2.totalHits);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  public void testHashCodeAndEquals() {
    int num = atLeast(100);
    List<BytesRef> terms = new ArrayList<>();
    Set<BytesRef> uniqueTerms = new HashSet<>();
    for (int i = 0; i < num; i++) {
      String string = TestUtil.randomRealisticUnicodeString(random());
      terms.add(new BytesRef(string));
      uniqueTerms.add(new BytesRef(string));
      TermInSetQuery left = new TermInSetQuery("field", uniqueTerms);
      Collections.shuffle(terms, random());
      TermInSetQuery right = new TermInSetQuery("field", terms);
      assertEquals(right, left);
      assertEquals(right.hashCode(), left.hashCode());
      if (uniqueTerms.size() > 1) {
        List<BytesRef> asList = new ArrayList<>(uniqueTerms);
        asList.remove(0);
        TermInSetQuery notEqual = new TermInSetQuery("field", asList);
        assertFalse(left.equals(notEqual));
        assertFalse(right.equals(notEqual));
      }
    }

    TermInSetQuery tq1 = new TermInSetQuery("thing", new BytesRef("apple"));
    TermInSetQuery tq2 = new TermInSetQuery("thing", new BytesRef("orange"));
    assertFalse(tq1.hashCode() == tq2.hashCode());

    // different fields with the same term should have differing hashcodes
    tq1 = new TermInSetQuery("thing", new BytesRef("apple"));
    tq2 = new TermInSetQuery("thing2", new BytesRef("apple"));
    assertFalse(tq1.hashCode() == tq2.hashCode());
  }

  public void testSimpleEquals() {
    // Two terms with the same hash code
    assertEquals("AaAaBB".hashCode(), "BBBBBB".hashCode());
    TermInSetQuery left = new TermInSetQuery("id", new BytesRef("AaAaAa"), new BytesRef("AaAaBB"));
    TermInSetQuery right = new TermInSetQuery("id", new BytesRef("AaAaAa"), new BytesRef("BBBBBB"));
    assertFalse(left.equals(right));
  }

  public void testToString() {
    TermInSetQuery termsQuery = new TermInSetQuery("field1",
        new BytesRef("a"), new BytesRef("b"), new BytesRef("c"));
    assertEquals("field1:a field1:b field1:c", termsQuery.toString());
  }

  public void testDedup() {
    Query query1 = new TermInSetQuery("foo", new BytesRef("bar"));
    Query query2 = new TermInSetQuery("foo", new BytesRef("bar"), new BytesRef("bar"));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testOrderDoesNotMatter() {
    // order of terms if different
    Query query1 = new TermInSetQuery("foo", new BytesRef("bar"), new BytesRef("baz"));
    Query query2 = new TermInSetQuery("foo", new BytesRef("baz"), new BytesRef("bar"));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testRamBytesUsed() {
    List<BytesRef> terms = new ArrayList<>();
    final int numTerms = 1000 + random().nextInt(1000);
    for (int i = 0; i < numTerms; ++i) {
      terms.add(new BytesRef(RandomStrings.randomUnicodeOfLength(random(), 10)));
    }
    TermInSetQuery query = new TermInSetQuery("f", terms);
    final long actualRamBytesUsed = RamUsageTester.sizeOf(query);
    final long expectedRamBytesUsed = query.ramBytesUsed();
    // error margin within 5%
    assertEquals(actualRamBytesUsed, expectedRamBytesUsed, actualRamBytesUsed / 20);
  }

  private static class TermsCountingDirectoryReaderWrapper extends FilterDirectoryReader {

    private final AtomicInteger counter;
    
    public TermsCountingDirectoryReaderWrapper(DirectoryReader in, AtomicInteger counter) throws IOException {
      super(in, new TermsCountingSubReaderWrapper(counter));
      this.counter = counter;
    }

    private static class TermsCountingSubReaderWrapper extends SubReaderWrapper {
      private final AtomicInteger counter;

      public TermsCountingSubReaderWrapper(AtomicInteger counter) {
        this.counter = counter;
      }

      @Override
      public LeafReader wrap(LeafReader reader) {
        return new TermsCountingLeafReaderWrapper(reader, counter);
      }
    }

    private static class TermsCountingLeafReaderWrapper extends FilterLeafReader {

      private final AtomicInteger counter;

      public TermsCountingLeafReaderWrapper(LeafReader in, AtomicInteger counter) {
        super(in);
        this.counter = counter;
      }

      @Override
      public Fields fields() throws IOException {
        return new FilterFields(in.fields()) {
          @Override
          public Terms terms(String field) throws IOException {
            final Terms in = this.in.terms(field);
            if (in == null) {
              return null;
            }
            return new FilterTerms(in) {
              @Override
              public TermsEnum iterator() throws IOException {
                counter.incrementAndGet();
                return super.iterator();
              }
            };
          }
        };
      }
      
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new TermsCountingDirectoryReaderWrapper(in, counter);
    }

  }

  public void testPullOneTermsEnum() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "1", Store.NO));
    w.addDocument(doc);
    DirectoryReader reader = w.getReader();
    w.close();
    final AtomicInteger counter = new AtomicInteger();
    DirectoryReader wrapped = new TermsCountingDirectoryReaderWrapper(reader, counter);

    final List<BytesRef> terms = new ArrayList<>();
    // enough terms to avoid the rewrite
    final int numTerms = TestUtil.nextInt(random(), TermInSetQuery.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD + 1, 100);
    for (int i = 0; i < numTerms; ++i) {
      final BytesRef term = new BytesRef(RandomStrings.randomUnicodeOfCodepointLength(random(), 10));
      terms.add(term);
    }

    assertEquals(0, new IndexSearcher(wrapped).count(new TermInSetQuery("bar", terms)));
    assertEquals(0, counter.get()); // missing field
    new IndexSearcher(wrapped).count(new TermInSetQuery("foo", terms));
    assertEquals(1, counter.get());
    wrapped.close();
    dir.close();
  }
  
  public void testBinaryToString() {
    TermInSetQuery query = new TermInSetQuery("field", new BytesRef(new byte[] { (byte) 0xff, (byte) 0xfe }));
    assertEquals("field:[ff fe]", query.toString());
  }

  public void testIsConsideredCostlyByQueryCache() throws IOException {
    TermInSetQuery query = new TermInSetQuery("foo", new BytesRef("bar"), new BytesRef("baz"));
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    assertFalse(policy.shouldCache(query));
    policy.onUse(query);
    policy.onUse(query);
    // cached after two uses
    assertTrue(policy.shouldCache(query));
  }
}
