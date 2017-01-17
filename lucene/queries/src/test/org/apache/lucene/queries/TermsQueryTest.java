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
package org.apache.lucene.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

public class TermsQueryTest extends LuceneTestCase {

  public void testDuel() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Term> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String field = usually() ? "f" : "g";
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(new Term(field, value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Term term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(term.field(), term.text(), Store.NO));
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
        for (Term t : queryTerms) {
          bq.add(new TermQuery(t), Occur.SHOULD);
        }
        final Query q1 = new ConstantScoreQuery(bq.build());
        final Query q2 = new TermsQuery(queryTerms);
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

  private TermsQuery termsQuery(boolean singleField, Term...terms) {
    return termsQuery(singleField, Arrays.asList(terms));
  }

  private TermsQuery termsQuery(boolean singleField, Collection<Term> termList) {
    if (!singleField) {
      return new TermsQuery(new ArrayList<>(termList));
    }
    final TermsQuery filter;
    List<BytesRef> bytes = new ArrayList<>();
    String field = null;
    for (Term term : termList) {
        bytes.add(term.bytes());
        if (field != null) {
          assertEquals(term.field(), field);
        }
        field = term.field();
    }
    assertNotNull(field);
    filter = new TermsQuery(field, bytes);
    return filter;
  }

  public void testHashCodeAndEquals() {
    int num = atLeast(100);
    final boolean singleField = random().nextBoolean();
    List<Term> terms = new ArrayList<>();
    Set<Term> uniqueTerms = new HashSet<>();
    for (int i = 0; i < num; i++) {
      String field = "field" + (singleField ? "1" : random().nextInt(100));
      String string = TestUtil.randomRealisticUnicodeString(random());
      terms.add(new Term(field, string));
      uniqueTerms.add(new Term(field, string));
      TermsQuery left = termsQuery(singleField ? random().nextBoolean() : false, uniqueTerms);
      Collections.shuffle(terms, random());
      TermsQuery right = termsQuery(singleField ? random().nextBoolean() : false, terms);
      assertEquals(right, left);
      assertEquals(right.hashCode(), left.hashCode());
      if (uniqueTerms.size() > 1) {
        List<Term> asList = new ArrayList<>(uniqueTerms);
        asList.remove(0);
        TermsQuery notEqual = termsQuery(singleField ? random().nextBoolean() : false, asList);
        assertFalse(left.equals(notEqual));
        assertFalse(right.equals(notEqual));
      }
    }

    TermsQuery tq1 = new TermsQuery(new Term("thing", "apple"));
    TermsQuery tq2 = new TermsQuery(new Term("thing", "orange"));
    assertFalse(tq1.hashCode() == tq2.hashCode());

    // different fields with the same term should have differing hashcodes
    tq1 = new TermsQuery(new Term("thing1", "apple"));
    tq2 = new TermsQuery(new Term("thing2", "apple"));
    assertFalse(tq1.hashCode() == tq2.hashCode());
  }

  public void testSingleFieldEquals() {
    // Two terms with the same hash code
    assertEquals("AaAaBB".hashCode(), "BBBBBB".hashCode());
    TermsQuery left = termsQuery(true, new Term("id", "AaAaAa"), new Term("id", "AaAaBB"));
    TermsQuery right = termsQuery(true, new Term("id", "AaAaAa"), new Term("id", "BBBBBB"));
    assertFalse(left.equals(right));
  }

  public void testToString() {
    TermsQuery termsQuery = new TermsQuery(new Term("field1", "a"),
                                              new Term("field1", "b"),
                                              new Term("field1", "c"));
    assertEquals("field1:a field1:b field1:c", termsQuery.toString());
  }

  public void testDedup() {
    Query query1 = new TermsQuery(new Term("foo", "bar"));
    Query query2 = new TermsQuery(new Term("foo", "bar"), new Term("foo", "bar"));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testOrderDoesNotMatter() {
    // order of terms if different
    Query query1 = new TermsQuery(new Term("foo", "bar"), new Term("foo", "baz"));
    Query query2 = new TermsQuery(new Term("foo", "baz"), new Term("foo", "bar"));
    QueryUtils.checkEqual(query1, query2);

    // order of fields is different
    query1 = new TermsQuery(new Term("foo", "bar"), new Term("bar", "bar"));
    query2 = new TermsQuery(new Term("bar", "bar"), new Term("foo", "bar"));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testRamBytesUsed() {
    List<Term> terms = new ArrayList<>();
    final int numTerms = 1000 + random().nextInt(1000);
    for (int i = 0; i < numTerms; ++i) {
      terms.add(new Term("f", RandomStrings.randomUnicodeOfLength(random(), 10)));
    }
    TermsQuery query = new TermsQuery(terms);
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
  
  public void testBinaryToString() {
    TermsQuery query = new TermsQuery(new Term("field", new BytesRef(new byte[] { (byte) 0xff, (byte) 0xfe })));
    assertEquals("field:[ff fe]", query.toString());
  }

  public void testIsConsideredCostlyByQueryCache() throws IOException {
    Query query = new TermsQuery(new Term("foo", "bar"), new Term("foo", "baz"));
    query = query.rewrite(new MultiReader());
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    assertFalse(policy.shouldCache(query));
    policy.onUse(query);
    policy.onUse(query);
    // cached after two uses
    assertTrue(policy.shouldCache(query));
  }
}
