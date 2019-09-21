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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

public class CommonTermsQueryTest extends LuceneTestCase {
  
  public void testBasics() throws IOException {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, analyzer);
    String[] docs = new String[] {"this is the end of the world right",
        "is this it or maybe not",
        "this is the end of the universe as we know it",
        "there is the famous restaurant at the end of the universe",};
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i, Field.Store.YES));
      doc.add(newTextField("field", docs[i], Field.Store.NO));
      w.addDocument(doc);
    }
    
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 3);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
      assertEquals("2", r.document(search.scoreDocs[1].doc).get("id"));
      assertEquals("3", r.document(search.scoreDocs[2].doc).get("id"));
    }
    
    { // only high freq
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 2);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
      assertEquals("2", r.document(search.scoreDocs[1].doc).get("id"));
    }
    
    { // low freq is mandatory
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.MUST,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 1);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
    }
    
    { // low freq is mandatory
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.MUST,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "restaurant"));
      query.add(new Term("field", "universe"));
      
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 1);
      assertEquals("3", r.document(search.scoreDocs[0].doc).get("id"));
      
    }
    IOUtils.close(r, w, dir, analyzer);
  }
  
  public void testEqualsHashCode() {
    CommonTermsQuery query = new CommonTermsQuery(randomOccur(random()),
        randomOccur(random()), random().nextFloat());
    int terms = atLeast(2);
    for (int i = 0; i < terms; i++) {
      query.add(new Term(TestUtil.randomRealisticUnicodeString(random()),
          TestUtil.randomRealisticUnicodeString(random())));
    }
    QueryUtils.checkHashEquals(query);
    QueryUtils.checkUnequal(new CommonTermsQuery(randomOccur(random()),
        randomOccur(random()), random().nextFloat()),
        query);
    
    {
      final long seed = random().nextLong();
      Random r = new Random(seed);
      CommonTermsQuery left = new CommonTermsQuery(randomOccur(r),
          randomOccur(r), r.nextFloat());
      int leftTerms = atLeast(r, 2);
      for (int i = 0; i < leftTerms; i++) {
        left.add(new Term(TestUtil.randomRealisticUnicodeString(r), TestUtil
            .randomRealisticUnicodeString(r)));
      }
      left.setHighFreqMinimumNumberShouldMatch(r.nextInt(4));
      left.setLowFreqMinimumNumberShouldMatch(r.nextInt(4));
      
      r = new Random(seed);
      CommonTermsQuery right = new CommonTermsQuery(randomOccur(r),
          randomOccur(r), r.nextFloat());
      int rightTerms = atLeast(r, 2);
      for (int i = 0; i < rightTerms; i++) {
        right.add(new Term(TestUtil.randomRealisticUnicodeString(r), TestUtil
            .randomRealisticUnicodeString(r)));
      }
      right.setHighFreqMinimumNumberShouldMatch(r.nextInt(4));
      right.setLowFreqMinimumNumberShouldMatch(r.nextInt(4));
      QueryUtils.checkEqual(left, right);
    }
  }
  
  private static Occur randomOccur(Random random) {
    return random.nextBoolean() ? Occur.MUST : Occur.SHOULD;
  }
  
  public void testNullTerm() {
    Random random = random();
    CommonTermsQuery query = new CommonTermsQuery(randomOccur(random),
        randomOccur(random), random().nextFloat());
    // null values are not supported
    expectThrows(IllegalArgumentException.class, () -> {
      query.add(null);
    });
  }
  
  public void testMinShouldMatch() throws IOException {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, analyzer);
    String[] docs = new String[] {"this is the end of the world right",
        "is this it or maybe not",
        "this is the end of the universe as we know it",
        "there is the famous restaurant at the end of the universe",};
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i, Field.Store.YES));
      doc.add(newTextField("field", docs[i], Field.Store.NO));
      w.addDocument(doc);
    }
    
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      query.setLowFreqMinimumNumberShouldMatch(0.5f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 1);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
    }
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      query.setLowFreqMinimumNumberShouldMatch(2.0f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 1);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
    }
    
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      query.setLowFreqMinimumNumberShouldMatch(0.49f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 3);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
      assertEquals("2", r.document(search.scoreDocs[1].doc).get("id"));
      assertEquals("3", r.document(search.scoreDocs[2].doc).get("id"));
    }
    
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      query.setLowFreqMinimumNumberShouldMatch(1.0f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 3);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
      assertEquals("2", r.document(search.scoreDocs[1].doc).get("id"));
      assertEquals("3", r.document(search.scoreDocs[2].doc).get("id"));
      assertTrue(search.scoreDocs[1].score >= search.scoreDocs[2].score);
    }
    
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      query.setLowFreqMinimumNumberShouldMatch(1.0f);
      query.setHighFreqMinimumNumberShouldMatch(4.0f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 3);
      assertEquals(search.scoreDocs[1].score, search.scoreDocs[2].score, 0.0f);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
      // doc 2 and 3 only get a score from low freq terms
      assertEquals(
          new HashSet<>(Arrays.asList("2", "3")),
          new HashSet<>(Arrays.asList(
              r.document(search.scoreDocs[1].doc).get("id"),
              r.document(search.scoreDocs[2].doc).get("id"))));
    }
    
    {
      // only high freq terms around - check that min should match is applied
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "the"));
      query.setLowFreqMinimumNumberShouldMatch(1.0f);
      query.setHighFreqMinimumNumberShouldMatch(2.0f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 4);
    }
    
    {
      // only high freq terms around - check that min should match is applied
      CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "the"));
      query.setLowFreqMinimumNumberShouldMatch(1.0f);
      query.setHighFreqMinimumNumberShouldMatch(2.0f);
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 2);
      assertEquals(
          new HashSet<>(Arrays.asList("0", "2")),
          new HashSet<>(Arrays.asList(
              r.document(search.scoreDocs[0].doc).get("id"),
              r.document(search.scoreDocs[1].doc).get("id"))));
    }
    IOUtils.close(r, w, dir, analyzer);
  }
  
  /** MUST_NOT is not supported */
  public void testIllegalOccur() {
    Random random = random();
    
    expectThrows(IllegalArgumentException.class, () -> {
      new CommonTermsQuery(Occur.MUST_NOT, randomOccur(random), random()
          .nextFloat());
    });
      
    expectThrows(IllegalArgumentException.class, () -> {
      new CommonTermsQuery(randomOccur(random), Occur.MUST_NOT, random()
          .nextFloat());
    });
  }

  @Test
  public void testExtend() throws IOException {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, analyzer);
    String[] docs = new String[] {"this is the end of the world right",
        "is this it or maybe not",
        "this is the end of the universe as we know it",
        "there is the famous restaurant at the end of the universe",};
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i, Field.Store.YES));
      doc.add(newTextField("field", docs[i], Field.Store.NO));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    // don't use a randomized similarity, e.g. stopwords for DFI can get scored as 0,
    // so boosting them is kind of crazy
    s.setSimilarity(new BM25Similarity());
    {
      CommonTermsQuery query = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 3);
      assertEquals("0", r.document(search.scoreDocs[0].doc).get("id"));
      assertEquals("2", r.document(search.scoreDocs[1].doc).get("id"));
      assertEquals("3", r.document(search.scoreDocs[2].doc).get("id"));
    }

    {
      // this one boosts the termQuery("field" "universe") by 10x
      CommonTermsQuery query = new ExtendedCommonTermsQuery(Occur.SHOULD, Occur.SHOULD,
          random().nextBoolean() ? 2.0f : 0.5f);
      query.add(new Term("field", "is"));
      query.add(new Term("field", "this"));
      query.add(new Term("field", "end"));
      query.add(new Term("field", "world"));
      query.add(new Term("field", "universe"));
      query.add(new Term("field", "right"));
      TopDocs search = s.search(query, 10);
      assertEquals(search.totalHits.value, 3);
      assertEquals("2", r.document(search.scoreDocs[0].doc).get("id"));
      assertEquals("3", r.document(search.scoreDocs[1].doc).get("id"));
      assertEquals("0", r.document(search.scoreDocs[2].doc).get("id"));
    }
    IOUtils.close(r, w, dir, analyzer);
  }
  
  public void testRandomIndex() throws IOException {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, analyzer);
    createRandomIndex(atLeast(50), w, random().nextLong());
    w.forceMerge(1);
    DirectoryReader reader = w.getReader();
    LeafReader wrapper = getOnlyLeafReader(reader);
    String field = "body";
    Terms terms = wrapper.terms(field);
    PriorityQueue<TermAndFreq> lowFreqQueue = new PriorityQueue<CommonTermsQueryTest.TermAndFreq>(
        5) {
      
      @Override
      protected boolean lessThan(TermAndFreq a, TermAndFreq b) {
        return a.freq > b.freq;
      }
      
    };
    PriorityQueue<TermAndFreq> highFreqQueue = new PriorityQueue<CommonTermsQueryTest.TermAndFreq>(
        5) {
      
      @Override
      protected boolean lessThan(TermAndFreq a, TermAndFreq b) {
        return a.freq < b.freq;
      }
      
    };
    try {
      TermsEnum iterator = terms.iterator();
      while (iterator.next() != null) {
        if (highFreqQueue.size() < 5) {
          highFreqQueue.add(new TermAndFreq(
              BytesRef.deepCopyOf(iterator.term()), iterator.docFreq()));
          lowFreqQueue.add(new TermAndFreq(
              BytesRef.deepCopyOf(iterator.term()), iterator.docFreq()));
        } else {
          if (highFreqQueue.top().freq < iterator.docFreq()) {
            highFreqQueue.top().freq = iterator.docFreq();
            highFreqQueue.top().term = BytesRef.deepCopyOf(iterator.term());
            highFreqQueue.updateTop();
          }
          
          if (lowFreqQueue.top().freq > iterator.docFreq()) {
            lowFreqQueue.top().freq = iterator.docFreq();
            lowFreqQueue.top().term = BytesRef.deepCopyOf(iterator.term());
            lowFreqQueue.updateTop();
          }
        }
      }
      int lowFreq = lowFreqQueue.top().freq;
      int highFreq = highFreqQueue.top().freq;
      assumeTrue("unlucky index", highFreq - 1 > lowFreq);
      List<TermAndFreq> highTerms = queueToList(highFreqQueue);
      List<TermAndFreq> lowTerms = queueToList(lowFreqQueue);
      
      IndexSearcher searcher = newSearcher(reader);
      Occur lowFreqOccur = randomOccur(random());
      BooleanQuery.Builder verifyQuery = new BooleanQuery.Builder();
      CommonTermsQuery cq = new CommonTermsQuery(randomOccur(random()),
          lowFreqOccur, highFreq - 1);
      for (TermAndFreq termAndFreq : lowTerms) {
        cq.add(new Term(field, termAndFreq.term));
        verifyQuery.add(new BooleanClause(new TermQuery(new Term(field,
            termAndFreq.term)), lowFreqOccur));
      }
      for (TermAndFreq termAndFreq : highTerms) {
        cq.add(new Term(field, termAndFreq.term));
      }
      
      TopDocs cqSearch = searcher.search(cq, reader.maxDoc());
      
      TopDocs verifySearch = searcher.search(verifyQuery.build(), reader.maxDoc());
      assertEquals(verifySearch.totalHits.value, cqSearch.totalHits.value);
      Set<Integer> hits = new HashSet<>();
      for (ScoreDoc doc : verifySearch.scoreDocs) {
        hits.add(doc.doc);
      }
      
      for (ScoreDoc doc : cqSearch.scoreDocs) {
        assertTrue(hits.remove(doc.doc));
      }
      
      assertTrue(hits.isEmpty());
      
      /*
       *  need to force merge here since QueryUtils adds checks based
       *  on leave readers which have different statistics than the top
       *  level reader if we have more than one segment. This could 
       *  result in a different query / results.
       */
      w.forceMerge(1); 
      DirectoryReader reader2 = w.getReader();
      QueryUtils.check(random(), cq, newSearcher(reader2));
      reader2.close();
    } finally {
      IOUtils.close(reader, w, dir, analyzer);
    }
    
  }
  
  private static List<TermAndFreq> queueToList(PriorityQueue<TermAndFreq> queue) {
    List<TermAndFreq> terms = new ArrayList<>();
    while (queue.size() > 0) {
      terms.add(queue.pop());
    }
    return terms;
  }
  
  private static class TermAndFreq {
    BytesRef term;
    int freq;
    
    public TermAndFreq(BytesRef term, int freq) {
      this.term = term;
      this.freq = freq;
      
    }
    
  }
  
  /**
   * populates a writer with random stuff. this must be fully reproducable with
   * the seed!
   */
  public static void createRandomIndex(int numdocs, RandomIndexWriter writer,
      long seed) throws IOException {
    Random random = new Random(seed);
    // primary source for our data is from linefiledocs, it's realistic.
    LineFileDocs lineFileDocs = new LineFileDocs(random);
    
    // TODO: we should add other fields that use things like docs&freqs but omit
    // positions,
    // because linefiledocs doesn't cover all the possibilities.
    for (int i = 0; i < numdocs; i++) {
      writer.addDocument(lineFileDocs.nextDoc());
    }
    
    lineFileDocs.close();
  }

  private static final class ExtendedCommonTermsQuery extends CommonTermsQuery {

    public ExtendedCommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency) {
      super(highFreqOccur, lowFreqOccur, maxTermFrequency);
    }

    @Override
    protected Query newTermQuery(Term term, TermStates termStates) {
      Query query = super.newTermQuery(term, termStates);
      if (term.text().equals("universe")) {
        query = new BoostQuery(query, 100f);
      }
      return query;
    }
  }
}
