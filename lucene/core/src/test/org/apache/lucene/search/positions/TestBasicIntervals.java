package org.apache.lucene.search.positions;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Weight.PostingFeatures;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestBasicIntervals extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;
  
  public static final String field = "field";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
            .setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(field, docFields[i], TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  private String[] docFields = {
      "w1 w2 w3 w4 w5", //0
      "w1 w3 w2 w3",//1
      "w1 xx w2 yy w3",//2
      "w1 w3 xx w2 yy w3",//3
      "u2 u2 u1", //4
      "u2 xx u2 u1",//5
      "u2 u2 xx u1", //6
      "u2 xx u2 yy u1", //7
      "u2 xx u1 u2",//8
      "u1 u2 xx u2",//9
      "u2 u1 xx u2",//10
      "t1 t2 t1 t3 t2 t3"};//11
  
  public TermQuery makeTermQuery(String text) {
    return new TermQuery(new Term(field, text));
  }
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, field, searcher, results);
  }
  
  private void orderedSlopTest3SQ(Query q1, Query q2, Query q3, int slop,
      int[] expectedDocs) throws IOException {
    BooleanQuery query = new BooleanQuery();
    query.add(q1, Occur.MUST);
    query.add(q2, Occur.MUST);
    query.add(q3, Occur.MUST);
    Query snq = new IntervalFilterQuery(query, new WithinOrderedFilter(slop));
    checkHits(snq, expectedDocs);
  }
  
  public void orderedSlopTest3(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(makeTermQuery("w1"), makeTermQuery("w2"),
        makeTermQuery("w3"), slop, expectedDocs);
  }
  
  public void orderedSlopTest3Equal(int slop, int[] expectedDocs)
      throws IOException {
    orderedSlopTest3SQ(makeTermQuery("w1"), makeTermQuery("w3"),
        makeTermQuery("w3"), slop, expectedDocs);
  }
  
  public void orderedSlopTest1Equal(int slop, int[] expectedDocs)
      throws IOException {
    orderedSlopTest3SQ(makeTermQuery("u2"), makeTermQuery("u2"),
        makeTermQuery("u1"), slop, expectedDocs);
  }
  
  public void testNearOrdered01() throws Exception {
    orderedSlopTest3(0, new int[] {0});
  }
  
  public void testNearOrdered02() throws Exception {
    orderedSlopTest3(1, new int[] {0, 1});
  }
  
  public void testNearOrdered03() throws Exception {
    orderedSlopTest3(2, new int[] {0, 1, 2});
  }
  
  public void testNearOrdered04() throws Exception {
    orderedSlopTest3(3, new int[] {0, 1, 2, 3});
  }
  
  public void testNearOrdered05() throws Exception {
    orderedSlopTest3(4, new int[] {0, 1, 2, 3});
  }
  
  public void testNearOrderedEqual01() throws Exception {
    orderedSlopTest3Equal(0, new int[] {});
  }
  
  public void testNearOrderedEqual02() throws Exception {
    orderedSlopTest3Equal(1, new int[] {1});
  }
  
  public void testNearOrderedEqual03() throws Exception {
    orderedSlopTest3Equal(2, new int[] {1});
  }
  
  public void testNearOrderedEqual04() throws Exception {
    orderedSlopTest3Equal(3, new int[] {1, 3});
  }
  
  public void testNearOrderedEqual11() throws Exception {
    orderedSlopTest1Equal(0, new int[] {4});
  }
  
  public void testNearOrderedEqual12() throws Exception {
    orderedSlopTest1Equal(0, new int[] {4});
  }
  
  public void testNearOrderedEqual13() throws Exception {
    orderedSlopTest1Equal(1, new int[] {4, 5, 6});
  }
  
  public void testNearOrderedEqual14() throws Exception {
    orderedSlopTest1Equal(2, new int[] {4, 5, 6, 7});
  }
  
  public void testNearOrderedEqual15() throws Exception {
    orderedSlopTest1Equal(3, new int[] {4, 5, 6, 7});
  }
  
   public void testNearOrderedOverlap() throws Exception {
   BooleanQuery query = new BooleanQuery(); //"t1 t2 t1 t3 t2 t3"
   query.add(new BooleanClause(new TermQuery(new Term(field, "t1")),
   Occur.MUST));
   query.add(new BooleanClause(new TermQuery(new Term(field, "t2")),
   Occur.MUST));
   query.add(new BooleanClause(new TermQuery(new Term(field, "t3")),
   Occur.MUST));
   IntervalFilterQuery positionFilterQuery = new IntervalFilterQuery( query, new WithinOrderedFilter(3));
   
   Query rewrite = this.searcher.rewrite(positionFilterQuery);
   AtomicReader r = this.reader.getContext().leaves().get(0).reader();
   Weight createWeight = rewrite.createWeight(new IndexSearcher(r));
   
   Scorer scorer = createWeight.scorer(r.getContext(), random()
       .nextBoolean(), true, PostingFeatures.POSITIONS, r.getLiveDocs());
   IntervalIterator positions = scorer.intervals(false);
   positions.scorer.advance(11);
   positions.scorerAdvanced(11);
   Interval interval = positions.next();
   assertNotNull("first range", interval);
   assertEquals("first doc", 11, positions.docID());
   assertEquals("first start", 0, interval.begin);
   assertEquals("first end", 3, interval.end);
  
   
   interval = positions.next();
   assertNotNull("second range", interval);
   assertEquals("second doc", 11, positions.docID());
   assertEquals("second start", 2, interval.begin);
   assertEquals("second end", 5, interval.end);
  
   assertNull("third range", positions.next());
   }

  public static class BlockPositionIteratorFilter implements IntervalFilter {

    @Override
    public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
      return new BlockIntervalIterator(collectIntervals, iter);
    }
    
  }
  
  private int advanceIter(IntervalIterator iter, int pos) throws IOException {
    return iter.scorerAdvanced(iter.scorer.advance(pos));
  }
  public void testNearUnOrdered() throws Exception {
    {
      BooleanQuery query = new BooleanQuery();
      query.add(makeTermQuery("u1"), Occur.MUST);
      query.add(makeTermQuery("u2"), Occur.MUST);
      Query snq = new IntervalFilterQuery(query, new WithinIntervalIterator(
          0));
      Query rewrite = this.searcher.rewrite(snq);
      AtomicReader r = this.reader.getContext().leaves().get(0).reader();
      Weight createWeight = rewrite.createWeight(new IndexSearcher(r));
      
      Scorer scorer = createWeight.scorer(r.getContext(), random()
          .nextBoolean(), true, PostingFeatures.POSITIONS, r.getLiveDocs());
      IntervalIterator positions = scorer.intervals(false);
      advanceIter(positions, 4);

      Interval interval = positions.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 4, positions.docID());
      assertEquals("start " + interval, 1, interval.begin);
      assertEquals("end", 2, interval.end);
      
      advanceIter(positions, 5);
      interval = positions.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 5, positions.docID());
      assertEquals("start", 2, interval.begin);
      assertEquals("end", 3, interval.end);
      
      advanceIter(positions, 8);
      interval = positions.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 8, positions.docID());
      assertEquals("start", 2, interval.begin);
      assertEquals("end", 3, interval.end);
      
      advanceIter(positions, 9);
      interval = positions.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 9, positions.docID());
      assertEquals("start", 0, interval.begin);
      assertEquals("end", 1, interval.end);
      
      advanceIter(positions, 10);
      interval = positions.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 10, positions.docID());
      assertEquals("start", 0, interval.begin);
      assertEquals("end", 1, interval.end);
      
     
      assertNull("Has next and it shouldn't: " + positions.docID(), positions.next());
    }
    
    {
      // ((u1 near u2) near u2)
      BooleanQuery query = new BooleanQuery();
      query.add(makeTermQuery("u1"), Occur.MUST);
      query.add(makeTermQuery("u2"), Occur.MUST);
      Query nearQuery = new IntervalFilterQuery(query,
          new WithinIntervalIterator(0));
      
      BooleanQuery topLevel = new BooleanQuery();
      topLevel.add(nearQuery, Occur.MUST);
      topLevel.add(makeTermQuery("u2"), Occur.MUST);


      Query rewrite = this.searcher.rewrite(new IntervalFilterQuery(topLevel,
          new WithinIntervalIterator(1)));
      AtomicReader r = this.reader.getContext().leaves().get(0).reader();
      Weight createWeight = rewrite.createWeight(new IndexSearcher(r));
      Scorer scorer = createWeight.scorer(r.getContext(), random()
          .nextBoolean(), true, PostingFeatures.POSITIONS, r.getLiveDocs());
      
      IntervalIterator iterator = scorer.intervals(false);
      assertEquals(4, advanceIter(iterator, 4));
      Interval interval = iterator.next();

      assertNotNull("Does not have next and it should", interval);
      // unordered spans can be subsets
      assertEquals("doc", 4, iterator.docID());
      assertEquals("start", 1, interval.begin);
      assertEquals("end", 2, interval.end);
      
      advanceIter(iterator, 5);


      interval = iterator.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 5, iterator.docID());
      assertEquals("start", 2, interval.begin);
      assertEquals("end", 3, interval.end);
      
      advanceIter(iterator, 8); // (u2 xx (u1 u2))

      interval = iterator.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 8, iterator.docID());
      assertEquals("start", 2, interval.begin);
      assertEquals("end", 3, interval.end);
      
      advanceIter(iterator, 9); // u2 u1 xx u2
      interval = iterator.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 9, iterator.docID());
      assertEquals("start", 0, interval.begin);
      assertEquals("end", 1, interval.end);
      
      interval = iterator.next();
      assertNull("Has next and it shouldn't", interval);
      
      advanceIter(iterator, 10);
      interval = iterator.next();
      assertNotNull("Does not have next and it should", interval);
      assertEquals("doc", 10, iterator.docID());
      assertEquals("start", 0, interval.begin);
      assertEquals("end", 1, interval.end);
      
      interval = iterator.next();
      assertNull("Has next and it shouldn't " + interval, interval);
    }
  }
  
  private IntervalIterator orIterator(String[] terms) throws Exception {
    BooleanQuery query = new BooleanQuery();
    
    for (int i = 0; i < terms.length; i++) {
      query.add(makeTermQuery(terms[i]), Occur.SHOULD);
    }
    Query rewrite = this.searcher.rewrite(query);
    AtomicReader r = this.reader.getContext().leaves().get(0).reader();
    Weight createWeight = rewrite.createWeight(new IndexSearcher(r));
    
    Scorer scorer = createWeight.scorer(r.getContext(), true, true, PostingFeatures.POSITIONS, r.getLiveDocs());
    return scorer.intervals(false);
  }
  
  private IntervalIterator tstNextPosition(
      IntervalIterator iterator, int doc, int start, int end)
      throws Exception {
    if (iterator.docID() != doc) {
      iterator.scorer.advance(doc);
      iterator.scorerAdvanced(doc);
    }
    assertEquals("doc", doc, iterator.docID());
    Interval next = iterator.next();
    assertNotNull("next", next);
    assertEquals("begin", start, next.begin);
    assertEquals("end", end, next.end + 1);
    return iterator;
  }
  
  public void testOrSingle() throws Exception {
    IntervalIterator spans = orIterator(new String[] {"w5"});
    tstNextPosition(spans, 0, 4, 5);
    assertNull("final next", spans.next());
  }
  
  public void testOrMovesForward() throws Exception {
    IntervalIterator iterator = orIterator(new String[] {"w1", "xx"});
    advanceIter(iterator, 0);
    assertNotNull(iterator.next());
    int doc = iterator.docID();
    assertEquals(0, doc);
    assertEquals(1, advanceIter(iterator, 1));
    
  }
  
  public void testSpanOrDouble() throws Exception {
    IntervalIterator iterator = orIterator(new String[] {"w5", "yy"});
    tstNextPosition(iterator, 0, 4, 5);
    tstNextPosition(iterator, 2, 3, 4);
    tstNextPosition(iterator, 3, 4, 5);
    tstNextPosition(iterator, 7, 3, 4);
    assertNull("final next", iterator.next());
  }
  
  public void testOrDoubleSkip() throws Exception {
    IntervalIterator iterator = orIterator(new String[] {"w5", "yy"});
    iterator.scorer.advance(3);
    assertEquals("initial skipTo", 3, iterator.scorerAdvanced(3));
    assertEquals("doc", 3, iterator.docID());
    Interval next = iterator.next();
    assertEquals("start", 4, next.begin);
    assertEquals("end", 4, next.end);
    tstNextPosition(iterator, 7, 3, 4);
    assertNull("final next", iterator.next());
  }
  
  public void testOrUnused() throws Exception {
    IntervalIterator iterator = orIterator(new String[] {"w5",
        "unusedTerm", "yy"});
    tstNextPosition(iterator, 0, 4, 5);
    tstNextPosition(iterator, 2, 3, 4);
    tstNextPosition(iterator, 3, 4, 5);
    tstNextPosition(iterator, 7, 3, 4);
    assertNull("final next", iterator.next());
  }
  
  public void testOrTripleSameDoc() throws Exception {
    IntervalIterator iterator = orIterator(new String[] {"t1", "t2",
        "t3"});
    tstNextPosition(iterator, 11, 0, 1);
    tstNextPosition(iterator, 11, 1, 2);
    tstNextPosition(iterator, 11, 2, 3);
    tstNextPosition(iterator, 11, 3, 4);
    tstNextPosition(iterator, 11, 4, 5);
    tstNextPosition(iterator, 11, 5, 6);
    assertNull("final next", iterator.next());
  }
  
}
