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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Weight.ScorerContext;
import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;

/**
 * This class contains tests related to {@link TermQuery}
 */
public class TestTermQuery extends LuceneTestCase {

  private String fieldName = "field";

  /**
   * Simple testcase for {@link TermScorer#positions()}
   */
  public void testPositionsSimple() throws IOException {
    Directory directory = newDirectory();

    final Analyzer analyzer = new MockAnalyzer(random,
        MockTokenizer.WHITESPACE, false, MockTokenFilter.EMPTY_STOPSET, true);
    RandomIndexWriter writer = new RandomIndexWriter(random, directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    for (int i = 0; i < 39; i++) {
      Document doc = new Document();
      doc.add(newField(fieldName, "1 2 3 4 5 6 7 8 9 10 "
          + "1 2 3 4 5 6 7 8 9 10 " + "1 2 3 4 5 6 7 8 9 10 "
          + "1 2 3 4 5 6 7 8 9 10", Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    DocsAndPositionsEnum docsAndPositions = MultiFields.getTerms(reader,
        fieldName).docsAndPositions(null, new BytesRef("1"), null);
    assertEquals(39, reader.docFreq(new Term(fieldName, "1")));
    docsAndPositions.nextDoc();
    docsAndPositions.nextPosition();
    boolean payloadsIndexed = docsAndPositions.hasPayload();

    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();

    for (int i = 0; i < 39 * RANDOM_MULTIPLIER; i++) {
      TermQuery one = new TermQuery(new Term(fieldName, "1"));
      ReaderContext topReaderContext = reader.getTopReaderContext();
      AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
      Weight weight = one.createWeight(searcher);
      for (AtomicReaderContext atomicReaderContext : leaves) {
        Scorer scorer = weight.scorer(atomicReaderContext, ScorerContext.def()
            .needsPositions(true).needsPayloads(payloadsIndexed));
        assertNotNull(scorer);
        int toDoc = 1 + random.nextInt(atomicReaderContext.reader.docFreq(new Term(fieldName, "1")) - 1 );
        final int advance = scorer.advance(toDoc);
        PositionIntervalIterator positions = scorer.positions();

        do {
          assertEquals(scorer.docID(), positions.advanceTo(scorer.docID()));

          PositionInterval interval = null;
          String msg = "Advanced to: " + advance + " current doc: "
              + scorer.docID() + " usePayloads: " + payloadsIndexed;
          assertNotNull(msg, (interval = positions.next()));
          assertEquals(msg, 4.0f, positions.getScorer().freq(), 0.0f);

          assertEquals(msg, 0, interval.begin);
          assertEquals(msg, 0, interval.end);
          checkPayload(0, interval, payloadsIndexed);

          assertNotNull(msg, (interval = positions.next()));
          assertEquals(msg, 4.0f, positions.getScorer().freq(), 0.0f);
          assertEquals(msg, 10, interval.begin);
          assertEquals(msg, 10, interval.end);
          checkPayload(10, interval, payloadsIndexed);

          assertNotNull(msg, (interval = positions.next()));
          assertEquals(msg, 4.0f, positions.getScorer().freq(), 0.0f);
          assertEquals(msg, 20, interval.begin);
          assertEquals(msg, 20, interval.end);
          checkPayload(20, interval, payloadsIndexed);

          assertNotNull(msg, (interval = positions.next()));
          assertEquals(msg, 4.0f, positions.getScorer().freq(), 0.0f);
          assertEquals(msg, 30, interval.begin);
          assertEquals(msg, 30, interval.end);
          checkPayload(30, interval, payloadsIndexed);

          assertNull(msg, (interval = positions.next()));
        } while (scorer.nextDoc() != Scorer.NO_MORE_DOCS);
      }
    }
    searcher.close();
    reader.close();
    directory.close();
  }

  public final void checkPayload(int pos, PositionInterval interval,
      boolean payloadsIndexed) throws IOException {
    if (payloadsIndexed) {
      boolean wasPayloadAvailable = interval.payloadAvailable();
      BytesRef bytes = new BytesRef();
      assertTrue(interval.nextPayload(bytes));
      assertFalse(interval.payloadAvailable());
      if (!wasPayloadAvailable) {
        // if payload has 0 length interval or rather docs&pos enum will treat is as not existing
        assertEquals(0, bytes.length);
      } else {
        assertTrue(bytes.length > 0);
      }
    } else {
      assertFalse(interval.payloadAvailable());
    }
  }

  /**
   * this test indexes random numbers within a range into a field and checks
   * their occurrences by searching for a number from that range selected at
   * random. All positions for that number are safed up front and compared to
   * the terms scorers positions.
   * 
   */
  public void testRandomPositons() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    int numDocs = 131;
    int max = 1051;
    int term = random.nextInt(max);
    Integer[][] positionsInDoc = new Integer[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      ArrayList<Integer> positions = new ArrayList<Integer>();
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < 3049; j++) {
        int nextInt = random.nextInt(max);
        builder.append(nextInt).append(" ");
        if (nextInt == term) {
          positions.add(Integer.valueOf(j));
        }
      }
      doc.add(newField(fieldName, builder.toString(), Field.Store.YES,
          Field.Index.ANALYZED));
      positionsInDoc[i] = positions.toArray(new Integer[0]);
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    for (int i = 0; i < 39 * RANDOM_MULTIPLIER; i++) {
      TermQuery one = new TermQuery(new Term(fieldName, "" + term));
      ReaderContext topReaderContext = reader.getTopReaderContext();
      AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
      Weight weight = one.createWeight(searcher);
      for (AtomicReaderContext atomicReaderContext : leaves) {
        Scorer scorer = weight.scorer(atomicReaderContext, ScorerContext.def()
            .needsPositions(true));
        assertNotNull(scorer);
        int initDoc = 0;
        int maxDoc = atomicReaderContext.reader.maxDoc();
        // initially advance or do next doc
        if (random.nextBoolean()) {
          initDoc = scorer.nextDoc();
        } else {
          initDoc = scorer.advance(random.nextInt(maxDoc));
        }
        // now run through the scorer and check if all positions are there...
        do {
          int docID = scorer.docID();
          if (docID == Scorer.NO_MORE_DOCS) {
            break;
          }
          PositionIntervalIterator positions = scorer.positions();
          Integer[] pos = positionsInDoc[atomicReaderContext.docBase + docID];

          assertEquals((float) pos.length, positions.getScorer().freq(), 0.0f);
          // number of positions read should be random - don't read all of them
          // allways
          final int howMany = random.nextInt(20) == 0 ? pos.length
              - random.nextInt(pos.length) : pos.length;
          PositionInterval interval = null;
          assertEquals(scorer.docID(), positions.advanceTo(scorer.docID()));
          for (int j = 0; j < howMany; j++) {
            assertNotNull((interval = positions.next()));
            assertEquals("iteration: " + i + " initDoc: " + initDoc + " doc: "
                + docID + " base: " + atomicReaderContext.docBase
                + " positions: " + Arrays.toString(pos), pos[j].intValue(),
                interval.begin);
            assertEquals(pos[j].intValue(), interval.end);
          }
          if (howMany == pos.length) {
            assertNull((interval = positions.next()));
          }

          if (random.nextInt(10) == 0) { // once is a while advance
            scorer.advance(docID + 1 + random.nextInt((maxDoc - docID)));
          }

        } while (scorer.nextDoc() != Scorer.NO_MORE_DOCS);
      }

    }
    reader.close();
    dir.close();
  }

  /**
   * tests retrieval of positions for terms that have a large number of
   * occurrences to force test of buffer refill during positions iteration.
   */
  public void testLargeNumberOfPositions() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    int howMany = 1000;
    for (int i = 0; i < 39; i++) {
      Document doc = new Document();
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < howMany; j++) {
        if (j % 2 == 0) {
          builder.append("even ");
        } else {
          builder.append("odd ");
        }
      }
      doc.add(newField(fieldName, builder.toString(), Field.Store.YES,
          Field.Index.ANALYZED));
      writer.addDocument(doc);
    }

    // now do seaches
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    for (int i = 0; i < 39 * RANDOM_MULTIPLIER; i++) {
      TermQuery one = new TermQuery(new Term(fieldName, "even"));
      ReaderContext topReaderContext = reader.getTopReaderContext();
      AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
      Weight weight = one.createWeight(searcher);
      PositionInterval interval = null;
      for (AtomicReaderContext atomicReaderContext : leaves) {
        Scorer scorer = weight.scorer(atomicReaderContext, ScorerContext.def()
            .needsPositions(true));
        assertNotNull(scorer);

        int initDoc = 0;
        int maxDoc = atomicReaderContext.reader.maxDoc();
        // initially advance or do next doc
        if (random.nextBoolean()) {
          initDoc = scorer.nextDoc();
        } else {
          initDoc = scorer.advance(random.nextInt(maxDoc));
        }
        String msg = "Iteration: " + i + " initDoc: " + initDoc;
        PositionIntervalIterator positions = scorer.positions();
        assertEquals(howMany / 2.f, positions.getScorer().freq(), 0.0);
        assertEquals(scorer.docID(), positions.advanceTo(scorer.docID()));
        for (int j = 0; j < howMany; j += 2) {
          assertNotNull("next returned nullat index: " + j + " with freq: "
              + positions.getScorer().freq() + " -- " + msg,
              (interval = positions.next()));
          assertEquals("position missmatch index: " + j + " with freq: "
              + positions.getScorer().freq() + " -- " + msg, j, interval.begin);
        }
        assertNull("next returned nonNull -- " + msg,
            (interval = positions.next()));

      }
    }
    reader.close();
    dir.close();
  }

}