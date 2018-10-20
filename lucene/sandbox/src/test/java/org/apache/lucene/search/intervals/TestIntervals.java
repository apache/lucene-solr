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

package org.apache.lucene.search.intervals;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestIntervals extends LuceneTestCase {

  //   0         1         2         3         4         5         6         7         8         9
  //   012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  private static String field1_docs[] = {
      "Nothing of interest to anyone here",
      "Pease porridge hot, pease porridge cold, pease porridge in the pot nine days old.  Some like it hot, some like it cold, some like it in the pot nine days old",
      "Pease porridge cold, pease porridge hot, pease porridge in the pot twelve days old.  Some like it cold, some like it hot, some like it in the fraggle",
      "Nor here, nowt hot going on in pease this one",
      "Pease porridge hot, pease porridge cold, pease porridge in the pot nine years old.  Some like it hot, some like it twelve",
      "Porridge is great"
  };

  private static String field2_docs[] = {
      "In Xanadu did Kubla Khan a stately pleasure dome decree",
      "Where Alph the sacred river ran through caverns measureless to man",
      "Down to a sunless sea",
      "So thrice five miles of fertile ground",
      "Pease hot porridge porridge",
      "Pease porridge porridge hot"
  };

  private static Directory directory;
  private static IndexSearcher searcher;
  private static Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);

  private static final FieldType FIELD_TYPE = new FieldType(TextField.TYPE_STORED);
  static {
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  @BeforeClass
  public static void setupIndex() throws IOException {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < field1_docs.length; i++) {
      Document doc = new Document();
      doc.add(new Field("field1", field1_docs[i], FIELD_TYPE));
      doc.add(new TextField("field2", field2_docs[i], Field.Store.NO));
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      doc.add(new NumericDocValuesField("id", i));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(DirectoryReader.open(directory));
  }

  @AfterClass
  public static void teardownIndex() throws IOException {
    IOUtils.close(searcher.getIndexReader(), directory);
  }

  private void checkIntervals(IntervalsSource source, String field, int expectedMatchCount, int[][] expected) throws IOException {
    int matchedDocs = 0;
    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
      assertNull(source.intervals(field + "fake", ctx));
      NumericDocValues ids = DocValues.getNumeric(ctx.reader(), "id");
      IntervalIterator intervals = source.intervals(field, ctx);
      if (intervals == null)
        continue;
      for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
        ids.advance(doc);
        int id = (int) ids.longValue();
        if (intervals.docID() == doc ||
            (intervals.docID() < doc && intervals.advance(doc) == doc)) {
          int i = 0, pos;
          assertEquals(-1, intervals.start());
          assertEquals(-1, intervals.end());
          while ((pos = intervals.nextInterval()) != IntervalIterator.NO_MORE_INTERVALS) {
            if (i >= expected[id].length) {
              fail("Unexpected match in doc " + id + ": " + intervals);
            }
            assertEquals("Wrong start value in doc " + id, expected[id][i], pos);
            assertEquals("start() != pos returned from nextInterval()", expected[id][i], intervals.start());
            assertEquals("Wrong end value in doc " + id, expected[id][i + 1], intervals.end());
            i += 2;
          }
          assertEquals("Wrong number of endpoints in doc " + id, expected[id].length, i);
          if (i > 0)
            matchedDocs++;
        }
        else {
          assertEquals(0, expected[id].length);
        }
      }
    }
    assertEquals(expectedMatchCount, matchedDocs);
  }

  private MatchesIterator getMatches(IntervalsSource source, int doc, String field) throws IOException {
    int ord = ReaderUtil.subIndex(doc, searcher.getIndexReader().leaves());
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(ord);
    return source.matches(field, ctx, doc - ctx.docBase);
  }

  private void assertMatch(MatchesIterator mi, int start, int end, int startOffset, int endOffset) throws IOException {
    assertTrue(mi.next());
    assertEquals(start, mi.startPosition());
    assertEquals(end, mi.endPosition());
    assertEquals(startOffset, mi.startOffset());
    assertEquals(endOffset, mi.endOffset());
  }

  public void testIntervalsOnFieldWithNoPositions() throws IOException {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      Intervals.term("wibble").intervals("id", searcher.getIndexReader().leaves().get(0));
    });
    assertEquals("Cannot create an IntervalIterator over field id because it has no indexed positions", e.getMessage());
  }

  public void testTermQueryIntervals() throws IOException {
    IntervalsSource source = Intervals.term("porridge");
    checkIntervals(source, "field1", 4, new int[][]{
        {},
        { 1, 1, 4, 4, 7, 7 },
        { 1, 1, 4, 4, 7, 7 },
        {},
        { 1, 1, 4, 4, 7, 7 },
        { 0, 0 }
    });
    assertNull(getMatches(source, 0, "field1"));
    assertNull(getMatches(source, 2, "no_such_field"));
    MatchesIterator mi = getMatches(source, 2, "field1");
    assertMatch(mi, 1, 1, 6, 14);
    assertMatch(mi, 4, 4, 27, 35);
    assertMatch(mi, 7, 7, 47, 55);
    assertFalse(mi.next());
  }

  public void testOrderedNearIntervals() throws IOException {
    IntervalsSource source = Intervals.ordered(Intervals.term("pease"), Intervals.term("hot"));
    checkIntervals(source, "field1", 3, new int[][]{
        {},
        { 0, 2, 6, 17 },
        { 3, 5, 6, 21 },
        {},
        { 0, 2, 6, 17 },
        { }
    });
    assertNull(getMatches(source, 3, "field1"));
    MatchesIterator mi = getMatches(source, 4, "field1");
    assertMatch(mi, 0, 2, 0, 18);
    MatchesIterator sub = mi.getSubMatches();
    assertMatch(sub, 0, 0, 0, 5);
    assertMatch(sub, 2, 2, 15, 18);
    assertFalse(sub.next());
    assertMatch(mi, 6, 17, 41, 100);
    sub = mi.getSubMatches();
    assertMatch(sub, 6, 6, 41, 46);
    assertMatch(sub, 17, 17, 97, 100);
    assertFalse(sub.next());
    assertFalse(mi.next());
  }

  public void testPhraseIntervals() throws IOException {
    IntervalsSource source = Intervals.phrase("pease", "porridge");
    checkIntervals(source, "field1", 3, new int[][]{
        {},
        { 0, 1, 3, 4, 6, 7 },
        { 0, 1, 3, 4, 6, 7 },
        {},
        { 0, 1, 3, 4, 6, 7 },
        {}
    });
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 1, 0, 14);
    assertMatch(mi, 3, 4, 20, 34);
    MatchesIterator sub = mi.getSubMatches();
    assertMatch(sub, 3, 3, 20, 25);
    assertMatch(sub, 4, 4, 26, 34);
    assertFalse(sub.next());
    assertMatch(mi, 6, 7, 41, 55);
  }

  public void testUnorderedNearIntervals() throws IOException {
    IntervalsSource source = Intervals.unordered(Intervals.term("pease"), Intervals.term("hot"));
    checkIntervals(source, "field1", 4, new int[][]{
            {},
            { 0, 2, 2, 3, 6, 17 },
            { 3, 5, 5, 6, 6, 21 },
            { 3, 7 },
            { 0, 2, 2, 3, 6, 17 },
            {}
        });
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 2, 0, 18);
    assertMatch(mi, 2, 3, 15, 25);
    assertMatch(mi, 6, 17, 41, 99);
    MatchesIterator sub = mi.getSubMatches();
    assertMatch(sub, 6, 6, 41, 46);
    assertMatch(sub, 17, 17, 96, 99);
    assertFalse(sub.next());
    assertFalse(mi.next());
  }

  public void testIntervalDisjunction() throws IOException {
    IntervalsSource source = Intervals.or(Intervals.term("pease"), Intervals.term("hot"), Intervals.term("notMatching"));
    checkIntervals(source, "field1", 4, new int[][]{
        {},
        { 0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
        { 0, 0, 3, 3, 5, 5, 6, 6, 21, 21},
        { 3, 3, 7, 7 },
        { 0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
        {}
    });
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator mi = getMatches(source, 3, "field1");
    assertMatch(mi, 3, 3, 15, 18);
    assertNull(mi.getSubMatches());
    assertMatch(mi, 7, 7, 31, 36);
    assertNull(mi.getSubMatches());
    assertFalse(mi.next());
  }

  public void testNesting() throws IOException {
    IntervalsSource source = Intervals.unordered(
        Intervals.term("pease"),
        Intervals.term("porridge"),
        Intervals.or(Intervals.term("hot"), Intervals.term("cold")));
    checkIntervals(source, "field1", 3, new int[][]{
        {},
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        {},
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        {}
    });
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 2, 0, 18);
    assertMatch(mi, 1, 3, 6, 25);
    assertMatch(mi, 2, 4, 15, 34);
    assertMatch(mi, 3, 5, 20, 39);
    MatchesIterator sub = mi.getSubMatches();
    assertMatch(sub, 3, 3, 20, 25);
    assertMatch(sub, 4, 4, 26, 34);
    assertMatch(sub, 5, 5, 35, 39);
    assertFalse(sub.next());
    assertMatch(mi, 4, 6, 26, 46);
    assertMatch(mi, 5, 7, 35, 55);
    assertMatch(mi, 6, 17, 41, 99);
    assertFalse(mi.next());
  }

  public void testNesting2() throws IOException {
    IntervalsSource source = Intervals.unordered(
        Intervals.ordered(
            Intervals.term("like"),
            Intervals.term("it"),
            Intervals.term("cold")
        ),
        Intervals.term("pease")
    );
    checkIntervals(source, "field1", 2, new int[][]{
            {},
            {6, 21},
            {6, 17},
            {},
            {},
            {}
        });
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator it = getMatches(source, 1, "field1");
    assertMatch(it, 6, 21, 41, 118);
    MatchesIterator sub = it.getSubMatches();
    assertMatch(sub, 6, 6, 41, 46);
    assertMatch(sub, 19, 19, 106, 110);
    assertMatch(sub, 20, 20, 111, 113);
    assertMatch(sub, 21, 21, 114, 118);
    assertFalse(sub.next());
    assertFalse(it.next());
  }

  public void testUnorderedDistinct() throws IOException {
    checkIntervals(Intervals.unordered(false, Intervals.term("pease"), Intervals.term("pease")),
        "field1", 3, new int[][]{
            {},
            { 0, 3, 3, 6 },
            { 0, 3, 3, 6 },
            {},
            { 0, 3, 3, 6 },
            {}
        });
    checkIntervals(Intervals.unordered(false,
        Intervals.unordered(Intervals.term("pease"), Intervals.term("porridge"), Intervals.term("hot")),
        Intervals.term("porridge")),
        "field1", 3, new int[][]{
            {},
            { 1, 4, 4, 17 },
            { 1, 5, 4, 7 },
            {},
            { 1, 4, 4, 17 },
            {}
        });
    checkIntervals(Intervals.unordered(false,
        Intervals.unordered(Intervals.term("pease"), Intervals.term("porridge"), Intervals.term("hot")),
        Intervals.term("porridge")),
        "field2", 1, new int[][]{
            {},
            {},
            {},
            {},
            { 0, 3 },
            {}
        });
  }

  public void testContainedBy() throws IOException {
    IntervalsSource source = Intervals.containedBy(
        Intervals.term("porridge"),
        Intervals.ordered(Intervals.term("pease"), Intervals.term("cold"))
    );
    checkIntervals(source, "field1", 3, new int[][]{
        {},
        { 4, 4, 7, 7 },
        { 1, 1, 7, 7 },
        {},
        { 4, 4 },
        {}
    });
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 4, 4, 20, 39);
    MatchesIterator subs = mi.getSubMatches();
    assertMatch(subs, 3, 3, 20, 25);
    assertMatch(subs, 4, 4, 26, 34);
    assertMatch(subs, 5, 5, 35, 39);
    assertFalse(subs.next());
    assertMatch(mi, 7, 7, 41, 118);
    subs = mi.getSubMatches();
    assertMatch(subs, 6, 6, 41, 46);
    assertMatch(subs, 7, 7, 47, 55);
    assertMatch(subs, 21, 21, 114, 118);
    assertFalse(subs.next());
    assertFalse(mi.next());
  }

  public void testContaining() throws IOException {
    IntervalsSource source = Intervals.containing(
        Intervals.ordered(Intervals.term("pease"), Intervals.term("cold")),
        Intervals.term("porridge")
    );
    checkIntervals(source, "field1", 3, new int[][]{
        {},
        { 3, 5, 6, 21 },
        { 0, 2, 6, 17 },
        {},
        { 3, 5 },
        {}
    });
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 3, 5, 20, 39);
    MatchesIterator subs = mi.getSubMatches();
    assertMatch(subs, 3, 3, 20, 25);
    assertMatch(subs, 4, 4, 26, 34);
    assertMatch(subs, 5, 5, 35, 39);
    assertFalse(subs.next());
    assertMatch(mi, 6, 21, 41, 118);
    subs = mi.getSubMatches();
    assertMatch(subs, 6, 6, 41, 46);
    assertMatch(subs, 7, 7, 47, 55);
    assertMatch(subs, 21, 21, 114, 118);
    assertFalse(subs.next());
    assertFalse(mi.next());
  }

  public void testNotContaining() throws IOException {
    IntervalsSource source = Intervals.notContaining(
        Intervals.ordered(Intervals.term("porridge"), Intervals.term("pease")),
        Intervals.term("hot")
    );
    checkIntervals(source, "field1", 3, new int[][]{
        {},
        { 4, 6 },
        { 1, 3 },
        {},
        { 4, 6 },
        {}
    });
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 4, 6, 26, 46);
    MatchesIterator subs = mi.getSubMatches();
    assertMatch(subs, 4, 4, 26, 34);
    assertMatch(subs, 6, 6, 41, 46);
    assertFalse(subs.next());
    assertFalse(mi.next());
  }

}
