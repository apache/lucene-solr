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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestIntervals extends LuceneTestCase {

  //   0         1         2         3         4         5         6         7         8         9
  //
  // 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  private static String field1_docs[] = {
    "Nothing of interest to anyone here",
    "Pease porridge hot, pease porridge cold, pease porridge in the pot nine days old.  Some like it hot, some like it cold, some like it in the pot nine days old",
    "Pease porridge cold, pease porridge hot, pease porridge in the pot twelve days old.  Some like it cold, some like it hot, some like it in the fraggle",
    "Nor here, nowt hot going on in pease this one",
    "Pease porridge hot, pease porridge cold, pease porridge in the pot nine years old.  Some like it hot, some like it twelve",
    "Porridge is great"
  };

  //   0         1         2         3         4         5         6         7         8         9
  //
  // 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  private static String field2_docs[] = {
    "In Xanadu did Kubla Khan a stately pleasure dome decree",
    "Where Alph the sacred river ran through caverns measureless to man",
    "a b a c b a b c",
    "So thrice five miles of fertile ground",
    "Pease hot porridge porridge",
    "w1 w2 w3 w4 w1 w6 w3 w8 w4 w7 w1 w6"
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
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < field1_docs.length; i++) {
      Document doc = new Document();
      doc.add(new Field("field1", field1_docs[i], FIELD_TYPE));
      doc.add(new Field("field2", field2_docs[i], FIELD_TYPE));
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

  private void checkIntervals(
      IntervalsSource source, String field, int expectedMatchCount, int[][] expected)
      throws IOException {
    int matchedDocs = 0;
    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
      assertNull(source.intervals(field + "fake", ctx));
      NumericDocValues ids = DocValues.getNumeric(ctx.reader(), "id");
      IntervalIterator intervals = source.intervals(field, ctx);
      if (intervals == null) {
        continue;
      }
      for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
        ids.advance(doc);
        MatchesIterator mi = source.matches(field, ctx, doc);
        int id = (int) ids.longValue();
        if (intervals.docID() == doc
            || (intervals.docID() < doc && intervals.advance(doc) == doc)) {
          int i = 0, pos;
          assertEquals(-1, intervals.start());
          assertEquals(-1, intervals.end());
          while ((pos = intervals.nextInterval()) != IntervalIterator.NO_MORE_INTERVALS) {
            if (i >= expected[id].length) {
              fail("Unexpected match in doc " + id + ": " + intervals);
            }
            assertEquals(source + ": wrong start value in doc " + id, expected[id][i], pos);
            assertEquals(
                "start() != pos returned from nextInterval()", expected[id][i], intervals.start());
            assertEquals("Wrong end value in doc " + id, expected[id][i + 1], intervals.end());
            i += 2;
            assertTrue(mi.next());
            assertEquals(
                source + ": wrong start value in match in doc " + id,
                intervals.start(),
                mi.startPosition());
            assertEquals(
                source + ": wrong end value in match in doc " + id,
                intervals.end(),
                mi.endPosition());
          }
          assertEquals(source + ": wrong number of endpoints in doc " + id, expected[id].length, i);
          assertEquals(IntervalIterator.NO_MORE_INTERVALS, intervals.start());
          assertEquals(IntervalIterator.NO_MORE_INTERVALS, intervals.end());
          if (i > 0) {
            matchedDocs++;
            assertFalse(mi.next());
          } else {
            assertNull("Expected null matches iterator on doc " + id, mi);
          }
        } else {
          assertEquals(0, expected[id].length);
          assertNull(mi);
        }
      }
    }
    assertEquals(expectedMatchCount, matchedDocs);
  }

  private void checkVisits(
      IntervalsSource source, int expectedVisitCount, String... expectedTerms) {
    Set<String> actualTerms = new HashSet<>();
    int[] visitedSources = new int[1];
    source.visit(
        "field",
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            visitedSources[0]++;
            actualTerms.addAll(Arrays.stream(terms).map(Term::text).collect(Collectors.toList()));
          }

          @Override
          public void visitLeaf(Query query) {
            visitedSources[0]++;
            super.visitLeaf(query);
          }

          @Override
          public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            visitedSources[0]++;
            return super.getSubVisitor(occur, parent);
          }
        });

    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedTerms));
    expectedSet.removeAll(actualTerms);
    actualTerms.removeAll(Arrays.asList(expectedTerms));
    assertEquals(expectedVisitCount, visitedSources[0]);
    assertTrue("Unexpected terms collected: " + actualTerms, actualTerms.isEmpty());
    assertTrue("Missing expected terms: " + expectedSet, expectedSet.isEmpty());
  }

  private MatchesIterator getMatches(IntervalsSource source, int doc, String field)
      throws IOException {
    int ord = ReaderUtil.subIndex(doc, searcher.getIndexReader().leaves());
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(ord);
    return source.matches(field, ctx, doc - ctx.docBase);
  }

  private void assertMatch(MatchesIterator mi, int start, int end, int startOffset, int endOffset)
      throws IOException {
    assertTrue(mi.next());
    assertEquals(start, mi.startPosition());
    assertEquals(end, mi.endPosition());
    assertEquals(startOffset, mi.startOffset());
    assertEquals(endOffset, mi.endOffset());
  }

  private void assertGaps(IntervalsSource source, int doc, String field, int[] expectedGaps)
      throws IOException {
    int ord = ReaderUtil.subIndex(doc, searcher.getIndexReader().leaves());
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(ord);
    IntervalIterator it = source.intervals(field, ctx);
    doc = doc - ctx.docBase;
    assertEquals(doc, it.advance(doc));
    for (int expectedGap : expectedGaps) {
      if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
        fail("Unexpected interval " + it);
      }
      assertEquals(expectedGap, it.gaps());
    }
  }

  public void testIntervalsOnFieldWithNoPositions() throws IOException {
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              Intervals.term("wibble").intervals("id", searcher.getIndexReader().leaves().get(0));
            });
    assertEquals(
        "Cannot create an IntervalIterator over field id because it has no indexed positions",
        e.getMessage());
  }

  public void testTermQueryIntervals() throws IOException {
    IntervalsSource source = Intervals.term("porridge");
    checkIntervals(
        source,
        "field1",
        4,
        new int[][] {{}, {1, 1, 4, 4, 7, 7}, {1, 1, 4, 4, 7, 7}, {}, {1, 1, 4, 4, 7, 7}, {0, 0}});
    assertNull(getMatches(source, 0, "field1"));
    assertNull(getMatches(source, 2, "no_such_field"));
    MatchesIterator mi = getMatches(source, 2, "field1");
    assertMatch(mi, 1, 1, 6, 14);
    final TermQuery porridge = new TermQuery(new Term("field1", "porridge"));
    assertEquals(porridge, mi.getQuery());
    assertMatch(mi, 4, 4, 27, 35);
    assertEquals(porridge, mi.getQuery());
    assertMatch(mi, 7, 7, 47, 55);
    assertEquals(porridge, mi.getQuery());
    assertFalse(mi.next());

    assertEquals(1, source.minExtent());

    checkVisits(source, 1, "porridge");
  }

  public void testOrderedNearIntervals() throws IOException {
    IntervalsSource source = Intervals.ordered(Intervals.term("pease"), Intervals.term("hot"));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {0, 2, 6, 17}, {3, 5, 6, 21}, {}, {0, 2, 6, 17}, {}});
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

    assertEquals(2, source.minExtent());

    checkVisits(source, 3, "pease", "hot");
  }

  public void testOrderedNearWithDuplicates() throws IOException {
    IntervalsSource source =
        Intervals.ordered(
            Intervals.term("pease"), Intervals.term("pease"), Intervals.term("porridge"));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {0, 4, 3, 7}, {0, 4, 3, 7}, {}, {0, 4, 3, 7}, {}});
    assertGaps(source, 1, "field1", new int[] {2, 2});

    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 4, 0, 34);
    MatchesIterator sub = mi.getSubMatches();
    assertNotNull(sub);
    assertMatch(sub, 0, 0, 0, 5);
    assertMatch(sub, 3, 3, 20, 25);
    assertMatch(sub, 4, 4, 26, 34);
    assertMatch(mi, 3, 7, 20, 55);
    assertFalse(mi.next());
  }

  public void testPhraseIntervals() throws IOException {
    IntervalsSource source = Intervals.phrase("pease", "porridge");
    checkIntervals(
        source,
        "field1",
        3,
        new int[][] {{}, {0, 1, 3, 4, 6, 7}, {0, 1, 3, 4, 6, 7}, {}, {0, 1, 3, 4, 6, 7}, {}});
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 1, 0, 14);
    assertMatch(mi, 3, 4, 20, 34);
    MatchesIterator sub = mi.getSubMatches();
    assertMatch(sub, 3, 3, 20, 25);
    assertEquals(new TermQuery(new Term("field1", "pease")), sub.getQuery());
    assertMatch(sub, 4, 4, 26, 34);
    assertEquals(new TermQuery(new Term("field1", "porridge")), sub.getQuery());
    assertFalse(sub.next());

    assertMatch(mi, 6, 7, 41, 55);
    sub = mi.getSubMatches();
    assertTrue(sub.next());
    assertEquals(new TermQuery(new Term("field1", "pease")), sub.getQuery());
    assertTrue(sub.next());
    assertEquals(new TermQuery(new Term("field1", "porridge")), sub.getQuery());
    assertFalse(sub.next());

    assertEquals(2, source.minExtent());

    checkVisits(source, 3, "pease", "porridge");
  }

  public void testUnorderedNearIntervals() throws IOException {
    IntervalsSource source = Intervals.unordered(Intervals.term("pease"), Intervals.term("hot"));
    checkIntervals(
        source,
        "field1",
        4,
        new int[][] {
          {}, {0, 2, 2, 3, 6, 17}, {3, 5, 5, 6, 6, 21}, {3, 7}, {0, 2, 2, 3, 6, 17}, {}
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

    assertGaps(source, 1, "field1", new int[] {1, 0, 10});

    assertEquals(2, source.minExtent());

    checkVisits(source, 3, "pease", "hot");
  }

  public void testUnorderedWithRepeats() throws IOException {
    IntervalsSource source =
        Intervals.unordered(
            Intervals.term("pease"), Intervals.term("pease"), Intervals.term("hot"));
    checkIntervals(
        source,
        "field1",
        3,
        new int[][] {{}, {0, 3, 2, 6, 3, 17}, {0, 5, 3, 6}, {}, {0, 3, 2, 6, 3, 17}, {}});
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 3, 0, 25);
    MatchesIterator sub = mi.getSubMatches();
    assertNotNull(sub);
    assertMatch(sub, 0, 0, 0, 5);
    assertMatch(sub, 2, 2, 15, 18);
    assertMatch(sub, 3, 3, 20, 25);
  }

  public void testUnorderedWithRepeatsAndMaxGaps() throws IOException {
    IntervalsSource source =
        Intervals.maxgaps(
            2,
            Intervals.unordered(
                Intervals.term("pease"), Intervals.term("pease"), Intervals.term("hot")));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {0, 3, 2, 6}, {3, 6}, {}, {0, 3, 2, 6}, {}});
  }

  public void testIntervalDisjunction() throws IOException {
    IntervalsSource source =
        Intervals.or(Intervals.term("pease"), Intervals.term("hot"), Intervals.term("notMatching"));
    checkIntervals(
        source,
        "field1",
        4,
        new int[][] {
          {},
          {0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
          {0, 0, 3, 3, 5, 5, 6, 6, 21, 21},
          {3, 3, 7, 7},
          {0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
          {}
        });
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator mi = getMatches(source, 3, "field1");
    assertMatch(mi, 3, 3, 15, 18);
    assertEquals(new TermQuery(new Term("field1", "hot")), mi.getQuery());
    assertNull(mi.getSubMatches());
    assertMatch(mi, 7, 7, 31, 36);
    assertEquals(new TermQuery(new Term("field1", "pease")), mi.getQuery());
    assertNull(mi.getSubMatches());
    assertFalse(mi.next());

    assertEquals(1, source.minExtent());

    checkVisits(source, 4, "pease", "hot", "notMatching");
  }

  public void testCombinationDisjunction() throws IOException {
    IntervalsSource source =
        Intervals.ordered(
            Intervals.or(Intervals.term("alph"), Intervals.term("sacred")),
            Intervals.term("measureless"));
    checkIntervals(source, "field2", 1, new int[][] {{}, {3, 8}, {}, {}, {}, {}});
    assertEquals(2, source.minExtent());

    checkVisits(source, 5, "alph", "sacred", "measureless");
  }

  public void testNesting() throws IOException {
    IntervalsSource source =
        Intervals.unordered(
            Intervals.term("pease"),
            Intervals.term("porridge"),
            Intervals.or(Intervals.term("hot"), Intervals.term("cold")));
    checkIntervals(
        source,
        "field1",
        3,
        new int[][] {
          {},
          {0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17},
          {0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17},
          {},
          {0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17},
          {}
        });
    assertEquals(3, source.minExtent());

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

    assertGaps(source, 1, "field1", new int[] {0, 0, 0, 0, 0, 0, 9});
  }

  public void testOffsetIntervals() throws IOException {
    IntervalsSource source =
        Intervals.unordered(
            Intervals.term("pease"),
            Intervals.term("porridge"),
            Intervals.or(Intervals.term("hot"), Intervals.term("cold")));

    IntervalsSource before = new OffsetIntervalsSource(source, true);
    checkIntervals(
        before,
        "field1",
        3,
        new int[][] {
          {},
          {0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
          {0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
          {},
          {0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
          {}
        });

    IntervalsSource after = new OffsetIntervalsSource(source, false);
    checkIntervals(
        after,
        "field1",
        3,
        new int[][] {
          {},
          {3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 18, 18},
          {3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 18, 18},
          {},
          {3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 18, 18},
          {}
        });

    checkVisits(before, 7, "pease", "porridge", "hot", "cold");
  }

  public void testNesting2() throws IOException {
    IntervalsSource source =
        Intervals.unordered(
            Intervals.ordered(Intervals.term("like"), Intervals.term("it"), Intervals.term("cold")),
            Intervals.term("pease"));
    checkIntervals(source, "field1", 2, new int[][] {{}, {6, 21}, {6, 17}, {}, {}, {}});
    assertNull(getMatches(source, 0, "field1"));
    MatchesIterator it = getMatches(source, 1, "field1");
    assertMatch(it, 6, 21, 41, 118);
    MatchesIterator sub = it.getSubMatches();
    assertMatch(sub, 6, 6, 41, 46);
    assertEquals(new TermQuery(new Term("field1", "pease")), sub.getQuery());
    assertMatch(sub, 19, 19, 106, 110);
    assertEquals(new TermQuery(new Term("field1", "like")), sub.getQuery());
    assertMatch(sub, 20, 20, 111, 113);
    assertEquals(new TermQuery(new Term("field1", "it")), sub.getQuery());
    assertMatch(sub, 21, 21, 114, 118);
    assertEquals(new TermQuery(new Term("field1", "cold")), sub.getQuery());
    assertFalse(sub.next());
    assertFalse(it.next());
    assertEquals(4, source.minExtent());
  }

  public void testInterleavedOrdered() throws IOException {
    IntervalsSource source =
        Intervals.ordered(Intervals.term("a"), Intervals.term("b"), Intervals.term("c"));
    checkIntervals(source, "field2", 1, new int[][] {{}, {}, {0, 3, 5, 7}, {}, {}, {}});
    assertGaps(source, 2, "field2", new int[] {1, 0});
  }

  public void testUnorderedDistinct() throws IOException {
    checkIntervals(
        Intervals.unorderedNoOverlaps(Intervals.term("pease"), Intervals.term("pease")),
        "field1",
        3,
        new int[][] {{}, {0, 3, 3, 6}, {0, 3, 3, 6}, {}, {0, 3, 3, 6}, {}});
    checkIntervals(
        Intervals.unorderedNoOverlaps(
            Intervals.unordered(
                Intervals.term("pease"), Intervals.term("porridge"), Intervals.term("hot")),
            Intervals.term("porridge")),
        "field1",
        3,
        new int[][] {{}, {1, 4, 2, 7, 4, 17}, {1, 5, 4, 7}, {}, {1, 4, 2, 7, 4, 17}, {}});
    checkIntervals(
        Intervals.unorderedNoOverlaps(
            Intervals.unordered(
                Intervals.term("pease"), Intervals.term("porridge"), Intervals.term("hot")),
            Intervals.term("porridge")),
        "field2",
        1,
        new int[][] {{}, {}, {}, {}, {0, 3}, {}});
    IntervalsSource source =
        Intervals.unorderedNoOverlaps(
            Intervals.term("porridge"),
            Intervals.unordered(Intervals.term("pease"), Intervals.term("porridge")));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {1, 4, 4, 7}, {1, 4, 4, 7}, {}, {1, 4, 4, 7}, {}});
    // automatic rewrites mean that we end up with 11 sources to visit
    checkVisits(source, 11, "porridge", "pease");
  }

  public void testContainedBy() throws IOException {
    IntervalsSource source =
        Intervals.containedBy(
            Intervals.term("porridge"),
            Intervals.ordered(Intervals.term("pease"), Intervals.term("cold")));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {4, 4, 7, 7}, {1, 1, 7, 7}, {}, {4, 4}, {}});
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
    assertEquals(1, source.minExtent());

    checkVisits(source, 5, "porridge", "pease", "cold");
  }

  public void testContaining() throws IOException {
    IntervalsSource source =
        Intervals.containing(
            Intervals.ordered(Intervals.term("pease"), Intervals.term("cold")),
            Intervals.term("porridge"));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {3, 5, 6, 21}, {0, 2, 6, 17}, {}, {3, 5}, {}});
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
    assertEquals(2, source.minExtent());
  }

  public void testNotContaining() throws IOException {
    IntervalsSource source =
        Intervals.notContaining(
            Intervals.ordered(Intervals.term("porridge"), Intervals.term("pease")),
            Intervals.term("hot"));
    checkIntervals(source, "field1", 3, new int[][] {{}, {4, 6}, {1, 3}, {}, {4, 6}, {}});
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 4, 6, 26, 46);
    MatchesIterator subs = mi.getSubMatches();
    assertMatch(subs, 4, 4, 26, 34);
    assertMatch(subs, 6, 6, 41, 46);
    assertFalse(subs.next());
    assertFalse(mi.next());
    assertEquals(2, source.minExtent());
  }

  public void testMaxGaps() throws IOException {

    IntervalsSource source =
        Intervals.maxgaps(
            1,
            Intervals.unordered(Intervals.term("w1"), Intervals.term("w3"), Intervals.term("w4")));
    checkIntervals(source, "field2", 1, new int[][] {{}, {}, {}, {}, {}, {0, 3, 2, 4, 3, 6}});

    MatchesIterator mi = getMatches(source, 5, "field2");
    assertMatch(mi, 0, 3, 0, 11);

    assertEquals(3, source.minExtent());
    assertEquals(source, source);
    assertEquals(
        source,
        Intervals.maxgaps(
            1,
            Intervals.unordered(Intervals.term("w1"), Intervals.term("w3"), Intervals.term("w4"))));
    assertNotEquals(
        source,
        Intervals.maxgaps(
            2,
            Intervals.unordered(Intervals.term("w1"), Intervals.term("w3"), Intervals.term("w4"))));
  }

  public void testMaxGapsWithRepeats() throws IOException {
    IntervalsSource source =
        Intervals.maxgaps(
            11,
            Intervals.ordered(
                Intervals.term("pease"), Intervals.term("pease"), Intervals.term("hot")));
    checkIntervals(source, "field1", 1, new int[][] {{}, {}, {0, 5}, {}, {}, {}});
    assertGaps(source, 2, "field1", new int[] {3});
  }

  public void testMaxGapsWithOnlyRepeats() throws IOException {
    IntervalsSource source =
        Intervals.maxgaps(
            1,
            Intervals.ordered(
                Intervals.or(Intervals.term("pease"), Intervals.term("hot")),
                Intervals.or(Intervals.term("pease"), Intervals.term("hot"))));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {0, 2, 2, 3}, {3, 5, 5, 6}, {}, {0, 2, 2, 3}, {}});
    assertGaps(source, 1, "field1", new int[] {1, 0});
  }

  public void testNestedMaxGaps() throws IOException {
    IntervalsSource source =
        Intervals.maxgaps(
            1,
            Intervals.unordered(
                Intervals.ordered(Intervals.term("w1"), Intervals.term("w3")),
                Intervals.term("w4")));
    checkIntervals(source, "field2", 1, new int[][] {{}, {}, {}, {}, {}, {0, 3, 3, 6, 4, 8}});

    assertGaps(source, 5, "field2", new int[] {0, 0, 1});

    MatchesIterator mi = getMatches(source, 5, "field2");
    assertMatch(mi, 0, 3, 0, 11);
    assertMatch(mi, 3, 6, 9, 20);
    assertMatch(mi, 4, 8, 12, 26);

    assertEquals(3, source.minExtent());
  }

  public void testMinimumShouldMatch() throws IOException {
    IntervalsSource source =
        Intervals.atLeast(
            3,
            Intervals.term("porridge"),
            Intervals.term("hot"),
            Intervals.term("twelve"),
            Intervals.term("nine"),
            Intervals.term("pease"));
    checkIntervals(
        source,
        "field1",
        3,
        new int[][] {
          {},
          {0, 2, 1, 3, 2, 4, 6, 11, 7, 17},
          {3, 5, 4, 6, 5, 7, 6, 11, 7, 21},
          {},
          {0, 2, 1, 3, 2, 4, 6, 11, 7, 17, 11, 21},
          {}
        });

    assertGaps(source, 1, "field1", new int[] {0, 0, 0, 3, 8});

    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 0, 2, 0, 18);
    MatchesIterator subs = mi.getSubMatches();
    assertNotNull(subs);
    assertMatch(subs, 0, 0, 0, 5);
    assertMatch(subs, 1, 1, 6, 14);
    assertMatch(subs, 2, 2, 15, 18);
    assertFalse(subs.next());
    assertTrue(mi.next());
    assertTrue(mi.next());
    assertMatch(mi, 6, 11, 41, 71);
    subs = mi.getSubMatches();
    assertMatch(subs, 6, 6, 41, 46);
    assertMatch(subs, 7, 7, 47, 55);
    assertMatch(subs, 11, 11, 67, 71);

    assertEquals(3, source.minExtent());
  }

  public void testDegenerateMinShouldMatch() throws IOException {
    IntervalsSource source =
        Intervals.ordered(
            Intervals.atLeast(1, Intervals.term("interest")),
            Intervals.atLeast(1, Intervals.term("anyone")));

    MatchesIterator mi = getMatches(source, 0, "field1");
    assertMatch(mi, 2, 4, 11, 29);
    MatchesIterator subs = mi.getSubMatches();
    assertNotNull(subs);
    assertMatch(subs, 2, 2, 11, 19);
    assertMatch(subs, 4, 4, 23, 29);
    assertFalse(subs.next());
    assertFalse(mi.next());
  }

  public void testNoMatchMinShouldMatch() throws IOException {
    IntervalsSource source = Intervals.atLeast(4, Intervals.term("a"), Intervals.term("b"));
    checkIntervals(source, "field", 0, new int[][] {});
  }

  public void testDefinedGaps() throws IOException {
    IntervalsSource source =
        Intervals.phrase(
            Intervals.term("pease"),
            Intervals.extend(Intervals.term("cold"), 1, 1),
            Intervals.term("porridge"));
    checkIntervals(source, "field1", 3, new int[][] {{}, {3, 7}, {0, 4}, {}, {3, 7}, {}});
    assertEquals(5, source.minExtent());

    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 3, 7, 20, 55);
    MatchesIterator sub = mi.getSubMatches();
    assertNotNull(sub);
    assertMatch(sub, 3, 3, 20, 25);
    assertMatch(sub, 4, 6, 35, 39);
    assertMatch(sub, 7, 7, 47, 55);

    source = Intervals.extend(Intervals.term("w1"), 5, Integer.MAX_VALUE);
    checkIntervals(
        source,
        "field2",
        1,
        new int[][] {
          {},
          {},
          {},
          {},
          {},
          {0, Integer.MAX_VALUE - 1, 0, Integer.MAX_VALUE - 1, 5, Integer.MAX_VALUE - 1}
        });

    assertEquals(Integer.MAX_VALUE, source.minExtent());
  }

  public void testAfter() throws IOException {
    IntervalsSource source =
        Intervals.after(
            Intervals.term("porridge"),
            Intervals.ordered(Intervals.term("pease"), Intervals.term("cold")));
    checkIntervals(source, "field1", 3, new int[][] {{}, {7, 7}, {4, 4, 7, 7}, {}, {7, 7}, {}});

    MatchesIterator mi = getMatches(source, 1, "field1");
    assertMatch(mi, 7, 7, 20, 55);
    MatchesIterator sub = mi.getSubMatches();
    assertNotNull(sub);
    assertMatch(sub, 3, 3, 20, 25);
    assertMatch(sub, 5, 5, 35, 39);
    assertMatch(sub, 7, 7, 47, 55);
    assertFalse(sub.next());

    assertEquals(1, source.minExtent());
  }

  public void testBefore() throws IOException {
    IntervalsSource source = Intervals.before(Intervals.term("cold"), Intervals.term("twelve"));
    checkIntervals(source, "field1", 2, new int[][] {{}, {}, {2, 2}, {}, {5, 5}, {}});
    assertEquals(1, source.minExtent());
  }

  public void testWithin() throws IOException {
    IntervalsSource source =
        Intervals.within(
            Intervals.term("hot"),
            6,
            Intervals.or(Intervals.term("porridge"), Intervals.term("fraggle")));
    checkIntervals(source, "field1", 3, new int[][] {{}, {2, 2}, {5, 5, 21, 21}, {}, {2, 2}, {}});
    assertEquals(1, source.minExtent());
  }

  public void testOverlapping() throws IOException {
    IntervalsSource source =
        Intervals.overlapping(
            Intervals.unordered(Intervals.term("hot"), Intervals.term("porridge")),
            Intervals.unordered(Intervals.term("cold"), Intervals.term("pease")));
    checkIntervals(
        source, "field1", 3, new int[][] {{}, {2, 4, 7, 17}, {5, 7, 7, 21}, {}, {2, 4}, {}});

    assertGaps(source, 2, "field1", new int[] {1, 13});

    MatchesIterator mi = getMatches(source, 1, "field1");
    assertNotNull(mi);
    assertMatch(mi, 2, 4, 15, 39);
    MatchesIterator sub = mi.getSubMatches();
    assertNotNull(sub);
    assertMatch(sub, 2, 2, 15, 18);
    assertMatch(sub, 3, 3, 20, 25);
    assertMatch(sub, 4, 4, 26, 34);
    assertMatch(sub, 5, 5, 35, 39);
    assertFalse(sub.next());
    assertMatch(mi, 7, 17, 41, 118);

    assertEquals(2, source.minExtent());
  }

  public void testFixedField() throws IOException {

    IntervalsSource source =
        Intervals.phrase(
            Intervals.term("alph"), Intervals.fixField("field1", Intervals.term("hot")));

    // We search in field2, but 'hot' will report intervals from field1
    checkIntervals(source, "field2", 1, new int[][] {{}, {1, 2}, {}, {}, {}, {}});

    MatchesIterator mi = getMatches(source, 1, "field2");
    assertNotNull(mi);
    assertMatch(mi, 1, 2, 6, 18);
  }

  public void testPrefix() throws IOException {
    IntervalsSource source = Intervals.prefix(new BytesRef("p"));
    checkIntervals(
        source,
        "field1",
        5,
        new int[][] {
          {},
          {0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7, 10, 10, 27, 27},
          {0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7, 10, 10},
          {7, 7},
          {0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7, 10, 10},
          {0, 0}
        });
    MatchesIterator mi = getMatches(source, 1, "field1");
    assertNotNull(mi);
    assertMatch(mi, 0, 0, 0, 5);
    assertMatch(mi, 1, 1, 6, 14);

    IntervalsSource noSuch = Intervals.prefix(new BytesRef("qqq"));
    checkIntervals(noSuch, "field1", 0, new int[][] {});

    IntervalsSource s = Intervals.prefix(new BytesRef("p"), 1);
    IllegalStateException e =
        expectThrows(
            IllegalStateException.class,
            () -> {
              for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                s.intervals("field1", ctx);
              }
            });
    assertEquals("Automaton [p*] expanded to too many terms (limit 1)", e.getMessage());

    checkVisits(Intervals.prefix(new BytesRef("p")), 1);
  }

  public void testWildcard() throws IOException {
    IntervalsSource source = Intervals.wildcard(new BytesRef("?ot"));
    checkIntervals(
        source,
        "field1",
        4,
        new int[][] {
          {},
          {2, 2, 10, 10, 17, 17, 27, 27},
          {5, 5, 10, 10, 21, 21},
          {3, 3},
          {2, 2, 10, 10, 17, 17},
          {}
        });
    MatchesIterator mi = getMatches(source, 4, "field1");
    assertNotNull(mi);
    assertMatch(mi, 2, 2, 15, 18);
    assertMatch(mi, 10, 10, 63, 66);
    assertMatch(mi, 17, 17, 97, 100);

    IllegalStateException e =
        expectThrows(
            IllegalStateException.class,
            () -> {
              IntervalsSource s = Intervals.wildcard(new BytesRef("?ot"), 1);
              for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                s.intervals("field1", ctx);
              }
            });
    assertEquals("Automaton [?ot] expanded to too many terms (limit 1)", e.getMessage());

    checkVisits(Intervals.wildcard(new BytesRef("p??")), 1);
  }

  public void testWrappedFilters() throws IOException {

    IntervalsSource source =
        Intervals.or(
            Intervals.term("nine"),
            Intervals.maxgaps(
                1,
                Intervals.or(
                    Intervals.ordered(Intervals.term("pease"), Intervals.term("hot")),
                    Intervals.ordered(Intervals.term("pease"), Intervals.term("cold")))));
    checkIntervals(
        source,
        "field1",
        3,
        new int[][] {{}, {0, 2, 3, 5, 11, 11, 28, 28}, {0, 2, 3, 5}, {}, {0, 2, 3, 5, 11, 11}, {}});
  }

  public void testMultiTerm() throws IOException {
    RegExp re = new RegExp("p.*e");
    IntervalsSource source =
        Intervals.multiterm(new CompiledAutomaton(re.toAutomaton()), re.toString());

    checkIntervals(
        source,
        "field1",
        5,
        new int[][] {
          {},
          {0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7},
          {0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7},
          {7, 7},
          {0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7},
          {0, 0}
        });

    IllegalStateException e =
        expectThrows(
            IllegalStateException.class,
            () -> {
              IntervalsSource s =
                  Intervals.multiterm(new CompiledAutomaton(re.toAutomaton()), 1, re.toString());
              for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                s.intervals("field1", ctx);
              }
            });
    assertEquals("Automaton [\\p(.)*\\e] expanded to too many terms (limit 1)", e.getMessage());

    checkVisits(source, 1);
  }
}
