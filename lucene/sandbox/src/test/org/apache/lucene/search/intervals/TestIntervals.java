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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestIntervals extends LuceneTestCase {

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

  @BeforeClass
  public static void setupIndex() throws IOException {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(analyzer));
    for (int i = 0; i < field1_docs.length; i++) {
      Document doc = new Document();
      doc.add(new TextField("field1", field1_docs[i], Field.Store.NO));
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
            //System.out.println(doc + ": " + intervals);
            assertEquals("Wrong start value", expected[id][i], pos);
            assertEquals("start() != pos returned from nextInterval()", expected[id][i], intervals.start());
            assertEquals("Wrong end value", expected[id][i + 1], intervals.end());
            i += 2;
          }
          assertEquals("Wrong number of endpoints", expected[id].length, i);
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

  public void testIntervalsOnFieldWithNoPositions() throws IOException {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      Intervals.term("wibble").intervals("id", searcher.getIndexReader().leaves().get(0));
    });
    assertEquals("Cannot create an IntervalIterator over field id because it has no indexed positions", e.getMessage());
  }

  public void testTermQueryIntervals() throws IOException {
    checkIntervals(Intervals.term("porridge"), "field1", 4, new int[][]{
        {},
        { 1, 1, 4, 4, 7, 7 },
        { 1, 1, 4, 4, 7, 7 },
        {},
        { 1, 1, 4, 4, 7, 7 },
        { 0, 0 }
    });
  }

  public void testOrderedNearIntervals() throws IOException {
    checkIntervals(Intervals.ordered(Intervals.term("pease"), Intervals.term("hot")),
        "field1", 3, new int[][]{
        {},
        { 0, 2, 6, 17 },
        { 3, 5, 6, 21 },
        {},
        { 0, 2, 6, 17 },
        { }
    });
  }

  public void testPhraseIntervals() throws IOException {
    checkIntervals(Intervals.phrase("pease", "porridge"), "field1", 3, new int[][]{
        {},
        { 0, 1, 3, 4, 6, 7 },
        { 0, 1, 3, 4, 6, 7 },
        {},
        { 0, 1, 3, 4, 6, 7 },
        {}
    });
  }

  public void testUnorderedNearIntervals() throws IOException {
    checkIntervals(Intervals.unordered(Intervals.term("pease"), Intervals.term("hot")),
        "field1", 4, new int[][]{
            {},
            { 0, 2, 2, 3, 6, 17 },
            { 3, 5, 5, 6, 6, 21 },
            { 3, 7 },
            { 0, 2, 2, 3, 6, 17 },
            {}
        });
  }

  public void testIntervalDisjunction() throws IOException {
    checkIntervals(Intervals.or(Intervals.term("pease"), Intervals.term("hot"), Intervals.term("notMatching")), "field1", 4, new int[][]{
        {},
        { 0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
        { 0, 0, 3, 3, 5, 5, 6, 6, 21, 21},
        { 3, 3, 7, 7 },
        { 0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
        {}
    });
  }

  public void testNesting() throws IOException {
    checkIntervals(Intervals.unordered(Intervals.term("pease"), Intervals.term("porridge"), Intervals.or(Intervals.term("hot"), Intervals.term("cold"))),
        "field1", 3, new int[][]{
        {},
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        {},
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        {}
    });
  }

  public void testNesting2() throws IOException {
    checkIntervals(
        Intervals.unordered(
            Intervals.ordered(
                Intervals.term("like"),
                Intervals.term("it"),
                Intervals.term("cold")
            ),
            Intervals.term("pease")
        ),
        "field1", 2, new int[][]{
            {},
            {6, 21},
            {6, 17},
            {},
            {},
            {}
        });
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

}
