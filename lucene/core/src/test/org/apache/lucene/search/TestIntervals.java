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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
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
      "With walls and towers were girdled round",
      "Which was nice"
  };

  private static Directory directory;
  private static IndexSearcher searcher;
  private static Analyzer analyzer = new StandardAnalyzer();

  @BeforeClass
  public static void setupIndex() throws IOException {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(analyzer));
    for (int i = 0; i < field1_docs.length; i++) {
      Document doc = new Document();
      doc.add(new TextField("field1", field1_docs[i], Field.Store.NO));
      doc.add(new TextField("field2", field2_docs[i], Field.Store.NO));
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

  private void checkIntervals(Query query, String field, int expectedMatchCount, int[][] expected) throws IOException {
    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_POSITIONS, 1f);
    int matchedDocs = 0;
    for (LeafReaderContext ctx : searcher.leafContexts) {
      Scorer scorer = weight.scorer(ctx);
      if (scorer == null)
        continue;
      assertNull(scorer.intervals(field + "1"));
      NumericDocValues ids = DocValues.getNumeric(ctx.reader(), "id");
      IntervalIterator intervals = scorer.intervals(field);
      DocIdSetIterator it = scorer.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        matchedDocs++;
        ids.advance(doc);
        int id = (int) ids.longValue();
        if (intervals.reset(doc)) {
          int i = 0, pos;
          while ((pos = intervals.nextInterval()) != IntervalIterator.NO_MORE_INTERVALS) {
            //System.out.println(doc + ": " + intervals.start() + "->" + intervals.end());
            assertEquals(expected[id][i], pos);
            assertEquals(expected[id][i], intervals.start());
            assertEquals(expected[id][i + 1], intervals.end());
            i += 2;
          }
          assertEquals(expected[id].length, i);
        }
        else {
          assertEquals(0, expected[id].length);
        }
      }
    }
    assertEquals(expectedMatchCount, matchedDocs);
  }

  public void testTermQueryIntervals() throws IOException {
    checkIntervals(new TermQuery(new Term("field1", "porridge")), "field1", 4, new int[][]{
        {},
        { 1, 1, 4, 4, 7, 7 },
        { 1, 1, 4, 4, 7, 7 },
        {},
        { 1, 1, 4, 4, 7, 7 },
        { 0, 0 }
    });
  }

  public void testExactPhraseQueryIntervals() throws IOException {
    checkIntervals(new PhraseQuery.Builder()
        .add(new Term("field1", "pease"))
        .add(new Term("field1", "porridge")).build(), "field1", 3, new int[][]{
        {},
        { 0, 1, 3, 4, 6, 7 },
        { 0, 1, 3, 4, 6, 7 },
        {},
        { 0, 1, 3, 4, 6, 7 },
        {}
    });
  }

  public void testSloppyPhraseQueryIntervals() throws IOException {
    checkIntervals(new PhraseQuery.Builder()
        .add(new Term("field1", "pease"))
        .add(new Term("field1", "porridge"))
        .add(new Term("field1", "hot"))
        .setSlop(3).build(), "field1", 3, new int[][]{
        {},
        { 0, 2, 1, 3, 2, 4 },
        { 0, 5, 3, 5, 3, 7, 5, 7 },
        {},
        { 0, 2, 1, 3, 2, 4 },
        {}
        }
    );
  }

  public void testOrderedNearIntervals() throws IOException {
    checkIntervals(Intervals.orderedQuery("field1", 100,
        new TermQuery(new Term("field1", "pease")), new TermQuery(new Term("field1", "hot"))),
        "field1", 3, new int[][]{
        {},
        { 0, 2, 6, 17 },
        { 3, 5, 6, 21 },
        {},
        { 0, 2, 6, 17 },
        { }
    });
  }

  public void testUnorderedNearIntervals() throws IOException {
    checkIntervals(Intervals.unorderedQuery("field1", 100,
        new TermQuery(new Term("field1", "pease")), new TermQuery(new Term("field1", "hot"))),
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
    checkIntervals(new BooleanQuery.Builder()
        .add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term("field1", "hot")), BooleanClause.Occur.SHOULD)
        .build(), "field1", 4, new int[][]{
        {},
        { 0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
        { 0, 0, 3, 3, 5, 5, 6, 6, 21, 21},
        { 3, 3, 7, 7 },
        { 0, 0, 2, 2, 3, 3, 6, 6, 17, 17},
        {}
    });
  }

  public void testNesting() throws IOException {
    checkIntervals(Intervals.unorderedQuery("field1", 100,
        new TermQuery(new Term("field1", "pease")),
        new TermQuery(new Term("field1", "porridge")),
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "hot")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("field1", "cold")), BooleanClause.Occur.SHOULD)
            .build()), "field1", 3, new int[][]{
        {},
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        {},
        { 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6, 17 },
        {}
    });
  }

  // x near ((a not b) or (c not d))
  public void testExclusionBooleans() throws IOException {
    checkIntervals(Intervals.unorderedQuery("field1",
        new TermQuery(new Term("field1", "pease")),
        new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "nine")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field1", "years")), BooleanClause.Occur.MUST_NOT)
                .build(), BooleanClause.Occur.SHOULD)
            .add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "twelve")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field1", "days")), BooleanClause.Occur.MUST_NOT)
                .build(), BooleanClause.Occur.SHOULD)
            .build()), "field1", 2, new int[][]{
        {},
        { 6, 11 },
        {},
        {},
        { 6, 21 },
        {}
    });
  }

  public void testConjunctionBooleans() throws IOException {
    checkIntervals(Intervals.unorderedQuery("field1",
        new TermQuery(new Term("field1", "pease")),
        new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "nine")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field2", "caverns")), BooleanClause.Occur.MUST)
                .build(), BooleanClause.Occur.SHOULD)
            .add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "twelve")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field2", "sunless")), BooleanClause.Occur.MUST)
                .build(), BooleanClause.Occur.SHOULD)
            .build()), "field1", 2, new int[][]{
        {},
        { 6, 11 },
        { 6, 11 },
        {},
        {},
        {}
    });
  }

  public void testMinimumShouldMatch() throws IOException {
    checkIntervals(new BooleanQuery.Builder()
        .add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.SHOULD)
        .add(new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "porridge")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("field1", "days")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("field1", "fraggle")), BooleanClause.Occur.SHOULD)
            .setMinimumNumberShouldMatch(2)
            .build(), BooleanClause.Occur.SHOULD)
        .build(), "field1", 4, new int[][]{
        {},
        { 0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7, 12, 12, 29, 29 },
        { 0, 0, 1, 1, 3, 3, 4, 4, 6, 6, 7, 7, 12, 12, 27, 27 },
        { 7, 7 },
        { 0, 0, 3, 3, 6, 6 },
        {}
    });
  }
}
