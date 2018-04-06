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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestMatchesIterator extends LuceneTestCase {

  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader;

  public static final String FIELD_WITH_OFFSETS = "field_offsets";
  public static final String FIELD_NO_OFFSETS = "field_no_offsets";

  public static final FieldType OFFSETS = new FieldType(TextField.TYPE_STORED);
  static {
    OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(FIELD_WITH_OFFSETS, docFields[i], OFFSETS));
      doc.add(newField(FIELD_NO_OFFSETS, docFields[i], TextField.TYPE_STORED));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(newField("id", Integer.toString(i), TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(getOnlyLeafReader(reader));
  }

  protected String[] docFields = {
      "w1 w2 w3 w4 w5",
      "w1 w3 w2 w3 zz",
      "w1 xx w2 yy w4",
      "w1 w2 w1 w4 w2 w3",
      "nothing matches this document"
  };

  void checkMatches(Query q, String field, int[][] expected) throws IOException {
    Weight w = searcher.createNormalizedWeight(q, ScoreMode.COMPLETE_NO_SCORES);
    for (int i = 0; i < expected.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(expected[i][0], searcher.leafContexts));
      int doc = expected[i][0] - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals(expected[i].length, 1);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      checkFieldMatches(it, expected[i]);
    }
  }

  void checkFieldMatches(MatchesIterator it, int[] expected) throws IOException {
    int pos = 1;
    while (it.next()) {
      //System.out.println(expected[i][pos] + "->" + expected[i][pos + 1] + "[" + expected[i][pos + 2] + "->" + expected[i][pos + 3] + "]");
      assertEquals(expected[pos], it.startPosition());
      assertEquals(expected[pos + 1], it.endPosition());
      assertEquals(expected[pos + 2], it.startOffset());
      assertEquals(expected[pos + 3], it.endOffset());
      pos += 4;
    }
    assertEquals(expected.length, pos);
  }

  void checkTerms(Query q, String field, String[][] expected) throws IOException {
    Weight w = searcher.createNormalizedWeight(q, ScoreMode.COMPLETE_NO_SCORES);
    for (int i = 0; i < expected.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(i, searcher.leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals(expected[i].length, 0);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      int pos = 0;
      while (it.next()) {
        assertEquals(expected[i][pos], it.term().utf8ToString());
        pos += 1;
      }
      assertEquals(expected[i].length, pos);
    }
  }

  public void testTermQuery() throws IOException {
    Query q = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1"));
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2 },
        { 1, 0, 0, 0, 2 },
        { 2, 0, 0, 0, 2 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 4 }
    });
  }

  public void testTermQueryNoStoredOffsets() throws IOException {
    Query q = new TermQuery(new Term(FIELD_NO_OFFSETS, "w1"));
    checkMatches(q, FIELD_NO_OFFSETS, new int[][]{
        { 0, 0, 0, -1, -1 },
        { 1, 0, 0, -1, -1 },
        { 2, 0, 0, -1, -1 },
        { 3, 0, 0, -1, -1, 2, 2, -1, -1 },
        { 4 }
    });
    checkTerms(q, FIELD_NO_OFFSETS, new String[][]{
        { "w1" },
        { "w1" },
        { "w1" },
        { "w1", "w1" },
        {}
    });
  }

  public void testDisjunction() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.SHOULD)
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 1, 0, 0, 0, 2, 1, 1, 3, 5, 3, 3, 9, 11 },
        { 2, 0, 0, 0, 2 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8, 5, 5, 15, 17 },
        { 4 }
    });
    checkTerms(q, FIELD_WITH_OFFSETS, new String[][]{
        { "w1", "w3" },
        { "w1", "w3", "w3" },
        { "w1" },
        { "w1", "w1", "w3" },
        {}
    });
  }

  public void testReqOpt() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 1, 0, 0, 0, 2, 1, 1, 3, 5, 3, 3, 9, 11 },
        { 2 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8, 5, 5, 15, 17 },
        { 4 }
    });
  }

  public void testMinShouldMatch() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.SHOULD)
        .add(new BooleanQuery.Builder()
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w4")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "xx")), BooleanClause.Occur.SHOULD)
            .setMinimumNumberShouldMatch(2)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 2, 2, 6, 8, 3, 3, 9, 11 },
        { 1, 1, 1, 3, 5, 3, 3, 9, 11 },
        { 2, 0, 0, 0, 2, 1, 1, 3, 5, 4, 4, 12, 14 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8, 3, 3, 9, 11, 5, 5, 15, 17 },
        { 4 }
    });
    checkTerms(q, FIELD_WITH_OFFSETS, new String[][]{
        { "w1", "w3", "w4" },
        { "w3", "w3" },
        { "w1", "xx", "w4" },
        { "w1", "w1", "w4", "w3" },
        {}
    });
  }

  public void testExclusion() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "zz")), BooleanClause.Occur.MUST_NOT)
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 2, 2, 6, 8 },
        { 1 },
        { 2 },
        { 3, 5, 5, 15, 17 },
        { 4 }
    });
  }

  public void testConjunction() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w4")), BooleanClause.Occur.MUST)
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 2, 2, 6, 8, 3, 3, 9, 11 },
        { 1 },
        { 2 },
        { 3, 3, 3, 9, 11, 5, 5, 15, 17 },
        { 4 }
    });
  }

  public void testWildcards() throws IOException {
    Query q = new PrefixQuery(new Term(FIELD_WITH_OFFSETS, "x"));
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0 },
        { 1 },
        { 2, 1, 1, 3, 5 },
        { 3 },
        { 4 }
    });
    checkTerms(q, FIELD_WITH_OFFSETS, new String[][]{
        {}, {}, { "xx" }, {}
    });

    Query rq = new RegexpQuery(new Term(FIELD_WITH_OFFSETS, "w[1-2]"));
    checkMatches(rq, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 1, 1, 3, 5 },
        { 1, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 2, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 3, 0, 0, 0, 2, 1, 1, 3, 5, 2, 2, 6, 8, 4, 4, 12, 14 },
        { 4 }
    });
  }

  public void testSynonymQuery() throws IOException {
    Query q = new SynonymQuery(new Term(FIELD_WITH_OFFSETS, "w1"), new Term(FIELD_WITH_OFFSETS, "w2"));
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 1, 1, 3, 5 },
        { 1, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 2, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 3, 0, 0, 0, 2, 1, 1, 3, 5, 2, 2, 6, 8, 4, 4, 12, 14 },
        { 4 }
    });
  }

  public void testMultipleFields() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("id", "1")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
        .build();
    Weight w = searcher.createNormalizedWeight(q, ScoreMode.COMPLETE);

    LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(1, searcher.leafContexts));
    Matches m = w.matches(ctx, 1 - ctx.docBase);
    assertNotNull(m);
    checkFieldMatches(m.getMatches("id"), new int[]{ -1, 0, 0, -1, -1 });
    checkFieldMatches(m.getMatches(FIELD_WITH_OFFSETS), new int[]{ -1, 1, 1, 3, 5, 3, 3, 9, 11 });
    assertNull(m.getMatches("bogus"));

    Set<String> fields = new HashSet<>();
    for (String field : m) {
      fields.add(field);
    }
    assertEquals(2, fields.size());
    assertTrue(fields.contains(FIELD_WITH_OFFSETS));
    assertTrue(fields.contains("id"));
  }

  protected String[] doc1Fields = {
      "w1 w2 w3 w4 w5",
      "w1 w3 w2 w3 zz",
      "w1 xx w2 yy w4",
      "w1 w2 w1 w4 w2 w3"
  };

}
