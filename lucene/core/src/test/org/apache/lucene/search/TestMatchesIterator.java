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

  private static final String FIELD_WITH_OFFSETS = "field_offsets";
  private static final String FIELD_NO_OFFSETS = "field_no_offsets";
  private static final String FIELD_DOCS_ONLY = "field_docs_only";
  private static final String FIELD_FREQS = "field_freqs";

  private static final FieldType OFFSETS = new FieldType(TextField.TYPE_STORED);
  static {
    OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  private static final FieldType DOCS = new FieldType(TextField.TYPE_STORED);
  static {
    DOCS.setIndexOptions(IndexOptions.DOCS);
  }

  private static final FieldType DOCS_AND_FREQS = new FieldType(TextField.TYPE_STORED);
  static {
    DOCS_AND_FREQS.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
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
      doc.add(newField(FIELD_DOCS_ONLY, docFields[i], DOCS));
      doc.add(newField(FIELD_FREQS, docFields[i], DOCS_AND_FREQS));
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
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
    for (int i = 0; i < expected.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(expected[i][0], searcher.leafContexts));
      int doc = expected[i][0] - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals(expected[i].length, 1);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      if (expected[i].length == 1) {
        assertNull(it);
        return;
      }
      checkFieldMatches(it, expected[i]);
      checkFieldMatches(matches.getMatches(field), expected[i]);  // test multiple calls
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

  void checkNoPositionsMatches(Query q, String field, boolean[] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
    for (int i = 0; i < expected.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(i, searcher.leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (expected[i]) {
        MatchesIterator mi = matches.getMatches(field);
        assertNull(mi);
      }
      else {
        assertNull(matches);
      }
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
  }

  public void testTermQueryNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_DOCS_ONLY, FIELD_FREQS }) {
      Query q = new TermQuery(new Term(field, "w1"));
      checkNoPositionsMatches(q, field, new boolean[]{ true, true, true, true, false });
    }
  }

  public void testDisjunction() throws IOException {
    Query w1 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1"));
    Query w3 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3"));
    Query q = new BooleanQuery.Builder()
        .add(w1, BooleanClause.Occur.SHOULD)
        .add(w3, BooleanClause.Occur.SHOULD)
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 1, 0, 0, 0, 2, 1, 1, 3, 5, 3, 3, 9, 11 },
        { 2, 0, 0, 0, 2 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8, 5, 5, 15, 17 },
        { 4 }
    });
  }

  public void testDisjunctionNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_DOCS_ONLY, FIELD_FREQS }) {
      Query q = new BooleanQuery.Builder()
          .add(new TermQuery(new Term(field, "w1")), BooleanClause.Occur.SHOULD)
          .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD)
          .build();
      checkNoPositionsMatches(q, field, new boolean[]{ true, true, true, true, false });
    }
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

  public void testReqOptNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_DOCS_ONLY, FIELD_FREQS }) {
      Query q = new BooleanQuery.Builder()
          .add(new TermQuery(new Term(field, "w1")), BooleanClause.Occur.SHOULD)
          .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST)
          .build();
      checkNoPositionsMatches(q, field, new boolean[]{ true, true, false, true, false });
    }
  }

  public void testMinShouldMatch() throws IOException {
    Query w1 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1"));
    Query w3 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3"));
    Query w4 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w4"));
    Query xx = new TermQuery(new Term(FIELD_WITH_OFFSETS, "xx"));
    Query q = new BooleanQuery.Builder()
        .add(w3, BooleanClause.Occur.SHOULD)
        .add(new BooleanQuery.Builder()
            .add(w1, BooleanClause.Occur.SHOULD)
            .add(w4, BooleanClause.Occur.SHOULD)
            .add(xx, BooleanClause.Occur.SHOULD)
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
  }

  public void testMinShouldMatchNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_FREQS, FIELD_DOCS_ONLY }) {
      Query q = new BooleanQuery.Builder()
          .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD)
          .add(new BooleanQuery.Builder()
              .add(new TermQuery(new Term(field, "w1")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term(field, "w4")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.SHOULD)
              .setMinimumNumberShouldMatch(2)
              .build(), BooleanClause.Occur.SHOULD)
          .build();
      checkNoPositionsMatches(q, field, new boolean[]{ true, true, true, true, false });
    }
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

  public void testExclusionNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_FREQS, FIELD_DOCS_ONLY }) {
      Query q = new BooleanQuery.Builder()
          .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD)
          .add(new TermQuery(new Term(field, "zz")), BooleanClause.Occur.MUST_NOT)
          .build();
      checkNoPositionsMatches(q, field, new boolean[]{ true, false, false, true, false });
    }
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

  public void testConjunctionNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_FREQS, FIELD_DOCS_ONLY }) {
      Query q = new BooleanQuery.Builder()
          .add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term(field, "w4")), BooleanClause.Occur.MUST)
          .build();
      checkNoPositionsMatches(q, field, new boolean[]{ true, false, false, true, false });
    }
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

    Query rq = new RegexpQuery(new Term(FIELD_WITH_OFFSETS, "w[1-2]"));
    checkMatches(rq, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 1, 1, 3, 5 },
        { 1, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 2, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 3, 0, 0, 0, 2, 1, 1, 3, 5, 2, 2, 6, 8, 4, 4, 12, 14 },
        { 4 }
    });

  }

  public void testNoMatchWildcards() throws IOException {
    Query nomatch = new PrefixQuery(new Term(FIELD_WITH_OFFSETS, "wibble"));
    Matches matches = searcher.createWeight(searcher.rewrite(nomatch), ScoreMode.COMPLETE_NO_SCORES, 1)
        .matches(searcher.leafContexts.get(0), 0);
    assertNull(matches);
  }

  public void testWildcardsNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_FREQS, FIELD_DOCS_ONLY }) {
      Query q = new PrefixQuery(new Term(field, "x"));
      checkNoPositionsMatches(q, field, new boolean[]{ false, false, true, false, false });
    }
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

  public void testSynonymQueryNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_FREQS, FIELD_DOCS_ONLY }) {
      Query q = new SynonymQuery(new Term(field, "w1"), new Term(field, "w2"));
      checkNoPositionsMatches(q, field, new boolean[]{ true, true, true, true, false });
    }
  }

  public void testMultipleFields() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("id", "1")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")), BooleanClause.Occur.MUST)
        .build();
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);

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

}
