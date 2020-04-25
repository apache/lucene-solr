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
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestMatchesIterator extends LuceneTestCase {

  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader = null;

  private static final String FIELD_WITH_OFFSETS = "field_offsets";
  private static final String FIELD_NO_OFFSETS = "field_no_offsets";
  private static final String FIELD_DOCS_ONLY = "field_docs_only";
  private static final String FIELD_FREQS = "field_freqs";
  private static final String FIELD_POINT = "field_point";

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
      doc.add(new IntPoint(FIELD_POINT, 10));
      doc.add(new NumericDocValuesField(FIELD_POINT, 10));
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
      "a phrase sentence with many phrase sentence iterations of a phrase sentence",
      "nothing matches this document"
  };

  private void checkMatches(Query q, String field, int[][] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
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
        continue;
      }
      checkFieldMatches(it, expected[i]);
      checkFieldMatches(matches.getMatches(field), expected[i]);  // test multiple calls
    }
  }

  private void checkLabelCount(Query q, String field, int[] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < expected.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(i, searcher.leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals("Expected to get matches on document " + i, 0, expected[i]);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      if (expected[i] == 0) {
        assertNull(it);
        continue;
      }
      else {
        assertNotNull(it);
      }
      IdentityHashMap<Query, Integer> labels = new IdentityHashMap<>();
      while (it.next()) {
        labels.put(it.getQuery(), 1);
      }
      assertEquals(expected[i], labels.size());
    }
  }

  private void checkFieldMatches(MatchesIterator it, int[] expected) throws IOException {
    int pos = 1;
    while (it.next()) {
      //System.out.println(expected[i][pos] + "->" + expected[i][pos + 1] + "[" + expected[i][pos + 2] + "->" + expected[i][pos + 3] + "]");
      assertEquals("Wrong start position", expected[pos], it.startPosition());
      assertEquals("Wrong end position", expected[pos + 1], it.endPosition());
      assertEquals("Wrong start offset", expected[pos + 2], it.startOffset());
      assertEquals("Wrong end offset", expected[pos + 3], it.endOffset());
      pos += 4;
    }
    assertEquals(expected.length, pos);
  }

  private void checkNoPositionsMatches(Query q, String field, boolean[] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
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

  private void checkSubMatches(Query q, String[][] expectedNames) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
    for (int i = 0; i < expectedNames.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(i, searcher.leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals("Expected to get no matches on document " + i, 0, expectedNames[i].length);
        continue;
      }
      Set<String> expectedQueries = new HashSet<>(Arrays.asList(expectedNames[i]));
      Set<String> actualQueries = NamedMatches.findNamedMatches(matches)
          .stream().map(NamedMatches::getName).collect(Collectors.toSet());

      Set<String> unexpected = new HashSet<>(actualQueries);
      unexpected.removeAll(expectedQueries);
      assertEquals("Unexpected matching leaf queries: " + unexpected, 0, unexpected.size());
      Set<String> missing = new HashSet<>(expectedQueries);
      missing.removeAll(actualQueries);
      assertEquals("Missing matching leaf queries: " + missing, 0, missing.size());
    }
  }

  private void assertIsLeafMatch(Query q, String field) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < searcher.reader.maxDoc(); i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(i, searcher.leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        return;
      }
      MatchesIterator mi = matches.getMatches(field);
      if (mi == null) {
        return;
      }
      while (mi.next()) {
        assertNull(mi.getSubMatches());
      }
    }
  }

  private void checkTermMatches(Query q, String field, TermMatch[][][] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < expected.length; i++) {
      LeafReaderContext ctx = searcher.leafContexts.get(ReaderUtil.subIndex(i, searcher.leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals(expected[i].length, 0);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      if (expected[i].length == 0) {
        assertNull(it);
        continue;
      }
      checkTerms(expected[i], it);
    }
  }

  private void checkTerms(TermMatch[][] expected, MatchesIterator it) throws IOException {
    int upTo = 0;
    while (it.next()) {
      Set<TermMatch> expectedMatches = new HashSet<>(Arrays.asList(expected[upTo]));
      MatchesIterator submatches = it.getSubMatches();
      while (submatches.next()) {
        TermMatch tm = new TermMatch(submatches.startPosition(), submatches.startOffset(), submatches.endOffset());
        if (expectedMatches.remove(tm) == false) {
          fail("Unexpected term match: " + tm);
        }
      }
      if (expectedMatches.size() != 0) {
        fail("Missing term matches: " + expectedMatches.stream().map(Object::toString).collect(Collectors.joining(", ")));
      }
      upTo++;
    }
    if (upTo < expected.length - 1) {
      fail("Missing expected match");
    }
  }

  static class TermMatch {

    public final int position;

    public final int startOffset;

    public final int endOffset;

    public TermMatch(PostingsEnum pe, int position) throws IOException {
      this.position = position;
      this.startOffset = pe.startOffset();
      this.endOffset = pe.endOffset();
    }

    public TermMatch(int position, int startOffset, int endOffset) {
      this.position = position;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TermMatch termMatch = (TermMatch) o;
      return position == termMatch.position &&
          startOffset == termMatch.startOffset &&
          endOffset == termMatch.endOffset;
    }

    @Override
    public int hashCode() {
      return Objects.hash(position, startOffset, endOffset);
    }

    @Override
    public String toString() {
      return position + "[" + startOffset + "->" + endOffset + "]";
    }
  }

  public void testTermQuery() throws IOException {
    Term t = new Term(FIELD_WITH_OFFSETS, "w1");
    Query q = NamedMatches.wrapQuery("q", new TermQuery(t));
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2 },
        { 1, 0, 0, 0, 2 },
        { 2, 0, 0, 0, 2 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 4 }
    });
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[]{ 1, 1, 1, 1, 0, 0 });
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
    checkSubMatches(q, new String[][]{ {"q"}, {"q"}, {"q"}, {"q"}, {}, {}});
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
    Query w1 = NamedMatches.wrapQuery("w1", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")));
    Query w3 = NamedMatches.wrapQuery("w3", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")));
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
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[]{ 2, 2, 1, 2, 0, 0 });
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
    checkSubMatches(q, new String[][]{ {"w1", "w3"}, {"w1", "w3"}, {"w1"}, {"w1", "w3"}, {}, {}});
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
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[]{ 2, 2, 0, 2, 0, 0 });
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
    Query w1 = NamedMatches.wrapQuery("w1", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w1")));
    Query w3 = NamedMatches.wrapQuery("w3", new TermQuery(new Term(FIELD_WITH_OFFSETS, "w3")));
    Query w4 = new TermQuery(new Term(FIELD_WITH_OFFSETS, "w4"));
    Query xx = NamedMatches.wrapQuery("xx", new TermQuery(new Term(FIELD_WITH_OFFSETS, "xx")));
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
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[]{ 3, 1, 3, 3, 0, 0 });
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
    checkSubMatches(q, new String[][]{ {"w1", "w3"}, {"w3"}, {"w1", "xx"}, {"w1", "w3"}, {}, {}});
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
    checkLabelCount(rq, FIELD_WITH_OFFSETS, new int[]{ 1, 1, 1, 1, 0 });
    assertIsLeafMatch(rq, FIELD_WITH_OFFSETS);

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
    Query q = new SynonymQuery.Builder(FIELD_WITH_OFFSETS)
        .addTerm(new Term(FIELD_WITH_OFFSETS, "w1")).addTerm(new Term(FIELD_WITH_OFFSETS, "w2"))
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2, 1, 1, 3, 5 },
        { 1, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 2, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 3, 0, 0, 0, 2, 1, 1, 3, 5, 2, 2, 6, 8, 4, 4, 12, 14 },
        { 4 }
    });
    assertIsLeafMatch(q, FIELD_WITH_OFFSETS);
  }

  public void testSynonymQueryNoPositions() throws IOException {
    for (String field : new String[]{ FIELD_FREQS, FIELD_DOCS_ONLY }) {
      Query q = new SynonymQuery.Builder(field).addTerm(new Term(field, "w1")).addTerm(new Term(field, "w2")).build();
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

    assertEquals(2, AssertingMatches.unWrap(m).getSubMatches().size());
  }

  //  0         1         2         3         4         5         6         7
  // "a phrase sentence with many phrase sentence iterations of a phrase sentence",

  public void testSloppyPhraseQueryWithRepeats() throws IOException {
    Term p = new Term(FIELD_WITH_OFFSETS, "phrase");
    Term s = new Term(FIELD_WITH_OFFSETS, "sentence");
    PhraseQuery pq = new PhraseQuery(10, FIELD_WITH_OFFSETS, "phrase", "sentence", "sentence");
    checkMatches(pq, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 1, 6, 2, 43, 2, 11, 9, 75, 5, 11, 28, 75, 6, 11, 35, 75 }
    });
    checkLabelCount(pq, FIELD_WITH_OFFSETS, new int[]{ 0, 0, 0, 0, 1 });
    assertIsLeafMatch(pq, FIELD_WITH_OFFSETS);
  }

  public void testSloppyPhraseQuery() throws IOException {
    PhraseQuery pq = new PhraseQuery(4, FIELD_WITH_OFFSETS, "a", "sentence");
    checkMatches(pq, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 0, 2, 0, 17, 6, 9, 35, 59, 9, 11, 58, 75 }
    });
    assertIsLeafMatch(pq, FIELD_WITH_OFFSETS);
  }

  public void testExactPhraseQuery() throws IOException {
    PhraseQuery pq = new PhraseQuery(FIELD_WITH_OFFSETS, "phrase", "sentence");
    checkMatches(pq, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 1, 2, 2, 17, 5, 6, 28, 43, 10, 11, 60, 75 }
    });

    Term a = new Term(FIELD_WITH_OFFSETS, "a");
    Term s = new Term(FIELD_WITH_OFFSETS, "sentence");
    PhraseQuery pq2 = new PhraseQuery.Builder()
        .add(a)
        .add(s, 2)
        .build();
    checkMatches(pq2, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 0, 2, 0, 17, 9, 11, 58, 75 }
    });
    assertIsLeafMatch(pq2, FIELD_WITH_OFFSETS);
  }

  //  0         1         2         3         4         5         6         7
  // "a phrase sentence with many phrase sentence iterations of a phrase sentence",

  public void testSloppyMultiPhraseQuery() throws IOException {
    Term p = new Term(FIELD_WITH_OFFSETS, "phrase");
    Term s = new Term(FIELD_WITH_OFFSETS, "sentence");
    Term i = new Term(FIELD_WITH_OFFSETS, "iterations");
    MultiPhraseQuery mpq = new MultiPhraseQuery.Builder()
        .add(p)
        .add(new Term[]{ s, i })
        .setSlop(4)
        .build();
    checkMatches(mpq, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 1, 2, 2, 17, 5, 6, 28, 43, 5, 7, 28, 54, 10, 11, 60, 75 }
    });
    assertIsLeafMatch(mpq, FIELD_WITH_OFFSETS);
  }

  public void testExactMultiPhraseQuery() throws IOException {
    MultiPhraseQuery mpq = new MultiPhraseQuery.Builder()
        .add(new Term(FIELD_WITH_OFFSETS, "sentence"))
        .add(new Term[]{ new Term(FIELD_WITH_OFFSETS, "with"), new Term(FIELD_WITH_OFFSETS, "iterations") })
        .build();
    checkMatches(mpq, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 2, 3, 9, 22, 6, 7, 35, 54 }
    });

    MultiPhraseQuery mpq2 = new MultiPhraseQuery.Builder()
        .add(new Term[]{ new Term(FIELD_WITH_OFFSETS, "a"), new Term(FIELD_WITH_OFFSETS, "many")})
        .add(new Term(FIELD_WITH_OFFSETS, "phrase"))
        .build();
    checkMatches(mpq2, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 0, 1, 0, 8, 4, 5, 23, 34, 9, 10, 58, 66 }
    });
    assertIsLeafMatch(mpq2, FIELD_WITH_OFFSETS);
  }

  //  0         1         2         3         4         5         6         7
  // "a phrase sentence with many phrase sentence iterations of a phrase sentence",

  public void testSpanQuery() throws IOException {
    SpanQuery subq = SpanNearQuery.newOrderedNearQuery(FIELD_WITH_OFFSETS)
        .addClause(new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "with")))
        .addClause(new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "many")))
        .build();
    Query q = SpanNearQuery.newOrderedNearQuery(FIELD_WITH_OFFSETS)
        .addClause(new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "sentence")))
        .addClause(new SpanOrQuery(subq, new SpanTermQuery(new Term(FIELD_WITH_OFFSETS, "iterations"))))
        .build();
    checkMatches(q, FIELD_WITH_OFFSETS, new int[][]{
        { 0 }, { 1 }, { 2 }, { 3 },
        { 4, 2, 4, 9, 27, 6, 7, 35, 54 }
    });
    checkLabelCount(q, FIELD_WITH_OFFSETS, new int[]{ 0, 0, 0, 0, 1 });
    checkTermMatches(q, FIELD_WITH_OFFSETS, new TermMatch[][][]{
        {}, {}, {}, {},
        {
            {
                new TermMatch(2, 9, 17),
                new TermMatch(3, 18, 22),
                new TermMatch(4, 23, 27)
            }, {
              new TermMatch(6, 35, 43), new TermMatch(7, 44, 54)
        }
        }
    });
  }

  public void testPointQuery() throws IOException {
    IndexOrDocValuesQuery pointQuery = new IndexOrDocValuesQuery(
        IntPoint.newExactQuery(FIELD_POINT, 10),
        NumericDocValuesField.newSlowExactQuery(FIELD_POINT, 10)
    );
    Term t = new Term(FIELD_WITH_OFFSETS, "w1");
    Query query = new BooleanQuery.Builder()
        .add(new TermQuery(t), BooleanClause.Occur.MUST)
        .add(pointQuery, BooleanClause.Occur.MUST)
        .build();

    checkMatches(pointQuery, FIELD_WITH_OFFSETS, new int[][]{});

    checkMatches(query, FIELD_WITH_OFFSETS, new int[][]{
        { 0, 0, 0, 0, 2 },
        { 1, 0, 0, 0, 2 },
        { 2, 0, 0, 0, 2 },
        { 3, 0, 0, 0, 2, 2, 2, 6, 8 },
        { 4 }
    });

    pointQuery = new IndexOrDocValuesQuery(
        IntPoint.newExactQuery(FIELD_POINT, 11),
        NumericDocValuesField.newSlowExactQuery(FIELD_POINT, 11)
    );

    query = new BooleanQuery.Builder()
        .add(new TermQuery(t), BooleanClause.Occur.MUST)
        .add(pointQuery, BooleanClause.Occur.MUST)
        .build();
    checkMatches(query, FIELD_WITH_OFFSETS, new int[][]{});

    query = new BooleanQuery.Builder()
        .add(new TermQuery(t), BooleanClause.Occur.MUST)
        .add(pointQuery, BooleanClause.Occur.SHOULD)
        .build();
    checkMatches(query, FIELD_WITH_OFFSETS, new int[][]{
        {0, 0, 0, 0, 2},
        {1, 0, 0, 0, 2},
        {2, 0, 0, 0, 2},
        {3, 0, 0, 0, 2, 2, 2, 6, 8},
        {4}
    });
  }

  public void testMinimalSeekingWithWildcards() throws IOException {
    SeekCountingLeafReader reader = new SeekCountingLeafReader(getOnlyLeafReader(this.reader));
    this.searcher = new IndexSearcher(reader);
    Query query = new PrefixQuery(new Term(FIELD_WITH_OFFSETS, "w"));
    Weight w = searcher.createWeight(query.rewrite(reader), ScoreMode.COMPLETE, 1);

    // docs 0-3 match several different terms here, but we only seek to the first term and
    // then short-cut return; other terms are ignored until we try and iterate over matches
    int[] expectedSeeks = new int[]{ 1, 1, 1, 1, 6, 6 };
    int i = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
        reader.seeks = 0;
        w.matches(ctx, doc);
        assertEquals("Unexpected seek count on doc " + doc, expectedSeeks[i], reader.seeks);
        i++;
      }
    }
  }

  private static class SeekCountingLeafReader extends FilterLeafReader {

    int seeks = 0;

    public SeekCountingLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = super.terms(field);
      if (terms == null) {
        return null;
      }
      return new FilterTerms(terms) {
        @Override
        public TermsEnum iterator() throws IOException {
          return new FilterTermsEnum(super.iterator()) {
            @Override
            public boolean seekExact(BytesRef text) throws IOException {
              seeks++;
              return super.seekExact(text);
            }
          };
        }
      };
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

}
