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
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@LuceneTestCase.SuppressCodecs("SimpleText")
public class TestDoubleValuesSource extends LuceneTestCase {

  private static final double LEAST_DOUBLE_VALUE = 45.72;

  private static Directory dir;
  private static IndexReader reader;
  private static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs;
    if (TEST_NIGHTLY) {
      numDocs = TestUtil.nextInt(random(), 2049, 4000);
    } else {
      numDocs = atLeast(546);
    }
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      document.add(newTextField("english", English.intToEnglish(i), Field.Store.NO));
      document.add(newTextField("oddeven", (i % 2 == 0) ? "even" : "odd", Field.Store.NO));
      document.add(new NumericDocValuesField("int", random().nextInt()));
      document.add(new NumericDocValuesField("long", random().nextLong()));
      document.add(new FloatDocValuesField("float", random().nextFloat()));
      document.add(new DoubleDocValuesField("double", random().nextDouble()));
      if (i == 545)
        document.add(new DoubleDocValuesField("onefield", LEAST_DOUBLE_VALUE));
      iw.addDocument(document);
    }
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    dir.close();
    searcher = null;
    reader = null;
    dir = null;
  }

  public void testSortMissingZeroDefault() throws Exception {
    // docs w/no value get default missing value = 0

    DoubleValuesSource onefield = DoubleValuesSource.fromDoubleField("onefield");
    // sort decreasing
    TopDocs results = searcher.search(new MatchAllDocsQuery(), 1, new Sort(onefield.getSortField(true)));
    FieldDoc first = (FieldDoc) results.scoreDocs[0];
    assertEquals(LEAST_DOUBLE_VALUE, first.fields[0]);

    // sort increasing
    results = searcher.search(new MatchAllDocsQuery(), 1, new Sort(onefield.getSortField(false)));
    first = (FieldDoc) results.scoreDocs[0];
    assertEquals(0d, first.fields[0]);
  }

  public void testSortMissingExplicit() throws Exception {
    // docs w/no value get provided missing value

    DoubleValuesSource onefield = DoubleValuesSource.fromDoubleField("onefield");

    // sort decreasing, missing last
    SortField oneFieldSort = onefield.getSortField(true);
    oneFieldSort.setMissingValue(Double.MIN_VALUE);

    TopDocs results = searcher.search(new MatchAllDocsQuery(), 1, new Sort(oneFieldSort));
    FieldDoc first = (FieldDoc) results.scoreDocs[0];
    assertEquals(LEAST_DOUBLE_VALUE, first.fields[0]);

    // sort increasing, missing last
    oneFieldSort = onefield.getSortField(false);
    oneFieldSort.setMissingValue(Double.MAX_VALUE);

    results = searcher.search(new MatchAllDocsQuery(), 1, new Sort(oneFieldSort));
    first = (FieldDoc) results.scoreDocs[0];
    assertEquals(LEAST_DOUBLE_VALUE, first.fields[0]);
  }

  public void testSimpleFieldEquivalences() throws Exception {
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("int", SortField.Type.INT, random().nextBoolean())));
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("long", SortField.Type.LONG, random().nextBoolean())));
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("float", SortField.Type.FLOAT, random().nextBoolean())));
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("double", SortField.Type.DOUBLE, random().nextBoolean())));
  }

  public void testHashCodeAndEquals() {
    DoubleValuesSource vs1 = DoubleValuesSource.fromDoubleField("double");
    DoubleValuesSource vs2 = DoubleValuesSource.fromDoubleField("double");
    assertEquals(vs1, vs2);
    assertEquals(vs1.hashCode(), vs2.hashCode());
    DoubleValuesSource v3 = DoubleValuesSource.fromLongField("long");
    assertFalse(vs1.equals(v3));

    assertEquals(DoubleValuesSource.constant(5), DoubleValuesSource.constant(5));
    assertEquals(DoubleValuesSource.constant(5).hashCode(), DoubleValuesSource.constant(5).hashCode());
    assertFalse((DoubleValuesSource.constant(5).equals(DoubleValuesSource.constant(6))));

    assertEquals(DoubleValuesSource.SCORES, DoubleValuesSource.SCORES);
    assertFalse(DoubleValuesSource.constant(5).equals(DoubleValuesSource.SCORES));

  }

  public void testSimpleFieldSortables() throws Exception {
    int n = atLeast(4);
    for (int i = 0; i < n; i++) {
      Sort sort = randomSort();
      checkSorts(new MatchAllDocsQuery(), sort);
      checkSorts(new TermQuery(new Term("english", "one")), sort);
    }
  }

  Sort randomSort() throws Exception {
    boolean reversed = random().nextBoolean();
    SortField fields[] = new SortField[] {
        new SortField("int", SortField.Type.INT, reversed),
        new SortField("long", SortField.Type.LONG, reversed),
        new SortField("float", SortField.Type.FLOAT, reversed),
        new SortField("double", SortField.Type.DOUBLE, reversed),
        new SortField("score", SortField.Type.SCORE)
    };
    Collections.shuffle(Arrays.asList(fields), random());
    int numSorts = TestUtil.nextInt(random(), 1, fields.length);
    return new Sort(ArrayUtil.copyOfSubArray(fields, 0, numSorts));
  }

  // Take a Sort, and replace any field sorts with Sortables
  Sort convertSortToSortable(Sort sort) {
    SortField original[] = sort.getSort();
    SortField mutated[] = new SortField[original.length];
    for (int i = 0; i < mutated.length; i++) {
      if (random().nextInt(3) > 0) {
        SortField s = original[i];
        boolean reverse = s.getType() == SortField.Type.SCORE || s.getReverse();
        switch (s.getType()) {
          case INT:
            mutated[i] = DoubleValuesSource.fromIntField(s.getField()).getSortField(reverse);
            break;
          case LONG:
            mutated[i] = DoubleValuesSource.fromLongField(s.getField()).getSortField(reverse);
            break;
          case FLOAT:
            mutated[i] = DoubleValuesSource.fromFloatField(s.getField()).getSortField(reverse);
            break;
          case DOUBLE:
            mutated[i] = DoubleValuesSource.fromDoubleField(s.getField()).getSortField(reverse);
            break;
          case SCORE:
            mutated[i] = DoubleValuesSource.SCORES.getSortField(reverse);
            break;
          default:
            mutated[i] = original[i];
        }
      } else {
        mutated[i] = original[i];
      }
    }

    return new Sort(mutated);
  }

  void checkSorts(Query query, Sort sort) throws Exception {
    int size = TestUtil.nextInt(random(), 1, searcher.getIndexReader().maxDoc() / 5);
    TopDocs expected = searcher.search(query, size, sort, random().nextBoolean());
    Sort mutatedSort = convertSortToSortable(sort);
    TopDocs actual = searcher.search(query, size, mutatedSort, random().nextBoolean());

    CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);

    if (size < actual.totalHits.value) {
      expected = searcher.searchAfter(expected.scoreDocs[size-1], query, size, sort);
      actual = searcher.searchAfter(actual.scoreDocs[size-1], query, size, mutatedSort);
      CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);
    }
  }

  static final Query[] testQueries = new Query[]{
      new MatchAllDocsQuery(),
      new TermQuery(new Term("oddeven", "odd")),
      new BooleanQuery.Builder()
          .add(new TermQuery(new Term("english", "one")), BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term("english", "two")), BooleanClause.Occur.MUST)
          .build()
  };

  public void testExplanations() throws Exception {
    for (Query q : testQueries) {
      testExplanations(q, DoubleValuesSource.fromQuery(new TermQuery(new Term("english", "one"))));
      testExplanations(q, DoubleValuesSource.fromIntField("int"));
      testExplanations(q, DoubleValuesSource.fromLongField("long"));
      testExplanations(q, DoubleValuesSource.fromFloatField("float"));
      testExplanations(q, DoubleValuesSource.fromDoubleField("double"));
      testExplanations(q, DoubleValuesSource.fromDoubleField("onefield"));
      testExplanations(q, DoubleValuesSource.constant(5.45));
    }
  }

  private void testExplanations(Query q, DoubleValuesSource vs) throws IOException {
    DoubleValuesSource rewritten = vs.rewrite(searcher);
    searcher.search(q, new SimpleCollector() {

      DoubleValues v;
      LeafReaderContext ctx;

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        this.ctx = context;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.v = rewritten.getValues(this.ctx, DoubleValuesSource.fromScorer(scorer));
      }

      @Override
      public void collect(int doc) throws IOException {
        Explanation scoreExpl = searcher.explain(q, ctx.docBase + doc);
        if (this.v.advanceExact(doc)) {
          CheckHits.verifyExplanation("", doc, (float) v.doubleValue(), true, rewritten.explain(ctx, doc, scoreExpl));
        }
        else {
          assertFalse(rewritten.explain(ctx, doc, scoreExpl).isMatch());
        }
      }

      @Override
      public ScoreMode scoreMode() {
        return vs.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
      }
    });
  }

  public void testQueryDoubleValuesSource() throws Exception {
    Query iteratingQuery = new TermQuery(new Term("english", "two"));
    Query approximatingQuery = new PhraseQuery.Builder()
      .add(new Term("english", "hundred"), 0)
      .add(new Term("english", "one"), 1)
      .build();

    doTestQueryDoubleValuesSources(iteratingQuery);
    doTestQueryDoubleValuesSources(approximatingQuery);
  }

  private void doTestQueryDoubleValuesSources(Query q) throws Exception {
    DoubleValuesSource vs = DoubleValuesSource.fromQuery(q).rewrite(searcher);
    searcher.search(q, new SimpleCollector() {

      DoubleValues v;
      Scorable scorer;
      LeafReaderContext ctx;

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        this.ctx = context;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
        this.v = vs.getValues(this.ctx, DoubleValuesSource.fromScorer(scorer));
      }

      @Override
      public void collect(int doc) throws IOException {
        assertTrue(v.advanceExact(doc));
        assertEquals(scorer.score(), v.doubleValue(), 0.00001);
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
      }
    });
  }

}
