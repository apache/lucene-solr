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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** tests BooleanScorer2's minShouldMatch */
public class TestMinShouldMatch2 extends LuceneTestCase {
  static Directory dir;
  static DirectoryReader r;
  static LeafReader reader;
  static IndexSearcher searcher;
  
  static final String alwaysTerms[] = { "a" };
  static final String commonTerms[] = { "b", "c", "d" };
  static final String mediumTerms[] = { "e", "f", "g" };
  static final String rareTerms[]   = { "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
  
  enum Mode {
    SCORER,
    BULK_SCORER,
    DOC_VALUES
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(300);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      
      addSome(doc, alwaysTerms);
      
      if (random().nextInt(100) < 90) {
        addSome(doc, commonTerms);
      }
      if (random().nextInt(100) < 50) {
        addSome(doc, mediumTerms);
      }
      if (random().nextInt(100) < 10) {
        addSome(doc, rareTerms);
      }
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    r = DirectoryReader.open(dir);
    reader = getOnlyLeafReader(r);
    searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new ClassicSimilarity());
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    dir.close();
    searcher = null;
    reader = null;
    r = null;
    dir = null;
  }
  
  private static void addSome(Document doc, String values[]) {
    List<String> list = Arrays.asList(values);
    Collections.shuffle(list, random());
    int howMany = TestUtil.nextInt(random(), 1, list.size());
    for (int i = 0; i < howMany; i++) {
      doc.add(new StringField("field", list.get(i), Field.Store.NO));
      doc.add(new SortedSetDocValuesField("dv", new BytesRef(list.get(i))));
    }
  }
  
  private Scorer scorer(String values[], int minShouldMatch, Mode mode) throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (String value : values) {
      bq.add(new TermQuery(new Term("field", value)), BooleanClause.Occur.SHOULD);
    }
    bq.setMinimumNumberShouldMatch(minShouldMatch);

    BooleanWeight weight = (BooleanWeight) searcher.createWeight(searcher.rewrite(bq.build()), ScoreMode.COMPLETE, 1);
    
    switch (mode) {
    case DOC_VALUES:
      return new SlowMinShouldMatchScorer(weight, reader, searcher);
    case SCORER:
      return weight.scorer(reader.getContext());
    case BULK_SCORER:
      final BulkScorer bulkScorer = weight.optionalBulkScorer(reader.getContext());
      if (bulkScorer == null) {
        if (weight.scorer(reader.getContext()) != null) {
          throw new AssertionError("BooleanScorer should be applicable for this query");
        }
        return null;
      }
      return new BulkScorerWrapperScorer(weight, bulkScorer, TestUtil.nextInt(random(), 1, 100));
    default:
      throw new AssertionError();
    }
  }
  
  private void assertNext(Scorer expected, Scorer actual) throws Exception {
    if (actual == null) {
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, expected.iterator().nextDoc());
      return;
    }
    int doc;
    DocIdSetIterator expectedIt = expected.iterator();
    DocIdSetIterator actualIt = actual.iterator();
    while ((doc = expectedIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(doc, actualIt.nextDoc());
      float expectedScore = expected.score();
      float actualScore = actual.score();
      assertEquals(expectedScore, actualScore, 0d);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, actualIt.nextDoc());
  }
  
  private void assertAdvance(Scorer expected, Scorer actual, int amount) throws Exception {
    if (actual == null) {
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, expected.iterator().nextDoc());
      return;
    }
    DocIdSetIterator expectedIt = expected.iterator();
    DocIdSetIterator actualIt = actual.iterator();
    int prevDoc = 0;
    int doc;
    while ((doc = expectedIt.advance(prevDoc+amount)) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(doc, actualIt.advance(prevDoc+amount));
      float expectedScore = expected.score();
      float actualScore = actual.score();
      assertEquals(expectedScore, actualScore, 0d);
      prevDoc = doc;
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, actualIt.advance(prevDoc+amount));
  }
  
  /** simple test for next(): minShouldMatch=2 on 3 terms (one common, one medium, one rare) */
  public void testNextCMR2() throws Exception {
    for (int common = 0; common < commonTerms.length; common++) {
      for (int medium = 0; medium < mediumTerms.length; medium++) {
        for (int rare = 0; rare < rareTerms.length; rare++) {
          Scorer expected = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.DOC_VALUES);
          Scorer actual = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.SCORER);
          assertNext(expected, actual);

          expected = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.DOC_VALUES);
          actual = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.BULK_SCORER);
          assertNext(expected, actual);
        }
      }
    }
  }
  
  /** simple test for advance(): minShouldMatch=2 on 3 terms (one common, one medium, one rare) */
  public void testAdvanceCMR2() throws Exception {
    for (int amount = 25; amount < 200; amount += 25) {
      for (int common = 0; common < commonTerms.length; common++) {
        for (int medium = 0; medium < mediumTerms.length; medium++) {
          for (int rare = 0; rare < rareTerms.length; rare++) {
            Scorer expected = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.DOC_VALUES);
            Scorer actual = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.SCORER);
            assertAdvance(expected, actual, amount);

            expected = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.DOC_VALUES);
            actual = scorer(new String[] { commonTerms[common], mediumTerms[medium], rareTerms[rare] }, 2, Mode.BULK_SCORER);
            assertAdvance(expected, actual, amount);
          }
        }
      }
    }
  }
  
  /** test next with giant bq of all terms with varying minShouldMatch */
  public void testNextAllTerms() throws Exception {
    List<String> termsList = new ArrayList<>();
    termsList.addAll(Arrays.asList(commonTerms));
    termsList.addAll(Arrays.asList(mediumTerms));
    termsList.addAll(Arrays.asList(rareTerms));
    String terms[] = termsList.toArray(new String[0]);
    
    for (int minNrShouldMatch = 1; minNrShouldMatch <= terms.length; minNrShouldMatch++) {
      Scorer expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
      Scorer actual = scorer(terms, minNrShouldMatch, Mode.SCORER);
      assertNext(expected, actual);

      expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
      actual = scorer(terms, minNrShouldMatch, Mode.BULK_SCORER);
      assertNext(expected, actual);
    }
  }
  
  /** test advance with giant bq of all terms with varying minShouldMatch */
  public void testAdvanceAllTerms() throws Exception {
    List<String> termsList = new ArrayList<>();
    termsList.addAll(Arrays.asList(commonTerms));
    termsList.addAll(Arrays.asList(mediumTerms));
    termsList.addAll(Arrays.asList(rareTerms));
    String terms[] = termsList.toArray(new String[0]);
    
    for (int amount = 25; amount < 200; amount += 25) {
      for (int minNrShouldMatch = 1; minNrShouldMatch <= terms.length; minNrShouldMatch++) {
        Scorer expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
        Scorer actual = scorer(terms, minNrShouldMatch, Mode.SCORER);
        assertAdvance(expected, actual, amount);

        expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
        actual = scorer(terms, minNrShouldMatch, Mode.BULK_SCORER);
        assertAdvance(expected, actual, amount);
      }
    }
  }
  
  /** test next with varying numbers of terms with varying minShouldMatch */
  public void testNextVaryingNumberOfTerms() throws Exception {
    List<String> termsList = new ArrayList<>();
    termsList.addAll(Arrays.asList(commonTerms));
    termsList.addAll(Arrays.asList(mediumTerms));
    termsList.addAll(Arrays.asList(rareTerms));
    Collections.shuffle(termsList, random());
    for (int numTerms = 2; numTerms <= termsList.size(); numTerms++) {
      String terms[] = termsList.subList(0, numTerms).toArray(new String[0]);
      for (int minNrShouldMatch = 1; minNrShouldMatch <= terms.length; minNrShouldMatch++) {
        Scorer expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
        Scorer actual = scorer(terms, minNrShouldMatch, Mode.SCORER);
        assertNext(expected, actual);

        expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
        actual = scorer(terms, minNrShouldMatch, Mode.BULK_SCORER);
        assertNext(expected, actual);
      }
    }
  }
  
  /** test advance with varying numbers of terms with varying minShouldMatch */
  @Nightly
  public void testAdvanceVaryingNumberOfTerms() throws Exception {
    List<String> termsList = new ArrayList<>();
    termsList.addAll(Arrays.asList(commonTerms));
    termsList.addAll(Arrays.asList(mediumTerms));
    termsList.addAll(Arrays.asList(rareTerms));
    Collections.shuffle(termsList, random());
    
    for (int amount = 25; amount < 200; amount += 25) {
      for (int numTerms = 2; numTerms <= termsList.size(); numTerms++) {
        String terms[] = termsList.subList(0, numTerms).toArray(new String[0]);
        for (int minNrShouldMatch = 1; minNrShouldMatch <= terms.length; minNrShouldMatch++) {
          Scorer expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
          Scorer actual = scorer(terms, minNrShouldMatch, Mode.SCORER);
          assertAdvance(expected, actual, amount);

          expected = scorer(terms, minNrShouldMatch, Mode.DOC_VALUES);
          actual = scorer(terms, minNrShouldMatch, Mode.SCORER);
          assertAdvance(expected, actual, amount);
        }
      }
    }
  }
  
  // TODO: more tests
  
  // a slow min-should match scorer that uses a docvalues field.
  // later, we can make debugging easier as it can record the set of ords it currently matched
  // and e.g. print out their values and so on for the document
  static class SlowMinShouldMatchScorer extends Scorer {
    int currentDoc = -1;     // current docid
    int currentMatched = -1; // current number of terms matched
    
    final SortedSetDocValues dv;
    final int maxDoc;

    final Set<Long> ords = new HashSet<>();
    final LeafSimScorer[] sims;
    final int minNrShouldMatch;
    
    double score = Float.NaN;

    SlowMinShouldMatchScorer(BooleanWeight weight, LeafReader reader, IndexSearcher searcher) throws IOException {
      super(weight);
      this.dv = reader.getSortedSetDocValues("dv");
      this.maxDoc = reader.maxDoc();
      BooleanQuery bq = (BooleanQuery) weight.getQuery();
      this.minNrShouldMatch = bq.getMinimumNumberShouldMatch();
      this.sims = new LeafSimScorer[(int)dv.getValueCount()];
      for (BooleanClause clause : bq.clauses()) {
        assert !clause.isProhibited();
        assert !clause.isRequired();
        Term term = ((TermQuery)clause.getQuery()).getTerm();
        long ord = dv.lookupTerm(term.bytes());
        if (ord >= 0) {
          boolean success = ords.add(ord);
          assert success; // no dups
          TermStates ts = TermStates.build(reader.getContext(), term, true);
          SimScorer w = weight.similarity.scorer(1f,
                        searcher.collectionStatistics("field"),
                        searcher.termStatistics(term, ts.docFreq(), ts.totalTermFreq()));
          sims[(int)ord] = new LeafSimScorer(w, reader, "field", true);
        }
      }
    }

    @Override
    public float score() throws IOException {
      assert score != 0 : currentMatched;
      return (float)score;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }

    public int docID() {
      return currentDoc;
    }

    @Override
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {
        
        @Override
        public int nextDoc() throws IOException {
          assert currentDoc != NO_MORE_DOCS;
          for (currentDoc = currentDoc+1; currentDoc < maxDoc; currentDoc++) {
            currentMatched = 0;
            score = 0;
            if (currentDoc > dv.docID()) {
              dv.advance(currentDoc);
            }
            if (currentDoc != dv.docID()) {
              continue;
            }
            long ord;
            while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              if (ords.contains(ord)) {
                currentMatched++;
                score += sims[(int)ord].score(currentDoc, 1);
              }
            }
            if (currentMatched >= minNrShouldMatch) {
              return currentDoc;
            }
          }
          return currentDoc = NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
          int doc;
          while ((doc = nextDoc()) < target) {
          }
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }
        
        @Override
        public int docID() {
          return currentDoc;
        }
      };
    }
  }
}
