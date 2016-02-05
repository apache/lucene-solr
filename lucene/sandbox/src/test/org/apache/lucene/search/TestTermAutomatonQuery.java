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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Transition;

public class TestTermAutomatonQuery extends LuceneTestCase {
  // "comes * sun"
  public void testBasic1() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    // matches
    doc.add(newTextField("field", "here comes the sun", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    // doesn't match
    doc.add(newTextField("field", "here comes the other sun", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "comes");
    int s2 = q.createState();
    q.addAnyTransition(s1, s2);
    int s3 = q.createState();
    q.setAccept(s3, true);
    q.addTransition(s2, s3, "sun");
    q.finish();

    assertEquals(1, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  // "comes * (sun|moon)"
  public void testBasicSynonym() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "here comes the sun", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "here comes the moon", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "comes");
    int s2 = q.createState();
    q.addAnyTransition(s1, s2);
    int s3 = q.createState();
    q.setAccept(s3, true);
    q.addTransition(s2, s3, "sun");
    q.addTransition(s2, s3, "moon");
    q.finish();

    assertEquals(2, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  // "comes sun" or "comes * sun"
  public void testBasicSlop() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "here comes the sun", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "here comes sun", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "here comes the other sun", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "comes");
    int s2 = q.createState();
    q.addAnyTransition(s1, s2);
    int s3 = q.createState();
    q.setAccept(s3, true);
    q.addTransition(s1, s3, "sun");
    q.addTransition(s2, s3, "sun");
    q.finish();

    assertEquals(2, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  // Verify posLength is "respected" at query time: index "speedy wifi
  // network", search on "fast wi fi network" using (simulated!)
  // query-time syn filter to add "wifi" over "wi fi" with posLength=2.
  // To make this real we need a version of TS2A that operates on whole
  // terms, not characters.
  public void testPosLengthAtQueryTimeMock() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "speedy wifi network", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "speedy wi fi network", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "fast wifi network", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "fast wi fi network", Field.Store.NO));
    w.addDocument(doc);

    // doesn't match:
    doc = new Document();
    doc.add(newTextField("field", "slow wi fi network", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "fast");
    q.addTransition(init, s1, "speedy");
    int s2 = q.createState();
    int s3 = q.createState();
    q.addTransition(s1, s2, "wi");
    q.addTransition(s1, s3, "wifi");
    q.addTransition(s2, s3, "fi");
    int s4 = q.createState();
    q.addTransition(s3, s4, "network");
    q.setAccept(s4, true);
    q.finish();

    // System.out.println("DOT:\n" + q.toDot());
    
    assertEquals(4, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  public void testPosLengthAtQueryTimeTrueish() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "speedy wifi network", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "speedy wi fi network", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "fast wifi network", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "fast wi fi network", Field.Store.NO));
    w.addDocument(doc);

    // doesn't match:
    doc = new Document();
    doc.add(newTextField("field", "slow wi fi network", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TokenStream ts = new CannedTokenStream(new Token[] {
        token("fast", 1, 1),
        token("speedy", 0, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
      });

    TermAutomatonQuery q = new TokenStreamToTermAutomatonQuery().toQuery("field", ts);
    // System.out.println("DOT: " + q.toDot());
    assertEquals(4, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  public void testFreq() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    // matches freq == 3
    doc.add(newTextField("field", "here comes the sun foo bar here comes another sun here comes shiny sun", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    // doesn't match
    doc.add(newTextField("field", "here comes the other sun", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "comes");
    int s2 = q.createState();
    q.addAnyTransition(s1, s2);
    int s3 = q.createState();
    q.setAccept(s3, true);
    q.addTransition(s2, s3, "sun");
    q.finish();

    s.search(q, new SimpleCollector() {
        private Scorer scorer;

        @Override
        public void setScorer(Scorer scorer) {
          this.scorer = scorer;
          while (scorer instanceof AssertingScorer) {
            scorer = ((AssertingScorer) scorer).getIn();
          }
          assert scorer instanceof TermAutomatonScorer;
        }

        @Override
        public void collect(int docID) throws IOException {
          assertEquals(3, scorer.freq());
        }
        
        @Override
        public boolean needsScores() {
          return true;
        }
      });

    w.close();
    r.close();
    dir.close();
  }

  public void testSegsMissingTerms() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "here comes the sun", Field.Store.NO));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(newTextField("field", "here comes the moon", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "comes");
    int s2 = q.createState();
    q.addAnyTransition(s1, s2);
    int s3 = q.createState();
    q.setAccept(s3, true);
    q.addTransition(s2, s3, "sun");
    q.addTransition(s2, s3, "moon");
    q.finish();

    assertEquals(2, s.search(q, 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  public void testInvalidLeadWithAny() throws Exception {
    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int s0 = q.createState();
    int s1 = q.createState();
    int s2 = q.createState();
    q.setAccept(s2, true);
    q.addAnyTransition(s0, s1);
    q.addTransition(s1, s2, "b");
    try {
      q.finish();
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  public void testInvalidTrailWithAny() throws Exception {
    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int s0 = q.createState();
    int s1 = q.createState();
    int s2 = q.createState();
    q.setAccept(s2, true);
    q.addTransition(s0, s1, "b");
    q.addAnyTransition(s1, s2);
    try {
      q.finish();
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
  }
  
  public void testAnyFromTokenStream() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "here comes the sun", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "here comes the moon", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "here comes sun", Field.Store.NO));
    w.addDocument(doc);

    // Should not match:
    doc = new Document();
    doc.add(newTextField("field", "here comes the other sun", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TokenStream ts = new CannedTokenStream(new Token[] {
        token("comes", 1, 1),
        token("comes", 0, 2),
        token("*", 1, 1),
        token("sun", 1, 1),
        token("moon", 0, 1)
      });

    TermAutomatonQuery q = new TokenStreamToTermAutomatonQuery().toQuery("field", ts);
    // System.out.println("DOT: " + q.toDot());
    assertEquals(3, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  private static Token token(String term, int posInc, int posLength) {
    final Token t = new Token(term, 0, term.length());
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }

  private static class RandomSynonymFilter extends TokenFilter {
    private boolean synNext;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    public RandomSynonymFilter(TokenFilter in) {
      super(in);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (synNext) {
        clearAttributes();
        posIncAtt.setPositionIncrement(0);
        termAtt.append(""+((char) 97 + random().nextInt(3)));
        synNext = false;
        return true;
      }

      if (input.incrementToken()) {
        if (random().nextInt(10) == 8) {
          synNext = true;
        }
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      synNext = false;
    }
  }

  public void testRandom() throws Exception {
    int numDocs = atLeast(100);
    Directory dir = newDirectory();

    // Adds occassional random synonyms:
    Analyzer analyzer = new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String fieldName) {
          MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true, 100);
          tokenizer.setEnableChecks(true);
          TokenFilter filt = new MockTokenFilter(tokenizer, MockTokenFilter.EMPTY_STOPSET);
          filt = new RandomSynonymFilter(filt);
          return new TokenStreamComponents(tokenizer, filt);
        }
      };

    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      int numTokens = atLeast(10);

      StringBuilder sb = new StringBuilder();
      for(int j=0;j<numTokens;j++) {
        sb.append(' ');
        sb.append((char) (97 + random().nextInt(3)));
      }
      String contents = sb.toString();
      doc.add(newTextField("field", contents, Field.Store.NO));
      doc.add(new StoredField("id", ""+i));
      if (VERBOSE) {
        System.out.println("  doc " + i + " -> " + contents);
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    // Used to match ANY using MultiPhraseQuery:
    Term[] allTerms = new Term[] {new Term("field", "a"),
                                  new Term("field", "b"),
                                  new Term("field", "c")};
    int numIters = atLeast(1000);
    for(int iter=0;iter<numIters;iter++) {

      // Build the (finite, no any transitions) TermAutomatonQuery and
      // also the "equivalent" BooleanQuery and make sure they match the
      // same docs:
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      int count = TestUtil.nextInt(random(), 1, 5);
      Set<BytesRef> strings = new HashSet<>();
      for(int i=0;i<count;i++) {
        StringBuilder sb = new StringBuilder();
        int numTokens = TestUtil.nextInt(random(), 1, 5);
        for(int j=0;j<numTokens;j++) {
          if (j > 0 && j < numTokens-1 && random().nextInt(5) == 3) {
            sb.append('*');
          } else {
            sb.append((char) (97 + random().nextInt(3)));
          }
        }
        String string = sb.toString();
        MultiPhraseQuery mpq = new MultiPhraseQuery();
        for(int j=0;j<string.length();j++) {
          if (string.charAt(j) == '*') {
            mpq.add(allTerms);
          } else {
            mpq.add(new Term("field", ""+string.charAt(j)));
          }
        }
        bq.add(mpq, BooleanClause.Occur.SHOULD);
        strings.add(new BytesRef(string));
      }

      List<BytesRef> stringsList = new ArrayList<>(strings);
      Collections.sort(stringsList);

      Automaton a = Automata.makeStringUnion(stringsList);

      // Translate automaton to query:
    
      TermAutomatonQuery q = new TermAutomatonQuery("field");
      int numStates = a.getNumStates();
      for(int i=0;i<numStates;i++) {
        q.createState();
        q.setAccept(i, a.isAccept(i));
      }

      Transition t = new Transition();
      for(int i=0;i<numStates;i++) {
        int transCount = a.initTransition(i, t);
        for(int j=0;j<transCount;j++) {
          a.getNextTransition(t);
          for(int label=t.min;label<=t.max;label++) {
            if ((char) label == '*') {
              q.addAnyTransition(t.source, t.dest);
            } else {
              q.addTransition(t.source, t.dest, ""+(char) label);
            }
          }
        }
      }
      q.finish();

      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
        for(BytesRef string : stringsList) {
          System.out.println("  string: " + string.utf8ToString());
        }
        System.out.println(q.toDot());
      }
      
      Query q1 = q;
      Query q2 = bq.build();
      if (random().nextInt(5) == 1) {
        if (VERBOSE) {
          System.out.println("  use random filter");
        }
        RandomQuery filter = new RandomQuery(random().nextLong(), random().nextFloat());
        q1 = new BooleanQuery.Builder()
            .add(q1, Occur.MUST)
            .add(filter, Occur.FILTER)
            .build();
        q2 = new BooleanQuery.Builder()
            .add(q2, Occur.MUST)
            .add(filter, Occur.FILTER)
            .build();
      }

      TopDocs hits1 = s.search(q1, numDocs);
      TopDocs hits2 = s.search(q2, numDocs);
      Set<String> hits1Docs = toDocIDs(s, hits1);
      Set<String> hits2Docs = toDocIDs(s, hits2);

      try {
        assertEquals(hits2.totalHits, hits1.totalHits);
        assertEquals(hits2Docs, hits1Docs);
      } catch (AssertionError ae) {
        System.out.println("FAILED:");
        for(String id : hits1Docs) {
          if (hits2Docs.contains(id) == false) {
            System.out.println(String.format(Locale.ROOT, "  id=%3s matched but should not have", id));
          }
        }
        for(String id : hits2Docs) {
          if (hits1Docs.contains(id) == false) {
            System.out.println(String.format(Locale.ROOT, "  id=%3s did not match but should have", id));
          }
        }
        throw ae;
      }
    }

    IOUtils.close(w, r, dir, analyzer);
  }

  private Set<String> toDocIDs(IndexSearcher s, TopDocs hits) throws IOException {
    Set<String> result = new HashSet<>();
    for(ScoreDoc hit : hits.scoreDocs) {
      result.add(s.doc(hit.doc).get("id"));
    }
    return result;
  }

  private static class RandomQuery extends Query {
    private final long seed;
    private float density;

    // density should be 0.0 ... 1.0
    public RandomQuery(long seed, float density) {
      this.seed = seed;
      this.density = density;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new ConstantScoreWeight(this) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          int maxDoc = context.reader().maxDoc();
          FixedBitSet bits = new FixedBitSet(maxDoc);
          Random random = new Random(seed ^ context.docBase);
          for(int docID=0;docID<maxDoc;docID++) {
            if (random.nextFloat() <= density) {
              bits.set(docID);
              //System.out.println("  acc id=" + idSource.getInt(docID) + " docID=" + docID);
            }
          }
          return new ConstantScoreScorer(this, score(), new BitSetIterator(bits, bits.approximateCardinality()));
        }
      };
    }

    @Override
    public String toString(String field) {
      return "RandomFilter(seed=" + seed + ",density=" + density + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      RandomQuery other = (RandomQuery) obj;
      return seed == other.seed && density == other.density;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), seed, density);
    }
  }

  /** See if we can create a TAQ with cycles */
  public void testWithCycles1() throws Exception {
    
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "here comes here comes", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "comes here", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "here");
    q.addTransition(s1, init, "comes");
    q.setAccept(init, true);
    q.finish();

    assertEquals(1, s.search(q, 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  /** See if we can create a TAQ with cycles */
  public void testWithCycles2() throws Exception {
    
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "here comes kaoma", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "here comes sun sun sun sun kaoma", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TermAutomatonQuery q = new TermAutomatonQuery("field");
    int init = q.createState();
    int s1 = q.createState();
    q.addTransition(init, s1, "here");
    int s2 = q.createState();
    q.addTransition(s1, s2, "comes");
    int s3 = q.createState();
    q.addTransition(s2, s3, "sun");
    q.addTransition(s3, s3, "sun");
    int s4 = q.createState();
    q.addTransition(s3, s4, "kaoma");
    q.setAccept(s4, true);
    q.finish();

    assertEquals(1, s.search(q, 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  public void testTermDoesNotExist() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "x y z", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TokenStream ts = new CannedTokenStream(new Token[] {
        token("a", 1, 1),
      });

    TermAutomatonQuery q = new TokenStreamToTermAutomatonQuery().toQuery("field", ts);
    // System.out.println("DOT: " + q.toDot());
    assertEquals(0, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }

  public void testOneTermDoesNotExist() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "x y z", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    TokenStream ts = new CannedTokenStream(new Token[] {
        token("a", 1, 1),
        token("x", 1, 1),
      });

    TermAutomatonQuery q = new TokenStreamToTermAutomatonQuery().toQuery("field", ts);
    // System.out.println("DOT: " + q.toDot());
    assertEquals(0, s.search(q, 1).totalHits);

    w.close();
    r.close();
    dir.close();
  }
}
