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
import java.util.BitSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;


public class TestScorerPerf extends LuceneTestCase {
  private final boolean validate = true;  // set to false when doing performance testing

  public void createRandomTerms(int nDocs, int nTerms, double power, Directory dir) throws Exception {
    int[] freq = new int[nTerms];
    Term[] terms = new Term[nTerms];
    for (int i=0; i<nTerms; i++) {
      int f = (nTerms+1)-i;  // make first terms less frequent
      freq[i] = (int)Math.ceil(Math.pow(f,power));
      terms[i] = new Term("f",Character.toString((char)('A'+i)));
    }

    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
    for (int i=0; i<nDocs; i++) {
      Document d = new Document();
      for (int j=0; j<nTerms; j++) {
        if (random().nextInt(freq[j]) == 0) {
          d.add(newStringField("f", terms[j].text(), Field.Store.NO));
          //System.out.println(d);
        }
      }
      iw.addDocument(d);
    }
    iw.forceMerge(1);
    iw.close();
  }


  public FixedBitSet randBitSet(int sz, int numBitsToSet) {
    FixedBitSet set = new FixedBitSet(sz);
    for (int i=0; i<numBitsToSet; i++) {
      set.set(random().nextInt(sz));
    }
    return set;
  }

  public FixedBitSet[] randBitSets(int numSets, int setSize) {
    FixedBitSet[] sets = new FixedBitSet[numSets];
    for (int i=0; i<sets.length; i++) {
      sets[i] = randBitSet(setSize, random().nextInt(setSize));
    }
    return sets;
  }

  public static class CountingHitCollector extends SimpleCollector {
    int count=0;
    int sum=0;
    protected int docBase = 0;
    
    @Override
    public void collect(int doc) {
      count++;
      sum += docBase + doc;  // use it to avoid any possibility of being eliminated by hotspot
    }

    public int getCount() { return count; }
    public int getSum() { return sum; }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }


  public static class MatchingHitCollector extends CountingHitCollector {
    FixedBitSet answer;
    int pos=-1;
    public MatchingHitCollector(FixedBitSet answer) {
      this.answer = answer;
    }

    public void collect(int doc, float score) {
      
      pos = answer.nextSetBit(pos+1);
      if (pos != doc + docBase) {
        throw new RuntimeException("Expected doc " + pos + " but got " + doc + docBase);
      }
      super.collect(doc);
    }
  }

  private static class BitSetQuery extends Query {

    private final FixedBitSet docs;

    BitSetQuery(FixedBitSet docs) {
      this.docs = docs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return new ConstantScoreScorer(this, score(), scoreMode, new BitSetIterator(docs, docs.approximateCardinality()));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }
    
    @Override
    public String toString(String field) {
      return "randomBitSetFilter";
    }
    
    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             docs.equals(((BitSetQuery) other).docs);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + docs.hashCode();
    }
  }

  FixedBitSet addClause(FixedBitSet[] sets, BooleanQuery.Builder bq, FixedBitSet result) {
    final FixedBitSet rnd = sets[random().nextInt(sets.length)];
    Query q = new BitSetQuery(rnd);
    bq.add(q, BooleanClause.Occur.MUST);
    if (validate) {
      if (result==null) result = rnd.clone();
      else result.and(rnd);
    }
    return result;
  }


  public int doConjunctions(IndexSearcher s, FixedBitSet[] sets, int iter, int maxClauses) throws IOException {
    int ret=0;

    for (int i=0; i<iter; i++) {
      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      FixedBitSet result=null;
      for (int j=0; j<nClauses; j++) {
        result = addClause(sets, bq, result);
      }

      CountingHitCollector hc = validate ? new MatchingHitCollector(result)
                                         : new CountingHitCollector();
      s.search(bq.build(), hc);
      ret += hc.getSum();

      if (validate) assertEquals(result.cardinality(), hc.getCount());
      // System.out.println(hc.getCount());
    }
    
    return ret;
  }

  public int doNestedConjunctions(IndexSearcher s,
                                  FixedBitSet[] sets,
                                  int iter,
                                  int maxOuterClauses,
                                  int maxClauses) throws IOException {
    int ret=0;
    long nMatches=0;

    for (int i=0; i<iter; i++) {
      int oClauses = random().nextInt(maxOuterClauses-1)+2;
      BooleanQuery.Builder oq = new BooleanQuery.Builder();
      FixedBitSet result=null;

      for (int o=0; o<oClauses; o++) {

      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      for (int j=0; j<nClauses; j++) {
        result = addClause(sets, bq,result);
      }

      oq.add(bq.build(), BooleanClause.Occur.MUST);
      } // outer

      CountingHitCollector hc = validate ? new MatchingHitCollector(result)
                                         : new CountingHitCollector();
      s.search(oq.build(), hc);
      nMatches += hc.getCount();
      ret += hc.getSum();
      if (validate) assertEquals(result.cardinality(), hc.getCount());
      // System.out.println(hc.getCount());
    }
    if (VERBOSE) System.out.println("Average number of matches="+(nMatches/iter));
    return ret;
  }

  public int doTermConjunctions(Term[] terms,
                                IndexSearcher s,
                                int termsInIndex,
                                int maxClauses,
                                int iter
  ) throws IOException {
    int ret=0;

    long nMatches=0;
    for (int i=0; i<iter; i++) {
      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      BitSet termflag = new BitSet(termsInIndex);
      for (int j=0; j<nClauses; j++) {
        int tnum;
        // don't pick same clause twice
        tnum = random().nextInt(termsInIndex);
        if (termflag.get(tnum)) tnum=termflag.nextClearBit(tnum);
        if (tnum<0 || tnum>=termsInIndex) tnum=termflag.nextClearBit(0);
        termflag.set(tnum);
        Query tq = new TermQuery(terms[tnum]);
        bq.add(tq, BooleanClause.Occur.MUST);
      }

      CountingHitCollector hc = new CountingHitCollector();
      s.search(bq.build(), hc);
      nMatches += hc.getCount();
      ret += hc.getSum();
    }
    if (VERBOSE) System.out.println("Average number of matches="+(nMatches/iter));

    return ret;
  }


  public int doNestedTermConjunctions(IndexSearcher s,
                                      Term[] terms,
                                      int termsInIndex,
                                      int maxOuterClauses,
                                      int maxClauses,
                                      int iter
  ) throws IOException {
    int ret=0;
    long nMatches=0;
    for (int i=0; i<iter; i++) {
      int oClauses = random().nextInt(maxOuterClauses-1)+2;
      BooleanQuery.Builder oq = new BooleanQuery.Builder();
      for (int o=0; o<oClauses; o++) {

      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      BitSet termflag = new BitSet(termsInIndex);
      for (int j=0; j<nClauses; j++) {
        int tnum;
        // don't pick same clause twice
        tnum = random().nextInt(termsInIndex);
        if (termflag.get(tnum)) tnum=termflag.nextClearBit(tnum);
        if (tnum<0 || tnum>=25) tnum=termflag.nextClearBit(0);
        termflag.set(tnum);
        Query tq = new TermQuery(terms[tnum]);
        bq.add(tq, BooleanClause.Occur.MUST);
      } // inner

      oq.add(bq.build(), BooleanClause.Occur.MUST);
      } // outer


      CountingHitCollector hc = new CountingHitCollector();
      s.search(oq.build(), hc);
      nMatches += hc.getCount();     
      ret += hc.getSum();
    }
    if (VERBOSE) System.out.println("Average number of matches="+(nMatches/iter));
    return ret;
  }


    public int doSloppyPhrase(IndexSearcher s,
                                int termsInIndex,
                                int maxClauses,
                                int iter
  ) throws IOException {
    int ret=0;

    for (int i=0; i<iter; i++) {
      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      PhraseQuery.Builder builder = new PhraseQuery.Builder();
      for (int j=0; j<nClauses; j++) {
        int tnum = random().nextInt(termsInIndex);
        builder.add(new Term("f", Character.toString((char)(tnum+'A'))));
      }
      // slop could be random too
      builder.setSlop(termsInIndex);
      PhraseQuery q = builder.build();

      CountingHitCollector hc = new CountingHitCollector();
      s.search(q, hc);
      ret += hc.getSum();
    }

    return ret;
  }

  public void testConjunctions() throws Exception {
    // test many small sets... the bugs will be found on boundary conditions
    try (Directory d = newDirectory()) {
      IndexWriter iw = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
      iw.addDocument(new Document());
      iw.close();

      try (DirectoryReader r = DirectoryReader.open(d)) {
        IndexSearcher s = newSearcher(r);
        s.setQueryCache(null);

        FixedBitSet[] sets = randBitSets(atLeast(1000), atLeast(10));

        doConjunctions(s, sets, atLeast(10000), atLeast(5));
        doNestedConjunctions(s, sets, atLeast(10000), atLeast(3), atLeast(3));
      }
    }
  }
}
