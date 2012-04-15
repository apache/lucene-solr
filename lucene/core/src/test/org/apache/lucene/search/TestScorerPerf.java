package org.apache.lucene.search;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdBitSet;
import org.apache.lucene.util.LuceneTestCase;

import java.util.BitSet;
import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;

/**
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

public class TestScorerPerf extends LuceneTestCase {
  boolean validate = true;  // set to false when doing performance testing

  BitSet[] sets;
  Term[] terms;
  IndexSearcher s;
  IndexReader r;
  Directory d;

  // TODO: this should be setUp()....
  public void createDummySearcher() throws Exception {
      // Create a dummy index with nothing in it.
    // This could possibly fail if Lucene starts checking for docid ranges...
    d = newDirectory();
    IndexWriter iw = new IndexWriter(d, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    iw.addDocument(new Document());
    iw.close();
    r = IndexReader.open(d);
    s = new IndexSearcher(r);
  }

  public void createRandomTerms(int nDocs, int nTerms, double power, Directory dir) throws Exception {
    int[] freq = new int[nTerms];
    terms = new Term[nTerms];
    for (int i=0; i<nTerms; i++) {
      int f = (nTerms+1)-i;  // make first terms less frequent
      freq[i] = (int)Math.ceil(Math.pow(f,power));
      terms[i] = new Term("f",Character.toString((char)('A'+i)));
    }

    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
    for (int i=0; i<nDocs; i++) {
      Document d = new Document();
      for (int j=0; j<nTerms; j++) {
        if (random().nextInt(freq[j]) == 0) {
          d.add(newField("f", terms[j].text(), StringField.TYPE_UNSTORED));
          //System.out.println(d);
        }
      }
      iw.addDocument(d);
    }
    iw.forceMerge(1);
    iw.close();
  }


  public BitSet randBitSet(int sz, int numBitsToSet) {
    BitSet set = new BitSet(sz);
    for (int i=0; i<numBitsToSet; i++) {
      set.set(random().nextInt(sz));
    }
    return set;
  }

  public BitSet[] randBitSets(int numSets, int setSize) {
    BitSet[] sets = new BitSet[numSets];
    for (int i=0; i<sets.length; i++) {
      sets[i] = randBitSet(setSize, random().nextInt(setSize));
    }
    return sets;
  }

  public static class CountingHitCollector extends Collector {
    int count=0;
    int sum=0;
    protected int docBase = 0;

    @Override
    public void setScorer(Scorer scorer) throws IOException {}
    
    @Override
    public void collect(int doc) {
      count++;
      sum += docBase + doc;  // use it to avoid any possibility of being eliminated by hotspot
    }

    public int getCount() { return count; }
    public int getSum() { return sum; }

    @Override
    public void setNextReader(AtomicReaderContext context) {
      docBase = context.docBase;
    }
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }


  public static class MatchingHitCollector extends CountingHitCollector {
    BitSet answer;
    int pos=-1;
    public MatchingHitCollector(BitSet answer) {
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


  BitSet addClause(BooleanQuery bq, BitSet result) {
    final BitSet rnd = sets[random().nextInt(sets.length)];
    Query q = new ConstantScoreQuery(new Filter() {
      @Override
      public DocIdSet getDocIdSet (AtomicReaderContext context, Bits acceptDocs) {
        assertNull("acceptDocs should be null, as we have an index without deletions", acceptDocs);
        return new DocIdBitSet(rnd);
      }
    });
    bq.add(q, BooleanClause.Occur.MUST);
    if (validate) {
      if (result==null) result = (BitSet)rnd.clone();
      else result.and(rnd);
    }
    return result;
  }


  public int doConjunctions(int iter, int maxClauses) throws IOException {
    int ret=0;

    for (int i=0; i<iter; i++) {
      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery bq = new BooleanQuery();
      BitSet result=null;
      for (int j=0; j<nClauses; j++) {
        result = addClause(bq,result);
      }

      CountingHitCollector hc = validate ? new MatchingHitCollector(result)
                                         : new CountingHitCollector();
      s.search(bq, hc);
      ret += hc.getSum();

      if (validate) assertEquals(result.cardinality(), hc.getCount());
      // System.out.println(hc.getCount());
    }
    
    return ret;
  }

  public int doNestedConjunctions(int iter, int maxOuterClauses, int maxClauses) throws IOException {
    int ret=0;
    long nMatches=0;

    for (int i=0; i<iter; i++) {
      int oClauses = random().nextInt(maxOuterClauses-1)+2;
      BooleanQuery oq = new BooleanQuery();
      BitSet result=null;

      for (int o=0; o<oClauses; o++) {

      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery bq = new BooleanQuery();
      for (int j=0; j<nClauses; j++) {
        result = addClause(bq,result);
      }

      oq.add(bq, BooleanClause.Occur.MUST);
      } // outer

      CountingHitCollector hc = validate ? new MatchingHitCollector(result)
                                         : new CountingHitCollector();
      s.search(oq, hc);
      nMatches += hc.getCount();
      ret += hc.getSum();
      if (validate) assertEquals(result.cardinality(), hc.getCount());
      // System.out.println(hc.getCount());
    }
    if (VERBOSE) System.out.println("Average number of matches="+(nMatches/iter));
    return ret;
  }

  
  public int doTermConjunctions(IndexSearcher s,
                                int termsInIndex,
                                int maxClauses,
                                int iter
  ) throws IOException {
    int ret=0;

    long nMatches=0;
    for (int i=0; i<iter; i++) {
      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery bq = new BooleanQuery();
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
      s.search(bq, hc);
      nMatches += hc.getCount();
      ret += hc.getSum();
    }
    if (VERBOSE) System.out.println("Average number of matches="+(nMatches/iter));

    return ret;
  }


  public int doNestedTermConjunctions(IndexSearcher s,
                                int termsInIndex,
                                int maxOuterClauses,
                                int maxClauses,
                                int iter
  ) throws IOException {
    int ret=0;
    long nMatches=0;
    for (int i=0; i<iter; i++) {
      int oClauses = random().nextInt(maxOuterClauses-1)+2;
      BooleanQuery oq = new BooleanQuery();
      for (int o=0; o<oClauses; o++) {

      int nClauses = random().nextInt(maxClauses-1)+2; // min 2 clauses
      BooleanQuery bq = new BooleanQuery();
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

      oq.add(bq, BooleanClause.Occur.MUST);
      } // outer


      CountingHitCollector hc = new CountingHitCollector();
      s.search(oq, hc);
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
      PhraseQuery q = new PhraseQuery();
      for (int j=0; j<nClauses; j++) {
        int tnum = random().nextInt(termsInIndex);
        q.add(new Term("f",Character.toString((char)(tnum+'A'))), j);
      }
      q.setSlop(termsInIndex);  // this could be random too

      CountingHitCollector hc = new CountingHitCollector();
      s.search(q, hc);
      ret += hc.getSum();
    }

    return ret;
  }


  public void testConjunctions() throws Exception {
    // test many small sets... the bugs will be found on boundary conditions
    createDummySearcher();
    validate=true;
    sets=randBitSets(atLeast(1000), atLeast(10));
    doConjunctions(atLeast(10000), atLeast(5));
    doNestedConjunctions(atLeast(10000), atLeast(3), atLeast(3));
    r.close();
    d.close();
  }

  /***
  int bigIter=10;

  public void testConjunctionPerf() throws Exception {
    r = newRandom();
    createDummySearcher();
    validate=false;
    sets=randBitSets(32,1000000);
    for (int i=0; i<bigIter; i++) {
      long start = System.currentTimeMillis();
      doConjunctions(500,6);
      long end = System.currentTimeMillis();
      if (VERBOSE) System.out.println("milliseconds="+(end-start));
    }
    s.close();
  }

  public void testNestedConjunctionPerf() throws Exception {
    r = newRandom();
    createDummySearcher();
    validate=false;
    sets=randBitSets(32,1000000);
    for (int i=0; i<bigIter; i++) {
      long start = System.currentTimeMillis();
      doNestedConjunctions(500,3,3);
      long end = System.currentTimeMillis();
      if (VERBOSE) System.out.println("milliseconds="+(end-start));
    }
    s.close();
  }


  public void testConjunctionTerms() throws Exception {
    r = newRandom();
    validate=false;
    RAMDirectory dir = new RAMDirectory();
    if (VERBOSE) System.out.println("Creating index");
    createRandomTerms(100000,25,.5, dir);
    s = new IndexSearcher(dir, true);
    if (VERBOSE) System.out.println("Starting performance test");
    for (int i=0; i<bigIter; i++) {
      long start = System.currentTimeMillis();
      doTermConjunctions(s,25,5,1000);
      long end = System.currentTimeMillis();
      if (VERBOSE) System.out.println("milliseconds="+(end-start));
    }
    s.close();
  }

  public void testNestedConjunctionTerms() throws Exception {
    r = newRandom();
    validate=false;    
    RAMDirectory dir = new RAMDirectory();
    if (VERBOSE) System.out.println("Creating index");
    createRandomTerms(100000,25,.2, dir);
    s = new IndexSearcher(dir, true);
    if (VERBOSE) System.out.println("Starting performance test");
    for (int i=0; i<bigIter; i++) {
      long start = System.currentTimeMillis();
      doNestedTermConjunctions(s,25,3,3,200);
      long end = System.currentTimeMillis();
      if (VERBOSE) System.out.println("milliseconds="+(end-start));
    }
    s.close();
  }


  public void testSloppyPhrasePerf() throws Exception {
    r = newRandom();
    validate=false;    
    RAMDirectory dir = new RAMDirectory();
    if (VERBOSE) System.out.println("Creating index");
    createRandomTerms(100000,25,2,dir);
    s = new IndexSearcher(dir, true);
    if (VERBOSE) System.out.println("Starting performance test");
    for (int i=0; i<bigIter; i++) {
      long start = System.currentTimeMillis();
      doSloppyPhrase(s,25,2,1000);
      long end = System.currentTimeMillis();
      if (VERBOSE) System.out.println("milliseconds="+(end-start));
    }
    s.close();
  }
   ***/


}
