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
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SingleTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Operations;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

public class TestAutomatonQuery extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;

  private final String FN = "field";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    Field titleField = newTextField("title", "some title", Field.Store.NO);
    Field field = newTextField(FN, "this is document one 2345", Field.Store.NO);
    Field footerField = newTextField("footer", "a footer", Field.Store.NO);
    doc.add(titleField);
    doc.add(field);
    doc.add(footerField);
    writer.addDocument(doc);
    field.setStringValue("some text from doc two a short piece 5678.91");
    writer.addDocument(doc);
    field.setStringValue("doc three has some different stuff"
        + " with numbers 1234 5678.9 and letter b");
    writer.addDocument(doc);
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  private Term newTerm(String value) {
    return new Term(FN, value);
  }
  
  private int automatonQueryNrHits(AutomatonQuery query) throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: run aq=" + query);
    }
    return searcher.search(query, 5).totalHits;
  }
  
  private void assertAutomatonHits(int expected, Automaton automaton)
      throws IOException {
    AutomatonQuery query = new AutomatonQuery(newTerm("bogus"), automaton);
    
    query.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
    assertEquals(expected, automatonQueryNrHits(query));
    
    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    assertEquals(expected, automatonQueryNrHits(query));
    
    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
    assertEquals(expected, automatonQueryNrHits(query));
  }
  
  /**
   * Test some very simple automata.
   */
  public void testAutomata() throws IOException {
    assertAutomatonHits(0, Automata.makeEmpty());
    assertAutomatonHits(0, Automata.makeEmptyString());
    assertAutomatonHits(2, Automata.makeAnyChar());
    assertAutomatonHits(3, Automata.makeAnyString());
    assertAutomatonHits(2, Automata.makeString("doc"));
    assertAutomatonHits(1, Automata.makeChar('a'));
    assertAutomatonHits(2, Automata.makeCharRange('a', 'b'));
    assertAutomatonHits(2, Automata.makeDecimalInterval(1233, 2346, 0));
    assertAutomatonHits(1, Automata.makeDecimalInterval(0, 2000, 0));
    assertAutomatonHits(2, Operations.union(Automata.makeChar('a'),
        Automata.makeChar('b')));
    assertAutomatonHits(0, Operations.intersection(Automata
        .makeChar('a'), Automata.makeChar('b')));
    assertAutomatonHits(1, Operations.minus(Automata.makeCharRange('a', 'b'), 
        Automata.makeChar('a'), DEFAULT_MAX_DETERMINIZED_STATES));
  }

  /**
   * Test that a nondeterministic automaton works correctly. (It should will be
   * determinized)
   */
  public void testNFA() throws IOException {
    // accept this or three, the union is an NFA (two transitions for 't' from
    // initial state)
    Automaton nfa = Operations.union(Automata.makeString("this"),
        Automata.makeString("three"));
    assertAutomatonHits(2, nfa);
  }
  
  public void testEquals() {
    AutomatonQuery a1 = new AutomatonQuery(newTerm("foobar"), Automata
        .makeString("foobar"));
    // reference to a1
    AutomatonQuery a2 = a1;
    // same as a1 (accepts the same language, same term)
    AutomatonQuery a3 = new AutomatonQuery(newTerm("foobar"),
                            Operations.concatenate(
                                 Automata.makeString("foo"),
                                 Automata.makeString("bar")));
    // different than a1 (same term, but different language)
    AutomatonQuery a4 = new AutomatonQuery(newTerm("foobar"),
                                           Automata.makeString("different"));
    // different than a1 (different term, same language)
    AutomatonQuery a5 = new AutomatonQuery(newTerm("blah"),
                                           Automata.makeString("foobar"));
    
    assertEquals(a1.hashCode(), a2.hashCode());
    assertEquals(a1, a2);
    
    assertEquals(a1.hashCode(), a3.hashCode());
    assertEquals(a1, a3);
  
    // different class
    AutomatonQuery w1 = new WildcardQuery(newTerm("foobar"));
    // different class
    AutomatonQuery w2 = new RegexpQuery(newTerm("foobar"));
    
    assertFalse(a1.equals(w1));
    assertFalse(a1.equals(w2));
    assertFalse(w1.equals(w2));
    assertFalse(a1.equals(a4));
    assertFalse(a1.equals(a5));
    assertFalse(a1.equals(null));
  }
  
  /**
   * Test that rewriting to a single term works as expected, preserves
   * MultiTermQuery semantics.
   */
  public void testRewriteSingleTerm() throws IOException {
    AutomatonQuery aq = new AutomatonQuery(newTerm("bogus"), Automata.makeString("piece"));
    Terms terms = MultiFields.getTerms(searcher.getIndexReader(), FN);
    assertTrue(aq.getTermsEnum(terms) instanceof SingleTermsEnum);
    assertEquals(1, automatonQueryNrHits(aq));
  }
  
  /**
   * Test that rewriting to a prefix query works as expected, preserves
   * MultiTermQuery semantics.
   */
  public void testRewritePrefix() throws IOException {
    Automaton pfx = Automata.makeString("do");
    Automaton prefixAutomaton = Operations.concatenate(pfx, Automata.makeAnyString());
    AutomatonQuery aq = new AutomatonQuery(newTerm("bogus"), prefixAutomaton);
    assertEquals(3, automatonQueryNrHits(aq));
  }
  
  /**
   * Test handling of the empty language
   */
  public void testEmptyOptimization() throws IOException {
    AutomatonQuery aq = new AutomatonQuery(newTerm("bogus"), Automata.makeEmpty());
    // not yet available: assertTrue(aq.getEnum(searcher.getIndexReader())
    // instanceof EmptyTermEnum);
    Terms terms = MultiFields.getTerms(searcher.getIndexReader(), FN);
    assertSame(TermsEnum.EMPTY, aq.getTermsEnum(terms));
    assertEquals(0, automatonQueryNrHits(aq));
  }
  
  public void testHashCodeWithThreads() throws Exception {
    final AutomatonQuery queries[] = new AutomatonQuery[1000];
    for (int i = 0; i < queries.length; i++) {
      queries[i] = new AutomatonQuery(new Term("bogus", "bogus"), AutomatonTestUtil.randomAutomaton(random()), Integer.MAX_VALUE);
    }
    final CountDownLatch startingGun = new CountDownLatch(1);
    int numThreads = TestUtil.nextInt(random(), 2, 5);
    Thread[] threads = new Thread[numThreads];
    for (int threadID = 0; threadID < numThreads; threadID++) {
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              for (int i = 0; i < queries.length; i++) {
                queries[i].hashCode();
              }
            } catch (Exception e) {
              Rethrow.rethrow(e);
            }
          }
        };
      threads[threadID] = thread;
      thread.start();
    }
    startingGun.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
  }

  public void testBiggishAutomaton() {
    List<BytesRef> terms = new ArrayList<>();
    while (terms.size() < 3000) {
      terms.add(new BytesRef(TestUtil.randomUnicodeString(random())));
    }
    Collections.sort(terms);
    new AutomatonQuery(new Term("foo", "bar"), Automata.makeStringUnion(terms), Integer.MAX_VALUE);
  }
}
