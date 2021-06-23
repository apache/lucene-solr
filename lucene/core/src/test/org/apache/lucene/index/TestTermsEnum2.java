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
package org.apache.lucene.index;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.*;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

public class TestTermsEnum2 extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private SortedSet<BytesRef> terms; // the terms we put in the index
  private Automaton termsAutomaton; // automata of the same
  int numIterations;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    numIterations = atLeast(50);
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false))
            .setMaxBufferedDocs(TestUtil.nextInt(random(), 50, 1000)));
    Document doc = new Document();
    Field field = newStringField("field", "", Field.Store.YES);
    doc.add(field);
    terms = new TreeSet<>();
 
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      String s = TestUtil.randomUnicodeString(random());
      field.setStringValue(s);
      terms.add(new BytesRef(s));
      writer.addDocument(doc);
    }
    
    termsAutomaton = Automata.makeStringUnion(terms);
    
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  /** tests a pre-intersected automaton against the original */
  public void testFiniteVersusInfinite() throws Exception {

    for (int i = 0; i < numIterations; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      Automaton automaton = Operations.determinize(new RegExp(reg, RegExp.NONE).toAutomaton(),
        DEFAULT_DETERMINIZE_WORK_LIMIT);
      final List<BytesRef> matchedTerms = new ArrayList<>();
      for(BytesRef t : terms) {
        if (Operations.run(automaton, t.utf8ToString())) {
          matchedTerms.add(t);
        }
      }

      Automaton alternate = Automata.makeStringUnion(matchedTerms);
      //System.out.println("match " + matchedTerms.size() + " " + alternate.getNumberOfStates() + " states, sigma=" + alternate.getStartPoints().length);
      //AutomatonTestUtil.minimizeSimple(alternate);
      //System.out.println("minimize done");
      AutomatonQuery a1 = new AutomatonQuery(new Term("field", ""), automaton);
      AutomatonQuery a2 = new AutomatonQuery(new Term("field", ""), alternate, Integer.MAX_VALUE);

      ScoreDoc[] origHits = searcher.search(a1, 25).scoreDocs;
      ScoreDoc[] newHits = searcher.search(a2, 25).scoreDocs;
      CheckHits.checkEqual(a1, origHits, newHits);
    }
  }
  
  /** seeks to every term accepted by some automata */
  public void testSeeking() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      Automaton automaton = Operations.determinize(new RegExp(reg, RegExp.NONE).toAutomaton(),
        DEFAULT_DETERMINIZE_WORK_LIMIT);
      TermsEnum te = MultiTerms.getTerms(reader, "field").iterator();
      ArrayList<BytesRef> unsortedTerms = new ArrayList<>(terms);
      Collections.shuffle(unsortedTerms, random());

      for (BytesRef term : unsortedTerms) {
        if (Operations.run(automaton, term.utf8ToString())) {
          // term is accepted
          if (random().nextBoolean()) {
            // seek exact
            assertTrue(te.seekExact(term));
          } else {
            // seek ceil
            assertEquals(SeekStatus.FOUND, te.seekCeil(term));
            assertEquals(term, te.term());
          }
        }
      }
    }
  }
  
  /** mixes up seek and next for all terms */
  public void testSeekingAndNexting() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      TermsEnum te = MultiTerms.getTerms(reader, "field").iterator();

      for (BytesRef term : terms) {
        int c = random().nextInt(3);
        if (c == 0) {
          assertEquals(term, te.next());
        } else if (c == 1) {
          assertEquals(SeekStatus.FOUND, te.seekCeil(term));
          assertEquals(term, te.term());
        } else {
          assertTrue(te.seekExact(term));
        }
      }
    }
  }
  
  /** tests intersect: TODO start at a random term! */
  public void testIntersect() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      Automaton automaton = new RegExp(reg, RegExp.NONE).toAutomaton();
      CompiledAutomaton ca = new CompiledAutomaton(automaton, Operations.isFinite(automaton), false);
      TermsEnum te = MultiTerms.getTerms(reader, "field").intersect(ca, null);
      Automaton expected = Operations.determinize(Operations.intersection(termsAutomaton, automaton),
        DEFAULT_DETERMINIZE_WORK_LIMIT);
      TreeSet<BytesRef> found = new TreeSet<>();
      while (te.next() != null) {
        found.add(BytesRef.deepCopyOf(te.term()));
      }
      
      Automaton actual = Operations.determinize(Automata.makeStringUnion(found),
        DEFAULT_DETERMINIZE_WORK_LIMIT);
      assertTrue(Operations.sameLanguage(expected, actual));
    }
  }
}
