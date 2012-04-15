package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.SpecialOperations;

public class TestTermsEnum2 extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private SortedSet<BytesRef> terms; // the terms we put in the index
  private Automaton termsAutomaton; // automata of the same
  int numIterations;

  public void setUp() throws Exception {
    super.setUp();
    // we generate aweful regexps: good for testing.
    // but for preflex codec, the test can be very slow, so use less iterations.
    numIterations = Codec.getDefault().getName().equals("Lucene3x") ? 10 * RANDOM_MULTIPLIER : atLeast(50);
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random(), MockTokenizer.KEYWORD, false))
            .setMaxBufferedDocs(_TestUtil.nextInt(random(), 50, 1000)));
    Document doc = new Document();
    Field field = newField("field", "", StringField.TYPE_STORED);
    doc.add(field);
    terms = new TreeSet<BytesRef>();
 
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      String s = _TestUtil.randomUnicodeString(random());
      field.setStringValue(s);
      terms.add(new BytesRef(s));
      writer.addDocument(doc);
    }
    
    termsAutomaton = DaciukMihovAutomatonBuilder.build(terms);
    
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }
  
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  /** tests a pre-intersected automaton against the original */
  public void testFiniteVersusInfinite() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      Automaton automaton = new RegExp(reg, RegExp.NONE).toAutomaton();
      final List<BytesRef> matchedTerms = new ArrayList<BytesRef>();
      for(BytesRef t : terms) {
        if (BasicOperations.run(automaton, t.utf8ToString())) {
          matchedTerms.add(t);
        }
      }

      Automaton alternate = DaciukMihovAutomatonBuilder.build(matchedTerms);
      //System.out.println("match " + matchedTerms.size() + " " + alternate.getNumberOfStates() + " states, sigma=" + alternate.getStartPoints().length);
      //AutomatonTestUtil.minimizeSimple(alternate);
      //System.out.println("minmize done");
      AutomatonQuery a1 = new AutomatonQuery(new Term("field", ""), automaton);
      AutomatonQuery a2 = new AutomatonQuery(new Term("field", ""), alternate);
      CheckHits.checkEqual(a1, searcher.search(a1, 25).scoreDocs, searcher.search(a2, 25).scoreDocs);
    }
  }
  
  /** seeks to every term accepted by some automata */
  public void testSeeking() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      Automaton automaton = new RegExp(reg, RegExp.NONE).toAutomaton();
      TermsEnum te = MultiFields.getTerms(reader, "field").iterator(null);
      ArrayList<BytesRef> unsortedTerms = new ArrayList<BytesRef>(terms);
      Collections.shuffle(unsortedTerms, random());

      for (BytesRef term : unsortedTerms) {
        if (BasicOperations.run(automaton, term.utf8ToString())) {
          // term is accepted
          if (random().nextBoolean()) {
            // seek exact
            assertTrue(te.seekExact(term, random().nextBoolean()));
          } else {
            // seek ceil
            assertEquals(SeekStatus.FOUND, te.seekCeil(term, random().nextBoolean()));
            assertEquals(term, te.term());
          }
        }
      }
    }
  }
  
  /** mixes up seek and next for all terms */
  public void testSeekingAndNexting() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      TermsEnum te = MultiFields.getTerms(reader, "field").iterator(null);

      for (BytesRef term : terms) {
        int c = random().nextInt(3);
        if (c == 0) {
          assertEquals(term, te.next());
        } else if (c == 1) {
          assertEquals(SeekStatus.FOUND, te.seekCeil(term, random().nextBoolean()));
          assertEquals(term, te.term());
        } else {
          assertTrue(te.seekExact(term, random().nextBoolean()));
        }
      }
    }
  }
  
  /** tests intersect: TODO start at a random term! */
  public void testIntersect() throws Exception {
    for (int i = 0; i < numIterations; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      Automaton automaton = new RegExp(reg, RegExp.NONE).toAutomaton();
      CompiledAutomaton ca = new CompiledAutomaton(automaton, SpecialOperations.isFinite(automaton), false);
      TermsEnum te = MultiFields.getTerms(reader, "field").intersect(ca, null);
      Automaton expected = BasicOperations.intersection(termsAutomaton, automaton);
      TreeSet<BytesRef> found = new TreeSet<BytesRef>();
      while (te.next() != null) {
        found.add(BytesRef.deepCopyOf(te.term()));
      }
      
      Automaton actual = DaciukMihovAutomatonBuilder.build(found);     
      assertTrue(BasicOperations.sameLanguage(expected, actual));
    }
  }
}
