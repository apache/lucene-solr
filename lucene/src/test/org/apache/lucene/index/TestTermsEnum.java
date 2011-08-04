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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;

public class TestTermsEnum extends LuceneTestCase {

  public void test() throws Exception {
    final LineFileDocs docs = new LineFileDocs(random);
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, d);
    final int numDocs = atLeast(10);
    for(int docCount=0;docCount<numDocs;docCount++) {
      w.addDocument(docs.nextDoc());
    }
    final IndexReader r = w.getReader();
    w.close();

    final List<BytesRef> terms = new ArrayList<BytesRef>();
    final TermsEnum termsEnum = MultiFields.getTerms(r, "body").iterator();
    BytesRef term;
    while((term = termsEnum.next()) != null) {
      terms.add(new BytesRef(term));
    }
    if (VERBOSE) {
      System.out.println("TEST: " + terms.size() + " terms");
    }

    int upto = -1;
    final int iters = atLeast(200);
    for(int iter=0;iter<iters;iter++) {
      final boolean isEnd;
      if (upto != -1 && random.nextBoolean()) {
        // next
        if (VERBOSE) {
          System.out.println("TEST: iter next");
        }
        isEnd = termsEnum.next() == null;
        upto++;
        if (isEnd) {
          if (VERBOSE) {
            System.out.println("  end");
          }
          assertEquals(upto, terms.size());
          upto = -1;
        } else {
          if (VERBOSE) {
            System.out.println("  got term=" + termsEnum.term().utf8ToString() + " expected=" + terms.get(upto).utf8ToString());
          }
          assertTrue(upto < terms.size());
          assertEquals(terms.get(upto), termsEnum.term());
        }
      } else {

        final BytesRef target;
        final String exists;
        if (random.nextBoolean()) {
          // likely fake term
          if (random.nextBoolean()) {
            target = new BytesRef(_TestUtil.randomSimpleString(random));
          } else {
            target = new BytesRef(_TestUtil.randomRealisticUnicodeString(random));
          }
          exists = "likely not";
        } else {
          // real term
          target = terms.get(random.nextInt(terms.size()));
          exists = "yes";
        }

        upto = Collections.binarySearch(terms, target);

        if (random.nextBoolean()) {
          if (VERBOSE) {
            System.out.println("TEST: iter seekCeil target=" + target.utf8ToString() + " exists=" + exists);
          }
          // seekCeil
          final TermsEnum.SeekStatus status = termsEnum.seekCeil(target, random.nextBoolean());
          if (VERBOSE) {
            System.out.println("  got " + status);
          }
          
          if (upto < 0) {
            upto = -(upto+1);
            if (upto >= terms.size()) {
              assertEquals(TermsEnum.SeekStatus.END, status);
              upto = -1;
            } else {
              assertEquals(TermsEnum.SeekStatus.NOT_FOUND, status);
              assertEquals(terms.get(upto), termsEnum.term());
            }
          } else {
            assertEquals(TermsEnum.SeekStatus.FOUND, status);
            assertEquals(terms.get(upto), termsEnum.term());
          }
        } else {
          if (VERBOSE) {
            System.out.println("TEST: iter seekExact target=" + target.utf8ToString() + " exists=" + exists);
          }
          // seekExact
          final boolean result = termsEnum.seekExact(target, false);
          if (VERBOSE) {
            System.out.println("  got " + result);
          }
          if (upto < 0) {
            assertFalse(result);
            upto = -1;
          } else {
            assertTrue(result);
            assertEquals(target, termsEnum.term());
          }
        }
      }
    }

    r.close();
    d.close();
  }

  private String randomString() {
    //return _TestUtil.randomSimpleString(random);
    return _TestUtil.randomRealisticUnicodeString(random);
  }

  private void addDoc(RandomIndexWriter w, Collection<String> terms, Map<BytesRef,Integer> termToID, int id) throws IOException {
    Document doc = new Document();
    doc.add(new NumericField("id").setIntValue(id));
    if (VERBOSE) {
      System.out.println("TEST: addDoc id:" + id + " terms=" + terms);
    }
    for (String s2 : terms) {
      doc.add(newField("f", s2, Field.Index.NOT_ANALYZED));
      termToID.put(new BytesRef(s2), id);
    }
    w.addDocument(doc);
    terms.clear();
  }

  private boolean accepts(CompiledAutomaton c, BytesRef b) {
    int state = c.runAutomaton.getInitialState();
    for(int idx=0;idx<b.length;idx++) {
      assertTrue(state != -1);
      state = c.runAutomaton.step(state, b.bytes[b.offset+idx] & 0xff);
    }
    return c.runAutomaton.isAccept(state);
  }

  // Tests Terms.intersect
  public void testIntersectRandom() throws IOException {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);
    
    final int numTerms = atLeast(1000);

    final Set<String> terms = new HashSet<String>();
    final Collection<String> pendingTerms = new ArrayList<String>();
    final Map<BytesRef,Integer> termToID = new HashMap<BytesRef,Integer>();
    int id = 0;
    while(terms.size() != numTerms) {
      final String s = randomString();
      if (!terms.contains(s)) {
        terms.add(s);
        pendingTerms.add(s);
        if (random.nextInt(20) == 7) {
          addDoc(w, pendingTerms, termToID, id++);
        }
      }
    }
    addDoc(w, pendingTerms, termToID, id++);

    final BytesRef[] termsArray = new BytesRef[terms.size()];
    final Set<BytesRef> termsSet = new HashSet<BytesRef>();
    {
      int upto = 0;
      for(String s : terms) {
        final BytesRef b = new BytesRef(s);
        termsArray[upto++] = b;
        termsSet.add(b);
      }
      Arrays.sort(termsArray);
    }

    if (VERBOSE) {
      System.out.println("\nTEST: indexed terms (unicode order):");
      for(BytesRef t : termsArray) {
        System.out.println("  " + t.utf8ToString() + " -> id:" + termToID.get(t));
      }
    }

    final IndexReader r = w.getReader();
    w.close();

    // NOTE: intentional insanity!!
    final int[] docIDToID = FieldCache.DEFAULT.getInts(r, "id");

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {

      // TODO: can we also test infinite As here...?

      // From the random terms, pick some ratio and compile an
      // automaton:
      final List<Automaton> as = new ArrayList<Automaton>();
      final Set<String> acceptTerms = new HashSet<String>();
      final TreeSet<BytesRef> sortedAcceptTerms = new TreeSet<BytesRef>();
      final double keepPct = random.nextDouble();
      Automaton a;
      if (iter == 0) {
        if (VERBOSE) {
          System.out.println("\nTEST: empty automaton");
        }
        a = BasicAutomata.makeEmpty();
      } else {
        if (VERBOSE) {
          System.out.println("\nTEST: keepPct=" + keepPct);
        }
        for (String s : terms) {
          final String s2;
          if (random.nextDouble() <= keepPct) {
            s2 = s;
          } else {
            s2 = randomString();
          }
          acceptTerms.add(s2);
          sortedAcceptTerms.add(new BytesRef(s2));
        }
        a = DaciukMihovAutomatonBuilder.build(sortedAcceptTerms);
      }
      final CompiledAutomaton c = new CompiledAutomaton(a, true);

      final BytesRef[] acceptTermsArray = new BytesRef[acceptTerms.size()];
      final Set<BytesRef> acceptTermsSet = new HashSet<BytesRef>();
      int upto = 0;
      for(String s : acceptTerms) {
        final BytesRef b = new BytesRef(s);
        acceptTermsArray[upto++] = b;
        acceptTermsSet.add(b);
        assertTrue(accepts(c, b));
      }
      Arrays.sort(acceptTermsArray);

      if (VERBOSE) {
        System.out.println("\nTEST: accept terms (unicode order):");
        for(BytesRef t : acceptTermsArray) {
          System.out.println("  " + t.utf8ToString() + (termsSet.contains(t) ? " (exists)" : ""));
        }
        System.out.println(a.toDot());
      }

      for(int iter2=0;iter2<100;iter2++) {
        final BytesRef startTerm = acceptTermsArray.length == 0 || random.nextBoolean() ? null : acceptTermsArray[random.nextInt(acceptTermsArray.length)];

        final TermsEnum te = MultiFields.getTerms(r, "f").intersect(c, startTerm);

        if (VERBOSE) {
          System.out.println("\nTEST: iter2=" + iter2 + " startTerm=" + (startTerm == null ? "<null>" : startTerm.utf8ToString()));
        }

        int loc;
        if (startTerm == null) {
          loc = 0;
        } else {
          loc = Arrays.binarySearch(termsArray, new BytesRef(startTerm));
          if (loc < 0) {
            loc = -(loc+1);
          } else {
            // startTerm exists in index
            loc++;
          }
        }
        while(loc < termsArray.length && !acceptTermsSet.contains(termsArray[loc])) {
          loc++;
        }

        DocsEnum docsEnum = null;
        while (loc < termsArray.length) {
          final BytesRef expected = termsArray[loc];
          final BytesRef actual = te.next();
          if (VERBOSE) {
            System.out.println("TEST:   next() expected=" + expected.utf8ToString() + " actual=" + actual.utf8ToString());
          }
          assertEquals(expected, actual);
          assertEquals(1, te.docFreq());
          docsEnum = te.docs(null, docsEnum);
          final int docID = docsEnum.nextDoc();
          assertTrue(docID != DocsEnum.NO_MORE_DOCS);
          assertEquals(docIDToID[docID], termToID.get(expected).intValue());
          do {
            loc++;
          } while (loc < termsArray.length && !acceptTermsSet.contains(termsArray[loc]));
        }

        assertNull(te.next());
      }
    }

    r.close();
    dir.close();
  }
}
