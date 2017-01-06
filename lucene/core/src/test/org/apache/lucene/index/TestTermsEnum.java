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


import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestTermsEnum extends LuceneTestCase {

  public void test() throws Exception {
    Random random = new Random(random().nextLong());
    final LineFileDocs docs = new LineFileDocs(random);
    final Directory d = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
    final RandomIndexWriter w = new RandomIndexWriter(random(), d, analyzer);
    final int numDocs = atLeast(10);
    for(int docCount=0;docCount<numDocs;docCount++) {
      w.addDocument(docs.nextDoc());
    }
    final IndexReader r = w.getReader();
    w.close();

    final List<BytesRef> terms = new ArrayList<>();
    final TermsEnum termsEnum = MultiFields.getTerms(r, "body").iterator();
    BytesRef term;
    while((term = termsEnum.next()) != null) {
      terms.add(BytesRef.deepCopyOf(term));
    }
    if (VERBOSE) {
      System.out.println("TEST: " + terms.size() + " terms");
    }

    int upto = -1;
    final int iters = atLeast(200);
    for(int iter=0;iter<iters;iter++) {
      final boolean isEnd;
      if (upto != -1 && random().nextBoolean()) {
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
        if (random().nextBoolean()) {
          // likely fake term
          if (random().nextBoolean()) {
            target = new BytesRef(TestUtil.randomSimpleString(random()));
          } else {
            target = new BytesRef(TestUtil.randomRealisticUnicodeString(random()));
          }
          exists = "likely not";
        } else {
          // real term
          target = terms.get(random().nextInt(terms.size()));
          exists = "yes";
        }

        upto = Collections.binarySearch(terms, target);

        if (random().nextBoolean()) {
          if (VERBOSE) {
            System.out.println("TEST: iter seekCeil target=" + target.utf8ToString() + " exists=" + exists);
          }
          // seekCeil
          final TermsEnum.SeekStatus status = termsEnum.seekCeil(target);
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
          final boolean result = termsEnum.seekExact(target);
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
    docs.close();
  }

  private void addDoc(RandomIndexWriter w, Collection<String> terms, Map<BytesRef,Integer> termToID, int id) throws IOException {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("id", id));
    if (VERBOSE) {
      System.out.println("TEST: addDoc id:" + id + " terms=" + terms);
    }
    for (String s2 : terms) {
      doc.add(newStringField("f", s2, Field.Store.NO));
      termToID.put(new BytesRef(s2), id);
    }
    w.addDocument(doc);
    terms.clear();
  }

  private boolean accepts(CompiledAutomaton c, BytesRef b) {
    int state = 0;
    for(int idx=0;idx<b.length;idx++) {
      assertTrue(state != -1);
      state = c.runAutomaton.step(state, b.bytes[b.offset+idx] & 0xff);
    }
    return c.runAutomaton.isAccept(state);
  }

  // Tests Terms.intersect
  public void testIntersectRandom() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final int numTerms = atLeast(300);
    //final int numTerms = 50;

    final Set<String> terms = new HashSet<>();
    final Collection<String> pendingTerms = new ArrayList<>();
    final Map<BytesRef,Integer> termToID = new HashMap<>();
    int id = 0;
    while(terms.size() != numTerms) {
      final String s = getRandomString();
      if (!terms.contains(s)) {
        terms.add(s);
        pendingTerms.add(s);
        if (random().nextInt(20) == 7) {
          addDoc(w, pendingTerms, termToID, id++);
        }
      }
    }
    addDoc(w, pendingTerms, termToID, id++);

    final BytesRef[] termsArray = new BytesRef[terms.size()];
    final Set<BytesRef> termsSet = new HashSet<>();
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

    final NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {

      // TODO: can we also test infinite As here...?

      // From the random terms, pick some ratio and compile an
      // automaton:
      final Set<String> acceptTerms = new HashSet<>();
      final TreeSet<BytesRef> sortedAcceptTerms = new TreeSet<>();
      final double keepPct = random().nextDouble();
      Automaton a;
      if (iter == 0) {
        if (VERBOSE) {
          System.out.println("\nTEST: empty automaton");
        }
        a = Automata.makeEmpty();
      } else {
        if (VERBOSE) {
          System.out.println("\nTEST: keepPct=" + keepPct);
        }
        for (String s : terms) {
          final String s2;
          if (random().nextDouble() <= keepPct) {
            s2 = s;
          } else {
            s2 = getRandomString();
          }
          acceptTerms.add(s2);
          sortedAcceptTerms.add(new BytesRef(s2));
        }
        a = Automata.makeStringUnion(sortedAcceptTerms);
      }
      
      final CompiledAutomaton c = new CompiledAutomaton(a, true, false, 1000000, false);

      final BytesRef[] acceptTermsArray = new BytesRef[acceptTerms.size()];
      final Set<BytesRef> acceptTermsSet = new HashSet<>();
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
        final BytesRef startTerm = acceptTermsArray.length == 0 || random().nextBoolean() ? null : acceptTermsArray[random().nextInt(acceptTermsArray.length)];

        if (VERBOSE) {
          System.out.println("\nTEST: iter2=" + iter2 + " startTerm=" + (startTerm == null ? "<null>" : startTerm.utf8ToString()));

          if (startTerm != null) {
            int state = 0;
            for(int idx=0;idx<startTerm.length;idx++) {
              final int label = startTerm.bytes[startTerm.offset+idx] & 0xff;
              System.out.println("  state=" + state + " label=" + label);
              state = c.runAutomaton.step(state, label);
              assertTrue(state != -1);
            }
            System.out.println("  state=" + state);
          }
        }

        final TermsEnum te = MultiFields.getTerms(r, "f").intersect(c, startTerm);

        int loc;
        if (startTerm == null) {
          loc = 0;
        } else {
          loc = Arrays.binarySearch(termsArray, BytesRef.deepCopyOf(startTerm));
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

        PostingsEnum postingsEnum = null;
        while (loc < termsArray.length) {
          final BytesRef expected = termsArray[loc];
          final BytesRef actual = te.next();
          if (VERBOSE) {
            System.out.println("TEST:   next() expected=" + expected.utf8ToString() + " actual=" + (actual == null ? "null" : actual.utf8ToString()));
          }
          assertEquals(expected, actual);
          assertEquals(1, te.docFreq());
          postingsEnum = TestUtil.docs(random(), te, postingsEnum, PostingsEnum.NONE);
          final int docID = postingsEnum.nextDoc();
          assertTrue(docID != DocIdSetIterator.NO_MORE_DOCS);
          assertEquals(docIDToID.get(docID), termToID.get(expected).intValue());
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

  private Directory d;
  private IndexReader r;

  private final String FIELD = "field";

  private IndexReader makeIndex(String... terms) throws Exception {
    d = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));

    /*
    iwc.setCodec(new StandardCodec(minTermsInBlock, maxTermsInBlock));
    */

    final RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc);
    for(String term : terms) {
      Document doc = new Document();
      Field f = newStringField(FIELD, term, Field.Store.NO);
      doc.add(f);
      w.addDocument(doc);
    }
    if (r != null) {
      close();
    }
    r = w.getReader();
    w.close();
    return r;
  }

  private void close() throws Exception {
    r.close();
    d.close();
  }

  private int docFreq(IndexReader r, String term) throws Exception {
    return r.docFreq(new Term(FIELD, term));
  }

  public void testEasy() throws Exception {
    // No floor arcs:
    r = makeIndex("aa0", "aa1", "aa2", "aa3", "bb0", "bb1", "bb2", "bb3", "aa");

    // First term in block:
    assertEquals(1, docFreq(r, "aa0"));

    // Scan forward to another term in same block
    assertEquals(1, docFreq(r, "aa2"));

    assertEquals(1, docFreq(r, "aa"));

    // Reset same block then scan forwards
    assertEquals(1, docFreq(r, "aa1"));

    // Not found, in same block
    assertEquals(0, docFreq(r, "aa5"));

    // Found, in same block
    assertEquals(1, docFreq(r, "aa2"));

    // Not found in index:
    assertEquals(0, docFreq(r, "b0"));

    // Found:
    assertEquals(1, docFreq(r, "aa2"));

    // Found, rewind:
    assertEquals(1, docFreq(r, "aa0"));


    // First term in block:
    assertEquals(1, docFreq(r, "bb0"));

    // Scan forward to another term in same block
    assertEquals(1, docFreq(r, "bb2"));

    // Reset same block then scan forwards
    assertEquals(1, docFreq(r, "bb1"));

    // Not found, in same block
    assertEquals(0, docFreq(r, "bb5"));

    // Found, in same block
    assertEquals(1, docFreq(r, "bb2"));

    // Not found in index:
    assertEquals(0, docFreq(r, "b0"));

    // Found:
    assertEquals(1, docFreq(r, "bb2"));

    // Found, rewind:
    assertEquals(1, docFreq(r, "bb0"));

    close();
  }

  // tests:
  //   - test same prefix has non-floor block and floor block (ie, has 2 long outputs on same term prefix)
  //   - term that's entirely in the index

  public void testFloorBlocks() throws Exception {
    final String[] terms = new String[] {"aa0", "aa1", "aa2", "aa3", "aa4", "aa5", "aa6", "aa7", "aa8", "aa9", "aa", "xx"};
    r = makeIndex(terms);
    //r = makeIndex("aa0", "aa1", "aa2", "aa3", "aa4", "aa5", "aa6", "aa7", "aa8", "aa9");

    // First term in first block:
    assertEquals(1, docFreq(r, "aa0"));
    assertEquals(1, docFreq(r, "aa4"));

    // No block
    assertEquals(0, docFreq(r, "bb0"));

    // Second block
    assertEquals(1, docFreq(r, "aa4"));

    // Backwards to prior floor block:
    assertEquals(1, docFreq(r, "aa0"));

    // Forwards to last floor block:
    assertEquals(1, docFreq(r, "aa9"));

    assertEquals(0, docFreq(r, "a"));
    assertEquals(1, docFreq(r, "aa"));
    assertEquals(0, docFreq(r, "a"));
    assertEquals(1, docFreq(r, "aa"));

    // Forwards to last floor block:
    assertEquals(1, docFreq(r, "xx"));
    assertEquals(1, docFreq(r, "aa1"));
    assertEquals(0, docFreq(r, "yy"));

    assertEquals(1, docFreq(r, "xx"));
    assertEquals(1, docFreq(r, "aa9"));

    assertEquals(1, docFreq(r, "xx"));
    assertEquals(1, docFreq(r, "aa4"));

    final TermsEnum te = MultiFields.getTerms(r, FIELD).iterator();
    while(te.next() != null) {
      //System.out.println("TEST: next term=" + te.term().utf8ToString());
    }

    assertTrue(seekExact(te, "aa1"));
    assertEquals("aa2", next(te));
    assertTrue(seekExact(te, "aa8"));
    assertEquals("aa9", next(te));
    assertEquals("xx", next(te));

    testRandomSeeks(r, terms);
    close();
  }

  public void testZeroTerms() throws Exception {
    d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newTextField("field", "one two three", Field.Store.NO));
    doc = new Document();
    doc.add(newTextField("field2", "one two three", Field.Store.NO));
    w.addDocument(doc);
    w.commit();
    w.deleteDocuments(new Term("field", "one"));
    w.forceMerge(1);
    IndexReader r = w.getReader();
    w.close();
    assertEquals(1, r.numDocs());
    assertEquals(1, r.maxDoc());
    Terms terms = MultiFields.getTerms(r, "field");
    if (terms != null) {
      assertNull(terms.iterator().next());
    }
    r.close();
    d.close();
  }

  private String getRandomString() {
    //return _TestUtil.randomSimpleString(random());
    return TestUtil.randomRealisticUnicodeString(random());
  }

  public void testRandomTerms() throws Exception {
    final String[] terms = new String[TestUtil.nextInt(random(), 1, atLeast(1000))];
    final Set<String> seen = new HashSet<>();

    final boolean allowEmptyString = random().nextBoolean();

    if (random().nextInt(10) == 7 && terms.length > 2) {
      // Sometimes add a bunch of terms sharing a longish common prefix:
      final int numTermsSamePrefix = random().nextInt(terms.length/2);
      if (numTermsSamePrefix > 0) {
        String prefix;
        while(true) {
          prefix = getRandomString();
          if (prefix.length() < 5) {
            continue;
          } else {
            break;
          }
        }
        while(seen.size() < numTermsSamePrefix) {
          final String t = prefix + getRandomString();
          if (!seen.contains(t)) {
            terms[seen.size()] = t;
            seen.add(t);
          }
        }
      }
    }

    while(seen.size() < terms.length) {
      final String t = getRandomString();
      if (!seen.contains(t) && (allowEmptyString || t.length() != 0)) {
        terms[seen.size()] = t;
        seen.add(t);
      }
    }
    r = makeIndex(terms);
    testRandomSeeks(r, terms);
    close();
  }

  // sugar
  private boolean seekExact(TermsEnum te, String term) throws IOException {
    return te.seekExact(new BytesRef(term));
  }

  // sugar
  private String next(TermsEnum te) throws IOException {
    final BytesRef br = te.next();
    if (br == null) {
      return null;
    } else {
      return br.utf8ToString();
    }
  }

  private BytesRef getNonExistTerm(BytesRef[] terms) {
    BytesRef t = null;
    while(true) {
      final String ts = getRandomString();
      t = new BytesRef(ts);
      if (Arrays.binarySearch(terms, t) < 0) {
        return t;
      }
    }
  }

  private static class TermAndState {
    public final BytesRef term;
    public final TermState state;

    public TermAndState(BytesRef term, TermState state) {
      this.term = term;
      this.state = state;
    }
  }

  private void testRandomSeeks(IndexReader r, String... validTermStrings) throws IOException {
    final BytesRef[] validTerms = new BytesRef[validTermStrings.length];
    for(int termIDX=0;termIDX<validTermStrings.length;termIDX++) {
      validTerms[termIDX] = new BytesRef(validTermStrings[termIDX]);
    }
    Arrays.sort(validTerms);
    if (VERBOSE) {
      System.out.println("TEST: " + validTerms.length + " terms:");
      for(BytesRef t : validTerms) {
        System.out.println("  " + t.utf8ToString() + " " + t);
      }
    }
    final TermsEnum te = MultiFields.getTerms(r, FIELD).iterator();

    final int END_LOC = -validTerms.length-1;
    
    final List<TermAndState> termStates = new ArrayList<>();

    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      final BytesRef t;
      int loc;
      final TermState termState;
      if (random().nextInt(6) == 4) {
        // pick term that doens't exist:
        t = getNonExistTerm(validTerms);
        termState = null;
        if (VERBOSE) {
          System.out.println("\nTEST: invalid term=" + t.utf8ToString());
        }
        loc = Arrays.binarySearch(validTerms, t);
      } else if (termStates.size() != 0 && random().nextInt(4) == 1) {
        final TermAndState ts = termStates.get(random().nextInt(termStates.size()));
        t = ts.term;
        loc = Arrays.binarySearch(validTerms, t);
        assertTrue(loc >= 0);
        termState = ts.state;
        if (VERBOSE) {
          System.out.println("\nTEST: valid termState term=" + t.utf8ToString());
        }
      } else {
        // pick valid term
        loc = random().nextInt(validTerms.length);
        t = BytesRef.deepCopyOf(validTerms[loc]);
        termState = null;
        if (VERBOSE) {
          System.out.println("\nTEST: valid term=" + t.utf8ToString());
        }
      }

      // seekCeil or seekExact:
      final boolean doSeekExact = random().nextBoolean();
      if (termState != null) {
        if (VERBOSE) {
          System.out.println("  seekExact termState");
        }
        te.seekExact(t, termState);
      } else if (doSeekExact) {
        if (VERBOSE) {
          System.out.println("  seekExact");
        }
        assertEquals(loc >= 0, te.seekExact(t));
      } else {
        if (VERBOSE) {
          System.out.println("  seekCeil");
        }

        final TermsEnum.SeekStatus result = te.seekCeil(t);
        if (VERBOSE) {
          System.out.println("  got " + result);
        }

        if (loc >= 0) {
          assertEquals(TermsEnum.SeekStatus.FOUND, result);
        } else if (loc == END_LOC) {
          assertEquals(TermsEnum.SeekStatus.END, result);
        } else {
          assert loc >= -validTerms.length;
          assertEquals(TermsEnum.SeekStatus.NOT_FOUND, result);
        }
      }

      if (loc >= 0) {
        assertEquals(t, te.term());
      } else if (doSeekExact) {
        // TermsEnum is unpositioned if seekExact returns false
        continue;
      } else if (loc == END_LOC) {
        continue;
      } else {
        loc = -loc-1;
        assertEquals(validTerms[loc], te.term());
      }

      // Do a bunch of next's after the seek
      final int numNext = random().nextInt(validTerms.length);

      for(int nextCount=0;nextCount<numNext;nextCount++) {
        if (VERBOSE) {
          System.out.println("\nTEST: next loc=" + loc + " of " + validTerms.length);
        }
        final BytesRef t2 = te.next();
        loc++;
        if (loc == validTerms.length) {
          assertNull(t2);
          break;
        } else {
          assertEquals(validTerms[loc], t2);
          if (random().nextInt(40) == 17 && termStates.size() < 100) {
            termStates.add(new TermAndState(validTerms[loc], te.termState()));
          }
        }
      }
    }
  }

  public void testIntersectBasic() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(new LogDocMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newTextField("field", "aaa", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newStringField("field", "bbb", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("field", "ccc", Field.Store.NO));
    w.addDocument(doc);

    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    w.close();
    LeafReader sub = getOnlyLeafReader(r);
    Terms terms = sub.fields().terms("field");
    Automaton automaton = new RegExp(".*", RegExp.NONE).toAutomaton();
    CompiledAutomaton ca = new CompiledAutomaton(automaton, false, false);    
    TermsEnum te = terms.intersect(ca, null);
    assertEquals("aaa", te.next().utf8ToString());
    assertEquals(0, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertEquals("bbb", te.next().utf8ToString());
    assertEquals(1, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertEquals("ccc", te.next().utf8ToString());
    assertEquals(2, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertNull(te.next());

    te = terms.intersect(ca, new BytesRef("abc"));
    assertEquals("bbb", te.next().utf8ToString());
    assertEquals(1, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertEquals("ccc", te.next().utf8ToString());
    assertEquals(2, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertNull(te.next());

    te = terms.intersect(ca, new BytesRef("aaa"));
    assertEquals("bbb", te.next().utf8ToString());
    assertEquals(1, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertEquals("ccc", te.next().utf8ToString());
    assertEquals(2, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertNull(te.next());

    r.close();
    dir.close();
  }
  public void testIntersectStartTerm() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(new LogDocMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("field", "abc", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newStringField("field", "abd", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newStringField("field", "acd", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newStringField("field", "bcd", Field.Store.NO));
    w.addDocument(doc);

    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    w.close();
    LeafReader sub = getOnlyLeafReader(r);
    Terms terms = sub.fields().terms("field");

    Automaton automaton = new RegExp(".*d", RegExp.NONE).toAutomaton();
    CompiledAutomaton ca = new CompiledAutomaton(automaton, false, false);    
    TermsEnum te;
    
    // should seek to startTerm
    te = terms.intersect(ca, new BytesRef("aad"));
    assertEquals("abd", te.next().utf8ToString());
    assertEquals(1, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertEquals("acd", te.next().utf8ToString());
    assertEquals(2, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertEquals("bcd", te.next().utf8ToString());
    assertEquals(3, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertNull(te.next());

    // should fail to find ceil label on second arc, rewind 
    te = terms.intersect(ca, new BytesRef("add"));
    assertEquals("bcd", te.next().utf8ToString());
    assertEquals(3, te.postings(null, PostingsEnum.NONE).nextDoc());
    assertNull(te.next());

    // should reach end
    te = terms.intersect(ca, new BytesRef("bcd"));
    assertNull(te.next());
    te = terms.intersect(ca, new BytesRef("ddd"));
    assertNull(te.next());

    r.close();
    dir.close();
  }

  public void testIntersectEmptyString() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(new LogDocMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("field", "", Field.Store.NO));
    doc.add(newStringField("field", "abc", Field.Store.NO));
    w.addDocument(doc);

    doc = new Document();
    // add empty string to both documents, so that singletonDocID == -1.
    // For a FST-based term dict, we'll expect to see the first arc is 
    // flaged with HAS_FINAL_OUTPUT
    doc.add(newStringField("field", "abc", Field.Store.NO));
    doc.add(newStringField("field", "", Field.Store.NO));
    w.addDocument(doc);

    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    w.close();
    LeafReader sub = getOnlyLeafReader(r);
    Terms terms = sub.fields().terms("field");

    Automaton automaton = new RegExp(".*", RegExp.NONE).toAutomaton();  // accept ALL
    CompiledAutomaton ca = new CompiledAutomaton(automaton, false, false);    

    TermsEnum te = terms.intersect(ca, null);
    PostingsEnum de;

    assertEquals("", te.next().utf8ToString());
    de = te.postings(null, PostingsEnum.NONE);
    assertEquals(0, de.nextDoc());
    assertEquals(1, de.nextDoc());

    assertEquals("abc", te.next().utf8ToString());
    de = te.postings(null, PostingsEnum.NONE);
    assertEquals(0, de.nextDoc());
    assertEquals(1, de.nextDoc());

    assertNull(te.next());

    // pass empty string
    te = terms.intersect(ca, new BytesRef(""));

    assertEquals("abc", te.next().utf8ToString());
    de = te.postings(null, PostingsEnum.NONE);
    assertEquals(0, de.nextDoc());
    assertEquals(1, de.nextDoc());

    assertNull(te.next());

    r.close();
    dir.close();
  }

  // LUCENE-5667
  public void testCommonPrefixTerms() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Set<String> terms = new HashSet<String>();
    //String prefix = TestUtil.randomSimpleString(random(), 1, 20);
    String prefix = TestUtil.randomRealisticUnicodeString(random(), 1, 20);
    int numTerms = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: " + numTerms + " terms; prefix=" + prefix);
    }
    while (terms.size() < numTerms) {
      //terms.add(prefix + TestUtil.randomSimpleString(random(), 1, 20));
      terms.add(prefix + TestUtil.randomRealisticUnicodeString(random(), 1, 20));
    }
    for(String term : terms) {
      Document doc = new Document();
      doc.add(newStringField("id", term, Field.Store.YES));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();
    if (VERBOSE) {
      System.out.println("\nTEST: reader=" + r);
    }

    TermsEnum termsEnum = MultiFields.getTerms(r, "id").iterator();
    PostingsEnum postingsEnum = null;
    PerThreadPKLookup pkLookup = new PerThreadPKLookup(r, "id");

    int iters = atLeast(numTerms*3);
    List<String> termsList = new ArrayList<>(terms);
    for(int iter=0;iter<iters;iter++) {
      String term;
      boolean shouldExist;
      if (random().nextBoolean()) {
        term = termsList.get(random().nextInt(terms.size()));
        shouldExist = true;
      } else {
        term = prefix + TestUtil.randomSimpleString(random(), 1, 20);
        shouldExist = terms.contains(term);
      }

      if (VERBOSE) {
        System.out.println("\nTEST: try term=" + term);
        System.out.println("  shouldExist?=" + shouldExist);
      }

      BytesRef termBytesRef = new BytesRef(term);

      boolean actualResult = termsEnum.seekExact(termBytesRef);
      assertEquals(shouldExist, actualResult);
      if (shouldExist) {
        postingsEnum = termsEnum.postings(postingsEnum, 0);
        int docID = postingsEnum.nextDoc();
        assertTrue(docID != PostingsEnum.NO_MORE_DOCS);
        assertEquals(docID, pkLookup.lookup(termBytesRef));
        Document doc = r.document(docID);
        assertEquals(term, doc.get("id"));

        if (random().nextInt(7) == 1) {
          termsEnum.next();
        }
      } else {
        assertEquals(-1, pkLookup.lookup(termBytesRef));
      }

      if (random().nextInt(7) == 1) {
        TermsEnum.SeekStatus status = termsEnum.seekCeil(termBytesRef);
        if (shouldExist) {
          assertEquals(TermsEnum.SeekStatus.FOUND, status);
        } else {
          assertNotSame(TermsEnum.SeekStatus.FOUND, status);
        }
      }
    }

    r.close();
    w.close();
    d.close();
  }

  // Stresses out many-terms-in-root-block case:
  @Slow
  public void testVaryingTermsPerSegment() throws Exception {
    Directory dir = newDirectory();
    Set<BytesRef> terms = new HashSet<BytesRef>();
    int MAX_TERMS = atLeast(1000);
    while (terms.size() < MAX_TERMS) {
      terms.add(new BytesRef(TestUtil.randomSimpleString(random(), 1, 40)));
    }
    List<BytesRef> termsList = new ArrayList<>(terms);
    StringBuilder sb = new StringBuilder();
    for(int termCount=0;termCount<MAX_TERMS;termCount++) {
      if (VERBOSE) {
        System.out.println("\nTEST: termCount=" + termCount + " add term=" + termsList.get(termCount).utf8ToString());
      }
      sb.append(' ');
      sb.append(termsList.get(termCount).utf8ToString());
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
      Document doc = new Document();
      doc.add(newTextField("field", sb.toString(), Field.Store.NO));
      w.addDocument(doc);
      IndexReader r = w.getReader();
      assertEquals(1, r.leaves().size());
      TermsEnum te = r.leaves().get(0).reader().fields().terms("field").iterator();
      for(int i=0;i<=termCount;i++) {
        assertTrue("term '" + termsList.get(i).utf8ToString() + "' should exist but doesn't", te.seekExact(termsList.get(i)));
      }
      for(int i=termCount+1;i<termsList.size();i++) {
        assertFalse("term '" + termsList.get(i) + "' shouldn't exist but does", te.seekExact(termsList.get(i)));
      }
      r.close();
      w.close();
    }
    dir.close();
  }

  // LUCENE-7576
  public void testIntersectRegexp() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newStringField("field", "foobar", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    Fields fields = MultiFields.getFields(r);
    CompiledAutomaton automaton = new CompiledAutomaton(new RegExp("do_not_match_anything").toAutomaton());
    Terms terms = fields.terms("field");
    String message = expectThrows(IllegalArgumentException.class, () -> {terms.intersect(automaton, null);}).getMessage();
    assertEquals("please use CompiledAutomaton.getTermsEnum instead", message);
    r.close();
    w.close();
    d.close();
  }

  // LUCENE-7576
  public void testInvalidAutomatonTermsEnum() throws Exception {
    expectThrows(IllegalArgumentException.class,
                 () -> {
                   new AutomatonTermsEnum(TermsEnum.EMPTY, new CompiledAutomaton(Automata.makeString("foo")));
                 });
  }
}
