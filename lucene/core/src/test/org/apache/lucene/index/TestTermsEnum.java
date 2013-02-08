package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestTermsEnum extends LuceneTestCase {

  public void test() throws Exception {
    Random random = new Random(random().nextLong());
    final LineFileDocs docs = new LineFileDocs(random, defaultCodecSupportsDocValues());
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    final int numDocs = atLeast(10);
    for(int docCount=0;docCount<numDocs;docCount++) {
      w.addDocument(docs.nextDoc());
    }
    final IndexReader r = w.getReader();
    w.close();

    final List<BytesRef> terms = new ArrayList<BytesRef>();
    final TermsEnum termsEnum = MultiFields.getTerms(r, "body").iterator(null);
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
            target = new BytesRef(_TestUtil.randomSimpleString(random()));
          } else {
            target = new BytesRef(_TestUtil.randomRealisticUnicodeString(random()));
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
          final TermsEnum.SeekStatus status = termsEnum.seekCeil(target, random().nextBoolean());
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
    docs.close();
  }

  private void addDoc(RandomIndexWriter w, Collection<String> terms, Map<BytesRef,Integer> termToID, int id) throws IOException {
    Document doc = new Document();
    doc.add(new IntField("id", id, Field.Store.NO));
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
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final int numTerms = atLeast(300);
    //final int numTerms = 50;

    final Set<String> terms = new HashSet<String>();
    final Collection<String> pendingTerms = new ArrayList<String>();
    final Map<BytesRef,Integer> termToID = new HashMap<BytesRef,Integer>();
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
    final FieldCache.Ints docIDToID = FieldCache.DEFAULT.getInts(SlowCompositeReaderWrapper.wrap(r), "id", false);

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {

      // TODO: can we also test infinite As here...?

      // From the random terms, pick some ratio and compile an
      // automaton:
      final Set<String> acceptTerms = new HashSet<String>();
      final TreeSet<BytesRef> sortedAcceptTerms = new TreeSet<BytesRef>();
      final double keepPct = random().nextDouble();
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
          if (random().nextDouble() <= keepPct) {
            s2 = s;
          } else {
            s2 = getRandomString();
          }
          acceptTerms.add(s2);
          sortedAcceptTerms.add(new BytesRef(s2));
        }
        a = BasicAutomata.makeStringUnion(sortedAcceptTerms);
      }
      
      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: reduce the automaton");
        }
        a.reduce();
      }

      final CompiledAutomaton c = new CompiledAutomaton(a, true, false);

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
        final BytesRef startTerm = acceptTermsArray.length == 0 || random().nextBoolean() ? null : acceptTermsArray[random().nextInt(acceptTermsArray.length)];

        if (VERBOSE) {
          System.out.println("\nTEST: iter2=" + iter2 + " startTerm=" + (startTerm == null ? "<null>" : startTerm.utf8ToString()));

          if (startTerm != null) {
            int state = c.runAutomaton.getInitialState();
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

        DocsEnum docsEnum = null;
        while (loc < termsArray.length) {
          final BytesRef expected = termsArray[loc];
          final BytesRef actual = te.next();
          if (VERBOSE) {
            System.out.println("TEST:   next() expected=" + expected.utf8ToString() + " actual=" + (actual == null ? "null" : actual.utf8ToString()));
          }
          assertEquals(expected, actual);
          assertEquals(1, te.docFreq());
          docsEnum = _TestUtil.docs(random(), te, null, docsEnum, DocsEnum.FLAG_NONE);
          final int docID = docsEnum.nextDoc();
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));

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

    final TermsEnum te = MultiFields.getTerms(r, FIELD).iterator(null);
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
      assertNull(terms.iterator(null).next());
    }
    r.close();
    d.close();
  }

  private String getRandomString() {
    //return _TestUtil.randomSimpleString(random());
    return _TestUtil.randomRealisticUnicodeString(random());
  }

  public void testRandomTerms() throws Exception {
    final String[] terms = new String[_TestUtil.nextInt(random(), 1, atLeast(1000))];
    final Set<String> seen = new HashSet<String>();

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
    return te.seekExact(new BytesRef(term), random().nextBoolean());
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
    final TermsEnum te = MultiFields.getTerms(r, FIELD).iterator(null);

    final int END_LOC = -validTerms.length-1;
    
    final List<TermAndState> termStates = new ArrayList<TermAndState>();

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
        assertEquals(loc >= 0, te.seekExact(t, random().nextBoolean()));
      } else {
        if (VERBOSE) {
          System.out.println("  seekCeil");
        }

        final TermsEnum.SeekStatus result = te.seekCeil(t, random().nextBoolean());
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    AtomicReader sub = getOnlySegmentReader(r);
    Terms terms = sub.fields().terms("field");
    Automaton automaton = new RegExp(".*", RegExp.NONE).toAutomaton();    
    CompiledAutomaton ca = new CompiledAutomaton(automaton, false, false);    
    TermsEnum te = terms.intersect(ca, null);
    assertEquals("aaa", te.next().utf8ToString());
    assertEquals(0, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertEquals("bbb", te.next().utf8ToString());
    assertEquals(1, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertEquals("ccc", te.next().utf8ToString());
    assertEquals(2, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertNull(te.next());

    te = terms.intersect(ca, new BytesRef("abc"));
    assertEquals("bbb", te.next().utf8ToString());
    assertEquals(1, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertEquals("ccc", te.next().utf8ToString());
    assertEquals(2, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertNull(te.next());

    te = terms.intersect(ca, new BytesRef("aaa"));
    assertEquals("bbb", te.next().utf8ToString());
    assertEquals(1, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertEquals("ccc", te.next().utf8ToString());
    assertEquals(2, te.docs(null, null, DocsEnum.FLAG_NONE).nextDoc());
    assertNull(te.next());

    r.close();
    dir.close();
  }
}
