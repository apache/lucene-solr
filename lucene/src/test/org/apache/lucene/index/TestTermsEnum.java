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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

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

    final List<Term> terms = new ArrayList<Term>();
    TermEnum termEnum = r.terms(new Term("body"));
    do {
      Term term = termEnum.term();
      if (term == null || !"body".equals(term.field())) {
        break;
      }
      terms.add(term);
    } while (termEnum.next());

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
        termEnum.next();
        isEnd = termEnum.term() == null || !"body".equals(termEnum.term().field());
        upto++;
        if (isEnd) {
          if (VERBOSE) {
            System.out.println("  end");
          }
          assertEquals(upto, terms.size());
          upto = -1;
        } else {
          if (VERBOSE) {
            System.out.println("  got term=" + termEnum.term() + " expected=" + terms.get(upto));
          }
          assertTrue(upto < terms.size());
          assertEquals(terms.get(upto), termEnum.term());
        }
      } else {

        final Term target;
        final String exists;
        if (random.nextBoolean()) {
          // likely fake term
          if (random.nextBoolean()) {
            target = new Term("body",
                              _TestUtil.randomSimpleString(random));
          } else {
            target = new Term("body",
                              _TestUtil.randomRealisticUnicodeString(random));
          }
          exists = "likely not";
        } else {
          // real term 
          target = terms.get(random.nextInt(terms.size()));
          exists = "yes";
        }

        upto = Collections.binarySearch(terms, target);

        if (VERBOSE) {
          System.out.println("TEST: iter seekCeil target=" + target + " exists=" + exists);
        }
        termEnum = r.terms(target);
        final Term actualTerm = termEnum.term();

        if (VERBOSE) {
          System.out.println("  got term=" + actualTerm);
        }
          
        if (upto < 0) {
          upto = -(upto+1);
          if (upto >= terms.size()) {
            assertTrue(actualTerm == null || !"body".equals(actualTerm.field()));
            upto = -1;
          } else {
            assertTrue(actualTerm != null && "body".equals(actualTerm.field()));
            assertEquals(terms.get(upto), actualTerm);
          }
        } else {
          assertEquals(terms.get(upto), actualTerm);
        }
      }
    }

    r.close();
    d.close();
  }

  private Directory d;
  private IndexReader r;

  private final String FIELD = "field";

  private IndexReader makeIndex(String... terms) throws Exception {
    d = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));

    /*
    CoreCodecProvider cp = new CoreCodecProvider();    
    cp.unregister(cp.lookup("Standard"));
    cp.register(new StandardCodec(minTermsInBlock, maxTermsInBlock));
    cp.setDefaultFieldCodec("Standard");
    iwc.setCodecProvider(cp);
    */

    final RandomIndexWriter w = new RandomIndexWriter(random, d, iwc);
    w.w.setInfoStream(VERBOSE ? System.out : null);
    for(String term : terms) {
      Document doc = new Document();
      Field f = newField(FIELD, term, Field.Store.NO, Field.Index.NOT_ANALYZED);
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
    final Directory d = ((SegmentReader) r.getSequentialSubReaders()[0]).directory();
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

    final TermEnum te = r.terms(new Term(FIELD));
    while(te.next()) {
      //System.out.println("TEST: next term=" + te.term().utf8ToString());
    }

    testRandomSeeks(r, terms);
    close();
  }

  public void testZeroTerms() throws Exception {
    d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, d);
    w.w.setInfoStream(VERBOSE ? System.out : null);
    Document doc = new Document();
    doc.add(newField("field", "one two three", Field.Store.NO, Field.Index.ANALYZED));
    doc = new Document();
    doc.add(newField("field2", "one two three", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);
    w.commit();
    w.deleteDocuments(new Term("field", "one"));
    w.forceMerge(1);
    IndexReader r = w.getReader();
    w.close();
    assertEquals(1, r.numDocs());
    assertEquals(1, r.maxDoc());
    TermEnum terms = r.terms(new Term("field"));
    if (terms != null) {
      assertTrue(!terms.next() || !"field".equals(terms.term().field()));
    }
    r.close();
    d.close();
  }

  private String getRandomString() {
    //return _TestUtil.randomSimpleString(random);
    return _TestUtil.randomRealisticUnicodeString(random);
  }

  public void testRandomTerms() throws Exception {
    final String[] terms = new String[_TestUtil.nextInt(random, 1, atLeast(1000))];
    final Set<String> seen = new HashSet<String>();

    final boolean allowEmptyString = random.nextBoolean();

    if (random.nextInt(10) == 7 && terms.length > 2) {
      // Sometimes add a bunch of terms sharing a longish common prefix:
      final int numTermsSamePrefix = random.nextInt(terms.length/2);
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

  private void testRandomSeeks(IndexReader r, String... validTermStrings) throws IOException {
    final BytesRef[] validTerms = new BytesRef[validTermStrings.length];
    for(int termIDX=0;termIDX<validTermStrings.length;termIDX++) {
      validTerms[termIDX] = new BytesRef(validTermStrings[termIDX]);
    }
    Arrays.sort(validTerms, BytesRef.getUTF8SortedAsUTF16Comparator());
    if (VERBOSE) {
      System.out.println("TEST: " + validTerms.length + " terms:");
      for(int idx=0;idx<validTerms.length;idx++) {
        System.out.println("  " + idx + ": " + validTerms[idx]);
      }
    }

    final int END_LOC = -validTerms.length-1;
    
    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      final BytesRef t;
      int loc;
      if (random.nextInt(6) == 4) {
        // pick term that doens't exist:
        t = getNonExistTerm(validTerms);
        if (VERBOSE) {
          System.out.println("\nTEST: invalid term=" + t.utf8ToString());
        }
        loc = Arrays.binarySearch(validTerms, t, BytesRef.getUTF8SortedAsUTF16Comparator());
      } else {
        // pick valid term
        loc = random.nextInt(validTerms.length);
        t = new BytesRef(validTerms[loc]);
        if (VERBOSE) {
          System.out.println("\nTEST: valid term=" + t.utf8ToString());
        }
      }
      final Term targetTerm = new Term(FIELD, t.utf8ToString());

      if (VERBOSE) {
        System.out.println("  seek term=" + targetTerm);
      }

      final TermEnum te = r.terms(targetTerm);
      Term actualTerm = te.term();
      if (VERBOSE) {
        System.out.println("  got " + actualTerm);
      }

      if (loc >= 0) {
        // assertEquals(TermsEnum.SeekStatus.FOUND, result);
      } else if (loc == END_LOC) {
        assertTrue(actualTerm == null || !FIELD.equals(actualTerm.field()));
      } else {
        assert loc >= -validTerms.length;
        assertTrue(actualTerm != null && FIELD.equals(actualTerm.field()));
        //assertEquals(TermsEnum.SeekStatus.NOT_FOUND, result);
      }

      if (loc >= 0) {
        assertEquals(targetTerm, actualTerm);
      } else if (loc == END_LOC) {
        continue;
      } else {
        loc = -loc-1;
        assertEquals(new Term(FIELD, validTerms[loc].utf8ToString()), actualTerm);
      }

      // Do a bunch of next's after the seek
      final int numNext = random.nextInt(validTerms.length);

      if (VERBOSE) {
        System.out.println("\nTEST: numNext=" + numNext);
      }

      for(int nextCount=0;nextCount<numNext;nextCount++) {
        if (VERBOSE) {
          System.out.println("\nTEST: next loc=" + loc + " of " + validTerms.length);
        }
        boolean result = te.next();
        actualTerm = te.term();
        loc++;

        if (loc == validTerms.length) {
          if (VERBOSE) {
            System.out.println("  actual=null");
          }
          assertFalse(result);
          assertTrue(actualTerm == null || !FIELD.equals(actualTerm.field()));
          break;
        } else {
          if (VERBOSE) {
            System.out.println("  actual=" + new BytesRef(actualTerm.text()));
          }
          assertTrue(result);
          assertTrue(actualTerm != null && FIELD.equals(actualTerm.field()));
          assertEquals(validTerms[loc], new BytesRef(actualTerm.text()));
        }
      }
    }
  }
}
