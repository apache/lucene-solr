package org.apache.lucene.index.codecs;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

// nocommit rename to TestSomethingTermsDict
// nocommit fix test to also test postings!

public class TestBlockTree extends LuceneTestCase {
  private Directory d;
  private IndexReader r;

  private final String FIELD = "field";

  private IndexReader makeIndex(int minTermsInBlock, int maxTermsInBlock, String... terms) throws Exception {
    // nocommit -- cutover to newDirectory
    d = new RAMDirectory();
    // nocommit -- switch to riw / other codecs:
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));

    CoreCodecProvider cp = new CoreCodecProvider();    
    cp.unregister(cp.lookup("Standard"));
    cp.register(new StandardCodec(minTermsInBlock, maxTermsInBlock));
    cp.setDefaultFieldCodec("Standard");
    iwc.setCodecProvider(cp);

    final IndexWriter w = new IndexWriter(d, iwc);
    w.setInfoStream(VERBOSE ? System.out : null);
    for(String term : terms) {
      Document doc = new Document();
      // nocommit -- switch to newField
      Field f = new Field(FIELD, term, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
      doc.add(f);
      w.addDocument(doc);
    }
    if (r != null) {
      close();
    }
    r = IndexReader.open(w, true);
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
    r = makeIndex(3, 6, "aa0", "aa1", "aa2", "aa3", "bb0", "bb1", "bb2", "bb3", "aa");

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
    // nocommit put 'aa' back!
    final String[] terms = new String[] {"aa0", "aa1", "aa2", "aa3", "aa4", "aa5", "aa6", "aa7", "aa8", "aa9", "aa", "xx"};
    r = makeIndex(3, 6, terms);
    //r = makeIndex(3, 6, "aa0", "aa1", "aa2", "aa3", "aa4", "aa5", "aa6", "aa7", "aa8", "aa9");

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

    final TermsEnum te = r.getSequentialSubReaders()[0].fields().terms(FIELD).iterator();
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

  // nocommit: test 0 terms case too!

  private String getRandomString() {
    // nocommit
    //return _TestUtil.randomSimpleString(random);
    return _TestUtil.randomRealisticUnicodeString(random);
  }

  public void testRandomTerms() throws Exception {
    // nocommit
    //final String[] terms = new String[_TestUtil.nextInt(random, 1, atLeast(1000))];
    final String[] terms = new String[_TestUtil.nextInt(random, 1, atLeast(500))];
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
      // nocommit -- use full unicode string
      final String t = getRandomString();
      if (!seen.contains(t) && (allowEmptyString || t.length() != 0)) {
        terms[seen.size()] = t;
        seen.add(t);
      }
    }
    final int minBlockSize = _TestUtil.nextInt(random, 1, 10);
    final int maxBlockSize = Math.max(2*(minBlockSize-1) + random.nextInt(60), 1);
    if (VERBOSE) {
      System.out.println("TEST: minBlockSize=" + minBlockSize + " maxBlockSize=" + maxBlockSize);
    }
    r = makeIndex(minBlockSize, maxBlockSize, terms);
    testRandomSeeks(r, terms);
    close();
  }

  // sugar
  private boolean seekExact(TermsEnum te, String term) throws IOException {
    return te.seekExact(new BytesRef(term), random.nextBoolean());
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

    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      final BytesRef t;
      int loc;
      if (random.nextInt(6) == 4) {
        // pick term that doens't exist:
        t = getNonExistTerm(validTerms);
        if (VERBOSE) {
          System.out.println("\nTEST: invalid term=" + t.utf8ToString());
        }
        loc = Arrays.binarySearch(validTerms, t);
      } else {
        // pick valid term
        loc = random.nextInt(validTerms.length);
        t = new BytesRef(validTerms[loc]);
        if (VERBOSE) {
          System.out.println("\nTEST: valid term=" + t.utf8ToString());
        }
      }

      // nocommit -- add some .termState() / seekExact(termState)

      // seekCeil or seekExact:
      final boolean doSeekExact = random.nextBoolean();
      if (doSeekExact) {
        if (VERBOSE) {
          System.out.println("  seekExact");
        }
        assertEquals(loc >= 0, te.seekExact(t, random.nextBoolean()));
      } else {
        if (VERBOSE) {
          System.out.println("  seekCeil");
        }

        final TermsEnum.SeekStatus result = te.seekCeil(t, random.nextBoolean());
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
      final int numNext = random.nextInt(validTerms.length);

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
          assertEquals(new BytesRef(validTerms[loc]), t2);
        }
      }
    }
  }
}
