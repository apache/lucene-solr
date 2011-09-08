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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util._TestUtil;

public class TestLongPostings extends LuceneTestCase {

  // Produces a realistic unicode random string that
  // survives MockAnalyzer unchanged:
  private String getRandomTerm(String other) throws IOException {
    Analyzer a = new MockAnalyzer(random);
    while(true) {
      String s = _TestUtil.randomRealisticUnicodeString(random);
      if (other != null && s.equals(other)) {
        continue;
      }
      final TokenStream ts = a.tokenStream("foo", new StringReader(s));
      final TermAttribute termAtt = ts.getAttribute(TermAttribute.class);
      int count = 0;
      ts.reset();
      while(ts.incrementToken()) {
        if (count == 0 && !termAtt.term().equals(s)) {
          break;
        }
        count++;
      }
      if (count == 1) {
        return s;
      }
    }
  }

  public void testLongPostings() throws Exception {
    // Don't use _TestUtil.getTempDir so that we own the
    // randomness (ie same seed will point to same dir):
    Directory dir = newFSDirectory(_TestUtil.getTempDir("longpostings" + "." + random.nextLong()));

    final int NUM_DOCS = atLeast(2000);

    if (VERBOSE) {
      System.out.println("TEST: NUM_DOCS=" + NUM_DOCS);
    }

    final String s1 = getRandomTerm(null);
    final String s2 = getRandomTerm(s1);

    if (VERBOSE) {
      System.out.println("\nTEST: s1=" + s1 + " s2=" + s2);
      /*
      for(int idx=0;idx<s1.length();idx++) {
        System.out.println("  s1 ch=0x" + Integer.toHexString(s1.charAt(idx)));
      }
      for(int idx=0;idx<s2.length();idx++) {
        System.out.println("  s2 ch=0x" + Integer.toHexString(s2.charAt(idx)));
      }
      */
    }

    final FixedBitSet isS1 = new FixedBitSet(NUM_DOCS);
    for(int idx=0;idx<NUM_DOCS;idx++) {
      if (random.nextBoolean()) {
        isS1.set(idx);
      }
    }

    final IndexReader r;
    if (true) { 
      final IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setMergePolicy(newLogMergePolicy());
      iwc.setRAMBufferSizeMB(16.0 + 16.0 * random.nextDouble());
      iwc.setMaxBufferedDocs(-1);
      final RandomIndexWriter riw = new RandomIndexWriter(random, dir, iwc);

      for(int idx=0;idx<NUM_DOCS;idx++) {
        final Document doc = new Document();
        String s = isS1.get(idx) ? s1 : s2;
        final Field f = newField("field", s, Field.Index.ANALYZED);
        final int count = _TestUtil.nextInt(random, 1, 4);
        for(int ct=0;ct<count;ct++) {
          doc.add(f);
        }
        riw.addDocument(doc);
      }

      r = riw.getReader();
      riw.close();
    } else {
      r = IndexReader.open(dir);
    }

    /*
    if (VERBOSE) {
      System.out.println("TEST: terms");
      TermEnum termEnum = r.terms();
      while(termEnum.next()) {
        System.out.println("  term=" + termEnum.term() + " len=" + termEnum.term().text().length());
        assertTrue(termEnum.docFreq() > 0);
        System.out.println("    s1?=" + (termEnum.term().text().equals(s1)) + " s1len=" + s1.length());
        System.out.println("    s2?=" + (termEnum.term().text().equals(s2)) + " s2len=" + s2.length());
        final String s = termEnum.term().text();
        for(int idx=0;idx<s.length();idx++) {
          System.out.println("      ch=0x" + Integer.toHexString(s.charAt(idx)));
        }
      }
    }
    */

    assertEquals(NUM_DOCS, r.numDocs());
    assertTrue(r.docFreq(new Term("field", s1)) > 0);
    assertTrue(r.docFreq(new Term("field", s2)) > 0);

    final byte[] payload = new byte[100];

    int num = atLeast(1000);
    for(int iter=0;iter<num;iter++) {

      final String term;
      final boolean doS1;
      if (random.nextBoolean()) {
        term = s1;
        doS1 = true;
      } else {
        term = s2;
        doS1 = false;
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " doS1=" + doS1);
      }
        
      final TermPositions postings = r.termPositions(new Term("field", term));

      int docID = -1;
      while(docID < Integer.MAX_VALUE) {
        final int what = random.nextInt(3);
        if (what == 0) {
          if (VERBOSE) {
            System.out.println("TEST: docID=" + docID + "; do next()");
          }
          // nextDoc
          int expected = docID+1;
          while(true) {
            if (expected == NUM_DOCS) {
              expected = Integer.MAX_VALUE;
              break;
            } else if (isS1.get(expected) == doS1) {
              break;
            } else {
              expected++;
            }
          }
          boolean result = postings.next();
          if (!result) {
            assertEquals(Integer.MAX_VALUE, expected);
            if (VERBOSE) {
              System.out.println("  end");
            }
            break;
          } else {
            docID = postings.doc();
            if (VERBOSE) {
              System.out.println("  got docID=" + docID);
            }
            assertEquals(expected, docID);

            if (random.nextInt(6) == 3) {
              final int freq = postings.freq();
              assertTrue(freq >=1 && freq <= 4);
              for(int pos=0;pos<freq;pos++) {
                assertEquals(pos, postings.nextPosition());
                if (random.nextBoolean() && postings.isPayloadAvailable()) {
                  postings.getPayload(payload, 0);
                }
              }
            }
          }
        } else {
          // advance
          final int targetDocID;
          if (docID == -1) {
            targetDocID = random.nextInt(NUM_DOCS+1);
          } else {
            targetDocID = docID + _TestUtil.nextInt(random, 1, NUM_DOCS - docID);
          }
          if (VERBOSE) {
            System.out.println("TEST: docID=" + docID + "; do skipTo(" + targetDocID + ")");
          }
          int expected = targetDocID;
          while(true) {
            if (expected == NUM_DOCS) {
              expected = Integer.MAX_VALUE;
              break;
            } else if (isS1.get(expected) == doS1) {
              break;
            } else {
              expected++;
            }
          }
          
          final boolean result = postings.skipTo(targetDocID);
          if (!result) {
            assertEquals(Integer.MAX_VALUE, expected);
            if (VERBOSE) {
              System.out.println("  end");
            }
            break;
          } else {
            docID = postings.doc();
            if (VERBOSE) {
              System.out.println("  got docID=" + docID);
            }
            assertEquals(expected, docID);
          
            if (random.nextInt(6) == 3) {
              final int freq = postings.freq();
              assertTrue(freq >=1 && freq <= 4);
              for(int pos=0;pos<freq;pos++) {
                assertEquals(pos, postings.nextPosition());
                if (random.nextBoolean() && postings.isPayloadAvailable()) {
                  postings.getPayload(payload, 0);
                }
              }
            }
          }
        }
      }
    }
    r.close();
    dir.close();
  }
  
  // a weaker form of testLongPostings, that doesnt check positions
  public void testLongPostingsNoPositions() throws Exception {
    doTestLongPostingsNoPositions(IndexOptions.DOCS_ONLY);
    doTestLongPostingsNoPositions(IndexOptions.DOCS_AND_FREQS);
  }
  
  public void doTestLongPostingsNoPositions(IndexOptions options) throws Exception {
    // Don't use _TestUtil.getTempDir so that we own the
    // randomness (ie same seed will point to same dir):
    Directory dir = newFSDirectory(_TestUtil.getTempDir("longpostings" + "." + random.nextLong()));

    final int NUM_DOCS = atLeast(2000);

    if (VERBOSE) {
      System.out.println("TEST: NUM_DOCS=" + NUM_DOCS);
    }

    final String s1 = getRandomTerm(null);
    final String s2 = getRandomTerm(s1);

    if (VERBOSE) {
      System.out.println("\nTEST: s1=" + s1 + " s2=" + s2);
      /*
      for(int idx=0;idx<s1.length();idx++) {
        System.out.println("  s1 ch=0x" + Integer.toHexString(s1.charAt(idx)));
      }
      for(int idx=0;idx<s2.length();idx++) {
        System.out.println("  s2 ch=0x" + Integer.toHexString(s2.charAt(idx)));
      }
      */
    }

    final FixedBitSet isS1 = new FixedBitSet(NUM_DOCS);
    for(int idx=0;idx<NUM_DOCS;idx++) {
      if (random.nextBoolean()) {
        isS1.set(idx);
      }
    }

    final IndexReader r;
    if (true) { 
      final IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setMergePolicy(newLogMergePolicy());
      iwc.setRAMBufferSizeMB(16.0 + 16.0 * random.nextDouble());
      iwc.setMaxBufferedDocs(-1);
      final RandomIndexWriter riw = new RandomIndexWriter(random, dir, iwc);

      for(int idx=0;idx<NUM_DOCS;idx++) {
        final Document doc = new Document();
        String s = isS1.get(idx) ? s1 : s2;
        final Field f = newField("field", s, Field.Index.ANALYZED);
        f.setIndexOptions(options);
        final int count = _TestUtil.nextInt(random, 1, 4);
        for(int ct=0;ct<count;ct++) {
          doc.add(f);
        }
        riw.addDocument(doc);
      }

      r = riw.getReader();
      riw.close();
    } else {
      r = IndexReader.open(dir);
    }

    /*
    if (VERBOSE) {
      System.out.println("TEST: terms");
      TermEnum termEnum = r.terms();
      while(termEnum.next()) {
        System.out.println("  term=" + termEnum.term() + " len=" + termEnum.term().text().length());
        assertTrue(termEnum.docFreq() > 0);
        System.out.println("    s1?=" + (termEnum.term().text().equals(s1)) + " s1len=" + s1.length());
        System.out.println("    s2?=" + (termEnum.term().text().equals(s2)) + " s2len=" + s2.length());
        final String s = termEnum.term().text();
        for(int idx=0;idx<s.length();idx++) {
          System.out.println("      ch=0x" + Integer.toHexString(s.charAt(idx)));
        }
      }
    }
    */

    assertEquals(NUM_DOCS, r.numDocs());
    assertTrue(r.docFreq(new Term("field", s1)) > 0);
    assertTrue(r.docFreq(new Term("field", s2)) > 0);

    final byte[] payload = new byte[100];

    int num = atLeast(1000);
    for(int iter=0;iter<num;iter++) {

      final String term;
      final boolean doS1;
      if (random.nextBoolean()) {
        term = s1;
        doS1 = true;
      } else {
        term = s2;
        doS1 = false;
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " doS1=" + doS1);
      }
        
      final TermDocs postings = r.termDocs(new Term("field", term));

      int docID = -1;
      while(docID < Integer.MAX_VALUE) {
        final int what = random.nextInt(3);
        if (what == 0) {
          if (VERBOSE) {
            System.out.println("TEST: docID=" + docID + "; do next()");
          }
          // nextDoc
          int expected = docID+1;
          while(true) {
            if (expected == NUM_DOCS) {
              expected = Integer.MAX_VALUE;
              break;
            } else if (isS1.get(expected) == doS1) {
              break;
            } else {
              expected++;
            }
          }
          boolean result = postings.next();
          if (!result) {
            assertEquals(Integer.MAX_VALUE, expected);
            if (VERBOSE) {
              System.out.println("  end");
            }
            break;
          } else {
            docID = postings.doc();
            if (VERBOSE) {
              System.out.println("  got docID=" + docID);
            }
            assertEquals(expected, docID);

            if (random.nextInt(6) == 3) {
              final int freq = postings.freq();
              assertTrue(freq >=1 && freq <= 4);
            }
          }
        } else {
          // advance
          final int targetDocID;
          if (docID == -1) {
            targetDocID = random.nextInt(NUM_DOCS+1);
          } else {
            targetDocID = docID + _TestUtil.nextInt(random, 1, NUM_DOCS - docID);
          }
          if (VERBOSE) {
            System.out.println("TEST: docID=" + docID + "; do skipTo(" + targetDocID + ")");
          }
          int expected = targetDocID;
          while(true) {
            if (expected == NUM_DOCS) {
              expected = Integer.MAX_VALUE;
              break;
            } else if (isS1.get(expected) == doS1) {
              break;
            } else {
              expected++;
            }
          }
          
          final boolean result = postings.skipTo(targetDocID);
          if (!result) {
            assertEquals(Integer.MAX_VALUE, expected);
            if (VERBOSE) {
              System.out.println("  end");
            }
            break;
          } else {
            docID = postings.doc();
            if (VERBOSE) {
              System.out.println("  got docID=" + docID);
            }
            assertEquals(expected, docID);
          
            if (random.nextInt(6) == 3) {
              final int freq = postings.freq();
              assertTrue(freq >=1 && freq <= 4);
            }
          }
        }
      }
    }
    r.close();
    dir.close();
  }
}
