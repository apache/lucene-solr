package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.store.*;
import org.apache.lucene.codecs.lucene3x.PreFlexRWCodec;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;

import java.util.*;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestSurrogates extends LuceneTestCase {
  /** we will manually instantiate preflex-rw here */
  @BeforeClass
  public static void beforeClass() {
    LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE = true;
  }

  private static String makeDifficultRandomUnicodeString(Random r) {
    final int end = r.nextInt(20);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      int t = r.nextInt(5);

      if (0 == t && i < end - 1) {
        // hi
        buffer[i++] = (char) (0xd800 + r.nextInt(2));
        // lo
        buffer[i] = (char) (0xdc00 + r.nextInt(2));
      } else if (t <= 3) {
        buffer[i] = (char) ('a' + r.nextInt(2));
      }  else if (4 == t) {
        buffer[i] = (char) (0xe000 + r.nextInt(2));
      }
    }

    return new String(buffer, 0, end);
  }

  private String toHexString(Term t) {
    return t.field() + ":" + UnicodeUtil.toHexString(t.text());
  }

  private String getRandomString(Random r) {
    String s;
    if (r.nextInt(5) == 1) {
      if (r.nextInt(3) == 1) {
        s = makeDifficultRandomUnicodeString(r);
      } else {
        s = _TestUtil.randomUnicodeString(r);
      }
    } else {
      s = _TestUtil.randomRealisticUnicodeString(r);
    }
    return s;
  }

  private static class SortTermAsUTF16Comparator implements Comparator<Term> {
    private static final Comparator<BytesRef> legacyComparator = 
      BytesRef.getUTF8SortedAsUTF16Comparator();

    public int compare(Term term1, Term term2) {
      if (term1.field().equals(term2.field())) {
        return legacyComparator.compare(term1.bytes(), term2.bytes());
      } else {
        return term1.field().compareTo(term2.field());
      }
    }
  }

  private static final SortTermAsUTF16Comparator termAsUTF16Comparator = new SortTermAsUTF16Comparator();

  // single straight enum
  private void doTestStraightEnum(List<Term> fieldTerms, IndexReader reader, int uniqueTermCount) throws IOException {

    if (VERBOSE) {
      System.out.println("\nTEST: top now enum reader=" + reader);
    }
    FieldsEnum fieldsEnum = MultiFields.getFields(reader).iterator();

    {
      // Test straight enum:
      String field;
      int termCount = 0;
      while((field = fieldsEnum.next()) != null) {
        Terms terms = fieldsEnum.terms();
        assertNotNull(terms);
        TermsEnum termsEnum = terms.iterator(null);
        BytesRef text;
        BytesRef lastText = null;
        while((text = termsEnum.next()) != null) {
          Term exp = fieldTerms.get(termCount);
          if (VERBOSE) {
            System.out.println("  got term=" + field + ":" + UnicodeUtil.toHexString(text.utf8ToString()));
            System.out.println("       exp=" + exp.field() + ":" + UnicodeUtil.toHexString(exp.text().toString()));
            System.out.println();
          }
          if (lastText == null) {
            lastText = BytesRef.deepCopyOf(text);
          } else {
            assertTrue(lastText.compareTo(text) < 0);
            lastText.copyBytes(text);
          }
          assertEquals(exp.field(), field);
          assertEquals(exp.bytes(), text);
          termCount++;
        }
        if (VERBOSE) {
          System.out.println("  no more terms for field=" + field);
        }
      }
      assertEquals(uniqueTermCount, termCount);
    }
  }

  // randomly seeks to term that we know exists, then next's
  // from there
  private void doTestSeekExists(Random r, List<Term> fieldTerms, IndexReader reader) throws IOException {

    final Map<String,TermsEnum> tes = new HashMap<String,TermsEnum>();

    // Test random seek to existing term, then enum:
    if (VERBOSE) {
      System.out.println("\nTEST: top now seek");
    }

    int num = atLeast(100);
    for (int iter = 0; iter < num; iter++) {

      // pick random field+term
      int spot = r.nextInt(fieldTerms.size());
      Term term = fieldTerms.get(spot);
      String field = term.field();

      if (VERBOSE) {
        System.out.println("TEST: exist seek field=" + field + " term=" + UnicodeUtil.toHexString(term.text()));
      }

      // seek to it
      TermsEnum te = tes.get(field);
      if (te == null) {
        te = MultiFields.getTerms(reader, field).iterator(null);
        tes.put(field, te);
      }

      if (VERBOSE) {
        System.out.println("  done get enum");
      }

      // seek should find the term
      assertEquals(TermsEnum.SeekStatus.FOUND,
                   te.seekCeil(term.bytes()));
      
      // now .next() this many times:
      int ct = _TestUtil.nextInt(r, 5, 100);
      for(int i=0;i<ct;i++) {
        if (VERBOSE) {
          System.out.println("TEST: now next()");
        }
        if (1+spot+i >= fieldTerms.size()) {
          break;
        }
        term = fieldTerms.get(1+spot+i);
        if (!term.field().equals(field)) {
          assertNull(te.next());
          break;
        } else {
          BytesRef t = te.next();

          if (VERBOSE) {
            System.out.println("  got term=" + (t == null ? null : UnicodeUtil.toHexString(t.utf8ToString())));
            System.out.println("       exp=" + UnicodeUtil.toHexString(term.text().toString()));
          }

          assertEquals(term.bytes(), t);
        }
      }
    }
  }

  private void doTestSeekDoesNotExist(Random r, int numField, List<Term> fieldTerms, Term[] fieldTermsArray, IndexReader reader) throws IOException {

    final Map<String,TermsEnum> tes = new HashMap<String,TermsEnum>();

    if (VERBOSE) {
      System.out.println("TEST: top random seeks");
    }

    {
      int num = atLeast(100);
      for (int iter = 0; iter < num; iter++) {
      
        // seek to random spot
        String field = ("f" + r.nextInt(numField)).intern();
        Term tx = new Term(field, getRandomString(r));

        int spot = Arrays.binarySearch(fieldTermsArray, tx);

        if (spot < 0) {
          if (VERBOSE) {
            System.out.println("TEST: non-exist seek to " + field + ":" + UnicodeUtil.toHexString(tx.text()));
          }

          // term does not exist:
          TermsEnum te = tes.get(field);
          if (te == null) {
            te = MultiFields.getTerms(reader, field).iterator(null);
            tes.put(field, te);
          }

          if (VERBOSE) {
            System.out.println("  got enum");
          }

          spot = -spot - 1;

          if (spot == fieldTerms.size() || !fieldTerms.get(spot).field().equals(field)) {
            assertEquals(TermsEnum.SeekStatus.END, te.seekCeil(tx.bytes()));
          } else {
            assertEquals(TermsEnum.SeekStatus.NOT_FOUND, te.seekCeil(tx.bytes()));

            if (VERBOSE) {
              System.out.println("  got term=" + UnicodeUtil.toHexString(te.term().utf8ToString()));
              System.out.println("  exp term=" + UnicodeUtil.toHexString(fieldTerms.get(spot).text()));
            }

            assertEquals(fieldTerms.get(spot).bytes(),
                         te.term());

            // now .next() this many times:
            int ct = _TestUtil.nextInt(r, 5, 100);
            for(int i=0;i<ct;i++) {
              if (VERBOSE) {
                System.out.println("TEST: now next()");
              }
              if (1+spot+i >= fieldTerms.size()) {
                break;
              }
              Term term = fieldTerms.get(1+spot+i);
              if (!term.field().equals(field)) {
                assertNull(te.next());
                break;
              } else {
                BytesRef t = te.next();

                if (VERBOSE) {
                  System.out.println("  got term=" + (t == null ? null : UnicodeUtil.toHexString(t.utf8ToString())));
                  System.out.println("       exp=" + UnicodeUtil.toHexString(term.text().toString()));
                }

                assertEquals(term.bytes(), t);
              }
            }

          }
        }
      }
    }
  }


  @Test
  public void testSurrogatesOrder() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(),
                                                dir,
                                                newIndexWriterConfig( TEST_VERSION_CURRENT,
                                                                      new MockAnalyzer(random())).setCodec(new PreFlexRWCodec()));

    final int numField = _TestUtil.nextInt(random(), 2, 5);

    int uniqueTermCount = 0;

    int tc = 0;

    List<Term> fieldTerms = new ArrayList<Term>();

    for(int f=0;f<numField;f++) {
      String field = "f" + f;
      final int numTerms = atLeast(200);

      final Set<String> uniqueTerms = new HashSet<String>();

      for(int i=0;i<numTerms;i++) {
        String term = getRandomString(random()) + "_ " + (tc++);
        uniqueTerms.add(term);
        fieldTerms.add(new Term(field, term));
        Document doc = new Document();
        doc.add(newField(field, term, StringField.TYPE_UNSTORED));
        w.addDocument(doc);
      }
      uniqueTermCount += uniqueTerms.size();
    }

    IndexReader reader = w.getReader();

    if (VERBOSE) {
      Collections.sort(fieldTerms, termAsUTF16Comparator);

      System.out.println("\nTEST: UTF16 order");
      for(Term t: fieldTerms) {
        System.out.println("  " + toHexString(t));
      }
    }

    // sorts in code point order:
    Collections.sort(fieldTerms);

    if (VERBOSE) {
      System.out.println("\nTEST: codepoint order");
      for(Term t: fieldTerms) {
        System.out.println("  " + toHexString(t));
      }
    }

    Term[] fieldTermsArray = fieldTerms.toArray(new Term[fieldTerms.size()]);

    //SegmentInfo si = makePreFlexSegment(r, "_0", dir, fieldInfos, codec, fieldTerms);

    //FieldsProducer fields = codec.fieldsProducer(new SegmentReadState(dir, si, fieldInfos, 1024, 1));
    //assertNotNull(fields);

    doTestStraightEnum(fieldTerms, reader, uniqueTermCount);
    doTestSeekExists(random(), fieldTerms, reader);
    doTestSeekDoesNotExist(random(), numField, fieldTerms, fieldTermsArray, reader);

    reader.close();
    w.close();
    dir.close();
  }
}
