package org.apache.lucene.index;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

public class FlexTestUtil {

  // index variations
  //   need del docs
  //   need payloads
  //   lots of randomness
  //   surrogate pairs
  //   

  // need more diverse index
  //    with omitTFAP
  //    with payloads
  //    bigger index

  // test advanceTo, mixed with getting or not getting positions
  // test that custom/null skipDocs work
  // test: direct flex compared to flex on non flex on flex
  // test with flex on one index and non-flex on the other
  //   (two dirs above)
  // temporarily force pre-flex emulation on flex emulation
  //   on pre-flex segment (in back compat test)
  // a sub-reader that has nonzero doc count but 100% are deleted
  // foreign bitdocs
  // advancing in DocsEnum, then next'ing, nextPosition'ing
  // mutlti-reader w/ empty sub-reader
  //   - eg from merge that had all del docs
  //   - eg from empty flush
  // make sure we test empty field (no terms)
  //  created from empty field in doc, but also, from
  //  removing all docs that had the field
  // test 1 vs many segment index
  // test multireader vs dir reader
  // test bulk read api
  // wrap reader as "external" reader -- double emulation test
  public static void verifyFlexVsPreFlex(Random rand, Directory d) throws Exception {
    IndexReader r = IndexReader.open(d);
    verifyFlexVsPreFlex(rand, r);
    r.close();
  }

  public static void verifyFlexVsPreFlex(Random rand, IndexWriter w) throws Exception {
    IndexReader r = w.getReader();
    verifyFlexVsPreFlex(rand, r);
    r.close();
  }
                                
  public static void verifyFlexVsPreFlex(Random rand, IndexReader r) throws Exception {
    // First test on DirReader

    verifyFlexVsPreFlexSingle(rand, r);

    // Then on each individual sub reader
    IndexReader[] subReaders = r.getSequentialSubReaders();
    IndexReader[] forcedSubReaders = new IndexReader[subReaders.length];
    for(int i=0;i<subReaders.length;i++) {
      forcedSubReaders[i] = new ForcedExternalReader(subReaders[i]);
      verifyFlexVsPreFlexSingle(rand, forcedSubReaders[i]);
      verifyFlexVsPreFlexSingle(rand, subReaders[i]);
    }

    // Then on a new MultiReader
    IndexReader m = new MultiReader(subReaders, false);
    verifyFlexVsPreFlexSingle(rand, m);
    m.close();

    // Then on a forced-external reader (forced flex to
    // emulate API on pre-flex API, which in turn is
    // emulating pre-flex on flex -- twisted, but, better
    // work):
    verifyFlexVsPreFlexSingle(rand, new ForcedExternalReader(r));
    m = new MultiReader(forcedSubReaders, false);
    verifyFlexVsPreFlexSingle(rand, m);
    m.close();
  }

  private static void verifyFlexVsPreFlexSingle(Random rand, IndexReader r) throws Exception {

    //List<Term> allTerms = new ArrayList<Term>();
    //System.out.println("TEST: now verify!!");
    testStraightEnum(r);
    testRandomSkips(rand, r);
    testRandomSeeks(rand, r);
    testBogusFieldTerms(rand, r);
  }

  private static void testBogusFieldTerms(Random rand, IndexReader r) throws Exception {
    final Fields fields = MultiFields.getFields(r);
    if (fields == null) {
      return;
    }
    for(int i=0;i<10;i++) {
      final String f = "bogus" + rand.nextInt() + "reallybogus";
      Terms terms = fields.terms(f);
      assertTrue(terms == null || terms.iterator().next() == null);
    }
  }

  private static void testStraightEnum(IndexReader r) throws Exception {

    // straight enum of fields/terms/docs/positions
    TermEnum termEnum = r.terms();
    final Fields fields = MultiFields.getFields(r);
    if (fields == null) {
      return;
    }
    FieldsEnum fieldsEnum = fields.iterator();
    while(true) {
      final String field = fieldsEnum.next();
      if (field == null) {
        boolean result = termEnum.next();
        if (result) {
          System.out.println("got unexpected term=" + termEnum.term() + " termEnum=" + termEnum);
        }
        assertFalse(result);
        break;
      }
      TermsEnum terms = fieldsEnum.terms();
      DocsAndPositionsEnum postings = null;
      DocsEnum docsEnum = null;
      final TermPositions termPos = r.termPositions();
      while(true) {
        final BytesRef termRef = terms.next();
        if (termRef == null) {
          break;
        } else {
          assertTrue(termEnum.next());
          Term t = termEnum.term();
          assertEquals(t.field(), field);
          assertEquals(t.text(), termRef.utf8ToString());
          assertEquals(termEnum.docFreq(), terms.docFreq());
          //allTerms.add(t);

          postings = terms.docsAndPositions(MultiFields.getDeletedDocs(r), postings);
          docsEnum = terms.docs(MultiFields.getDeletedDocs(r), docsEnum);

          final DocsEnum docs;
          if (postings != null) {
            docs = postings;
          } else {
            docs = docsEnum;
          }

          termPos.seek(t);
          while(true) {
            final int doc = docs.nextDoc();
            if (doc == DocsEnum.NO_MORE_DOCS) {
              assertFalse(termPos.next());
              break;
            } else {
              assertTrue(termPos.next());
              assertEquals(termPos.doc(), doc);
              assertEquals(termPos.freq(), docs.freq());
              final int freq = docs.freq();
              if (postings == null) {
                assertEquals(1, freq);
                // Old API did not always do this,
                // specifically in the MultiTermPositions
                // case when some segs omit positions and
                // some don't
                //assertEquals(0, termPos.nextPosition());
                assertEquals(false, termPos.isPayloadAvailable());
              } else {
                for(int i=0;i<freq;i++) {
                  final int position = postings.nextPosition();
                  assertEquals(position, termPos.nextPosition());
                  assertEquals(postings.hasPayload(), termPos.isPayloadAvailable());
                  if (postings.hasPayload()) {
                    BytesRef payload = postings.getPayload();
                    assertEquals(payload.length, termPos.getPayloadLength());
                    byte[] b2 = termPos.getPayload(null, 0);
                    assertNotNull(payload);
                    assertNotNull(b2);
                    assertTrue(equals(payload, b2));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private static void testRandomSkips(Random rand, IndexReader r) throws Exception {

    TermEnum termEnum = r.terms();
    Fields fields = MultiFields.getFields(r);
    if (fields == null) {
      return;
    }
    FieldsEnum fieldsEnum = fields.iterator();
    boolean skipNext = false;
    int[] docs2 = new int[16];
    int[] freqs2 = new int[16];
    while(true) {
      final String field = fieldsEnum.next();
      if (field == null) {
        boolean result = termEnum.next();
        if (result) {
          System.out.println("got unexpected term=" + termEnum.term() + " termEnum=" + termEnum);
        }
        assertFalse(result);
        break;
      }
      if (rand.nextInt(3) <= 1) {
        // Enum the terms
        //System.out.println("TEST:   get terms");
        TermsEnum terms = fieldsEnum.terms();
        final TermPositions termPos = r.termPositions();
        final TermDocs termDocs = r.termDocs();
        DocsEnum docs = null;
        DocsAndPositionsEnum postings = null;
        while(true) {
          final BytesRef termRef = terms.next();
          if (termRef == null) {
            break;
          } else {
            if (skipNext) {
              skipNext = false;
            } else {
              assertTrue(termEnum.next());
            }
            Term t = termEnum.term();
            assertEquals(t.field(), field);
            assertEquals(t.text(), termRef.utf8ToString());
            assertEquals(termEnum.docFreq(), terms.docFreq());
            //allTerms.add(t);

            if (rand.nextInt(3) <= 1) {
              docs = terms.docs(MultiFields.getDeletedDocs(r), docs);
              assert !(docs instanceof DocsAndPositionsEnum): "docs=" + docs;
              postings = terms.docsAndPositions(MultiFields.getDeletedDocs(r), postings);
              final DocsEnum docsEnum;
              if (postings == null) {
                docsEnum = docs;
              } else {
                docsEnum = postings;
              }
              if (rand.nextBoolean()) {
                // use bulk read API
                termDocs.seek(t);
                DocsEnum.BulkReadResult result1 = docs.getBulkResult();
                int result1Count = 0;
                int count2 = 0;
                while(true) {
                  if (result1Count == 0) {
                    result1Count = docs.read();
                  }
                  if (count2 == 0) {
                    count2 = termDocs.read(docs2, freqs2);
                  }

                  if (result1Count == 0 || count2 == 0) {
                    assertEquals(0, count2);
                    assertEquals(0, result1Count);
                    break;
                  }
                  final int limit = Math.min(result1Count, count2);
                  for(int i=0;i<limit;i++) {
                    assertEquals(result1.docs.ints[i], docs2[i]);
                    assertEquals(result1.freqs.ints[i], freqs2[i]);
                  }
                  if (result1Count > limit) {
                    // copy down
                    // TODO: in general I should not muck w/
                    // the int[]'s returned to me like
                    // this... this could mess up codecs
                    // that have persistent RAM storage of
                    // these int[]'s
                    System.arraycopy(result1.docs.ints, limit, result1.docs.ints, 0, result1Count-limit);
                    System.arraycopy(result1.freqs.ints, limit, result1.freqs.ints, 0, result1Count-limit);
                  }
                  result1Count -= limit;

                  if (count2 > limit) {
                    // copy down
                    System.arraycopy(docs2, limit, docs2, 0, count2-limit);
                    System.arraycopy(freqs2, limit, freqs2, 0, count2-limit);
                  }
                  count2 -= limit;
                }
              } else {
                // Enum the docs one by one
                //System.out.println("TEST:      get docs");
                termPos.seek(t);
                while(true) {
                  final int doc = docsEnum.nextDoc();
                  if (doc == DocsEnum.NO_MORE_DOCS) {
                    assertFalse(termPos.next());
                    break;
                  } else {
                    assertTrue(termPos.next());
                    assertEquals(termPos.doc(), doc);
                    assertEquals(termPos.freq(), docsEnum.freq());
                    //System.out.println("TEST:     doc=" + doc + " freq=" + docs.freq());
                    if (rand.nextInt(3) <= 1) {
                      // enum the positions
                      final int freq = docsEnum.freq();
                      if (postings == null) {
                        assertEquals(1, termPos.freq());
                        // Old API did not always do this,
                        // specifically in the MultiTermPositions
                        // case when some segs omit positions and
                        // some don't
                        //assertEquals(0, termPos.nextPosition());
                        assertFalse(termPos.isPayloadAvailable());
                      } else {
                        // we have positions
                        for(int i=0;i<freq;i++) {
                          final int position = postings.nextPosition();
                          //System.out.println("TEST:       pos=" + position);
                          assertEquals(position, termPos.nextPosition());
                          assertEquals(postings.hasPayload(), termPos.isPayloadAvailable());
                          if (postings.hasPayload()) {
                            if (rand.nextInt(3) <= 1) {
                              BytesRef payload = postings.getPayload();
                              assertEquals(payload.length, termPos.getPayloadLength());
                              byte[] b2 = termPos.getPayload(null, 0);
                              assertNotNull(payload);
                              assertNotNull(b2);
                              assertTrue(equals(payload, b2));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            } else {
              //System.out.println("TEST:      skip docs");
            }
          }
        }
      } else {
        // Skip terms for this field
        termEnum = r.terms(new Term(field, "\uFFFF"));
        skipNext = true;
        //System.out.println("TEST:   skip terms; now=" + termEnum.term());
      }
    }

    // seek to before first term in a field
    // seek to after last term in a field
    // seek to random terms
    // enum docs, sometimes skipping
    // enum positions, sometimes skipping payloads
  }

  private static boolean equals(BytesRef b1, byte[] b2) {
    if (b1.length == b2.length) {
      final int end = b1.offset + b1.length;
      for(int i=b1.offset;i<end;i++) {
        if (b1.bytes[i] != b2[i-b1.offset]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public static int nextInt(Random rand, int min, int max) {
    return min + rand.nextInt(max-min);
  }

  public static int nextInt(Random rand, int max) {
    return rand.nextInt(max);
  }

  public static String getRandomText(Random rand, int minLen, int maxLen, boolean doUnpairedSurr) {
    final int len = nextInt(rand, minLen, maxLen);
    char[] buffer = new char[len];
    
    for(int i=0;i<len;i++) {
      int t = rand.nextInt(doUnpairedSurr ? 6 : 5);
      if (0 == t && i < len-1) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) nextInt(rand, 0xd800, 0xdc00);
        // Low surrogate
        buffer[i] = (char) nextInt(rand, 0xdc00, 0xe000);
      } else if (t <= 1) {
        buffer[i] = (char) nextInt(rand, 0x80);
      } else if (2 == t) {
        buffer[i] = (char) nextInt(rand, 0x80, 0x800);
      } else if (3 == t) {
        buffer[i] = (char) nextInt(rand, 0x800, 0xd800);
      } else if (4 == t) {
        buffer[i] = (char) nextInt(rand, 0xe000, 0xffff);
      } else if (5 == t) {
        // Illegal unpaired surrogate
        if (rand.nextBoolean()) {
          buffer[i] = (char) nextInt(rand, 0xd800, 0xdc00);
        } else {
          buffer[i] = (char) nextInt(rand, 0xdc00, 0xe000);
        }
      }
    }

    return new String(buffer);
  }

  private static void testRandomSeeks(Random rand, IndexReader r) throws Exception {
    final int ITER = 100 * _TestUtil.getRandomMultiplier();
    List<String> allFields = new ArrayList<String>();
    Fields fields = MultiFields.getFields(r);
    if (fields == null) {
      return;
    }
    FieldsEnum fieldsEnum = fields.iterator();
    while(true) {
      String f = fieldsEnum.next();
      if (f == null) {
        break;
      }
      allFields.add(f);
    }
    final int fieldCount = allFields.size();
    if (fieldCount == 0) {
      return;
    }
    
    final TermPositions termPositions = r.termPositions();
    DocsEnum docs = null;
    DocsAndPositionsEnum postings = null;

    for(int i=0;i<ITER;i++) {
      // Random field:
      String f = allFields.get(rand.nextInt(fieldCount));

      String text = getRandomText(rand, 1, 3, false);
      final TermsEnum termsEnum = MultiFields.getFields(r).terms(f).iterator();

      final TermsEnum.SeekStatus seekStatus = termsEnum.seek(new BytesRef(text));
      Term t = new Term(f, text);
      //System.out.println("seek to " + t);

      final TermEnum termEnum = r.terms(t);
      
      if (seekStatus == TermsEnum.SeekStatus.END) {
        //System.out.println("found end");
        assertTrue(termEnum.term() == null || termEnum.term().field() != f);
        continue;
      } else if (seekStatus == TermsEnum.SeekStatus.FOUND) {
        //System.out.println("found exact");
        assertEquals(t, termEnum.term());
      } else {
        //System.out.println("found other");
        assertEquals(termsEnum.term().utf8ToString(), termEnum.term().text());
      }

      assertEquals(termsEnum.docFreq(), termEnum.docFreq());

      docs = termsEnum.docs(MultiFields.getDeletedDocs(r), docs);
      postings = termsEnum.docsAndPositions(MultiFields.getDeletedDocs(r), postings);

      termPositions.seek(termEnum.term());

      int doc = 0;

      final DocsEnum docsEnum;
      if (postings != null) {
        docsEnum = postings;
      } else {
        docsEnum = docs;
      }
      for(int j=0;j<20;j++) {
        final int inc = nextInt(rand, 1, Math.max(10, r.maxDoc()/15));
        int newDoc1 = docsEnum.advance(doc+inc);
        boolean found = termPositions.skipTo(doc+inc);
        int newDoc2;

        if (newDoc1 == DocsEnum.NO_MORE_DOCS) {
          assertFalse(found);
          break;
        } else {
          assertTrue(found);
          newDoc2 = termPositions.doc();
        }
        
        assertEquals(newDoc1, newDoc2);
        assertEquals(docsEnum.freq(), termPositions.freq());

        doc = newDoc1;

        if (postings == null) {
          assertEquals(1, termPositions.freq());
          // Old API did not always do this,
          // specifically in the MultiTermPositions
          // case when some segs omit positions and
          // some don't
          //assertEquals(0, termPositions.nextPosition());
          assertFalse(termPositions.isPayloadAvailable());
        } else {
          for(int k=0;k<docsEnum.freq();k++) {
            int pos1 = postings.nextPosition();
            int pos2 = termPositions.nextPosition();
            assertEquals(pos1, pos2);
            assertEquals(postings.hasPayload(), termPositions.isPayloadAvailable());
            if (postings.hasPayload()) {
              BytesRef b1 = postings.getPayload();
              assertEquals(b1.length, termPositions.getPayloadLength());
              byte[] b2 = termPositions.getPayload(null, 0);
              assertNotNull(b1);
              assertNotNull(b2);
              assertTrue(equals(b1, b2));
            }
          }
        }
      }
    }
  }

  // Delegates to a "normal" IndexReader, making it look
  // "external", to force testing of the "flex API on
  // external reader" layer.  DO NOT OVERRIDE
  // getSequentialSubReaders!!
  public final static class ForcedExternalReader extends IndexReader {
    private final IndexReader r;
    public ForcedExternalReader(IndexReader r) {
      this.r = r;
    }

    public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
      return r.getTermFreqVectors(docNumber);
    }

    public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
      return r.getTermFreqVector(docNumber, field);
    }

    public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
      r.getTermFreqVector(docNumber, field, mapper);
    }

    public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
      r.getTermFreqVector(docNumber, mapper);
    }

    public Bits getDeletedDocs() throws IOException {
      return MultiFields.getDeletedDocs(r);
    }

    public int numDocs() {
      return r.numDocs();
    }

    public int maxDoc() {
      return r.maxDoc();
    }

    public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
      return r.document(n, fieldSelector);
    }

    public boolean isDeleted(int n) {
      return r.isDeleted(n);
    }

    public boolean hasDeletions() {
      return r.hasDeletions();
    }

    public byte[] norms(String field) throws IOException {
      return r.norms(field);
    }

    public String toString() {
      return "ForcedExternalReader(" + r + ")";
    }

    public void norms(String field, byte[] bytes, int offset) 
      throws IOException {
      r.norms(field, bytes, offset);
    }
    
    protected  void doSetNorm(int doc, String field, byte value)
      throws CorruptIndexException, IOException {
      r.doSetNorm(doc, field, value);
    }

    public TermEnum terms() throws IOException {
      return r.terms();
    }

    public TermEnum terms(Term t) throws IOException {
      return r.terms(t);
    }

    public int docFreq(Term t) throws IOException {
      return r.docFreq(t);
    }

    public TermDocs termDocs() throws IOException {
      return r.termDocs();
    }

    public TermPositions termPositions() throws IOException {
      return r.termPositions();
    }

    public void doDelete(int docID) throws IOException {
      r.doDelete(docID);
    }

    public void doUndeleteAll() throws IOException {
      r.doUndeleteAll();
    }

    protected void doCommit(Map<String, String> commitUserData) throws IOException {
      r.doCommit(commitUserData);
    }

    protected void doClose() throws IOException {
      r.doClose();
    }

    public Collection<String> getFieldNames(FieldOption fldOption) {
      return r.getFieldNames(fldOption);
    }
  }

  public static void main(String[] args) throws Exception {
    //Directory dir = FSDirectory.open(new File("/x/lucene/wiki.5M/index"));
    Directory dir = FSDirectory.open(new File("/x/lucene/flex.wiki.1M/index"));
    verifyFlexVsPreFlex(new Random(), dir);
    dir.close();
  }
}