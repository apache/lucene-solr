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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestStressIndexing2 extends LuceneTestCase {
  static int maxFields = 4;
  static int bigFieldSize = 10;
  static boolean sameFieldOrder = false;
  static int mergeFactor = 3;
  static int maxBufferedDocs = 3;
  static int seed = 0;

  public void testRandomIWReader() throws Throwable {
    Directory dir = newMaybeVirusCheckingDirectory();

    // TODO: verify equals using IW.getReader
    DocsAndWriter dw = indexRandomIWReader(5, 3, 100, dir);
    DirectoryReader reader = dw.writer.getReader();
    dw.writer.commit();
    verifyEquals(random(), reader, dir, "id");
    reader.close();
    dw.writer.close();
    dir.close();
  }

  public void testRandom() throws Throwable {
    Directory dir1 = newMaybeVirusCheckingDirectory();
    Directory dir2 = newMaybeVirusCheckingDirectory();
    // mergeFactor=2; maxBufferedDocs=2; Map docs = indexRandom(1, 3, 2, dir1);
    boolean doReaderPooling = random().nextBoolean();
    Map<String, Document> docs = indexRandom(5, 3, 100, dir1, doReaderPooling);
    indexSerial(random(), docs, dir2);

    // verifying verify
    // verifyEquals(dir1, dir1, "id");
    // verifyEquals(dir2, dir2, "id");

    verifyEquals(dir1, dir2, "id");
    dir1.close();
    dir2.close();
  }

  public void testMultiConfig() throws Throwable {
    // test lots of smaller different params together

    int num = atLeast(3);
    for (int i = 0; i < num; i++) { // increase iterations for better testing
      if (VERBOSE) {
        System.out.println("\n\nTEST: top iter=" + i);
      }
      sameFieldOrder = random().nextBoolean();
      mergeFactor = random().nextInt(3) + 2;
      maxBufferedDocs = random().nextInt(3) + 2;
      boolean doReaderPooling = random().nextBoolean();
      seed++;

      int nThreads = random().nextInt(5) + 1;
      int iter = random().nextInt(5) + 1;
      int range = random().nextInt(20) + 1;
      Directory dir1 = newDirectory();
      Directory dir2 = newDirectory();
      if (VERBOSE) {
        System.out.println(
            "  nThreads="
                + nThreads
                + " iter="
                + iter
                + " range="
                + range
                + " doPooling="
                + doReaderPooling
                + " sameFieldOrder="
                + sameFieldOrder
                + " mergeFactor="
                + mergeFactor
                + " maxBufferedDocs="
                + maxBufferedDocs);
      }
      Map<String, Document> docs = indexRandom(nThreads, iter, range, dir1, doReaderPooling);
      if (VERBOSE) {
        System.out.println("TEST: index serial");
      }
      indexSerial(random(), docs, dir2);
      if (VERBOSE) {
        System.out.println("TEST: verify");
      }
      verifyEquals(dir1, dir2, "id");
      dir1.close();
      dir2.close();
    }
  }

  static Term idTerm = new Term("id", "");
  IndexingThread[] threads;
  static Comparator<IndexableField> fieldNameComparator =
      new Comparator<IndexableField>() {
        @Override
        public int compare(IndexableField o1, IndexableField o2) {
          return o1.name().compareTo(o2.name());
        }
      };

  // This test avoids using any extra synchronization in the multiple
  // indexing threads to test that IndexWriter does correctly synchronize
  // everything.

  public static class DocsAndWriter {
    Map<String, Document> docs;
    IndexWriter writer;
  }

  public DocsAndWriter indexRandomIWReader(int nThreads, int iterations, int range, Directory dir)
      throws IOException, InterruptedException {
    Map<String, Document> docs = new HashMap<>();
    IndexWriter w =
        RandomIndexWriter.mockIndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setOpenMode(OpenMode.CREATE)
                .setRAMBufferSizeMB(0.1)
                .setMaxBufferedDocs(maxBufferedDocs)
                .setMergePolicy(newLogMergePolicy()),
            random());
    w.commit();
    LogMergePolicy lmp = (LogMergePolicy) w.getConfig().getMergePolicy();
    lmp.setNoCFSRatio(0.0);
    lmp.setMergeFactor(mergeFactor);
    /***
     * w.setMaxMergeDocs(Integer.MAX_VALUE);
     * w.setMaxFieldLength(10000);
     * w.setRAMBufferSizeMB(1);
     * w.setMergeFactor(10);
     ***/

    threads = new IndexingThread[nThreads];
    for (int i = 0; i < threads.length; i++) {
      IndexingThread th = new IndexingThread();
      th.w = w;
      th.base = 1000000 * i;
      th.range = range;
      th.iterations = iterations;
      threads[i] = th;
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    // w.forceMerge(1);
    // w.close();

    for (int i = 0; i < threads.length; i++) {
      IndexingThread th = threads[i];
      synchronized (th) {
        docs.putAll(th.docs);
      }
    }

    TestUtil.checkIndex(dir);
    DocsAndWriter dw = new DocsAndWriter();
    dw.docs = docs;
    dw.writer = w;
    return dw;
  }

  public Map<String, Document> indexRandom(
      int nThreads, int iterations, int range, Directory dir, boolean doReaderPooling)
      throws IOException, InterruptedException {
    Map<String, Document> docs = new HashMap<>();
    IndexWriter w =
        RandomIndexWriter.mockIndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setOpenMode(OpenMode.CREATE)
                .setRAMBufferSizeMB(0.1)
                .setMaxBufferedDocs(maxBufferedDocs)
                .setReaderPooling(doReaderPooling)
                .setMergePolicy(newLogMergePolicy()),
            random());
    LogMergePolicy lmp = (LogMergePolicy) w.getConfig().getMergePolicy();
    lmp.setNoCFSRatio(0.0);
    lmp.setMergeFactor(mergeFactor);

    threads = new IndexingThread[nThreads];
    for (int i = 0; i < threads.length; i++) {
      IndexingThread th = new IndexingThread();
      th.w = w;
      th.base = 1000000 * i;
      th.range = range;
      th.iterations = iterations;
      threads[i] = th;
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    // w.forceMerge(1);
    w.close();

    for (int i = 0; i < threads.length; i++) {
      IndexingThread th = threads[i];
      synchronized (th) {
        docs.putAll(th.docs);
      }
    }

    // System.out.println("TEST: checkindex");
    TestUtil.checkIndex(dir);

    return docs;
  }

  public static void indexSerial(Random random, Map<String, Document> docs, Directory dir)
      throws IOException {
    IndexWriter w =
        new IndexWriter(
            dir,
            LuceneTestCase.newIndexWriterConfig(random, new MockAnalyzer(random))
                .setMergePolicy(newLogMergePolicy()));

    // index all docs in a single thread
    Iterator<Document> iter = docs.values().iterator();
    while (iter.hasNext()) {
      Document d = iter.next();
      ArrayList<IndexableField> fields = new ArrayList<>();
      fields.addAll(d.getFields());
      // put fields in same order each time
      Collections.sort(fields, fieldNameComparator);

      Document d1 = new Document();
      for (int i = 0; i < fields.size(); i++) {
        d1.add(fields.get(i));
      }
      w.addDocument(d1);
      // System.out.println("indexing "+d1);
    }

    w.close();
  }

  public void verifyEquals(Random r, DirectoryReader r1, Directory dir2, String idField)
      throws Throwable {
    DirectoryReader r2 = DirectoryReader.open(dir2);
    verifyEquals(r1, r2, idField);
    r2.close();
  }

  public void verifyEquals(Directory dir1, Directory dir2, String idField) throws Throwable {
    DirectoryReader r1 = DirectoryReader.open(dir1);
    DirectoryReader r2 = DirectoryReader.open(dir2);
    verifyEquals(r1, r2, idField);
    r1.close();
    r2.close();
  }

  private static void printDocs(DirectoryReader r) throws Throwable {
    for (LeafReaderContext ctx : r.leaves()) {
      // TODO: improve this
      LeafReader sub = ctx.reader();
      Bits liveDocs = sub.getLiveDocs();
      System.out.println("  " + ((SegmentReader) sub).getSegmentInfo());
      for (int docID = 0; docID < sub.maxDoc(); docID++) {
        Document doc = sub.document(docID);
        if (liveDocs == null || liveDocs.get(docID)) {
          System.out.println("    docID=" + docID + " id:" + doc.get("id"));
        } else {
          System.out.println("    DEL docID=" + docID + " id:" + doc.get("id"));
        }
      }
    }
  }

  private static int nextNonDeletedDoc(PostingsEnum it, Bits liveDocs) throws IOException {
    int doc = it.nextDoc();
    while (doc != DocIdSetIterator.NO_MORE_DOCS && liveDocs != null && liveDocs.get(doc) == false) {
      doc = it.nextDoc();
    }
    return doc;
  }

  public void verifyEquals(DirectoryReader r1, DirectoryReader r2, String idField)
      throws Throwable {
    if (VERBOSE) {
      System.out.println("\nr1 docs:");
      printDocs(r1);
      System.out.println("\nr2 docs:");
      printDocs(r2);
    }
    if (r1.numDocs() != r2.numDocs()) {
      assert false : "r1.numDocs()=" + r1.numDocs() + " vs r2.numDocs()=" + r2.numDocs();
    }
    boolean hasDeletes = !(r1.maxDoc() == r2.maxDoc() && r1.numDocs() == r1.maxDoc());

    int[] r2r1 = new int[r2.maxDoc()]; // r2 id to r1 id mapping

    // create mapping from id2 space to id2 based on idField
    if (FieldInfos.getIndexedFields(r1).isEmpty()) {
      assertTrue(FieldInfos.getIndexedFields(r2).isEmpty());
      return;
    }
    final Terms terms1 = MultiTerms.getTerms(r1, idField);
    if (terms1 == null) {
      assertTrue(MultiTerms.getTerms(r2, idField) == null);
      return;
    }
    final TermsEnum termsEnum = terms1.iterator();

    final Bits liveDocs1 = MultiBits.getLiveDocs(r1);
    final Bits liveDocs2 = MultiBits.getLiveDocs(r2);

    Terms terms2 = MultiTerms.getTerms(r2, idField);
    if (terms2 == null) {
      // make sure r1 is in fact empty (eg has only all
      // deleted docs):
      Bits liveDocs = MultiBits.getLiveDocs(r1);
      PostingsEnum docs = null;
      while (termsEnum.next() != null) {
        docs = TestUtil.docs(random(), termsEnum, docs, PostingsEnum.NONE);
        while (nextNonDeletedDoc(docs, liveDocs) != DocIdSetIterator.NO_MORE_DOCS) {
          fail("r1 is not empty but r2 is");
        }
      }
      return;
    }
    TermsEnum termsEnum2 = terms2.iterator();

    PostingsEnum termDocs1 = null;
    PostingsEnum termDocs2 = null;

    while (true) {
      BytesRef term = termsEnum.next();
      // System.out.println("TEST: match id term=" + term);
      if (term == null) {
        break;
      }

      termDocs1 = TestUtil.docs(random(), termsEnum, termDocs1, PostingsEnum.NONE);
      if (termsEnum2.seekExact(term)) {
        termDocs2 = TestUtil.docs(random(), termsEnum2, termDocs2, PostingsEnum.NONE);
      } else {
        termDocs2 = null;
      }

      if (nextNonDeletedDoc(termDocs1, liveDocs1) == DocIdSetIterator.NO_MORE_DOCS) {
        // This doc is deleted and wasn't replaced
        assertTrue(
            termDocs2 == null
                || nextNonDeletedDoc(termDocs2, liveDocs2) == DocIdSetIterator.NO_MORE_DOCS);
        continue;
      }

      int id1 = termDocs1.docID();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, nextNonDeletedDoc(termDocs1, liveDocs1));

      assertTrue(nextNonDeletedDoc(termDocs2, liveDocs2) != DocIdSetIterator.NO_MORE_DOCS);
      int id2 = termDocs2.docID();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, nextNonDeletedDoc(termDocs2, liveDocs2));

      r2r1[id2] = id1;

      // verify stored fields are equivalent
      try {
        verifyEquals(r1.document(id1), r2.document(id2));
      } catch (Throwable t) {
        System.out.println("FAILED id=" + term + " id1=" + id1 + " id2=" + id2 + " term=" + term);
        System.out.println("  d1=" + r1.document(id1));
        System.out.println("  d2=" + r2.document(id2));
        throw t;
      }

      try {
        // verify term vectors are equivalent
        verifyEquals(r1.getTermVectors(id1), r2.getTermVectors(id2));
      } catch (Throwable e) {
        System.out.println("FAILED id=" + term + " id1=" + id1 + " id2=" + id2);
        Fields tv1 = r1.getTermVectors(id1);
        System.out.println("  d1=" + tv1);
        if (tv1 != null) {
          PostingsEnum dpEnum = null;
          PostingsEnum dEnum = null;
          for (String field : tv1) {
            System.out.println("    " + field + ":");
            Terms terms3 = tv1.terms(field);
            assertNotNull(terms3);
            TermsEnum termsEnum3 = terms3.iterator();
            BytesRef term2;
            while ((term2 = termsEnum3.next()) != null) {
              System.out.println(
                  "      " + term2.utf8ToString() + ": freq=" + termsEnum3.totalTermFreq());
              dpEnum = termsEnum3.postings(dpEnum, PostingsEnum.ALL);
              if (terms3.hasPositions()) {
                assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                final int freq = dpEnum.freq();
                System.out.println("        doc=" + dpEnum.docID() + " freq=" + freq);
                for (int posUpto = 0; posUpto < freq; posUpto++) {
                  System.out.println("          pos=" + dpEnum.nextPosition());
                }
              } else {
                dEnum = TestUtil.docs(random(), termsEnum3, dEnum, PostingsEnum.FREQS);
                assertNotNull(dEnum);
                assertTrue(dEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                final int freq = dEnum.freq();
                System.out.println("        doc=" + dEnum.docID() + " freq=" + freq);
              }
            }
          }
        }

        Fields tv2 = r2.getTermVectors(id2);
        System.out.println("  d2=" + tv2);
        if (tv2 != null) {
          PostingsEnum dpEnum = null;
          PostingsEnum dEnum = null;
          for (String field : tv2) {
            System.out.println("    " + field + ":");
            Terms terms3 = tv2.terms(field);
            assertNotNull(terms3);
            TermsEnum termsEnum3 = terms3.iterator();
            BytesRef term2;
            while ((term2 = termsEnum3.next()) != null) {
              System.out.println(
                  "      " + term2.utf8ToString() + ": freq=" + termsEnum3.totalTermFreq());
              dpEnum = termsEnum3.postings(dpEnum, PostingsEnum.ALL);
              if (dpEnum != null) {
                assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                final int freq = dpEnum.freq();
                System.out.println("        doc=" + dpEnum.docID() + " freq=" + freq);
                for (int posUpto = 0; posUpto < freq; posUpto++) {
                  System.out.println("          pos=" + dpEnum.nextPosition());
                }
              } else {
                dEnum = TestUtil.docs(random(), termsEnum3, dEnum, PostingsEnum.FREQS);
                assertNotNull(dEnum);
                assertTrue(dEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
                final int freq = dEnum.freq();
                System.out.println("        doc=" + dEnum.docID() + " freq=" + freq);
              }
            }
          }
        }

        throw e;
      }
    }

    // System.out.println("TEST: done match id");

    // Verify postings
    // System.out.println("TEST: create te1");
    final Iterator<String> fields1Enum =
        FieldInfos.getIndexedFields(r1).stream().sorted().iterator();
    final Iterator<String> fields2Enum =
        FieldInfos.getIndexedFields(r2).stream().sorted().iterator();

    String field1 = null, field2 = null;
    TermsEnum termsEnum1 = null;
    termsEnum2 = null;
    PostingsEnum docs1 = null, docs2 = null;

    // pack both doc and freq into single element for easy sorting
    long[] info1 = new long[r1.numDocs()];
    long[] info2 = new long[r2.numDocs()];

    for (; ; ) {
      BytesRef term1 = null, term2 = null;

      // iterate until we get some docs
      int len1;
      for (; ; ) {
        len1 = 0;
        if (termsEnum1 == null) {
          if (!fields1Enum.hasNext()) {
            break;
          }
          field1 = fields1Enum.next();
          Terms terms = MultiTerms.getTerms(r1, field1);
          if (terms == null) {
            continue;
          }
          termsEnum1 = terms.iterator();
        }
        term1 = termsEnum1.next();
        if (term1 == null) {
          // no more terms in this field
          termsEnum1 = null;
          continue;
        }

        // System.out.println("TEST: term1=" + term1);
        docs1 = TestUtil.docs(random(), termsEnum1, docs1, PostingsEnum.FREQS);
        while (docs1.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int d = docs1.docID();
          if (liveDocs1 != null && liveDocs1.get(d) == false) {
            continue;
          }
          int f = docs1.freq();
          info1[len1] = (((long) d) << 32) | f;
          len1++;
        }
        if (len1 > 0) break;
      }

      // iterate until we get some docs
      int len2;
      for (; ; ) {
        len2 = 0;
        if (termsEnum2 == null) {
          if (!fields2Enum.hasNext()) {
            break;
          }
          field2 = fields2Enum.next();
          Terms terms = MultiTerms.getTerms(r2, field2);
          if (terms == null) {
            continue;
          }
          termsEnum2 = terms.iterator();
        }
        term2 = termsEnum2.next();
        if (term2 == null) {
          // no more terms in this field
          termsEnum2 = null;
          continue;
        }

        // System.out.println("TEST: term1=" + term1);
        docs2 = TestUtil.docs(random(), termsEnum2, docs2, PostingsEnum.FREQS);
        while (docs2.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          if (liveDocs2 != null && liveDocs2.get(docs2.docID()) == false) {
            continue;
          }
          int d = r2r1[docs2.docID()];
          int f = docs2.freq();
          info2[len2] = (((long) d) << 32) | f;
          len2++;
        }
        if (len2 > 0) break;
      }

      assertEquals(len1, len2);
      if (len1 == 0) break; // no more terms

      assertEquals(field1, field2);
      assertEquals(term1, term2);

      if (!hasDeletes) assertEquals(termsEnum1.docFreq(), termsEnum2.docFreq());

      assertEquals("len1=" + len1 + " len2=" + len2 + " deletes?=" + hasDeletes, term1, term2);

      // sort info2 to get it into ascending docid
      Arrays.sort(info2, 0, len2);

      // now compare
      for (int i = 0; i < len1; i++) {
        assertEquals(
            "i="
                + i
                + " len="
                + len1
                + " d1="
                + (info1[i] >>> 32)
                + " f1="
                + (info1[i] & Integer.MAX_VALUE)
                + " d2="
                + (info2[i] >>> 32)
                + " f2="
                + (info2[i] & Integer.MAX_VALUE)
                + " field="
                + field1
                + " term="
                + term1.utf8ToString(),
            info1[i],
            info2[i]);
      }
    }
  }

  public static void verifyEquals(Document d1, Document d2) {
    List<IndexableField> ff1 = new ArrayList<>(d1.getFields());
    List<IndexableField> ff2 = new ArrayList<>(d2.getFields());

    Collections.sort(ff1, fieldNameComparator);
    Collections.sort(ff2, fieldNameComparator);

    assertEquals(ff1 + " : " + ff2, ff1.size(), ff2.size());

    for (int i = 0; i < ff1.size(); i++) {
      IndexableField f1 = ff1.get(i);
      IndexableField f2 = ff2.get(i);
      if (f1.binaryValue() != null) {
        assert (f2.binaryValue() != null);
      } else {
        String s1 = f1.stringValue();
        String s2 = f2.stringValue();
        assertEquals(ff1 + " : " + ff2, s1, s2);
      }
    }
  }

  public static void verifyEquals(Fields d1, Fields d2) throws IOException {
    if (d1 == null) {
      assertTrue(d2 == null || d2.size() == 0);
      return;
    }
    assertTrue(d2 != null);

    Iterator<String> fieldsEnum2 = d2.iterator();

    for (String field1 : d1) {
      String field2 = fieldsEnum2.next();
      assertEquals(field1, field2);

      Terms terms1 = d1.terms(field1);
      assertNotNull(terms1);
      TermsEnum termsEnum1 = terms1.iterator();

      Terms terms2 = d2.terms(field2);
      assertNotNull(terms2);
      TermsEnum termsEnum2 = terms2.iterator();

      PostingsEnum dpEnum1 = null;
      PostingsEnum dpEnum2 = null;
      PostingsEnum dEnum1 = null;
      PostingsEnum dEnum2 = null;

      BytesRef term1;
      while ((term1 = termsEnum1.next()) != null) {
        BytesRef term2 = termsEnum2.next();
        assertEquals(term1, term2);
        assertEquals(termsEnum1.totalTermFreq(), termsEnum2.totalTermFreq());

        dpEnum1 = termsEnum1.postings(dpEnum1, PostingsEnum.ALL);
        dpEnum2 = termsEnum2.postings(dpEnum2, PostingsEnum.ALL);

        if (terms1.hasPositions()) {
          assertTrue(terms2.hasPositions());
          int docID1 = dpEnum1.nextDoc();
          dpEnum2.nextDoc();
          // docIDs are not supposed to be equal
          // int docID2 = dpEnum2.nextDoc();
          // assertEquals(docID1, docID2);
          assertTrue(docID1 != DocIdSetIterator.NO_MORE_DOCS);

          int freq1 = dpEnum1.freq();
          int freq2 = dpEnum2.freq();
          assertEquals(freq1, freq2);

          for (int posUpto = 0; posUpto < freq1; posUpto++) {
            int pos1 = dpEnum1.nextPosition();
            int pos2 = dpEnum2.nextPosition();
            assertEquals(pos1, pos2);
            if (terms1.hasOffsets()) {
              assertTrue(terms2.hasOffsets());
              assertEquals(dpEnum1.startOffset(), dpEnum2.startOffset());
              assertEquals(dpEnum1.endOffset(), dpEnum2.endOffset());
            }
          }
          assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum1.nextDoc());
          assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum2.nextDoc());
        } else {
          dEnum1 = TestUtil.docs(random(), termsEnum1, dEnum1, PostingsEnum.FREQS);
          dEnum2 = TestUtil.docs(random(), termsEnum2, dEnum2, PostingsEnum.FREQS);
          assertNotNull(dEnum1);
          assertNotNull(dEnum2);
          int docID1 = dEnum1.nextDoc();
          dEnum2.nextDoc();
          // docIDs are not supposed to be equal
          // int docID2 = dEnum2.nextDoc();
          // assertEquals(docID1, docID2);
          assertTrue(docID1 != DocIdSetIterator.NO_MORE_DOCS);
          int freq1 = dEnum1.freq();
          int freq2 = dEnum2.freq();
          assertEquals(freq1, freq2);
          assertEquals(DocIdSetIterator.NO_MORE_DOCS, dEnum1.nextDoc());
          assertEquals(DocIdSetIterator.NO_MORE_DOCS, dEnum2.nextDoc());
        }
      }

      assertNull(termsEnum2.next());
    }
    assertFalse(fieldsEnum2.hasNext());
  }

  private static class IndexingThread extends Thread {
    IndexWriter w;
    int base;
    int range;
    int iterations;
    Map<String, Document> docs = new HashMap<>();
    Random r;

    public int nextInt(int lim) {
      return r.nextInt(lim);
    }

    // start is inclusive and end is exclusive
    public int nextInt(int start, int end) {
      return start + r.nextInt(end - start);
    }

    char[] buffer = new char[100];

    private int addUTF8Token(int start) {
      final int end = start + nextInt(20);
      if (buffer.length < 1 + end) {
        char[] newBuffer = new char[(int) ((1 + end) * 1.25)];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        buffer = newBuffer;
      }

      for (int i = start; i < end; i++) {
        int t = nextInt(5);
        if (0 == t && i < end - 1) {
          // Make a surrogate pair
          // High surrogate
          buffer[i++] = (char) nextInt(0xd800, 0xdc00);
          // Low surrogate
          buffer[i] = (char) nextInt(0xdc00, 0xe000);
        } else if (t <= 1) buffer[i] = (char) nextInt(0x80);
        else if (2 == t) buffer[i] = (char) nextInt(0x80, 0x800);
        else if (3 == t) buffer[i] = (char) nextInt(0x800, 0xd800);
        else if (4 == t) buffer[i] = (char) nextInt(0xe000, 0xffff);
      }
      buffer[end] = ' ';
      return 1 + end;
    }

    public String getString(int nTokens) {
      nTokens = nTokens != 0 ? nTokens : r.nextInt(4) + 1;

      // Half the time make a random UTF8 string
      if (r.nextBoolean()) return getUTF8String(nTokens);

      // avoid StringBuffer because it adds extra synchronization.
      char[] arr = new char[nTokens * 2];
      for (int i = 0; i < nTokens; i++) {
        arr[i * 2] = (char) ('A' + r.nextInt(10));
        arr[i * 2 + 1] = ' ';
      }
      return new String(arr);
    }

    public String getUTF8String(int nTokens) {
      int upto = 0;
      Arrays.fill(buffer, (char) 0);
      for (int i = 0; i < nTokens; i++) upto = addUTF8Token(upto);
      return new String(buffer, 0, upto);
    }

    public String getIdString() {
      return Integer.toString(base + nextInt(range));
    }

    public void indexDoc() throws IOException {
      Document d = new Document();

      FieldType customType1 = new FieldType(TextField.TYPE_STORED);
      customType1.setTokenized(false);
      customType1.setOmitNorms(true);

      ArrayList<Field> fields = new ArrayList<>();
      String idString = getIdString();
      Field idField = newField("id", idString, customType1);
      fields.add(idField);

      Map<String, FieldType> tvTypes = new HashMap<>();

      int nFields = nextInt(maxFields);
      for (int i = 0; i < nFields; i++) {

        String fieldName = "f" + nextInt(100);
        FieldType customType;

        // Use the same term vector settings if we already
        // added this field to the doc:
        FieldType oldTVType = tvTypes.get(fieldName);
        if (oldTVType != null) {
          customType = new FieldType(oldTVType);
        } else {
          customType = new FieldType();
          switch (nextInt(4)) {
            case 0:
              break;
            case 1:
              customType.setStoreTermVectors(true);
              break;
            case 2:
              customType.setStoreTermVectors(true);
              customType.setStoreTermVectorPositions(true);
              break;
            case 3:
              customType.setStoreTermVectors(true);
              customType.setStoreTermVectorOffsets(true);
              break;
          }
          FieldType newType = new FieldType(customType);
          newType.freeze();
          tvTypes.put(fieldName, newType);
        }

        switch (nextInt(4)) {
          case 0:
            customType.setStored(true);
            customType.setOmitNorms(true);
            customType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            customType.freeze();
            fields.add(newField(fieldName, getString(1), customType));
            break;
          case 1:
            customType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            customType.setTokenized(true);
            customType.freeze();
            fields.add(newField(fieldName, getString(0), customType));
            break;
          case 2:
            customType.setStored(true);
            customType.setStoreTermVectors(false);
            customType.setStoreTermVectorOffsets(false);
            customType.setStoreTermVectorPositions(false);
            customType.freeze();
            fields.add(newField(fieldName, getString(0), customType));
            break;
          case 3:
            customType.setStored(true);
            customType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            customType.setTokenized(true);
            customType.freeze();
            fields.add(newField(fieldName, getString(bigFieldSize), customType));
            break;
        }
      }

      if (sameFieldOrder) {
        Collections.sort(fields, fieldNameComparator);
      } else {
        // random placement of id field also
        Collections.swap(fields, nextInt(fields.size()), 0);
      }

      for (int i = 0; i < fields.size(); i++) {
        d.add(fields.get(i));
      }
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": indexing id:" + idString);
      }
      w.updateDocument(new Term("id", idString), d);
      // System.out.println(Thread.currentThread().getName() + ": indexing "+d);
      docs.put(idString, d);
    }

    public void deleteDoc() throws IOException {
      String idString = getIdString();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": del id:" + idString);
      }
      w.deleteDocuments(new Term("id", idString));
      docs.remove(idString);
    }

    public void deleteByQuery() throws IOException {
      String idString = getIdString();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": del query id:" + idString);
      }
      w.deleteDocuments(new TermQuery(new Term("id", idString)));
      docs.remove(idString);
    }

    @Override
    public void run() {
      try {
        r = new Random(base + range + seed);
        for (int i = 0; i < iterations; i++) {
          int what = nextInt(100);
          if (what < 5) {
            deleteDoc();
          } else if (what < 10) {
            deleteByQuery();
          } else {
            indexDoc();
          }
        }
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }

      synchronized (this) {
        docs.size();
      }
    }
  }
}
