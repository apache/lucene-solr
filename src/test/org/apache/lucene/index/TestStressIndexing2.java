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
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.search.TermQuery;

import java.util.*;
import java.io.IOException;

import junit.framework.TestCase;

public class TestStressIndexing2 extends LuceneTestCase {
  static int maxFields=4;
  static int bigFieldSize=10;
  static boolean sameFieldOrder=false;
  static boolean autoCommit=false;
  static int mergeFactor=3;
  static int maxBufferedDocs=3;
  static int seed=0;

  static Random r = new Random(0);

  public class MockIndexWriter extends IndexWriter {

    public MockIndexWriter(Directory dir, boolean autoCommit, Analyzer a, boolean create) throws IOException {
      super(dir, autoCommit, a, create);
    }

    boolean testPoint(String name) {
      //      if (name.equals("startCommit")) {
      if (r.nextInt(4) == 2)
        Thread.yield();
      return true;
    }
  }

  public void testRandom() throws Throwable {
    Directory dir1 = new MockRAMDirectory();
    // dir1 = FSDirectory.getDirectory("foofoofoo");
    Directory dir2 = new MockRAMDirectory();
    // mergeFactor=2; maxBufferedDocs=2; Map docs = indexRandom(1, 3, 2, dir1);
    Map docs = indexRandom(10, 100, 100, dir1);
    indexSerial(docs, dir2);

    // verifying verify
    // verifyEquals(dir1, dir1, "id");
    // verifyEquals(dir2, dir2, "id");

    verifyEquals(dir1, dir2, "id");
  }

  public void testMultiConfig() throws Throwable {
    // test lots of smaller different params together
    for (int i=0; i<100; i++) {  // increase iterations for better testing
      sameFieldOrder=r.nextBoolean();
      autoCommit=r.nextBoolean();
      mergeFactor=r.nextInt(3)+2;
      maxBufferedDocs=r.nextInt(3)+2;
      seed++;

      int nThreads=r.nextInt(5)+1;
      int iter=r.nextInt(10)+1;
      int range=r.nextInt(20)+1;
      Directory dir1 = new MockRAMDirectory();
      Directory dir2 = new MockRAMDirectory();
      Map docs = indexRandom(nThreads, iter, range, dir1);
      indexSerial(docs, dir2);
      verifyEquals(dir1, dir2, "id");
    }
  }


  static Term idTerm = new Term("id","");
  IndexingThread[] threads;
  static Comparator fieldNameComparator = new Comparator() {
        public int compare(Object o1, Object o2) {
          return ((Fieldable)o1).name().compareTo(((Fieldable)o2).name());
        }
  };

  // This test avoids using any extra synchronization in the multiple
  // indexing threads to test that IndexWriter does correctly synchronize
  // everything.

  public Map indexRandom(int nThreads, int iterations, int range, Directory dir) throws IOException, InterruptedException {
    Map docs = new HashMap();
    for(int iter=0;iter<3;iter++) {
      IndexWriter w = new MockIndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true);
      w.setUseCompoundFile(false);

      /***
          w.setMaxMergeDocs(Integer.MAX_VALUE);
          w.setMaxFieldLength(10000);
          w.setRAMBufferSizeMB(1);
          w.setMergeFactor(10);
      ***/

      // force many merges
      w.setMergeFactor(mergeFactor);
      w.setRAMBufferSizeMB(.1);
      w.setMaxBufferedDocs(maxBufferedDocs);

      threads = new IndexingThread[nThreads];
      for (int i=0; i<threads.length; i++) {
        IndexingThread th = new IndexingThread();
        th.w = w;
        th.base = 1000000*i;
        th.range = range;
        th.iterations = iterations;
        threads[i] = th;
      }

      for (int i=0; i<threads.length; i++) {
        threads[i].start();
      }
      for (int i=0; i<threads.length; i++) {
        threads[i].join();
      }

      // w.optimize();
      w.close();    

      for (int i=0; i<threads.length; i++) {
        IndexingThread th = threads[i];
        synchronized(th) {
          docs.putAll(th.docs);
        }
      }
    }

    _TestUtil.checkIndex(dir);

    return docs;
  }

  
  public static void indexSerial(Map docs, Directory dir) throws IOException {
    IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);

    // index all docs in a single thread
    Iterator iter = docs.values().iterator();
    while (iter.hasNext()) {
      Document d = (Document)iter.next();
      ArrayList fields = new ArrayList();
      fields.addAll(d.getFields());
      // put fields in same order each time
      Collections.sort(fields, fieldNameComparator);
      
      Document d1 = new Document();
      d1.setBoost(d.getBoost());
      for (int i=0; i<fields.size(); i++) {
        d1.add((Fieldable) fields.get(i));
      }
      w.addDocument(d1);
      // System.out.println("indexing "+d1);
    }
    
    w.close();
  }

  public static void verifyEquals(Directory dir1, Directory dir2, String idField) throws Throwable {
    IndexReader r1 = IndexReader.open(dir1);
    IndexReader r2 = IndexReader.open(dir2);
    verifyEquals(r1, r2, idField);
    r1.close();
    r2.close();
  }


  public static void verifyEquals(IndexReader r1, IndexReader r2, String idField) throws Throwable {
    assertEquals(r1.numDocs(), r2.numDocs());
    boolean hasDeletes = !(r1.maxDoc()==r2.maxDoc() && r1.numDocs()==r1.maxDoc());

    int[] r2r1 = new int[r2.maxDoc()];   // r2 id to r1 id mapping

    TermDocs termDocs1 = r1.termDocs();
    TermDocs termDocs2 = r2.termDocs();

    // create mapping from id2 space to id2 based on idField
    idField = idField.intern();
    TermEnum termEnum = r1.terms (new Term (idField, ""));
    do {
      Term term = termEnum.term();
      if (term==null || term.field() != idField) break;

      termDocs1.seek (termEnum);
      if (!termDocs1.next()) {
        // This doc is deleted and wasn't replaced
        termDocs2.seek(termEnum);
        assertFalse(termDocs2.next());
        continue;
      }

      int id1 = termDocs1.doc();
      assertFalse(termDocs1.next());

      termDocs2.seek(termEnum);
      assertTrue(termDocs2.next());
      int id2 = termDocs2.doc();
      assertFalse(termDocs2.next());

      r2r1[id2] = id1;

      // verify stored fields are equivalent
      verifyEquals(r1.document(id1), r2.document(id2));

      try {
        // verify term vectors are equivalent        
        verifyEquals(r1.getTermFreqVectors(id1), r2.getTermFreqVectors(id2));
      } catch (Throwable e) {
        System.out.println("FAILED id=" + term + " id1=" + id1 + " id2=" + id2);
        TermFreqVector[] tv1 = r1.getTermFreqVectors(id1);
        System.out.println("  d1=" + tv1);
        if (tv1 != null)
          for(int i=0;i<tv1.length;i++)
            System.out.println("    " + i + ": " + tv1[i]);
        
        TermFreqVector[] tv2 = r2.getTermFreqVectors(id2);
        System.out.println("  d2=" + tv2);
        if (tv2 != null)
          for(int i=0;i<tv2.length;i++)
            System.out.println("    " + i + ": " + tv2[i]);
        
        throw e;
      }

    } while (termEnum.next());

    termEnum.close();

    // Verify postings
    TermEnum termEnum1 = r1.terms (new Term ("", ""));
    TermEnum termEnum2 = r2.terms (new Term ("", ""));

    // pack both doc and freq into single element for easy sorting
    long[] info1 = new long[r1.numDocs()];
    long[] info2 = new long[r2.numDocs()];

    for(;;) {
      Term term1,term2;

      // iterate until we get some docs
      int len1;
      for(;;) {
        len1=0;
        term1 = termEnum1.term();
        if (term1==null) break;
        termDocs1.seek(termEnum1);
        while (termDocs1.next()) {
          int d1 = termDocs1.doc();
          int f1 = termDocs1.freq();
          info1[len1] = (((long)d1)<<32) | f1;
          len1++;
        }
        if (len1>0) break;
        if (!termEnum1.next()) break;
      }

       // iterate until we get some docs
      int len2;
      for(;;) {
        len2=0;
        term2 = termEnum2.term();
        if (term2==null) break;
        termDocs2.seek(termEnum2);
        while (termDocs2.next()) {
          int d2 = termDocs2.doc();
          int f2 = termDocs2.freq();
          info2[len2] = (((long)r2r1[d2])<<32) | f2;
          len2++;
        }
        if (len2>0) break;
        if (!termEnum2.next()) break;
      }

      if (!hasDeletes)
        assertEquals(termEnum1.docFreq(), termEnum2.docFreq());

      assertEquals(len1, len2);
      if (len1==0) break;  // no more terms

      assertEquals(term1, term2);

      // sort info2 to get it into ascending docid
      Arrays.sort(info2, 0, len2);

      // now compare
      for (int i=0; i<len1; i++) {
        assertEquals(info1[i], info2[i]);
      }

      termEnum1.next();
      termEnum2.next();
    }
  }

  public static void verifyEquals(Document d1, Document d2) {
    List ff1 = d1.getFields();
    List ff2 = d2.getFields();

    Collections.sort(ff1, fieldNameComparator);
    Collections.sort(ff2, fieldNameComparator);

    if (ff1.size() != ff2.size()) {
      System.out.println(ff1);
      System.out.println(ff2);
      assertEquals(ff1.size(), ff2.size());
    }


    for (int i=0; i<ff1.size(); i++) {
      Fieldable f1 = (Fieldable)ff1.get(i);
      Fieldable f2 = (Fieldable)ff2.get(i);
      if (f1.isBinary()) {
        assert(f2.isBinary());
        //TODO
      } else {
        String s1 = f1.stringValue();
        String s2 = f2.stringValue();
        if (!s1.equals(s2)) {
          // print out whole doc on error
          System.out.println(ff1);
          System.out.println(ff2);          
          assertEquals(s1,s2);
        }
      }
    }
  }

  public static void verifyEquals(TermFreqVector[] d1, TermFreqVector[] d2) {
    if (d1 == null) {
      assertTrue(d2 == null);
      return;
    }
    assertTrue(d2 != null);

    assertEquals(d1.length, d2.length);
    for(int i=0;i<d1.length;i++) {
      TermFreqVector v1 = d1[i];
      TermFreqVector v2 = d2[i];
      if (v1 == null || v2 == null)
        System.out.println("v1=" + v1 + " v2=" + v2 + " i=" + i + " of " + d1.length);
      assertEquals(v1.size(), v2.size());
      int numTerms = v1.size();
      String[] terms1 = v1.getTerms();
      String[] terms2 = v2.getTerms();
      int[] freq1 = v1.getTermFrequencies();
      int[] freq2 = v2.getTermFrequencies();
      for(int j=0;j<numTerms;j++) {
        if (!terms1[j].equals(terms2[j]))
          assertEquals(terms1[j], terms2[j]);
        assertEquals(freq1[j], freq2[j]);
      }
      if (v1 instanceof TermPositionVector) {
        assertTrue(v2 instanceof TermPositionVector);
        TermPositionVector tpv1 = (TermPositionVector) v1;
        TermPositionVector tpv2 = (TermPositionVector) v2;
        for(int j=0;j<numTerms;j++) {
          int[] pos1 = tpv1.getTermPositions(j);
          int[] pos2 = tpv2.getTermPositions(j);
          assertEquals(pos1.length, pos2.length);
          TermVectorOffsetInfo[] offsets1 = tpv1.getOffsets(j);
          TermVectorOffsetInfo[] offsets2 = tpv2.getOffsets(j);
          if (offsets1 == null)
            assertTrue(offsets2 == null);
          else
            assertTrue(offsets2 != null);
          for(int k=0;k<pos1.length;k++) {
            assertEquals(pos1[k], pos2[k]);
            if (offsets1 != null) {
              assertEquals(offsets1[k].getStartOffset(),
                           offsets2[k].getStartOffset());
              assertEquals(offsets1[k].getEndOffset(),
                           offsets2[k].getEndOffset());
            }
          }
        }
      }
    }
  }

  private static class IndexingThread extends Thread {
    IndexWriter w;
    int base;
    int range;
    int iterations;
    Map docs = new HashMap();  // Map<String,Document>
    Random r;

    public int nextInt(int lim) {
      return r.nextInt(lim);
    }

    // start is inclusive and end is exclusive
    public int nextInt(int start, int end) {
      return start + r.nextInt(end-start);
    }

    char[] buffer = new char[100];

    private int addUTF8Token(int start) {
      final int end = start + nextInt(20);
      if (buffer.length < 1+end) {
        char[] newBuffer = new char[(int) ((1+end)*1.25)];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        buffer = newBuffer;
      }

      for(int i=start;i<end;i++) {
        int t = nextInt(6);
        if (0 == t && i < end-1) {
          // Make a surrogate pair
          // High surrogate
          buffer[i++] = (char) nextInt(0xd800, 0xdc00);
          // Low surrogate
          buffer[i] = (char) nextInt(0xdc00, 0xe000);
        } else if (t <= 1)
          buffer[i] = (char) nextInt(0x80);
        else if (2 == t)
          buffer[i] = (char) nextInt(0x80, 0x800);
        else if (3 == t)
          buffer[i] = (char) nextInt(0x800, 0xd800);
        else if (4 == t)
          buffer[i] = (char) nextInt(0xe000, 0xffff);
        else if (5 == t) {
          // Illegal unpaired surrogate
          if (r.nextBoolean())
            buffer[i] = (char) nextInt(0xd800, 0xdc00);
          else
            buffer[i] = (char) nextInt(0xdc00, 0xe000);
        }
      }
      buffer[end] = ' ';
      return 1+end;
    }

    public String getString(int nTokens) {
      nTokens = nTokens!=0 ? nTokens : r.nextInt(4)+1;

      // Half the time make a random UTF8 string
      if (r.nextBoolean())
        return getUTF8String(nTokens);

      // avoid StringBuffer because it adds extra synchronization.
      char[] arr = new char[nTokens*2];
      for (int i=0; i<nTokens; i++) {
        arr[i*2] = (char)('A' + r.nextInt(10));
        arr[i*2+1] = ' ';
      }
      return new String(arr);
    }
    
    public String getUTF8String(int nTokens) {
      int upto = 0;
      Arrays.fill(buffer, (char) 0);
      for(int i=0;i<nTokens;i++)
        upto = addUTF8Token(upto);
      return new String(buffer, 0, upto);
    }

    public String getIdString() {
      return Integer.toString(base + nextInt(range));
    }

    public void indexDoc() throws IOException {
      Document d = new Document();

      ArrayList fields = new ArrayList();      
      String idString = getIdString();
      Field idField =  new Field(idTerm.field(), idString, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
      fields.add(idField);

      int nFields = nextInt(maxFields);
      for (int i=0; i<nFields; i++) {

        Field.TermVector tvVal = Field.TermVector.NO;
        switch (nextInt(4)) {
        case 0:
          tvVal = Field.TermVector.NO;
          break;
        case 1:
          tvVal = Field.TermVector.YES;
          break;
        case 2:
          tvVal = Field.TermVector.WITH_POSITIONS;
          break;
        case 3:
          tvVal = Field.TermVector.WITH_POSITIONS_OFFSETS;
          break;
        }
        
        switch (nextInt(4)) {
          case 0:
            fields.add(new Field("f" + nextInt(100), getString(1), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, tvVal));
            break;
          case 1:
            fields.add(new Field("f" + nextInt(100), getString(0), Field.Store.NO, Field.Index.ANALYZED, tvVal));
            break;
          case 2:
            fields.add(new Field("f" + nextInt(100), getString(0), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
            break;
          case 3:
            fields.add(new Field("f" + nextInt(100), getString(bigFieldSize), Field.Store.YES, Field.Index.ANALYZED, tvVal));
            break;          
        }
      }

      if (sameFieldOrder) {
        Collections.sort(fields, fieldNameComparator);
      } else {
        // random placement of id field also
        Collections.swap(fields,nextInt(fields.size()), 0);
      }

      for (int i=0; i<fields.size(); i++) {
        d.add((Fieldable) fields.get(i));
      }
      w.updateDocument(idTerm.createTerm(idString), d);
      // System.out.println("indexing "+d);
      docs.put(idString, d);
    }

    public void deleteDoc() throws IOException {
      String idString = getIdString();
      w.deleteDocuments(idTerm.createTerm(idString));
      docs.remove(idString);
    }

    public void deleteByQuery() throws IOException {
      String idString = getIdString();
      w.deleteDocuments(new TermQuery(idTerm.createTerm(idString)));
      docs.remove(idString);
    }

    public void run() {
      try {
        r = new Random(base+range+seed);
        for (int i=0; i<iterations; i++) {
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
        e.printStackTrace();
        TestCase.fail(e.toString());
      }

      synchronized (this) {
        docs.size();
      }
    }
  }

}
