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


  public void testRandom() throws Exception {
    Directory dir1 = new RAMDirectory();
    // dir1 = FSDirectory.getDirectory("foofoofoo");
    Directory dir2 = new RAMDirectory();
    // mergeFactor=2; maxBufferedDocs=2; Map docs = indexRandom(1, 3, 2, dir1);
    Map docs = indexRandom(10, 100, 100, dir1);
    indexSerial(docs, dir2);

    // verifying verify
    // verifyEquals(dir1, dir1, "id");
    // verifyEquals(dir2, dir2, "id");

    verifyEquals(dir1, dir2, "id");
  }

  public void testMultiConfig() throws Exception {
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

      Directory dir1 = new RAMDirectory();
      Directory dir2 = new RAMDirectory();
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
    IndexWriter w = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true);
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

    Map docs = new HashMap();
    for (int i=0; i<threads.length; i++) {
      IndexingThread th = threads[i];
      synchronized(th) {
        docs.putAll(th.docs);
      }
    }

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

  public static void verifyEquals(Directory dir1, Directory dir2, String idField) throws IOException {
    IndexReader r1 = IndexReader.open(dir1);
    IndexReader r2 = IndexReader.open(dir2);
    verifyEquals(r1, r2, idField);
    r1.close();
    r2.close();
  }


  public static void verifyEquals(IndexReader r1, IndexReader r2, String idField) throws IOException {
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
      assertTrue(termDocs1.next());
      int id1 = termDocs1.doc();
      assertFalse(termDocs1.next());

      termDocs2.seek(termEnum);
      assertTrue(termDocs2.next());
      int id2 = termDocs2.doc();
      assertFalse(termDocs2.next());

      r2r1[id2] = id1;

      // verify stored fields are equivalent
     verifyEquals(r1.document(id1), r2.document(id2));

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

    public String getString(int nTokens) {
      nTokens = nTokens!=0 ? nTokens : r.nextInt(4)+1;
      // avoid StringBuffer because it adds extra synchronization.
      char[] arr = new char[nTokens*2];
      for (int i=0; i<nTokens; i++) {
        arr[i*2] = (char)('A' + r.nextInt(10));
        arr[i*2+1] = ' ';
      }
      return new String(arr);
    }


    public void indexDoc() throws IOException {
      Document d = new Document();

      ArrayList fields = new ArrayList();      
      int id = base + nextInt(range);
      String idString = ""+id;
      Field idField =  new Field("id", idString, Field.Store.YES, Field.Index.NO_NORMS);
      fields.add(idField);

      int nFields = nextInt(maxFields);
      for (int i=0; i<nFields; i++) {
        switch (nextInt(4)) {
          case 0:
            fields.add(new Field("f0", getString(1), Field.Store.YES, Field.Index.NO_NORMS));
            break;
          case 1:
            fields.add(new Field("f1", getString(0), Field.Store.NO, Field.Index.TOKENIZED));
            break;
          case 2:
            fields.add(new Field("f2", getString(0), Field.Store.YES, Field.Index.NO));
            break;
          case 3:
            fields.add(new Field("f3", getString(bigFieldSize), Field.Store.YES, Field.Index.TOKENIZED));
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

    public void run() {
      try {
        r = new Random(base+range+seed);
        for (int i=0; i<iterations; i++) {
          indexDoc();
        }
      } catch (Exception e) {
        e.printStackTrace();
        TestCase.fail(e.toString());
      }

      synchronized (this) {
        docs.size();
      }
    }
  }

}
