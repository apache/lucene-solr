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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.util.EmptyStackException;
import java.util.Random;
import java.util.Stack;

/**
 * Tests for the "IndexModifier" class, including accesses from two threads at the
 * same time.
 *
 * @deprecated
 */
public class TestIndexModifier extends LuceneTestCase {

  private int docCount = 0;
  
  private final Term allDocTerm = new Term("all", "x");

  public void testIndex() throws IOException {
    Directory ramDir = new RAMDirectory();
    IndexModifier i = new IndexModifier(ramDir, new StandardAnalyzer(), true);
    i.addDocument(getDoc());
    assertEquals(1, i.docCount());
    i.flush();
    i.addDocument(getDoc(), new SimpleAnalyzer());
    assertEquals(2, i.docCount());
    i.optimize();
    assertEquals(2, i.docCount());
    i.flush();
    i.deleteDocument(0);
    assertEquals(1, i.docCount());
    i.flush();
    assertEquals(1, i.docCount());
    i.addDocument(getDoc());
    i.addDocument(getDoc());
    i.flush();
    // depend on merge policy - assertEquals(3, i.docCount());
    i.deleteDocuments(allDocTerm);
    assertEquals(0, i.docCount());
    i.optimize();
    assertEquals(0, i.docCount());
    
    //  Lucene defaults:
    assertNull(i.getInfoStream());
    assertTrue(i.getUseCompoundFile());
    assertEquals(IndexWriter.DISABLE_AUTO_FLUSH, i.getMaxBufferedDocs());
    assertEquals(10000, i.getMaxFieldLength());
    assertEquals(10, i.getMergeFactor());
    // test setting properties:
    i.setMaxBufferedDocs(100);
    i.setMergeFactor(25);
    i.setMaxFieldLength(250000);
    i.addDocument(getDoc());
    i.setUseCompoundFile(false);
    i.flush();
    assertEquals(100, i.getMaxBufferedDocs());
    assertEquals(25, i.getMergeFactor());
    assertEquals(250000, i.getMaxFieldLength());
    assertFalse(i.getUseCompoundFile());

    // test setting properties when internally the reader is opened:
    i.deleteDocuments(allDocTerm);
    i.setMaxBufferedDocs(100);
    i.setMergeFactor(25);
    i.setMaxFieldLength(250000);
    i.addDocument(getDoc());
    i.setUseCompoundFile(false);
    i.optimize();
    assertEquals(100, i.getMaxBufferedDocs());
    assertEquals(25, i.getMergeFactor());
    assertEquals(250000, i.getMaxFieldLength());
    assertFalse(i.getUseCompoundFile());

    i.close();
    try {
      i.docCount();
      fail();
    } catch (IllegalStateException e) {
      // expected exception
    }
  }

  public void testExtendedIndex() throws IOException {
    Directory ramDir = new RAMDirectory();
    PowerIndex powerIndex = new PowerIndex(ramDir, new StandardAnalyzer(), true);
    powerIndex.addDocument(getDoc());
    powerIndex.addDocument(getDoc());
    powerIndex.addDocument(getDoc());
    powerIndex.addDocument(getDoc());
    powerIndex.addDocument(getDoc());
    powerIndex.flush();
    assertEquals(5, powerIndex.docFreq(allDocTerm));
    powerIndex.close();
  }
  
  private Document getDoc() {
    Document doc = new Document();
    doc.add(new Field("body", Integer.toString(docCount), Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("all", "x", Field.Store.YES, Field.Index.NOT_ANALYZED));
    docCount++;
    return doc;
  }
  
  public void testIndexWithThreads() throws IOException {
    testIndexInternal(0);
    testIndexInternal(10);
    testIndexInternal(50);
  }
  
  private void testIndexInternal(int maxWait) throws IOException {
    final boolean create = true;
    //Directory rd = new RAMDirectory();
    // work on disk to make sure potential lock problems are tested:
    String tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null)
      throw new IOException("java.io.tmpdir undefined, cannot run test");
    File indexDir = new File(tempDir, "lucenetestindex");
    Directory rd = FSDirectory.getDirectory(indexDir);
    IndexThread.id = 0;
    IndexThread.idStack.clear();
    IndexModifier index = new IndexModifier(rd, new StandardAnalyzer(), create);
    IndexThread thread1 = new IndexThread(index, maxWait, 1);
    thread1.start();
    IndexThread thread2 = new IndexThread(index, maxWait, 2);
    thread2.start();
    while(thread1.isAlive() || thread2.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    index.optimize();
    int added = thread1.added + thread2.added;
    int deleted = thread1.deleted + thread2.deleted;
    assertEquals(added-deleted, index.docCount());
    index.close();
    
    try {
      index.close();
      fail();
    } catch(IllegalStateException e) {
      // expected exception
    }
    rmDir(indexDir);
  }
  
  private void rmDir(File dir) {
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
    dir.delete();
  }
  
  private class PowerIndex extends IndexModifier {
    public PowerIndex(Directory dir, Analyzer analyzer, boolean create) throws IOException {
      super(dir, analyzer, create);
    }
    public int docFreq(Term term) throws IOException {
      synchronized(directory) {
        assureOpen();
        createIndexReader();
        return indexReader.docFreq(term);
      }
    }
  }
  
}

class IndexThread extends Thread {

  private final static int TEST_SECONDS = 3;       // how many seconds to run each test 

  static int id = 0;
  static Stack idStack = new Stack();

  int added = 0;
  int deleted = 0;

  private int maxWait = 10;
  private IndexModifier index;
  private int threadNumber;
  private Random random;
  
  IndexThread(IndexModifier index, int maxWait, int threadNumber) {
    this.index = index;
    this.maxWait = maxWait;
    this.threadNumber = threadNumber;
    // TODO: test case is not reproducible despite pseudo-random numbers:
    random = new Random(101+threadNumber);        // constant seed for better reproducability
  }
  
  public void run() {

    final long endTime = System.currentTimeMillis() + 1000*TEST_SECONDS;
    try {
      while(System.currentTimeMillis() < endTime) {
        int rand = random.nextInt(101);
        if (rand < 5) {
          index.optimize();
        } else if (rand < 60) {
          Document doc = getDocument();
          index.addDocument(doc);
          idStack.push(doc.get("id"));
          added++;
        } else {
          // we just delete the last document added and remove it
          // from the id stack so that it won't be removed twice:
          String delId = null;
          try {
            delId = (String)idStack.pop();
          } catch (EmptyStackException e) {
            continue;
          }
          Term delTerm = new Term("id", new Integer(delId).toString());
          int delCount = index.deleteDocuments(delTerm);
          if (delCount != 1) {
            throw new RuntimeException("Internal error: " + threadNumber + " deleted " + delCount + 
                " documents, term=" + delTerm);
          }
          deleted++;
        }
        if (maxWait > 0) {
          try {
            rand = random.nextInt(maxWait);
            //System.out.println("waiting " + rand + "ms");
            Thread.sleep(rand);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Document getDocument() {
    Document doc = new Document();
    synchronized (getClass()) {
      doc.add(new Field("id", Integer.toString(id), Field.Store.YES,
          Field.Index.NOT_ANALYZED));
      id++;
    }
    // add random stuff:
    doc.add(new Field("content", Integer.toString(random.nextInt(1000)), Field.Store.YES,
        Field.Index.ANALYZED));
    doc.add(new Field("content", Integer.toString(random.nextInt(1000)), Field.Store.YES,
        Field.Index.ANALYZED));
    doc.add(new Field("all", "x", Field.Store.YES, Field.Index.ANALYZED));
    return doc;
  }
  
}
