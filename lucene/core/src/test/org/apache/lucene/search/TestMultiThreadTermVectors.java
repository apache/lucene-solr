package org.apache.lucene.search;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

public class TestMultiThreadTermVectors extends LuceneTestCase {
  private Directory directory;
  public int numDocs = 100;
  public int numThreads = 3;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    //writer.setUseCompoundFile(false);
    //writer.infoStream = System.out;
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    customType.setStoreTermVectors(true);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      Field fld = newField("field", English.intToEnglish(i), customType);
      doc.add(fld);
      writer.addDocument(doc);
    }
    writer.close();
    
  }
  
  @Override
  public void tearDown() throws Exception {
    directory.close();
    super.tearDown();
  }
  
  public void test() throws Exception {
    
    IndexReader reader = null;
    
    try {
      reader = DirectoryReader.open(directory);
      for(int i = 1; i <= numThreads; i++)
        testTermPositionVectors(reader, i);
      
      
    }
    catch (IOException ioe) {
      fail(ioe.getMessage());
    }
    finally {
      if (reader != null) {
        try {
          /** close the opened reader */
          reader.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    }
  }
  
  public void testTermPositionVectors(final IndexReader reader, int threadCount) throws Exception {
    MultiThreadTermVectorsReader[] mtr = new MultiThreadTermVectorsReader[threadCount];
    for (int i = 0; i < threadCount; i++) {
      mtr[i] = new MultiThreadTermVectorsReader();
      mtr[i].init(reader);
    }
    
    
    /** run until all threads finished */ 
    int threadsAlive = mtr.length;
    while (threadsAlive > 0) {
        //System.out.println("Threads alive");
        Thread.sleep(10);
        threadsAlive = mtr.length;
        for (int i = 0; i < mtr.length; i++) {
          if (mtr[i].isAlive() == true) {
            break;
          }
          
          threadsAlive--; 
        }
    }
    
    long totalTime = 0L;
    for (int i = 0; i < mtr.length; i++) {
      totalTime += mtr[i].timeElapsed;
      mtr[i] = null;
    }
    
    //System.out.println("threadcount: " + mtr.length + " average term vector time: " + totalTime/mtr.length);
    
  }
  
}

class MultiThreadTermVectorsReader implements Runnable {
  
  private IndexReader reader = null;
  private Thread t = null;
  
  private final int runsToDo = 100;
  long timeElapsed = 0;
  
  
  public void init(IndexReader reader) {
    this.reader = reader;
    timeElapsed = 0;
    t=new Thread(this);
    t.start();
  }
    
  public boolean isAlive() {
    if (t == null) return false;
    
    return t.isAlive();
  }
  
  @Override
  public void run() {
      try {
        // run the test 100 times
        for (int i = 0; i < runsToDo; i++)
          testTermVectors();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      return;
  }
  
  private void testTermVectors() throws Exception {
    // check:
    int numDocs = reader.numDocs();
    long start = 0L;
    for (int docId = 0; docId < numDocs; docId++) {
      start = System.currentTimeMillis();
      Fields vectors = reader.getTermVectors(docId);
      timeElapsed += System.currentTimeMillis()-start;
      
      // verify vectors result
      verifyVectors(vectors, docId);
      
      start = System.currentTimeMillis();
      Terms vector = reader.getTermVectors(docId).terms("field");
      timeElapsed += System.currentTimeMillis()-start;
      
      verifyVector(vector.iterator(null), docId);
    }
  }
  
  private void verifyVectors(Fields vectors, int num) throws IOException {
    for (String field : vectors) {
      Terms terms = vectors.terms(field);
      assert terms != null;
      verifyVector(terms.iterator(null), num);
    }
  }

  private void verifyVector(TermsEnum vector, int num) throws IOException {
    StringBuilder temp = new StringBuilder();
    while(vector.next() != null) {
      temp.append(vector.term().utf8ToString());
    }
    if (!English.intToEnglish(num).trim().equals(temp.toString().trim()))
        System.out.println("wrong term result");
  }
}
