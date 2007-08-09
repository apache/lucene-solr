package org.apache.lucene.search;

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

import junit.framework.TestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;

import java.io.IOException;

/**
 *
 * @version $rcs = ' $Id$ ' ;
 */
public class TestMultiThreadTermVectors extends TestCase {
  private RAMDirectory directory = new RAMDirectory();
  public int numDocs = 100;
  public int numThreads = 3;
  
  public TestMultiThreadTermVectors(String s) {
    super(s);
  }
  
  public void setUp() throws Exception {
    IndexWriter writer
            = new IndexWriter(directory, new SimpleAnalyzer(), true);
    //writer.setUseCompoundFile(false);
    //writer.infoStream = System.out;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      Fieldable fld = new Field("field", English.intToEnglish(i), Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.YES);
      doc.add(fld);
      writer.addDocument(doc);
    }
    writer.close();
    
  }
  
  public void test() {
    
    IndexReader reader = null;
    
    try {
      reader = IndexReader.open(directory);
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
  
  public void testTermPositionVectors(final IndexReader reader, int threadCount) {
    MultiThreadTermVectorsReader[] mtr = new MultiThreadTermVectorsReader[threadCount];
    for (int i = 0; i < threadCount; i++) {
      mtr[i] = new MultiThreadTermVectorsReader();
      mtr[i].init(reader);
    }
    
    
    /** run until all threads finished */ 
    int threadsAlive = mtr.length;
    while (threadsAlive > 0) {
      try {
        //System.out.println("Threads alive");
        Thread.sleep(10);
        threadsAlive = mtr.length;
        for (int i = 0; i < mtr.length; i++) {
          if (mtr[i].isAlive() == true) {
            break;
          }
          
          threadsAlive--; 
          
      }
        
      } catch (InterruptedException ie) {} 
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
      TermFreqVector [] vectors = reader.getTermFreqVectors(docId);
      timeElapsed += System.currentTimeMillis()-start;
      
      // verify vectors result
      verifyVectors(vectors, docId);
      
      start = System.currentTimeMillis();
      TermFreqVector vector = reader.getTermFreqVector(docId, "field");
      timeElapsed += System.currentTimeMillis()-start;
      
      vectors = new TermFreqVector[1];
      vectors[0] = vector;
      
      verifyVectors(vectors, docId);
      
    }
  }
  
  private void verifyVectors(TermFreqVector[] vectors, int num) {
    StringBuffer temp = new StringBuffer();
    String[] terms = null;
    for (int i = 0; i < vectors.length; i++) {
      terms = vectors[i].getTerms();
      for (int z = 0; z < terms.length; z++) {
        temp.append(terms[z]);
      }
    }
    
    if (!English.intToEnglish(num).trim().equals(temp.toString().trim()))
        System.out.println("wrong term result");
  }
}
