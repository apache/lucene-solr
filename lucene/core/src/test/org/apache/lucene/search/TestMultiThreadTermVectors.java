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
package org.apache.lucene.search;


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
  private int numDocs;
  private int numThreads;
  private int numIterations;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    numDocs = TEST_NIGHTLY ? 1000 : 50;
    numThreads = TEST_NIGHTLY ? 3 : 2;
    numIterations = TEST_NIGHTLY ? 100 : 50;
    directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
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
    try (IndexReader reader = DirectoryReader.open(directory)) {
      testTermPositionVectors(reader, numThreads);
    }
  }
  
  public void testTermPositionVectors(final IndexReader reader, int threadCount) throws Exception {
    MultiThreadTermVectorsReader[] mtr = new MultiThreadTermVectorsReader[threadCount];
    for (int i = 0; i < threadCount; i++) {
      mtr[i] = new MultiThreadTermVectorsReader();
      mtr[i].init(reader);
    }

    for (MultiThreadTermVectorsReader vectorReader : mtr) {
      vectorReader.start();
    }

    for (MultiThreadTermVectorsReader vectorReader : mtr) {
      vectorReader.join();
    }
  }
  
  static void verifyVectors(Fields vectors, int num) throws IOException {
    for (String field : vectors) {
      Terms terms = vectors.terms(field);
      assert terms != null;
      verifyVector(terms.iterator(), num);
    }
  }

  static void verifyVector(TermsEnum vector, int num) throws IOException {
    StringBuilder temp = new StringBuilder();
    while(vector.next() != null) {
      temp.append(vector.term().utf8ToString());
    }
    assertEquals(English.intToEnglish(num).trim(), temp.toString().trim());
  }

  class MultiThreadTermVectorsReader extends Thread {
    private IndexReader reader = null;  

    public void init(IndexReader reader) {
      this.reader = reader;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < numIterations; i++) {
          testTermVectors();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void testTermVectors() throws Exception {
      // check:
      int numDocs = reader.numDocs();
      for (int docId = 0; docId < numDocs; docId++) {
        Fields vectors = reader.getTermVectors(docId);      
        // verify vectors result
        verifyVectors(vectors, docId);
        Terms vector = reader.getTermVectors(docId).terms("field");
        verifyVector(vector.iterator(), docId);
      }
    }
  }
}
