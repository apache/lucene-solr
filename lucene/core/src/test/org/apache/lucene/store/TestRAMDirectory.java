package org.apache.lucene.store;

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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Field;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.English;

/**
 * JUnit testcase to test RAMDirectory. RAMDirectory itself is used in many testcases,
 * but not one of them uses an different constructor other than the default constructor.
 */
public class TestRAMDirectory extends LuceneTestCase {
  
  private File indexDir = null;
  
  // add enough document so that the index will be larger than RAMDirectory.READ_BUFFER_SIZE
  private final int docsToAdd = 500;
  
  // setup the index
  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexDir = _TestUtil.getTempDir("RAMDirIndex");
    
    Directory dir = newFSDirectory(indexDir);
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
    // add some documents
    Document doc = null;
    for (int i = 0; i < docsToAdd; i++) {
      doc = new Document();
      doc.add(newStringField("content", English.intToEnglish(i).trim(), Field.Store.YES));
      writer.addDocument(doc);
    }
    assertEquals(docsToAdd, writer.maxDoc());
    writer.close();
    dir.close();
  }
  
  public void testRAMDirectory () throws IOException {
    
    Directory dir = newFSDirectory(indexDir);
    MockDirectoryWrapper ramDir = new MockDirectoryWrapper(random(), new RAMDirectory(dir, newIOContext(random())));
    
    // close the underlaying directory
    dir.close();
    
    // Check size
    assertEquals(ramDir.sizeInBytes(), ramDir.getRecomputedSizeInBytes());
    
    // open reader to test document count
    IndexReader reader = DirectoryReader.open(ramDir);
    assertEquals(docsToAdd, reader.numDocs());
    
    // open search zo check if all doc's are there
    IndexSearcher searcher = newSearcher(reader);
    
    // search for all documents
    for (int i = 0; i < docsToAdd; i++) {
      Document doc = searcher.doc(i);
      assertTrue(doc.getField("content") != null);
    }

    // cleanup
    reader.close();
  }
  
  private final int numThreads = 10;
  private final int docsPerThread = 40;
  
  public void testRAMDirectorySize() throws IOException, InterruptedException {
      
    Directory dir = newFSDirectory(indexDir);
    final MockDirectoryWrapper ramDir = new MockDirectoryWrapper(random(), new RAMDirectory(dir, newIOContext(random())));
    dir.close();
    
    final IndexWriter writer = new IndexWriter(ramDir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    
    assertEquals(ramDir.sizeInBytes(), ramDir.getRecomputedSizeInBytes());
    
    Thread[] threads = new Thread[numThreads];
    for (int i=0; i<numThreads; i++) {
      final int num = i;
      threads[i] = new Thread(){
        @Override
        public void run() {
          for (int j=1; j<docsPerThread; j++) {
            Document doc = new Document();
            doc.add(newStringField("sizeContent", English.intToEnglish(num*docsPerThread+j).trim(), Field.Store.YES));
            try {
              writer.addDocument(doc);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }
    for (int i=0; i<numThreads; i++)
      threads[i].start();
    for (int i=0; i<numThreads; i++)
      threads[i].join();

    writer.forceMerge(1);
    assertEquals(ramDir.sizeInBytes(), ramDir.getRecomputedSizeInBytes());
    
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    // cleanup 
    if (indexDir != null && indexDir.exists()) {
      rmDir (indexDir);
    }
    super.tearDown();
  }

  // LUCENE-1196
  public void testIllegalEOF() throws Exception {
    RAMDirectory dir = new RAMDirectory();
    IndexOutput o = dir.createOutput("out", newIOContext(random()));
    byte[] b = new byte[1024];
    o.writeBytes(b, 0, 1024);
    o.close();
    IndexInput i = dir.openInput("out", newIOContext(random()));
    i.seek(1024);
    i.close();
    dir.close();
  }
  
  private void rmDir(File dir) {
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
    dir.delete();
  }

  // LUCENE-2852
  public void testSeekToEOFThenBack() throws Exception {
    RAMDirectory dir = new RAMDirectory();

    IndexOutput o = dir.createOutput("out", newIOContext(random()));
    byte[] bytes = new byte[3*RAMInputStream.BUFFER_SIZE];
    o.writeBytes(bytes, 0, bytes.length);
    o.close();

    IndexInput i = dir.openInput("out", newIOContext(random()));
    i.seek(2*RAMInputStream.BUFFER_SIZE-1);
    i.seek(3*RAMInputStream.BUFFER_SIZE);
    i.seek(RAMInputStream.BUFFER_SIZE);
    i.readBytes(bytes, 0, 2*RAMInputStream.BUFFER_SIZE);
    i.close();
    dir.close();
  }
}
