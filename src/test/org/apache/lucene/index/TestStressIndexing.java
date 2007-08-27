package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
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

import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

import junit.framework.TestCase;

import java.util.Random;
import java.io.File;

public class TestStressIndexing extends TestCase {
  private static final Analyzer ANALYZER = new SimpleAnalyzer();
  private static final Random RANDOM = new Random();
  private static Searcher SEARCHER;

  private static int RUN_TIME_SEC = 15;

  private static class IndexerThread extends Thread {
    IndexWriter modifier;
    int nextID;
    public int count;
    boolean failed;

    public IndexerThread(IndexWriter modifier) {
      this.modifier = modifier;
    }

    public void run() {
      long stopTime = System.currentTimeMillis() + 1000*RUN_TIME_SEC;
      try {
        while(true) {

          if (System.currentTimeMillis() > stopTime) {
            break;
          }

          // Add 10 docs:
          for(int j=0; j<10; j++) {
            Document d = new Document();
            int n = RANDOM.nextInt();
            d.add(new Field("id", Integer.toString(nextID++), Field.Store.YES, Field.Index.UN_TOKENIZED));
            d.add(new Field("contents", English.intToEnglish(n), Field.Store.NO, Field.Index.TOKENIZED));
            modifier.addDocument(d);
          }

          // Delete 5 docs:
          int deleteID = nextID;
          for(int j=0; j<5; j++) {
            modifier.deleteDocuments(new Term("id", ""+deleteID));
            deleteID -= 2;
          }

          count++;
        }
        
      } catch (Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
        failed = true;
      }
    }
  }

  private static class SearcherThread extends Thread {
    private Directory directory;
    public int count;
    boolean failed;

    public SearcherThread(Directory directory) {
      this.directory = directory;
    }

    public void run() {
      long stopTime = System.currentTimeMillis() + 1000*RUN_TIME_SEC;
      try {
        while(true) {
          for (int i=0; i<100; i++) {
            (new IndexSearcher(directory)).close();
          }
          count += 100;
          if (System.currentTimeMillis() > stopTime) {
            break;
          }
        }
      } catch (Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
        failed = true;
      }
    }
  }

  /*
    Run one indexer and 2 searchers against single index as
    stress test.
  */
  public void runStressTest(Directory directory) throws Exception {
    IndexWriter modifier = new IndexWriter(directory, ANALYZER, true);

    // One modifier that writes 10 docs then removes 5, over
    // and over:
    IndexerThread indexerThread = new IndexerThread(modifier);
    indexerThread.start();
      
    IndexerThread indexerThread2 = new IndexerThread(modifier);
    indexerThread2.start();
      
    // Two searchers that constantly just re-instantiate the searcher:
    SearcherThread searcherThread1 = new SearcherThread(directory);
    searcherThread1.start();

    SearcherThread searcherThread2 = new SearcherThread(directory);
    searcherThread2.start();

    indexerThread.join();
    indexerThread2.join();
    searcherThread1.join();
    searcherThread2.join();

    modifier.close();

    assertTrue("hit unexpected exception in indexer", !indexerThread.failed);
    assertTrue("hit unexpected exception in indexer2", !indexerThread2.failed);
    assertTrue("hit unexpected exception in search1", !searcherThread1.failed);
    assertTrue("hit unexpected exception in search2", !searcherThread2.failed);
    //System.out.println("    Writer: " + indexerThread.count + " iterations");
    //System.out.println("Searcher 1: " + searcherThread1.count + " searchers created");
    //System.out.println("Searcher 2: " + searcherThread2.count + " searchers created");
  }

  /*
    Run above stress test against RAMDirectory and then
    FSDirectory.
  */
  public void testStressIndexAndSearching() throws Exception {

    // First in a RAM directory:
    Directory directory = new MockRAMDirectory();
    runStressTest(directory);
    directory.close();

    // Second in an FSDirectory:
    String tempDir = System.getProperty("java.io.tmpdir");
    File dirPath = new File(tempDir, "lucene.test.stress");
    directory = FSDirectory.getDirectory(dirPath);
    runStressTest(directory);
    directory.close();
    rmDir(dirPath);
  }

  private void rmDir(File dir) {
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
    dir.delete();
  }
}
