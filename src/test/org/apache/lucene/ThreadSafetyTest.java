package org.apache.lucene;

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

import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

import java.util.Random;
import java.io.File;

class ThreadSafetyTest {
  private static final Analyzer ANALYZER = new SimpleAnalyzer();
  private static final Random RANDOM = new Random();
  private static Searcher SEARCHER;

  private static int ITERATIONS = 1;

  private static int random(int i) {		  // for JDK 1.1 compatibility
    int r = RANDOM.nextInt();
    if (r < 0) r = -r;
    return r % i;
  }

  private static class IndexerThread extends Thread {
    private final int reopenInterval = 30 + random(60);
    IndexWriter writer;

    public IndexerThread(IndexWriter writer) {
      this.writer = writer;
    }

    public void run() {
      try {
        boolean useCompoundFiles = false;
        
        for (int i = 0; i < 1024*ITERATIONS; i++) {
          Document d = new Document();
          int n = RANDOM.nextInt();
          d.add(new Field("id", Integer.toString(n), Field.Store.YES, Field.Index.NOT_ANALYZED));
          d.add(new Field("contents", English.intToEnglish(n), Field.Store.NO, Field.Index.ANALYZED));
          System.out.println("Adding " + n);
          
          // Switch between single and multiple file segments
          useCompoundFiles = Math.random() < 0.5;
          writer.setUseCompoundFile(useCompoundFiles);
          
          writer.addDocument(d);

          if (i%reopenInterval == 0) {
            writer.close();
            writer = new IndexWriter("index", ANALYZER, false, IndexWriter.MaxFieldLength.LIMITED);
          }
        }
        
        writer.close();

      } catch (Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
        System.exit(0);
      }
    }
  }

  private static class SearcherThread extends Thread {
    private IndexSearcher searcher;
    private final int reopenInterval = 10 + random(20);

    public SearcherThread(boolean useGlobal) throws java.io.IOException {
      if (!useGlobal)
        this.searcher = new IndexSearcher("index");
    }

    public void run() {
      try {
        for (int i = 0; i < 512*ITERATIONS; i++) {
          searchFor(RANDOM.nextInt(), (searcher==null)?SEARCHER:searcher);
          if (i%reopenInterval == 0) {
            if (searcher == null) {
              SEARCHER = new IndexSearcher("index");
            } else {
              searcher.close();
              searcher = new IndexSearcher("index");
            }
          }
        }
      } catch (Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
        System.exit(0);
      }
    }

    private void searchFor(int n, Searcher searcher)
      throws Exception {
      System.out.println("Searching for " + n);
        QueryParser parser = new QueryParser("contents", ANALYZER);
      ScoreDoc[] hits =
        searcher.search(parser.parse(English.intToEnglish(n)), null, 1000).scoreDocs;
      System.out.println("Search for " + n + ": total=" + hits.length);
      for (int j = 0; j < Math.min(3, hits.length); j++) {
        System.out.println("Hit for " + n + ": " + searcher.doc(hits[j].doc).get("id"));
      }
    }
  }

  public static void main(String[] args) throws Exception {

    boolean readOnly = false;
    boolean add = false;

    for (int i = 0; i < args.length; i++) {
      if ("-ro".equals(args[i]))
        readOnly = true;
      if ("-add".equals(args[i]))
        add = true;
    }

    File indexDir = new File("index");
    if (! indexDir.exists()) indexDir.mkdirs();
    
    IndexReader.unlock(FSDirectory.getDirectory(indexDir));

    if (!readOnly) {
      IndexWriter writer = new IndexWriter(indexDir, ANALYZER, !add, IndexWriter.MaxFieldLength.LIMITED);
      
      Thread indexerThread = new IndexerThread(writer);
      indexerThread.start();
      
      Thread.sleep(1000);
    }
      
    SearcherThread searcherThread1 = new SearcherThread(false);
    searcherThread1.start();

    SEARCHER = new IndexSearcher(indexDir.toString());

    SearcherThread searcherThread2 = new SearcherThread(true);
    searcherThread2.start();

    SearcherThread searcherThread3 = new SearcherThread(true);
    searcherThread3.start();
  }
}
