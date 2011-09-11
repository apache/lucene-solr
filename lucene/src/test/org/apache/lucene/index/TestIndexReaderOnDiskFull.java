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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexReaderOnDiskFull extends LuceneTestCase {
  /**
   * Make sure if reader tries to commit but hits disk
   * full that reader remains consistent and usable.
   */
  public void testDiskFull() throws IOException {

    Term searchTerm = new Term("content", "aaa");
    int START_COUNT = 157;
    int END_COUNT = 144;
    
    // First build up a starting index:
    MockDirectoryWrapper startDir = newDirectory();
    IndexWriter writer = new IndexWriter(startDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    if (VERBOSE) {
      System.out.println("TEST: create initial index");
      writer.setInfoStream(System.out);
    }
    for(int i=0;i<157;i++) {
      Document d = new Document();
      d.add(newField("id", Integer.toString(i), StringField.TYPE_STORED));
      d.add(newField("content", "aaa " + i, TextField.TYPE_UNSTORED));
      writer.addDocument(d);
      if (0==i%10)
        writer.commit();
    }
    writer.close();

    {
      IndexReader r = IndexReader.open(startDir);
      IndexSearcher searcher = newSearcher(r);
      ScoreDoc[] hits = null;
      try {
        hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      } catch (IOException e) {
        e.printStackTrace();
        fail("exception when init searching: " + e);
      }
      searcher.close();
      r.close();
    }

    long diskUsage = startDir.getRecomputedActualSizeInBytes();
    long diskFree = diskUsage+_TestUtil.nextInt(random, 50, 200);

    IOException err = null;

    boolean done = false;
    boolean gotExc = false;

    // Iterate w/ ever increasing free disk space:
    while(!done) {
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random, new RAMDirectory(startDir, newIOContext(random)));

      // If IndexReader hits disk full, it can write to
      // the same files again.
      dir.setPreventDoubleWrite(false);

      IndexReader reader = IndexReader.open(dir, false);

      // For each disk size, first try to commit against
      // dir that will hit random IOExceptions & disk
      // full; after, give it infinite disk space & turn
      // off random IOExceptions & retry w/ same reader:
      boolean success = false;

      for(int x=0;x<2;x++) {

        double rate = 0.05;
        double diskRatio = ((double) diskFree)/diskUsage;
        long thisDiskFree;
        String testName;

        if (0 == x) {
          thisDiskFree = diskFree;
          if (diskRatio >= 2.0) {
            rate /= 2;
          }
          if (diskRatio >= 4.0) {
            rate /= 2;
          }
          if (diskRatio >= 6.0) {
            rate = 0.0;
          }
          if (VERBOSE) {
            System.out.println("\ncycle: " + diskFree + " bytes");
          }
          testName = "disk full during reader.close() @ " + thisDiskFree + " bytes";
        } else {
          thisDiskFree = 0;
          rate = 0.0;
          if (VERBOSE) {
            System.out.println("\ncycle: same writer: unlimited disk space");
          }
          testName = "reader re-use after disk full";
        }

        dir.setMaxSizeInBytes(thisDiskFree);
        dir.setRandomIOExceptionRate(rate);
        DefaultSimilarity sim = new DefaultSimilarity();
        try {
          if (0 == x) {
            int docId = 12;
            for(int i=0;i<13;i++) {
              reader.deleteDocument(docId);
              reader.setNorm(docId, "content", sim.encodeNormValue(2.0f));
              docId += 12;
            }
          }
          reader.close();
          success = true;
          if (0 == x) {
            done = true;
          }
        } catch (IOException e) {
          if (VERBOSE) {
            System.out.println("  hit IOException: " + e);
            e.printStackTrace(System.out);
          }
          err = e;
          gotExc = true;
          if (1 == x) {
            e.printStackTrace();
            fail(testName + " hit IOException after disk space was freed up");
          }
        }

        // Finally, verify index is not corrupt, and, if
        // we succeeded, we see all docs changed, and if
        // we failed, we see either all docs or no docs
        // changed (transactional semantics):
        IndexReader newReader = null;
        try {
          newReader = IndexReader.open(dir, false);
        } catch (IOException e) {
          e.printStackTrace();
          fail(testName + ":exception when creating IndexReader after disk full during close: " + e);
        }
        /*
        int result = newReader.docFreq(searchTerm);
        if (success) {
          if (result != END_COUNT) {
            fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + END_COUNT);
          }
        } else {
          // On hitting exception we still may have added
          // all docs:
          if (result != START_COUNT && result != END_COUNT) {
            err.printStackTrace();
            fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
          }
        }
        */

        IndexSearcher searcher = newSearcher(newReader);
        ScoreDoc[] hits = null;
        try {
          hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
        } catch (IOException e) {
          e.printStackTrace();
          fail(testName + ": exception when searching: " + e);
        }
        int result2 = hits.length;
        if (success) {
          if (result2 != END_COUNT) {
            fail(testName + ": method did not throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + END_COUNT);
          }
        } else {
          // On hitting exception we still may have added
          // all docs:
          if (result2 != START_COUNT && result2 != END_COUNT) {
            err.printStackTrace();
            fail(testName + ": method did throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + START_COUNT);
          }
        }

        searcher.close();
        newReader.close();

        if (result2 == END_COUNT) {
          if (!gotExc)
            fail("never hit disk full");
          break;
        }
      }

      dir.close();

      // Try again with more bytes of free space:
      diskFree += TEST_NIGHTLY ? _TestUtil.nextInt(random, 5, 20) : _TestUtil.nextInt(random, 50, 200);
    }

    startDir.close();
  }
}
