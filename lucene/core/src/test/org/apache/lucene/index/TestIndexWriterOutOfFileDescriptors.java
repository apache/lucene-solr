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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.TestUtil;

public class TestIndexWriterOutOfFileDescriptors extends LuceneTestCase {
  public void test() throws Exception {
    MockDirectoryWrapper dir = newMockFSDirectory(createTempDir("TestIndexWriterOutOfFileDescriptors"));
    dir.setPreventDoubleWrite(false);
    double rate = random().nextDouble()*0.01;
    //System.out.println("rate=" + rate);
    dir.setRandomIOExceptionRateOnOpen(rate);
    int iters = atLeast(20);
    LineFileDocs docs = new LineFileDocs(random());
    DirectoryReader r = null;
    DirectoryReader r2 = null;
    boolean any = false;
    MockDirectoryWrapper dirCopy = null;
    int lastNumDocs = 0;
    for(int iter=0;iter<iters;iter++) {

      IndexWriter w = null;
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      try {
        MockAnalyzer analyzer = new MockAnalyzer(random());
        analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
        IndexWriterConfig iwc = newIndexWriterConfig(analyzer);

        if (VERBOSE) {
          // Do this ourselves instead of relying on LTC so
          // we see incrementing messageID:
          iwc.setInfoStream(new PrintStreamInfoStream(System.out));
        }
        MergeScheduler ms = iwc.getMergeScheduler();
        if (ms instanceof ConcurrentMergeScheduler) {
          ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
        }
        w = new IndexWriter(dir, iwc);
        if (r != null && random().nextInt(5) == 3) {
          if (random().nextBoolean()) {
            if (VERBOSE) {
              System.out.println("TEST: addIndexes LR[]");
            }
            TestUtil.addIndexesSlowly(w, r);
          } else {
            if (VERBOSE) {
              System.out.println("TEST: addIndexes Directory[]");
            }
            w.addIndexes(new Directory[] {dirCopy});
          }
        } else {
          if (VERBOSE) {
            System.out.println("TEST: addDocument");
          }
          w.addDocument(docs.nextDoc());
        }
        dir.setRandomIOExceptionRateOnOpen(0.0);
        if (ms instanceof ConcurrentMergeScheduler) {
          ((ConcurrentMergeScheduler) ms).sync();
        }
        // If exc hit CMS then writer will be tragically closed:
        if (w.getTragicException() == null) {
          w.close();
        }
        w = null;

        // NOTE: This is O(N^2)!  Only enable for temporary debugging:
        //dir.setRandomIOExceptionRateOnOpen(0.0);
        //_TestUtil.checkIndex(dir);
        //dir.setRandomIOExceptionRateOnOpen(rate);

        // Verify numDocs only increases, to catch IndexWriter
        // accidentally deleting the index:
        dir.setRandomIOExceptionRateOnOpen(0.0);
        assertTrue(DirectoryReader.indexExists(dir));
        if (r2 == null) {
          r2 = DirectoryReader.open(dir);
        } else {
          DirectoryReader r3 = DirectoryReader.openIfChanged(r2);
          if (r3 != null) {
            r2.close();
            r2 = r3;
          }
        }
        assertTrue("before=" + lastNumDocs + " after=" + r2.numDocs(), r2.numDocs() >= lastNumDocs);
        lastNumDocs = r2.numDocs();
        //System.out.println("numDocs=" + lastNumDocs);
        dir.setRandomIOExceptionRateOnOpen(rate);

        any = true;
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + ": success");
        }
      } catch (AssertionError | IOException ioe) {
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + ": exception");
          ioe.printStackTrace();
        }
        if (w != null) {
          // NOTE: leave random IO exceptions enabled here,
          // to verify that rollback does not try to write
          // anything:
          w.rollback();
        }
      }

      if (any && r == null && random().nextBoolean()) {
        // Make a copy of a non-empty index so we can use
        // it to addIndexes later:
        dir.setRandomIOExceptionRateOnOpen(0.0);
        r = DirectoryReader.open(dir);
        dirCopy = newMockFSDirectory(createTempDir("TestIndexWriterOutOfFileDescriptors.copy"));
        Set<String> files = new HashSet<>();
        for (String file : dir.listAll()) {
          if (file.startsWith(IndexFileNames.SEGMENTS) || IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
            dirCopy.copyFrom(dir, file, file, IOContext.DEFAULT);
            files.add(file);
          }
        }
        dirCopy.sync(files);
        // Have IW kiss the dir so we remove any leftover
        // files ... we can easily have leftover files at
        // the time we take a copy because we are holding
        // open a reader:
        new IndexWriter(dirCopy, newIndexWriterConfig(new MockAnalyzer(random()))).close();
        dirCopy.setRandomIOExceptionRate(rate);
        dir.setRandomIOExceptionRateOnOpen(rate);
      }
    }

    if (r2 != null) {
      r2.close();
    }
    if (r != null) {
      r.close();
      dirCopy.close();
    }
    dir.close();
  }
}
