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


import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.Before;

// TODO
//   - mix in forceMerge, addIndexes
//   - randomoly mix in non-congruent docs

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestNRTThreads extends ThreadedIndexingAndSearchingTestCase {
  
  private boolean useNonNrtReaders = true;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    useNonNrtReaders  = random().nextBoolean();
  }
  
  @Override
  protected void doSearching(ExecutorService es, long stopTime) throws Exception {

    boolean anyOpenDelFiles = false;

    DirectoryReader r = DirectoryReader.open(writer);

    while (System.currentTimeMillis() < stopTime && !failed.get()) {
      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: now reopen r=" + r);
        }
        final DirectoryReader r2 = DirectoryReader.openIfChanged(r);
        if (r2 != null) {
          r.close();
          r = r2;
        }
      } else {
        if (VERBOSE) {
          System.out.println("TEST: now close reader=" + r);
        }
        r.close();
        writer.commit();
        final Set<String> openDeletedFiles = ((MockDirectoryWrapper) dir).getOpenDeletedFiles();
        if (openDeletedFiles.size() > 0) {
          System.out.println("OBD files: " + openDeletedFiles);
        }
        anyOpenDelFiles |= openDeletedFiles.size() > 0;
        //assertEquals("open but deleted: " + openDeletedFiles, 0, openDeletedFiles.size());
        if (VERBOSE) {
          System.out.println("TEST: now open");
        }
        r = DirectoryReader.open(writer);
      }
      if (VERBOSE) {
        System.out.println("TEST: got new reader=" + r);
      }
      //System.out.println("numDocs=" + r.numDocs() + "
      //openDelFileCount=" + dir.openDeleteFileCount());

      if (r.numDocs() > 0) {
        fixedSearcher = new IndexSearcher(r, es);
        smokeTestSearcher(fixedSearcher);
        runSearchThreads(System.currentTimeMillis() + 500);
      }
    }
    r.close();

    //System.out.println("numDocs=" + r.numDocs() + " openDelFileCount=" + dir.openDeleteFileCount());
    final Set<String> openDeletedFiles = ((MockDirectoryWrapper) dir).getOpenDeletedFiles();
    if (openDeletedFiles.size() > 0) {
      System.out.println("OBD files: " + openDeletedFiles);
    }
    anyOpenDelFiles |= openDeletedFiles.size() > 0;

    assertFalse("saw non-zero open-but-deleted count", anyOpenDelFiles);
  }
  
  @Override
  protected Directory getDirectory(Directory in) {
    assert in instanceof MockDirectoryWrapper;
    if (!useNonNrtReaders) ((MockDirectoryWrapper) in).setAssertNoDeleteOpenFile(true);
    return in;
  }

  @Override
  protected void doAfterWriter(ExecutorService es) throws Exception {
    // Force writer to do reader pooling, always, so that
    // all merged segments, even for merges before
    // doSearching is called, are warmed:
    writer.getReader().close();
  }
  
  private IndexSearcher fixedSearcher;

  @Override
  protected IndexSearcher getCurrentSearcher() throws Exception {
    return fixedSearcher;
  }

  @Override
  protected void releaseSearcher(IndexSearcher s) throws Exception {
    if (s != fixedSearcher) {
      // Final searcher:
      s.getIndexReader().close();
    }
  }

  @Override
  protected IndexSearcher getFinalSearcher() throws Exception {
    final IndexReader r2;
    if (useNonNrtReaders) {
      if (random().nextBoolean()) {
        r2 = writer.getReader();
      } else {
        writer.commit();
        r2 = DirectoryReader.open(dir);
      }
    } else {
      r2 = writer.getReader();
    }
    return newSearcher(r2);
  }

  public void testNRTThreads() throws Exception {
    runTest("TestNRTThreads");
  }
}
