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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexWriterOptimize extends LuceneTestCase {
  public void testOptimizeMaxNumSegments() throws IOException {

    MockDirectoryWrapper dir = newDirectory();

    final Document doc = new Document();
    doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
    final int incrMin = TEST_NIGHTLY ? 15 : 40;
    for(int numDocs=10;numDocs<500;numDocs += _TestUtil.nextInt(random, incrMin, 5*incrMin)) {
      LogDocMergePolicy ldmp = new LogDocMergePolicy();
      ldmp.setMinMergeDocs(1);
      ldmp.setMergeFactor(5);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(2).setMergePolicy(
            ldmp));
      for(int j=0;j<numDocs;j++)
        writer.addDocument(doc);
      writer.close();

      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      final int segCount = sis.size();

      ldmp = new LogDocMergePolicy();
      ldmp.setMergeFactor(5);
      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT,
        new MockAnalyzer(random)).setMergePolicy(ldmp));
      writer.optimize(3);
      writer.close();

      sis = new SegmentInfos();
      sis.read(dir);
      final int optSegCount = sis.size();

      if (segCount < 3)
        assertEquals(segCount, optSegCount);
      else
        assertEquals(3, optSegCount);
    }
    dir.close();
  }

  public void testOptimizeMaxNumSegments2() throws IOException {
    MockDirectoryWrapper dir = newDirectory();

    final Document doc = new Document();
    doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));

    LogDocMergePolicy ldmp = new LogDocMergePolicy();
    ldmp.setMinMergeDocs(1);
    ldmp.setMergeFactor(4);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
      TEST_VERSION_CURRENT, new MockAnalyzer(random))
      .setMaxBufferedDocs(2).setMergePolicy(ldmp).setMergeScheduler(new ConcurrentMergeScheduler()));

    for(int iter=0;iter<10;iter++) {
      for(int i=0;i<19;i++)
        writer.addDocument(doc);

      writer.commit();
      writer.waitForMerges();
      writer.commit();

      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);

      final int segCount = sis.size();

      writer.optimize(7);
      writer.commit();
      writer.waitForMerges();

      sis = new SegmentInfos();
      sis.read(dir);
      final int optSegCount = sis.size();

      if (segCount < 7)
        assertEquals(segCount, optSegCount);
      else
        assertEquals(7, optSegCount);
    }
    writer.close();
    dir.close();
  }

  /**
   * Make sure optimize doesn't use any more than 1X
   * starting index size as its temporary free space
   * required.
   */
  public void testOptimizeTempSpaceUsage() throws IOException {

    MockDirectoryWrapper dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(10).setMergePolicy(newLogMergePolicy()));
    if (VERBOSE) {
      System.out.println("TEST: config1=" + writer.getConfig());
    }

    for(int j=0;j<500;j++) {
      TestIndexWriter.addDocWithIndex(writer, j);
    }
    final int termIndexInterval = writer.getConfig().getTermIndexInterval();
    // force one extra segment w/ different doc store so
    // we see the doc stores get merged
    writer.commit();
    TestIndexWriter.addDocWithIndex(writer, 500);
    writer.close();

    if (VERBOSE) {
      System.out.println("TEST: start disk usage");
    }
    long startDiskUsage = 0;
    String[] files = dir.listAll();
    for(int i=0;i<files.length;i++) {
      startDiskUsage += dir.fileLength(files[i]);
      if (VERBOSE) {
        System.out.println(files[i] + ": " + dir.fileLength(files[i]));
      }
    }

    dir.resetMaxUsedSizeInBytes();
    dir.setTrackDiskUsage(true);

    // Import to use same term index interval else a
    // smaller one here could increase the disk usage and
    // cause a false failure:
    writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND).setTermIndexInterval(termIndexInterval).setMergePolicy(newLogMergePolicy()));
    writer.setInfoStream(VERBOSE ? System.out : null);
    writer.optimize();
    writer.close();
    long maxDiskUsage = dir.getMaxUsedSizeInBytes();
    assertTrue("optimize used too much temporary space: starting usage was " + startDiskUsage + " bytes; max temp usage was " + maxDiskUsage + " but should have been " + (4*startDiskUsage) + " (= 4X starting usage)",
               maxDiskUsage <= 4*startDiskUsage);
    dir.close();
  }
  
  // Test calling optimize(false) whereby optimize is kicked
  // off but we don't wait for it to finish (but
  // writer.close()) does wait
  public void testBackgroundOptimize() throws IOException {

    Directory dir = newDirectory();
    for(int pass=0;pass<2;pass++) {
      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(2).
              setMergePolicy(newLogMergePolicy(51))
      );
      Document doc = new Document();
      doc.add(newField("field", "aaa", Store.NO, Index.NOT_ANALYZED));
      for(int i=0;i<100;i++)
        writer.addDocument(doc);
      writer.optimize(false);

      if (0 == pass) {
        writer.close();
        IndexReader reader = IndexReader.open(dir, true);
        assertTrue(reader.isOptimized());
        reader.close();
      } else {
        // Get another segment to flush so we can verify it is
        // NOT included in the optimization
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.close();

        IndexReader reader = IndexReader.open(dir, true);
        assertTrue(!reader.isOptimized());
        reader.close();

        SegmentInfos infos = new SegmentInfos();
        infos.read(dir);
        assertEquals(2, infos.size());
      }
    }

    dir.close();
  }
}
