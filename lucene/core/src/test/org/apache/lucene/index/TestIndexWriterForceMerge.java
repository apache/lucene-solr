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
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexWriterForceMerge extends LuceneTestCase {
  public void testPartialMerge() throws IOException {

    MockDirectoryWrapper dir = newDirectory();

    final Document doc = new Document();
    doc.add(newField("content", "aaa", StringField.TYPE_UNSTORED));
    final int incrMin = TEST_NIGHTLY ? 15 : 40;
    for(int numDocs=10;numDocs<500;numDocs += _TestUtil.nextInt(random(), incrMin, 5*incrMin)) {
      LogDocMergePolicy ldmp = new LogDocMergePolicy();
      ldmp.setMinMergeDocs(1);
      ldmp.setMergeFactor(5);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random()))
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
        new MockAnalyzer(random())).setMergePolicy(ldmp));
      writer.forceMerge(3);
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

  public void testMaxNumSegments2() throws IOException {
    MockDirectoryWrapper dir = newDirectory();

    final Document doc = new Document();
    doc.add(newField("content", "aaa", StringField.TYPE_UNSTORED));

    LogDocMergePolicy ldmp = new LogDocMergePolicy();
    ldmp.setMinMergeDocs(1);
    ldmp.setMergeFactor(4);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
      TEST_VERSION_CURRENT, new MockAnalyzer(random()))
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

      writer.forceMerge(7);
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
   * Make sure forceMerge doesn't use any more than 1X
   * starting index size as its temporary free space
   * required.
   */
  public void testForceMergeTempSpaceUsage() throws IOException {

    MockDirectoryWrapper dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(10).setMergePolicy(newLogMergePolicy()));
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
    writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND).setTermIndexInterval(termIndexInterval).setMergePolicy(newLogMergePolicy()));
    writer.forceMerge(1);
    writer.close();
    long maxDiskUsage = dir.getMaxUsedSizeInBytes();
    assertTrue("forceMerge used too much temporary space: starting usage was " + startDiskUsage + " bytes; max temp usage was " + maxDiskUsage + " but should have been " + (4*startDiskUsage) + " (= 4X starting usage)",
               maxDiskUsage <= 4*startDiskUsage);
    dir.close();
  }
  
  // Test calling forceMerge(1, false) whereby forceMerge is kicked
  // off but we don't wait for it to finish (but
  // writer.close()) does wait
  public void testBackgroundForceMerge() throws IOException {

    Directory dir = newDirectory();
    for(int pass=0;pass<2;pass++) {
      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(2).
              setMergePolicy(newLogMergePolicy(51))
      );
      Document doc = new Document();
      doc.add(newField("field", "aaa", StringField.TYPE_UNSTORED));
      for(int i=0;i<100;i++)
        writer.addDocument(doc);
      writer.forceMerge(1, false);

      if (0 == pass) {
        writer.close();
        DirectoryReader reader = IndexReader.open(dir);
        assertEquals(1, reader.getSequentialSubReaders().length);
        reader.close();
      } else {
        // Get another segment to flush so we can verify it is
        // NOT included in the merging
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.close();

        DirectoryReader reader = IndexReader.open(dir);
        assertTrue(reader.getSequentialSubReaders().length > 1);
        reader.close();

        SegmentInfos infos = new SegmentInfos();
        infos.read(dir);
        assertEquals(2, infos.size());
      }
    }

    dir.close();
  }
}
