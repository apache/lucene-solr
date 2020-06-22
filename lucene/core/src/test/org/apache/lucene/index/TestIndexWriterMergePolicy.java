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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;

import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterMergePolicy extends LuceneTestCase {

  private static final MergePolicy MERGE_ON_COMMIT_POLICY = new LogDocMergePolicy() {
    @Override
    public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) {
      // Optimize down to a single segment on commit
      if (mergeTrigger == MergeTrigger.COMMIT && segmentInfos.size() > 1) {
        List<SegmentCommitInfo> nonMergingSegments = new ArrayList<>();
        for (SegmentCommitInfo sci : segmentInfos) {
          if (mergeContext.getMergingSegments().contains(sci) == false) {
            nonMergingSegments.add(sci);
          }
        }
        if (nonMergingSegments.size() > 1) {
          MergeSpecification mergeSpecification = new MergeSpecification();
          mergeSpecification.add(new OneMerge(nonMergingSegments));
          return mergeSpecification;
        }
      }
      return null;
    }
  };

  // Test the normal case
  public void testNormalCase() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(10)
                                                .setMergePolicy(new LogDocMergePolicy()));

    for (int i = 0; i < 100; i++) {
      addDoc(writer);
      checkInvariants(writer);
    }

    writer.close();
    dir.close();
  }

  // Test to see if there is over merge
  public void testNoOverMerge() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(10)
                                                .setMergePolicy(new LogDocMergePolicy()));

    boolean noOverMerge = false;
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
      checkInvariants(writer);
      if (writer.getNumBufferedDocuments() + writer.getSegmentCount() >= 18) {
        noOverMerge = true;
      }
    }
    assertTrue(noOverMerge);

    writer.close();
    dir.close();
  }

  // Test the case where flush is forced after every addDoc
  public void testForceFlush() throws IOException {
    Directory dir = newDirectory();

    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(100);
    mp.setMergeFactor(10);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(10)
                                                .setMergePolicy(mp));

    for (int i = 0; i < 100; i++) {
      addDoc(writer);
      writer.close();

      mp = new LogDocMergePolicy();
      mp.setMergeFactor(10);
      writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                      .setOpenMode(OpenMode.APPEND)
                                      .setMaxBufferedDocs(10)
                                      .setMergePolicy(mp));
      mp.setMinMergeDocs(100);
      checkInvariants(writer);
    }

    writer.close();
    dir.close();
  }

  // Test the case where mergeFactor changes
  public void testMergeFactorChange() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(10)
            .setMergePolicy(newLogMergePolicy())
            .setMergeScheduler(new SerialMergeScheduler())
    );

    for (int i = 0; i < 250; i++) {
      addDoc(writer);
      checkInvariants(writer);
    }

    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(5);

    // merge policy only fixes segments on levels where merges
    // have been triggered, so check invariants after all adds
    for (int i = 0; i < 10; i++) {
      addDoc(writer);
    }
    checkInvariants(writer);

    writer.close();
    dir.close();
  }

  // Test the case where both mergeFactor and maxBufferedDocs change
  @Nightly
  public void testMaxBufferedDocsChange() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(101)
        .setMergePolicy(new LogDocMergePolicy())
        .setMergeScheduler(new SerialMergeScheduler()));

    // leftmost* segment has 1 doc
    // rightmost* segment has 100 docs
    for (int i = 1; i <= 100; i++) {
      for (int j = 0; j < i; j++) {
        addDoc(writer);
        checkInvariants(writer);
      }
      writer.close();

      writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                      .setOpenMode(OpenMode.APPEND)
                                      .setMaxBufferedDocs(101)
                                      .setMergePolicy(new LogDocMergePolicy())
                                      .setMergeScheduler(new SerialMergeScheduler()));
    }

    writer.close();
    LogDocMergePolicy ldmp = new LogDocMergePolicy();
    ldmp.setMergeFactor(10);
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setOpenMode(OpenMode.APPEND)
                                    .setMaxBufferedDocs(10)
                                    .setMergePolicy(ldmp)
                                    .setMergeScheduler(new SerialMergeScheduler()));

    // merge policy only fixes segments on levels where merges
    // have been triggered, so check invariants after all adds
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
    }
    checkInvariants(writer);

    for (int i = 100; i < 1000; i++) {
      addDoc(writer);
    }
    writer.commit();
    writer.waitForMerges();
    writer.commit();
    checkInvariants(writer);

    writer.close();
    dir.close();
  }

  // Test the case where a merge results in no doc at all
  public void testMergeDocCount0() throws IOException {
    Directory dir = newDirectory();

    LogDocMergePolicy ldmp = new LogDocMergePolicy();
    ldmp.setMergeFactor(100);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(10)
                                                .setMergePolicy(ldmp));

    for (int i = 0; i < 250; i++) {
      addDoc(writer);
      checkInvariants(writer);
    }
    writer.close();

    // delete some docs without merging
    writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(NoMergePolicy.INSTANCE)
    );
    writer.deleteDocuments(new Term("content", "aaa"));
    writer.close();

    ldmp = new LogDocMergePolicy();
    ldmp.setMergeFactor(5);
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setOpenMode(OpenMode.APPEND)
                                    .setMaxBufferedDocs(10)
                                    .setMergePolicy(ldmp)
                                    .setMergeScheduler(new ConcurrentMergeScheduler()));

    // merge factor is changed, so check invariants after all adds
    for (int i = 0; i < 10; i++) {
      addDoc(writer);
    }
    writer.commit();
    writer.waitForMerges();
    writer.commit();
    checkInvariants(writer);
    assertEquals(10, writer.getDocStats().maxDoc);

    writer.close();
    dir.close();
  }

  private void addDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    writer.addDocument(doc);
  }

  private void checkInvariants(IndexWriter writer) throws IOException {
    writer.waitForMerges();
    int maxBufferedDocs = writer.getConfig().getMaxBufferedDocs();
    int mergeFactor = ((LogMergePolicy) writer.getConfig().getMergePolicy()).getMergeFactor();
    int maxMergeDocs = ((LogMergePolicy) writer.getConfig().getMergePolicy()).getMaxMergeDocs();

    int ramSegmentCount = writer.getNumBufferedDocuments();
    assertTrue(ramSegmentCount < maxBufferedDocs);

    int lowerBound = -1;
    int upperBound = maxBufferedDocs;
    int numSegments = 0;

    int segmentCount = writer.getSegmentCount();
    for (int i = segmentCount - 1; i >= 0; i--) {
      int docCount = writer.maxDoc(i);
      assertTrue("docCount=" + docCount + " lowerBound=" + lowerBound + " upperBound=" + upperBound + " i=" + i + " segmentCount=" + segmentCount + " index=" + writer.segString() + " config=" + writer.getConfig(), docCount > lowerBound);

      if (docCount <= upperBound) {
        numSegments++;
      } else {
        if (upperBound * mergeFactor <= maxMergeDocs) {
          assertTrue("maxMergeDocs=" + maxMergeDocs + "; numSegments=" + numSegments + "; upperBound=" + upperBound + "; mergeFactor=" + mergeFactor + "; segs=" + writer.segString() + " config=" + writer.getConfig(), numSegments < mergeFactor);
        }

        do {
          lowerBound = upperBound;
          upperBound *= mergeFactor;
        } while (docCount > upperBound);
        numSegments = 1;
      }
    }
    if (upperBound * mergeFactor <= maxMergeDocs) {
      assertTrue(numSegments < mergeFactor);
    }
  }

  private static final double EPSILON = 1E-14;
  
  public void testSetters() {
    assertSetters(new LogByteSizeMergePolicy());
    assertSetters(new LogDocMergePolicy());
  }

  // Test basic semantics of merge on commit
  public void testMergeOnCommit() throws IOException {
    Directory dir = newDirectory();

    IndexWriter firstWriter = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(NoMergePolicy.INSTANCE));
    for (int i = 0; i < 5; i++) {
      TestIndexWriter.addDoc(firstWriter);
      firstWriter.flush();
    }
    DirectoryReader firstReader = DirectoryReader.open(firstWriter);
    assertEquals(5, firstReader.leaves().size());
    firstReader.close();
    firstWriter.close(); // When this writer closes, it does not merge on commit.

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(MERGE_ON_COMMIT_POLICY).setMaxCommitMergeWaitSeconds(30);


    IndexWriter writerWithMergePolicy = new IndexWriter(dir, iwc);
    writerWithMergePolicy.commit(); // No changes. Commit doesn't trigger a merge.

    DirectoryReader unmergedReader = DirectoryReader.open(writerWithMergePolicy);
    assertEquals(5, unmergedReader.leaves().size());
    unmergedReader.close();

    TestIndexWriter.addDoc(writerWithMergePolicy);
    writerWithMergePolicy.commit(); // Doc added, do merge on commit.
    assertEquals(1, writerWithMergePolicy.getSegmentCount()); //

    DirectoryReader mergedReader = DirectoryReader.open(writerWithMergePolicy);
    assertEquals(1, mergedReader.leaves().size());
    mergedReader.close();

    try (IndexReader reader = writerWithMergePolicy.getReader()) {
      IndexSearcher searcher = new IndexSearcher(reader);
      assertEquals(6, reader.numDocs());
      assertEquals(6, searcher.count(new MatchAllDocsQuery()));
    }

    writerWithMergePolicy.close();
    dir.close();
  }

  private void assertSetters(MergePolicy lmp) {
    lmp.setMaxCFSSegmentSizeMB(2.0);
    assertEquals(2.0, lmp.getMaxCFSSegmentSizeMB(), EPSILON);
    
    lmp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    assertEquals(Long.MAX_VALUE/1024/1024., lmp.getMaxCFSSegmentSizeMB(), EPSILON*Long.MAX_VALUE);
    
    lmp.setMaxCFSSegmentSizeMB(Long.MAX_VALUE/1024/1024.);
    assertEquals(Long.MAX_VALUE/1024/1024., lmp.getMaxCFSSegmentSizeMB(), EPSILON*Long.MAX_VALUE);
    
    expectThrows(IllegalArgumentException.class, () -> {
      lmp.setMaxCFSSegmentSizeMB(-2.0);
    });
    
    // TODO: Add more checks for other non-double setters!
  }
}
