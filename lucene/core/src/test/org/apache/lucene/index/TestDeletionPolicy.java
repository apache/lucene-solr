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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/
public class TestDeletionPolicy extends LuceneTestCase {
  
  private void verifyCommitOrder(List<? extends IndexCommit> commits) {
    if (commits.isEmpty()) {
      return;
    }
    final IndexCommit firstCommit = commits.get(0);
    long last = SegmentInfos.generationFromSegmentsFileName(firstCommit.getSegmentsFileName());
    assertEquals(last, firstCommit.getGeneration());
    for(int i=1;i<commits.size();i++) {
      final IndexCommit commit =  commits.get(i);
      long now = SegmentInfos.generationFromSegmentsFileName(commit.getSegmentsFileName());
      assertTrue("SegmentInfos commits are out-of-order", now > last);
      assertEquals(now, commit.getGeneration());
      last = now;
    }
  }

  class KeepAllDeletionPolicy extends IndexDeletionPolicy {
    int numOnInit;
    int numOnCommit;
    Directory dir;

    KeepAllDeletionPolicy(Directory dir) {
      this.dir = dir;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
      verifyCommitOrder(commits);
      numOnInit++;
    }
    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
      IndexCommit lastCommit =  commits.get(commits.size()-1);
      DirectoryReader r = DirectoryReader.open(dir);
      assertEquals("lastCommit.segmentCount()=" + lastCommit.getSegmentCount() + " vs IndexReader.segmentCount=" + r.leaves().size(), r.leaves().size(), lastCommit.getSegmentCount());
      r.close();
      verifyCommitOrder(commits);
      numOnCommit++;
    }

  }

  /**
   * This is useful for adding to a big index when you know
   * readers are not using it.
   */
  class KeepNoneOnInitDeletionPolicy extends IndexDeletionPolicy {
    int numOnInit;
    int numOnCommit;
    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
      verifyCommitOrder(commits);
      numOnInit++;
      // On init, delete all commit points:
      for (final IndexCommit commit : commits) {
        commit.delete();
        assertTrue(commit.isDeleted());
      }
    }
    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
      verifyCommitOrder(commits);
      int size = commits.size();
      // Delete all but last one:
      for(int i=0;i<size-1;i++) {
        ((IndexCommit) commits.get(i)).delete();
      }
      numOnCommit++;
    }
  }

  class KeepLastNDeletionPolicy extends IndexDeletionPolicy {
    int numOnInit;
    int numOnCommit;
    int numToKeep;
    int numDelete;
    Set<String> seen = new HashSet<>();

    public KeepLastNDeletionPolicy(int numToKeep) {
      this.numToKeep = numToKeep;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
      if (VERBOSE) {
        System.out.println("TEST: onInit");
      }
      verifyCommitOrder(commits);
      numOnInit++;
      // do no deletions on init
      doDeletes(commits, false);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
      if (VERBOSE) {
        System.out.println("TEST: onCommit");
      }
      verifyCommitOrder(commits);
      doDeletes(commits, true);
    }
    
    private void doDeletes(List<? extends IndexCommit> commits, boolean isCommit) {

      // Assert that we really are only called for each new
      // commit:
      if (isCommit) {
        String fileName = ((IndexCommit) commits.get(commits.size()-1)).getSegmentsFileName();
        if (seen.contains(fileName)) {
          throw new RuntimeException("onCommit was called twice on the same commit point: " + fileName);
        }
        seen.add(fileName);
        numOnCommit++;
      }
      int size = commits.size();
      for(int i=0;i<size-numToKeep;i++) {
        ((IndexCommit) commits.get(i)).delete();
        numDelete++;
      }
    }
  }

  static long getCommitTime(IndexCommit commit) throws IOException {
    return Long.parseLong(commit.getUserData().get("commitTime"));
  }

  /*
   * Delete a commit only when it has been obsoleted by N
   * seconds.
   */
  class ExpirationTimeDeletionPolicy extends IndexDeletionPolicy {

    Directory dir;
    double expirationTimeSeconds;
    int numDelete;

    public ExpirationTimeDeletionPolicy(Directory dir, double seconds) {
      this.dir = dir;
      this.expirationTimeSeconds = seconds;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
      if (commits.isEmpty()) {
        return;
      }
      verifyCommitOrder(commits);
      onCommit(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
      verifyCommitOrder(commits);

      IndexCommit lastCommit = commits.get(commits.size()-1);

      // Any commit older than expireTime should be deleted:
      double expireTime = getCommitTime(lastCommit)/1000.0 - expirationTimeSeconds;

      for (final IndexCommit commit : commits) {
        double modTime = getCommitTime(commit)/1000.0;
        if (commit != lastCommit && modTime < expireTime) {
          commit.delete();
          numDelete += 1;
        }
      }
    }
  }

  /*
   * Test "by time expiration" deletion policy:
   */
  // TODO: this wall-clock-dependent test doesn't seem to actually test any deletionpolicy logic?
  @Nightly
  public void testExpirationTimeDeletionPolicy() throws IOException, InterruptedException {

    final double SECONDS = 2.0;

    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
        .setIndexDeletionPolicy(new ExpirationTimeDeletionPolicy(dir, SECONDS));
    MergePolicy mp = conf.getMergePolicy();
    mp.setNoCFSRatio(1.0);
    IndexWriter writer = new IndexWriter(dir, conf);
    ExpirationTimeDeletionPolicy policy = (ExpirationTimeDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    Map<String,String> commitData = new HashMap<>();
    commitData.put("commitTime", String.valueOf(System.currentTimeMillis()));
    writer.setLiveCommitData(commitData.entrySet());
    writer.commit();
    writer.close();

    long lastDeleteTime = 0;
    final int targetNumDelete = TestUtil.nextInt(random(), 1, 5);
    while (policy.numDelete < targetNumDelete) {
      // Record last time when writer performed deletes of
      // past commits
      lastDeleteTime = System.currentTimeMillis();
      conf = newIndexWriterConfig(new MockAnalyzer(random()))
               .setOpenMode(OpenMode.APPEND)
               .setIndexDeletionPolicy(policy);
      mp = conf.getMergePolicy();
      mp.setNoCFSRatio(1.0);
      writer = new IndexWriter(dir, conf);
      policy = (ExpirationTimeDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
      for(int j=0;j<17;j++) {
        addDoc(writer);
      }
      commitData = new HashMap<>();
      commitData.put("commitTime", String.valueOf(System.currentTimeMillis()));
      writer.setLiveCommitData(commitData.entrySet());
      writer.commit();
      writer.close();

      Thread.sleep((int) (1000.0*(SECONDS/5.0)));
    }

    // Then simplistic check: just verify that the
    // segments_N's that still exist are in fact within SECONDS
    // seconds of the last one's mod time, and, that I can
    // open a reader on each:
    long gen = SegmentInfos.getLastCommitGeneration(dir);
    
    String fileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                            "",
                                                            gen);
    boolean oneSecondResolution = true;

    while(gen > 0) {
      try {
        IndexReader reader = DirectoryReader.open(dir);
        reader.close();
        fileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                         "",
                                                         gen);

        // if we are on a filesystem that seems to have only
        // 1 second resolution, allow +1 second in commit
        // age tolerance:
        SegmentInfos sis = SegmentInfos.readCommit(dir, fileName);
        assertEquals(Version.LATEST, sis.getCommitLuceneVersion());
        assertEquals(Version.LATEST, sis.getMinSegmentLuceneVersion());
        long modTime = Long.parseLong(sis.getUserData().get("commitTime"));
        oneSecondResolution &= (modTime % 1000) == 0;
        final long leeway = (long) ((SECONDS + (oneSecondResolution ? 1.0:0.0))*1000);

        assertTrue("commit point was older than " + SECONDS + " seconds (" + (lastDeleteTime - modTime) + " msec) but did not get deleted ", lastDeleteTime - modTime <= leeway);
      } catch (IOException e) {
        // OK
        break;
      }
      
      dir.deleteFile(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
      gen--;
    }

    dir.close();
  }

  /*
   * Test a silly deletion policy that keeps all commits around.
   */
  public void testKeepAllDeletionPolicy() throws IOException {
    for(int pass=0;pass<2;pass++) {

      if (VERBOSE) {
        System.out.println("TEST: cycle pass=" + pass);
      }

      boolean useCompoundFile = (pass % 2) != 0;

      Directory dir = newDirectory();

      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
          .setIndexDeletionPolicy(new KeepAllDeletionPolicy(dir))
          .setMaxBufferedDocs(10)
          .setMergeScheduler(new SerialMergeScheduler());
      MergePolicy mp = conf.getMergePolicy();
      mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
      IndexWriter writer = new IndexWriter(dir, conf);
      KeepAllDeletionPolicy policy = (KeepAllDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
      for(int i=0;i<107;i++) {
        addDoc(writer);
      }
      writer.close();

      final boolean needsMerging;
      {
        DirectoryReader r = DirectoryReader.open(dir);
        needsMerging = r.leaves().size() != 1;
        r.close();
      }
      if (needsMerging) {
        conf = newIndexWriterConfig(new MockAnalyzer(random()))
                 .setOpenMode(OpenMode.APPEND)
                 .setIndexDeletionPolicy(policy);
        mp = conf.getMergePolicy();
        mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
        if (VERBOSE) {
          System.out.println("TEST: open writer for forceMerge");
        }
        writer = new IndexWriter(dir, conf);
        policy = (KeepAllDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        writer.forceMerge(1);
        writer.close();
      }

      assertEquals(needsMerging ? 2:1, policy.numOnInit);

      // If we are not auto committing then there should
      // be exactly 2 commits (one per close above):
      assertEquals(1 + (needsMerging ? 1:0), policy.numOnCommit);

      // Test listCommits
      Collection<IndexCommit> commits = DirectoryReader.listCommits(dir);
      // 2 from closing writer
      assertEquals(1 + (needsMerging ? 1:0), commits.size());

      // Make sure we can open a reader on each commit:
      for (final IndexCommit commit : commits) {
        IndexReader r = DirectoryReader.open(commit);
        r.close();
      }

      // Simplistic check: just verify all segments_N's still
      // exist, and, I can open a reader on each:
      long gen = SegmentInfos.getLastCommitGeneration(dir);
      while(gen > 0) {
        IndexReader reader = DirectoryReader.open(dir);
        reader.close();
        dir.deleteFile(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        gen--;

        if (gen > 0) {
          // Now that we've removed a commit point, which
          // should have orphan'd at least one index file.
          // Open & close a writer and assert that it
          // actually removed something:
          int preCount = dir.listAll().length;
          writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
              .setOpenMode(OpenMode.APPEND)
              .setIndexDeletionPolicy(policy));
          writer.close();
          int postCount = dir.listAll().length;
          assertTrue(postCount < preCount);
        }
      }

      dir.close();
    }
  }

  /* Uses KeepAllDeletionPolicy to keep all commits around,
   * then, opens a new IndexWriter on a previous commit
   * point. */
  public void testOpenPriorSnapshot() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).
            setIndexDeletionPolicy(new KeepAllDeletionPolicy(dir)).
            setMaxBufferedDocs(2).
            setMergePolicy(newLogMergePolicy(10))
    );
    KeepAllDeletionPolicy policy = (KeepAllDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    for(int i=0;i<10;i++) {
      addDoc(writer);
      if ((1+i)%2 == 0)
        writer.commit();
    }
    writer.close();

    Collection<IndexCommit> commits = DirectoryReader.listCommits(dir);
    assertEquals(5, commits.size());
    IndexCommit lastCommit = null;
    for (final IndexCommit commit : commits) {
      if (lastCommit == null || commit.getGeneration() > lastCommit.getGeneration())
        lastCommit = commit;
    }
    assertTrue(lastCommit != null);

    // Now add 1 doc and merge
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setIndexDeletionPolicy(policy));
    addDoc(writer);
    assertEquals(11, writer.getDocStats().numDocs);
    writer.forceMerge(1);
    writer.close();

    assertEquals(6, DirectoryReader.listCommits(dir).size());

    // Now open writer on the commit just before merge:
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setIndexDeletionPolicy(policy)
                                    .setIndexCommit(lastCommit));
    assertEquals(10, writer.getDocStats().numDocs);

    // Should undo our rollback:
    writer.rollback();

    DirectoryReader r = DirectoryReader.open(dir);
    // Still merged, still 11 docs
    assertEquals(1, r.leaves().size());
    assertEquals(11, r.numDocs());
    r.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setIndexDeletionPolicy(policy)
                                    .setIndexCommit(lastCommit));
    assertEquals(10, writer.getDocStats().numDocs);
    // Commits the rollback:
    writer.close();

    // Now 8 because we made another commit
    assertEquals(7, DirectoryReader.listCommits(dir).size());
    
    r = DirectoryReader.open(dir);
    // Not fully merged because we rolled it back, and now only
    // 10 docs
    assertTrue(r.leaves().size() > 1);
    assertEquals(10, r.numDocs());
    r.close();

    // Re-merge
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setIndexDeletionPolicy(policy));
    writer.forceMerge(1);
    writer.close();

    r = DirectoryReader.open(dir);
    assertEquals(1, r.leaves().size());
    assertEquals(10, r.numDocs());
    r.close();

    // Now open writer on the commit just before merging,
    // but this time keeping only the last commit:
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setIndexCommit(lastCommit));
    assertEquals(10, writer.getDocStats().numDocs);
    
    // Reader still sees fully merged index, because writer
    // opened on the prior commit has not yet committed:
    r = DirectoryReader.open(dir);
    assertEquals(1, r.leaves().size());
    assertEquals(10, r.numDocs());
    r.close();

    writer.close();

    // Now reader sees not-fully-merged index:
    r = DirectoryReader.open(dir);
    assertTrue(r.leaves().size() > 1);
    assertEquals(10, r.numDocs());
    r.close();

    dir.close();
  }


  /* Test keeping NO commit points.  This is a viable and
   * useful case eg where you want to build a big index and
   * you know there are no readers.
   */
  public void testKeepNoneOnInitDeletionPolicy() throws IOException {
    for(int pass=0;pass<2;pass++) {

      boolean useCompoundFile = (pass % 2) != 0;

      Directory dir = newDirectory();

      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
          .setOpenMode(OpenMode.CREATE)
          .setIndexDeletionPolicy(new KeepNoneOnInitDeletionPolicy())
          .setMaxBufferedDocs(10);
      MergePolicy mp = conf.getMergePolicy();
      mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
      IndexWriter writer = new IndexWriter(dir, conf);
      KeepNoneOnInitDeletionPolicy policy = (KeepNoneOnInitDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
      for(int i=0;i<107;i++) {
        addDoc(writer);
      }
      writer.close();

      conf = newIndexWriterConfig(new MockAnalyzer(random()))
          .setOpenMode(OpenMode.APPEND)
          .setIndexDeletionPolicy(policy);
      mp = conf.getMergePolicy();
      mp.setNoCFSRatio(1.0);
      writer = new IndexWriter(dir, conf);
      policy = (KeepNoneOnInitDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
      writer.forceMerge(1);
      writer.close();

      assertEquals(2, policy.numOnInit);
      // If we are not auto committing then there should
      // be exactly 2 commits (one per close above):
      assertEquals(2, policy.numOnCommit);

      // Simplistic check: just verify the index is in fact
      // readable:
      IndexReader reader = DirectoryReader.open(dir);
      reader.close();

      dir.close();
    }
  }

  /*
   * Test a deletion policy that keeps last N commits.
   */
  public void testKeepLastNDeletionPolicy() throws IOException {
    final int N = 5;

    for(int pass=0;pass<2;pass++) {

      boolean useCompoundFile = (pass % 2) != 0;

      Directory dir = newDirectory();

      KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(N);
      for(int j=0;j<N+1;j++) {
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(OpenMode.CREATE)
            .setIndexDeletionPolicy(policy)
            .setMaxBufferedDocs(10);
        MergePolicy mp = conf.getMergePolicy();
        mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
        IndexWriter writer = new IndexWriter(dir, conf);
        policy = (KeepLastNDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        for(int i=0;i<17;i++) {
          addDoc(writer);
        }
        writer.forceMerge(1);
        writer.close();
      }

      assertTrue(policy.numDelete > 0);
      assertEquals(N+1, policy.numOnInit);
      assertEquals(N+1, policy.numOnCommit);

      // Simplistic check: just verify only the past N segments_N's still
      // exist, and, I can open a reader on each:
      long gen = SegmentInfos.getLastCommitGeneration(dir);
      for(int i=0;i<N+1;i++) {
        try {
          IndexReader reader = DirectoryReader.open(dir);
          reader.close();
          if (i == N) {
            fail("should have failed on commits prior to last " + N);
          }
        } catch (IOException e) {
          if (i != N) {
            throw e;
          }
        }
        if (i < N) {
          dir.deleteFile(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        }
        gen--;
      }

      dir.close();
    }
  }

  /*
   * Test a deletion policy that keeps last N commits
   * around, through creates.
   */
  public void testKeepLastNDeletionPolicyWithCreates() throws IOException {
    
    final int N = 10;

    for(int pass=0;pass<2;pass++) {

      boolean useCompoundFile = (pass % 2) != 0;

      Directory dir = newDirectory();
      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
          .setOpenMode(OpenMode.CREATE)
          .setIndexDeletionPolicy(new KeepLastNDeletionPolicy(N))
          .setMaxBufferedDocs(10);
      MergePolicy mp = conf.getMergePolicy();
      mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
      IndexWriter writer = new IndexWriter(dir, conf);
      KeepLastNDeletionPolicy policy = (KeepLastNDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
      writer.close();
      Term searchTerm = new Term("content", "aaa");        
      Query query = new TermQuery(searchTerm);

      for(int i=0;i<N+1;i++) {

        conf = newIndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(OpenMode.APPEND)
            .setIndexDeletionPolicy(policy)
            .setMaxBufferedDocs(10);
        mp = conf.getMergePolicy();
        mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
        writer = new IndexWriter(dir, conf);
        policy = (KeepLastNDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        for(int j=0;j<17;j++) {
          addDocWithID(writer, i*(N+1)+j);
        }
        // this is a commit
        writer.close();
        conf = new IndexWriterConfig(new MockAnalyzer(random()))
          .setIndexDeletionPolicy(policy)
          .setMergePolicy(NoMergePolicy.INSTANCE);
        writer = new IndexWriter(dir, conf);
        policy = (KeepLastNDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        writer.deleteDocuments(new Term("id", "" + (i*(N+1)+3)));
        // this is a commit
        writer.close();
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = newSearcher(reader);
        ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
        assertEquals(16, hits.length);
        reader.close();

        writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(OpenMode.CREATE)
            .setIndexDeletionPolicy(policy));
        policy = (KeepLastNDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        // This will not commit: there are no changes
        // pending because we opened for "create":
        writer.close();
      }

      assertEquals(3*(N+1)+1, policy.numOnInit);
      assertEquals(3*(N+1)+1, policy.numOnCommit);

      IndexReader rwReader = DirectoryReader.open(dir);
      IndexSearcher searcher = newSearcher(rwReader);
      ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
      assertEquals(0, hits.length);

      // Simplistic check: just verify only the past N segments_N's still
      // exist, and, I can open a reader on each:
      long gen = SegmentInfos.getLastCommitGeneration(dir);

      int expectedCount = 0;
      
      rwReader.close();

      for(int i=0;i<N+1;i++) {
        try {
          IndexReader reader = DirectoryReader.open(dir);

          // Work backwards in commits on what the expected
          // count should be.
          searcher = newSearcher(reader);
          hits = searcher.search(query, 1000).scoreDocs;
          assertEquals(expectedCount, hits.length);
          if (expectedCount == 0) {
            expectedCount = 16;
          } else if (expectedCount == 16) {
            expectedCount = 17;
          } else if (expectedCount == 17) {
            expectedCount = 0;
          }
          reader.close();
          if (i == N) {
            fail("should have failed on commits before last " + N);
          }
        } catch (IOException e) {
          if (i != N) {
            throw e;
          }
        }
        if (i < N) {
          dir.deleteFile(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        }
        gen--;
      }
      
      dir.close();
    }
  }

  private void addDocWithID(IndexWriter writer, int id) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(newStringField("id", "" + id, Field.Store.NO));
    writer.addDocument(doc);
  }
  
  private void addDoc(IndexWriter writer) throws IOException
  {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    writer.addDocument(doc);
  }
}
