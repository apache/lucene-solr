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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Collection;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestDeletionPolicy extends LuceneTestCase
{
  private void verifyCommitOrder(List commits) throws IOException {
    final IndexCommit firstCommit = ((IndexCommit) commits.get(0));
    long last = SegmentInfos.generationFromSegmentsFileName(firstCommit.getSegmentsFileName());
    assertEquals(last, firstCommit.getGeneration());
    long lastVersion = firstCommit.getVersion();
    long lastTimestamp = firstCommit.getTimestamp();
    for(int i=1;i<commits.size();i++) {
      final IndexCommit commit = ((IndexCommit) commits.get(i));
      long now = SegmentInfos.generationFromSegmentsFileName(commit.getSegmentsFileName());
      long nowVersion = commit.getVersion();
      long nowTimestamp = commit.getTimestamp();
      assertTrue("SegmentInfos commits are out-of-order", now > last);
      assertTrue("SegmentInfos versions are out-of-order", nowVersion > lastVersion);
      assertTrue("SegmentInfos timestamps are out-of-order: now=" + nowTimestamp + " vs last=" + lastTimestamp, nowTimestamp >= lastTimestamp);
      assertEquals(now, commit.getGeneration());
      last = now;
      lastVersion = nowVersion;
      lastTimestamp = nowTimestamp;
    }
  }

  class KeepAllDeletionPolicy implements IndexDeletionPolicy {
    int numOnInit;
    int numOnCommit;
    Directory dir;
    public void onInit(List commits) throws IOException {
      verifyCommitOrder(commits);
      numOnInit++;
    }
    public void onCommit(List commits) throws IOException {
      IndexCommit lastCommit = (IndexCommit) commits.get(commits.size()-1);
      IndexReader r = IndexReader.open(dir);
      assertEquals("lastCommit.isOptimized()=" + lastCommit.isOptimized() + " vs IndexReader.isOptimized=" + r.isOptimized(), r.isOptimized(), lastCommit.isOptimized());
      r.close();
      verifyCommitOrder(commits);
      numOnCommit++;
    }
  }

  /**
   * This is useful for adding to a big index w/ autoCommit
   * false when you know readers are not using it.
   */
  class KeepNoneOnInitDeletionPolicy implements IndexDeletionPolicy {
    int numOnInit;
    int numOnCommit;
    public void onInit(List commits) throws IOException {
      verifyCommitOrder(commits);
      numOnInit++;
      // On init, delete all commit points:
      Iterator it = commits.iterator();
      while(it.hasNext()) {
        final IndexCommit commit = (IndexCommit) it.next();
        commit.delete();
        assertTrue(commit.isDeleted());
      }
    }
    public void onCommit(List commits) throws IOException {
      verifyCommitOrder(commits);
      int size = commits.size();
      // Delete all but last one:
      for(int i=0;i<size-1;i++) {
        ((IndexCommit) commits.get(i)).delete();
      }
      numOnCommit++;
    }
  }

  class KeepLastNDeletionPolicy implements IndexDeletionPolicy {
    int numOnInit;
    int numOnCommit;
    int numToKeep;
    int numDelete;
    Set seen = new HashSet();

    public KeepLastNDeletionPolicy(int numToKeep) {
      this.numToKeep = numToKeep;
    }

    public void onInit(List commits) throws IOException {
      verifyCommitOrder(commits);
      numOnInit++;
      // do no deletions on init
      doDeletes(commits, false);
    }

    public void onCommit(List commits) throws IOException {
      verifyCommitOrder(commits);
      doDeletes(commits, true);
    }
    
    private void doDeletes(List commits, boolean isCommit) {

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

  /*
   * Delete a commit only when it has been obsoleted by N
   * seconds.
   */
  class ExpirationTimeDeletionPolicy implements IndexDeletionPolicy {

    Directory dir;
    double expirationTimeSeconds;
    int numDelete;

    public ExpirationTimeDeletionPolicy(Directory dir, double seconds) {
      this.dir = dir;
      this.expirationTimeSeconds = seconds;
    }

    public void onInit(List commits) throws IOException {
      verifyCommitOrder(commits);
      onCommit(commits);
    }

    public void onCommit(List commits) throws IOException {
      verifyCommitOrder(commits);

      IndexCommit lastCommit = (IndexCommit) commits.get(commits.size()-1);

      // Any commit older than expireTime should be deleted:
      double expireTime = dir.fileModified(lastCommit.getSegmentsFileName())/1000.0 - expirationTimeSeconds;

      Iterator it = commits.iterator();

      while(it.hasNext()) {
        IndexCommit commit = (IndexCommit) it.next();
        double modTime = dir.fileModified(commit.getSegmentsFileName())/1000.0;
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
  public void testExpirationTimeDeletionPolicy() throws IOException, InterruptedException {

    final double SECONDS = 2.0;

    boolean autoCommit = false;
    boolean useCompoundFile = true;

    Directory dir = new RAMDirectory();
    ExpirationTimeDeletionPolicy policy = new ExpirationTimeDeletionPolicy(dir, SECONDS);
    IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
    writer.setUseCompoundFile(useCompoundFile);
    writer.close();

    long lastDeleteTime = 0;
    for(int i=0;i<7;i++) {
      // Record last time when writer performed deletes of
      // past commits
      lastDeleteTime = System.currentTimeMillis();
      writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
      writer.setUseCompoundFile(useCompoundFile);
      for(int j=0;j<17;j++) {
        addDoc(writer);
      }
      writer.close();

      // Make sure to sleep long enough so that some commit
      // points will be deleted:
      Thread.sleep((int) (1000.0*(SECONDS/5.0)));
    }

    // First, make sure the policy in fact deleted something:
    assertTrue("no commits were deleted", policy.numDelete > 0);

    // Then simplistic check: just verify that the
    // segments_N's that still exist are in fact within SECONDS
    // seconds of the last one's mod time, and, that I can
    // open a reader on each:
    long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
    
    String fileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                            "",
                                                            gen);
    dir.deleteFile(IndexFileNames.SEGMENTS_GEN);
    while(gen > 0) {
      try {
        IndexReader reader = IndexReader.open(dir);
        reader.close();
        fileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                         "",
                                                         gen);
        long modTime = dir.fileModified(fileName);
        assertTrue("commit point was older than " + SECONDS + " seconds (" + (lastDeleteTime - modTime) + " msec) but did not get deleted", lastDeleteTime - modTime <= (SECONDS*1000));
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

    for(int pass=0;pass<4;pass++) {

      boolean autoCommit = pass < 2;
      boolean useCompoundFile = (pass % 2) > 0;

      // Never deletes a commit
      KeepAllDeletionPolicy policy = new KeepAllDeletionPolicy();

      Directory dir = new RAMDirectory();
      policy.dir = dir;

      IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
      writer.setMaxBufferedDocs(10);
      writer.setUseCompoundFile(useCompoundFile);
      writer.setMergeScheduler(new SerialMergeScheduler());
      for(int i=0;i<107;i++) {
        addDoc(writer);
        if (autoCommit && i%10 == 0)
          writer.commit();
      }
      writer.close();

      writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
      writer.setUseCompoundFile(useCompoundFile);
      writer.optimize();
      writer.close();

      assertEquals(2, policy.numOnInit);
      if (!autoCommit)
        // If we are not auto committing then there should
        // be exactly 2 commits (one per close above):
        assertEquals(2, policy.numOnCommit);

      // Test listCommits
      Collection commits = IndexReader.listCommits(dir);
      if (!autoCommit)
        // 1 from opening writer + 2 from closing writer
        assertEquals(3, commits.size());
      else
        // 1 from opening writer + 2 from closing writer +
        // 11 from calling writer.commit() explicitly above
        assertEquals(14, commits.size());

      Iterator it = commits.iterator();
      // Make sure we can open a reader on each commit:
      while(it.hasNext()) {
        IndexCommit commit = (IndexCommit) it.next();
        IndexReader r = IndexReader.open(commit, null);
        r.close();
      }

      // Simplistic check: just verify all segments_N's still
      // exist, and, I can open a reader on each:
      dir.deleteFile(IndexFileNames.SEGMENTS_GEN);
      long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
      while(gen > 0) {
        IndexReader reader = IndexReader.open(dir);
        reader.close();
        dir.deleteFile(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        gen--;

        if (gen > 0) {
          // Now that we've removed a commit point, which
          // should have orphan'd at least one index file.
          // Open & close a writer and assert that it
          // actually removed something:
          int preCount = dir.list().length;
          writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, policy, IndexWriter.MaxFieldLength.LIMITED);
          writer.close();
          int postCount = dir.list().length;
          assertTrue(postCount < preCount);
        }
      }

      dir.close();
    }
  }

  /* Test keeping NO commit points.  This is a viable and
   * useful case eg where you want to build a big index with
   * autoCommit false and you know there are no readers.
   */
  public void testKeepNoneOnInitDeletionPolicy() throws IOException {

    for(int pass=0;pass<4;pass++) {

      boolean autoCommit = pass < 2;
      boolean useCompoundFile = (pass % 2) > 0;

      KeepNoneOnInitDeletionPolicy policy = new KeepNoneOnInitDeletionPolicy();

      Directory dir = new RAMDirectory();

      IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
      writer.setMaxBufferedDocs(10);
      writer.setUseCompoundFile(useCompoundFile);
      for(int i=0;i<107;i++) {
        addDoc(writer);
      }
      writer.close();

      writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
      writer.setUseCompoundFile(useCompoundFile);
      writer.optimize();
      writer.close();

      assertEquals(2, policy.numOnInit);
      if (!autoCommit)
        // If we are not auto committing then there should
        // be exactly 2 commits (one per close above):
        assertEquals(2, policy.numOnCommit);

      // Simplistic check: just verify the index is in fact
      // readable:
      IndexReader reader = IndexReader.open(dir);
      reader.close();

      dir.close();
    }
  }

  /*
   * Test a deletion policy that keeps last N commits.
   */
  public void testKeepLastNDeletionPolicy() throws IOException {

    final int N = 5;

    for(int pass=0;pass<4;pass++) {

      boolean autoCommit = pass < 2;
      boolean useCompoundFile = (pass % 2) > 0;

      Directory dir = new RAMDirectory();

      KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(N);

      for(int j=0;j<N+1;j++) {
        IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
        writer.setMaxBufferedDocs(10);
        writer.setUseCompoundFile(useCompoundFile);
        for(int i=0;i<17;i++) {
          addDoc(writer);
        }
        writer.optimize();
        writer.close();
      }

      assertTrue(policy.numDelete > 0);
      assertEquals(N+1, policy.numOnInit);
      if (autoCommit) {
        assertTrue(policy.numOnCommit > 1);
      } else {
        assertEquals(N+1, policy.numOnCommit);
      }

      // Simplistic check: just verify only the past N segments_N's still
      // exist, and, I can open a reader on each:
      dir.deleteFile(IndexFileNames.SEGMENTS_GEN);
      long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
      for(int i=0;i<N+1;i++) {
        try {
          IndexReader reader = IndexReader.open(dir);
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
   * around, with reader doing deletes.
   */
  public void testKeepLastNDeletionPolicyWithReader() throws IOException {

    final int N = 10;

    for(int pass=0;pass<4;pass++) {

      boolean autoCommit = pass < 2;
      boolean useCompoundFile = (pass % 2) > 0;

      KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(N);

      Directory dir = new RAMDirectory();
      IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
      writer.setUseCompoundFile(useCompoundFile);
      writer.close();
      Term searchTerm = new Term("content", "aaa");        
      Query query = new TermQuery(searchTerm);

      for(int i=0;i<N+1;i++) {
        writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
        writer.setUseCompoundFile(useCompoundFile);
        for(int j=0;j<17;j++) {
          addDoc(writer);
        }
        // this is a commit when autoCommit=false:
        writer.close();
        IndexReader reader = IndexReader.open(dir, policy);
        reader.deleteDocument(3*i+1);
        reader.setNorm(4*i+1, "content", 2.0F);
        IndexSearcher searcher = new IndexSearcher(reader);
        ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
        assertEquals(16*(1+i), hits.length);
        // this is a commit when autoCommit=false:
        reader.close();
        searcher.close();
      }
      writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
      writer.setUseCompoundFile(useCompoundFile);
      writer.optimize();
      // this is a commit when autoCommit=false:
      writer.close();

      assertEquals(2*(N+2), policy.numOnInit);
      if (!autoCommit)
        assertEquals(2*(N+2)-1, policy.numOnCommit);

      IndexSearcher searcher = new IndexSearcher(dir);
      ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
      assertEquals(176, hits.length);

      // Simplistic check: just verify only the past N segments_N's still
      // exist, and, I can open a reader on each:
      long gen = SegmentInfos.getCurrentSegmentGeneration(dir);

      dir.deleteFile(IndexFileNames.SEGMENTS_GEN);
      int expectedCount = 176;

      for(int i=0;i<N+1;i++) {
        try {
          IndexReader reader = IndexReader.open(dir);

          // Work backwards in commits on what the expected
          // count should be.  Only check this in the
          // autoCommit false case:
          if (!autoCommit) {
            searcher = new IndexSearcher(reader);
            hits = searcher.search(query, null, 1000).scoreDocs;
            if (i > 1) {
              if (i % 2 == 0) {
                expectedCount += 1;
              } else {
                expectedCount -= 17;
              }
            }
            assertEquals(expectedCount, hits.length);
            searcher.close();
          }
          reader.close();
          if (i == N) {
            fail("should have failed on commits before last 5");
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

    for(int pass=0;pass<4;pass++) {

      boolean autoCommit = pass < 2;
      boolean useCompoundFile = (pass % 2) > 0;

      KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(N);

      Directory dir = new RAMDirectory();
      IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
      writer.setMaxBufferedDocs(10);
      writer.setUseCompoundFile(useCompoundFile);
      writer.close();
      Term searchTerm = new Term("content", "aaa");        
      Query query = new TermQuery(searchTerm);

      for(int i=0;i<N+1;i++) {

        writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
        writer.setMaxBufferedDocs(10);
        writer.setUseCompoundFile(useCompoundFile);
        for(int j=0;j<17;j++) {
          addDoc(writer);
        }
        // this is a commit when autoCommit=false:
        writer.close();
        IndexReader reader = IndexReader.open(dir, policy);
        reader.deleteDocument(3);
        reader.setNorm(5, "content", 2.0F);
        IndexSearcher searcher = new IndexSearcher(reader);
        ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
        assertEquals(16, hits.length);
        // this is a commit when autoCommit=false:
        reader.close();
        searcher.close();

        writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
        // This will not commit: there are no changes
        // pending because we opened for "create":
        writer.close();
      }

      assertEquals(1+3*(N+1), policy.numOnInit);
      if (!autoCommit)
        assertEquals(2*(N+1), policy.numOnCommit);

      IndexSearcher searcher = new IndexSearcher(dir);
      ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
      assertEquals(0, hits.length);

      // Simplistic check: just verify only the past N segments_N's still
      // exist, and, I can open a reader on each:
      long gen = SegmentInfos.getCurrentSegmentGeneration(dir);

      dir.deleteFile(IndexFileNames.SEGMENTS_GEN);
      int expectedCount = 0;

      for(int i=0;i<N+1;i++) {
        try {
          IndexReader reader = IndexReader.open(dir);

          // Work backwards in commits on what the expected
          // count should be.  Only check this in the
          // autoCommit false case:
          if (!autoCommit) {
            searcher = new IndexSearcher(reader);
            hits = searcher.search(query, null, 1000).scoreDocs;
            assertEquals(expectedCount, hits.length);
            searcher.close();
            if (expectedCount == 0) {
              expectedCount = 16;
            } else if (expectedCount == 16) {
              expectedCount = 17;
            } else if (expectedCount == 17) {
              expectedCount = 0;
            }
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

  private void addDoc(IndexWriter writer) throws IOException
  {
    Document doc = new Document();
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
  }
}
