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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.MergePolicy.MergeContext;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NullInfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

/**
 * Base test case for {@link MergePolicy}.
 */
public abstract class BaseMergePolicyTestCase extends LuceneTestCase {
  
  /** Create a new {@link MergePolicy} instance. */
  protected abstract MergePolicy mergePolicy();

  /**
   * Assert that the given segment infos match expectations of the merge
   * policy, assuming segments that have only been either flushed or merged with
   * this merge policy.
   */
  protected abstract void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) throws IOException;

  /**
   * Assert that the given merge matches expectations of the merge policy.
   */
  protected abstract void assertMerge(MergePolicy policy, MergeSpecification merge) throws IOException;

  public void testForceMergeNotNeeded() throws IOException {
    try (Directory dir = newDirectory()) {
      final AtomicBoolean mayMerge = new AtomicBoolean(true);
      final MergeScheduler mergeScheduler = new SerialMergeScheduler() {
          @Override
          synchronized public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
            if (mayMerge.get() == false) {
              MergePolicy.OneMerge merge = mergeSource.getNextMerge();
              if (merge != null) {
                System.out.println("TEST: we should not need any merging, yet merge policy returned merge " + merge);
                throw new AssertionError();
              }
            }

            super.merge(mergeSource, trigger);
          }
        };

      MergePolicy mp = mergePolicy();
      assumeFalse("this test cannot tolerate random forceMerges", mp.toString().contains("MockRandomMergePolicy"));
      mp.setNoCFSRatio(random().nextBoolean() ? 0 : 1);

      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergeScheduler(mergeScheduler);
      iwc.setMergePolicy(mp);

      IndexWriter writer = new IndexWriter(dir, iwc);
      final int numSegments = TestUtil.nextInt(random(), 2, 20);
      for (int i = 0; i < numSegments; ++i) {
        final int numDocs = TestUtil.nextInt(random(), 1, 5);
        for (int j = 0; j < numDocs; ++j) {
          writer.addDocument(new Document());
        }
        writer.getReader().close();
      }
      for (int i = 5; i >= 0; --i) {
        final int segmentCount = writer.getSegmentCount();
        final int maxNumSegments = i == 0 ? 1 : TestUtil.nextInt(random(), 1, 10);
        mayMerge.set(segmentCount > maxNumSegments);
        if (VERBOSE) {
          System.out.println("TEST: now forceMerge(maxNumSegments=" + maxNumSegments + ") vs segmentCount=" + segmentCount);
        }
        writer.forceMerge(maxNumSegments);
      }
      writer.close();
    }
  }

  public void testFindForcedDeletesMerges() throws IOException {
    MergePolicy mp = mergePolicy();
    if (mp instanceof FilterMergePolicy) {
      assumeFalse("test doesn't work with MockRandomMP",
          ((FilterMergePolicy) mp).in instanceof MockRandomMergePolicy);
    }
    SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
    try (Directory directory = newDirectory()) {
      MergePolicy.MergeContext context = new MockMergeContext(s -> 0);
      int numSegs = random().nextInt(10);
      for (int i = 0; i < numSegs; i++) {
        SegmentInfo info = new SegmentInfo(
            directory, // dir
            Version.LATEST, // version
            Version.LATEST, // min version
            TestUtil.randomSimpleString(random()), // name
            random().nextInt(Integer.MAX_VALUE), // maxDoc
            random().nextBoolean(), // isCompoundFile
            null, // codec
            Collections.emptyMap(), // diagnostics
            TestUtil.randomSimpleString(// id
                random(),
                StringHelper.ID_LENGTH,
                StringHelper.ID_LENGTH).getBytes(StandardCharsets.US_ASCII),
            Collections.emptyMap(), // attributes
            null /* indexSort */);
        info.setFiles(Collections.emptyList());
        infos.add(new SegmentCommitInfo(info, random().nextInt(1), 0, -1, -1, -1, StringHelper.randomId()));
      }
      MergePolicy.MergeSpecification forcedDeletesMerges = mp.findForcedDeletesMerges(infos, context);
      if (forcedDeletesMerges != null) {
        assertEquals(0, forcedDeletesMerges.merges.size());
      }
    }
  }

  /**
   * Simple mock merge context for tests
   */
  public static final class MockMergeContext implements MergePolicy.MergeContext {
    private final ToIntFunction<SegmentCommitInfo> numDeletesFunc;
    private final InfoStream infoStream = new NullInfoStream() {
      @Override
      public boolean isEnabled(String component) {
        // otherwise tests that simulate merging may bottleneck on generating messages
        return false;
      }
    };

    private Set<SegmentCommitInfo> mergingSegments = Collections.emptySet();

    public MockMergeContext(ToIntFunction<SegmentCommitInfo> numDeletesFunc) {
      this.numDeletesFunc = numDeletesFunc;
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo info) {
      return numDeletesFunc.applyAsInt(info);
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo info) {
      return numDeletesToMerge(info);
    }

    @Override
    public InfoStream getInfoStream() {
      return infoStream;
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
      return mergingSegments;
    }

    public void setMergingSegments(Set<SegmentCommitInfo> mergingSegments) {
      this.mergingSegments = mergingSegments;
    }
  }

  /**
   * Make a new {@link SegmentCommitInfo} with the given {@code maxDoc},
   * {@code numDeletedDocs} and {@code sizeInBytes}, which are usually the
   * numbers that merge policies care about.
   */
  protected static SegmentCommitInfo makeSegmentCommitInfo(String name, int maxDoc, int numDeletedDocs, double sizeMB, String source) {
    if (name.startsWith("_") == false) {
      throw new IllegalArgumentException("name must start with an _, got " + name);
    }
    byte[] id = new byte[StringHelper.ID_LENGTH];
    random().nextBytes(id);
    SegmentInfo info = new SegmentInfo(FAKE_DIRECTORY, Version.LATEST, Version.LATEST,
        name, maxDoc, false, TestUtil.getDefaultCodec(), Collections.emptyMap(), id,
        Collections.singletonMap(IndexWriter.SOURCE, source), null);
    info.setFiles(Collections.singleton(name + "_size=" + Long.toString((long) (sizeMB * 1024 * 1024)) + ".fake"));
    return new SegmentCommitInfo(info, numDeletedDocs, 0, 0, 0, 0, StringHelper.randomId());
  }

  /** A directory that computes the length of a file based on its name. */
  private static final Directory FAKE_DIRECTORY = new Directory() {

    @Override
    public String[] listAll() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long fileLength(String name) throws IOException {
      if (name.endsWith(".liv")) {
        return 0L;
      }
      if (name.endsWith(".fake") == false) {
        throw new IllegalArgumentException(name);
      }
      int startIndex = name.indexOf("_size=") + "_size=".length();
      int endIndex = name.length() - ".fake".length();
      return Long.parseLong(name.substring(startIndex, endIndex));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
      throw new UnsupportedOperationException();
    }
  };

  /**
   * Apply a merge to a {@link SegmentInfos} instance, accumulating the number
   * of written bytes into {@code stats}.
   */
  protected static SegmentInfos applyMerge(SegmentInfos infos, OneMerge merge, String mergedSegmentName, IOStats stats) throws IOException {
    LinkedHashSet<SegmentCommitInfo> scis = new LinkedHashSet<>(infos.asList());
    int newMaxDoc = 0;
    double newSize = 0;
    for (SegmentCommitInfo sci : merge.segments) {
      int numLiveDocs = sci.info.maxDoc() - sci.getDelCount();
      newSize += (double) sci.sizeInBytes() * numLiveDocs / sci.info.maxDoc() / 1024 / 1024;
      newMaxDoc += numLiveDocs;
      boolean removed = scis.remove(sci);
      assertTrue(removed);
    }
    SegmentInfos newInfos = new SegmentInfos(Version.LATEST.major);
    newInfos.addAll(scis);
    // Now add the merged segment
    newInfos.add(makeSegmentCommitInfo(mergedSegmentName, newMaxDoc, 0, newSize, IndexWriter.SOURCE_MERGE));
    stats.mergeBytesWritten += newSize * 1024 * 1024;
    return newInfos;
  }

  /**
   * Apply {@code numDeletes} uniformly across all segments of {@code infos}.
   */
  protected static SegmentInfos applyDeletes(SegmentInfos infos, int numDeletes) {
    List<SegmentCommitInfo> infoList = infos.asList();
    int totalNumDocs = infoList.stream()
        .mapToInt(s -> s.info.maxDoc() - s.getDelCount())
        .sum();
    if (numDeletes > totalNumDocs) {
      throw new IllegalArgumentException("More deletes than documents");
    }
    double w = (double) numDeletes / totalNumDocs;
    List<SegmentCommitInfo> newInfoList = new ArrayList<>();
    for (int i = 0; i < infoList.size(); ++i) {
      assert numDeletes >= 0;
      SegmentCommitInfo sci = infoList.get(i);
      int segDeletes;
      if (i == infoList.size() - 1) {
        segDeletes = numDeletes;
      } else {
        segDeletes = Math.min(numDeletes, (int) Math.ceil(w * (sci.info.maxDoc() - sci.getDelCount())));
      }
      int newDelCount = sci.getDelCount() + segDeletes;
      assert newDelCount <= sci.info.maxDoc();
      if (newDelCount < sci.info.maxDoc()) { // drop fully deleted segments
        SegmentCommitInfo newInfo = new SegmentCommitInfo(sci.info, sci.getDelCount() + segDeletes, 0, sci.getDelGen() + 1, sci.getFieldInfosGen(), sci.getDocValuesGen(), StringHelper.randomId());
        newInfoList.add(newInfo);
      }
      numDeletes -= segDeletes;
    }
    assert numDeletes == 0;
    SegmentInfos newInfos = new SegmentInfos(Version.LATEST.major);
    newInfos.addAll(newInfoList);
    return newInfos;
  }

  /**
   * Simulate an append-only use-case, ie. there are no deletes.
   */
  public void testSimulateAppendOnly() throws IOException {
    doTestSimulateAppendOnly(mergePolicy(), 100_000_000, 10_000);
  }

  /**
   * Simulate an append-only use-case, ie. there are no deletes.
   * {@code totalDocs} exist in the index in the end, and flushes contribute at most
   * {@code maxDocsPerFlush} documents.
   */
  protected void doTestSimulateAppendOnly(MergePolicy mergePolicy, int totalDocs, int maxDocsPerFlush) throws IOException {
    IOStats stats = new IOStats();
    AtomicLong segNameGenerator = new AtomicLong();
    MergeContext mergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    final double avgDocSizeMB = 5. / 1024; // 5kB
    for (int numDocs = 0; numDocs < totalDocs; ) {
      int flushDocCount = TestUtil.nextInt(random(), 1, maxDocsPerFlush);
      numDocs += flushDocCount;
      double flushSizeMB = flushDocCount * avgDocSizeMB;
      stats.flushBytesWritten += flushSizeMB * 1024 * 1024;
      segmentInfos.add(makeSegmentCommitInfo("_" + segNameGenerator.getAndIncrement(), flushDocCount, 0, flushSizeMB, IndexWriter.SOURCE_FLUSH));

      MergeSpecification merges = mergePolicy.findMerges(MergeTrigger.SEGMENT_FLUSH, segmentInfos, mergeContext);
      while (merges != null) {
        assertTrue(merges.merges.size() > 0);
        assertMerge(mergePolicy, merges);
        for (OneMerge oneMerge : merges.merges) {
          segmentInfos = applyMerge(segmentInfos, oneMerge, "_" + segNameGenerator.getAndIncrement(), stats);
        }
        merges = mergePolicy.findMerges(MergeTrigger.MERGE_FINISHED, segmentInfos, mergeContext);
      }
      assertSegmentInfos(mergePolicy, segmentInfos);
    }

    if (VERBOSE) {
      System.out.println("Write amplification for append-only: " + (double) (stats.flushBytesWritten + stats.mergeBytesWritten) / stats.flushBytesWritten);
    }
  }

  /**
   * Simulate an update use-case where documents are uniformly updated across segments.
   */
  public void testSimulateUpdates() throws IOException {
    int numDocs = atLeast(1_000_000);
    doTestSimulateUpdates(mergePolicy(), numDocs, 2500);
  }

  /**
   * Simulate an update use-case where documents are uniformly updated across segments.
   * {@code totalDocs} exist in the index in the end, and flushes contribute at most
   * {@code maxDocsPerFlush} documents.
   */
  protected void doTestSimulateUpdates(MergePolicy mergePolicy, int totalDocs, int maxDocsPerFlush) throws IOException {
    IOStats stats = new IOStats();
    AtomicLong segNameGenerator = new AtomicLong();
    MergeContext mergeContext = new MockMergeContext(SegmentCommitInfo::getDelCount);
    SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
    final double avgDocSizeMB = 5. / 1024; // 5kB
    for (int numDocs = 0; numDocs < totalDocs; ) {
      final int flushDocCount;
      if (usually()) {
        // reasonable value
        flushDocCount = TestUtil.nextInt(random(), maxDocsPerFlush/2, maxDocsPerFlush);
      } else {
        // crazy value
        flushDocCount = TestUtil.nextInt(random(), 1, maxDocsPerFlush);
      }
      // how many of these documents are actually updates
      int delCount = (int) (flushDocCount * 0.9 * numDocs / totalDocs);
      numDocs += flushDocCount - delCount;
      segmentInfos = applyDeletes(segmentInfos, delCount);
      double flushSize = flushDocCount * avgDocSizeMB;
      stats.flushBytesWritten += flushSize * 1024 * 1024;
      segmentInfos.add(makeSegmentCommitInfo("_" + segNameGenerator.getAndIncrement(), flushDocCount, 0, flushSize, IndexWriter.SOURCE_FLUSH));
      MergeSpecification merges = mergePolicy.findMerges(MergeTrigger.SEGMENT_FLUSH, segmentInfos, mergeContext);
      while (merges != null) {
        assertMerge(mergePolicy, merges);
        for (OneMerge oneMerge : merges.merges) {
          segmentInfos = applyMerge(segmentInfos, oneMerge, "_" + segNameGenerator.getAndIncrement(), stats);
        }
        merges = mergePolicy.findMerges(MergeTrigger.MERGE_FINISHED, segmentInfos, mergeContext);
      }
      assertSegmentInfos(mergePolicy, segmentInfos);
    }

    if (VERBOSE) {
      System.out.println("Write amplification for update: " + (double) (stats.flushBytesWritten + stats.mergeBytesWritten) / stats.flushBytesWritten);
      int totalDelCount = segmentInfos.asList().stream()
          .mapToInt(SegmentCommitInfo::getDelCount)
          .sum();
      int totalMaxDoc = segmentInfos.asList().stream()
          .map(s -> s.info)
          .mapToInt(SegmentInfo::maxDoc)
          .sum();
      System.out.println("Final live ratio: " + (1 - (double) totalDelCount / totalMaxDoc));
    }
  }

  /** Statistics about bytes written to storage. */
  public static class IOStats {
    /** Bytes written through flushes. */
    long flushBytesWritten;
    /** Bytes written through merges. */
    long mergeBytesWritten;
  }
}
