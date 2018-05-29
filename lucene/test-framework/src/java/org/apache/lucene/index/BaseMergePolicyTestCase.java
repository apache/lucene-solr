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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NullInfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToIntFunction;

/**
 * Base test case for {@link MergePolicy}.
 */
public abstract class BaseMergePolicyTestCase extends LuceneTestCase {
  
  /** Create a new {@link MergePolicy} instance. */
  protected abstract MergePolicy mergePolicy();

  public void testForceMergeNotNeeded() throws IOException {
    try (Directory dir = newDirectory()) {
      final AtomicBoolean mayMerge = new AtomicBoolean(true);
      final MergeScheduler mergeScheduler = new SerialMergeScheduler() {
          @Override
          synchronized public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {
            if (mayMerge.get() == false) {
              MergePolicy.OneMerge merge = writer.getNextMerge();
              if (merge != null) {
                System.out.println("TEST: we should not need any merging, yet merge policy returned merge " + merge);
                throw new AssertionError();
              }
            }

            super.merge(writer, trigger, newMergesFound);
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
        infos.add(new SegmentCommitInfo(info, random().nextInt(1), -1, -1, -1));
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
    private final InfoStream infoStream = new NullInfoStream();

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
      return Collections.emptySet();
    }
  }
  
}
