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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * MergePolicy that makes random decisions for testing.
 */
public class MockRandomMergePolicy extends MergePolicy {
  private final Random random;
  boolean doNonBulkMerges = true;

  public MockRandomMergePolicy(Random random) {
    // fork a private random, since we are called
    // unpredictably from threads:
    this.random = new Random(random.nextLong());
  }
  
  /** 
   * Set to true if sometimes readers to be merged should be wrapped in a FilterReader
   * to mixup bulk merging.
   */
  public void setDoNonBulkMerges(boolean v) {
    doNonBulkMerges = v;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer) {
    MergeSpecification mergeSpec = null;
    //System.out.println("MRMP: findMerges sis=" + segmentInfos);

    int numSegments = segmentInfos.size();

    List<SegmentCommitInfo> segments = new ArrayList<>();
    final Collection<SegmentCommitInfo> merging = writer.getMergingSegments();

    for(SegmentCommitInfo sipc : segmentInfos) {
      if (!merging.contains(sipc)) {
        segments.add(sipc);
      }
    }

    numSegments = segments.size();

    if (numSegments > 1 && (numSegments > 30 || random.nextInt(5) == 3)) {

      Collections.shuffle(segments, random);

      // TODO: sometimes make more than 1 merge?
      mergeSpec = new MergeSpecification();
      final int segsToMerge = TestUtil.nextInt(random, 1, numSegments);
      if (doNonBulkMerges && random.nextBoolean()) {
        mergeSpec.add(new MockRandomOneMerge(segments.subList(0, segsToMerge),random.nextLong()));
      } else {
        mergeSpec.add(new OneMerge(segments.subList(0, segsToMerge)));
      }
    }

    return mergeSpec;
  }

  @Override
  public MergeSpecification findForcedMerges(
       SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
    throws IOException {

    final List<SegmentCommitInfo> eligibleSegments = new ArrayList<>();
    for(SegmentCommitInfo info : segmentInfos) {
      if (segmentsToMerge.containsKey(info)) {
        eligibleSegments.add(info);
      }
    }

    //System.out.println("MRMP: findMerges sis=" + segmentInfos + " eligible=" + eligibleSegments);
    MergeSpecification mergeSpec = null;
    if (eligibleSegments.size() > 1 || (eligibleSegments.size() == 1 && isMerged(segmentInfos, eligibleSegments.get(0), writer) == false)) {
      mergeSpec = new MergeSpecification();
      // Already shuffled having come out of a set but
      // shuffle again for good measure:
      Collections.shuffle(eligibleSegments, random);
      int upto = 0;
      while(upto < eligibleSegments.size()) {
        int max = Math.min(10, eligibleSegments.size()-upto);
        int inc = max <= 2 ? max : TestUtil.nextInt(random, 2, max);
        if (doNonBulkMerges && random.nextBoolean()) {
          mergeSpec.add(new MockRandomOneMerge(eligibleSegments.subList(upto, upto+inc), random.nextLong()));
        } else {
          mergeSpec.add(new OneMerge(eligibleSegments.subList(upto, upto+inc)));
        }
        upto += inc;
      }
    }

    if (mergeSpec != null) {
      for(OneMerge merge : mergeSpec.merges) {
        for(SegmentCommitInfo info : merge.segments) {
          assert segmentsToMerge.containsKey(info);
        }
      }
    }
    return mergeSpec;
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    return findMerges(null, segmentInfos, writer);
  }

  @Override
  public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, IndexWriter writer) throws IOException {
    // 80% of the time we create CFS:
    return random.nextInt(5) != 1;
  }
  
  static class MockRandomOneMerge extends OneMerge {
    final Random r;

    MockRandomOneMerge(List<SegmentCommitInfo> segments, long seed) {
      super(segments);
      r = new Random(seed);
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {

      // wrap it (e.g. prevent bulk merge etc)
      // TODO: cut this over to FilterCodecReader api, we can explicitly
      // enable/disable bulk merge for portions of the index we want.
      int thingToDo = r.nextInt(7);
      if (thingToDo == 0) {
        // simple no-op FilterReader
        if (LuceneTestCase.VERBOSE) {
          System.out.println("NOTE: MockRandomMergePolicy now swaps in a SlowCodecReaderWrapper for merging reader=" + reader);
        }
        return SlowCodecReaderWrapper.wrap(new FilterLeafReader(new MergeReaderWrapper(reader)) {});
      } else if (thingToDo == 1) {
        // renumber fields
        // NOTE: currently this only "blocks" bulk merges just by
        // being a FilterReader. But it might find bugs elsewhere, 
        // and maybe the situation can be improved in the future.
        if (LuceneTestCase.VERBOSE) {
          System.out.println("NOTE: MockRandomMergePolicy now swaps in a MismatchedLeafReader for merging reader=" + reader);
        }
        return SlowCodecReaderWrapper.wrap(new MismatchedLeafReader(new MergeReaderWrapper(reader), r));
      } else {
        // otherwise, reader is unchanged
        return reader;
      }
    }
  }
}
