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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Map;

import org.apache.lucene.util._TestUtil;

/**
 * MergePolicy that makes random decisions for testing.
 */
public class MockRandomMergePolicy extends MergePolicy {
  private final Random random;

  public MockRandomMergePolicy(Random random) {
    // fork a private random, since we are called
    // unpredictably from threads:
    this.random = new Random(random.nextLong());
  }

  @Override
  public MergeSpecification findMerges(SegmentInfos segmentInfos) {
    MergeSpecification mergeSpec = null;
    //System.out.println("MRMP: findMerges sis=" + segmentInfos);

    if (segmentInfos.size() > 1 && random.nextInt(5) == 3) {
      
      List<SegmentInfo> segments = new ArrayList<SegmentInfo>(segmentInfos.asList());
      Collections.shuffle(segments, random);

      // TODO: sometimes make more than 1 merge?
      mergeSpec = new MergeSpecification();
      final int segsToMerge = _TestUtil.nextInt(random, 1, segmentInfos.size());
      mergeSpec.add(new OneMerge(segments.subList(0, segsToMerge)));
    }

    return mergeSpec;
  }

  @Override
  public MergeSpecification findForcedMerges(
       SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentInfo,Boolean> segmentsToMerge)
    throws CorruptIndexException, IOException {

    final List<SegmentInfo> eligibleSegments = new ArrayList<SegmentInfo>();
    for(SegmentInfo info : segmentInfos) {
      if (segmentsToMerge.containsKey(info)) {
        eligibleSegments.add(info);
      }
    }

    //System.out.println("MRMP: findMerges sis=" + segmentInfos + " eligible=" + eligibleSegments);
    MergeSpecification mergeSpec = null;
    if (eligibleSegments.size() > 1 || (eligibleSegments.size() == 1 && eligibleSegments.get(0).hasDeletions())) {
      mergeSpec = new MergeSpecification();
      // Already shuffled having come out of a set but
      // shuffle again for good measure:
      Collections.shuffle(eligibleSegments, random);
      int upto = 0;
      while(upto < eligibleSegments.size()) {
        int max = Math.min(10, eligibleSegments.size()-upto);
        int inc = max <= 2 ? max : _TestUtil.nextInt(random, 2, max);
        mergeSpec.add(new OneMerge(eligibleSegments.subList(upto, upto+inc)));
        upto += inc;
      }
    }

    if (mergeSpec != null) {
      for(OneMerge merge : mergeSpec.merges) {
        for(SegmentInfo info : merge.segments) {
          assert segmentsToMerge.containsKey(info);
        }
      }
    }
    return mergeSpec;
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos)
    throws CorruptIndexException, IOException {
    return findMerges(segmentInfos);
  }

  @Override
  public void close() {
  }

  @Override
  public boolean useCompoundFile(SegmentInfos infos, SegmentInfo mergedInfo) throws IOException {
    // 80% of the time we create CFS:
    return random.nextInt(5) != 1;
  }
}
