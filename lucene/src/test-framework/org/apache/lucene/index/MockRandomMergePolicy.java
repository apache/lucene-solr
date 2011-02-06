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
import java.util.Collections;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.util._TestUtil;

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
      
      SegmentInfos segmentInfos2 = new SegmentInfos();
      segmentInfos2.addAll(segmentInfos);
      Collections.shuffle(segmentInfos2, random);

      // TODO: sometimes make more than 1 merge?
      mergeSpec = new MergeSpecification();
      final int segsToMerge = _TestUtil.nextInt(random, 1, segmentInfos.size());
      mergeSpec.add(new OneMerge(segmentInfos2.range(0, segsToMerge)));
    }

    return mergeSpec;
  }

  @Override
  public MergeSpecification findMergesForOptimize(
      SegmentInfos segmentInfos, int maxSegmentCount, Set<SegmentInfo> segmentsToOptimize)
    throws CorruptIndexException, IOException {

    //System.out.println("MRMP: findMergesForOptimize sis=" + segmentInfos);
    MergeSpecification mergeSpec = null;
    if (segmentInfos.size() > 1 || (segmentInfos.size() == 1 && segmentInfos.info(0).hasDeletions())) {
      mergeSpec = new MergeSpecification();
      SegmentInfos segmentInfos2 = new SegmentInfos();
      segmentInfos2.addAll(segmentInfos);
      Collections.shuffle(segmentInfos2, random);
      int upto = 0;
      while(upto < segmentInfos.size()) {
        int max = Math.min(10, segmentInfos.size()-upto);
        int inc = max <= 2 ? max : _TestUtil.nextInt(random, 2, max);
        mergeSpec.add(new OneMerge(segmentInfos2.range(upto, upto+inc)));
        upto += inc;
      }
    }
    return mergeSpec;
  }

  @Override
  public MergeSpecification findMergesToExpungeDeletes(
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
