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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;

public class EagerCheapMergePolicyTest extends LuceneTestCase {

  public void testMergeSelection() {
    EagerCheapMergePolicy mergePolicy = new EagerCheapMergePolicy(new TieredMergePolicy());
    mergePolicy.setCheapMergeThresholdMB(2.0);
    mergePolicy.setCheapMinMergeAtOnce(2);
    mergePolicy.setCheapMaxSizeRatio(1.5);
    // last two total to cheap merge threshold; won't get more
    assertMergeBySizes(mergePolicy, szMB(2.0, 1.0, 1.0), szMB(1.0, 1.0));
    // 1/10th, so now include 2.0
    assertMergeBySizes(mergePolicy, szMB(0.2, 0.1, 0.1), szMB(0.2, 0.1, 0.1));
    // 0.2->0.8... 0.8/(0.1+0.1) = 2.0 which exceeds 1.5 ratio, so don't include it
    assertMergeBySizes(mergePolicy, szMB(0.8, 0.1, 0.1), szMB(0.1, 0.1));
    // last segment is tiny then big jump; no other options
    assertMergeBySizes(mergePolicy, szMB(0.8, 0.1), null);
    // merge them all; smallest segment doesn't matter
    assertMergeBySizes(mergePolicy, szMB(0.8, 0.8, 0.1), szMB(0.8, 0.8, 0.1));
  }

  private void assertMergeBySizes(EagerCheapMergePolicy mergePolicy, long[] inputSizes, long[] expectMergeSizes) {
    List<SegmentCommitInfo> segments = mergePolicy.findCheapMerge(newSegInfos(inputSizes), Collections.emptySet());
    long[] resultMergeSizes = null;
    if (segments != null) {
      resultMergeSizes = segments.stream().map(infoPerC -> {
        try { // yuck!  lambdas don't deal with exceptions well
          return infoPerC.sizeInBytes();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).sorted(Comparator.comparingLong(Long::longValue).reversed()).mapToLong(Long::longValue).toArray();
    }

    assertArrayEquals(expectMergeSizes, resultMergeSizes);
  }

  /** convert the array of MB sizes to an array of byte sizes */
  private long[] szMB(double... sizesMB) {
    return Arrays.stream(sizesMB).mapToLong(dbl->(long) (dbl*1024.0*1024.0)).toArray();
  }

  private List<SegmentCommitInfo> newSegInfos(long... sizes) {
    List<SegmentCommitInfo> infoPerCommits = new ArrayList<>(sizes.length);
    for (long size : sizes) {
      // a hack implementation to specify the size explicitly
      infoPerCommits.add(new SegmentCommitInfo(null, 0, 0, -1, -1, -1, StringHelper.randomId()) {
        final long szField = size; // necessary since "size" must be final

        @Override
        public long sizeInBytes() throws IOException {
          return szField;
        }
      });
    }
    return infoPerCommits;
  }
}