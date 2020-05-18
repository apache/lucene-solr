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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class TestOneMergeWrappingMergePolicy extends LuceneTestCase {

  private static class PredeterminedMergePolicy extends MergePolicy {

    final private MergePolicy.MergeSpecification merges;
    final private MergePolicy.MergeSpecification forcedMerges;
    final private MergePolicy.MergeSpecification forcedDeletesMerges;

    public PredeterminedMergePolicy(
        MergePolicy.MergeSpecification merges,
        MergePolicy.MergeSpecification forcedMerges,
        MergePolicy.MergeSpecification forcedDeletesMerges) {
      this.merges = merges;
      this.forcedMerges = forcedMerges;
      this.forcedDeletesMerges = forcedDeletesMerges;
    }

    @Override
    public MergePolicy.MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      return merges;
    }

    @Override
    public MergePolicy.MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                                           Map<SegmentCommitInfo,Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
      return forcedMerges;
    }

    @Override
    public MergePolicy.MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      return forcedDeletesMerges;
    }

  }

  private static class WrappedOneMerge extends MergePolicy.OneMerge {

    final MergePolicy.OneMerge original;

    public WrappedOneMerge(MergePolicy.OneMerge original) {
      super(original.segments);
      this.original = original;
    }

  }

  @Test
  public void testSegmentsAreWrapped() throws IOException {
    try (final Directory dir = newDirectory()) {
      // first create random merge specs
      final MergePolicy.MergeSpecification msM = createRandomMergeSpecification(dir);
      final MergePolicy.MergeSpecification msF = createRandomMergeSpecification(dir);
      final MergePolicy.MergeSpecification msD = createRandomMergeSpecification(dir);
      // secondly, pass them to the predetermined merge policy constructor
      final MergePolicy originalMP = new PredeterminedMergePolicy(msM, msF, msD);
      // thirdly wrap the predetermined merge policy
      final MergePolicy oneMergeWrappingMP = new OneMergeWrappingMergePolicy(
          originalMP,
          merge -> new WrappedOneMerge(merge));
      // finally, ask for merges and check what we got
      implTestSegmentsAreWrapped(msM, oneMergeWrappingMP.findMerges(null, null, null));
      implTestSegmentsAreWrapped(msF, oneMergeWrappingMP.findForcedMerges(null, 0, null, null));
      implTestSegmentsAreWrapped(msD, oneMergeWrappingMP.findForcedDeletesMerges(null, null));
    }
  }

  private static void implTestSegmentsAreWrapped(MergePolicy.MergeSpecification originalMS, MergePolicy.MergeSpecification testMS) {
    // wrapping does not add or remove merge specs
    assertEquals((originalMS == null), (testMS == null));
    if (originalMS == null) return;
    assertEquals(originalMS.merges.size(), testMS.merges.size());
    // wrapping does not re-order merge specs
    for (int ii = 0; ii < originalMS.merges.size(); ++ii) {
        final MergePolicy.OneMerge originalOM = originalMS.merges.get(ii);
        final MergePolicy.OneMerge testOM = testMS.merges.get(ii);
        // wrapping wraps
        assertTrue(testOM instanceof WrappedOneMerge);
        final WrappedOneMerge wrappedOM = (WrappedOneMerge)testOM;
        // and what is wrapped is what was originally passed in
        assertEquals(originalOM, wrappedOM.original);
    }
  }

  private static MergePolicy.MergeSpecification createRandomMergeSpecification(Directory dir) {
    MergePolicy.MergeSpecification ms;
    if (0 < random().nextInt(10)) { // ~ 1 in 10 times return null
      ms = new MergePolicy.MergeSpecification();
      // append up to 10 (random non-sensical) one merge objects
      for (int ii = 0; ii < random().nextInt(10); ++ii) {
        final SegmentInfo si = new SegmentInfo(
            dir, // dir
            Version.LATEST, // version
            Version.LATEST, // min version
            TestUtil.randomSimpleString(random()), // name
            random().nextInt(), // maxDoc
            random().nextBoolean(), // isCompoundFile
            null, // codec
            Collections.emptyMap(), // diagnostics
            TestUtil.randomSimpleString(// id
                random(),
                StringHelper.ID_LENGTH,
                StringHelper.ID_LENGTH).getBytes(StandardCharsets.US_ASCII),
            Collections.emptyMap(), // attributes
            null /* indexSort */);
        final List<SegmentCommitInfo> segments = new LinkedList<SegmentCommitInfo>();
        segments.add(new SegmentCommitInfo(si, 0, 0, 0, 0, 0, StringHelper.randomId()));
        ms.add(new MergePolicy.OneMerge(segments));
      }
    }
    return null;
  }

}
