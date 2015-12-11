package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

/**
 * A {@link MergePolicy} that only returns forced merges.
 * <p><b>NOTE</b>: Use this policy if you wish to disallow background
 * merges but wish to run optimize/forceMerge segment merges.
 *
 *  @lucene.experimental
 */
public final class ForceMergePolicy extends MergePolicy {

  final MergePolicy in;

  /** Create a new {@code ForceMergePolicy} around the given {@code MergePolicy} */
  public ForceMergePolicy(MergePolicy in) {
    this.in = in;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger,
      SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    return null;
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
      int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
      throws IOException {
    return in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    return in.findForcedDeletesMerges(segmentInfos, writer);
  }

  @Override
  public boolean useCompoundFile(SegmentInfos segments,
      SegmentCommitInfo newSegment, IndexWriter writer) throws IOException {
    return in.useCompoundFile(segments, newSegment, writer);
  }

  @Override
  protected long size(SegmentCommitInfo info, IndexWriter writer) throws IOException {
    return in.size(info, writer);
  }

  @Override
  public String toString() {
    return "ForceMergePolicy(" + in + ")";
  }
}
