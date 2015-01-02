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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/** Wraps another {@link MergePolicy}, except when forceMerge is requested, if the wrapped merge
 *  policy woulid do nothing, this one will always merge the one segment. */
public class AlwaysForceMergePolicy extends MergePolicy {

  /** Wrapped {@link MergePolicy}. */
  protected final MergePolicy in;

  /** Wrap the given {@link MergePolicy} and intercept forceMerge requests to
   * only upgrade segments written with previous Lucene versions. */
  public AlwaysForceMergePolicy(MergePolicy in) {
    this.in = in;
  }

  private boolean didForceMerge;
  
  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    return in.findMerges(null, segmentInfos, writer);
  }

  /** Call this to "force" a force merge again. */
  public void reset() {
    didForceMerge = false;
  }
  
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    MergeSpecification spec = in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
    if (spec == null && didForceMerge == false) {
      didForceMerge = true;
      List<SegmentCommitInfo> infos = new ArrayList<>();
      for(SegmentCommitInfo info : segmentInfos) {
        infos.add(info);
      }
      if (infos.isEmpty()) {
        spec = null;
      } else {
        spec = new MergeSpecification();
        spec.add(new OneMerge(infos));
      }
    }
    return spec;
  }
  
  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    return in.findForcedDeletesMerges(segmentInfos, writer);
  }
  
  @Override
  public boolean useCompoundFile(SegmentInfos segments, SegmentCommitInfo newSegment, IndexWriter writer) throws IOException {
    return in.useCompoundFile(segments, newSegment, writer);
  }
  
  @Override
  public String toString() {
    return "[" + getClass().getSimpleName() + "->" + in + "]";
  }
}
