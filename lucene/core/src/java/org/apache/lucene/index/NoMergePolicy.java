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
import java.util.Map;

import org.apache.lucene.util.IOSupplier;

/**
 * A {@link MergePolicy} which never returns merges to execute. Use it if you
 * want to prevent segment merges.
 */
public final class NoMergePolicy extends MergePolicy {

  /** Singleton instance. */
  public static final MergePolicy INSTANCE = new NoMergePolicy();

  private NoMergePolicy() {
    super();
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) { return null; }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
                                             int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, MergeContext mergeContext) { return null; }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) { return null; }

  @Override
  public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) { return null; }

  @Override
  public boolean useCompoundFile(SegmentInfos segments, SegmentCommitInfo newSegment, MergeContext mergeContext) {
    return newSegment.info.getUseCompoundFile();
  }

  @Override
  protected long size(SegmentCommitInfo info, MergeContext context) throws IOException {
    return Long.MAX_VALUE;
  }

  @Override
  public double getNoCFSRatio() {
    return super.getNoCFSRatio();
  }

  @Override
  public double getMaxCFSSegmentSizeMB() {
    return super.getMaxCFSSegmentSizeMB();
  }

  @Override
  public void setMaxCFSSegmentSizeMB(double v) {
    super.setMaxCFSSegmentSizeMB(v);
  }

  @Override
  public void setNoCFSRatio(double noCFSRatio) {
    super.setNoCFSRatio(noCFSRatio);
  }

  @Override
  public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return super.keepFullyDeletedSegment(readerIOSupplier);
  }

  @Override
  public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) throws IOException {
    return  super.numDeletesToMerge(info, delCount, readerSupplier);
  }

  @Override
  public String toString() {
    return "NoMergePolicy";
  }
}
