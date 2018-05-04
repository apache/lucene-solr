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
 * A wrapper for {@link MergePolicy} instances.
 *
 * @lucene.experimental
 */
public class FilterMergePolicy extends MergePolicy {

  /** The wrapped {@link MergePolicy}. */
  protected final MergePolicy in;

  /**
   * Creates a new filter merge policy instance wrapping another.
   *
   * @param in the wrapped {@link MergePolicy}
   */
  public FilterMergePolicy(MergePolicy in) {
    this.in = in;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    return in.findMerges(mergeTrigger, segmentInfos, writer);
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
      Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    return in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    return in.findForcedDeletesMerges(segmentInfos, writer);
  }

  @Override
  public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, IndexWriter writer)
      throws IOException {
    return in.useCompoundFile(infos, mergedInfo, writer);
  }

  @Override
  protected long size(SegmentCommitInfo info, IndexWriter writer) throws IOException {
    return in.size(info, writer);
  }

  @Override
  public double getNoCFSRatio() {
    return in.getNoCFSRatio();
  }

  @Override
  public final void setNoCFSRatio(double noCFSRatio) {
    in.setNoCFSRatio(noCFSRatio);
  }

  @Override
  public final void setMaxCFSSegmentSizeMB(double v) {
    in.setMaxCFSSegmentSizeMB(v);
  }

  @Override
  public final double getMaxCFSSegmentSizeMB() {
    return in.getMaxCFSSegmentSizeMB();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + in + ")";
  }

  @Override
  public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return in.keepFullyDeletedSegment(readerIOSupplier);
  }

  @Override
  public int numDeletesToMerge(SegmentCommitInfo info, int pendingDeleteCount,
                               IOSupplier<CodecReader> readerSupplier) throws IOException {
    return in.numDeletesToMerge(info, pendingDeleteCount, readerSupplier);
  }
}
