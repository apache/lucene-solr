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

/**
 * Callback interface to signal various actions taken by IndexWriter.
 *
 * @lucene.experimental
 */
public interface IndexWriterEvents {
  /**
   * A default implementation that ignores all events.
   */
  IndexWriterEvents NULL_EVENTS = new IndexWriterEvents() {
    @Override
    public void beginMergeOnCommit() { }

    @Override
    public void finishMergeOnCommit() { }

    @Override
    public void abandonedMergesOnCommit(int abandonedCount) { }
  };

  /**
   * Signals the start of waiting for a merge on commit, returned from
   * {@link MergePolicy#findFullFlushMerges(MergeTrigger, SegmentInfos, MergePolicy.MergeContext)}.
   */
  void beginMergeOnCommit();

  /**
   * Signals the end of waiting for merges on commit. This may be either because the merges completed, or because we timed out according
   * to the limit set in {@link IndexWriterConfig#setMaxCommitMergeWaitSeconds(long)}.
   */
  void finishMergeOnCommit();

  /**
   * Called to signal that we abandoned some merges on commit upon reaching the timeout specified in
   * {@link IndexWriterConfig#setMaxCommitMergeWaitSeconds(long)}.
   */
  void abandonedMergesOnCommit(int abandonedCount);
}
