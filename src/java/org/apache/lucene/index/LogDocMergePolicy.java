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

/** This is a {@link LogMergePolicy} that measures size of a
 *  segment as the number of documents (not taking deletions
 *  into account). */

public class LogDocMergePolicy extends LogMergePolicy {

  /** Default minimum segment size.  @see setMinMergeDocs */
  public static final int DEFAULT_MIN_MERGE_DOCS = 1000;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged.  @see setMaxMergeDocs */
  public static final int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;

  public LogDocMergePolicy() {
    super();
    minMergeSize = DEFAULT_MIN_MERGE_DOCS;
    maxMergeSize = DEFAULT_MAX_MERGE_DOCS;
  }
  protected long size(SegmentInfo info) {
    return info.docCount;
  }

  /** Sets the maximum size for a segment to be merged.
   *  When a segment is this size or larger it will never be
   *  merged. */
  public void setMaxMergeDocs(int maxMergeDocs) {
    maxMergeSize = maxMergeDocs;
  }

  /** Get the maximum size for a segment to be merged.
   *  @see #setMaxMergeDocs */
  public int getMaxMergeDocs() {
    return (int) maxMergeSize;
  }

  /** Sets the minimum size for the lowest level segments.
   * Any segments below this size are considered to be on
   * the same level (even if they vary drastically in size)
   * and will be merged whenever there are mergeFactor of
   * them.  This effectively truncates the "long tail" of
   * small segments that would otherwise be created into a
   * single level.  If you set this too large, it could
   * greatly increase the merging cost during indexing (if
   * you flush many small segments). */
  public void setMinMergeDocs(int minMergeDocs) {
    minMergeSize = minMergeDocs;
  }

  /** Get the minimum size for a segment to remain
   *  un-merged.
   *  @see #setMinMergeDocs **/
  public int getMinMergeDocs() {
    return (int) minMergeSize;
  }
}

