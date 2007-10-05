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

/** This is a {@link LogMergePolicy} that measures size of a
 *  segment as the total byte size of the segment's files. */
public class LogByteSizeMergePolicy extends LogMergePolicy {

  /** Default minimum segment size.  @see setMinMergeMB */
  public static final double DEFAULT_MIN_MERGE_MB = 1.6;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged.  @see setMaxMergeMB */
  public static final double DEFAULT_MAX_MERGE_MB = (double) Long.MAX_VALUE;

  public LogByteSizeMergePolicy() {
    super();
    minMergeSize = (long) (DEFAULT_MIN_MERGE_MB*1024*1024);
    maxMergeSize = (long) (DEFAULT_MAX_MERGE_MB*1024*1024);
  }
  protected long size(SegmentInfo info) throws IOException {
    return info.sizeInBytes();
  }

  /** Sets the maximum size for a segment to be merged.
   *  When a segment is this size or larger it will never be
   *  merged.  Note that {@link #setMaxMergeDocs} is also
   *  used to check whether a segment is too large for
   *  merging (it's either or). */
  public void setMaxMergeMB(double mb) {
    maxMergeSize = (long) (mb*1024*1024);
  }

  /** Get the maximum size for a segment to be merged.
   *  @see #setMaxMergeMB */
  public double getMaxMergeMB() {
    return ((double) maxMergeSize)/1024/1024;
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
  public void setMinMergeMB(double mb) {
    minMergeSize = (long) (mb*1024*1024);
  }

  /** Get the minimum size for a segment to remain
   *  un-merged.
   *  @see #setMinMergeMB **/
  public double getMinMergeMB() {
    return ((double) minMergeSize)/1024/1024;
  }
}

