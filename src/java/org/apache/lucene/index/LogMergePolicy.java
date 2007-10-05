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
import java.util.Set;

import org.apache.lucene.store.Directory;

/** <p>This class implements a {@link MergePolicy} that tries
 *  to merge segments into levels of exponentially
 *  increasing size, where each level has < mergeFactor
 *  segments in it.  Whenever a given levle has mergeFactor
 *  segments or more in it, they will be merged.</p>
 *
 * <p>This class is abstract and requires a subclass to
 * define the {@link #size} method which specifies how a
 * segment's size is determined.  {@link LogDocMergePolicy}
 * is one subclass that measures size by document count in
 * the segment.  {@link LogByteSizeMergePolicy} is another
 * subclass that measures size as the total byte size of the
 * file(s) for the segment.</p>
 */

public abstract class LogMergePolicy implements MergePolicy {

  /** Defines the allowed range of log(size) for each
   *  level.  A level is computed by taking the max segment
   *  log size, minuse LEVEL_LOG_SPAN, and finding all
   *  segments falling within that range. */
  public static final double LEVEL_LOG_SPAN = 0.75;

  /** Default merge factor, which is how many segments are
   *  merged at a time */
  public static final int DEFAULT_MERGE_FACTOR = 10;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged.  @see setMaxMergeDocs */
  public static final int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;

  private int mergeFactor = DEFAULT_MERGE_FACTOR;

  long minMergeSize;
  long maxMergeSize;
  int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;

  private boolean useCompoundFile = true;
  private boolean useCompoundDocStore = true;

  /** <p>Returns the number of segments that are merged at
   * once and also controls the total number of segments
   * allowed to accumulate in the index.</p> */
  public int getMergeFactor() {
    return mergeFactor;
  }

  /** Determines how often segment indices are merged by
   * addDocument().  With smaller values, less RAM is used
   * while indexing, and searches on unoptimized indices are
   * faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while
   * searches on unoptimized indices are slower, indexing is
   * faster.  Thus larger values (> 10) are best for batch
   * index creation, and smaller values (< 10) for indices
   * that are interactively maintained. */
  public void setMergeFactor(int mergeFactor) {
    if (mergeFactor < 2)
      throw new IllegalArgumentException("mergeFactor cannot be less than 2");
    this.mergeFactor = mergeFactor;
  }

  // Javadoc inherited
  public boolean useCompoundFile(SegmentInfos infos, SegmentInfo info) {
    return useCompoundFile;
  }

  /** Sets whether compound file format should be used for
   *  newly flushed and newly merged segments. */
  public void setUseCompoundFile(boolean useCompoundFile) {
    this.useCompoundFile = useCompoundFile;
  }

  /** Returns true if newly flushed and newly merge segments
   *  are written in compound file format. @see
   *  #setUseCompoundFile */
  public boolean getUseCompoundFile() {
    return useCompoundFile;
  }

  // Javadoc inherited
  public boolean useCompoundDocStore(SegmentInfos infos) {
    return useCompoundDocStore;
  }

  /** Sets whether compound file format should be used for
   *  newly flushed and newly merged doc store
   *  segment files (term vectors and stored fields). */
  public void setUseCompoundDocStore(boolean useCompoundDocStore) {
    this.useCompoundDocStore = useCompoundDocStore;
  }

  /** Returns true if newly flushed and newly merge doc
   *  store segment files (term vectors and stored fields)
   *  are written in compound file format. @see
   *  #setUseCompoundDocStore */
  public boolean getUseCompoundDocStore() {
    return useCompoundDocStore;
  }

  public void close() {}

  abstract protected long size(SegmentInfo info) throws IOException;

  private boolean isOptimized(SegmentInfos infos, IndexWriter writer, int maxNumSegments, Set segmentsToOptimize) throws IOException {
    final int numSegments = infos.size();
    int numToOptimize = 0;
    SegmentInfo optimizeInfo = null;
    for(int i=0;i<numSegments && numToOptimize <= maxNumSegments;i++) {
      final SegmentInfo info = infos.info(i);
      if (segmentsToOptimize.contains(info)) {
        numToOptimize++;
        optimizeInfo = info;
      }
    }

    return numToOptimize <= maxNumSegments &&
      (numToOptimize != 1 || isOptimized(writer, optimizeInfo));
  }

  /** Returns true if this single nfo is optimized (has no
   *  pending norms or deletes, is in the same dir as the
   *  writer, and matches the current compound file setting */
  private boolean isOptimized(IndexWriter writer, SegmentInfo info)
    throws IOException {
    return !info.hasDeletions() &&
      !info.hasSeparateNorms() &&
      info.dir == writer.getDirectory() &&
      info.getUseCompoundFile() == useCompoundFile;
  }

  /** Returns the merges necessary to optimize the index.
   *  This merge policy defines "optimized" to mean only one
   *  segment in the index, where that segment has no
   *  deletions pending nor separate norms, and it is in
   *  compound file format if the current useCompoundFile
   *  setting is true.  This method returns multiple merges
   *  (mergeFactor at a time) so the {@link MergeScheduler}
   *  in use may make use of concurrency. */
  public MergeSpecification findMergesForOptimize(SegmentInfos infos, IndexWriter writer, int maxNumSegments, Set segmentsToOptimize) throws IOException {
    final Directory dir = writer.getDirectory();
    MergeSpecification spec;
    
    if (!isOptimized(infos, writer, maxNumSegments, segmentsToOptimize)) {

      int numSegments = infos.size();
      while(numSegments > 0) {
        final SegmentInfo info = infos.info(--numSegments);
        if (segmentsToOptimize.contains(info)) {
          numSegments++;
          break;
        }
      }

      if (numSegments > 0) {

        spec = new MergeSpecification();
        while (numSegments > 0) {
        
          final int first;
          if (numSegments > mergeFactor)
            first = numSegments-mergeFactor;
          else
            first = 0;

          if (numSegments > 1 || !isOptimized(writer, infos.info(0)))
            spec.add(new OneMerge(infos.range(first, numSegments), useCompoundFile));

          numSegments -= mergeFactor;
        }

      } else
        spec = null;
    } else
      spec = null;

    return spec;
  }

  /** Checks if any merges are now necessary and returns a
   *  {@link MergePolicy.MergeSpecification} if so.  A merge
   *  is necessary when there are more than {@link
   *  #setMergeFactor} segments at a given level.  When
   *  multiple levels have too many segments, this method
   *  will return multiple merges, allowing the {@link
   *  MergeScheduler} to use concurrency. */
  public MergeSpecification findMerges(SegmentInfos infos, IndexWriter writer) throws IOException {

    final int numSegments = infos.size();

    // Compute levels, which is just log (base mergeFactor)
    // of the size of each segment
    float[] levels = new float[numSegments];
    final float norm = (float) Math.log(mergeFactor);

    final Directory directory = writer.getDirectory();

    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = infos.info(i);
      long size = size(info);

      // Refuse to import a segment that's too large
      if (info.docCount > maxMergeDocs && info.dir != directory)
        throw new IllegalArgumentException("Segment is too large (" + info.docCount + " docs vs max docs " + maxMergeDocs + ")");

      if (size >= maxMergeSize && info.dir != directory)
        throw new IllegalArgumentException("Segment is too large (" + size + " vs max size " + maxMergeSize + ")");

      // Floor tiny segments
      if (size < 1)
        size = 1;
      levels[i] = (float) Math.log(size)/norm;
    }

    final float levelFloor;
    if (minMergeSize <= 0)
      levelFloor = (float) 0.0;
    else
      levelFloor = (float) (Math.log(minMergeSize)/norm);

    // Now, we quantize the log values into levels.  The
    // first level is any segment whose log size is within
    // LEVEL_LOG_SPAN of the max size, or, who has such as
    // segment "to the right".  Then, we find the max of all
    // other segments and use that to define the next level
    // segment, etc.

    MergeSpecification spec = null;

    int start = 0;
    while(start < numSegments) {

      // Find max level of all segments not already
      // quantized.
      float maxLevel = levels[start];
      for(int i=1+start;i<numSegments;i++) {
        final float level = levels[i];
        if (level > maxLevel)
          maxLevel = level;
      }

      // Now search backwards for the rightmost segment that
      // falls into this level:
      float levelBottom;
      if (maxLevel < levelFloor)
        // All remaining segments fall into the min level
        levelBottom = -1.0F;
      else {
        levelBottom = (float) (maxLevel - LEVEL_LOG_SPAN);

        // Force a boundary at the level floor
        if (levelBottom < levelFloor && maxLevel >= levelFloor)
          levelBottom = levelFloor;
      }

      int upto = numSegments-1;
      while(upto >= start) {
        if (levels[upto] >= levelBottom) {
          break;
        }
        upto--;
      }

      // Finally, record all merges that are viable at this level:
      int end = start + mergeFactor;
      while(end <= 1+upto) {
        boolean anyTooLarge = false;
        for(int i=start;i<end;i++) {
          final SegmentInfo info = infos.info(i);
          anyTooLarge |= (size(info) >= maxMergeSize || info.docCount >= maxMergeDocs);
        }

        if (!anyTooLarge) {
          if (spec == null)
            spec = new MergeSpecification();
          spec.add(new OneMerge(infos.range(start, end), useCompoundFile));
        }
        start = end;
        end = start + mergeFactor;
      }

      start = 1+upto;
    }

    return spec;
  }

  /** Sets the maximum docs for a segment to be merged.
   *  When a segment has this many docs or more it will never be
   *  merged. */
  public void setMaxMergeDocs(int maxMergeDocs) {
    this.maxMergeDocs = maxMergeDocs;
  }

  /** Get the maximum docs for a segment to be merged.
   *  @see #setMaxMergeDocs */
  public int getMaxMergeDocs() {
    return maxMergeDocs;
  }

}
