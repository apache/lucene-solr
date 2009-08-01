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

/** <p>This class implements a {@link MergePolicy} that tries
 *  to merge segments into levels of exponentially
 *  increasing size, where each level has fewer segments than
 *  the value of the merge factor. Whenever extra segments
 *  (beyond the merge factor upper bound) are encountered,
 *  all segments within the level are merged. You can get or
 *  set the merge factor using {@link #getMergeFactor()} and
 *  {@link #setMergeFactor(int)} respectively.</p>
 *
 * <p>This class is abstract and requires a subclass to
 * define the {@link #size} method which specifies how a
 * segment's size is determined.  {@link LogDocMergePolicy}
 * is one subclass that measures size by document count in
 * the segment.  {@link LogByteSizeMergePolicy} is another
 * subclass that measures size as the total byte size of the
 * file(s) for the segment.</p>
 */

public abstract class LogMergePolicy extends MergePolicy {

  /** Defines the allowed range of log(size) for each
   *  level.  A level is computed by taking the max segment
   *  log size, minus LEVEL_LOG_SPAN, and finding all
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

  /* TODO 3.0: change this default to true */
  protected boolean calibrateSizeByDeletes = false;
  
  private boolean useCompoundFile = true;
  private boolean useCompoundDocStore = true;

  public LogMergePolicy(IndexWriter writer) {
    super(writer);
  }
  
  protected boolean verbose() {
    return writer != null && writer.verbose();
  }
  
  private void message(String message) {
    if (verbose())
      writer.message("LMP: " + message);
  }

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

  /** Sets whether the segment size should be calibrated by
   *  the number of deletes when choosing segments for merge. */
  public void setCalibrateSizeByDeletes(boolean calibrateSizeByDeletes) {
    this.calibrateSizeByDeletes = calibrateSizeByDeletes;
  }

  /** Returns true if the segment size should be calibrated 
   *  by the number of deletes when choosing segments for merge. */
  public boolean getCalibrateSizeByDeletes() {
    return calibrateSizeByDeletes;
  }

  public void close() {}

  abstract protected long size(SegmentInfo info) throws IOException;

  protected long sizeDocs(SegmentInfo info) throws IOException {
    if (calibrateSizeByDeletes) {
      int delCount = writer.numDeletedDocs(info);
      return (info.docCount - (long)delCount);
    } else {
      return info.docCount;
    }
  }
  
  protected long sizeBytes(SegmentInfo info) throws IOException {
    long byteSize = info.sizeInBytes();
    if (calibrateSizeByDeletes) {
      int delCount = writer.numDeletedDocs(info);
      float delRatio = (info.docCount <= 0 ? 0.0f : ((float)delCount / (float)info.docCount));
      return (info.docCount <= 0 ?  byteSize : (long)(byteSize * (1.0f - delRatio)));
    } else {
      return byteSize;
    }
  }
  
  private boolean isOptimized(SegmentInfos infos, int maxNumSegments, Set segmentsToOptimize) throws IOException {
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
      (numToOptimize != 1 || isOptimized(optimizeInfo));
  }

  /** Returns true if this single info is optimized (has no
   *  pending norms or deletes, is in the same dir as the
   *  writer, and matches the current compound file setting */
  private boolean isOptimized(SegmentInfo info)
    throws IOException {
    boolean hasDeletions = writer.numDeletedDocs(info) > 0;
    return !hasDeletions &&
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
  public MergeSpecification findMergesForOptimize(SegmentInfos infos,
      int maxNumSegments, Set segmentsToOptimize) throws IOException {
    MergeSpecification spec;

    assert maxNumSegments > 0;

    if (!isOptimized(infos, maxNumSegments, segmentsToOptimize)) {

      // Find the newest (rightmost) segment that needs to
      // be optimized (other segments may have been flushed
      // since optimize started):
      int last = infos.size();
      while(last > 0) {
        final SegmentInfo info = infos.info(--last);
        if (segmentsToOptimize.contains(info)) {
          last++;
          break;
        }
      }

      if (last > 0) {

        spec = new MergeSpecification();

        // First, enroll all "full" merges (size
        // mergeFactor) to potentially be run concurrently:
        while (last - maxNumSegments + 1 >= mergeFactor) {
          spec.add(new OneMerge(infos.range(last-mergeFactor, last), useCompoundFile));
          last -= mergeFactor;
        }

        // Only if there are no full merges pending do we
        // add a final partial (< mergeFactor segments) merge:
        if (0 == spec.merges.size()) {
          if (maxNumSegments == 1) {

            // Since we must optimize down to 1 segment, the
            // choice is simple:
            if (last > 1 || !isOptimized(infos.info(0)))
              spec.add(new OneMerge(infos.range(0, last), useCompoundFile));
          } else if (last > maxNumSegments) {

            // Take care to pick a partial merge that is
            // least cost, but does not make the index too
            // lopsided.  If we always just picked the
            // partial tail then we could produce a highly
            // lopsided index over time:

            // We must merge this many segments to leave
            // maxNumSegments in the index (from when
            // optimize was first kicked off):
            final int finalMergeSize = last - maxNumSegments + 1;

            // Consider all possible starting points:
            long bestSize = 0;
            int bestStart = 0;

            for(int i=0;i<last-finalMergeSize+1;i++) {
              long sumSize = 0;
              for(int j=0;j<finalMergeSize;j++)
                sumSize += size(infos.info(j+i));
              if (i == 0 || (sumSize < 2*size(infos.info(i-1)) && sumSize < bestSize)) {
                bestStart = i;
                bestSize = sumSize;
              }
            }

            spec.add(new OneMerge(infos.range(bestStart, bestStart+finalMergeSize), useCompoundFile));
          }
        }
        
      } else
        spec = null;
    } else
      spec = null;

    return spec;
  }

  /**
   * Finds merges necessary to expunge all deletes from the
   * index.  We simply merge adjacent segments that have
   * deletes, up to mergeFactor at a time.
   */ 
  public MergeSpecification findMergesToExpungeDeletes(SegmentInfos segmentInfos)
      throws CorruptIndexException, IOException {
    final int numSegments = segmentInfos.size();

    if (verbose())
      message("findMergesToExpungeDeletes: " + numSegments + " segments");

    MergeSpecification spec = new MergeSpecification();
    int firstSegmentWithDeletions = -1;
    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = segmentInfos.info(i);
      int delCount = writer.numDeletedDocs(info);
      if (delCount > 0) {
        if (verbose())
          message("  segment " + info.name + " has deletions");
        if (firstSegmentWithDeletions == -1)
          firstSegmentWithDeletions = i;
        else if (i - firstSegmentWithDeletions == mergeFactor) {
          // We've seen mergeFactor segments in a row with
          // deletions, so force a merge now:
          if (verbose())
            message("  add merge " + firstSegmentWithDeletions + " to " + (i-1) + " inclusive");
          spec.add(new OneMerge(segmentInfos.range(firstSegmentWithDeletions, i), useCompoundFile));
          firstSegmentWithDeletions = i;
        }
      } else if (firstSegmentWithDeletions != -1) {
        // End of a sequence of segments with deletions, so,
        // merge those past segments even if it's fewer than
        // mergeFactor segments
        if (verbose())
          message("  add merge " + firstSegmentWithDeletions + " to " + (i-1) + " inclusive");
        spec.add(new OneMerge(segmentInfos.range(firstSegmentWithDeletions, i), useCompoundFile));
        firstSegmentWithDeletions = -1;
      }
    }

    if (firstSegmentWithDeletions != -1) {
      if (verbose())
        message("  add merge " + firstSegmentWithDeletions + " to " + (numSegments-1) + " inclusive");
      spec.add(new OneMerge(segmentInfos.range(firstSegmentWithDeletions, numSegments), useCompoundFile));
    }

    return spec;
  }

  /** Checks if any merges are now necessary and returns a
   *  {@link MergePolicy.MergeSpecification} if so.  A merge
   *  is necessary when there are more than {@link
   *  #setMergeFactor} segments at a given level.  When
   *  multiple levels have too many segments, this method
   *  will return multiple merges, allowing the {@link
   *  MergeScheduler} to use concurrency. */
  public MergeSpecification findMerges(SegmentInfos infos) throws IOException {

    final int numSegments = infos.size();
    if (verbose())
      message("findMerges: " + numSegments + " segments");

    // Compute levels, which is just log (base mergeFactor)
    // of the size of each segment
    float[] levels = new float[numSegments];
    final float norm = (float) Math.log(mergeFactor);

    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = infos.info(i);
      long size = size(info);

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
      if (verbose())
        message("  level " + levelBottom + " to " + maxLevel + ": " + (1+upto-start) + " segments");

      // Finally, record all merges that are viable at this level:
      int end = start + mergeFactor;
      while(end <= 1+upto) {
        boolean anyTooLarge = false;
        for(int i=start;i<end;i++) {
          final SegmentInfo info = infos.info(i);
          anyTooLarge |= (size(info) >= maxMergeSize || sizeDocs(info) >= maxMergeDocs);
        }

        if (!anyTooLarge) {
          if (spec == null)
            spec = new MergeSpecification();
          if (verbose())
            message("    " + start + " to " + end + ": add this merge");
          spec.add(new OneMerge(infos.range(start, end), useCompoundFile));
        } else if (verbose())
          message("    " + start + " to " + end + ": contains segment over maxMergeSize or maxMergeDocs; skipping");

        start = end;
        end = start + mergeFactor;
      }

      start = 1+upto;
    }

    return spec;
  }

  /** <p>Determines the largest segment (measured by
   * document count) that may be merged with other segments.
   * Small values (e.g., less than 10,000) are best for
   * interactive indexing, as this limits the length of
   * pauses while indexing to a few seconds.  Larger values
   * are best for batched indexing and speedier
   * searches.</p>
   *
   * <p>The default value is {@link Integer#MAX_VALUE}.</p>
   *
   * <p>The default merge policy ({@link
   * LogByteSizeMergePolicy}) also allows you to set this
   * limit by net size (in MB) of the segment, using {@link
   * LogByteSizeMergePolicy#setMaxMergeMB}.</p>
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    this.maxMergeDocs = maxMergeDocs;
  }

  /** Returns the largest segment (measured by document
   *  count) that may be merged with other segments.
   *  @see #setMaxMergeDocs */
  public int getMaxMergeDocs() {
    return maxMergeDocs;
  }

}
