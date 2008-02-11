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

import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

/**
 * <p>Expert: a MergePolicy determines the sequence of
 * primitive merge operations to be used for overall merge
 * and optimize operations.</p>
 * 
 * <p>Whenever the segments in an index have been altered by
 * {@link IndexWriter}, either the addition of a newly
 * flushed segment, addition of many segments from
 * addIndexes* calls, or a previous merge that may now need
 * to cascade, {@link IndexWriter} invokes {@link
 * #findMerges} to give the MergePolicy a chance to pick
 * merges that are now required.  This method returns a
 * {@link MergeSpecification} instance describing the set of
 * merges that should be done, or null if no merges are
 * necessary.  When IndexWriter.optimize is called, it calls
 * {@link #findMergesForOptimize} and the MergePolicy should
 * then return the necessary merges.</p>
 *
 * <p>Note that the policy can return more than one merge at
 * a time.  In this case, if the writer is using {@link
 * SerialMergeScheduler}, the merges will be run
 * sequentially but if it is using {@link
 * ConcurrentMergeScheduler} they will be run concurrently.</p>
 * 
 * <p>The default MergePolicy is {@link
 * LogByteSizeMergePolicy}.</p>
 * <p><b>NOTE:</b> This API is new and still experimental
 * (subject to change suddenly in the next release)</p>
 */

public abstract class MergePolicy {

  /** OneMerge provides the information necessary to perform
   *  an individual primitive merge operation, resulting in
   *  a single new segment.  The merge spec includes the
   *  subset of segments to be merged as well as whether the
   *  new segment should use the compound file format. */

  public static class OneMerge {

    SegmentInfo info;               // used by IndexWriter
    boolean mergeDocStores;         // used by IndexWriter
    boolean optimize;               // used by IndexWriter
    SegmentInfos segmentsClone;     // used by IndexWriter
    boolean increfDone;             // used by IndexWriter
    boolean registerDone;           // used by IndexWriter
    long mergeGen;                  // used by IndexWriter
    boolean isExternal;             // used by IndexWriter
    int maxNumSegmentsOptimize;     // used by IndexWriter

    final SegmentInfos segments;
    final boolean useCompoundFile;
    boolean aborted;
    Throwable error;

    public OneMerge(SegmentInfos segments, boolean useCompoundFile) {
      if (0 == segments.size())
        throw new RuntimeException("segments must include at least one segment");
      this.segments = segments;
      this.useCompoundFile = useCompoundFile;
    }

    /** Record that an exception occurred while executing
     *  this merge */
    synchronized void setException(Throwable error) {
      this.error = error;
    }

    /** Retrieve previous exception set by {@link
     *  #setException}. */
    synchronized Throwable getException() {
      return error;
    }

    /** Mark this merge as aborted.  If this is called
     *  before the merge is committed then the merge will
     *  not be committed. */
    synchronized void abort() {
      aborted = true;
    }

    /** Returns true if this merge was aborted. */
    synchronized boolean isAborted() {
      return aborted;
    }

    synchronized void checkAborted(Directory dir) throws MergeAbortedException {
      if (aborted)
        throw new MergeAbortedException("merge is aborted: " + segString(dir));
    }

    String segString(Directory dir) {
      StringBuffer b = new StringBuffer();
      final int numSegments = segments.size();
      for(int i=0;i<numSegments;i++) {
        if (i > 0) b.append(' ');
        b.append(segments.info(i).segString(dir));
      }
      if (info != null)
        b.append(" into ").append(info.name);
      if (optimize)
        b.append(" [optimize]");
      return b.toString();
    }
  }

  /**
   * A MergeSpecification instance provides the information
   * necessary to perform multiple merges.  It simply
   * contains a list of {@link OneMerge} instances.
   */

  public static class MergeSpecification {

    /**
     * The subset of segments to be included in the primitive merge.
     */

    public List merges = new ArrayList();

    public void add(OneMerge merge) {
      merges.add(merge);
    }

    public String segString(Directory dir) {
      StringBuffer b = new StringBuffer();
      b.append("MergeSpec:\n");
      final int count = merges.size();
      for(int i=0;i<count;i++)
        b.append("  ").append(1 + i).append(": ").append(((OneMerge) merges.get(i)).segString(dir));
      return b.toString();
    }
  }

  /** Exception thrown if there are any problems while
   *  executing a merge. */
  public static class MergeException extends RuntimeException {
    private Directory dir;
    /** @deprecated
     *  Use {@link #MergePolicy.MergeException(String,Directory)} instead */
    public MergeException(String message) {
      super(message);
    }
    public MergeException(String message, Directory dir) {
      super(message);
      this.dir = dir;
    }
    /** @deprecated
     *  Use {@link #MergePolicy.MergeException(Throwable,Directory)} instead */
    public MergeException(Throwable exc) {
      super(exc);
    }
    public MergeException(Throwable exc, Directory dir) {
      super(exc);
      this.dir = dir;
    }
    /** Returns the {@link Directory} of the index that hit
     *  the exception. */
    public Directory getDirectory() {
      return dir;
    }
  }

  public static class MergeAbortedException extends IOException {
    public MergeAbortedException() {
      super("merge is aborted");
    }
    public MergeAbortedException(String message) {
      super(message);
    }
  }

  /**
   * Determine what set of merge operations are now
   * necessary on the index.  The IndexWriter calls this
   * whenever there is a change to the segments.  This call
   * is always synchronized on the IndexWriter instance so
   * only one thread at a time will call this method.
   *
   * @param segmentInfos the total set of segments in the index
   * @param writer IndexWriter instance
   */
  abstract MergeSpecification findMerges(SegmentInfos segmentInfos,
                                         IndexWriter writer)
    throws CorruptIndexException, IOException;

  /**
   * Determine what set of merge operations is necessary in
   * order to optimize the index.  The IndexWriter calls
   * this when its optimize() method is called.  This call
   * is always synchronized on the IndexWriter instance so
   * only one thread at a time will call this method.
   *
   * @param segmentInfos the total set of segments in the index
   * @param writer IndexWriter instance
   * @param maxSegmentCount requested maximum number of
   *   segments in the index (currently this is always 1)
   * @param segmentsToOptimize contains the specific
   *   SegmentInfo instances that must be merged away.  This
   *   may be a subset of all SegmentInfos.
   */
  abstract MergeSpecification findMergesForOptimize(SegmentInfos segmentInfos,
                                                    IndexWriter writer,
                                                    int maxSegmentCount,
                                                    Set segmentsToOptimize)
    throws CorruptIndexException, IOException;

  /**
   * Determine what set of merge operations is necessary in
   * order to expunge all deletes from the index.
   * @param segmentInfos the total set of segments in the index
   * @param writer IndexWriter instance
   */
  MergeSpecification findMergesToExpungeDeletes(SegmentInfos segmentInfos,
                                                 IndexWriter writer)
    throws CorruptIndexException, IOException
  {
    throw new RuntimeException("not implemented");
  }

  /**
   * Release all resources for the policy.
   */
  abstract void close();

  /**
   * Returns true if a newly flushed (not from merge)
   * segment should use the compound file format.
   */
  abstract boolean useCompoundFile(SegmentInfos segments, SegmentInfo newSegment);

  /**
   * Returns true if the doc store files should use the
   * compound file format.
   */
  abstract boolean useCompoundDocStore(SegmentInfos segments);
}
