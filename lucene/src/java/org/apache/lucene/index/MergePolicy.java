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
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.SetOnce.AlreadySetException;

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
 *
 * @lucene.experimental
 */

public abstract class MergePolicy implements java.io.Closeable {

  /** OneMerge provides the information necessary to perform
   *  an individual primitive merge operation, resulting in
   *  a single new segment.  The merge spec includes the
   *  subset of segments to be merged as well as whether the
   *  new segment should use the compound file format. */

  public static class OneMerge {

    SegmentInfo info;               // used by IndexWriter
    boolean optimize;               // used by IndexWriter
    boolean registerDone;           // used by IndexWriter
    long mergeGen;                  // used by IndexWriter
    boolean isExternal;             // used by IndexWriter
    int maxNumSegmentsOptimize;     // used by IndexWriter
    List<SegmentReader> readers;        // used by IndexWriter
    List<SegmentReader> readerClones;   // used by IndexWriter
    public final SegmentInfos segments;
    boolean aborted;
    Throwable error;
    boolean paused;

    public OneMerge(SegmentInfos segments) {
      if (0 == segments.size())
        throw new RuntimeException("segments must include at least one segment");
      this.segments = segments;
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
      notifyAll();
    }

    /** Returns true if this merge was aborted. */
    synchronized boolean isAborted() {
      return aborted;
    }

    public synchronized void checkAborted(Directory dir) throws MergeAbortedException {
      if (aborted) {
        throw new MergeAbortedException("merge is aborted: " + segString(dir));
      }

      while (paused) {
        try {
          // In theory we could wait() indefinitely, but we
          // do 1000 msec, defensively
          wait(1000);
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
        if (aborted) {
          throw new MergeAbortedException("merge is aborted: " + segString(dir));
        }
      }
    }

    synchronized public void setPause(boolean paused) {
      this.paused = paused;
      if (!paused) {
        // Wakeup merge thread, if it's waiting
        notifyAll();
      }
    }

    synchronized public boolean getPause() {
      return paused;
    }

    public String segString(Directory dir) {
      StringBuilder b = new StringBuilder();
      final int numSegments = segments.size();
      for(int i=0;i<numSegments;i++) {
        if (i > 0) b.append(' ');
        b.append(segments.info(i).toString(dir, 0));
      }
      if (info != null)
        b.append(" into ").append(info.name);
      if (optimize)
        b.append(" [optimize]");
      if (aborted) {
        b.append(" [ABORTED]");
      }
      return b.toString();
    }
    
    /**
     * Returns the total size in bytes of this merge. Note that this does not
     * indicate the size of the merged segment, but the input total size.
     * */
    public long totalBytesSize() throws IOException {
      long total = 0;
      for (SegmentInfo info : segments) {
        total += info.sizeInBytes(true);
      }
      return total;
    }

    /**
     * Returns the total number of documents that are included with this merge.
     * Note that this does not indicate the number of documents after the merge.
     * */
    public int totalNumDocs() throws IOException {
      int total = 0;
      for (SegmentInfo info : segments) {
        total += info.docCount;
      }
      return total;
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

    public final List<OneMerge> merges = new ArrayList<OneMerge>();

    public void add(OneMerge merge) {
      merges.add(merge);
    }

    public String segString(Directory dir) {
      StringBuilder b = new StringBuilder();
      b.append("MergeSpec:\n");
      final int count = merges.size();
      for(int i=0;i<count;i++)
        b.append("  ").append(1 + i).append(": ").append(merges.get(i).segString(dir));
      return b.toString();
    }
  }

  /** Exception thrown if there are any problems while
   *  executing a merge. */
  public static class MergeException extends RuntimeException {
    private Directory dir;

    public MergeException(String message, Directory dir) {
      super(message);
      this.dir = dir;
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

  protected final SetOnce<IndexWriter> writer;

  /**
   * Creates a new merge policy instance. Note that if you intend to use it
   * without passing it to {@link IndexWriter}, you should call
   * {@link #setIndexWriter(IndexWriter)}.
   */
  public MergePolicy() {
    writer = new SetOnce<IndexWriter>();
  }

  /**
   * Sets the {@link IndexWriter} to use by this merge policy. This method is
   * allowed to be called only once, and is usually set by IndexWriter. If it is
   * called more than once, {@link AlreadySetException} is thrown.
   * 
   * @see SetOnce
   */
  public void setIndexWriter(IndexWriter writer) {
    this.writer.set(writer);
  }
  
  /**
   * Determine what set of merge operations are now necessary on the index.
   * {@link IndexWriter} calls this whenever there is a change to the segments.
   * This call is always synchronized on the {@link IndexWriter} instance so
   * only one thread at a time will call this method.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   */
  public abstract MergeSpecification findMerges(SegmentInfos segmentInfos)
      throws CorruptIndexException, IOException;

  /**
   * Determine what set of merge operations is necessary in order to optimize
   * the index. {@link IndexWriter} calls this when its
   * {@link IndexWriter#optimize()} method is called. This call is always
   * synchronized on the {@link IndexWriter} instance so only one thread at a
   * time will call this method.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   * @param maxSegmentCount
   *          requested maximum number of segments in the index (currently this
   *          is always 1)
   * @param segmentsToOptimize
   *          contains the specific SegmentInfo instances that must be merged
   *          away. This may be a subset of all SegmentInfos.
   */
  public abstract MergeSpecification findMergesForOptimize(
      SegmentInfos segmentInfos, int maxSegmentCount, Set<SegmentInfo> segmentsToOptimize)
      throws CorruptIndexException, IOException;

  /**
   * Determine what set of merge operations is necessary in order to expunge all
   * deletes from the index.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   */
  public abstract MergeSpecification findMergesToExpungeDeletes(
      SegmentInfos segmentInfos) throws CorruptIndexException, IOException;

  /**
   * Release all resources for the policy.
   */
  public abstract void close();

  /**
   * Returns true if a new segment (regardless of its origin) should use the compound file format.
   */
  public abstract boolean useCompoundFile(SegmentInfos segments, SegmentInfo newSegment) throws IOException;
}
