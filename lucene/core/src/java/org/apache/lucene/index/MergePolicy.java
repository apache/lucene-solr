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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.apache.lucene.util.SetOnce;

/**
 * <p>Expert: a MergePolicy determines the sequence of
 * primitive merge operations.</p>
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
 * necessary.  When IndexWriter.forceMerge is called, it calls
 * {@link #findForcedMerges(SegmentInfos,int,Map)} and the MergePolicy should
 * then return the necessary merges.</p>
 *
 * <p>Note that the policy can return more than one merge at
 * a time.  In this case, if the writer is using {@link
 * SerialMergeScheduler}, the merges will be run
 * sequentially but if it is using {@link
 * ConcurrentMergeScheduler} they will be run concurrently.</p>
 * 
 * <p>The default MergePolicy is {@link
 * TieredMergePolicy}.</p>
 *
 * @lucene.experimental
 */

public abstract class MergePolicy implements java.io.Closeable, Cloneable {

  /** A map of doc IDs. */
  public static abstract class DocMap {
    /** Sole constructor, typically invoked from sub-classes constructors. */
    protected DocMap() {}

    /** Return the new doc ID according to its old value. */
    public abstract int map(int old);

    /** Useful from an assert. */
    boolean isConsistent(int maxDoc) {
      final FixedBitSet targets = new FixedBitSet(maxDoc);
      for (int i = 0; i < maxDoc; ++i) {
        final int target = map(i);
        if (target < 0 || target >= maxDoc) {
          assert false : "out of range: " + target + " not in [0-" + maxDoc + "[";
          return false;
        } else if (targets.get(target)) {
          assert false : target + " is already taken (" + i + ")";
          return false;
        }
      }
      return true;
    }
  }

  /** OneMerge provides the information necessary to perform
   *  an individual primitive merge operation, resulting in
   *  a single new segment.  The merge spec includes the
   *  subset of segments to be merged as well as whether the
   *  new segment should use the compound file format. */

  public static class OneMerge {

    SegmentInfoPerCommit info;      // used by IndexWriter
    boolean registerDone;           // used by IndexWriter
    long mergeGen;                  // used by IndexWriter
    boolean isExternal;             // used by IndexWriter
    int maxNumSegments = -1;        // used by IndexWriter

    /** Estimated size in bytes of the merged segment. */
    public volatile long estimatedMergeBytes;       // used by IndexWriter

    // Sum of sizeInBytes of all SegmentInfos; set by IW.mergeInit
    volatile long totalMergeBytes;

    List<SegmentReader> readers;        // used by IndexWriter

    /** Segments to be merged. */
    public final List<SegmentInfoPerCommit> segments;

    /** Number of documents in the merged segment. */
    public final int totalDocCount;
    boolean aborted;
    Throwable error;
    boolean paused;

    /** Sole constructor.
     * @param segments List of {@link SegmentInfoPerCommit}s
     *        to be merged. */
    public OneMerge(List<SegmentInfoPerCommit> segments) {
      if (0 == segments.size())
        throw new RuntimeException("segments must include at least one segment");
      // clone the list, as the in list may be based off original SegmentInfos and may be modified
      this.segments = new ArrayList<SegmentInfoPerCommit>(segments);
      int count = 0;
      for(SegmentInfoPerCommit info : segments) {
        count += info.info.getDocCount();
      }
      totalDocCount = count;
    }

    /** Expert: Get the list of readers to merge. Note that this list does not
     *  necessarily match the list of segments to merge and should only be used
     *  to feed SegmentMerger to initialize a merge. When a {@link OneMerge}
     *  reorders doc IDs, it must override {@link #getDocMap} too so that
     *  deletes that happened during the merge can be applied to the newly
     *  merged segment. */
    public List<AtomicReader> getMergeReaders() throws IOException {
      if (readers == null) {
        throw new IllegalStateException("IndexWriter has not initialized readers from the segment infos yet");
      }
      final List<AtomicReader> readers = new ArrayList<AtomicReader>(this.readers.size());
      for (AtomicReader reader : this.readers) {
        if (reader.numDocs() > 0) {
          readers.add(reader);
        }
      }
      return Collections.unmodifiableList(readers);
    }
    
    /**
     * Expert: Sets the {@link SegmentInfoPerCommit} of this {@link OneMerge}.
     * Allows sub-classes to e.g. set diagnostics properties.
     */
    public void setInfo(SegmentInfoPerCommit info) {
      this.info = info;
    }

    /** Expert: If {@link #getMergeReaders()} reorders document IDs, this method
     *  must be overridden to return a mapping from the <i>natural</i> doc ID
     *  (the doc ID that would result from a natural merge) to the actual doc
     *  ID. This mapping is used to apply deletions that happened during the
     *  merge to the new segment. */
    public DocMap getDocMap(MergeState mergeState) {
      return new DocMap() {
        @Override
        public int map(int docID) {
          return docID;
        }
      };
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

    /** Called periodically by {@link IndexWriter} while
     *  merging to see if the merge is aborted. */
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

    /** Set or clear whether this merge is paused paused (for example
     *  {@link ConcurrentMergeScheduler} will pause merges
     *  if too many are running). */
    synchronized public void setPause(boolean paused) {
      this.paused = paused;
      if (!paused) {
        // Wakeup merge thread, if it's waiting
        notifyAll();
      }
    }

    /** Returns true if this merge is paused.
     *
     *  @see #setPause(boolean) */
    synchronized public boolean getPause() {
      return paused;
    }

    /** Returns a readable description of the current merge
     *  state. */
    public String segString(Directory dir) {
      StringBuilder b = new StringBuilder();
      final int numSegments = segments.size();
      for(int i=0;i<numSegments;i++) {
        if (i > 0) b.append(' ');
        b.append(segments.get(i).toString(dir, 0));
      }
      if (info != null) {
        b.append(" into ").append(info.info.name);
      }
      if (maxNumSegments != -1)
        b.append(" [maxNumSegments=" + maxNumSegments + "]");
      if (aborted) {
        b.append(" [ABORTED]");
      }
      return b.toString();
    }
    
    /**
     * Returns the total size in bytes of this merge. Note that this does not
     * indicate the size of the merged segment, but the
     * input total size. This is only set once the merge is
     * initialized by IndexWriter.
     */
    public long totalBytesSize() throws IOException {
      return totalMergeBytes;
    }

    /**
     * Returns the total number of documents that are included with this merge.
     * Note that this does not indicate the number of documents after the merge.
     * */
    public int totalNumDocs() throws IOException {
      int total = 0;
      for (SegmentInfoPerCommit info : segments) {
        total += info.info.getDocCount();
      }
      return total;
    }

    /** Return {@link MergeInfo} describing this merge. */
    public MergeInfo getMergeInfo() {
      return new MergeInfo(totalDocCount, estimatedMergeBytes, isExternal, maxNumSegments);
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

    /** Sole constructor.  Use {@link
     *  #add(MergePolicy.OneMerge)} to add merges. */
    public MergeSpecification() {
    }

    /** Adds the provided {@link OneMerge} to this
     *  specification. */
    public void add(OneMerge merge) {
      merges.add(merge);
    }

    /** Returns a description of the merges in this
    *  specification. */
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

    /** Create a {@code MergeException}. */
    public MergeException(String message, Directory dir) {
      super(message);
      this.dir = dir;
    }

    /** Create a {@code MergeException}. */
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

  /** Thrown when a merge was explicity aborted because
   *  {@link IndexWriter#close(boolean)} was called with
   *  <code>false</code>.  Normally this exception is
   *  privately caught and suppresed by {@link IndexWriter}.  */
  public static class MergeAbortedException extends IOException {
    /** Create a {@link MergeAbortedException}. */
    public MergeAbortedException() {
      super("merge is aborted");
    }

    /** Create a {@link MergeAbortedException} with a
     *  specified message. */
    public MergeAbortedException(String message) {
      super(message);
    }
  }

  /** {@link IndexWriter} that contains this instance. */
  protected SetOnce<IndexWriter> writer;

  @Override
  public MergePolicy clone() {
    MergePolicy clone;
    try {
      clone = (MergePolicy) super.clone();
    } catch (CloneNotSupportedException e) {
      // should not happen
      throw new RuntimeException(e);
    }
    clone.writer = new SetOnce<IndexWriter>();
    return clone;
  }

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
   * @param mergeTrigger the event that triggered the merge
   * @param segmentInfos
   *          the total set of segments in the index
   */
  public abstract MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos)
      throws IOException;

  /**
   * Determine what set of merge operations is necessary in
   * order to merge to <= the specified segment count. {@link IndexWriter} calls this when its
   * {@link IndexWriter#forceMerge} method is called. This call is always
   * synchronized on the {@link IndexWriter} instance so only one thread at a
   * time will call this method.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   * @param maxSegmentCount
   *          requested maximum number of segments in the index (currently this
   *          is always 1)
   * @param segmentsToMerge
   *          contains the specific SegmentInfo instances that must be merged
   *          away. This may be a subset of all
   *          SegmentInfos.  If the value is True for a
   *          given SegmentInfo, that means this segment was
   *          an original segment present in the
   *          to-be-merged index; else, it was a segment
   *          produced by a cascaded merge.
   */
  public abstract MergeSpecification findForcedMerges(
          SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentInfoPerCommit,Boolean> segmentsToMerge)
      throws IOException;

  /**
   * Determine what set of merge operations is necessary in order to expunge all
   * deletes from the index.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   */
  public abstract MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos) throws IOException;

  /**
   * Release all resources for the policy.
   */
  @Override
  public abstract void close();
  
  
  /**
   * Returns true if a new segment (regardless of its origin) should use the compound file format.
   */
  public abstract boolean useCompoundFile(SegmentInfos segments, SegmentInfoPerCommit newSegment) throws IOException;
  
  /**
   * MergeTrigger is passed to
   * {@link MergePolicy#findMerges(MergeTrigger, SegmentInfos)} to indicate the
   * event that triggered the merge.
   */
  public static enum MergeTrigger {
    /**
     * Merge was triggered by a segment flush.
     */
    SEGMENT_FLUSH, 
    /**
     * Merge was triggered by a full flush. Full flushes
     * can be caused by a commit, NRT reader reopen or a close call on the index writer.
     */
    FULL_FLUSH,
    /**
     * Merge has been triggered explicitly by the user.
     */
    EXPLICIT,
    
    /**
     * Merge was triggered by a successfully finished merge.
     */
    MERGE_FINISHED,
  }
}
