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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.index.FieldInfos.FieldNumberBiMap;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.PayloadProcessorProvider.DirPayloadProcessor;
import org.apache.lucene.index.SegmentCodecs.SegmentCodecsBuilder;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.MapBackedSet;
import org.apache.lucene.util.TwoPhaseCommit;

/**
  An <code>IndexWriter</code> creates and maintains an index.

  <p>The {@link OpenMode} option on 
  {@link IndexWriterConfig#setOpenMode(OpenMode)} determines 
  whether a new index is created, or whether an existing index is
  opened. Note that you can open an index with {@link OpenMode#CREATE}
  even while readers are using the index. The old readers will 
  continue to search the "point in time" snapshot they had opened, 
  and won't see the newly created index until they re-open. If 
  {@link OpenMode#CREATE_OR_APPEND} is used IndexWriter will create a 
  new index if there is not already an index at the provided path
  and otherwise open the existing index.</p>

  <p>In either case, documents are added with {@link #addDocument(Iterable)
  addDocument} and removed with {@link #deleteDocuments(Term)} or {@link
  #deleteDocuments(Query)}. A document can be updated with {@link
  #updateDocument(Term, Iterable) updateDocument} (which just deletes
  and then adds the entire document). When finished adding, deleting 
  and updating documents, {@link #close() close} should be called.</p>

  <a name="flush"></a>
  <p>These changes are buffered in memory and periodically
  flushed to the {@link Directory} (during the above method
  calls). A flush is triggered when there are enough added documents
  since the last flush. Flushing is triggered either by RAM usage of the
  documents (see {@link IndexWriterConfig#setRAMBufferSizeMB}) or the
  number of added documents (see {@link IndexWriterConfig#setMaxBufferedDocs(int)}).
  The default is to flush when RAM usage hits
  {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB} MB. For
  best indexing speed you should flush by RAM usage with a
  large RAM buffer. Additionally, if IndexWriter reaches the configured number of
  buffered deletes (see {@link IndexWriterConfig#setMaxBufferedDeleteTerms})
  the deleted terms and queries are flushed and applied to existing segments.
  In contrast to the other flush options {@link IndexWriterConfig#setRAMBufferSizeMB} and 
  {@link IndexWriterConfig#setMaxBufferedDocs(int)}, deleted terms
  won't trigger a segment flush. Note that flushing just moves the
  internal buffered state in IndexWriter into the index, but
  these changes are not visible to IndexReader until either
  {@link #commit()} or {@link #close} is called.  A flush may
  also trigger one or more segment merges which by default
  run with a background thread so as not to block the
  addDocument calls (see <a href="#mergePolicy">below</a>
  for changing the {@link MergeScheduler}).</p>

  <p>If an index will not have more documents added for a while and optimal search
  performance is desired, then either the full {@link #optimize() optimize}
  method or partial {@link #optimize(int)} method should be
  called before the index is closed.</p>

  <p>Opening an <code>IndexWriter</code> creates a lock file for the directory in use. Trying to open
  another <code>IndexWriter</code> on the same directory will lead to a
  {@link LockObtainFailedException}. The {@link LockObtainFailedException}
  is also thrown if an IndexReader on the same directory is used to delete documents
  from the index.</p>
  
  <a name="deletionPolicy"></a>
  <p>Expert: <code>IndexWriter</code> allows an optional
  {@link IndexDeletionPolicy} implementation to be
  specified.  You can use this to control when prior commits
  are deleted from the index.  The default policy is {@link
  KeepOnlyLastCommitDeletionPolicy} which removes all prior
  commits as soon as a new commit is done (this matches
  behavior before 2.2).  Creating your own policy can allow
  you to explicitly keep previous "point in time" commits
  alive in the index for some time, to allow readers to
  refresh to the new commit without having the old commit
  deleted out from under them.  This is necessary on
  filesystems like NFS that do not support "delete on last
  close" semantics, which Lucene's "point in time" search
  normally relies on. </p>

  <a name="mergePolicy"></a> <p>Expert:
  <code>IndexWriter</code> allows you to separately change
  the {@link MergePolicy} and the {@link MergeScheduler}.
  The {@link MergePolicy} is invoked whenever there are
  changes to the segments in the index.  Its role is to
  select which merges to do, if any, and return a {@link
  MergePolicy.MergeSpecification} describing the merges.  It
  also selects merges to do for optimize().  (The default is
  {@link LogByteSizeMergePolicy}.  Then, the {@link
  MergeScheduler} is invoked with the requested merges and
  it decides when and how to run the merges.  The default is
  {@link ConcurrentMergeScheduler}. </p>

  <a name="OOME"></a><p><b>NOTE</b>: if you hit an
  OutOfMemoryError then IndexWriter will quietly record this
  fact and block all future segment commits.  This is a
  defensive measure in case any internal state (buffered
  documents and deletions) were corrupted.  Any subsequent
  calls to {@link #commit()} will throw an
  IllegalStateException.  The only course of action is to
  call {@link #close()}, which internally will call {@link
  #rollback()}, to undo any changes to the index since the
  last commit.  You can also just call {@link #rollback()}
  directly.</p>

  <a name="thread-safety"></a><p><b>NOTE</b>: {@link
  IndexWriter} instances are completely thread
  safe, meaning multiple threads can call any of its
  methods, concurrently.  If your application requires
  external synchronization, you should <b>not</b>
  synchronize on the <code>IndexWriter</code> instance as
  this may cause deadlock; use your own (non-Lucene) objects
  instead. </p>
  
  <p><b>NOTE</b>: If you call
  <code>Thread.interrupt()</code> on a thread that's within
  IndexWriter, IndexWriter will try to catch this (eg, if
  it's in a wait() or Thread.sleep()), and will then throw
  the unchecked exception {@link ThreadInterruptedException}
  and <b>clear</b> the interrupt status on the thread.</p>
*/

/*
 * Clarification: Check Points (and commits)
 * IndexWriter writes new index files to the directory without writing a new segments_N
 * file which references these new files. It also means that the state of
 * the in memory SegmentInfos object is different than the most recent
 * segments_N file written to the directory.
 *
 * Each time the SegmentInfos is changed, and matches the (possibly
 * modified) directory files, we have a new "check point".
 * If the modified/new SegmentInfos is written to disk - as a new
 * (generation of) segments_N file - this check point is also an
 * IndexCommit.
 *
 * A new checkpoint always replaces the previous checkpoint and
 * becomes the new "front" of the index. This allows the IndexFileDeleter
 * to delete files that are referenced only by stale checkpoints.
 * (files that were created since the last commit, but are no longer
 * referenced by the "front" of the index). For this, IndexFileDeleter
 * keeps track of the last non commit checkpoint.
 */
public class IndexWriter implements Closeable, TwoPhaseCommit {
  /**
   * Name of the write lock in the index.
   */
  public static final String WRITE_LOCK_NAME = "write.lock";

  /**
   * Absolute hard maximum length for a term, in bytes once
   * encoded as UTF8.  If a term arrives from the analyzer
   * longer than this length, it is skipped and a message is
   * printed to infoStream, if set (see {@link
   * #setInfoStream}).
   */
  public final static int MAX_TERM_LENGTH = DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8;
  // Used for printing messages
  private static final AtomicInteger MESSAGE_ID = new AtomicInteger();
  private int messageID = MESSAGE_ID.getAndIncrement();
  volatile private boolean hitOOM;

  private final Directory directory;  // where this index resides
  private final Analyzer analyzer;    // how to analyze text

  private volatile long changeCount; // increments every time a change is completed
  private long lastCommitChangeCount; // last changeCount that was committed

  private List<SegmentInfo> rollbackSegments;      // list of segmentInfo we will fallback to if the commit fails

  volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
  volatile long pendingCommitChangeCount;

  final SegmentInfos segmentInfos;       // the segments
  final FieldNumberBiMap globalFieldNumberMap;

  private DocumentsWriter docWriter;
  final IndexFileDeleter deleter;

  private Map<SegmentInfo,Boolean> segmentsToOptimize = new HashMap<SegmentInfo,Boolean>();           // used by optimize to note those needing optimization
  private int optimizeMaxNumSegments;

  private Lock writeLock;

  private boolean closed;
  private boolean closing;

  // Holds all SegmentInfo instances currently involved in
  // merges
  private HashSet<SegmentInfo> mergingSegments = new HashSet<SegmentInfo>();

  private MergePolicy mergePolicy;
  private final MergeScheduler mergeScheduler;
  private LinkedList<MergePolicy.OneMerge> pendingMerges = new LinkedList<MergePolicy.OneMerge>();
  private Set<MergePolicy.OneMerge> runningMerges = new HashSet<MergePolicy.OneMerge>();
  private List<MergePolicy.OneMerge> mergeExceptions = new ArrayList<MergePolicy.OneMerge>();
  private long mergeGen;
  private boolean stopMerges;

  final AtomicInteger flushCount = new AtomicInteger();
  final AtomicInteger flushDeletesCount = new AtomicInteger();

  final ReaderPool readerPool = new ReaderPool();
  final BufferedDeletesStream bufferedDeletesStream;

  // This is a "write once" variable (like the organic dye
  // on a DVD-R that may or may not be heated by a laser and
  // then cooled to permanently record the event): it's
  // false, until getReader() is called for the first time,
  // at which point it's switched to true and never changes
  // back to false.  Once this is true, we hold open and
  // reuse SegmentReader instances internally for applying
  // deletes, doing merges, and reopening near real-time
  // readers.
  private volatile boolean poolReaders;

  // The instance that was passed to the constructor. It is saved only in order
  // to allow users to query an IndexWriter settings.
  private final IndexWriterConfig config;

  // The PayloadProcessorProvider to use when segments are merged
  private PayloadProcessorProvider payloadProcessorProvider;

  // for testing
  boolean anyNonBulkMerges;

  IndexReader getReader() throws IOException {
    return getReader(true);
  }

  /**
   * Expert: returns a readonly reader, covering all
   * committed as well as un-committed changes to the index.
   * This provides "near real-time" searching, in that
   * changes made during an IndexWriter session can be
   * quickly made available for searching without closing
   * the writer nor calling {@link #commit}.
   *
   * <p>Note that this is functionally equivalent to calling
   * {#flush} and then using {@link IndexReader#open} to
   * open a new reader.  But the turnaround time of this
   * method should be faster since it avoids the potentially
   * costly {@link #commit}.</p>
   *
   * <p>You must close the {@link IndexReader} returned by
   * this method once you are done using it.</p>
   *
   * <p>It's <i>near</i> real-time because there is no hard
   * guarantee on how quickly you can get a new reader after
   * making changes with IndexWriter.  You'll have to
   * experiment in your situation to determine if it's
   * fast enough.  As this is a new and experimental
   * feature, please report back on your findings so we can
   * learn, improve and iterate.</p>
   *
   * <p>The resulting reader supports {@link
   * IndexReader#reopen}, but that call will simply forward
   * back to this method (though this may change in the
   * future).</p>
   *
   * <p>The very first time this method is called, this
   * writer instance will make every effort to pool the
   * readers that it opens for doing merges, applying
   * deletes, etc.  This means additional resources (RAM,
   * file descriptors, CPU time) will be consumed.</p>
   *
   * <p>For lower latency on reopening a reader, you should
   * call {@link #setMergedSegmentWarmer} to
   * pre-warm a newly merged segment before it's committed
   * to the index.  This is important for minimizing
   * index-to-search delay after a large merge.  </p>
   *
   * <p>If an addIndexes* call is running in another thread,
   * then this reader will only search those segments from
   * the foreign index that have been successfully copied
   * over, so far</p>.
   *
   * <p><b>NOTE</b>: Once the writer is closed, any
   * outstanding readers may continue to be used.  However,
   * if you attempt to reopen any of those readers, you'll
   * hit an {@link AlreadyClosedException}.</p>
   *
   * @lucene.experimental
   *
   * @return IndexReader that covers entire index plus all
   * changes made so far by this IndexWriter instance
   *
   * @throws IOException
   */
  IndexReader getReader(boolean applyAllDeletes) throws IOException {
    ensureOpen();

    final long tStart = System.currentTimeMillis();

    if (infoStream != null) {
      message("flush at getReader");
    }
    // Do this up front before flushing so that the readers
    // obtained during this flush are pooled, the first time
    // this method is called:
    poolReaders = true;
    final IndexReader r;
    doBeforeFlush();
    boolean anySegmentFlushed = false;
    /*
     * for releasing a NRT reader we must ensure that 
     * DW doesn't add any segments or deletes until we are
     * done with creating the NRT DirectoryReader. 
     * We release the two stage full flush after we are done opening the
     * directory reader!
     */
    synchronized (fullFlushLock) {
      boolean success = false;
      try {
        anySegmentFlushed = docWriter.flushAllThreads();
        if (!anySegmentFlushed) {
          // prevent double increment since docWriter#doFlush increments the flushcount
          // if we flushed anything.
          flushCount.incrementAndGet();
        }
        success = true;
        // Prevent segmentInfos from changing while opening the
        // reader; in theory we could do similar retry logic,
        // just like we do when loading segments_N
        synchronized(this) {
          maybeApplyDeletes(applyAllDeletes);
          r = new DirectoryReader(this, segmentInfos, codecs, applyAllDeletes);
          if (infoStream != null) {
            message("return reader version=" + r.getVersion() + " reader=" + r);
          }
        }
      } catch (OutOfMemoryError oom) {
        handleOOM(oom, "getReader");
        // never reached but javac disagrees:
        return null;
      } finally {
        if (!success && infoStream != null) {
          message("hit exception during NRT reader");
        }
        // Done: finish the full flush!
        docWriter.finishFullFlush(success);
        doAfterFlush();
      }
    }
    if (anySegmentFlushed) {
      maybeMerge();
    }
    if (infoStream != null) {
      message("getReader took " + (System.currentTimeMillis() - tStart) + " msec");
    }
    return r;
  }

  // Used for all SegmentReaders we open
  private final Collection<IndexReader.ReaderFinishedListener> readerFinishedListeners = new MapBackedSet<IndexReader.ReaderFinishedListener>(new ConcurrentHashMap<IndexReader.ReaderFinishedListener,Boolean>());

  Collection<IndexReader.ReaderFinishedListener> getReaderFinishedListeners() throws IOException {
    return readerFinishedListeners;
  }

  /** Holds shared SegmentReader instances. IndexWriter uses
   *  SegmentReaders for 1) applying deletes, 2) doing
   *  merges, 3) handing out a real-time reader.  This pool
   *  reuses instances of the SegmentReaders in all these
   *  places if it is in "near real-time mode" (getReader()
   *  has been called on this instance). */

  class ReaderPool {
    
    final class SegmentCacheKey {
      public final SegmentInfo si;
      public final IOContext.Context context;
      
      public SegmentCacheKey(SegmentInfo segInfo, IOContext.Context context) {
        assert context == IOContext.Context.MERGE || context == IOContext.Context.READ;
        this.si = segInfo;
        this.context = context;
      }
      
      @Override
      public int hashCode() {
        return si.hashCode() + context.hashCode();
      }

      @Override
      public String toString() {
        return "SegmentCacheKey(" + si + "," + context + ")";
      }

      @Override
      public boolean equals(Object _other) {
        if (!(_other instanceof SegmentCacheKey)) {
          return false;
        }
        final SegmentCacheKey other = (SegmentCacheKey) _other;
        return si.equals(other.si) && context == other.context;
      }
    }

    private final Map<SegmentCacheKey,SegmentReader> readerMap = new HashMap<SegmentCacheKey,SegmentReader>();

    /** Forcefully clear changes for the specified segments.  This is called on successful merge. */
    synchronized void clear(List<SegmentInfo> infos) throws IOException {
      if (infos == null) {
        for (Map.Entry<SegmentCacheKey,SegmentReader> ent: readerMap.entrySet()) {
          ent.getValue().hasChanges = false;
        }
      } else {
        for (final SegmentInfo info: infos) {
          final SegmentReader r = readerMap.get(new SegmentCacheKey(info, IOContext.Context.MERGE));
          if (r != null) {
            r.hasChanges = false;
          }
        }
      }
    }

    // used only by asserts
    public synchronized boolean infoIsLive(SegmentInfo info) {
      return infoIsLive(info, "");
    }

    public synchronized boolean infoIsLive(SegmentInfo info, String message) {
      int idx = segmentInfos.indexOf(info);
      assert idx != -1: "info=" + info + " isn't live: " + message;
      assert segmentInfos.info(idx) == info: "info=" + info + " doesn't match live info in segmentInfos: " + message;
      return true;
    }

    public synchronized SegmentInfo mapToLive(SegmentInfo info) {
      int idx = segmentInfos.indexOf(info);
      if (idx != -1) {
        info = segmentInfos.info(idx);
      }
      return info;
    }

    /**
     * Release the segment reader (i.e. decRef it and close if there
     * are no more references.
     * @return true if this release altered the index (eg
     * the SegmentReader had pending changes to del docs and
     * was closed).  Caller must call checkpoint() if so.
     * @param sr
     * @throws IOException
     */
    public synchronized boolean release(SegmentReader sr, IOContext.Context context) throws IOException {
      return release(sr, false, context);
    }

    /**
     * Release the segment reader (i.e. decRef it and close if there
     * are no more references.
     * @return true if this release altered the index (eg
     * the SegmentReader had pending changes to del docs and
     * was closed).  Caller must call checkpoint() if so.
     * @param sr
     * @throws IOException
     */
    public synchronized boolean release(SegmentReader sr, boolean drop) throws IOException {
      final SegmentCacheKey cacheKey = new SegmentCacheKey(sr.getSegmentInfo(), IOContext.Context.READ);
      final SegmentReader other = readerMap.get(cacheKey);
      if (sr == other) {
        return release(sr, drop, IOContext.Context.READ);
      } else {
        assert sr == readerMap.get(new SegmentCacheKey(sr.getSegmentInfo(), IOContext.Context.MERGE));
        return release(sr, drop, IOContext.Context.MERGE);
      }
    }

    /**
     * Release the segment reader (i.e. decRef it and close if there
     * are no more references.
     * @return true if this release altered the index (eg
     * the SegmentReader had pending changes to del docs and
     * was closed).  Caller must call checkpoint() if so.
     * @param sr
     * @throws IOException
     */
    public synchronized boolean release(SegmentReader sr, boolean drop, IOContext.Context context) throws IOException {

      SegmentCacheKey cacheKey = new SegmentCacheKey(sr.getSegmentInfo(), context);
      
      final boolean pooled = readerMap.containsKey(cacheKey);

      assert !pooled || readerMap.get(cacheKey) == sr;

      // Drop caller's ref; for an external reader (not
      // pooled), this decRef will close it
      sr.decRef();

      if (pooled && (drop || (!poolReaders && sr.getRefCount() == 1))) {

        // We invoke deleter.checkpoint below, so we must be
        // sync'd on IW if there are changes:
        assert !sr.hasChanges || Thread.holdsLock(IndexWriter.this);

        // Discard (don't save) changes when we are dropping
        // the reader; this is used only on the sub-readers
        // after a successful merge.
        final boolean hasChanges;
        if (drop) {
          hasChanges = sr.hasChanges = false;
        } else {
          hasChanges = sr.hasChanges;
        }

        // Drop our ref -- this will commit any pending
        // changes to the dir
        sr.close();

        // We are the last ref to this reader; since we're
        // not pooling readers, we release it:
        readerMap.remove(cacheKey);

        if (drop && context == IOContext.Context.MERGE) {
          // Also drop the READ reader if present: we don't
          // need its deletes since they've been carried
          // over to the merged segment
          cacheKey = new SegmentCacheKey(sr.getSegmentInfo(), IOContext.Context.READ);
          SegmentReader sr2 = readerMap.get(cacheKey);
          if (sr2 != null) {
            readerMap.remove(cacheKey);
            sr2.hasChanges = false;
            sr2.close();
          }
        }

        return hasChanges;
      }

      return false;
    }

    public synchronized void drop(List<SegmentInfo> infos) throws IOException {
      drop(infos, IOContext.Context.READ);
      drop(infos, IOContext.Context.MERGE);
    }

    public synchronized void drop(List<SegmentInfo> infos, IOContext.Context context) throws IOException {
      for(SegmentInfo info : infos) {
        drop(info, context);
      }
    }

    public synchronized void drop(SegmentInfo info) throws IOException {
      drop(info, IOContext.Context.READ);
      drop(info, IOContext.Context.MERGE);
    }

    public synchronized void drop(SegmentInfo info, IOContext.Context context) throws IOException {
      final SegmentReader sr;
      if ((sr = readerMap.remove(new SegmentCacheKey(info, context))) != null) {
        sr.hasChanges = false;
        readerMap.remove(new SegmentCacheKey(info, context));
        sr.close();
      }
    }

    /** Remove all our references to readers, and commits
     *  any pending changes. */
    synchronized void close() throws IOException {
      // We invoke deleter.checkpoint below, so we must be
      // sync'd on IW:
      assert Thread.holdsLock(IndexWriter.this);

      Iterator<Map.Entry<SegmentCacheKey,SegmentReader>> iter = readerMap.entrySet().iterator();
      while (iter.hasNext()) {

        Map.Entry<SegmentCacheKey,SegmentReader> ent = iter.next();

        SegmentReader sr = ent.getValue();
        if (sr.hasChanges) {
          assert infoIsLive(sr.getSegmentInfo(), "key=" + ent.getKey());
          sr.doCommit(null);

          // Must checkpoint w/ deleter, because this
          // segment reader will have created new _X_N.del
          // file.
          deleter.checkpoint(segmentInfos, false);
        }

        iter.remove();

        // NOTE: it is allowed that this decRef does not
        // actually close the SR; this can happen when a
        // near real-time reader is kept open after the
        // IndexWriter instance is closed
        sr.decRef();
      }
    }

    /**
     * Commit all segment reader in the pool.
     * @throws IOException
     */
    synchronized void commit(SegmentInfos infos) throws IOException {

      // We invoke deleter.checkpoint below, so we must be
      // sync'd on IW:
      assert Thread.holdsLock(IndexWriter.this);
      
      for (SegmentInfo info : infos) {
        final SegmentReader sr = readerMap.get(new SegmentCacheKey(info, IOContext.Context.READ));
        if (sr != null && sr.hasChanges) {
          assert infoIsLive(info);
          sr.doCommit(null);
          // Must checkpoint w/ deleter, because this
          // segment reader will have created new _X_N.del
          // file.
          deleter.checkpoint(segmentInfos, false);
        }
      }
    }

    public synchronized SegmentReader getReadOnlyClone(SegmentInfo info, IOContext context) throws IOException {
      return getReadOnlyClone(info, true, context);
    }

    /**
     * Returns a ref to a clone.  NOTE: this clone is not
     * enrolled in the pool, so you should simply close()
     * it when you're done (ie, do not call release()).
     */
    public synchronized SegmentReader getReadOnlyClone(SegmentInfo info, boolean doOpenStores, IOContext context) throws IOException {
      SegmentReader sr = get(info, doOpenStores, context);
      try {
        return (SegmentReader) sr.clone(true);
      } finally {
        sr.decRef();
      }
    }

    public synchronized SegmentReader get(SegmentInfo info, IOContext context) throws IOException {
      return get(info, true, context);
    }

    /**
     * Obtain a SegmentReader from the readerPool.  The reader
     * must be returned by calling {@link #release(SegmentReader)}
     * @see #release(SegmentReader)
     * @param info
     * @param doOpenStores
     * @throws IOException
     */
    public synchronized SegmentReader get(SegmentInfo info, boolean doOpenStores, IOContext context) throws IOException {

      SegmentCacheKey cacheKey = new SegmentCacheKey(info, context.context);
      SegmentReader sr = readerMap.get(cacheKey);
      if (sr == null) {
        // TODO: we may want to avoid doing this while
        // synchronized
        // Returns a ref, which we xfer to readerMap:
        sr = SegmentReader.get(false, info.dir, info, doOpenStores, context.context == IOContext.Context.MERGE ? -1 : config.getReaderTermsIndexDivisor(), context);
        sr.readerFinishedListeners = readerFinishedListeners;

        if (info.dir == directory) {
          // Only pool if reader is not external
          readerMap.put(cacheKey, sr);
        }
      } else {
        if (doOpenStores) {
          sr.openDocStores();
        }
      }

      // Return a ref to our caller
      if (info.dir == directory) {
        // Only incRef if we pooled (reader is not external)
        sr.incRef();
      }
      return sr;
    }

    // Returns a ref
    public synchronized SegmentReader getIfExists(SegmentInfo info) throws IOException {
      SegmentReader sr = getIfExists(info, IOContext.Context.READ);
      if (sr == null) {
        sr = getIfExists(info, IOContext.Context.MERGE);
      }
      return sr;
    }
    
    // Returns a ref
    public synchronized SegmentReader getIfExists(SegmentInfo info, IOContext.Context context) throws IOException {
      SegmentCacheKey cacheKey = new SegmentCacheKey(info, context);
      SegmentReader sr = readerMap.get(cacheKey);
      if (sr != null) {
        sr.incRef();
      }
      return sr;
    }
  }  

  /**
   * Obtain the number of deleted docs for a pooled reader.
   * If the reader isn't being pooled, the segmentInfo's 
   * delCount is returned.
   */
  public int numDeletedDocs(SegmentInfo info) throws IOException {
    SegmentReader reader = readerPool.getIfExists(info);
    try {
      if (reader != null) {
        // the pulled reader could be from an in-flight merge 
        // while the info we see has already new applied deletes after a commit
        // we max out the delets since deletes never shrink
        return Math.max(info.getDelCount(), reader.numDeletedDocs());
      } else {
        return info.getDelCount();
      }
    } finally {
      if (reader != null) {
        readerPool.release(reader, false);
      }
    }
  }

  /**
   * Used internally to throw an {@link
   * AlreadyClosedException} if this IndexWriter has been
   * closed.
   * @throws AlreadyClosedException if this IndexWriter is closed
   */
  protected final void ensureOpen(boolean includePendingClose) throws AlreadyClosedException {
    if (closed || (includePendingClose && closing)) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  protected final void ensureOpen() throws AlreadyClosedException {
    ensureOpen(true);
  }

  /**
   * Prints a message to the infoStream (if non-null),
   * prefixed with the identifying information for this
   * writer and the thread that's calling it.
   */
  public void message(String message) {
    if (infoStream != null)
      infoStream.println("IW " + messageID + " [" + new Date() + "; " + Thread.currentThread().getName() + "]: " + message);
  }

  CodecProvider codecs;

  /**
   * Constructs a new IndexWriter per the settings given in <code>conf</code>.
   * Note that the passed in {@link IndexWriterConfig} is
   * privately cloned; if you need to make subsequent "live"
   * changes to the configuration use {@link #getConfig}.
   * <p>
   * 
   * @param d
   *          the index directory. The index is either created or appended
   *          according <code>conf.getOpenMode()</code>.
   * @param conf
   *          the configuration settings according to which IndexWriter should
   *          be initialized.
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws LockObtainFailedException
   *           if another writer has this index open (<code>write.lock</code>
   *           could not be obtained)
   * @throws IOException
   *           if the directory cannot be read/written to, or if it does not
   *           exist and <code>conf.getOpenMode()</code> is
   *           <code>OpenMode.APPEND</code> or if there is any other low-level
   *           IO error
   */
  public IndexWriter(Directory d, IndexWriterConfig conf)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    config = (IndexWriterConfig) conf.clone();
    directory = d;
    analyzer = conf.getAnalyzer();
    infoStream = defaultInfoStream;
    mergePolicy = conf.getMergePolicy();
    mergePolicy.setIndexWriter(this);
    mergeScheduler = conf.getMergeScheduler();
    codecs = conf.getCodecProvider();

    bufferedDeletesStream = new BufferedDeletesStream(messageID);
    bufferedDeletesStream.setInfoStream(infoStream);
    poolReaders = conf.getReaderPooling();

    writeLock = directory.makeLock(WRITE_LOCK_NAME);

    if (!writeLock.obtain(conf.getWriteLockTimeout())) // obtain write lock
      throw new LockObtainFailedException("Index locked for write: " + writeLock);

    OpenMode mode = conf.getOpenMode();
    boolean create;
    if (mode == OpenMode.CREATE) {
      create = true;
    } else if (mode == OpenMode.APPEND) {
      create = false;
    } else {
      // CREATE_OR_APPEND - create only if an index does not exist
      create = !IndexReader.indexExists(directory);
    }
    boolean success = false;

    // If index is too old, reading the segments will throw
    // IndexFormatTooOldException.
    segmentInfos = new SegmentInfos(codecs);
    try {
      if (create) {
        // Try to read first.  This is to allow create
        // against an index that's currently open for
        // searching.  In this case we write the next
        // segments_N file with no segments:
        try {
          segmentInfos.read(directory, codecs);
          segmentInfos.clear();
        } catch (IOException e) {
          // Likely this means it's a fresh directory
        }

        // Record that we have a change (zero out all
        // segments) pending:
        changeCount++;
        segmentInfos.changed();
      } else {
        segmentInfos.read(directory, codecs);

        IndexCommit commit = conf.getIndexCommit();
        if (commit != null) {
          // Swap out all segments, but, keep metadata in
          // SegmentInfos, like version & generation, to
          // preserve write-once.  This is important if
          // readers are open against the future commit
          // points.
          if (commit.getDirectory() != directory)
            throw new IllegalArgumentException("IndexCommit's directory doesn't match my directory");
          SegmentInfos oldInfos = new SegmentInfos(codecs);
          oldInfos.read(directory, commit.getSegmentsFileName(), codecs);
          segmentInfos.replace(oldInfos);
          changeCount++;
          segmentInfos.changed();
          if (infoStream != null)
            message("init: loaded commit \"" + commit.getSegmentsFileName() + "\"");
        }
      }

      rollbackSegments = segmentInfos.createBackupSegmentInfos(true);

      // start with previous field numbers, but new FieldInfos
      globalFieldNumberMap = segmentInfos.getOrLoadGlobalFieldNumberMap(directory);
      docWriter = new DocumentsWriter(config, directory, this, globalFieldNumberMap, bufferedDeletesStream);
      docWriter.setInfoStream(infoStream);

      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:
      deleter = new IndexFileDeleter(directory,
                                     conf.getIndexDeletionPolicy(),
                                     segmentInfos, infoStream, codecs);

      if (deleter.startingCommitDeleted) {
        // Deletion policy deleted the "head" commit point.
        // We have to mark ourself as changed so that if we
        // are closed w/o any further changes we write a new
        // segments_N file.
        changeCount++;
        segmentInfos.changed();
      }

      if (infoStream != null) {
        message("init: create=" + create);
        messageState();
      }

      success = true;

    } finally {
      if (!success) {
        if (infoStream != null) {
          message("init: hit exception on init; releasing write lock");
        }
        try {
          writeLock.release();
        } catch (Throwable t) {
          // don't mask the original exception
        }
        writeLock = null;
      }
    }
  }
  
  /**
   * Returns the private {@link IndexWriterConfig}, cloned
   * from the {@link IndexWriterConfig} passed to
   * {@link #IndexWriter(Directory, IndexWriterConfig)}.
   * <p>
   * <b>NOTE:</b> some settings may be changed on the
   * returned {@link IndexWriterConfig}, and will take
   * effect in the current IndexWriter instance.  See the
   * javadocs for the specific setters in {@link
   * IndexWriterConfig} for details.
   */
  public IndexWriterConfig getConfig() {
    return config;
  }

  /** If non-null, this will be the default infoStream used
   * by a newly instantiated IndexWriter.
   * @see #setInfoStream
   */
  public static void setDefaultInfoStream(PrintStream infoStream) {
    IndexWriter.defaultInfoStream = infoStream;
  }

  /**
   * Returns the current default infoStream for newly
   * instantiated IndexWriters.
   * @see #setDefaultInfoStream
   */
  public static PrintStream getDefaultInfoStream() {
    return IndexWriter.defaultInfoStream;
  }

  /** If non-null, information about merges, deletes and a
   * message when maxFieldLength is reached will be printed
   * to this.
   */
  public void setInfoStream(PrintStream infoStream) throws IOException {
    ensureOpen();
    this.infoStream = infoStream;
    docWriter.setInfoStream(infoStream);
    deleter.setInfoStream(infoStream);
    bufferedDeletesStream.setInfoStream(infoStream);
    if (infoStream != null)
      messageState();
  }

  private void messageState() throws IOException {
    message("\ndir=" + directory + "\n" +
            "index=" + segString() + "\n" +
            "version=" + Constants.LUCENE_VERSION + "\n" +
            config.toString());
  }

  /**
   * Returns the current infoStream in use by this writer.
   * @see #setInfoStream
   */
  public PrintStream getInfoStream() {
    ensureOpen();
    return infoStream;
  }

  /** Returns true if verbosing is enabled (i.e., infoStream != null). */
  public boolean verbose() {
    return infoStream != null;
  }

  /**
   * Commits all changes to an index and closes all
   * associated files.  Note that this may be a costly
   * operation, so, try to re-use a single writer instead of
   * closing and opening a new one.  See {@link #commit()} for
   * caveats about write caching done by some IO devices.
   *
   * <p> If an Exception is hit during close, eg due to disk
   * full or some other reason, then both the on-disk index
   * and the internal state of the IndexWriter instance will
   * be consistent.  However, the close will not be complete
   * even though part of it (flushing buffered documents)
   * may have succeeded, so the write lock will still be
   * held.</p>
   *
   * <p> If you can correct the underlying cause (eg free up
   * some disk space) then you can call close() again.
   * Failing that, if you want to force the write lock to be
   * released (dangerous, because you may then lose buffered
   * docs in the IndexWriter instance) then you can do
   * something like this:</p>
   *
   * <pre>
   * try {
   *   writer.close();
   * } finally {
   *   if (IndexWriter.isLocked(directory)) {
   *     IndexWriter.unlock(directory);
   *   }
   * }
   * </pre>
   *
   * after which, you must be certain not to use the writer
   * instance anymore.</p>
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer, again.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void close() throws CorruptIndexException, IOException {
    close(true);
  }

  /**
   * Closes the index with or without waiting for currently
   * running merges to finish.  This is only meaningful when
   * using a MergeScheduler that runs merges in background
   * threads.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer, again.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * <p><b>NOTE</b>: it is dangerous to always call
   * close(false), especially when IndexWriter is not open
   * for very long, because this can result in "merge
   * starvation" whereby long merges will never have a
   * chance to finish.  This will cause too many segments in
   * your index over time.</p>
   *
   * @param waitForMerges if true, this call will block
   * until all merges complete; else, it will ask all
   * running merges to abort, wait until those merges have
   * finished (which should be at most a few seconds), and
   * then return.
   */
  public void close(boolean waitForMerges) throws CorruptIndexException, IOException {

    // Ensure that only one thread actually gets to do the closing:
    if (shouldClose()) {
      // If any methods have hit OutOfMemoryError, then abort
      // on close, in case the internal state of IndexWriter
      // or DocumentsWriter is corrupt
      if (hitOOM)
        rollbackInternal();
      else
        closeInternal(waitForMerges);
    }
  }

  // Returns true if this thread should attempt to close, or
  // false if IndexWriter is now closed; else, waits until
  // another thread finishes closing
  synchronized private boolean shouldClose() {
    while(true) {
      if (!closed) {
        if (!closing) {
          closing = true;
          return true;
        } else {
          // Another thread is presently trying to close;
          // wait until it finishes one way (closes
          // successfully) or another (fails to close)
          doWait();
        }
      } else
        return false;
    }
  }

  private void closeInternal(boolean waitForMerges) throws CorruptIndexException, IOException {

    try {
      if (infoStream != null) {
        message("now flush at close waitForMerges=" + waitForMerges);
      }

      docWriter.close();

      // Only allow a new merge to be triggered if we are
      // going to wait for merges:
      if (!hitOOM) {
        flush(waitForMerges, true);
      }

      if (waitForMerges)
        // Give merge scheduler last chance to run, in case
        // any pending merges are waiting:
        mergeScheduler.merge(this);

      mergePolicy.close();

      synchronized(this) {
        finishMerges(waitForMerges);
        stopMerges = true;
      }

      mergeScheduler.close();

      if (infoStream != null)
        message("now call final commit()");

      if (!hitOOM) {
        commitInternal(null);
      }

      if (infoStream != null)
        message("at close: " + segString());
      // used by assert below
      final DocumentsWriter oldWriter = docWriter;
      synchronized(this) {
        readerPool.close();
        docWriter = null;
        deleter.close();
      }

      if (writeLock != null) {
        writeLock.release();                          // release write lock
        writeLock = null;
      }
      synchronized(this) {
        closed = true;
      }
      assert oldWriter.assertNoActiveDWPT();
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "closeInternal");
    } finally {
      synchronized(this) {
        closing = false;
        notifyAll();
        if (!closed) {
          if (infoStream != null)
            message("hit exception while closing");
        }
      }
    }
  }
  
 

  /** Returns the Directory used by this index. */
  public Directory getDirectory() {
    return directory;
  }

  /** Returns the analyzer used by this index. */
  public Analyzer getAnalyzer() {
    ensureOpen();
    return analyzer;
  }

  /** Returns total number of docs in this index, including
   *  docs not yet flushed (still in the RAM buffer),
   *  not counting deletions.
   *  @see #numDocs */
  public synchronized int maxDoc() {
    int count;
    if (docWriter != null)
      count = docWriter.getNumDocs();
    else
      count = 0;

    count += segmentInfos.totalDocCount();
    return count;
  }

  /** Returns total number of docs in this index, including
   *  docs not yet flushed (still in the RAM buffer), and
   *  including deletions.  <b>NOTE:</b> buffered deletions
   *  are not counted.  If you really need these to be
   *  counted you should call {@link #commit()} first.
   *  @see #numDocs */
  public synchronized int numDocs() throws IOException {
    int count;
    if (docWriter != null)
      count = docWriter.getNumDocs();
    else
      count = 0;

    for (final SegmentInfo info : segmentInfos) {
      count += info.docCount - numDeletedDocs(info);
    }
    return count;
  }

  public synchronized boolean hasDeletions() throws IOException {
    ensureOpen();
    if (bufferedDeletesStream.any()) {
      return true;
    }
    if (docWriter.anyDeletions()) {
      return true;
    }
    for (final SegmentInfo info : segmentInfos) {
      if (info.hasDeletions()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adds a document to this index.
   *
   * <p> Note that if an Exception is hit (for example disk full)
   * then the index will be consistent, but this document
   * may not have been added.  Furthermore, it's possible
   * the index will have one segment in non-compound format
   * even when using compound files (when a merge has
   * partially succeeded).</p>
   *
   * <p> This method periodically flushes pending documents
   * to the Directory (see <a href="#flush">above</a>), and
   * also periodically triggers segment merges in the index
   * according to the {@link MergePolicy} in use.</p>
   *
   * <p>Merges temporarily consume space in the
   * directory. The amount of space required is up to 1X the
   * size of all segments being merged, when no
   * readers/searchers are open against the index, and up to
   * 2X the size of all segments being merged when
   * readers/searchers are open against the index (see
   * {@link #optimize()} for details). The sequence of
   * primitive merge operations performed is governed by the
   * merge policy.
   *
   * <p>Note that each term in the document can be no longer
   * than 16383 characters, otherwise an
   * IllegalArgumentException will be thrown.</p>
   *
   * <p>Note that it's possible to create an invalid Unicode
   * string in java if a UTF16 surrogate pair is malformed.
   * In this case, the invalid characters are silently
   * replaced with the Unicode replacement character
   * U+FFFD.</p>
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addDocument(Iterable<? extends IndexableField> doc) throws CorruptIndexException, IOException {
    addDocument(doc, analyzer);
  }

  /**
   * Adds a document to this index, using the provided analyzer instead of the
   * value of {@link #getAnalyzer()}.
   *
   * <p>See {@link #addDocument(Iterable)} for details on
   * index and IndexWriter state after an Exception, and
   * flushing/merging temporary free space requirements.</p>
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addDocument(Iterable<? extends IndexableField> doc, Analyzer analyzer) throws CorruptIndexException, IOException {
    updateDocument(null, doc, analyzer);
  }

  /**
   * Atomically adds a block of documents with sequentially
   * assigned document IDs, such that an external reader
   * will see all or none of the documents.
   *
   * <p><b>WARNING</b>: the index does not currently record
   * which documents were added as a block.  Today this is
   * fine, because merging will preserve the block (as long
   * as none them were deleted).  But it's possible in the
   * future that Lucene may more aggressively re-order
   * documents (for example, perhaps to obtain better index
   * compression), in which case you may need to fully
   * re-index your documents at that time.
   *
   * <p>See {@link #addDocument(Iterable)} for details on
   * index and IndexWriter state after an Exception, and
   * flushing/merging temporary free space requirements.</p>
   *
   * <p><b>NOTE</b>: tools that do offline splitting of an index
   * (for example, IndexSplitter in contrib) or
   * re-sorting of documents (for example, IndexSorter in
   * contrib) are not aware of these atomically added documents
   * and will likely break them up.  Use such tools at your
   * own risk!
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public void addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws CorruptIndexException, IOException {
    addDocuments(docs, analyzer);
  }

  /**
   * Atomically adds a block of documents, analyzed using the
   * provided analyzer, with sequentially assigned document
   * IDs, such that an external reader will see all or none
   * of the documents. 
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public void addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer analyzer) throws CorruptIndexException, IOException {
    updateDocuments(null, docs, analyzer);
  }

  /**
   * Atomically deletes documents matching the provided
   * delTerm and adds a block of documents with sequentially
   * assigned document IDs, such that an external reader
   * will see all or none of the documents. 
   *
   * See {@link #addDocuments(Iterable)}.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public void updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws CorruptIndexException, IOException {
    updateDocuments(delTerm, docs, analyzer);
  }

  /**
   * Atomically deletes documents matching the provided
   * delTerm and adds a block of documents, analyzed  using
   * the provided analyzer, with sequentially
   * assigned document IDs, such that an external reader
   * will see all or none of the documents. 
   *
   * See {@link #addDocuments(Iterable)}.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public void updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer analyzer) throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      boolean success = false;
      boolean anySegmentFlushed = false;
      try {
        anySegmentFlushed = docWriter.updateDocuments(docs, analyzer, delTerm);
        success = true;
      } finally {
        if (!success && infoStream != null) {
          message("hit exception updating document");
        }
      }
      if (anySegmentFlushed) {
        maybeMerge();
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateDocuments");
    }
  }

  /**
   * Deletes the document(s) containing <code>term</code>.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param term the term to identify the documents to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      docWriter.deleteTerms(term);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Term)");
    }
  }

  /**
   * Deletes the document(s) containing any of the
   * terms. All given deletes are applied and flushed atomically
   * at the same time.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param terms array of terms to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Term... terms) throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      docWriter.deleteTerms(terms);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Term..)");
    }
  }

  /**
   * Deletes the document(s) matching the provided query.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param query the query to identify the documents to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      docWriter.deleteQueries(query);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Query)");
    }
  }

  /**
   * Deletes the document(s) matching any of the provided queries.
   * All given deletes are applied and flushed atomically at the same time.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param queries array of queries to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Query... queries) throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      docWriter.deleteQueries(queries);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Query..)");
    }
  }

  /**
   * Updates a document by first deleting the document(s)
   * containing <code>term</code> and then adding the new
   * document.  The delete and then add are atomic as seen
   * by a reader on the same index (flush may happen only after
   * the add).
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param term the term to identify the document(s) to be
   * deleted
   * @param doc the document to be added
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void updateDocument(Term term, Iterable<? extends IndexableField> doc) throws CorruptIndexException, IOException {
    ensureOpen();
    updateDocument(term, doc, getAnalyzer());
  }

  /**
   * Updates a document by first deleting the document(s)
   * containing <code>term</code> and then adding the new
   * document.  The delete and then add are atomic as seen
   * by a reader on the same index (flush may happen only after
   * the add).
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param term the term to identify the document(s) to be
   * deleted
   * @param doc the document to be added
   * @param analyzer the analyzer to use when analyzing the document
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void updateDocument(Term term, Iterable<? extends IndexableField> doc, Analyzer analyzer)
      throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      boolean success = false;
      boolean anySegmentFlushed = false;
      try {
        anySegmentFlushed = docWriter.updateDocument(doc, analyzer, term);
        success = true;
      } finally {
        if (!success && infoStream != null)
          message("hit exception updating document");
      }

      if (anySegmentFlushed) {
        maybeMerge();
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateDocument");
    }
  }

  // for test purpose
  final synchronized int getSegmentCount(){
    return segmentInfos.size();
  }

  // for test purpose
  final synchronized int getNumBufferedDocuments(){
    return docWriter.getNumDocs();
  }

  // for test purpose
  final synchronized Collection<String> getIndexFileNames() throws IOException {
    return segmentInfos.files(directory, true);
  }

  // for test purpose
  final synchronized int getDocCount(int i) {
    if (i >= 0 && i < segmentInfos.size()) {
      return segmentInfos.info(i).docCount;
    } else {
      return -1;
    }
  }

  // for test purpose
  final int getFlushCount() {
    return flushCount.get();
  }

  // for test purpose
  final int getFlushDeletesCount() {
    return flushDeletesCount.get();
  }

  final String newSegmentName() {
    // Cannot synchronize on IndexWriter because that causes
    // deadlock
    synchronized(segmentInfos) {
      // Important to increment changeCount so that the
      // segmentInfos is written on close.  Otherwise we
      // could close, re-open and re-return the same segment
      // name that was previously returned which can cause
      // problems at least with ConcurrentMergeScheduler.
      changeCount++;
      segmentInfos.changed();
      return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
    }
  }

  /** If non-null, information about merges will be printed to this.
   */
  private PrintStream infoStream;
  private static PrintStream defaultInfoStream;

  /**
   * Requests an "optimize" operation on an index, priming the index
   * for the fastest available search. Traditionally this has meant
   * merging all segments into a single segment as is done in the
   * default merge policy, but individual merge policies may implement
   * optimize in different ways.
   *
   * <p> Optimize is a fairly costly operation, so you
   * should only do it if your search performance really
   * requires it.  Many search applications do fine never
   * calling optimize. </p>
   *
   * <p>Note that optimize requires 2X the index size free
   * space in your Directory (3X if you're using compound
   * file format).  For example, if your index size is 10 MB
   * then you need 20 MB free for optimize to complete (30
   * MB if you're using compound file format).  Also,
   * it's best to call {@link #commit()} after the optimize
   * completes to allow IndexWriter to free up disk space.</p>
   *
   * <p>If some but not all readers re-open while an
   * optimize is underway, this will cause > 2X temporary
   * space to be consumed as those new readers will then
   * hold open the partially optimized segments at that
   * time.  It is best not to re-open readers while optimize
   * is running.</p>
   *
   * <p>The actual temporary usage could be much less than
   * these figures (it depends on many factors).</p>
   *
   * <p>In general, once the optimize completes, the total size of the
   * index will be less than the size of the starting index.
   * It could be quite a bit smaller (if there were many
   * pending deletes) or just slightly smaller.</p>
   *
   * <p>If an Exception is hit during optimize(), for example
   * due to disk full, the index will not be corrupt and no
   * documents will have been lost.  However, it may have
   * been partially optimized (some segments were merged but
   * not all), and it's possible that one of the segments in
   * the index will be in non-compound format even when
   * using compound file format.  This will occur when the
   * Exception is hit during conversion of the segment into
   * compound format.</p>
   *
   * <p>This call will optimize those segments present in
   * the index when the call started.  If other threads are
   * still adding documents and flushing segments, those
   * newly created segments will not be optimized unless you
   * call optimize again.</p>
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * <p><b>NOTE</b>: if you call {@link #close(boolean)}
   * with <tt>false</tt>, which aborts all running merges,
   * then any thread still running this method might hit a
   * {@link MergePolicy.MergeAbortedException}.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @see MergePolicy#findMergesForOptimize
  */
  public void optimize() throws CorruptIndexException, IOException {
    optimize(true);
  }

  /**
   * Optimize the index down to <= maxNumSegments.  If
   * maxNumSegments==1 then this is the same as {@link
   * #optimize()}.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @param maxNumSegments maximum number of segments left
   * in the index after optimization finishes
   */
  public void optimize(int maxNumSegments) throws CorruptIndexException, IOException {
    optimize(maxNumSegments, true);
  }

  /** Just like {@link #optimize()}, except you can specify
   *  whether the call should block until the optimize
   *  completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   */
  public void optimize(boolean doWait) throws CorruptIndexException, IOException {
    optimize(1, doWait);
  }

  /** Just like {@link #optimize(int)}, except you can
   *  specify whether the call should block until the
   *  optimize completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   */
  public void optimize(int maxNumSegments, boolean doWait) throws CorruptIndexException, IOException {
    ensureOpen();

    if (maxNumSegments < 1)
      throw new IllegalArgumentException("maxNumSegments must be >= 1; got " + maxNumSegments);

    if (infoStream != null) {
      message("optimize: index now " + segString());
      message("now flush at optimize");
    }

    flush(true, true);

    synchronized(this) {
      resetMergeExceptions();
      segmentsToOptimize.clear();
      for(SegmentInfo info : segmentInfos) {
        segmentsToOptimize.put(info, Boolean.TRUE);
      }
      optimizeMaxNumSegments = maxNumSegments;

      // Now mark all pending & running merges as optimize
      // merge:
      for(final MergePolicy.OneMerge merge  : pendingMerges) {
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = maxNumSegments;
        segmentsToOptimize.put(merge.info, Boolean.TRUE);
      }

      for ( final MergePolicy.OneMerge merge: runningMerges ) {
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = maxNumSegments;
        segmentsToOptimize.put(merge.info, Boolean.TRUE);
      }
    }

    maybeMerge(maxNumSegments, true);

    if (doWait) {
      synchronized(this) {
        while(true) {

          if (hitOOM) {
            throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot complete optimize");
          }

          if (mergeExceptions.size() > 0) {
            // Forward any exceptions in background merge
            // threads to the current thread:
            final int size = mergeExceptions.size();
            for(int i=0;i<size;i++) {
              final MergePolicy.OneMerge merge = mergeExceptions.get(i);
              if (merge.optimize) {
                IOException err = new IOException("background merge hit exception: " + merge.segString(directory));
                final Throwable t = merge.getException();
                if (t != null)
                  err.initCause(t);
                throw err;
              }
            }
          }

          if (optimizeMergesPending())
            doWait();
          else
            break;
        }
      }

      // If close is called while we are still
      // running, throw an exception so the calling
      // thread will know the optimize did not
      // complete
      ensureOpen();
    }

    // NOTE: in the ConcurrentMergeScheduler case, when
    // doWait is false, we can return immediately while
    // background threads accomplish the optimization
  }

  /** Returns true if any merges in pendingMerges or
   *  runningMerges are optimization merges. */
  private synchronized boolean optimizeMergesPending() {
    for (final MergePolicy.OneMerge merge : pendingMerges) {
      if (merge.optimize)
        return true;
    }

    for (final MergePolicy.OneMerge merge : runningMerges) {
      if (merge.optimize)
        return true;
    }

    return false;
  }

  /** Just like {@link #expungeDeletes()}, except you can
   *  specify whether the call should block until the
   *  operation completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * <p><b>NOTE</b>: if you call {@link #close(boolean)}
   * with <tt>false</tt>, which aborts all running merges,
   * then any thread still running this method might hit a
   * {@link MergePolicy.MergeAbortedException}.
   */
  public void expungeDeletes(boolean doWait)
    throws CorruptIndexException, IOException {
    ensureOpen();

    flush(true, true);

    if (infoStream != null)
      message("expungeDeletes: index now " + segString());

    MergePolicy.MergeSpecification spec;

    synchronized(this) {
      spec = mergePolicy.findMergesToExpungeDeletes(segmentInfos);
      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++)
          registerMerge(spec.merges.get(i));
      }
    }

    mergeScheduler.merge(this);

    if (spec != null && doWait) {
      final int numMerges = spec.merges.size();
      synchronized(this) {
        boolean running = true;
        while(running) {

          if (hitOOM) {
            throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot complete expungeDeletes");
          }

          // Check each merge that MergePolicy asked us to
          // do, to see if any of them are still running and
          // if any of them have hit an exception.
          running = false;
          for(int i=0;i<numMerges;i++) {
            final MergePolicy.OneMerge merge = spec.merges.get(i);
            if (pendingMerges.contains(merge) || runningMerges.contains(merge))
              running = true;
            Throwable t = merge.getException();
            if (t != null) {
              IOException ioe = new IOException("background merge hit exception: " + merge.segString(directory));
              ioe.initCause(t);
              throw ioe;
            }
          }

          // If any of our merges are still running, wait:
          if (running)
            doWait();
        }
      }
    }

    // NOTE: in the ConcurrentMergeScheduler case, when
    // doWait is false, we can return immediately while
    // background threads accomplish the optimization
  }


  /** Expunges all deletes from the index.  When an index
   *  has many document deletions (or updates to existing
   *  documents), it's best to either call optimize or
   *  expungeDeletes to remove all unused data in the index
   *  associated with the deleted documents.  To see how
   *  many deletions you have pending in your index, call
   *  {@link IndexReader#numDeletedDocs}
   *  This saves disk space and memory usage while
   *  searching.  expungeDeletes should be somewhat faster
   *  than optimize since it does not insist on reducing the
   *  index to a single segment (though, this depends on the
   *  {@link MergePolicy}; see {@link
   *  MergePolicy#findMergesToExpungeDeletes}.). Note that
   *  this call does not first commit any buffered
   *  documents, so you must do so yourself if necessary.
   *  See also {@link #expungeDeletes(boolean)}
   *
   *  <p><b>NOTE</b>: this method first flushes a new
   *  segment (if there are indexed documents), and applies
   *  all buffered deletes.
   *
   *  <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   *  you should immediately close the writer.  See <a
   *  href="#OOME">above</a> for details.</p>
   */
  public void expungeDeletes() throws CorruptIndexException, IOException {
    expungeDeletes(true);
  }

  /**
   * Expert: asks the mergePolicy whether any merges are
   * necessary now and if so, runs the requested merges and
   * then iterate (test again if merges are needed) until no
   * more merges are returned by the mergePolicy.
   *
   * Explicit calls to maybeMerge() are usually not
   * necessary. The most common case is when merge policy
   * parameters have changed.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   */
  public final void maybeMerge() throws CorruptIndexException, IOException {
    maybeMerge(false);
  }

  private final void maybeMerge(boolean optimize) throws CorruptIndexException, IOException {
    maybeMerge(1, optimize);
  }

  private final void maybeMerge(int maxNumSegmentsOptimize, boolean optimize) throws CorruptIndexException, IOException {
    updatePendingMerges(maxNumSegmentsOptimize, optimize);
    mergeScheduler.merge(this);
  }

  private synchronized void updatePendingMerges(int maxNumSegmentsOptimize, boolean optimize)
    throws CorruptIndexException, IOException {
    assert !optimize || maxNumSegmentsOptimize > 0;

    if (stopMerges) {
      return;
    }

    // Do not start new merges if we've hit OOME
    if (hitOOM) {
      return;
    }

    final MergePolicy.MergeSpecification spec;
    if (optimize) {
      spec = mergePolicy.findMergesForOptimize(segmentInfos, maxNumSegmentsOptimize, Collections.unmodifiableMap(segmentsToOptimize));
      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++) {
          final MergePolicy.OneMerge merge = spec.merges.get(i);
          merge.optimize = true;
          merge.maxNumSegmentsOptimize = maxNumSegmentsOptimize;
        }
      }

    } else {
      spec = mergePolicy.findMerges(segmentInfos);
    }

    if (spec != null) {
      final int numMerges = spec.merges.size();
      for(int i=0;i<numMerges;i++) {
        registerMerge(spec.merges.get(i));
      }
    }
  }

  /** Expert: to be used by a {@link MergePolicy} to avoid
   *  selecting merges for segments already being merged.
   *  The returned collection is not cloned, and thus is
   *  only safe to access if you hold IndexWriter's lock
   *  (which you do when IndexWriter invokes the
   *  MergePolicy).
   *
   *  <p>Do not alter the returned collection! */
  public synchronized Collection<SegmentInfo> getMergingSegments() {
    return mergingSegments;
  }

  /**
   * Expert: the {@link MergeScheduler} calls this method to retrieve the next
   * merge requested by the MergePolicy
   * 
   * @lucene.experimental
   */
  public synchronized MergePolicy.OneMerge getNextMerge() {
    if (pendingMerges.size() == 0)
      return null;
    else {
      // Advance the merge from pending to running
      MergePolicy.OneMerge merge = pendingMerges.removeFirst();
      runningMerges.add(merge);
      return merge;
    }
  }

  /**
   * Close the <code>IndexWriter</code> without committing
   * any changes that have occurred since the last commit
   * (or since it was opened, if commit hasn't been called).
   * This removes any temporary files that had been created,
   * after which the state of the index will be the same as
   * it was when commit() was last called or when this
   * writer was first opened.  This also clears a previous
   * call to {@link #prepareCommit}.
   * @throws IOException if there is a low-level IO error
   */
  public void rollback() throws IOException {
    ensureOpen();

    // Ensure that only one thread actually gets to do the closing:
    if (shouldClose())
      rollbackInternal();
  }

  private void rollbackInternal() throws IOException {

    boolean success = false;

    if (infoStream != null ) {
      message("rollback");
    }

    try {
      synchronized(this) {
        finishMerges(false);
        stopMerges = true;
      }

      if (infoStream != null ) {
        message("rollback: done finish merges");
      }

      // Must pre-close these two, in case they increment
      // changeCount so that we can then set it to false
      // before calling closeInternal
      mergePolicy.close();
      mergeScheduler.close();

      bufferedDeletesStream.clear();

      synchronized(this) {

        if (pendingCommit != null) {
          pendingCommit.rollbackCommit(directory);
          deleter.decRef(pendingCommit);
          pendingCommit = null;
          notifyAll();
        }

        // Keep the same segmentInfos instance but replace all
        // of its SegmentInfo instances.  This is so the next
        // attempt to commit using this instance of IndexWriter
        // will always write to a new generation ("write
        // once").
        segmentInfos.rollbackSegmentInfos(rollbackSegments);
        if (infoStream != null ) {
          message("rollback: infos=" + segString(segmentInfos));
        }

        docWriter.abort();

        assert testPoint("rollback before checkpoint");

        // Ask deleter to locate unreferenced files & remove
        // them:
        deleter.checkpoint(segmentInfos, false);
        deleter.refresh();
      }

      // Don't bother saving any changes in our segmentInfos
      readerPool.clear(null);

      lastCommitChangeCount = changeCount;

      success = true;
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "rollbackInternal");
    } finally {
      synchronized(this) {
        if (!success) {
          closing = false;
          notifyAll();
          if (infoStream != null)
            message("hit exception during rollback");
        }
      }
    }

    closeInternal(false);
  }

  /**
   * Delete all documents in the index.
   *
   * <p>This method will drop all buffered documents and will
   *    remove all segments from the index. This change will not be
   *    visible until a {@link #commit()} has been called. This method
   *    can be rolled back using {@link #rollback()}.</p>
   *
   * <p>NOTE: this method is much faster than using deleteDocuments( new MatchAllDocsQuery() ).</p>
   *
   * <p>NOTE: this method will forcefully abort all merges
   *    in progress.  If other threads are running {@link
   *    #optimize()}, {@link #addIndexes(IndexReader[])} or
   *    {@link #expungeDeletes} methods, they may receive
   *    {@link MergePolicy.MergeAbortedException}s.
   */
  public synchronized void deleteAll() throws IOException {
    try {

      // Abort any running merges
      finishMerges(false);

      // Remove any buffered docs
      docWriter.abort();

      // Remove all segments
      segmentInfos.clear();

      // Ask deleter to locate unreferenced files & remove them:
      deleter.checkpoint(segmentInfos, false);
      deleter.refresh();

      // Don't bother saving any changes in our segmentInfos
      readerPool.clear(null);

      // Mark that the index has changed
      ++changeCount;
      segmentInfos.changed();
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteAll");
    } finally {
      if (infoStream != null) {
        message("hit exception during deleteAll");
      }
    }
  }

  private synchronized void finishMerges(boolean waitForMerges) throws IOException {
    if (!waitForMerges) {

      stopMerges = true;

      // Abort all pending & running merges:
      for (final MergePolicy.OneMerge merge : pendingMerges) {
        if (infoStream != null)
          message("now abort pending merge " + merge.segString(directory));
        merge.abort();
        mergeFinish(merge);
      }
      pendingMerges.clear();

      for (final MergePolicy.OneMerge merge : runningMerges) {
        if (infoStream != null)
          message("now abort running merge " + merge.segString(directory));
        merge.abort();
      }

      // These merges periodically check whether they have
      // been aborted, and stop if so.  We wait here to make
      // sure they all stop.  It should not take very long
      // because the merge threads periodically check if
      // they are aborted.
      while(runningMerges.size() > 0) {
        if (infoStream != null)
          message("now wait for " + runningMerges.size() + " running merge to abort");
        doWait();
      }

      stopMerges = false;
      notifyAll();

      assert 0 == mergingSegments.size();

      if (infoStream != null)
        message("all running merges have aborted");

    } else {
      // waitForMerges() will ensure any running addIndexes finishes.
      // It's fine if a new one attempts to start because from our
      // caller above the call will see that we are in the
      // process of closing, and will throw an
      // AlreadyClosedException.
      waitForMerges();
    }
  }

  /**
   * Wait for any currently outstanding merges to finish.
   *
   * <p>It is guaranteed that any merges started prior to calling this method
   *    will have completed once this method completes.</p>
   */
  public synchronized void waitForMerges() {
    if (infoStream != null) {
      message("waitForMerges");
    }
    while(pendingMerges.size() > 0 || runningMerges.size() > 0) {
      doWait();
    }

    // sanity check
    assert 0 == mergingSegments.size();

    if (infoStream != null) {
      message("waitForMerges done");
    }
  }

  /**
   * Called whenever the SegmentInfos has been updated and
   * the index files referenced exist (correctly) in the
   * index directory.
   */
  synchronized void checkpoint() throws IOException {
    changeCount++;
    segmentInfos.changed();
    deleter.checkpoint(segmentInfos, false);
  }

  /**
   * Prepares the {@link SegmentInfo} for the new flushed segment and persists
   * the deleted documents {@link BitVector}. Use
   * {@link #publishFlushedSegment(SegmentInfo, FrozenBufferedDeletes)} to
   * publish the returned {@link SegmentInfo} together with its segment private
   * delete packet.
   * 
   * @see #publishFlushedSegment(SegmentInfo, FrozenBufferedDeletes)
   */
  SegmentInfo prepareFlushedSegment(FlushedSegment flushedSegment) throws IOException {
    assert flushedSegment != null;

    SegmentInfo newSegment = flushedSegment.segmentInfo;

    setDiagnostics(newSegment, "flush");
    
    IOContext context = new IOContext(new FlushInfo(newSegment.docCount, newSegment.sizeInBytes(true)));

    boolean success = false;
    try {
      if (useCompoundFile(newSegment)) {
        String compoundFileName = IndexFileNames.segmentFileName(newSegment.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION);
        message("creating compound file " + compoundFileName);
        // Now build compound file
        final Directory cfsDir = new CompoundFileDirectory(directory, compoundFileName, context, true);
        IOException prior = null;
        try {
          for(String fileName : newSegment.files()) {
            directory.copy(cfsDir, fileName, fileName, context);
          }
        } catch(IOException ex) {
          prior = ex;
        } finally {
          IOUtils.closeWhileHandlingException(prior, cfsDir);
        }
        // Perform the merge
        
        synchronized(this) {
          deleter.deleteNewFiles(newSegment.files());
        }

        newSegment.setUseCompoundFile(true);
      }

      // Must write deleted docs after the CFS so we don't
      // slurp the del file into CFS:
      if (flushedSegment.liveDocs != null) {
        final int delCount = flushedSegment.segmentInfo.docCount - flushedSegment.liveDocs.count();
        assert delCount > 0;
        newSegment.setDelCount(delCount);
        newSegment.advanceDelGen();
        final String delFileName = newSegment.getDelFileName();
        if (infoStream != null) {
          message("flush: write " + delCount + " deletes to " + delFileName);
        }
        boolean success2 = false;
        try {
          // TODO: in the NRT case it'd be better to hand
          // this del vector over to the
          // shortly-to-be-opened SegmentReader and let it
          // carry the changes; there's no reason to use
          // filesystem as intermediary here.
          flushedSegment.liveDocs.write(directory, delFileName, context);
          success2 = true;
        } finally {
          if (!success2) {
            try {
              directory.deleteFile(delFileName);
            } catch (Throwable t) {
              // suppress this so we keep throwing the
              // original exception
            }
          }
        }
      }

      success = true;
    } finally {
      if (!success) {
        if (infoStream != null) {
          message("hit exception " +
              "reating compound file for newly flushed segment " + newSegment.name);
        }

        synchronized(this) {
          deleter.refresh(newSegment.name);
        }
      }
    }
    return newSegment;
  }
  
  /**
   * Atomically adds the segment private delete packet and publishes the flushed
   * segments SegmentInfo to the index writer. NOTE: use
   * {@link #prepareFlushedSegment(FlushedSegment)} to obtain the
   * {@link SegmentInfo} for the flushed segment.
   * 
   * @see #prepareFlushedSegment(FlushedSegment)
   */
  synchronized void publishFlushedSegment(SegmentInfo newSegment,
      FrozenBufferedDeletes packet, FrozenBufferedDeletes globalPacket) throws IOException {
    // Lock order IW -> BDS
    synchronized (bufferedDeletesStream) {
      if (infoStream != null) {
        message("publishFlushedSegment");  
      }
      
      if (globalPacket != null && globalPacket.any()) {
        bufferedDeletesStream.push(globalPacket);
      } 
      // Publishing the segment must be synched on IW -> BDS to make the sure
      // that no merge prunes away the seg. private delete packet
      final long nextGen;
      if (packet != null && packet.any()) {
        nextGen = bufferedDeletesStream.push(packet);
      } else {
        // Since we don't have a delete packet to apply we can get a new
        // generation right away
        nextGen = bufferedDeletesStream.getNextGen();
      }
      if (infoStream != null) {
        message("publish sets newSegment delGen=" + nextGen);
      }
      newSegment.setBufferedDeletesGen(nextGen);
      segmentInfos.add(newSegment);
      checkpoint();
    }
  }

  synchronized boolean useCompoundFile(SegmentInfo segmentInfo) throws IOException {
    return mergePolicy.useCompoundFile(segmentInfos, segmentInfo);
  }

  private synchronized void resetMergeExceptions() {
    mergeExceptions = new ArrayList<MergePolicy.OneMerge>();
    mergeGen++;
  }

  private void noDupDirs(Directory... dirs) {
    HashSet<Directory> dups = new HashSet<Directory>();
    for(int i=0;i<dirs.length;i++) {
      if (dups.contains(dirs[i]))
        throw new IllegalArgumentException("Directory " + dirs[i] + " appears more than once");
      if (dirs[i] == directory)
        throw new IllegalArgumentException("Cannot add directory to itself");
      dups.add(dirs[i]);
    }
  }

  /**
   * Adds all segments from an array of indexes into this index.
   *
   * <p>This may be used to parallelize batch indexing. A large document
   * collection can be broken into sub-collections. Each sub-collection can be
   * indexed in parallel, on a different thread, process or machine. The
   * complete index can then be created by merging sub-collection indexes
   * with this method.
   *
   * <p>
   * <b>NOTE:</b> the index in each {@link Directory} must not be
   * changed (opened by a writer) while this method is
   * running.  This method does not acquire a write lock in
   * each input Directory, so it is up to the caller to
   * enforce this.
   *
   * <p>This method is transactional in how Exceptions are
   * handled: it does not commit a new segments_N file until
   * all indexes are added.  This means if an Exception
   * occurs (for example disk full), then either no indexes
   * will have been added or they all will have been.
   *
   * <p>Note that this requires temporary free space in the
   * {@link Directory} up to 2X the sum of all input indexes
   * (including the starting index). If readers/searchers
   * are open against the starting index, then temporary
   * free space required will be higher by the size of the
   * starting index (see {@link #optimize()} for details).
   *
   * <p>
   * <b>NOTE:</b> this method only copies the segments of the incoming indexes
   * and does not merge them. Therefore deleted documents are not removed and
   * the new segments are not merged with the existing ones. Also, if the merge 
   * policy allows compound files, then any segment that is not compound is 
   * converted to such. However, if the segment is compound, it is copied as-is
   * even if the merge policy does not allow compound files.
   *
   * <p>This requires this index not be among those to be added.
   *
   * <p>
   * <b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer. See <a
   * href="#OOME">above</a> for details.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addIndexes(Directory... dirs) throws CorruptIndexException, IOException {
    ensureOpen();

    noDupDirs(dirs);

    try {
      if (infoStream != null)
        message("flush at addIndexes(Directory...)");
      flush(false, true);

      int docCount = 0;
      List<SegmentInfo> infos = new ArrayList<SegmentInfo>();
      Comparator<String> versionComparator = StringHelper.getVersionComparator();
      for (Directory dir : dirs) {
        if (infoStream != null) {
          message("addIndexes: process directory " + dir);
        }
        SegmentInfos sis = new SegmentInfos(codecs); // read infos from dir
        sis.read(dir, codecs);
        final Set<String> dsFilesCopied = new HashSet<String>();
        final Map<String, String> dsNames = new HashMap<String, String>();
        for (SegmentInfo info : sis) {
          assert !infos.contains(info): "dup info dir=" + info.dir + " name=" + info.name;

          docCount += info.docCount;
          String newSegName = newSegmentName();
          String dsName = info.getDocStoreSegment();

          if (infoStream != null) {
            message("addIndexes: process segment origName=" + info.name + " newName=" + newSegName + " dsName=" + dsName + " info=" + info);
          }

          // create CFS only if the source segment is not CFS, and MP agrees it
          // should be CFS.
          boolean createCFS;
          synchronized (this) { // Guard segmentInfos
            createCFS = !info.getUseCompoundFile()
                && mergePolicy.useCompoundFile(segmentInfos, info)
                // optimize case only for segments that don't share doc stores
                && versionComparator.compare(info.getVersion(), "3.1") >= 0;
          }
          
          IOContext context = new IOContext(new MergeInfo(info.docCount, info.sizeInBytes(true), true, false));
          
          if (createCFS) {
            copySegmentIntoCFS(info, newSegName, context);
          } else {
            copySegmentAsIs(info, newSegName, dsNames, dsFilesCopied, context);
          }

          infos.add(info);
        }
      }

      synchronized (this) {
        ensureOpen();
        segmentInfos.addAll(infos);
        checkpoint();
      }

    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addIndexes(Directory...)");
    }
  }

  /** Merges the provided indexes into this index.
   * <p>After this completes, the index is optimized. </p>
   * <p>The provided IndexReaders are not closed.</p>
   *
   * <p><b>NOTE:</b> while this is running, any attempts to
   * add or delete documents (with another thread) will be
   * paused until this method completes.
   *
   * <p>See {@link #addIndexes} for details on transactional 
   * semantics, temporary free space required in the Directory, 
   * and non-CFS segments on an Exception.</p>
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * <p><b>NOTE</b>: if you call {@link #close(boolean)}
   * with <tt>false</tt>, which aborts all running merges,
   * then any thread still running this method might hit a
   * {@link MergePolicy.MergeAbortedException}.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addIndexes(IndexReader... readers) throws CorruptIndexException, IOException {
    ensureOpen();
    int numDocs = 0;

    try {
      if (infoStream != null)
        message("flush at addIndexes(IndexReader...)");
      flush(false, true);

      String mergedName = newSegmentName();
      for (IndexReader indexReader : readers) {
        numDocs += indexReader.numDocs();
       }
       final IOContext context = new IOContext(new MergeInfo(numDocs, -1, true, false));      

      // TODO: somehow we should fix this merge so it's
      // abortable so that IW.close(false) is able to stop it
      SegmentMerger merger = new SegmentMerger(directory, config.getTermIndexInterval(),
                                               mergedName, null, payloadProcessorProvider,
                                               globalFieldNumberMap.newFieldInfos(SegmentCodecsBuilder.create(codecs)), context);

      for (IndexReader reader : readers)      // add new indexes
        merger.add(reader);
      int docCount = merger.merge();                // merge 'em

      final FieldInfos fieldInfos = merger.fieldInfos();
      SegmentInfo info = new SegmentInfo(mergedName, docCount, directory,
                                         false, merger.getSegmentCodecs(),
                                         fieldInfos);
      setDiagnostics(info, "addIndexes(IndexReader...)");

      boolean useCompoundFile;
      synchronized(this) { // Guard segmentInfos
        if (stopMerges) {
          deleter.deleteNewFiles(info.files());
          return;
        }
        ensureOpen();
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, info);
      }

      // Now create the compound file if needed
      if (useCompoundFile) {
        merger.createCompoundFile(mergedName + ".cfs", info, context);

        // delete new non cfs files directly: they were never
        // registered with IFD
        deleter.deleteNewFiles(info.files());
        info.setUseCompoundFile(true);
      }

      // Register the new segment
      synchronized(this) {
        if (stopMerges) {
          deleter.deleteNewFiles(info.files());
          return;
        }
        ensureOpen();
        segmentInfos.add(info);
        checkpoint();
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addIndexes(IndexReader...)");
    }
  }

  /** Copies the segment into the IndexWriter's directory, as a compound segment. */
  private void copySegmentIntoCFS(SegmentInfo info, String segName, IOContext context) throws IOException {
    String segFileName = IndexFileNames.segmentFileName(segName, "", IndexFileNames.COMPOUND_FILE_EXTENSION);
    Collection<String> files = info.files();
    final CompoundFileDirectory cfsdir = new CompoundFileDirectory(directory, segFileName, context, true);
    try {
      for (String file : files) {
        String newFileName = segName + IndexFileNames.stripSegmentName(file);
        if (!IndexFileNames.matchesExtension(file, IndexFileNames.DELETES_EXTENSION)
            && !IndexFileNames.isSeparateNormsFile(file)) {
          info.dir.copy(cfsdir, file, file, context);
        } else {
          assert !directory.fileExists(newFileName): "file \"" + newFileName + "\" already exists";
          info.dir.copy(directory, file, newFileName, context);
        }
      }
    } finally {
      IOUtils.close(cfsdir);
    }
    
    info.dir = directory;
    info.name = segName;
    info.setUseCompoundFile(true);
  }
  
  /** Copies the segment files as-is into the IndexWriter's directory. */
  private void copySegmentAsIs(SegmentInfo info, String segName,
      Map<String, String> dsNames, Set<String> dsFilesCopied, IOContext context)
      throws IOException {
    // Determine if the doc store of this segment needs to be copied. It's
    // only relevant for segments that share doc store with others,
    // because the DS might have been copied already, in which case we
    // just want to update the DS name of this SegmentInfo.
    // NOTE: pre-3x segments include a null DSName if they don't share doc
    // store. The following code ensures we don't accidentally insert
    // 'null' to the map.
    String dsName = info.getDocStoreSegment();
    final String newDsName;
    if (dsName != null) {
      if (dsNames.containsKey(dsName)) {
        newDsName = dsNames.get(dsName);
      } else {
        dsNames.put(dsName, segName);
        newDsName = segName;
      }
    } else {
      newDsName = segName;
    }
    
    // Copy the segment files
    for (String file: info.files()) {
      final String newFileName;
      if (IndexFileNames.isDocStoreFile(file)) {
        newFileName = newDsName + IndexFileNames.stripSegmentName(file);
        if (dsFilesCopied.contains(newFileName)) {
          continue;
        }
        dsFilesCopied.add(newFileName);
      } else {
        newFileName = segName + IndexFileNames.stripSegmentName(file);
      }
      
      assert !directory.fileExists(newFileName): "file \"" + newFileName + "\" already exists";
      info.dir.copy(directory, file, newFileName, context);
    }
    
    info.setDocStore(info.getDocStoreOffset(), newDsName, info.getDocStoreIsCompoundFile());
    info.dir = directory;
    info.name = segName;
  }
  
  /**
   * A hook for extending classes to execute operations after pending added and
   * deleted documents have been flushed to the Directory but before the change
   * is committed (new segments_N file written).
   */
  protected void doAfterFlush() throws IOException {}

  /**
   * A hook for extending classes to execute operations before pending added and
   * deleted documents are flushed to the Directory.
   */
  protected void doBeforeFlush() throws IOException {}

  /** Expert: prepare for commit.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @see #prepareCommit(Map) */
  public final void prepareCommit() throws CorruptIndexException, IOException {
    ensureOpen();
    prepareCommit(null);
  }

  /** <p>Expert: prepare for commit, specifying
   *  commitUserData Map (String -> String).  This does the
   *  first phase of 2-phase commit. This method does all
   *  steps necessary to commit changes since this writer
   *  was opened: flushes pending added and deleted docs,
   *  syncs the index files, writes most of next segments_N
   *  file.  After calling this you must call either {@link
   *  #commit()} to finish the commit, or {@link
   *  #rollback()} to revert the commit and undo all changes
   *  done since the writer was opened.</p>
   *
   *  You can also just call {@link #commit(Map)} directly
   *  without prepareCommit first in which case that method
   *  will internally call prepareCommit.
   *
   *  <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   *  you should immediately close the writer.  See <a
   *  href="#OOME">above</a> for details.</p>
   *
   *  @param commitUserData Opaque Map (String->String)
   *  that's recorded into the segments file in the index,
   *  and retrievable by {@link
   *  IndexReader#getCommitUserData}.  Note that when
   *  IndexWriter commits itself during {@link #close}, the
   *  commitUserData is unchanged (just carried over from
   *  the prior commit).  If this is null then the previous
   *  commitUserData is kept.  Also, the commitUserData will
   *  only "stick" if there are actually changes in the
   *  index to commit.
   */
  public final void prepareCommit(Map<String,String> commitUserData) throws CorruptIndexException, IOException {

    if (infoStream != null) {
      message("prepareCommit: flush");
      message("  index before flush " + segString());
    }

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot commit");
    }

    if (pendingCommit != null) {
      throw new IllegalStateException("prepareCommit was already called with no corresponding call to commit");
    }

    doBeforeFlush();
    assert testPoint("startDoFlush");
    SegmentInfos toCommit = null;
    boolean anySegmentsFlushed = false;

    // This is copied from doFlush, except it's modified to
    // clone & incRef the flushed SegmentInfos inside the
    // sync block:

    try {

      synchronized (fullFlushLock) {
        boolean flushSuccess = false;
        boolean success = false;
        try {
          anySegmentsFlushed = docWriter.flushAllThreads();
          if (!anySegmentsFlushed) {
            // prevent double increment since docWriter#doFlush increments the flushcount
            // if we flushed anything.
            flushCount.incrementAndGet();
          }
          flushSuccess = true;

          synchronized(this) {
            maybeApplyDeletes(true);

            readerPool.commit(segmentInfos);

            // Must clone the segmentInfos while we still
            // hold fullFlushLock and while sync'd so that
            // no partial changes (eg a delete w/o
            // corresponding add from an updateDocument) can
            // sneak into the commit point:
            toCommit = (SegmentInfos) segmentInfos.clone();

            pendingCommitChangeCount = changeCount;

            // This protects the segmentInfos we are now going
            // to commit.  This is important in case, eg, while
            // we are trying to sync all referenced files, a
            // merge completes which would otherwise have
            // removed the files we are now syncing.
            deleter.incRef(toCommit, false);
          }
          success = true;
        } finally {
          if (!success && infoStream != null) {
            message("hit exception during prepareCommit");
          }
          // Done: finish the full flush!
          docWriter.finishFullFlush(flushSuccess);
          doAfterFlush();
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "prepareCommit");
    }
 
    boolean success = false;
    try {
      if (anySegmentsFlushed) {
        maybeMerge();
      }
      success = true;
    } finally {
      if (!success) {
        deleter.decRef(toCommit);
      }
    }

    startCommit(toCommit, commitUserData);
  }

  // Used only by commit, below; lock order is commitLock -> IW
  private final Object commitLock = new Object();

  /**
   * <p>Commits all pending changes (added & deleted
   * documents, optimizations, segment merges, added
   * indexes, etc.) to the index, and syncs all referenced
   * index files, such that a reader will see the changes
   * and the index updates will survive an OS or machine
   * crash or power loss.  Note that this does not wait for
   * any running background merges to finish.  This may be a
   * costly operation, so you should test the cost in your
   * application and do it only when really necessary.</p>
   *
   * <p> Note that this operation calls Directory.sync on
   * the index files.  That call should not return until the
   * file contents & metadata are on stable storage.  For
   * FSDirectory, this calls the OS's fsync.  But, beware:
   * some hardware devices may in fact cache writes even
   * during fsync, and return before the bits are actually
   * on stable storage, to give the appearance of faster
   * performance.  If you have such a device, and it does
   * not have a battery backup (for example) then on power
   * loss it may still lose data.  Lucene cannot guarantee
   * consistency on such devices.  </p>
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   *
   * @see #prepareCommit
   * @see #commit(Map)
   */
  public final void commit() throws CorruptIndexException, IOException {
    commit(null);
  }

  /** Commits all changes to the index, specifying a
   *  commitUserData Map (String -> String).  This just
   *  calls {@link #prepareCommit(Map)} (if you didn't
   *  already call it) and then {@link #finishCommit}.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   */
  public final void commit(Map<String,String> commitUserData) throws CorruptIndexException, IOException {

    ensureOpen();

    commitInternal(commitUserData);
  }

  private final void commitInternal(Map<String,String> commitUserData) throws CorruptIndexException, IOException {

    if (infoStream != null) {
      message("commit: start");
    }

    synchronized(commitLock) {
      if (infoStream != null) {
        message("commit: enter lock");
      }

      if (pendingCommit == null) {
        if (infoStream != null) {
          message("commit: now prepare");
        }
        prepareCommit(commitUserData);
      } else if (infoStream != null) {
        message("commit: already prepared");
      }

      finishCommit();
    }
  }

  private synchronized final void finishCommit() throws CorruptIndexException, IOException {

    if (pendingCommit != null) {
      try {
        if (infoStream != null)
    	  message("commit: pendingCommit != null");
        pendingCommit.finishCommit(directory);
        if (infoStream != null)
          message("commit: wrote segments file \"" + pendingCommit.getCurrentSegmentFileName() + "\"");
        lastCommitChangeCount = pendingCommitChangeCount;
        segmentInfos.updateGeneration(pendingCommit);
        segmentInfos.setUserData(pendingCommit.getUserData());
        rollbackSegments = pendingCommit.createBackupSegmentInfos(true);
        deleter.checkpoint(pendingCommit, true);
      } finally {
        // Matches the incRef done in startCommit:
        deleter.decRef(pendingCommit);
        pendingCommit = null;
        notifyAll();
      }

    } else if (infoStream != null) {
      message("commit: pendingCommit == null; skip");
    }

    if (infoStream != null) {
      message("commit: done");
    }
  }

  // Ensures only one flush() is actually flushing segments
  // at a time:
  private final Object fullFlushLock = new Object();

  /**
   * Flush all in-memory buffered updates (adds and deletes)
   * to the Directory.
   * @param triggerMerge if true, we may merge segments (if
   *  deletes or docs were flushed) if necessary
   * @param applyAllDeletes whether pending deletes should also
   */
  protected final void flush(boolean triggerMerge, boolean applyAllDeletes) throws CorruptIndexException, IOException {

    // NOTE: this method cannot be sync'd because
    // maybeMerge() in turn calls mergeScheduler.merge which
    // in turn can take a long time to run and we don't want
    // to hold the lock for that.  In the case of
    // ConcurrentMergeScheduler this can lead to deadlock
    // when it stalls due to too many running merges.

    // We can be called during close, when closing==true, so we must pass false to ensureOpen:
    ensureOpen(false);
    if (doFlush(applyAllDeletes) && triggerMerge) {
      maybeMerge();
    }
  }

  private boolean doFlush(boolean applyAllDeletes) throws CorruptIndexException, IOException {
    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot flush");
    }

    doBeforeFlush();
    assert testPoint("startDoFlush");
    boolean success = false;
    try {

      if (infoStream != null) {
        message("  start flush: applyAllDeletes=" + applyAllDeletes);
        message("  index before flush " + segString());
      }
      final boolean anySegmentFlushed;
      
      synchronized (fullFlushLock) {
        try {
          anySegmentFlushed = docWriter.flushAllThreads();
          success = true;
        } finally {
          docWriter.finishFullFlush(success);
        }
      }
      success = false;
      synchronized(this) {
        maybeApplyDeletes(applyAllDeletes);
        doAfterFlush();
        if (!anySegmentFlushed) {
          // flushCount is incremented in flushAllThreads
          flushCount.incrementAndGet();
        }
        success = true;
        return anySegmentFlushed;
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "doFlush");
      // never hit
      return false;
    } finally {
      if (!success && infoStream != null)
        message("hit exception during flush");
    }
  }
  
  final synchronized void maybeApplyDeletes(boolean applyAllDeletes) throws IOException {
    if (applyAllDeletes) {
      if (infoStream != null) {
        message("apply all deletes during flush");
      }
      applyAllDeletes();
    } else if (infoStream != null) {
      message("don't apply deletes now delTermCount=" + bufferedDeletesStream.numTerms() + " bytesUsed=" + bufferedDeletesStream.bytesUsed());
    }
  }
  
  final synchronized void applyAllDeletes() throws IOException {
    flushDeletesCount.incrementAndGet();
    final BufferedDeletesStream.ApplyDeletesResult result;
    result = bufferedDeletesStream.applyDeletes(readerPool, segmentInfos.asList());
    if (result.anyDeletes) {
      checkpoint();
    }
    if (!keepFullyDeletedSegments && result.allDeleted != null) {
      if (infoStream != null) {
        message("drop 100% deleted segments: " + segString(result.allDeleted));
      }
      for (SegmentInfo info : result.allDeleted) {
        // If a merge has already registered for this
        // segment, we leave it in the readerPool; the
        // merge will skip merging it and will then drop
        // it once it's done:
        if (!mergingSegments.contains(info)) {
          segmentInfos.remove(info);
          if (readerPool != null) {
            readerPool.drop(info);
          }
        }
      }
      checkpoint();
    }
    bufferedDeletesStream.prune(segmentInfos);
  }

  /** Expert:  Return the total size of all index files currently cached in memory.
   * Useful for size management with flushRamDocs()
   */
  public final long ramSizeInBytes() {
    ensureOpen();
    return docWriter.flushControl.netBytes() + bufferedDeletesStream.bytesUsed();
  }
  
  // for testing only
  DocumentsWriter getDocsWriter() {
    boolean test = false;
    assert test = true;
    return test ? docWriter : null;
  }

  /** Expert:  Return the number of documents currently
   *  buffered in RAM. */
  public final synchronized int numRamDocs() {
    ensureOpen();
    return docWriter.getNumDocs();
  }

  private void ensureValidMerge(MergePolicy.OneMerge merge) throws IOException {
    for(SegmentInfo info : merge.segments) {
      if (!segmentInfos.contains(info)) {
        throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.name + ") that is not in the current index " + segString(), directory);
      }
    }
  }

  /** Carefully merges deletes for the segments we just
   *  merged.  This is tricky because, although merging will
   *  clear all deletes (compacts the documents), new
   *  deletes may have been flushed to the segments since
   *  the merge was started.  This method "carries over"
   *  such new deletes onto the newly merged segment, and
   *  saves the resulting deletes file (incrementing the
   *  delete generation for merge.info).  If no deletes were
   *  flushed, no new deletes file is saved. */
  synchronized private void commitMergedDeletes(MergePolicy.OneMerge merge, SegmentReader mergedReader) throws IOException {

    assert testPoint("startCommitMergeDeletes");

    final List<SegmentInfo> sourceSegments = merge.segments;

    if (infoStream != null)
      message("commitMergeDeletes " + merge.segString(directory));

    // Carefully merge deletes that occurred after we
    // started merging:
    int docUpto = 0;
    int delCount = 0;
    long minGen = Long.MAX_VALUE;

    for(int i=0; i < sourceSegments.size(); i++) {
      SegmentInfo info = sourceSegments.get(i);
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
      final int docCount = info.docCount;
      final BitVector prevLiveDocs = merge.readerLiveDocs.get(i);
      final BitVector currentLiveDocs;
      {
        final SegmentReader currentReader = readerPool.getIfExists(info, IOContext.Context.READ);
        if (currentReader != null) {
          currentLiveDocs = (BitVector) currentReader.getLiveDocs();
          readerPool.release(currentReader, false, IOContext.Context.READ);
        } else {
          assert readerPool.infoIsLive(info);
          if (info.hasDeletions()) {
            currentLiveDocs = new BitVector(directory,
                                            info.getDelFileName(),
                                            new IOContext(IOContext.Context.READ));
          } else {
            currentLiveDocs = null;
          }
        }
      }

      if (prevLiveDocs != null) {

        // There were deletes on this segment when the merge
        // started.  The merge has collapsed away those
        // deletes, but, if new deletes were flushed since
        // the merge started, we must now carefully keep any
        // newly flushed deletes but mapping them to the new
        // docIDs.

        if (currentLiveDocs.count() < prevLiveDocs.count()) {
          // This means this segment received new deletes
          // since we started the merge, so we
          // must merge them:
          for(int j=0;j<docCount;j++) {
            if (!prevLiveDocs.get(j)) {
              assert !currentLiveDocs.get(j);
            } else {
              if (!currentLiveDocs.get(j)) {
                mergedReader.doDelete(docUpto);
                delCount++;
              }
              docUpto++;
            }
          }
        } else {
          assert currentLiveDocs.count() == prevLiveDocs.count(): "currentLiveDocs.count()==" + currentLiveDocs.count() + " vs prevLiveDocs.count()=" + prevLiveDocs.count() + " info=" + info;
          docUpto += currentLiveDocs.count();
        }
      } else if (currentLiveDocs != null) {
        // This segment had no deletes before but now it
        // does:
        for(int j=0; j<docCount; j++) {
          if (!currentLiveDocs.get(j)) {
            mergedReader.doDelete(docUpto);
            delCount++;
          }
          docUpto++;
        }
      } else {
        // No deletes before or after
        docUpto += info.docCount;
      }
    }

    assert mergedReader.numDeletedDocs() == delCount;

    mergedReader.hasChanges = delCount > 0;

    // If new deletes were applied while we were merging
    // (which happens if eg commit() or getReader() is
    // called during our merge), then it better be the case
    // that the delGen has increased for all our merged
    // segments:
    assert !mergedReader.hasChanges || minGen > mergedReader.getSegmentInfo().getBufferedDeletesGen();

    mergedReader.getSegmentInfo().setBufferedDeletesGen(minGen);
  }

  synchronized private boolean commitMerge(MergePolicy.OneMerge merge, SegmentReader mergedReader) throws IOException {

    assert testPoint("startCommitMerge");

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot complete merge");
    }

    if (infoStream != null)
      message("commitMerge: " + merge.segString(directory) + " index=" + segString());

    assert merge.registerDone;

    // If merge was explicitly aborted, or, if rollback() or
    // rollbackTransaction() had been called since our merge
    // started (which results in an unqualified
    // deleter.refresh() call that will remove any index
    // file that current segments does not reference), we
    // abort this merge
    if (merge.isAborted()) {
      if (infoStream != null)
        message("commitMerge: skipping merge " + merge.segString(directory) + ": it was aborted");
      return false;
    }

    commitMergedDeletes(merge, mergedReader);

    // If the doc store we are using has been closed and
    // is in now compound format (but wasn't when we
    // started), then we will switch to the compound
    // format as well:

    assert !segmentInfos.contains(merge.info);

    final boolean allDeleted = mergedReader.numDocs() == 0;

    if (infoStream != null && allDeleted) {
      message("merged segment " + merge.info + " is 100% deleted" +  (keepFullyDeletedSegments ? "" : "; skipping insert"));
    }

    final boolean dropSegment = allDeleted && !keepFullyDeletedSegments;
    segmentInfos.applyMergeChanges(merge, dropSegment);
    
    if (dropSegment) {
      readerPool.drop(merge.info);
    }
    
    if (infoStream != null) {
      message("after commit: " + segString());
    }

    closeMergeReaders(merge, false);

    // Must note the change to segmentInfos so any commits
    // in-flight don't lose it:
    checkpoint();

    // If the merged segments had pending changes, clear
    // them so that they don't bother writing them to
    // disk, updating SegmentInfo, etc.:
    readerPool.clear(merge.segments);

    if (merge.optimize) {
      // cascade the optimize:
      if (!segmentsToOptimize.containsKey(merge.info)) {
        segmentsToOptimize.put(merge.info, Boolean.FALSE);
      }
    }

    return true;
  }

  final private void handleMergeException(Throwable t, MergePolicy.OneMerge merge) throws IOException {

    if (infoStream != null) {
      message("handleMergeException: merge=" + merge.segString(directory) + " exc=" + t);
    }

    // Set the exception on the merge, so if
    // optimize() is waiting on us it sees the root
    // cause exception:
    merge.setException(t);
    addMergeException(merge);

    if (t instanceof MergePolicy.MergeAbortedException) {
      // We can ignore this exception (it happens when
      // close(false) or rollback is called), unless the
      // merge involves segments from external directories,
      // in which case we must throw it so, for example, the
      // rollbackTransaction code in addIndexes* is
      // executed.
      if (merge.isExternal)
        throw (MergePolicy.MergeAbortedException) t;
    } else if (t instanceof IOException)
      throw (IOException) t;
    else if (t instanceof RuntimeException)
      throw (RuntimeException) t;
    else if (t instanceof Error)
      throw (Error) t;
    else
      // Should not get here
      throw new RuntimeException(t);
  }

  /**
   * Merges the indicated segments, replacing them in the stack with a
   * single segment.
   * 
   * @lucene.experimental
   */
  public void merge(MergePolicy.OneMerge merge)
    throws CorruptIndexException, IOException {

    boolean success = false;

    final long t0 = System.currentTimeMillis();
    //System.out.println(Thread.currentThread().getName() + ": merge start: size=" + (merge.estimatedMergeBytes/1024./1024.) + " MB\n  merge=" + merge.segString(directory) + "\n  idx=" + segString());

    try {
      try {
        try {
          mergeInit(merge);

          if (infoStream != null)
            message("now merge\n  merge=" + merge.segString(directory) + "\n  index=" + segString());

          mergeMiddle(merge);
          mergeSuccess(merge);
          success = true;
        } catch (Throwable t) {
          handleMergeException(t, merge);
        }
      } finally {
        synchronized(this) {
          mergeFinish(merge);

          if (!success) {
            if (infoStream != null)
              message("hit exception during merge");
            if (merge.info != null && !segmentInfos.contains(merge.info))
              deleter.refresh(merge.info.name);
          }

          // This merge (and, generally, any change to the
          // segments) may now enable new merges, so we call
          // merge policy & update pending merges.
          if (success && !merge.isAborted() && (merge.optimize || (!closed && !closing))) {
            updatePendingMerges(merge.maxNumSegmentsOptimize, merge.optimize);
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "merge");
    }
    if (infoStream != null && merge.info != null) {
      message("merge time " + (System.currentTimeMillis()-t0) + " msec for " + merge.info.docCount + " docs");
    }
    //System.out.println(Thread.currentThread().getName() + ": merge end");
  }

  /** Hook that's called when the specified merge is complete. */
  void mergeSuccess(MergePolicy.OneMerge merge) {
  }

  /** Checks whether this merge involves any segments
   *  already participating in a merge.  If not, this merge
   *  is "registered", meaning we record that its segments
   *  are now participating in a merge, and true is
   *  returned.  Else (the merge conflicts) false is
   *  returned. */
  final synchronized boolean registerMerge(MergePolicy.OneMerge merge) throws MergePolicy.MergeAbortedException, IOException {

    if (merge.registerDone)
      return true;

    if (stopMerges) {
      merge.abort();
      throw new MergePolicy.MergeAbortedException("merge is aborted: " + merge.segString(directory));
    }

    boolean isExternal = false;
    for(SegmentInfo info : merge.segments) {
      if (mergingSegments.contains(info)) {
        return false;
      }
      if (!segmentInfos.contains(info)) {
        return false;
      }
      if (info.dir != directory) {
        isExternal = true;
      }
      if (segmentsToOptimize.containsKey(info)) {
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = optimizeMaxNumSegments;
      }
    }

    ensureValidMerge(merge);

    pendingMerges.add(merge);

    if (infoStream != null)
      message("add merge to pendingMerges: " + merge.segString(directory) + " [total " + pendingMerges.size() + " pending]");

    merge.mergeGen = mergeGen;
    merge.isExternal = isExternal;

    // OK it does not conflict; now record that this merge
    // is running (while synchronized) to avoid race
    // condition where two conflicting merges from different
    // threads, start
    if (infoStream != null) {
      StringBuilder builder = new StringBuilder("registerMerge merging= [");
      for (SegmentInfo info : mergingSegments) {
        builder.append(info.name).append(", ");  
      }
      builder.append("]");
      // don't call mergingSegments.toString() could lead to ConcurrentModException
      // since merge updates the segments FieldInfos
      message(builder.toString());  
    }
    for(SegmentInfo info : merge.segments) {
      message("registerMerge info=" + info);
      mergingSegments.add(info);
    }

    // Merge is now registered
    merge.registerDone = true;

    return true;
  }

  /** Does initial setup for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance.  */
  final synchronized void mergeInit(MergePolicy.OneMerge merge) throws IOException {
    boolean success = false;
    try {
      _mergeInit(merge);
      success = true;
    } finally {
      if (!success) {
        if (infoStream != null) {
          message("hit exception in mergeInit");
        }
        mergeFinish(merge);
      }
    }
  }

  synchronized private void _mergeInit(MergePolicy.OneMerge merge) throws IOException {

    assert testPoint("startMergeInit");

    assert merge.registerDone;
    assert !merge.optimize || merge.maxNumSegmentsOptimize > 0;

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot merge");
    }

    // TODO: is there any perf benefit to sorting
    // merged segments?  eg biggest to smallest?

    if (merge.info != null)
      // mergeInit already done
      return;

    if (merge.isAborted())
      return;

    // Bind a new segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.
    merge.info = new SegmentInfo(newSegmentName(), 0, directory, false, null, globalFieldNumberMap.newFieldInfos(SegmentCodecsBuilder.create(codecs)));

    // Lock order: IW -> BD
    final BufferedDeletesStream.ApplyDeletesResult result = bufferedDeletesStream.applyDeletes(readerPool, merge.segments);

    if (result.anyDeletes) {
      checkpoint();
    }

    if (!keepFullyDeletedSegments && result.allDeleted != null) {
      if (infoStream != null) {
        message("drop 100% deleted segments: " + result.allDeleted);
      }
      for(SegmentInfo info : result.allDeleted) {
        segmentInfos.remove(info);
        if (merge.segments.contains(info)) {
          mergingSegments.remove(info);
          merge.segments.remove(info);
        }
      }
      if (readerPool != null) {
        readerPool.drop(result.allDeleted);
      }
      checkpoint();
    }

    merge.info.setBufferedDeletesGen(result.gen);

    // Lock order: IW -> BD
    bufferedDeletesStream.prune(segmentInfos);
    Map<String,String> details = new HashMap<String,String>();
    details.put("optimize", Boolean.toString(merge.optimize));
    details.put("mergeFactor", Integer.toString(merge.segments.size()));
    setDiagnostics(merge.info, "merge", details);

    if (infoStream != null) {
      message("merge seg=" + merge.info.name);
    }

    assert merge.estimatedMergeBytes == 0;
    for(SegmentInfo info : merge.segments) {
      if (info.docCount > 0) {
        final int delCount = numDeletedDocs(info);
        assert delCount <= info.docCount;
        final double delRatio = ((double) delCount)/info.docCount;
        merge.estimatedMergeBytes += info.sizeInBytes(true) * (1.0 - delRatio);
      }
    }

    // TODO: I think this should no longer be needed (we
    // now build CFS before adding segment to the infos);
    // however, on removing it, tests fail for some reason!

    // Also enroll the merged segment into mergingSegments;
    // this prevents it from getting selected for a merge
    // after our merge is done but while we are building the
    // CFS:
    mergingSegments.add(merge.info);
  }

  static void setDiagnostics(SegmentInfo info, String source) {
    setDiagnostics(info, source, null);
  }

  private static void setDiagnostics(SegmentInfo info, String source, Map<String,String> details) {
    Map<String,String> diagnostics = new HashMap<String,String>();
    diagnostics.put("source", source);
    diagnostics.put("lucene.version", Constants.LUCENE_VERSION);
    diagnostics.put("os", Constants.OS_NAME);
    diagnostics.put("os.arch", Constants.OS_ARCH);
    diagnostics.put("os.version", Constants.OS_VERSION);
    diagnostics.put("java.version", Constants.JAVA_VERSION);
    diagnostics.put("java.vendor", Constants.JAVA_VENDOR);
    if (details != null) {
      diagnostics.putAll(details);
    }
    info.setDiagnostics(diagnostics);
  }

  /** Does fininishing for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance. */
  final synchronized void mergeFinish(MergePolicy.OneMerge merge) throws IOException {

    // Optimize, addIndexes or finishMerges may be waiting
    // on merges to finish.
    notifyAll();

    // It's possible we are called twice, eg if there was an
    // exception inside mergeInit
    if (merge.registerDone) {
      final List<SegmentInfo> sourceSegments = merge.segments;
      for(SegmentInfo info : sourceSegments) {
        mergingSegments.remove(info);
      }
      // TODO: if we remove the add in _mergeInit, we should
      // also remove this:
      mergingSegments.remove(merge.info);
      merge.registerDone = false;
    }

    runningMerges.remove(merge);
  }

  private final synchronized void closeMergeReaders(MergePolicy.OneMerge merge, boolean suppressExceptions) throws IOException {
    final int numSegments = merge.readers.size();
    Throwable th = null;

    boolean anyChanges = false;
    boolean drop = !suppressExceptions;
    
    for (int i = 0; i < numSegments; i++) {
      if (merge.readers.get(i) != null) {
        try {
          anyChanges |= readerPool.release(merge.readers.get(i), drop, IOContext.Context.MERGE);
        } catch (Throwable t) {
          if (th == null) {
            th = t;
          }
        }
        merge.readers.set(i, null);
      }
    }
    
    if (suppressExceptions && anyChanges) {
      checkpoint();
    }
    
    // If any error occured, throw it.
    if (!suppressExceptions && th != null) {
      if (th instanceof IOException) throw (IOException) th;
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      throw new RuntimeException(th);
    }
  }

  private synchronized BitVector getLiveDocsClone(SegmentInfo info, SegmentReader other) throws IOException {
    final SegmentReader delReader = readerPool.getIfExists(info, IOContext.Context.READ);
    BitVector liveDocs;
    if (delReader != null) {
      liveDocs = (BitVector) delReader.getLiveDocs();
      readerPool.release(delReader, false, IOContext.Context.READ);
      if (liveDocs != null) {
        // We clone the del docs because other
        // deletes may come in while we're merging.  We
        // need frozen deletes while merging, and then
        // we carry over any new deletions in
        // commitMergedDeletes.
        liveDocs = (BitVector) liveDocs.clone();
      }
    } else {
      liveDocs = (BitVector) other.getLiveDocs();
    }

    return liveDocs;
  }

  /** Does the actual (time-consuming) work of the merge,
   *  but without holding synchronized lock on IndexWriter
   *  instance */
  private int mergeMiddle(MergePolicy.OneMerge merge)
    throws CorruptIndexException, IOException {

    merge.checkAborted(directory);

    final String mergedName = merge.info.name;

    int mergedDocCount = 0;

    List<SegmentInfo> sourceSegments = merge.segments;
    
    IOContext context = new IOContext(merge.getMergeInfo());

    SegmentMerger merger = new SegmentMerger(directory, config.getTermIndexInterval(), mergedName, merge,
                                             payloadProcessorProvider, merge.info.getFieldInfos(), context);

    if (infoStream != null) {
      message("merging " + merge.segString(directory) + " mergeVectors=" + merge.info.getFieldInfos().hasVectors());
    }

    merge.readers = new ArrayList<SegmentReader>();
    merge.readerLiveDocs = new ArrayList<BitVector>();

    // This is try/finally to make sure merger's readers are
    // closed:
    boolean success = false;
    try {
      int totDocCount = 0;
      int segUpto = 0;
      while(segUpto < sourceSegments.size()) {

        final SegmentInfo info = sourceSegments.get(segUpto);

        // Hold onto the "live" reader; we will use this to
        // commit merged deletes
        final SegmentReader reader = readerPool.get(info, context);

        // Carefully pull the most recent live docs:
        final BitVector liveDocs = getLiveDocsClone(info, reader);

        merge.readerLiveDocs.add(liveDocs);
        merge.readers.add(reader);

        if (liveDocs == null || liveDocs.count() > 0) {
          merger.add(reader, liveDocs);
          totDocCount += liveDocs == null ? reader.maxDoc() : liveDocs.count();
        }
        segUpto++;
      }

      if (infoStream != null) {
        message("merge: total " + totDocCount + " docs");
      }

      merge.checkAborted(directory);

      // This is where all the work happens:
      mergedDocCount = merge.info.docCount = merger.merge();

      // Record which codec was used to write the segment
      merge.info.setSegmentCodecs(merger.getSegmentCodecs());

      if (infoStream != null) {
        message("merge segmentCodecs=" + merger.getSegmentCodecs());
        message("merge store matchedCount=" + merger.getMatchedSubReaderCount() + " vs " + merge.readers.size());
      }
      anyNonBulkMerges |= merger.getAnyNonBulkMerges();

      assert mergedDocCount == totDocCount: "mergedDocCount=" + mergedDocCount + " vs " + totDocCount;

      // Very important to do this before opening the reader
      // because codec must know if prox was written for
      // this segment:
      //System.out.println("merger set hasProx=" + merger.hasProx() + " seg=" + merge.info.name);
      boolean useCompoundFile;
      synchronized (this) { // Guard segmentInfos
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, merge.info);
      }
      
      if (useCompoundFile) {
        success = false;
        final String compoundFileName = IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_EXTENSION);

        try {
          if (infoStream != null) {
            message("create compound file " + compoundFileName);
          }
          merger.createCompoundFile(compoundFileName, merge.info, new IOContext(merge.getMergeInfo()));
          success = true;
        } catch (IOException ioe) {
          synchronized(this) {
            if (merge.isAborted()) {
              // This can happen if rollback or close(false)
              // is called -- fall through to logic below to
              // remove the partially created CFS:
            } else {
              handleMergeException(ioe, merge);
            }
          }
        } catch (Throwable t) {
          handleMergeException(t, merge);
        } finally {
          if (!success) {
            if (infoStream != null) {
              message("hit exception creating compound file during merge");
            }

            synchronized(this) {
              deleter.deleteFile(compoundFileName);
              
              deleter.deleteFile(IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
              deleter.deleteNewFiles(merge.info.files());
            }
          }
        }

        success = false;

        synchronized(this) {

          // delete new non cfs files directly: they were never
          // registered with IFD
          deleter.deleteNewFiles(merge.info.files());

          if (merge.isAborted()) {
            if (infoStream != null) {
              message("abort merge after building CFS");
            }
            deleter.deleteFile(compoundFileName);
            return 0;
          }
        }

        merge.info.setUseCompoundFile(true);
      }

      if (infoStream != null) {
        message(String.format("merged segment size=%.3f MB vs estimate=%.3f MB", merge.info.sizeInBytes(true)/1024./1024., merge.estimatedMergeBytes/1024/1024.));
      }

      final IndexReaderWarmer mergedSegmentWarmer = config.getMergedSegmentWarmer();

      // TODO: in the non-realtime case, we may want to only
      // keep deletes (it's costly to open entire reader
      // when we just need deletes)

      final boolean loadDocStores;
      if (mergedSegmentWarmer != null) {
        // Load terms index & doc stores so the segment
        // warmer can run searches, load documents/term
        // vectors
        loadDocStores = true;
      } else {
        loadDocStores = false;
      }

      // Force READ context because we merge deletes onto
      // this reader:
      final SegmentReader mergedReader = readerPool.get(merge.info, loadDocStores, new IOContext(IOContext.Context.READ));
      try {
        if (poolReaders && mergedSegmentWarmer != null) {
          mergedSegmentWarmer.warm(mergedReader);
        }

        if (!commitMerge(merge, mergedReader)) {
          // commitMerge will return false if this merge was aborted
          return 0;
        }
      } finally {
        synchronized(this) {
          if (readerPool.release(mergedReader, IOContext.Context.READ)) {
            // Must checkpoint after releasing the
            // mergedReader since it may have written a new
            // deletes file:
            checkpoint();
          }
        }
      }

      success = true;

    } finally {
      // Readers are already closed in commitMerge if we didn't hit
      // an exc:
      if (!success) {
        closeMergeReaders(merge, true);
      }
    }

    return mergedDocCount;
  }

  synchronized void addMergeException(MergePolicy.OneMerge merge) {
    assert merge.getException() != null;
    if (!mergeExceptions.contains(merge) && mergeGen == merge.mergeGen)
      mergeExceptions.add(merge);
  }

  // For test purposes.
  final int getBufferedDeleteTermsSize() {
    return docWriter.getBufferedDeleteTermsSize();
  }

  // For test purposes.
  final int getNumBufferedDeleteTerms() {
    return docWriter.getNumBufferedDeleteTerms();
  }

  // utility routines for tests
  synchronized SegmentInfo newestSegment() {
    return segmentInfos.size() > 0 ? segmentInfos.info(segmentInfos.size()-1) : null;
  }

  /** @lucene.internal */
  public synchronized String segString() throws IOException {
    return segString(segmentInfos);
  }

  /** @lucene.internal */
  public synchronized String segString(Iterable<SegmentInfo> infos) throws IOException {
    final StringBuilder buffer = new StringBuilder();
    for(final SegmentInfo s : infos) {
      if (buffer.length() > 0) {
        buffer.append(' ');
      }
      buffer.append(segString(s));
    }
    return buffer.toString();
  }

  /** @lucene.internal */
  public synchronized String segString(SegmentInfo info) throws IOException {
    StringBuilder buffer = new StringBuilder();
    SegmentReader reader = readerPool.getIfExists(info);
    try {
      if (reader != null) {
        buffer.append(reader.toString());
      } else {
        buffer.append(info.toString(directory, 0));
        if (info.dir != directory) {
          buffer.append("**");
        }
      }
    } finally {
      if (reader != null) {
        readerPool.release(reader, false);
      }
    }
    return buffer.toString();
  }

  private synchronized void doWait() {
    // NOTE: the callers of this method should in theory
    // be able to do simply wait(), but, as a defense
    // against thread timing hazards where notifyAll()
    // fails to be called, we wait for at most 1 second
    // and then return so caller can check if wait
    // conditions are satisfied:
    try {
      wait(1000);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }

  private boolean keepFullyDeletedSegments;

  /** Only for testing.
   *
   * @lucene.internal */
  void keepFullyDeletedSegments() {
    keepFullyDeletedSegments = true;
  }

  boolean getKeepFullyDeletedSegments() {
    return keepFullyDeletedSegments;
  }

  // called only from assert
  private boolean filesExist(SegmentInfos toSync) throws IOException {
    
    Collection<String> files = toSync.files(directory, false);
    for(final String fileName: files) {
      assert directory.fileExists(fileName): "file " + fileName + " does not exist";
      // If this trips it means we are missing a call to
      // .checkpoint somewhere, because by the time we
      // are called, deleter should know about every
      // file referenced by the current head
      // segmentInfos:
      assert deleter.exists(fileName): "IndexFileDeleter doesn't know about file " + fileName;
    }
    return true;
  }

  /** Walk through all files referenced by the current
   *  segmentInfos and ask the Directory to sync each file,
   *  if it wasn't already.  If that succeeds, then we
   *  prepare a new segments_N file but do not fully commit
   *  it. */
  private void startCommit(final SegmentInfos toSync, final Map<String,String> commitUserData) throws IOException {

    assert testPoint("startStartCommit");
    assert pendingCommit == null;

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot commit");
    }

    try {

      if (infoStream != null) {
        message("startCommit(): start");
      }

      synchronized(this) {

        assert lastCommitChangeCount <= changeCount;

        if (pendingCommitChangeCount == lastCommitChangeCount) {
          if (infoStream != null) {
            message("  skip startCommit(): no changes pending");
          }
          deleter.decRef(toSync);
          return;
        }

        if (infoStream != null) {
          message("startCommit index=" + segString(toSync) + " changeCount=" + changeCount);
        }

        assert filesExist(toSync);

        if (commitUserData != null) {
          toSync.setUserData(commitUserData);
        }
      }

      assert testPoint("midStartCommit");

      boolean pendingCommitSet = false;

      try {
        // This call can take a long time -- 10s of seconds
        // or more.  We do it without sync:
        directory.sync(toSync.files(directory, false));

        assert testPoint("midStartCommit2");

        synchronized(this) {

          assert pendingCommit == null;

          assert segmentInfos.getGeneration() == toSync.getGeneration();

          // Exception here means nothing is prepared
          // (this method unwinds everything it did on
          // an exception)
          toSync.prepareCommit(directory);

          pendingCommitSet = true;
          pendingCommit = toSync;
        }

        if (infoStream != null) {
          message("done all syncs");
        }

        assert testPoint("midStartCommitSuccess");

      } finally {
        synchronized(this) {
          // Have our master segmentInfos record the
          // generations we just prepared.  We do this
          // on error or success so we don't
          // double-write a segments_N file.
          segmentInfos.updateGeneration(toSync);

          if (!pendingCommitSet) {
            if (infoStream != null) {
              message("hit exception committing segments file");
            }

            // Hit exception
            deleter.decRef(toSync);
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "startCommit");
    }
    assert testPoint("finishStartCommit");
  }

  /**
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   * @param directory the directory to check for a lock
   * @throws IOException if there is a low-level IO error
   */
  public static boolean isLocked(Directory directory) throws IOException {
    return directory.makeLock(WRITE_LOCK_NAME).isLocked();
  }

  /**
   * Forcibly unlocks the index in the named directory.
   * <P>
   * Caution: this should only be used by failure recovery code,
   * when it is known that no other process nor thread is in fact
   * currently accessing this index.
   */
  public static void unlock(Directory directory) throws IOException {
    directory.makeLock(IndexWriter.WRITE_LOCK_NAME).release();
  }

  /** If {@link #getReader} has been called (ie, this writer
   *  is in near real-time mode), then after a merge
   *  completes, this class can be invoked to warm the
   *  reader on the newly merged segment, before the merge
   *  commits.  This is not required for near real-time
   *  search, but will reduce search latency on opening a
   *  new near real-time reader after a merge completes.
   *
   * @lucene.experimental
   *
   * <p><b>NOTE</b>: warm is called before any deletes have
   * been carried over to the merged segment. */
  public static abstract class IndexReaderWarmer {
    public abstract void warm(IndexReader reader) throws IOException;
  }

  private void handleOOM(OutOfMemoryError oom, String location) {
    if (infoStream != null) {
      message("hit OutOfMemoryError inside " + location);
    }
    hitOOM = true;
    throw oom;
  }

  // Used only by assert for testing.  Current points:
  //   startDoFlush
  //   startCommitMerge
  //   startStartCommit
  //   midStartCommit
  //   midStartCommit2
  //   midStartCommitSuccess
  //   finishStartCommit
  //   startCommitMergeDeletes
  //   startMergeInit
  //   DocumentsWriter.ThreadState.init start
  boolean testPoint(String name) {
    return true;
  }

  synchronized boolean nrtIsCurrent(SegmentInfos infos) {
    //System.out.println("IW.nrtIsCurrent " + (infos.version == segmentInfos.version && !docWriter.anyChanges() && !bufferedDeletesStream.any()));
    return infos.version == segmentInfos.version && !docWriter.anyChanges() && !bufferedDeletesStream.any();
  }

  synchronized boolean isClosed() {
    return closed;
  }

  /** Expert: remove any index files that are no longer
   *  used.
   *
   *  <p> IndexWriter normally deletes unused files itself,
   *  during indexing.  However, on Windows, which disallows
   *  deletion of open files, if there is a reader open on
   *  the index then those files cannot be deleted.  This is
   *  fine, because IndexWriter will periodically retry
   *  the deletion.</p>
   *
   *  <p> However, IndexWriter doesn't try that often: only
   *  on open, close, flushing a new segment, and finishing
   *  a merge.  If you don't do any of these actions with your
   *  IndexWriter, you'll see the unused files linger.  If
   *  that's a problem, call this method to delete them
   *  (once you've closed the open readers that were
   *  preventing their deletion). 
   *  
   *  <p> In addition, you can call this method to delete 
   *  unreferenced index commits. This might be useful if you 
   *  are using an {@link IndexDeletionPolicy} which holds
   *  onto index commits until some criteria are met, but those
   *  commits are no longer needed. Otherwise, those commits will
   *  be deleted the next time commit() is called.
   */
  public synchronized void deleteUnusedFiles() throws IOException {
    deleter.deletePendingFiles();
    deleter.revisitPolicy();
  }

  /**
   * Sets the {@link PayloadProcessorProvider} to use when merging payloads.
   * Note that the given <code>pcp</code> will be invoked for every segment that
   * is merged, not only external ones that are given through
   * {@link #addIndexes}. If you want only the payloads of the external segments
   * to be processed, you can return <code>null</code> whenever a
   * {@link DirPayloadProcessor} is requested for the {@link Directory} of the
   * {@link IndexWriter}.
   * <p>
   * The default is <code>null</code> which means payloads are processed
   * normally (copied) during segment merges. You can also unset it by passing
   * <code>null</code>.
   * <p>
   * <b>NOTE:</b> the set {@link PayloadProcessorProvider} will be in effect
   * immediately, potentially for already running merges too. If you want to be
   * sure it is used for further operations only, such as {@link #addIndexes} or
   * {@link #optimize}, you can call {@link #waitForMerges()} before.
   */
  public void setPayloadProcessorProvider(PayloadProcessorProvider pcp) {
    payloadProcessorProvider = pcp;
  }

  /**
   * Returns the {@link PayloadProcessorProvider} that is used during segment
   * merges to process payloads.
   */
  public PayloadProcessorProvider getPayloadProcessorProvider() {
    return payloadProcessorProvider;
  }
}
