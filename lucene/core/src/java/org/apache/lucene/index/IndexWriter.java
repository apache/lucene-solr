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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.codecs.lucene3x.Lucene3xSegmentInfoFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfos.FieldNumbers;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MergeState.CheckAbort;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.Version;

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
  addDocument} and removed with {@link #deleteDocuments(Term...)} or {@link
  #deleteDocuments(Query...)}. A document can be updated with {@link
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
  MergePolicy.MergeSpecification} describing the merges.
  The default is {@link LogByteSizeMergePolicy}.  Then, the {@link
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
public class IndexWriter implements Closeable, TwoPhaseCommit, Accountable {

  /** Hard limit on maximum number of documents that may be added to the
   *  index.  If you try to add more than this you'll hit {@code IllegalStateException}. */
  // We defensively subtract 128 to be well below the lowest
  // ArrayUtil.MAX_ARRAY_LENGTH on "typical" JVMs.  We don't just use
  // ArrayUtil.MAX_ARRAY_LENGTH here because this can vary across JVMs:
  public static final int MAX_DOCS = Integer.MAX_VALUE - 128;

  // Use package-private instance var to enforce the limit so testing
  // can use less electricity:
  private static int actualMaxDocs = MAX_DOCS;

  /** Used only for testing. */
  static void setMaxDocs(int maxDocs) {
    if (maxDocs > MAX_DOCS) {
      // Cannot go higher than the hard max:
      throw new IllegalArgumentException("maxDocs must be <= IndexWriter.MAX_DOCS=" + MAX_DOCS + "; got: " + maxDocs);
    }
    IndexWriter.actualMaxDocs = maxDocs;
  }

  static int getActualMaxDocs() {
    return IndexWriter.actualMaxDocs;
  }

  private static final int UNBOUNDED_MAX_MERGE_SEGMENTS = -1;
  
  /**
   * Name of the write lock in the index.
   */
  public static final String WRITE_LOCK_NAME = "write.lock";

  /** Key for the source of a segment in the {@link SegmentInfo#getDiagnostics() diagnostics}. */
  public static final String SOURCE = "source";
  /** Source of a segment which results from a merge of other segments. */
  public static final String SOURCE_MERGE = "merge";
  /** Source of a segment which results from a flush. */
  public static final String SOURCE_FLUSH = "flush";
  /** Source of a segment which results from a call to {@link #addIndexes(IndexReader...)}. */
  public static final String SOURCE_ADDINDEXES_READERS = "addIndexes(IndexReader...)";

  /**
   * Absolute hard maximum length for a term, in bytes once
   * encoded as UTF8.  If a term arrives from the analyzer
   * longer than this length, an
   * <code>IllegalArgumentException</code>  is thrown
   * and a message is printed to infoStream, if set (see {@link
   * IndexWriterConfig#setInfoStream(InfoStream)}).
   */
  public final static int MAX_TERM_LENGTH = DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8;
  volatile private boolean hitOOM;

  private final Directory directory;  // where this index resides
  private final Analyzer analyzer;    // how to analyze text

  private volatile long changeCount; // increments every time a change is completed
  private volatile long lastCommitChangeCount; // last changeCount that was committed

  private List<SegmentCommitInfo> rollbackSegments;      // list of segmentInfo we will fallback to if the commit fails

  volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
  volatile long pendingCommitChangeCount;

  private Collection<String> filesToCommit;

  final SegmentInfos segmentInfos;       // the segments
  final FieldNumbers globalFieldNumberMap;

  private final DocumentsWriter docWriter;
  private final Queue<Event> eventQueue;
  final IndexFileDeleter deleter;

  // used by forceMerge to note those needing merging
  private Map<SegmentCommitInfo,Boolean> segmentsToMerge = new HashMap<>();
  private int mergeMaxNumSegments;

  private Lock writeLock;

  private volatile boolean closed;
  private volatile boolean closing;

  // Holds all SegmentInfo instances currently involved in
  // merges
  private HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();

  private final MergeScheduler mergeScheduler;
  private LinkedList<MergePolicy.OneMerge> pendingMerges = new LinkedList<>();
  private Set<MergePolicy.OneMerge> runningMerges = new HashSet<>();
  private List<MergePolicy.OneMerge> mergeExceptions = new ArrayList<>();
  private long mergeGen;
  private boolean stopMerges;
  private boolean didMessageState;

  final AtomicInteger flushCount = new AtomicInteger();
  final AtomicInteger flushDeletesCount = new AtomicInteger();

  final ReaderPool readerPool = new ReaderPool();
  final BufferedUpdatesStream bufferedUpdatesStream;

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
  private final LiveIndexWriterConfig config;

  /** System.nanoTime() when commit started; used to write
   *  an infoStream message about how long commit took. */
  private long startCommitTime;

  /** How many documents are in the index, or are in the process of being
   *  added (reserved).  E.g., operations like addIndexes will first reserve
   *  the right to add N docs, before they actually change the index,
   *  much like how hotels place an "authorization hold" on your credit
   *  card to make sure they can later charge you when you check out. */
  final AtomicLong pendingNumDocs = new AtomicLong();

  DirectoryReader getReader() throws IOException {
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
   * {#flush} and then opening a new reader.  But the turnaround time of this
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
   * DirectoryReader#openIfChanged}, but that call will simply forward
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
   * call {@link IndexWriterConfig#setMergedSegmentWarmer} to
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
   * @throws IOException If there is a low-level I/O error
   */
  DirectoryReader getReader(boolean applyAllDeletes) throws IOException {
    ensureOpen();

    final long tStart = System.currentTimeMillis();

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "flush at getReader");
    }
    // Do this up front before flushing so that the readers
    // obtained during this flush are pooled, the first time
    // this method is called:
    poolReaders = true;
    DirectoryReader r = null;
    doBeforeFlush();
    boolean anySegmentFlushed = false;
    /*
     * for releasing a NRT reader we must ensure that 
     * DW doesn't add any segments or deletes until we are
     * done with creating the NRT DirectoryReader. 
     * We release the two stage full flush after we are done opening the
     * directory reader!
     */
    boolean success2 = false;
    try {
      synchronized (fullFlushLock) {
        boolean success = false;
        try {
          anySegmentFlushed = docWriter.flushAllThreads(this);
          if (!anySegmentFlushed) {
            // prevent double increment since docWriter#doFlush increments the flushcount
            // if we flushed anything.
            flushCount.incrementAndGet();
          }
          success = true;
          // Prevent segmentInfos from changing while opening the
          // reader; in theory we could instead do similar retry logic,
          // just like we do when loading segments_N
          synchronized(this) {
            maybeApplyDeletes(applyAllDeletes);
            r = StandardDirectoryReader.open(this, segmentInfos, applyAllDeletes);
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "return reader version=" + r.getVersion() + " reader=" + r);
            }
          }
        } catch (OutOfMemoryError oom) {
          handleOOM(oom, "getReader");
          // never reached but javac disagrees:
          return null;
        } finally {
          if (!success) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception during NRT reader");
            }
          }
          // Done: finish the full flush!
          docWriter.finishFullFlush(success);
          processEvents(false, true);
          doAfterFlush();
        }
      }
      if (anySegmentFlushed) {
        maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
      }
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "getReader took " + (System.currentTimeMillis() - tStart) + " msec");
      }
      success2 = true;
    } finally {
      if (!success2) {
        IOUtils.closeWhileHandlingException(r);
      }
    }
    return r;
  }

  @Override
  public final long ramBytesUsed() {
    ensureOpen();
    return docWriter.ramBytesUsed();
  }

  /** Holds shared SegmentReader instances. IndexWriter uses
   *  SegmentReaders for 1) applying deletes, 2) doing
   *  merges, 3) handing out a real-time reader.  This pool
   *  reuses instances of the SegmentReaders in all these
   *  places if it is in "near real-time mode" (getReader()
   *  has been called on this instance). */

  class ReaderPool implements Closeable {
    
    private final Map<SegmentCommitInfo,ReadersAndUpdates> readerMap = new HashMap<>();

    // used only by asserts
    public synchronized boolean infoIsLive(SegmentCommitInfo info) {
      int idx = segmentInfos.indexOf(info);
      assert idx != -1: "info=" + info + " isn't live";
      assert segmentInfos.info(idx) == info: "info=" + info + " doesn't match live info in segmentInfos";
      return true;
    }

    public synchronized void drop(SegmentCommitInfo info) throws IOException {
      final ReadersAndUpdates rld = readerMap.get(info);
      if (rld != null) {
        assert info == rld.info;
//        System.out.println("[" + Thread.currentThread().getName() + "] ReaderPool.drop: " + info);
        readerMap.remove(info);
        rld.dropReaders();
      }
    }

    public synchronized boolean anyPendingDeletes() {
      for(ReadersAndUpdates rld : readerMap.values()) {
        if (rld.getPendingDeleteCount() != 0) {
          return true;
        }
      }

      return false;
    }

    public synchronized void release(ReadersAndUpdates rld) throws IOException {
      release(rld, true);
    }

    public synchronized void release(ReadersAndUpdates rld, boolean assertInfoLive) throws IOException {

      // Matches incRef in get:
      rld.decRef();

      // Pool still holds a ref:
      assert rld.refCount() >= 1;

      if (!poolReaders && rld.refCount() == 1) {
        // This is the last ref to this RLD, and we're not
        // pooling, so remove it:
//        System.out.println("[" + Thread.currentThread().getName() + "] ReaderPool.release: " + rld.info);
        if (rld.writeLiveDocs(directory)) {
          // Make sure we only write del docs for a live segment:
          assert assertInfoLive == false || infoIsLive(rld.info);
          // Must checkpoint because we just
          // created new _X_N.del and field updates files;
          // don't call IW.checkpoint because that also
          // increments SIS.version, which we do not want to
          // do here: it was done previously (after we
          // invoked BDS.applyDeletes), whereas here all we
          // did was move the state to disk:
          checkpointNoSIS();
        }
        //System.out.println("IW: done writeLiveDocs for info=" + rld.info);

//        System.out.println("[" + Thread.currentThread().getName() + "] ReaderPool.release: drop readers " + rld.info);
        rld.dropReaders();
        readerMap.remove(rld.info);
      }
    }
    
    @Override
    public void close() throws IOException {
      dropAll(false);
    }

    /** Remove all our references to readers, and commits
     *  any pending changes. */
    synchronized void dropAll(boolean doSave) throws IOException {
      Throwable priorE = null;
      final Iterator<Map.Entry<SegmentCommitInfo,ReadersAndUpdates>> it = readerMap.entrySet().iterator();
      while(it.hasNext()) {
        final ReadersAndUpdates rld = it.next().getValue();

        try {
          if (doSave && rld.writeLiveDocs(directory)) {
            // Make sure we only write del docs and field updates for a live segment:
            assert infoIsLive(rld.info);
            // Must checkpoint because we just
            // created new _X_N.del and field updates files;
            // don't call IW.checkpoint because that also
            // increments SIS.version, which we do not want to
            // do here: it was done previously (after we
            // invoked BDS.applyDeletes), whereas here all we
            // did was move the state to disk:
            checkpointNoSIS();
          }
        } catch (Throwable t) {
          if (doSave) {
            IOUtils.reThrow(t);
          } else if (priorE == null) {
            priorE = t;
          }
        }

        // Important to remove as-we-go, not with .clear()
        // in the end, in case we hit an exception;
        // otherwise we could over-decref if close() is
        // called again:
        it.remove();

        // NOTE: it is allowed that these decRefs do not
        // actually close the SRs; this happens when a
        // near real-time reader is kept open after the
        // IndexWriter instance is closed:
        try {
          rld.dropReaders();
        } catch (Throwable t) {
          if (doSave) {
            IOUtils.reThrow(t);
          } else if (priorE == null) {
            priorE = t;
          }
        }
      }
      assert readerMap.size() == 0;
      IOUtils.reThrow(priorE);
    }

    /**
     * Commit live docs changes for the segment readers for
     * the provided infos.
     *
     * @throws IOException If there is a low-level I/O error
     */
    public synchronized void commit(SegmentInfos infos) throws IOException {
      for (SegmentCommitInfo info : infos) {
        final ReadersAndUpdates rld = readerMap.get(info);
        if (rld != null) {
          assert rld.info == info;
          if (rld.writeLiveDocs(directory)) {
            // Make sure we only write del docs for a live segment:
            assert infoIsLive(info);
            // Must checkpoint because we just
            // created new _X_N.del and field updates files;
            // don't call IW.checkpoint because that also
            // increments SIS.version, which we do not want to
            // do here: it was done previously (after we
            // invoked BDS.applyDeletes), whereas here all we
            // did was move the state to disk:
            checkpointNoSIS();
          }
        }
      }
    }

    /**
     * Obtain a ReadersAndLiveDocs instance from the
     * readerPool.  If create is true, you must later call
     * {@link #release(ReadersAndUpdates)}.
     */
    public synchronized ReadersAndUpdates get(SegmentCommitInfo info, boolean create) {

      assert info.info.dir == directory: "info.dir=" + info.info.dir + " vs " + directory;

      ReadersAndUpdates rld = readerMap.get(info);
      if (rld == null) {
        if (!create) {
          return null;
        }
        rld = new ReadersAndUpdates(IndexWriter.this, info);
        // Steal initial reference:
        readerMap.put(info, rld);
      } else {
        assert rld.info == info: "rld.info=" + rld.info + " info=" + info + " isLive?=" + infoIsLive(rld.info) + " vs " + infoIsLive(info);
      }

      if (create) {
        // Return ref to caller:
        rld.incRef();
      }

      assert noDups();

      return rld;
    }

    // Make sure that every segment appears only once in the
    // pool:
    private boolean noDups() {
      Set<String> seen = new HashSet<>();
      for(SegmentCommitInfo info : readerMap.keySet()) {
        assert !seen.contains(info.info.name);
        seen.add(info.info.name);
      }
      return true;
    }
  }

  /**
   * Obtain the number of deleted docs for a pooled reader.
   * If the reader isn't being pooled, the segmentInfo's 
   * delCount is returned.
   */
  public int numDeletedDocs(SegmentCommitInfo info) {
    ensureOpen(false);
    int delCount = info.getDelCount();

    final ReadersAndUpdates rld = readerPool.get(info, false);
    if (rld != null) {
      delCount += rld.getPendingDeleteCount();
    }
    return delCount;
  }

  /**
   * Used internally to throw an {@link AlreadyClosedException} if this
   * IndexWriter has been closed or is in the process of closing.
   * 
   * @param failIfClosing
   *          if true, also fail when {@code IndexWriter} is in the process of
   *          closing ({@code closing=true}) but not yet done closing (
   *          {@code closed=false})
   * @throws AlreadyClosedException
   *           if this IndexWriter is closed or in the process of closing
   */
  protected final void ensureOpen(boolean failIfClosing) throws AlreadyClosedException {
    if (closed || (failIfClosing && closing)) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  /**
   * Used internally to throw an {@link
   * AlreadyClosedException} if this IndexWriter has been
   * closed ({@code closed=true}) or is in the process of
   * closing ({@code closing=true}).
   * <p>
   * Calls {@link #ensureOpen(boolean) ensureOpen(true)}.
   * @throws AlreadyClosedException if this IndexWriter is closed
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    ensureOpen(true);
  }

  final Codec codec; // for writing new segments

  /**
   * Constructs a new IndexWriter per the settings given in <code>conf</code>.
   * If you want to make "live" changes to this writer instance, use
   * {@link #getConfig()}.
   * 
   * <p>
   * <b>NOTE:</b> after ths writer is created, the given configuration instance
   * cannot be passed to another writer. If you intend to do so, you should
   * {@link IndexWriterConfig#clone() clone} it beforehand.
   * 
   * @param d
   *          the index directory. The index is either created or appended
   *          according <code>conf.getOpenMode()</code>.
   * @param conf
   *          the configuration settings according to which IndexWriter should
   *          be initialized.
   * @throws IOException
   *           if the directory cannot be read/written to, or if it does not
   *           exist and <code>conf.getOpenMode()</code> is
   *           <code>OpenMode.APPEND</code> or if there is any other low-level
   *           IO error
   */
  public IndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
    conf.setIndexWriter(this); // prevent reuse by other instances
    config = conf;
    directory = d;
    analyzer = config.getAnalyzer();
    infoStream = config.getInfoStream();
    mergeScheduler = config.getMergeScheduler();
    codec = config.getCodec();

    bufferedUpdatesStream = new BufferedUpdatesStream(infoStream);
    poolReaders = config.getReaderPooling();

    writeLock = directory.makeLock(WRITE_LOCK_NAME);

    if (!writeLock.obtain(config.getWriteLockTimeout())) // obtain write lock
      throw new LockObtainFailedException("Index locked for write: " + writeLock);

    boolean success = false;
    try {
      OpenMode mode = config.getOpenMode();
      boolean create;
      if (mode == OpenMode.CREATE) {
        create = true;
      } else if (mode == OpenMode.APPEND) {
        create = false;
      } else {
        // CREATE_OR_APPEND - create only if an index does not exist
        create = !DirectoryReader.indexExists(directory);
      }

      // If index is too old, reading the segments will throw
      // IndexFormatTooOldException.
      segmentInfos = new SegmentInfos();

      boolean initialIndexExists = true;

      if (create) {
        // Try to read first.  This is to allow create
        // against an index that's currently open for
        // searching.  In this case we write the next
        // segments_N file with no segments:
        try {
          segmentInfos.read(directory);
          segmentInfos.clear();
        } catch (IOException e) {
          // Likely this means it's a fresh directory
          initialIndexExists = false;
        }

        // Record that we have a change (zero out all
        // segments) pending:
        changed();
      } else {
        segmentInfos.read(directory);

        IndexCommit commit = config.getIndexCommit();
        if (commit != null) {
          // Swap out all segments, but, keep metadata in
          // SegmentInfos, like version & generation, to
          // preserve write-once.  This is important if
          // readers are open against the future commit
          // points.
          if (commit.getDirectory() != directory)
            throw new IllegalArgumentException("IndexCommit's directory doesn't match my directory");
          SegmentInfos oldInfos = new SegmentInfos();
          oldInfos.read(directory, commit.getSegmentsFileName());
          segmentInfos.replace(oldInfos);
          changed();
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "init: loaded commit \"" + commit.getSegmentsFileName() + "\"");
          }
        }
      }

      rollbackSegments = segmentInfos.createBackupSegmentInfos();

      // start with previous field numbers, but new FieldInfos
      globalFieldNumberMap = getFieldNumberMap();
      config.getFlushPolicy().init(config);
      docWriter = new DocumentsWriter(this, config, directory);
      eventQueue = docWriter.eventQueue();

      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:
      synchronized(this) {
        deleter = new IndexFileDeleter(directory,
                                       config.getIndexDeletionPolicy(),
                                       segmentInfos, infoStream, this,
                                       initialIndexExists);
      }

      if (deleter.startingCommitDeleted) {
        // Deletion policy deleted the "head" commit point.
        // We have to mark ourself as changed so that if we
        // are closed w/o any further changes we write a new
        // segments_N file.
        changed();
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "init: create=" + create);
        messageState();
      }

      success = true;

    } finally {
      if (!success) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "init: hit exception on init; releasing write lock");
        }
        IOUtils.closeWhileHandlingException(writeLock);
        writeLock = null;
      }
    }
  }

  /**
   * Loads or returns the already loaded the global field number map for this {@link SegmentInfos}.
   * If this {@link SegmentInfos} has no global field number map the returned instance is empty
   */
  private FieldNumbers getFieldNumberMap() throws IOException {
    final FieldNumbers map = new FieldNumbers();

    for(SegmentCommitInfo info : segmentInfos) {
      for(FieldInfo fi : SegmentReader.readFieldInfos(info)) {
        map.addOrGet(fi.name, fi.number, fi.getDocValuesType());
      }
    }

    return map;
  }
  
  /**
   * Returns a {@link LiveIndexWriterConfig}, which can be used to query the IndexWriter
   * current settings, as well as modify "live" ones.
   */
  public LiveIndexWriterConfig getConfig() {
    ensureOpen(false);
    return config;
  }

  private void messageState() {
    if (infoStream.isEnabled("IW") && didMessageState == false) {
      didMessageState = true;
      infoStream.message("IW", "\ndir=" + directory + "\n" +
            "index=" + segString() + "\n" +
            "version=" + Version.LATEST.toString() + "\n" +
            config.toString());
    }
  }

  /**
   * Gracefully closes (commits, waits for merges), but calls rollback
   * if there's an exc so the IndexWriter is always closed.
   */
  private void shutdown(boolean waitForMerges) throws IOException {
    if (pendingCommit != null) {
      throw new IllegalStateException("cannot close: prepareCommit was already called with no corresponding call to commit");
    }
    // Ensure that only one thread actually gets to do the
    // closing
    if (shouldClose()) {
      boolean success = false;
      try {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "now flush at close");
        }
        flush(true, true);
        if (waitForMerges) {
          waitForMerges();
        } else {
          abortMerges();
        }
        commitInternal(config.getMergePolicy());
        rollbackInternal(); // ie close, since we just committed
        success = true;
      } finally {
        if (success == false) {
          // Be certain to close the index on any exception
          try {
            rollbackInternal();
          } catch (Throwable t) {
            // Suppress so we keep throwing original exception
          }
        }
      }
    }
  }

  /**
   * Commits all changes to an index, waits for pending merges
   * to complete, closes all associated files and releases the
   * write lock.  
   *
   * <p>Note that:
   * <ul>
   *   <li>If you called prepareCommit but failed to call commit, this
   *       method will throw {@code IllegalStateException} and the {@code IndexWriter}
   *       will not be closed.</li>
   *   <li>If this method throws any other exception, the {@code IndexWriter}
   *       will be closed, but changes may have been lost.</li>
   * </ul>
   *
   * <p>
   * Note that this may be a costly
   * operation, so, try to re-use a single writer instead of
   * closing and opening a new one.  See {@link #commit()} for
   * caveats about write caching done by some IO devices.
   *
   * <p><b>NOTE</b>: You must ensure no other threads are still making
   * changes at the same time that this method is invoked.</p>
   */
  @Override
  public void close() throws IOException {
    close(true);
  }

  /**
   * Closes the index with or without waiting for currently
   * running merges to finish.  This is only meaningful when
   * using a MergeScheduler that runs merges in background
   * threads.  See {@link #close()} for details on behavior
   * when exceptions are thrown.
   *
   * <p><b>NOTE</b>: it is dangerous to always call
   * close(false), especially when IndexWriter is not open
   * for very long, because this can result in "merge
   * starvation" whereby long merges will never have a
   * chance to finish.  This will cause too many segments in
   * your index over time, which leads to all sorts of
   * problems like slow searches, too much RAM and too
   * many file descriptors used by readers, etc. </p>
   *
   * @param waitForMerges if true, this call will block
   * until all merges complete; else, it will ask all
   * running merges to abort, wait until those merges have
   * finished (which should be at most a few seconds), and
   * then return.
   *
   * @deprecated To abort merges and then close, call
   * {@link #commit} and then {@link #rollback} instead.
   */
  @Deprecated
  public void close(boolean waitForMerges) throws IOException {
    shutdown(waitForMerges);
  }

  private boolean assertEventQueueAfterClose() {
    if (eventQueue.isEmpty()) {
      return true;
    }
    for (Event e : eventQueue) {
      assert e instanceof DocumentsWriter.MergePendingEvent : e;
    }
    return true;
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
      } else {
        return false;
      }
    }
  }

  private void closeInternal(boolean waitForMerges, boolean doFlush) throws IOException {
    boolean interrupted = false;
    try {

      if (pendingCommit != null) {
        throw new IllegalStateException("cannot close: prepareCommit was already called with no corresponding call to commit");
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now flush at close waitForMerges=" + waitForMerges);
      }

      docWriter.close();

      try {
        // Only allow a new merge to be triggered if we are
        // going to wait for merges:
        if (doFlush) {
          flush(waitForMerges, true);
        } else {
          docWriter.abort(this); // already closed -- never sync on IW 
        }
        
      } finally {
        try {
          // clean up merge scheduler in all cases, although flushing may have failed:
          interrupted = Thread.interrupted();
        
          if (waitForMerges) {
            try {
              // Give merge scheduler last chance to run, in case
              // any pending merges are waiting:
              mergeScheduler.merge(this, MergeTrigger.CLOSING, false);
            } catch (ThreadInterruptedException tie) {
              // ignore any interruption, does not matter
              interrupted = true;
              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "interrupted while waiting for final merges");
              }
            }
          }
          
          synchronized(this) {
            for (;;) {
              try {
                if (waitForMerges && !interrupted) {
                  waitForMerges();
                } else {
                  abortMerges();
                }
                break;
              } catch (ThreadInterruptedException tie) {
                // by setting the interrupted status, the next iteration will abort merges
                interrupted = true;
                if (infoStream.isEnabled("IW")) {
                  infoStream.message("IW", "interrupted while waiting for merges to finish");
                }
              }
            }
            stopMerges = true;
          }
          
        } finally {
          // shutdown policy, scheduler and all threads (this call is not interruptible):
          IOUtils.closeWhileHandlingException(mergeScheduler);
        }
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now call final commit()");
      }

      if (doFlush) {
        commitInternal(config.getMergePolicy());
      }
      processEvents(false, true);
      synchronized(this) {
        // commitInternal calls ReaderPool.commit, which
        // writes any pending liveDocs from ReaderPool, so
        // it's safe to drop all readers now:
        readerPool.dropAll(true);
        deleter.close();
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "at close: " + segString());
      }

      if (writeLock != null) {
        writeLock.close();                          // release write lock
        writeLock = null;
      }
      synchronized(this) {
        closed = true;
      }
      assert docWriter.perThreadPool.numDeactivatedThreadStates() == docWriter.perThreadPool.getMaxThreadStates() : "" +  docWriter.perThreadPool.numDeactivatedThreadStates() + " " +  docWriter.perThreadPool.getMaxThreadStates();
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "closeInternal");
    } finally {
      synchronized(this) {
        closing = false;
        notifyAll();
        if (!closed) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit exception while closing");
          }
        }
      }
      // finally, restore interrupt status:
      if (interrupted) Thread.currentThread().interrupt();
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
    ensureOpen();
    return docWriter.getNumDocs() + segmentInfos.totalDocCount();
  }

  /** Returns total number of docs in this index, including
   *  docs not yet flushed (still in the RAM buffer), and
   *  including deletions.  <b>NOTE:</b> buffered deletions
   *  are not counted.  If you really need these to be
   *  counted you should call {@link #commit()} first.
   *  @see #numDocs */
  public synchronized int numDocs() {
    ensureOpen();
    int count = docWriter.getNumDocs();
    for (final SegmentCommitInfo info : segmentInfos) {
      count += info.info.getDocCount() - numDeletedDocs(info);
    }
    return count;
  }

  /**
   * Returns true if this index has deletions (including
   * buffered deletions).  Note that this will return true
   * if there are buffered Term/Query deletions, even if it
   * turns out those buffered deletions don't match any
   * documents.
   */
  public synchronized boolean hasDeletions() {
    ensureOpen();
    if (bufferedUpdatesStream.any()) {
      return true;
    }
    if (docWriter.anyDeletions()) {
      return true;
    }
    if (readerPool.anyPendingDeletes()) {
      return true;
    }
    for (final SegmentCommitInfo info : segmentInfos) {
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
   * {@link #forceMerge(int)} for details). The sequence of
   * primitive merge operations performed is governed by the
   * merge policy.
   *
   * <p>Note that each term in the document can be no longer
   * than {@link #MAX_TERM_LENGTH} in bytes, otherwise an
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
  public void addDocument(Iterable<? extends IndexableField> doc) throws IOException {
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
  public void addDocument(Iterable<? extends IndexableField> doc, Analyzer analyzer) throws IOException {
    updateDocument(null, doc, analyzer);
  }

  /**
   * Atomically adds a block of documents with sequentially
   * assigned document IDs, such that an external reader
   * will see all or none of the documents.
   *
   * <p><b>WARNING</b>: the index does not currently record
   * which documents were added as a block.  Today this is
   * fine, because merging will preserve a block. The order of
   * documents within a segment will be preserved, even when child
   * documents within a block are deleted. Most search features
   * (like result grouping and block joining) require you to
   * mark documents; when these documents are deleted these
   * search features will not work as expected. Obviously adding
   * documents to an existing block will require you the reindex
   * the entire block.
   *
   * <p>However it's possible that in the future Lucene may
   * merge more aggressively re-order documents (for example,
   * perhaps to obtain better index compression), in which case
   * you may need to fully re-index your documents at that time.
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
  public void addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
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
  public void addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer analyzer) throws IOException {
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
  public void updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
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
  public void updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer analyzer) throws IOException {
    ensureOpen();
    try {
      boolean success = false;
      try {
        if (docWriter.updateDocuments(docs, analyzer, delTerm)) {
          processEvents(true, false);
        }
        success = true;
      } finally {
        if (!success) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit exception updating document");
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateDocuments");
    }
  }

  /** Expert: attempts to delete by document ID, as long as
   *  the provided reader is a near-real-time reader (from {@link
   *  DirectoryReader#open(IndexWriter,boolean)}).  If the
   *  provided reader is an NRT reader obtained from this
   *  writer, and its segment has not been merged away, then
   *  the delete succeeds and this method returns true; else, it
   *  returns false the caller must then separately delete by
   *  Term or Query.
   *
   *  <b>NOTE</b>: this method can only delete documents
   *  visible to the currently open NRT reader.  If you need
   *  to delete documents indexed after opening the NRT
   *  reader you must use {@link #deleteDocuments(Term...)}). */
  public synchronized boolean tryDeleteDocument(IndexReader readerIn, int docID) throws IOException {

    final AtomicReader reader;
    if (readerIn instanceof AtomicReader) {
      // Reader is already atomic: use the incoming docID:
      reader = (AtomicReader) readerIn;
    } else {
      // Composite reader: lookup sub-reader and re-base docID:
      List<AtomicReaderContext> leaves = readerIn.leaves();
      int subIndex = ReaderUtil.subIndex(docID, leaves);
      reader = leaves.get(subIndex).reader();
      docID -= leaves.get(subIndex).docBase;
      assert docID >= 0;
      assert docID < reader.maxDoc();
    }

    if (!(reader instanceof SegmentReader)) {
      throw new IllegalArgumentException("the reader must be a SegmentReader or composite reader containing only SegmentReaders");
    }
      
    final SegmentCommitInfo info = ((SegmentReader) reader).getSegmentInfo();

    // TODO: this is a slow linear search, but, number of
    // segments should be contained unless something is
    // seriously wrong w/ the index, so it should be a minor
    // cost:

    if (segmentInfos.indexOf(info) != -1) {
      ReadersAndUpdates rld = readerPool.get(info, false);
      if (rld != null) {
        synchronized(bufferedUpdatesStream) {
          rld.initWritableLiveDocs();
          if (rld.delete(docID)) {
            final int fullDelCount = rld.info.getDelCount() + rld.getPendingDeleteCount();
            if (fullDelCount == rld.info.info.getDocCount()) {
              // If a merge has already registered for this
              // segment, we leave it in the readerPool; the
              // merge will skip merging it and will then drop
              // it once it's done:
              if (!mergingSegments.contains(rld.info)) {
                segmentInfos.remove(rld.info);
                readerPool.drop(rld.info);
                checkpoint();
              }
            }

            // Must bump changeCount so if no other changes
            // happened, we still commit this change:
            changed();
          }
          //System.out.println("  yes " + info.info.name + " " + docID);
          return true;
        }
      } else {
        //System.out.println("  no rld " + info.info.name + " " + docID);
      }
    } else {
      //System.out.println("  no seg " + info.info.name + " " + docID);
    }
    return false;
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
  public void deleteDocuments(Term... terms) throws IOException {
    ensureOpen();
    try {
      if (docWriter.deleteTerms(terms)) {
        processEvents(true, false);
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Term..)");
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
  public void deleteDocuments(Query... queries) throws IOException {
    ensureOpen();
    try {
      if (docWriter.deleteQueries(queries)) {
        processEvents(true, false);
      }
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
  public void updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
    ensureOpen();
    updateDocument(term, doc, analyzer);
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
      throws IOException {
    ensureOpen();
    try {
      boolean success = false;
      try {
        if (docWriter.updateDocument(doc, analyzer, term)) {
          processEvents(true, false);
        }
        success = true;
      } finally {
        if (!success) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit exception updating document");
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateDocument");
    }
  }

  /**
   * Updates a document's {@link NumericDocValues} for <code>field</code> to the
   * given <code>value</code>. You can only update fields that already exist in
   * the index, not add new fields through this method.
   * 
   * <p>
   * <b>NOTE</b>: if this method hits an OutOfMemoryError you should immediately
   * close the writer. See <a href="#OOME">above</a> for details.
   * </p>
   * 
   * @param term
   *          the term to identify the document(s) to be updated
   * @param field
   *          field name of the {@link NumericDocValues} field
   * @param value
   *          new value for the field
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public void updateNumericDocValue(Term term, String field, long value) throws IOException {
    ensureOpen();
    if (!globalFieldNumberMap.contains(field, DocValuesType.NUMERIC)) {
      throw new IllegalArgumentException("can only update existing numeric-docvalues fields!");
    }
    try {
      if (docWriter.updateDocValues(new NumericDocValuesUpdate(term, field, value))) {
        processEvents(true, false);
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateNumericDocValue");
    }
  }

  /**
   * Updates a document's {@link BinaryDocValues} for <code>field</code> to the
   * given <code>value</code>. You can only update fields that already exist in
   * the index, not add new fields through this method.
   * 
   * <p>
   * <b>NOTE:</b> this method currently replaces the existing value of all
   * affected documents with the new value.
   * 
   * <p>
   * <b>NOTE:</b> if this method hits an OutOfMemoryError you should immediately
   * close the writer. See <a href="#OOME">above</a> for details.
   * </p>
   * 
   * @param term
   *          the term to identify the document(s) to be updated
   * @param field
   *          field name of the {@link BinaryDocValues} field
   * @param value
   *          new value for the field
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public void updateBinaryDocValue(Term term, String field, BytesRef value) throws IOException {
    ensureOpen();
    if (value == null) {
      throw new IllegalArgumentException("cannot update a field to a null value: " + field);
    }
    if (!globalFieldNumberMap.contains(field, DocValuesType.BINARY)) {
      throw new IllegalArgumentException("can only update existing binary-docvalues fields!");
    }
    try {
      if (docWriter.updateDocValues(new BinaryDocValuesUpdate(term, field, value))) {
        processEvents(true, false);
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateBinaryDocValue");
    }
  }
  
  /**
   * Updates documents' DocValues fields to the given values. Each field update
   * is applied to the set of documents that are associated with the
   * {@link Term} to the same value. All updates are atomically applied and
   * flushed together.
   * 
   * <p>
   * <b>NOTE</b>: if this method hits an OutOfMemoryError you should immediately
   * close the writer. See <a href="#OOME">above</a> for details.
   * </p>
   * 
   * @param updates
   *          the updates to apply
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public void updateDocValues(Term term, Field... updates) throws IOException {
    ensureOpen();
    DocValuesUpdate[] dvUpdates = new DocValuesUpdate[updates.length];
    for (int i = 0; i < updates.length; i++) {
      final Field f = updates[i];
      final DocValuesType dvType = f.fieldType().docValueType();
      if (dvType == null) {
        throw new IllegalArgumentException("can only update NUMERIC or BINARY fields! field=" + f.name());
      }
      if (!globalFieldNumberMap.contains(f.name(), dvType)) {
        throw new IllegalArgumentException("can only update existing docvalues fields! field=" + f.name() + ", type=" + dvType);
      }
      switch (dvType) {
        case NUMERIC:
          dvUpdates[i] = new NumericDocValuesUpdate(term, f.name(), (Long) f.numericValue());
          break;
        case BINARY:
          dvUpdates[i] = new BinaryDocValuesUpdate(term, f.name(), f.binaryValue());
          break;
        default:
          throw new IllegalArgumentException("can only update NUMERIC or BINARY fields: field=" + f.name() + ", type=" + dvType);
      }
    }
    try {
      if (docWriter.updateDocValues(dvUpdates)) {
        processEvents(true, false);
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "updateDocValues");
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
      return segmentInfos.info(i).info.getDocCount();
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
  final InfoStream infoStream;

  /**
   * Forces merge policy to merge segments until there are <=
   * maxNumSegments.  The actual merges to be
   * executed are determined by the {@link MergePolicy}.
   *
   * <p>This is a horribly costly operation, especially when
   * you pass a small {@code maxNumSegments}; usually you
   * should only call this if the index is static (will no
   * longer be changed).</p>
   *
   * <p>Note that this requires up to 2X the index size free
   * space in your Directory (3X if you're using compound
   * file format).  For example, if your index size is 10 MB
   * then you need up to 20 MB free for this to complete (30
   * MB if you're using compound file format).  Also,
   * it's best to call {@link #commit()} afterwards,
   * to allow IndexWriter to free up disk space.</p>
   *
   * <p>If some but not all readers re-open while merging
   * is underway, this will cause > 2X temporary
   * space to be consumed as those new readers will then
   * hold open the temporary segments at that time.  It is
   * best not to re-open readers while merging is running.</p>
   *
   * <p>The actual temporary usage could be much less than
   * these figures (it depends on many factors).</p>
   *
   * <p>In general, once this completes, the total size of the
   * index will be less than the size of the starting index.
   * It could be quite a bit smaller (if there were many
   * pending deletes) or just slightly smaller.</p>
   *
   * <p>If an Exception is hit, for example
   * due to disk full, the index will not be corrupted and no
   * documents will be lost.  However, it may have
   * been partially merged (some segments were merged but
   * not all), and it's possible that one of the segments in
   * the index will be in non-compound format even when
   * using compound file format.  This will occur when the
   * Exception is hit during conversion of the segment into
   * compound format.</p>
   *
   * <p>This call will merge those segments present in
   * the index when the call started.  If other threads are
   * still adding documents and flushing segments, those
   * newly created segments will not be merged unless you
   * call forceMerge again.</p>
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
   * @param maxNumSegments maximum number of segments left
   * in the index after merging finishes
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @see MergePolicy#findMerges
   *
  */
  public void forceMerge(int maxNumSegments) throws IOException {
    forceMerge(maxNumSegments, true);
  }

  /** Just like {@link #forceMerge(int)}, except you can
   *  specify whether the call should block until
   *  all merging completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads.
   *
   *  <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   *  you should immediately close the writer.  See <a
   *  href="#OOME">above</a> for details.</p>
   */
  public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
    ensureOpen();

    if (maxNumSegments < 1)
      throw new IllegalArgumentException("maxNumSegments must be >= 1; got " + maxNumSegments);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "forceMerge: index now " + segString());
      infoStream.message("IW", "now flush at forceMerge");
    }

    flush(true, true);

    synchronized(this) {
      resetMergeExceptions();
      segmentsToMerge.clear();
      for(SegmentCommitInfo info : segmentInfos) {
        segmentsToMerge.put(info, Boolean.TRUE);
      }
      mergeMaxNumSegments = maxNumSegments;

      // Now mark all pending & running merges for forced
      // merge:
      for(final MergePolicy.OneMerge merge  : pendingMerges) {
        merge.maxNumSegments = maxNumSegments;
        segmentsToMerge.put(merge.info, Boolean.TRUE);
      }

      for (final MergePolicy.OneMerge merge: runningMerges) {
        merge.maxNumSegments = maxNumSegments;
        segmentsToMerge.put(merge.info, Boolean.TRUE);
      }
    }

    maybeMerge(config.getMergePolicy(), MergeTrigger.EXPLICIT, maxNumSegments);

    if (doWait) {
      synchronized(this) {
        while(true) {

          if (hitOOM) {
            throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot complete forceMerge");
          }

          if (mergeExceptions.size() > 0) {
            // Forward any exceptions in background merge
            // threads to the current thread:
            final int size = mergeExceptions.size();
            for(int i=0;i<size;i++) {
              final MergePolicy.OneMerge merge = mergeExceptions.get(i);
              if (merge.maxNumSegments != -1) {
                throw new IOException("background merge hit exception: " + merge.segString(directory), merge.getException());
              }
            }
          }

          if (maxNumSegmentsMergesPending())
            doWait();
          else
            break;
        }
      }

      // If close is called while we are still
      // running, throw an exception so the calling
      // thread will know merging did not
      // complete
      ensureOpen();
    }
    // NOTE: in the ConcurrentMergeScheduler case, when
    // doWait is false, we can return immediately while
    // background threads accomplish the merging
  }

  /** Returns true if any merges in pendingMerges or
   *  runningMerges are maxNumSegments merges. */
  private synchronized boolean maxNumSegmentsMergesPending() {
    for (final MergePolicy.OneMerge merge : pendingMerges) {
      if (merge.maxNumSegments != -1)
        return true;
    }

    for (final MergePolicy.OneMerge merge : runningMerges) {
      if (merge.maxNumSegments != -1)
        return true;
    }

    return false;
  }

  /** Just like {@link #forceMergeDeletes()}, except you can
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
  public void forceMergeDeletes(boolean doWait)
    throws IOException {
    ensureOpen();

    flush(true, true);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "forceMergeDeletes: index now " + segString());
    }

    final MergePolicy mergePolicy = config.getMergePolicy();
    MergePolicy.MergeSpecification spec;
    boolean newMergesFound = false;
    synchronized(this) {
      spec = mergePolicy.findForcedDeletesMerges(segmentInfos, this);
      newMergesFound = spec != null;
      if (newMergesFound) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++)
          registerMerge(spec.merges.get(i));
      }
    }

    mergeScheduler.merge(this, MergeTrigger.EXPLICIT, newMergesFound);

    if (spec != null && doWait) {
      final int numMerges = spec.merges.size();
      synchronized(this) {
        boolean running = true;
        while(running) {

          if (hitOOM) {
            throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot complete forceMergeDeletes");
          }

          // Check each merge that MergePolicy asked us to
          // do, to see if any of them are still running and
          // if any of them have hit an exception.
          running = false;
          for(int i=0;i<numMerges;i++) {
            final MergePolicy.OneMerge merge = spec.merges.get(i);
            if (pendingMerges.contains(merge) || runningMerges.contains(merge)) {
              running = true;
            }
            Throwable t = merge.getException();
            if (t != null) {
              throw new IOException("background merge hit exception: " + merge.segString(directory), t);
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
    // background threads accomplish the merging
  }


  /**
   *  Forces merging of all segments that have deleted
   *  documents.  The actual merges to be executed are
   *  determined by the {@link MergePolicy}.  For example,
   *  the default {@link TieredMergePolicy} will only
   *  pick a segment if the percentage of
   *  deleted docs is over 10%.
   *
   *  <p>This is often a horribly costly operation; rarely
   *  is it warranted.</p>
   *
   *  <p>To see how
   *  many deletions you have pending in your index, call
   *  {@link IndexReader#numDeletedDocs}.</p>
   *
   *  <p><b>NOTE</b>: this method first flushes a new
   *  segment (if there are indexed documents), and applies
   *  all buffered deletes.
   *
   *  <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   *  you should immediately close the writer.  See <a
   *  href="#OOME">above</a> for details.</p>
   */
  public void forceMergeDeletes() throws IOException {
    forceMergeDeletes(true);
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
   * This method will call the {@link MergePolicy} with
   * {@link MergeTrigger#EXPLICIT}.
   *
   * <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   * you should immediately close the writer.  See <a
   * href="#OOME">above</a> for details.</p>
   */
  public final void maybeMerge() throws IOException {
    maybeMerge(config.getMergePolicy(), MergeTrigger.EXPLICIT, UNBOUNDED_MAX_MERGE_SEGMENTS);
  }

  private final void maybeMerge(MergePolicy mergePolicy, MergeTrigger trigger, int maxNumSegments) throws IOException {
    ensureOpen(false);
    boolean newMergesFound = updatePendingMerges(mergePolicy, trigger, maxNumSegments);
    mergeScheduler.merge(this, trigger, newMergesFound);
  }

  private synchronized boolean updatePendingMerges(MergePolicy mergePolicy, MergeTrigger trigger, int maxNumSegments)
    throws IOException {

    // In case infoStream was disabled on init, but then enabled at some
    // point, try again to log the config here:
    messageState();

    assert maxNumSegments == -1 || maxNumSegments > 0;
    assert trigger != null;
    if (stopMerges) {
      return false;
    }

    // Do not start new merges if we've hit OOME
    if (hitOOM) {
      return false;
    }
    boolean newMergesFound = false;
    final MergePolicy.MergeSpecification spec;
    if (maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS) {
      assert trigger == MergeTrigger.EXPLICIT || trigger == MergeTrigger.MERGE_FINISHED :
        "Expected EXPLICT or MERGE_FINISHED as trigger even with maxNumSegments set but was: " + trigger.name();
      spec = mergePolicy.findForcedMerges(segmentInfos, maxNumSegments, Collections.unmodifiableMap(segmentsToMerge), this);
      newMergesFound = spec != null;
      if (newMergesFound) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++) {
          final MergePolicy.OneMerge merge = spec.merges.get(i);
          merge.maxNumSegments = maxNumSegments;
        }
      }
    } else {
      spec = mergePolicy.findMerges(trigger, segmentInfos, this);
    }
    newMergesFound = spec != null;
    if (newMergesFound) {
      final int numMerges = spec.merges.size();
      for(int i=0;i<numMerges;i++) {
        registerMerge(spec.merges.get(i));
      }
    }
    return newMergesFound;
  }

  /** Expert: to be used by a {@link MergePolicy} to avoid
   *  selecting merges for segments already being merged.
   *  The returned collection is not cloned, and thus is
   *  only safe to access if you hold IndexWriter's lock
   *  (which you do when IndexWriter invokes the
   *  MergePolicy).
   *
   *  <p>Do not alter the returned collection! */
  public synchronized Collection<SegmentCommitInfo> getMergingSegments() {
    return mergingSegments;
  }

  /**
   * Expert: the {@link MergeScheduler} calls this method to retrieve the next
   * merge requested by the MergePolicy
   * 
   * @lucene.experimental
   */
  public synchronized MergePolicy.OneMerge getNextMerge() {
    if (pendingMerges.size() == 0) {
      return null;
    } else {
      // Advance the merge from pending to running
      MergePolicy.OneMerge merge = pendingMerges.removeFirst();
      runningMerges.add(merge);
      return merge;
    }
  }

  /**
   * Expert: returns true if there are merges waiting to be scheduled.
   * 
   * @lucene.experimental
   */
  public synchronized boolean hasPendingMerges() {
    return pendingMerges.size() != 0;
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
  @Override
  public void rollback() throws IOException {
    // don't call ensureOpen here: this acts like "close()" in closeable.
    
    // Ensure that only one thread actually gets to do the
    // closing, and make sure no commit is also in progress:
    synchronized(commitLock) {
      if (shouldClose()) {
        rollbackInternal();
      }
    }
  }

  private void rollbackInternal() throws IOException {

    boolean success = false;

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "rollback");
    }
    
    try {
      synchronized(this) {
        abortMerges();
        stopMerges = true;
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "rollback: done finish merges");
      }

      // Must pre-close in case it increments changeCount so that we can then
      // set it to false before calling closeInternal
      mergeScheduler.close();

      bufferedUpdatesStream.clear();
      docWriter.close(); // mark it as closed first to prevent subsequent indexing actions/flushes 
      docWriter.abort(this); // don't sync on IW here
      synchronized(this) {

        if (pendingCommit != null) {
          pendingCommit.rollbackCommit(directory);
          deleter.decRef(pendingCommit);
          pendingCommit = null;
          notifyAll();
        }

        // Don't bother saving any changes in our segmentInfos
        readerPool.dropAll(false);

        // Keep the same segmentInfos instance but replace all
        // of its SegmentInfo instances.  This is so the next
        // attempt to commit using this instance of IndexWriter
        // will always write to a new generation ("write
        // once").
        segmentInfos.rollbackSegmentInfos(rollbackSegments);
        if (infoStream.isEnabled("IW") ) {
          infoStream.message("IW", "rollback: infos=" + segString(segmentInfos));
        }

        assert testPoint("rollback before checkpoint");

        // Ask deleter to locate unreferenced files & remove
        // them:
        deleter.checkpoint(segmentInfos, false);
        deleter.refresh();

        lastCommitChangeCount = changeCount;
        
        deleter.refresh();
        deleter.close();

        IOUtils.close(writeLock);                     // release write lock
        writeLock = null;
        
        assert docWriter.perThreadPool.numDeactivatedThreadStates() == docWriter.perThreadPool.getMaxThreadStates() : "" +  docWriter.perThreadPool.numDeactivatedThreadStates() + " " +  docWriter.perThreadPool.getMaxThreadStates();
      }

      success = true;
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "rollbackInternal");
    } finally {
      if (!success) {
        // Must not hold IW's lock while closing
        // mergeScheduler: this can lead to deadlock,
        // e.g. TestIW.testThreadInterruptDeadlock
        IOUtils.closeWhileHandlingException(mergeScheduler);
      }
      synchronized(this) {
        if (!success) {
          // we tried to be nice about it: do the minimum
          
          // don't leak a segments_N file if there is a pending commit
          if (pendingCommit != null) {
            try {
              pendingCommit.rollbackCommit(directory);
              deleter.decRef(pendingCommit);
            } catch (Throwable t) {}
          }
          
          // close all the closeables we can (but important is readerPool and writeLock to prevent leaks)
          IOUtils.closeWhileHandlingException(readerPool, deleter, writeLock);
          writeLock = null;
        }
        closed = true;
        closing = false;
      }
    }
  }

  /**
   * Delete all documents in the index.
   * 
   * <p>
   * This method will drop all buffered documents and will remove all segments
   * from the index. This change will not be visible until a {@link #commit()}
   * has been called. This method can be rolled back using {@link #rollback()}.
   * </p>
   * 
   * <p>
   * NOTE: this method is much faster than using deleteDocuments( new
   * MatchAllDocsQuery() ). Yet, this method also has different semantics
   * compared to {@link #deleteDocuments(Query...)} since internal
   * data-structures are cleared as well as all segment information is
   * forcefully dropped anti-viral semantics like omitting norms are reset or
   * doc value types are cleared. Essentially a call to {@link #deleteAll()} is
   * equivalent to creating a new {@link IndexWriter} with
   * {@link OpenMode#CREATE} which a delete query only marks documents as
   * deleted.
   * </p>
   * 
   * <p>
   * NOTE: this method will forcefully abort all merges in progress. If other
   * threads are running {@link #forceMerge}, {@link #addIndexes(IndexReader[])}
   * or {@link #forceMergeDeletes} methods, they may receive
   * {@link MergePolicy.MergeAbortedException}s.
   */
  public void deleteAll() throws IOException {
    ensureOpen();
    // Remove any buffered docs
    boolean success = false;
    /* hold the full flush lock to prevent concurrency commits / NRT reopens to
     * get in our way and do unnecessary work. -- if we don't lock this here we might
     * get in trouble if */
    synchronized (fullFlushLock) { 
        /*
         * We first abort and trash everything we have in-memory
         * and keep the thread-states locked, the lockAndAbortAll operation
         * also guarantees "point in time semantics" ie. the checkpoint that we need in terms
         * of logical happens-before relationship in the DW. So we do
         * abort all in memory structures 
         * We also drop global field numbering before during abort to make
         * sure it's just like a fresh index.
         */
      try {
        docWriter.lockAndAbortAll(this);
        processEvents(false, true);
        synchronized (this) {
          try {
            // Abort any running merges
            abortMerges();
            // Remove all segments
            segmentInfos.clear();
            // Ask deleter to locate unreferenced files & remove them:
            deleter.checkpoint(segmentInfos, false);
            /* don't refresh the deleter here since there might
             * be concurrent indexing requests coming in opening
             * files on the directory after we called DW#abort()
             * if we do so these indexing requests might hit FNF exceptions.
             * We will remove the files incrementally as we go...
             */
            // Don't bother saving any changes in our segmentInfos
            readerPool.dropAll(false);
            // Mark that the index has changed
            ++changeCount;
            segmentInfos.changed();
            globalFieldNumberMap.clear();
            success = true;
          } catch (OutOfMemoryError oom) {
            handleOOM(oom, "deleteAll");
          } finally {
            if (!success) {
              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "hit exception during deleteAll");
              }
            }
          }
        }
      } finally {
        docWriter.unlockAllAfterAbortAll(this);
      }
    }
  }

  /** Aborts running merges.  Be careful when using this
   *  method: when you abort a long-running merge, you lose
   *  a lot of work that must later be redone. */
  public synchronized void abortMerges() {
    stopMerges = true;

    // Abort all pending & running merges:
    for (final MergePolicy.OneMerge merge : pendingMerges) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now abort pending merge " + segString(merge.segments));
      }
      merge.abort();
      mergeFinish(merge);
    }
    pendingMerges.clear();

    for (final MergePolicy.OneMerge merge : runningMerges) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now abort running merge " + segString(merge.segments));
      }
      merge.abort();
    }

    // These merges periodically check whether they have
    // been aborted, and stop if so.  We wait here to make
    // sure they all stop.  It should not take very long
    // because the merge threads periodically check if
    // they are aborted.
    while(runningMerges.size() > 0) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now wait for " + runningMerges.size() + " running merge/s to abort");
      }
      doWait();
    }

    stopMerges = false;
    notifyAll();

    assert 0 == mergingSegments.size();

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "all running merges have aborted");
    }
  }

  /**
   * Wait for any currently outstanding merges to finish.
   *
   * <p>It is guaranteed that any merges started prior to calling this method
   *    will have completed once this method completes.</p>
   */
  public void waitForMerges() throws IOException {

    // Give merge scheduler last chance to run, in case
    // any pending merges are waiting. We can't hold IW's lock
    // when going into merge because it can lead to deadlock.
    mergeScheduler.merge(this, MergeTrigger.CLOSING, false);

    synchronized (this) {
      ensureOpen(false);
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "waitForMerges");
      }


      while (pendingMerges.size() > 0 || runningMerges.size() > 0) {
        doWait();
      }

      // sanity check
      assert 0 == mergingSegments.size();

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "waitForMerges done");
      }
    }
  }

  /**
   * Called whenever the SegmentInfos has been updated and
   * the index files referenced exist (correctly) in the
   * index directory.
   */
  synchronized void checkpoint() throws IOException {
    changed();
    deleter.checkpoint(segmentInfos, false);
  }

  /** Checkpoints with IndexFileDeleter, so it's aware of
   *  new files, and increments changeCount, so on
   *  close/commit we will write a new segments file, but
   *  does NOT bump segmentInfos.version. */
  synchronized void checkpointNoSIS() throws IOException {
    changeCount++;
    deleter.checkpoint(segmentInfos, false);
  }

  /** Called internally if any index state has changed. */
  synchronized void changed() {
    changeCount++;
    segmentInfos.changed();
  }

  synchronized void publishFrozenUpdates(FrozenBufferedUpdates packet) {
    assert packet != null && packet.any();
    synchronized (bufferedUpdatesStream) {
      bufferedUpdatesStream.push(packet);
    }
  }
  
  /**
   * Atomically adds the segment private delete packet and publishes the flushed
   * segments SegmentInfo to the index writer.
   */
  void publishFlushedSegment(SegmentCommitInfo newSegment,
      FrozenBufferedUpdates packet, FrozenBufferedUpdates globalPacket) throws IOException {
    try {
      synchronized (this) {
        // Lock order IW -> BDS
        synchronized (bufferedUpdatesStream) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "publishFlushedSegment");
          }
          
          if (globalPacket != null && globalPacket.any()) {
            bufferedUpdatesStream.push(globalPacket);
          } 
          // Publishing the segment must be synched on IW -> BDS to make the sure
          // that no merge prunes away the seg. private delete packet
          final long nextGen;
          if (packet != null && packet.any()) {
            nextGen = bufferedUpdatesStream.push(packet);
          } else {
            // Since we don't have a delete packet to apply we can get a new
            // generation right away
            nextGen = bufferedUpdatesStream.getNextGen();
          }
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "publish sets newSegment delGen=" + nextGen + " seg=" + segString(newSegment));
          }
          newSegment.setBufferedDeletesGen(nextGen);
          segmentInfos.add(newSegment);
          checkpoint();
        }
      }
    } finally {
      flushCount.incrementAndGet();
      doAfterFlush();
    }
  }

  private synchronized void resetMergeExceptions() {
    mergeExceptions = new ArrayList<>();
    mergeGen++;
  }

  private void noDupDirs(Directory... dirs) {
    HashSet<Directory> dups = new HashSet<>();
    for(int i=0;i<dirs.length;i++) {
      if (dups.contains(dirs[i]))
        throw new IllegalArgumentException("Directory " + dirs[i] + " appears more than once");
      if (dirs[i] == directory)
        throw new IllegalArgumentException("Cannot add directory to itself");
      dups.add(dirs[i]);
    }
  }

  /** Acquires write locks on all the directories; be sure
   *  to match with a call to {@link IOUtils#close} in a
   *  finally clause. */
  private List<Lock> acquireWriteLocks(Directory... dirs) throws IOException {
    List<Lock> locks = new ArrayList<>();
    for(int i=0;i<dirs.length;i++) {
      boolean success = false;
      try {
        Lock lock = dirs[i].makeLock(WRITE_LOCK_NAME);
        locks.add(lock);
        lock.obtain(config.getWriteLockTimeout());
        success = true;
      } finally {
        if (success == false) {
          // Release all previously acquired locks:
          IOUtils.closeWhileHandlingException(locks);
        }
      }
    }
    return locks;
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
   * <b>NOTE:</b> this method acquires the write lock in
   * each directory, to ensure that no {@code IndexWriter}
   * is currently open or tries to open while this is
   * running.
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
   * starting index (see {@link #forceMerge(int)} for details).
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
   * @throws LockObtainFailedException if we were unable to
   *   acquire the write lock in at least one directory
   */
  public void addIndexes(Directory... dirs) throws IOException {
    ensureOpen();

    noDupDirs(dirs);

    List<Lock> locks = acquireWriteLocks(dirs);

    boolean successTop = false;

    try {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "flush at addIndexes(Directory...)");
      }

      flush(false, true);

      List<SegmentCommitInfo> infos = new ArrayList<>();

      int totalDocCount = 0;

      boolean success = false;
      try {
        for (Directory dir : dirs) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "addIndexes: process directory " + dir);
          }
          SegmentInfos sis = new SegmentInfos(); // read infos from dir
          sis.read(dir);

          final Set<String> dsFilesCopied = new HashSet<>();
          final Map<String, String> dsNames = new HashMap<>();
          final Set<String> copiedFiles = new HashSet<>();

          totalDocCount += sis.totalDocCount();

          for (SegmentCommitInfo info : sis) {
            assert !infos.contains(info): "dup info dir=" + info.info.dir + " name=" + info.info.name;

            String newSegName = newSegmentName();

            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "addIndexes: process segment origName=" + info.info.name + " newName=" + newSegName + " info=" + info);
            }

            IOContext context = new IOContext(new MergeInfo(info.info.getDocCount(), info.sizeInBytes(), true, -1));

            for(FieldInfo fi : SegmentReader.readFieldInfos(info)) {
              globalFieldNumberMap.addOrGet(fi.name, fi.number, fi.getDocValuesType());
            }
            infos.add(copySegmentAsIs(info, newSegName, dsNames, dsFilesCopied, context, copiedFiles));
          }
        }
        success = true;
      } finally {
        if (!success) {
          for(SegmentCommitInfo sipc : infos) {
            for(String file : sipc.files()) {
              try {
                directory.deleteFile(file);
              } catch (Throwable t) {
              }
            }
          }
        }
      }

      synchronized (this) {
        success = false;
        try {
          ensureOpen();
          // Make sure adding the new documents to this index won't
          // exceed the limit:
          reserveDocs(totalDocCount);
          success = true;
        } finally {
          if (!success) {
            for(SegmentCommitInfo sipc : infos) {
              for(String file : sipc.files()) {
                try {
                  directory.deleteFile(file);
                } catch (Throwable t) {
                }
              }
            }
          }
        }
        segmentInfos.addAll(infos);
        checkpoint();
      }

      successTop = true;

    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addIndexes(Directory...)");
    } finally {
      if (successTop) {
        IOUtils.close(locks);
      } else {
        IOUtils.closeWhileHandlingException(locks);
      }
    }
    maybeMerge();
  }
  
  /**
   * Merges the provided indexes into this index.
   * 
   * <p>
   * The provided IndexReaders are not closed.
   * 
   * <p>
   * See {@link #addIndexes} for details on transactional semantics, temporary
   * free space required in the Directory, and non-CFS segments on an Exception.
   * 
   * <p>
   * <b>NOTE</b>: if this method hits an OutOfMemoryError you should immediately
   * close the writer. See <a href="#OOME">above</a> for details.
   * 
   * <p>
   * <b>NOTE:</b> empty segments are dropped by this method and not added to this
   * index.
   * 
   * <p>
   * <b>NOTE:</b> this method merges all given {@link IndexReader}s in one
   * merge. If you intend to merge a large number of readers, it may be better
   * to call this method multiple times, each time with a small set of readers.
   * In principle, if you use a merge policy with a {@code mergeFactor} or
   * {@code maxMergeAtOnce} parameter, you should pass that many readers in one
   * call. Also, if the given readers are {@link DirectoryReader}s, they can be
   * opened with {@code termIndexInterval=-1} to save RAM, since during merge
   * the in-memory structure is not used. See
   * {@link DirectoryReader#open(Directory, int)}.
   * 
   * <p>
   * <b>NOTE</b>: if you call {@link #close(boolean)} with <tt>false</tt>, which
   * aborts all running merges, then any thread still running this method might
   * hit a {@link MergePolicy.MergeAbortedException}.
   * 
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public void addIndexes(IndexReader... readers) throws IOException {
    ensureOpen();
    int numDocs = 0;

    try {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "flush at addIndexes(IndexReader...)");
      }
      flush(false, true);

      String mergedName = newSegmentName();
      final List<AtomicReader> mergeReaders = new ArrayList<>();
      for (IndexReader indexReader : readers) {
        numDocs += indexReader.numDocs();
        for (AtomicReaderContext ctx : indexReader.leaves()) {
          mergeReaders.add(ctx.reader());
        }
      }

      // Make sure adding the new documents to this index won't
      // exceed the limit:
      reserveDocs(numDocs);
      
      final IOContext context = new IOContext(new MergeInfo(numDocs, -1, true, -1));

      // TODO: somehow we should fix this merge so it's
      // abortable so that IW.close(false) is able to stop it
      TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(directory);

      SegmentInfo info = new SegmentInfo(directory, Version.LATEST, mergedName, -1,
                                         false, codec, null);

      SegmentMerger merger = new SegmentMerger(mergeReaders, info, infoStream, trackingDir, config.getTermIndexInterval(),
                                               MergeState.CheckAbort.NONE, globalFieldNumberMap, 
                                               context, config.getCheckIntegrityAtMerge());
      
      if (!merger.shouldMerge()) {
        return;
      }

      MergeState mergeState;
      boolean success = false;
      try {
        mergeState = merger.merge();                // merge 'em
        success = true;
      } finally {
        if (!success) { 
          synchronized(this) {
            deleter.refresh(info.name);
          }
        }
      }

      SegmentCommitInfo infoPerCommit = new SegmentCommitInfo(info, 0, -1L, -1L, -1L);

      info.setFiles(new HashSet<>(trackingDir.getCreatedFiles()));
      trackingDir.getCreatedFiles().clear();
                                         
      setDiagnostics(info, SOURCE_ADDINDEXES_READERS);

      final MergePolicy mergePolicy = config.getMergePolicy();
      boolean useCompoundFile;
      synchronized(this) { // Guard segmentInfos
        if (stopMerges) {
          deleter.deleteNewFiles(infoPerCommit.files());
          return;
        }
        ensureOpen();
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, infoPerCommit, this);
      }

      // Now create the compound file if needed
      if (useCompoundFile) {
        Collection<String> filesToDelete = infoPerCommit.files();
        try {
          createCompoundFile(infoStream, directory, MergeState.CheckAbort.NONE, info, context);
        } finally {
          // delete new non cfs files directly: they were never
          // registered with IFD
          synchronized(this) {
            deleter.deleteNewFiles(filesToDelete);
          }
        }
        info.setUseCompoundFile(true);
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      success = false;
      try {
        codec.segmentInfoFormat().getSegmentInfoWriter().write(trackingDir, info, mergeState.fieldInfos, context);
        success = true;
      } finally {
        if (!success) {
          synchronized(this) {
            deleter.refresh(info.name);
          }
        }
      }

      info.addFiles(trackingDir.getCreatedFiles());

      // Register the new segment
      synchronized(this) {
        if (stopMerges) {
          deleter.deleteNewFiles(info.files());
          return;
        }
        ensureOpen();
        segmentInfos.add(infoPerCommit);
        checkpoint();
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addIndexes(IndexReader...)");
    }
    maybeMerge();
  }

  /** Copies the segment files as-is into the IndexWriter's directory. */
  private SegmentCommitInfo copySegmentAsIs(SegmentCommitInfo info, String segName,
                                               Map<String, String> dsNames, Set<String> dsFilesCopied, IOContext context,
                                               Set<String> copiedFiles)
      throws IOException {
    // Determine if the doc store of this segment needs to be copied. It's
    // only relevant for segments that share doc store with others,
    // because the DS might have been copied already, in which case we
    // just want to update the DS name of this SegmentInfo.
    final String dsName = Lucene3xSegmentInfoFormat.getDocStoreSegment(info.info);
    assert dsName != null;
    final String newDsName;
    if (dsNames.containsKey(dsName)) {
      newDsName = dsNames.get(dsName);
    } else {
      dsNames.put(dsName, segName);
      newDsName = segName;
    }

    // note: we don't really need this fis (its copied), but we load it up
    // so we don't pass a null value to the si writer
    FieldInfos fis = SegmentReader.readFieldInfos(info);
    
    Set<String> docStoreFiles3xOnly = Lucene3xCodec.getDocStoreFiles(info.info);

    final Map<String,String> attributes;
    // copy the attributes map, we might modify it below.
    // also we need to ensure its read-write, since we will invoke the SIwriter (which might want to set something).
    if (info.info.attributes() == null) {
      attributes = new HashMap<>();
    } else {
      attributes = new HashMap<>(info.info.attributes());
    }
    if (docStoreFiles3xOnly != null) {
      // only violate the codec this way if it's preflex &
      // shares doc stores
      // change docStoreSegment to newDsName
      attributes.put(Lucene3xSegmentInfoFormat.DS_NAME_KEY, newDsName);
    }

    //System.out.println("copy seg=" + info.info.name + " version=" + info.info.getVersion());
    // Same SI as before but we change directory, name and docStoreSegment:
    SegmentInfo newInfo = new SegmentInfo(directory, info.info.getVersion(), segName, info.info.getDocCount(),
                                          info.info.getUseCompoundFile(), info.info.getCodec(), 
                                          info.info.getDiagnostics(), attributes);
    SegmentCommitInfo newInfoPerCommit = new SegmentCommitInfo(newInfo,
        info.getDelCount(), info.getDelGen(), info.getFieldInfosGen(),
        info.getDocValuesGen());

    Set<String> segFiles = new HashSet<>();

    // Build up new segment's file names.  Must do this
    // before writing SegmentInfo:
    for (String file: info.files()) {
      final String newFileName;
      if (docStoreFiles3xOnly != null && docStoreFiles3xOnly.contains(file)) {
        newFileName = newDsName + IndexFileNames.stripSegmentName(file);
      } else {
        newFileName = segName + IndexFileNames.stripSegmentName(file);
      }
      segFiles.add(newFileName);
    }
    newInfo.setFiles(segFiles);

    // We must rewrite the SI file because it references
    // segment name (its own name, if its 3.x, and doc
    // store segment name):
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(directory);
    final Codec currentCodec = newInfo.getCodec();
    try {
      currentCodec.segmentInfoFormat().getSegmentInfoWriter().write(trackingDir, newInfo, fis, context);
    } catch (UnsupportedOperationException uoe) {
      if (currentCodec instanceof Lucene3xCodec) {
        // OK: 3x codec cannot write a new SI file;
        // SegmentInfos will write this on commit
      } else {
        throw uoe;
      }
    }

    final Collection<String> siFiles = trackingDir.getCreatedFiles();

    boolean success = false;
    try {

      // Copy the segment's files
      for (String file: info.files()) {

        final String newFileName;
        if (docStoreFiles3xOnly != null && docStoreFiles3xOnly.contains(file)) {
          newFileName = newDsName + IndexFileNames.stripSegmentName(file);
          if (dsFilesCopied.contains(newFileName)) {
            continue;
          }
          dsFilesCopied.add(newFileName);
        } else {
          newFileName = segName + IndexFileNames.stripSegmentName(file);
        }

        if (siFiles.contains(newFileName)) {
          // We already rewrote this above
          continue;
        }

        assert !slowFileExists(directory, newFileName): "file \"" + newFileName + "\" already exists; siFiles=" + siFiles;
        assert !copiedFiles.contains(file): "file \"" + file + "\" is being copied more than once";
        copiedFiles.add(file);
        info.info.dir.copy(directory, file, newFileName, context);
      }
      success = true;
    } finally {
      if (!success) {
        for(String file : newInfo.files()) {
          try {
            directory.deleteFile(file);
          } catch (Throwable t) {
          }
        }
      }
    }
    
    return newInfoPerCommit;
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

  /** <p>Expert: prepare for commit.  This does the
   *  first phase of 2-phase commit. This method does all
   *  steps necessary to commit changes since this writer
   *  was opened: flushes pending added and deleted docs,
   *  syncs the index files, writes most of next segments_N
   *  file.  After calling this you must call either {@link
   *  #commit()} to finish the commit, or {@link
   *  #rollback()} to revert the commit and undo all changes
   *  done since the writer was opened.</p>
   *
   * <p>You can also just call {@link #commit()} directly
   *  without prepareCommit first in which case that method
   *  will internally call prepareCommit.
   *
   *  <p><b>NOTE</b>: if this method hits an OutOfMemoryError
   *  you should immediately close the writer.  See <a
   *  href="#OOME">above</a> for details.</p>
   */
  @Override
  public final void prepareCommit() throws IOException {
    ensureOpen();
    prepareCommitInternal(config.getMergePolicy());
  }

  private void prepareCommitInternal(MergePolicy mergePolicy) throws IOException {
    startCommitTime = System.nanoTime();
    synchronized(commitLock) {
      ensureOpen(false);
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "prepareCommit: flush");
        infoStream.message("IW", "  index before flush " + segString());
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
            anySegmentsFlushed = docWriter.flushAllThreads(this);
            if (!anySegmentsFlushed) {
              // prevent double increment since docWriter#doFlush increments the flushcount
              // if we flushed anything.
              flushCount.incrementAndGet();
            }
            processEvents(false, true);
            flushSuccess = true;

            synchronized(this) {
              maybeApplyDeletes(true);

              readerPool.commit(segmentInfos);

              // Must clone the segmentInfos while we still
              // hold fullFlushLock and while sync'd so that
              // no partial changes (eg a delete w/o
              // corresponding add from an updateDocument) can
              // sneak into the commit point:
              toCommit = segmentInfos.clone();

              pendingCommitChangeCount = changeCount;

              // This protects the segmentInfos we are now going
              // to commit.  This is important in case, eg, while
              // we are trying to sync all referenced files, a
              // merge completes which would otherwise have
              // removed the files we are now syncing.    
              filesToCommit = toCommit.files(directory, false);
              deleter.incRef(filesToCommit);
            }
            success = true;
          } finally {
            if (!success) {
              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "hit exception during prepareCommit");
              }
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
          maybeMerge(mergePolicy, MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
        }
        startCommit(toCommit);
        success = true;
      } finally {
        if (!success) {
          synchronized (this) {
            if (filesToCommit != null) {
              deleter.decRef(filesToCommit);
              filesToCommit = null;
            }
          }
        }
      }
    }
  }
  
  /**
   * Sets the commit user data map. That method is considered a transaction by
   * {@link IndexWriter} and will be {@link #commit() committed} even if no other
   * changes were made to the writer instance. Note that you must call this method
   * before {@link #prepareCommit()}, or otherwise it won't be included in the
   * follow-on {@link #commit()}.
   * <p>
   * <b>NOTE:</b> the map is cloned internally, therefore altering the map's
   * contents after calling this method has no effect.
   */
  public final synchronized void setCommitData(Map<String,String> commitUserData) {
    segmentInfos.setUserData(new HashMap<>(commitUserData));
    ++changeCount;
  }
  
  /**
   * Returns the commit user data map that was last committed, or the one that
   * was set on {@link #setCommitData(Map)}.
   */
  public final synchronized Map<String,String> getCommitData() {
    return segmentInfos.getUserData();
  }
  
  // Used only by commit and prepareCommit, below; lock
  // order is commitLock -> IW
  private final Object commitLock = new Object();

  /**
   * <p>Commits all pending changes (added & deleted
   * documents, segment merges, added
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
   */
  @Override
  public final void commit() throws IOException {
    ensureOpen();
    commitInternal(config.getMergePolicy());
  }

  /** Returns true if there may be changes that have not been
   *  committed.  There are cases where this may return true
   *  when there are no actual "real" changes to the index,
   *  for example if you've deleted by Term or Query but
   *  that Term or Query does not match any documents.
   *  Also, if a merge kicked off as a result of flushing a
   *  new segment during {@link #commit}, or a concurrent
   *  merged finished, this method may return true right
   *  after you had just called {@link #commit}. */
  public final boolean hasUncommittedChanges() {
    return changeCount != lastCommitChangeCount || docWriter.anyChanges() || bufferedUpdatesStream.any();
  }

  private final void commitInternal(MergePolicy mergePolicy) throws IOException {

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commit: start");
    }

    synchronized(commitLock) {
      ensureOpen(false);

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "commit: enter lock");
      }

      if (pendingCommit == null) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "commit: now prepare");
        }
        prepareCommitInternal(mergePolicy);
      } else {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "commit: already prepared");
        }
      }

      finishCommit();
    }
  }

  private synchronized final void finishCommit() throws IOException {

    if (pendingCommit != null) {
      try {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "commit: pendingCommit != null");
        }
        pendingCommit.finishCommit(directory);
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "commit: wrote segments file \"" + pendingCommit.getSegmentsFileName() + "\"");
        }
        segmentInfos.updateGeneration(pendingCommit);
        lastCommitChangeCount = pendingCommitChangeCount;
        rollbackSegments = pendingCommit.createBackupSegmentInfos();
        // NOTE: don't use this.checkpoint() here, because
        // we do not want to increment changeCount:
        deleter.checkpoint(pendingCommit, true);
      } finally {
        // Matches the incRef done in prepareCommit:
        deleter.decRef(filesToCommit);
        filesToCommit = null;
        pendingCommit = null;
        notifyAll();
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", String.format(Locale.ROOT, "commit: took %.1f msec", (System.nanoTime()-startCommitTime)/1000000.0));
      }
      
    } else {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "commit: pendingCommit == null; skip");
      }
    }

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commit: done");
    }
  }

  // Ensures only one flush() is actually flushing segments
  // at a time:
  private final Object fullFlushLock = new Object();
  
  // for assert
  boolean holdsFullFlushLock() {
    return Thread.holdsLock(fullFlushLock);
  }

  /**
   * Flush all in-memory buffered updates (adds and deletes)
   * to the Directory.
   * @param triggerMerge if true, we may merge segments (if
   *  deletes or docs were flushed) if necessary
   * @param applyAllDeletes whether pending deletes should also
   */
  protected final void flush(boolean triggerMerge, boolean applyAllDeletes) throws IOException {

    // NOTE: this method cannot be sync'd because
    // maybeMerge() in turn calls mergeScheduler.merge which
    // in turn can take a long time to run and we don't want
    // to hold the lock for that.  In the case of
    // ConcurrentMergeScheduler this can lead to deadlock
    // when it stalls due to too many running merges.

    // We can be called during close, when closing==true, so we must pass false to ensureOpen:
    ensureOpen(false);
    if (doFlush(applyAllDeletes) && triggerMerge) {
      maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
    }
  }

  private boolean doFlush(boolean applyAllDeletes) throws IOException {
    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot flush");
    }

    doBeforeFlush();
    assert testPoint("startDoFlush");
    boolean success = false;
    try {

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "  start flush: applyAllDeletes=" + applyAllDeletes);
        infoStream.message("IW", "  index before flush " + segString());
      }
      final boolean anySegmentFlushed;
      
      synchronized (fullFlushLock) {
      boolean flushSuccess = false;
        try {
          anySegmentFlushed = docWriter.flushAllThreads(this);
          flushSuccess = true;
        } finally {
          docWriter.finishFullFlush(flushSuccess);
          processEvents(false, true);
        }
      }
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
      if (!success) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "hit exception during flush");
        }
      }
    }
  }
  
  final synchronized void maybeApplyDeletes(boolean applyAllDeletes) throws IOException {
    if (applyAllDeletes) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "apply all deletes during flush");
      }
      applyAllDeletesAndUpdates();
    } else if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "don't apply deletes now delTermCount=" + bufferedUpdatesStream.numTerms() + " bytesUsed=" + bufferedUpdatesStream.ramBytesUsed());
    }
  }
  
  final synchronized void applyAllDeletesAndUpdates() throws IOException {
    flushDeletesCount.incrementAndGet();
    final BufferedUpdatesStream.ApplyDeletesResult result;
    result = bufferedUpdatesStream.applyDeletesAndUpdates(readerPool, segmentInfos.asList());
    if (result.anyDeletes) {
      checkpoint();
    }
    if (!keepFullyDeletedSegments && result.allDeleted != null) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "drop 100% deleted segments: " + segString(result.allDeleted));
      }
      for (SegmentCommitInfo info : result.allDeleted) {
        // If a merge has already registered for this
        // segment, we leave it in the readerPool; the
        // merge will skip merging it and will then drop
        // it once it's done:
        if (!mergingSegments.contains(info)) {
          segmentInfos.remove(info);
          pendingNumDocs.addAndGet(-info.info.getDocCount());
          readerPool.drop(info);
        }
      }
      checkpoint();
    }
    bufferedUpdatesStream.prune(segmentInfos);
  }
  
   /** Expert:  Return the total size of all index files currently cached in memory.
    * Useful for size management with flushRamDocs()
    * @deprecated use #ramBytesUsed() instead
    */
   @Deprecated
   public final long ramSizeInBytes() {
     return ramBytesUsed();
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

  private synchronized void ensureValidMerge(MergePolicy.OneMerge merge) {
    for(SegmentCommitInfo info : merge.segments) {
      if (!segmentInfos.contains(info)) {
        throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.info.name + ") that is not in the current index " + segString(), directory);
      }
    }
  }

  private void skipDeletedDoc(DocValuesFieldUpdates.Iterator[] updatesIters, int deletedDoc) {
    for (DocValuesFieldUpdates.Iterator iter : updatesIters) {
      if (iter.doc() == deletedDoc) {
        iter.nextDoc();
      }
      // when entering the method, all iterators must already be beyond the
      // deleted document, or right on it, in which case we advance them over
      // and they must be beyond it now.
      assert iter.doc() > deletedDoc : "updateDoc=" + iter.doc() + " deletedDoc=" + deletedDoc;
    }
  }
  
  private static class MergedDeletesAndUpdates {
    ReadersAndUpdates mergedDeletesAndUpdates = null;
    MergePolicy.DocMap docMap = null;
    boolean initializedWritableLiveDocs = false;
    
    MergedDeletesAndUpdates() {}
    
    final void init(ReaderPool readerPool, MergePolicy.OneMerge merge, MergeState mergeState, boolean initWritableLiveDocs) throws IOException {
      if (mergedDeletesAndUpdates == null) {
        mergedDeletesAndUpdates = readerPool.get(merge.info, true);
        docMap = merge.getDocMap(mergeState);
        assert docMap.isConsistent(merge.info.info.getDocCount());
      }
      if (initWritableLiveDocs && !initializedWritableLiveDocs) {
        mergedDeletesAndUpdates.initWritableLiveDocs();
        this.initializedWritableLiveDocs = true;
      }
    }
    
  }
  
  private void maybeApplyMergedDVUpdates(MergePolicy.OneMerge merge, MergeState mergeState, int docUpto, 
      MergedDeletesAndUpdates holder, String[] mergingFields, DocValuesFieldUpdates[] dvFieldUpdates,
      DocValuesFieldUpdates.Iterator[] updatesIters, int curDoc) throws IOException {
    int newDoc = -1;
    for (int idx = 0; idx < mergingFields.length; idx++) {
      DocValuesFieldUpdates.Iterator updatesIter = updatesIters[idx];
      if (updatesIter.doc() == curDoc) { // document has an update
        if (holder.mergedDeletesAndUpdates == null) {
          holder.init(readerPool, merge, mergeState, false);
        }
        if (newDoc == -1) { // map once per all field updates, but only if there are any updates
          newDoc = holder.docMap.map(docUpto);
        }
        DocValuesFieldUpdates dvUpdates = dvFieldUpdates[idx];
        dvUpdates.add(newDoc, updatesIter.value());
        updatesIter.nextDoc(); // advance to next document
      } else {
        assert updatesIter.doc() > curDoc : "field=" + mergingFields[idx] + " updateDoc=" + updatesIter.doc() + " curDoc=" + curDoc;
      }
    }
  }

  /**
   * Carefully merges deletes and updates for the segments we just merged. This
   * is tricky because, although merging will clear all deletes (compacts the
   * documents) and compact all the updates, new deletes and updates may have
   * been flushed to the segments since the merge was started. This method
   * "carries over" such new deletes and updates onto the newly merged segment,
   * and saves the resulting deletes and updates files (incrementing the delete
   * and DV generations for merge.info). If no deletes were flushed, no new
   * deletes file is saved.
   */
  synchronized private ReadersAndUpdates commitMergedDeletesAndUpdates(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {

    assert testPoint("startCommitMergeDeletes");

    final List<SegmentCommitInfo> sourceSegments = merge.segments;

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commitMergeDeletes " + segString(merge.segments));
    }

    // Carefully merge deletes that occurred after we
    // started merging:
    int docUpto = 0;
    long minGen = Long.MAX_VALUE;

    // Lazy init (only when we find a delete to carry over):
    final MergedDeletesAndUpdates holder = new MergedDeletesAndUpdates();
    final DocValuesFieldUpdates.Container mergedDVUpdates = new DocValuesFieldUpdates.Container();
    
    for (int i = 0; i < sourceSegments.size(); i++) {
      SegmentCommitInfo info = sourceSegments.get(i);
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
      final int docCount = info.info.getDocCount();
      final Bits prevLiveDocs = merge.readers.get(i).getLiveDocs();
      final ReadersAndUpdates rld = readerPool.get(info, false);
      // We hold a ref so it should still be in the pool:
      assert rld != null: "seg=" + info.info.name;
      final Bits currentLiveDocs = rld.getLiveDocs();
      final Map<String,DocValuesFieldUpdates> mergingFieldUpdates = rld.getMergingFieldUpdates();
      final String[] mergingFields;
      final DocValuesFieldUpdates[] dvFieldUpdates;
      final DocValuesFieldUpdates.Iterator[] updatesIters;
      if (mergingFieldUpdates.isEmpty()) {
        mergingFields = null;
        updatesIters = null;
        dvFieldUpdates = null;
      } else {
        mergingFields = new String[mergingFieldUpdates.size()];
        dvFieldUpdates = new DocValuesFieldUpdates[mergingFieldUpdates.size()];
        updatesIters = new DocValuesFieldUpdates.Iterator[mergingFieldUpdates.size()];
        int idx = 0;
        for (Entry<String,DocValuesFieldUpdates> e : mergingFieldUpdates.entrySet()) {
          String field = e.getKey();
          DocValuesFieldUpdates updates = e.getValue();
          mergingFields[idx] = field;
          dvFieldUpdates[idx] = mergedDVUpdates.getUpdates(field, updates.type);
          if (dvFieldUpdates[idx] == null) {
            dvFieldUpdates[idx] = mergedDVUpdates.newUpdates(field, updates.type, mergeState.segmentInfo.getDocCount());
          }
          updatesIters[idx] = updates.iterator();
          updatesIters[idx].nextDoc(); // advance to first update doc
          ++idx;
        }
      }
//      System.out.println("[" + Thread.currentThread().getName() + "] IW.commitMergedDeletes: info=" + info + ", mergingUpdates=" + mergingUpdates);

      if (prevLiveDocs != null) {

        // If we had deletions on starting the merge we must
        // still have deletions now:
        assert currentLiveDocs != null;
        assert prevLiveDocs.length() == docCount;
        assert currentLiveDocs.length() == docCount;

        // There were deletes on this segment when the merge
        // started.  The merge has collapsed away those
        // deletes, but, if new deletes were flushed since
        // the merge started, we must now carefully keep any
        // newly flushed deletes but mapping them to the new
        // docIDs.

        // Since we copy-on-write, if any new deletes were
        // applied after merging has started, we can just
        // check if the before/after liveDocs have changed.
        // If so, we must carefully merge the liveDocs one
        // doc at a time:
        if (currentLiveDocs != prevLiveDocs) {
          // This means this segment received new deletes
          // since we started the merge, so we
          // must merge them:
          for (int j = 0; j < docCount; j++) {
            if (!prevLiveDocs.get(j)) {
              assert !currentLiveDocs.get(j);
            } else {
              if (!currentLiveDocs.get(j)) {
                if (holder.mergedDeletesAndUpdates == null || !holder.initializedWritableLiveDocs) {
                  holder.init(readerPool, merge, mergeState, true);
                }
                holder.mergedDeletesAndUpdates.delete(holder.docMap.map(docUpto));
                if (mergingFields != null) { // advance all iters beyond the deleted document
                  skipDeletedDoc(updatesIters, j);
                }
              } else if (mergingFields != null) {
                maybeApplyMergedDVUpdates(merge, mergeState, docUpto, holder, mergingFields, dvFieldUpdates, updatesIters, j);
              }
              docUpto++;
            }
          }
        } else if (mergingFields != null) {
          // need to check each non-deleted document if it has any updates
          for (int j = 0; j < docCount; j++) {
            if (prevLiveDocs.get(j)) {
              // document isn't deleted, check if any of the fields have an update to it
              maybeApplyMergedDVUpdates(merge, mergeState, docUpto, holder, mergingFields, dvFieldUpdates, updatesIters, j);
              // advance docUpto for every non-deleted document
              docUpto++;
            } else {
              // advance all iters beyond the deleted document
              skipDeletedDoc(updatesIters, j);
            }
          }
        } else {
          docUpto += info.info.getDocCount() - info.getDelCount() - rld.getPendingDeleteCount();
        }
      } else if (currentLiveDocs != null) {
        assert currentLiveDocs.length() == docCount;
        // This segment had no deletes before but now it
        // does:
        for (int j = 0; j < docCount; j++) {
          if (!currentLiveDocs.get(j)) {
            if (holder.mergedDeletesAndUpdates == null || !holder.initializedWritableLiveDocs) {
              holder.init(readerPool, merge, mergeState, true);
            }
            holder.mergedDeletesAndUpdates.delete(holder.docMap.map(docUpto));
            if (mergingFields != null) { // advance all iters beyond the deleted document
              skipDeletedDoc(updatesIters, j);
            }
          } else if (mergingFields != null) {
            maybeApplyMergedDVUpdates(merge, mergeState, docUpto, holder, mergingFields, dvFieldUpdates, updatesIters, j);
          }
          docUpto++;
        }
      } else if (mergingFields != null) {
        // no deletions before or after, but there were updates
        for (int j = 0; j < docCount; j++) {
          maybeApplyMergedDVUpdates(merge, mergeState, docUpto, holder, mergingFields, dvFieldUpdates, updatesIters, j);
          // advance docUpto for every non-deleted document
          docUpto++;
        }
      } else {
        // No deletes or updates before or after
        docUpto += info.info.getDocCount();
      }
    }

    assert docUpto == merge.info.info.getDocCount();

    if (mergedDVUpdates.any()) {
//      System.out.println("[" + Thread.currentThread().getName() + "] IW.commitMergedDeletes: mergedDeletes.info=" + mergedDeletes.info + ", mergedFieldUpdates=" + mergedFieldUpdates);
      boolean success = false;
      try {
        // if any error occurs while writing the field updates we should release
        // the info, otherwise it stays in the pool but is considered not "live"
        // which later causes false exceptions in pool.dropAll().
        // NOTE: currently this is the only place which throws a true
        // IOException. If this ever changes, we need to extend that try/finally
        // block to the rest of the method too.
        holder.mergedDeletesAndUpdates.writeFieldUpdates(directory, mergedDVUpdates);
        success = true;
      } finally {
        if (!success) {
          holder.mergedDeletesAndUpdates.dropChanges();
          readerPool.drop(merge.info);
        }
      }
    }
    
    if (infoStream.isEnabled("IW")) {
      if (holder.mergedDeletesAndUpdates == null) {
        infoStream.message("IW", "no new deletes or field updates since merge started");
      } else {
        String msg = holder.mergedDeletesAndUpdates.getPendingDeleteCount() + " new deletes";
        if (mergedDVUpdates.any()) {
          msg += " and " + mergedDVUpdates.size() + " new field updates";
        }
        msg += " since merge started";
        infoStream.message("IW", msg);
      }
    }

    merge.info.setBufferedDeletesGen(minGen);

    return holder.mergedDeletesAndUpdates;
  }

  synchronized private boolean commitMerge(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {

    assert testPoint("startCommitMerge");

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot complete merge");
    }

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commitMerge: " + segString(merge.segments) + " index=" + segString());
    }

    assert merge.registerDone;

    // If merge was explicitly aborted, or, if rollback() or
    // rollbackTransaction() had been called since our merge
    // started (which results in an unqualified
    // deleter.refresh() call that will remove any index
    // file that current segments does not reference), we
    // abort this merge
    if (merge.isAborted()) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "commitMerge: skip: it was aborted");
      }
      // In case we opened and pooled a reader for this
      // segment, drop it now.  This ensures that we close
      // the reader before trying to delete any of its
      // files.  This is not a very big deal, since this
      // reader will never be used by any NRT reader, and
      // another thread is currently running close(false)
      // so it will be dropped shortly anyway, but not
      // doing this  makes  MockDirWrapper angry in
      // TestNRTThreads (LUCENE-5434):
      readerPool.drop(merge.info);
      deleter.deleteNewFiles(merge.info.files());
      return false;
    }

    final ReadersAndUpdates mergedUpdates = merge.info.info.getDocCount() == 0 ? null : commitMergedDeletesAndUpdates(merge, mergeState);
//    System.out.println("[" + Thread.currentThread().getName() + "] IW.commitMerge: mergedDeletes=" + mergedDeletes);

    // If the doc store we are using has been closed and
    // is in now compound format (but wasn't when we
    // started), then we will switch to the compound
    // format as well:

    assert !segmentInfos.contains(merge.info);

    final boolean allDeleted = merge.segments.size() == 0 ||
      merge.info.info.getDocCount() == 0 ||
      (mergedUpdates != null &&
       mergedUpdates.getPendingDeleteCount() == merge.info.info.getDocCount());

    if (infoStream.isEnabled("IW")) {
      if (allDeleted) {
        infoStream.message("IW", "merged segment " + merge.info + " is 100% deleted" +  (keepFullyDeletedSegments ? "" : "; skipping insert"));
      }
    }

    final boolean dropSegment = allDeleted && !keepFullyDeletedSegments;

    // If we merged no segments then we better be dropping
    // the new segment:
    assert merge.segments.size() > 0 || dropSegment;

    assert merge.info.info.getDocCount() != 0 || keepFullyDeletedSegments || dropSegment;

    if (mergedUpdates != null) {
      boolean success = false;
      try {
        if (dropSegment) {
          mergedUpdates.dropChanges();
        }
        // Pass false for assertInfoLive because the merged
        // segment is not yet live (only below do we commit it
        // to the segmentInfos):
        readerPool.release(mergedUpdates, false);
        success = true;
      } finally {
        if (!success) {
          mergedUpdates.dropChanges();
          readerPool.drop(merge.info);
        }
      }
    }

    // Must do this after readerPool.release, in case an
    // exception is hit e.g. writing the live docs for the
    // merge segment, in which case we need to abort the
    // merge:
    segmentInfos.applyMergeChanges(merge, dropSegment);

    // Now deduct the deleted docs that we just reclaimed from this
    // merge:
    int delDocCount = merge.totalDocCount - merge.info.info.getDocCount();
    assert delDocCount >= 0;
    pendingNumDocs.addAndGet(-delDocCount);

    if (dropSegment) {
      assert !segmentInfos.contains(merge.info);
      readerPool.drop(merge.info);
      deleter.deleteNewFiles(merge.info.files());
    }

    boolean success = false;
    try {
      // Must close before checkpoint, otherwise IFD won't be
      // able to delete the held-open files from the merge
      // readers:
      closeMergeReaders(merge, false);
      success = true;
    } finally {
      // Must note the change to segmentInfos so any commits
      // in-flight don't lose it (IFD will incRef/protect the
      // new files we created):
      if (success) {
        checkpoint();
      } else {
        try {
          checkpoint();
        } catch (Throwable t) {
          // Ignore so we keep throwing original exception.
        }
      }
    }

    deleter.deletePendingFiles();

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "after commitMerge: " + segString());
    }

    if (merge.maxNumSegments != -1 && !dropSegment) {
      // cascade the forceMerge:
      if (!segmentsToMerge.containsKey(merge.info)) {
        segmentsToMerge.put(merge.info, Boolean.FALSE);
      }
    }

    return true;
  }

  final private void handleMergeException(Throwable t, MergePolicy.OneMerge merge) throws IOException {

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "handleMergeException: merge=" + segString(merge.segments) + " exc=" + t);
    }

    // Set the exception on the merge, so if
    // forceMerge is waiting on us it sees the root
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
      if (merge.isExternal) {
        throw (MergePolicy.MergeAbortedException) t;
      }
    } else {
      IOUtils.reThrow(t);
    }
  }

  /**
   * Merges the indicated segments, replacing them in the stack with a
   * single segment.
   * 
   * @lucene.experimental
   */
  public void merge(MergePolicy.OneMerge merge) throws IOException {

    boolean success = false;

    final long t0 = System.currentTimeMillis();

    final MergePolicy mergePolicy = config.getMergePolicy();
    try {
      try {
        try {
          mergeInit(merge);
          //if (merge.info != null) {
          //System.out.println("MERGE: " + merge.info.info.name);
          //}

          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "now merge\n  merge=" + segString(merge.segments) + "\n  index=" + segString());
          }

          mergeMiddle(merge, mergePolicy);
          mergeSuccess(merge);
          success = true;
        } catch (Throwable t) {
          handleMergeException(t, merge);
        }
      } finally {
        synchronized(this) {
          mergeFinish(merge);

          if (!success) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception during merge");
            }
            if (merge.info != null && !segmentInfos.contains(merge.info)) {
              deleter.refresh(merge.info.info.name);
            }
          }

          // This merge (and, generally, any change to the
          // segments) may now enable new merges, so we call
          // merge policy & update pending merges.
          if (success && !merge.isAborted() && (merge.maxNumSegments != -1 || (!closed && !closing))) {
            updatePendingMerges(mergePolicy, MergeTrigger.MERGE_FINISHED, merge.maxNumSegments);
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "merge");
    }
    if (merge.info != null && !merge.isAborted()) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "merge time " + (System.currentTimeMillis()-t0) + " msec for " + merge.info.info.getDocCount() + " docs");
      }
    }
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
  final synchronized boolean registerMerge(MergePolicy.OneMerge merge) throws IOException {

    if (merge.registerDone) {
      return true;
    }
    assert merge.segments.size() > 0;

    if (stopMerges) {
      merge.abort();
      throw new MergePolicy.MergeAbortedException("merge is aborted: " + segString(merge.segments));
    }

    boolean isExternal = false;
    for(SegmentCommitInfo info : merge.segments) {
      if (mergingSegments.contains(info)) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "reject merge " + segString(merge.segments) + ": segment " + segString(info) + " is already marked for merge");
        }
        return false;
      }
      if (!segmentInfos.contains(info)) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "reject merge " + segString(merge.segments) + ": segment " + segString(info) + " does not exist in live infos");
        }
        return false;
      }
      if (info.info.dir != directory) {
        isExternal = true;
      }
      if (segmentsToMerge.containsKey(info)) {
        merge.maxNumSegments = mergeMaxNumSegments;
      }
    }

    ensureValidMerge(merge);

    pendingMerges.add(merge);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "add merge to pendingMerges: " + segString(merge.segments) + " [total " + pendingMerges.size() + " pending]");
    }

    merge.mergeGen = mergeGen;
    merge.isExternal = isExternal;

    // OK it does not conflict; now record that this merge
    // is running (while synchronized) to avoid race
    // condition where two conflicting merges from different
    // threads, start
    if (infoStream.isEnabled("IW")) {
      StringBuilder builder = new StringBuilder("registerMerge merging= [");
      for (SegmentCommitInfo info : mergingSegments) {
        builder.append(info.info.name).append(", ");  
      }
      builder.append("]");
      // don't call mergingSegments.toString() could lead to ConcurrentModException
      // since merge updates the segments FieldInfos
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", builder.toString());  
      }
    }
    for(SegmentCommitInfo info : merge.segments) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "registerMerge info=" + segString(info));
      }
      mergingSegments.add(info);
    }

    assert merge.estimatedMergeBytes == 0;
    assert merge.totalMergeBytes == 0;
    for(SegmentCommitInfo info : merge.segments) {
      if (info.info.getDocCount() > 0) {
        final int delCount = numDeletedDocs(info);
        assert delCount <= info.info.getDocCount();
        final double delRatio = ((double) delCount)/info.info.getDocCount();
        merge.estimatedMergeBytes += info.sizeInBytes() * (1.0 - delRatio);
        merge.totalMergeBytes += info.sizeInBytes();
      }
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
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "hit exception in mergeInit");
        }
        mergeFinish(merge);
      }
    }
  }

  synchronized private void _mergeInit(MergePolicy.OneMerge merge) throws IOException {

    assert testPoint("startMergeInit");

    assert merge.registerDone;
    assert merge.maxNumSegments == -1 || merge.maxNumSegments > 0;

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot merge");
    }

    if (merge.info != null) {
      // mergeInit already done
      return;
    }

    if (merge.isAborted()) {
      return;
    }

    // TODO: in the non-pool'd case this is somewhat
    // wasteful, because we open these readers, close them,
    // and then open them again for merging.  Maybe  we
    // could pre-pool them somehow in that case...

    // Lock order: IW -> BD
    final BufferedUpdatesStream.ApplyDeletesResult result = bufferedUpdatesStream.applyDeletesAndUpdates(readerPool, merge.segments);
    
    if (result.anyDeletes) {
      checkpoint();
    }

    if (!keepFullyDeletedSegments && result.allDeleted != null) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "drop 100% deleted segments: " + result.allDeleted);
      }
      for(SegmentCommitInfo info : result.allDeleted) {
        segmentInfos.remove(info);
        pendingNumDocs.addAndGet(-info.info.getDocCount());
        if (merge.segments.contains(info)) {
          mergingSegments.remove(info);
          merge.segments.remove(info);
        }
        readerPool.drop(info);
      }
      checkpoint();
    }

    // Bind a new segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.
    final String mergeSegmentName = newSegmentName();
    SegmentInfo si = new SegmentInfo(directory, Version.LATEST, mergeSegmentName, -1, false, codec, null);
    Map<String,String> details = new HashMap<>();
    details.put("mergeMaxNumSegments", "" + merge.maxNumSegments);
    details.put("mergeFactor", Integer.toString(merge.segments.size()));
    setDiagnostics(si, SOURCE_MERGE, details);
    merge.setInfo(new SegmentCommitInfo(si, 0, -1L, -1L, -1L));

//    System.out.println("[" + Thread.currentThread().getName() + "] IW._mergeInit: " + segString(merge.segments) + " into " + si);

    // Lock order: IW -> BD
    bufferedUpdatesStream.prune(segmentInfos);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "merge seg=" + merge.info.info.name + " " + segString(merge.segments));
    }
  }

  static void setDiagnostics(SegmentInfo info, String source) {
    setDiagnostics(info, source, null);
  }

  private static void setDiagnostics(SegmentInfo info, String source, Map<String,String> details) {
    Map<String,String> diagnostics = new HashMap<>();
    diagnostics.put("source", source);
    diagnostics.put("lucene.version", Version.LATEST.toString());
    diagnostics.put("os", Constants.OS_NAME);
    diagnostics.put("os.arch", Constants.OS_ARCH);
    diagnostics.put("os.version", Constants.OS_VERSION);
    diagnostics.put("java.version", Constants.JAVA_VERSION);
    diagnostics.put("java.vendor", Constants.JAVA_VENDOR);
    diagnostics.put("timestamp", Long.toString(new Date().getTime()));
    if (details != null) {
      diagnostics.putAll(details);
    }
    info.setDiagnostics(diagnostics);
  }

  /** Does fininishing for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance. */
  final synchronized void mergeFinish(MergePolicy.OneMerge merge) {

    // forceMerge, addIndexes or waitForMerges may be waiting
    // on merges to finish.
    notifyAll();

    // It's possible we are called twice, eg if there was an
    // exception inside mergeInit
    if (merge.registerDone) {
      final List<SegmentCommitInfo> sourceSegments = merge.segments;
      for (SegmentCommitInfo info : sourceSegments) {
        mergingSegments.remove(info);
      }
      merge.registerDone = false;
    }

    runningMerges.remove(merge);
  }

  private final synchronized void closeMergeReaders(MergePolicy.OneMerge merge, boolean suppressExceptions) throws IOException {
    final int numSegments = merge.readers.size();
    Throwable th = null;

    boolean drop = !suppressExceptions;
    
    for (int i = 0; i < numSegments; i++) {
      final SegmentReader sr = merge.readers.get(i);
      if (sr != null) {
        try {
          final ReadersAndUpdates rld = readerPool.get(sr.getSegmentInfo(), false);
          // We still hold a ref so it should not have been removed:
          assert rld != null;
          if (drop) {
            rld.dropChanges();
          } else {
            rld.dropMergingUpdates();
          }
          rld.release(sr);
          readerPool.release(rld);
          if (drop) {
            readerPool.drop(rld.info);
          }
        } catch (Throwable t) {
          if (th == null) {
            th = t;
          }
        }
        merge.readers.set(i, null);
      }
    }
    
    // If any error occured, throw it.
    if (!suppressExceptions) {
      IOUtils.reThrow(th);
    }
  }

  /** Does the actual (time-consuming) work of the merge,
   *  but without holding synchronized lock on IndexWriter
   *  instance */
  private int mergeMiddle(MergePolicy.OneMerge merge, MergePolicy mergePolicy) throws IOException {

    merge.checkAborted(directory);

    final String mergedName = merge.info.info.name;

    List<SegmentCommitInfo> sourceSegments = merge.segments;
    
    IOContext context = new IOContext(merge.getMergeInfo());

    final MergeState.CheckAbort checkAbort = new MergeState.CheckAbort(merge, directory);
    final TrackingDirectoryWrapper dirWrapper = new TrackingDirectoryWrapper(directory);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "merging " + segString(merge.segments));
    }

    merge.readers = new ArrayList<>();

    // This is try/finally to make sure merger's readers are
    // closed:
    boolean success = false;
    try {
      int segUpto = 0;
      while(segUpto < sourceSegments.size()) {

        final SegmentCommitInfo info = sourceSegments.get(segUpto);

        // Hold onto the "live" reader; we will use this to
        // commit merged deletes
        final ReadersAndUpdates rld = readerPool.get(info, true);

        // Carefully pull the most recent live docs and reader
        SegmentReader reader;
        final Bits liveDocs;
        final int delCount;

        synchronized (this) {
          // Must sync to ensure BufferedDeletesStream cannot change liveDocs,
          // pendingDeleteCount and field updates while we pull a copy:
          reader = rld.getReaderForMerge(context);
          liveDocs = rld.getReadOnlyLiveDocs();
          delCount = rld.getPendingDeleteCount() + info.getDelCount();

          assert reader != null;
          assert rld.verifyDocCounts();

          if (infoStream.isEnabled("IW")) {
            if (rld.getPendingDeleteCount() != 0) {
              infoStream.message("IW", "seg=" + segString(info) + " delCount=" + info.getDelCount() + " pendingDelCount=" + rld.getPendingDeleteCount());
            } else if (info.getDelCount() != 0) {
              infoStream.message("IW", "seg=" + segString(info) + " delCount=" + info.getDelCount());
            } else {
              infoStream.message("IW", "seg=" + segString(info) + " no deletes");
            }
          }
        }

        // Deletes might have happened after we pulled the merge reader and
        // before we got a read-only copy of the segment's actual live docs
        // (taking pending deletes into account). In that case we need to
        // make a new reader with updated live docs and del count.
        if (reader.numDeletedDocs() != delCount) {
          // fix the reader's live docs and del count
          assert delCount > reader.numDeletedDocs(); // beware of zombies

          SegmentReader newReader = new SegmentReader(info, reader, liveDocs, info.info.getDocCount() - delCount);
          boolean released = false;
          try {
            rld.release(reader);
            released = true;
          } finally {
            if (!released) {
              newReader.decRef();
            }
          }

          reader = newReader;
        }

        merge.readers.add(reader);
        assert delCount <= info.info.getDocCount(): "delCount=" + delCount + " info.docCount=" + info.info.getDocCount() + " rld.pendingDeleteCount=" + rld.getPendingDeleteCount() + " info.getDelCount()=" + info.getDelCount();
        segUpto++;
      }

//      System.out.println("[" + Thread.currentThread().getName() + "] IW.mergeMiddle: merging " + merge.getMergeReaders());
      
      // we pass merge.getMergeReaders() instead of merge.readers to allow the
      // OneMerge to return a view over the actual segments to merge
      final SegmentMerger merger = new SegmentMerger(merge.getMergeReaders(),
          merge.info.info, infoStream, dirWrapper, config.getTermIndexInterval(),
          checkAbort, globalFieldNumberMap, 
          context, config.getCheckIntegrityAtMerge());

      merge.checkAborted(directory);

      // This is where all the work happens:
      MergeState mergeState;
      boolean success3 = false;
      try {
        if (!merger.shouldMerge()) {
          // would result in a 0 document segment: nothing to merge!
          mergeState = new MergeState(new ArrayList<AtomicReader>(), merge.info.info, infoStream, checkAbort);
        } else {
          mergeState = merger.merge();
        }
        success3 = true;
      } finally {
        if (!success3) {
          synchronized(this) {  
            deleter.refresh(merge.info.info.name);
          }
        }
      }
      assert mergeState.segmentInfo == merge.info.info;
      merge.info.info.setFiles(new HashSet<>(dirWrapper.getCreatedFiles()));

      // Record which codec was used to write the segment

      if (infoStream.isEnabled("IW")) {
        if (merge.info.info.getDocCount() == 0) {
          infoStream.message("IW", "merge away fully deleted segments");
        } else {
          infoStream.message("IW", "merge codec=" + codec + " docCount=" + merge.info.info.getDocCount() + "; merged segment has " +
                           (mergeState.fieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
                           (mergeState.fieldInfos.hasNorms() ? "norms" : "no norms") + "; " + 
                           (mergeState.fieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " + 
                           (mergeState.fieldInfos.hasProx() ? "prox" : "no prox") + "; " + 
                           (mergeState.fieldInfos.hasProx() ? "freqs" : "no freqs"));
        }
      }

      // Very important to do this before opening the reader
      // because codec must know if prox was written for
      // this segment:
      //System.out.println("merger set hasProx=" + merger.hasProx() + " seg=" + merge.info.name);
      boolean useCompoundFile;
      synchronized (this) { // Guard segmentInfos
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, merge.info, this);
      }

      if (useCompoundFile) {
        success = false;

        Collection<String> filesToRemove = merge.info.files();

        try {
          filesToRemove = createCompoundFile(infoStream, directory, checkAbort, merge.info.info, context);
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
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception creating compound file during merge");
            }

            synchronized(this) {
              deleter.deleteFile(IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
              deleter.deleteFile(IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
              deleter.deleteNewFiles(merge.info.files());
            }
          }
        }

        // So that, if we hit exc in deleteNewFiles (next)
        // or in commitMerge (later), we close the
        // per-segment readers in the finally clause below:
        success = false;

        synchronized(this) {

          // delete new non cfs files directly: they were never
          // registered with IFD
          deleter.deleteNewFiles(filesToRemove);

          if (merge.isAborted()) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "abort merge after building CFS");
            }
            deleter.deleteFile(IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
            deleter.deleteFile(IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
            return 0;
          }
        }

        merge.info.info.setUseCompoundFile(true);
      } else {
        // So that, if we hit exc in commitMerge (later),
        // we close the per-segment readers in the finally
        // clause below:
        success = false;
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      boolean success2 = false;
      try {
        codec.segmentInfoFormat().getSegmentInfoWriter().write(directory, merge.info.info, mergeState.fieldInfos, context);
        success2 = true;
      } finally {
        if (!success2) {
          synchronized(this) {
            deleter.deleteNewFiles(merge.info.files());
          }
        }
      }

      // TODO: ideally we would freeze merge.info here!!
      // because any changes after writing the .si will be
      // lost... 

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", String.format(Locale.ROOT, "merged segment size=%.3f MB vs estimate=%.3f MB", merge.info.sizeInBytes()/1024./1024., merge.estimatedMergeBytes/1024/1024.));
      }

      final IndexReaderWarmer mergedSegmentWarmer = config.getMergedSegmentWarmer();
      if (poolReaders && mergedSegmentWarmer != null && merge.info.info.getDocCount() != 0) {
        final ReadersAndUpdates rld = readerPool.get(merge.info, true);
        final SegmentReader sr = rld.getReader(IOContext.READ);
        try {
          mergedSegmentWarmer.warm(sr);
        } finally {
          synchronized(this) {
            rld.release(sr);
            readerPool.release(rld);
          }
        }
      }

      // Force READ context because we merge deletes onto
      // this reader:
      if (!commitMerge(merge, mergeState)) {
        // commitMerge will return false if this merge was
        // aborted
        return 0;
      }

      success = true;

    } finally {
      // Readers are already closed in commitMerge if we didn't hit
      // an exc:
      if (!success) {
        closeMergeReaders(merge, true);
      }
    }

    return merge.info.info.getDocCount();
  }

  synchronized void addMergeException(MergePolicy.OneMerge merge) {
    assert merge.getException() != null;
    if (!mergeExceptions.contains(merge) && mergeGen == merge.mergeGen) {
      mergeExceptions.add(merge);
    }
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
  synchronized SegmentCommitInfo newestSegment() {
    return segmentInfos.size() > 0 ? segmentInfos.info(segmentInfos.size()-1) : null;
  }

  /** Returns a string description of all segments, for
   *  debugging.
   *
   * @lucene.internal */
  public synchronized String segString() {
    return segString(segmentInfos);
  }

  /** Returns a string description of the specified
   *  segments, for debugging.
   *
   * @lucene.internal */
  public synchronized String segString(Iterable<SegmentCommitInfo> infos) {
    final StringBuilder buffer = new StringBuilder();
    for(final SegmentCommitInfo info : infos) {
      if (buffer.length() > 0) {
        buffer.append(' ');
      }
      buffer.append(segString(info));
    }
    return buffer.toString();
  }

  /** Returns a string description of the specified
   *  segment, for debugging.
   *
   * @lucene.internal */
  public synchronized String segString(SegmentCommitInfo info) {
    return info.toString(info.info.dir, numDeletedDocs(info) - info.getDelCount());
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
  void setKeepFullyDeletedSegments(boolean v) {
    keepFullyDeletedSegments = v;
  }

  boolean getKeepFullyDeletedSegments() {
    return keepFullyDeletedSegments;
  }

  // called only from assert
  private boolean filesExist(SegmentInfos toSync) throws IOException {
    
    Collection<String> files = toSync.files(directory, false);
    for(final String fileName: files) {
      assert slowFileExists(directory, fileName): "file " + fileName + " does not exist; files=" + Arrays.toString(directory.listAll());
      // If this trips it means we are missing a call to
      // .checkpoint somewhere, because by the time we
      // are called, deleter should know about every
      // file referenced by the current head
      // segmentInfos:
      assert deleter.exists(fileName): "IndexFileDeleter doesn't know about file " + fileName;
    }
    return true;
  }

  // For infoStream output
  synchronized SegmentInfos toLiveInfos(SegmentInfos sis) {
    final SegmentInfos newSIS = new SegmentInfos();
    final Map<SegmentCommitInfo,SegmentCommitInfo> liveSIS = new HashMap<>();
    for(SegmentCommitInfo info : segmentInfos) {
      liveSIS.put(info, info);
    }
    for(SegmentCommitInfo info : sis) {
      SegmentCommitInfo liveInfo = liveSIS.get(info);
      if (liveInfo != null) {
        info = liveInfo;
      }
      newSIS.add(info);
    }

    return newSIS;
  }

  /** Walk through all files referenced by the current
   *  segmentInfos and ask the Directory to sync each file,
   *  if it wasn't already.  If that succeeds, then we
   *  prepare a new segments_N file but do not fully commit
   *  it. */
  private void startCommit(final SegmentInfos toSync) throws IOException {

    assert testPoint("startStartCommit");
    assert pendingCommit == null;

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot commit");
    }

    try {

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "startCommit(): start");
      }

      synchronized(this) {

        assert lastCommitChangeCount <= changeCount: "lastCommitChangeCount=" + lastCommitChangeCount + " changeCount=" + changeCount;

        if (pendingCommitChangeCount == lastCommitChangeCount) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "  skip startCommit(): no changes pending");
          }
          deleter.decRef(filesToCommit);
          filesToCommit = null;
          return;
        }

        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "startCommit index=" + segString(toLiveInfos(toSync)) + " changeCount=" + changeCount);
        }

        assert filesExist(toSync);
      }

      assert testPoint("midStartCommit");

      boolean pendingCommitSet = false;

      try {

        assert testPoint("midStartCommit2");

        synchronized(this) {

          assert pendingCommit == null;

          assert segmentInfos.getGeneration() == toSync.getGeneration();

          // Exception here means nothing is prepared
          // (this method unwinds everything it did on
          // an exception)
          toSync.prepareCommit(directory);
          //System.out.println("DONE prepareCommit");

          pendingCommitSet = true;
          pendingCommit = toSync;
        }

        // This call can take a long time -- 10s of seconds
        // or more.  We do it without syncing on this:
        boolean success = false;
        final Collection<String> filesToSync;
        try {
          filesToSync = toSync.files(directory, false);
          directory.sync(filesToSync);
          success = true;
        } finally {
          if (!success) {
            pendingCommitSet = false;
            pendingCommit = null;
            toSync.rollbackCommit(directory);
          }
        }

        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "done all syncs: " + filesToSync);
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
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception committing segments file");
            }

            // Hit exception
            deleter.decRef(filesToCommit);
            filesToCommit = null;
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
    directory.makeLock(IndexWriter.WRITE_LOCK_NAME).close();
  }

  /** If {@link DirectoryReader#open(IndexWriter,boolean)} has
   *  been called (ie, this writer is in near real-time
   *  mode), then after a merge completes, this class can be
   *  invoked to warm the reader on the newly merged
   *  segment, before the merge commits.  This is not
   *  required for near real-time search, but will reduce
   *  search latency on opening a new near real-time reader
   *  after a merge completes.
   *
   * @lucene.experimental
   *
   * <p><b>NOTE</b>: warm is called before any deletes have
   * been carried over to the merged segment. */
  public static abstract class IndexReaderWarmer {

    /** Sole constructor. (For invocation by subclass 
     *  constructors, typically implicit.) */
    protected IndexReaderWarmer() {
    }

    /** Invoked on the {@link AtomicReader} for the newly
     *  merged segment, before that segment is made visible
     *  to near-real-time readers. */
    public abstract void warm(AtomicReader reader) throws IOException;
  }

  private void handleOOM(OutOfMemoryError oom, String location) {
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "hit OutOfMemoryError inside " + location);
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
  private final boolean testPoint(String message) {
    if (infoStream.isEnabled("TP")) {
      infoStream.message("TP", message);
    }
    return true;
  }

  synchronized boolean nrtIsCurrent(SegmentInfos infos) {
    //System.out.println("IW.nrtIsCurrent " + (infos.version == segmentInfos.version && !docWriter.anyChanges() && !bufferedDeletesStream.any()));
    ensureOpen();
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "nrtIsCurrent: infoVersion matches: " + (infos.version == segmentInfos.version) + "; DW changes: " + docWriter.anyChanges() + "; BD changes: "+ bufferedUpdatesStream.any());
    }
    return infos.version == segmentInfos.version && !docWriter.anyChanges() && !bufferedUpdatesStream.any();
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
    ensureOpen(false);
    deleter.deletePendingFiles();
    deleter.revisitPolicy();
  }

  private synchronized void deletePendingFiles() throws IOException {
    deleter.deletePendingFiles();
  }
  
  /**
   * NOTE: this method creates a compound file for all files returned by
   * info.files(). While, generally, this may include separate norms and
   * deletion files, this SegmentInfo must not reference such files when this
   * method is called, because they are not allowed within a compound file.
   */
  static final Collection<String> createCompoundFile(InfoStream infoStream, Directory directory, CheckAbort checkAbort, final SegmentInfo info, IOContext context)
          throws IOException {

    final String fileName = IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "create compound file " + fileName);
    }
    assert Lucene3xSegmentInfoFormat.getDocStoreOffset(info) == -1;
    // Now merge all added files
    Collection<String> files = info.files();
    CompoundFileDirectory cfsDir = new CompoundFileDirectory(directory, fileName, context, true);
    boolean success = false;
    try {
      for (String file : files) {
        directory.copy(cfsDir, file, file, context);
        checkAbort.work(directory.fileLength(file));
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(cfsDir);
      } else {
        IOUtils.closeWhileHandlingException(cfsDir);
        try {
          directory.deleteFile(fileName);
        } catch (Throwable t) {
        }
        try {
          directory.deleteFile(IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
        } catch (Throwable t) {
        }
      }
    }

    // Replace all previous files with the CFS/CFE files:
    Set<String> siFiles = new HashSet<>();
    siFiles.add(fileName);
    siFiles.add(IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
    info.setFiles(siFiles);

    return files;
  }
  
  /**
   * Tries to delete the given files if unreferenced
   * @param files the files to delete
   * @throws IOException if an {@link IOException} occurs
   * @see IndexFileDeleter#deleteNewFiles(Collection)
   */
  synchronized final void deleteNewFiles(Collection<String> files) throws IOException {
    deleter.deleteNewFiles(files);
  }
  
  /**
   * Cleans up residuals from a segment that could not be entirely flushed due to an error
   * @see IndexFileDeleter#refresh(String) 
   */
  synchronized final void flushFailed(SegmentInfo info) throws IOException {
    deleter.refresh(info.name);
  }
  
  final int purge(boolean forced) throws IOException {
    return docWriter.purgeBuffer(this, forced);
  }

  final void applyDeletesAndPurge(boolean forcePurge) throws IOException {
    try {
      purge(forcePurge);
    } finally {
      applyAllDeletesAndUpdates();
      flushCount.incrementAndGet();
    }
  }
  
  final void doAfterSegmentFlushed(boolean triggerMerge, boolean forcePurge) throws IOException {
    try {
      purge(forcePurge);
    } finally {
      if (triggerMerge) {
        maybeMerge(config.getMergePolicy(), MergeTrigger.SEGMENT_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
      }
    }
    
  }
  
  synchronized void incRefDeleter(SegmentInfos segmentInfos) throws IOException {
    ensureOpen();
    deleter.incRef(segmentInfos, false);
  }
  
  synchronized void decRefDeleter(SegmentInfos segmentInfos) throws IOException {
    ensureOpen();
    deleter.decRef(segmentInfos);
  }
  
  private boolean processEvents(boolean triggerMerge, boolean forcePurge) throws IOException {
    return processEvents(eventQueue, triggerMerge, forcePurge);
  }
  
  private boolean processEvents(Queue<Event> queue, boolean triggerMerge, boolean forcePurge) throws IOException {
    Event event;
    boolean processed = false;
    while((event = queue.poll()) != null)  {
      processed = true;
      event.process(this, triggerMerge, forcePurge);
    }
    return processed;
  }
  
  /**
   * Interface for internal atomic events. See {@link DocumentsWriter} for details. Events are executed concurrently and no order is guaranteed.
   * Each event should only rely on the serializeability within it's process method. All actions that must happen before or after a certain action must be
   * encoded inside the {@link #process(IndexWriter, boolean, boolean)} method.
   *
   */
  static interface Event {
    
    /**
     * Processes the event. This method is called by the {@link IndexWriter}
     * passed as the first argument.
     * 
     * @param writer
     *          the {@link IndexWriter} that executes the event.
     * @param triggerMerge
     *          <code>false</code> iff this event should not trigger any segment merges
     * @param clearBuffers
     *          <code>true</code> iff this event should clear all buffers associated with the event.
     * @throws IOException
     *           if an {@link IOException} occurs
     */
    void process(IndexWriter writer, boolean triggerMerge, boolean clearBuffers) throws IOException;
  }

  /** Used only by asserts: returns true if the file exists
   *  (can be opened), false if it cannot be opened, and
   *  (unlike Java's File.exists) throws IOException if
   *  there's some unexpected error. */
  private static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.openInput(fileName, IOContext.DEFAULT).close();
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }

  /** Anything that will add N docs to the index should reserve first to
   *  make sure it's allowed.  This will throw {@code
   *  IllegalStateException} if it's not allowed. */ 
  private void reserveDocs(int numDocs) {
    if (pendingNumDocs.addAndGet(numDocs) > actualMaxDocs) {
      // Reserve failed
      pendingNumDocs.addAndGet(-numDocs);
      throw new IllegalStateException("number of documents in the index cannot exceed " + actualMaxDocs);
    }
  }
}
