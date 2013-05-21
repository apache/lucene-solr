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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.codecs.lucene3x.Lucene3xSegmentInfoFormat;
import org.apache.lucene.index.FieldInfos.FieldNumbers;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MergePolicy.MergeTrigger;
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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

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
public class IndexWriter implements Closeable, TwoPhaseCommit {
  
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
   * longer than this length, it is skipped and a message is
   * printed to infoStream, if set (see {@link
   * IndexWriterConfig#setInfoStream(InfoStream)}).
   */
  public final static int MAX_TERM_LENGTH = DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8;
  volatile private boolean hitOOM;

  private final Directory directory;  // where this index resides
  private final Analyzer analyzer;    // how to analyze text

  private volatile long changeCount; // increments every time a change is completed
  private long lastCommitChangeCount; // last changeCount that was committed

  private List<SegmentInfoPerCommit> rollbackSegments;      // list of segmentInfo we will fallback to if the commit fails

  volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
  volatile long pendingCommitChangeCount;

  private Collection<String> filesToCommit;

  final SegmentInfos segmentInfos;       // the segments
  final FieldNumbers globalFieldNumberMap;

  private DocumentsWriter docWriter;
  final IndexFileDeleter deleter;

  // used by forceMerge to note those needing merging
  private Map<SegmentInfoPerCommit,Boolean> segmentsToMerge = new HashMap<SegmentInfoPerCommit,Boolean>();
  private int mergeMaxNumSegments;

  private Lock writeLock;

  private volatile boolean closed;
  private volatile boolean closing;

  // Holds all SegmentInfo instances currently involved in
  // merges
  private HashSet<SegmentInfoPerCommit> mergingSegments = new HashSet<SegmentInfoPerCommit>();

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
  private final LiveIndexWriterConfig config;

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
          anySegmentFlushed = docWriter.flushAllThreads();
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
          doAfterFlush();
        }
      }
      if (anySegmentFlushed) {
        maybeMerge(MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
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

  /** Holds shared SegmentReader instances. IndexWriter uses
   *  SegmentReaders for 1) applying deletes, 2) doing
   *  merges, 3) handing out a real-time reader.  This pool
   *  reuses instances of the SegmentReaders in all these
   *  places if it is in "near real-time mode" (getReader()
   *  has been called on this instance). */

  class ReaderPool {
    
    private final Map<SegmentInfoPerCommit,ReadersAndLiveDocs> readerMap = new HashMap<SegmentInfoPerCommit,ReadersAndLiveDocs>();

    // used only by asserts
    public synchronized boolean infoIsLive(SegmentInfoPerCommit info) {
      int idx = segmentInfos.indexOf(info);
      assert idx != -1: "info=" + info + " isn't live";
      assert segmentInfos.info(idx) == info: "info=" + info + " doesn't match live info in segmentInfos";
      return true;
    }

    public synchronized void drop(SegmentInfoPerCommit info) throws IOException {
      final ReadersAndLiveDocs rld = readerMap.get(info);
      if (rld != null) {
        assert info == rld.info;
        readerMap.remove(info);
        rld.dropReaders();
      }
    }

    public synchronized boolean anyPendingDeletes() {
      for(ReadersAndLiveDocs rld : readerMap.values()) {
        if (rld.getPendingDeleteCount() != 0) {
          return true;
        }
      }

      return false;
    }

    public synchronized void release(ReadersAndLiveDocs rld) throws IOException {

      // Matches incRef in get:
      rld.decRef();

      // Pool still holds a ref:
      assert rld.refCount() >= 1;

      if (!poolReaders && rld.refCount() == 1) {
        // This is the last ref to this RLD, and we're not
        // pooling, so remove it:
        if (rld.writeLiveDocs(directory)) {
          // Make sure we only write del docs for a live segment:
          assert infoIsLive(rld.info);
          // Must checkpoint w/ deleter, because we just
          // created created new _X_N.del file.
          deleter.checkpoint(segmentInfos, false);
        }

        rld.dropReaders();
        readerMap.remove(rld.info);
      }
    }

    /** Remove all our references to readers, and commits
     *  any pending changes. */
    synchronized void dropAll(boolean doSave) throws IOException {
      Throwable priorE = null;
      final Iterator<Map.Entry<SegmentInfoPerCommit,ReadersAndLiveDocs>> it = readerMap.entrySet().iterator();
      while(it.hasNext()) {
        final ReadersAndLiveDocs rld = it.next().getValue();

        try {
          if (doSave && rld.writeLiveDocs(directory)) {
            // Make sure we only write del docs for a live segment:
            assert infoIsLive(rld.info);
            // Must checkpoint w/ deleter, because we just
            // created created new _X_N.del file.
            deleter.checkpoint(segmentInfos, false);
          }
        } catch (Throwable t) {
          if (priorE != null) {
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
          if (priorE != null) {
            priorE = t;
          }
        }
      }
      assert readerMap.size() == 0;
      if (priorE != null) {
        throw new RuntimeException(priorE);
      }
    }

    /**
     * Commit live docs changes for the segment readers for
     * the provided infos.
     *
     * @throws IOException If there is a low-level I/O error
     */
    public synchronized void commit(SegmentInfos infos) throws IOException {
      for (SegmentInfoPerCommit info : infos) {
        final ReadersAndLiveDocs rld = readerMap.get(info);
        if (rld != null) {
          assert rld.info == info;
          if (rld.writeLiveDocs(directory)) {
            // Make sure we only write del docs for a live segment:
            assert infoIsLive(info);
            // Must checkpoint w/ deleter, because we just
            // created created new _X_N.del file.
            deleter.checkpoint(segmentInfos, false);
          }
        }
      }
    }

    /**
     * Obtain a ReadersAndLiveDocs instance from the
     * readerPool.  If create is true, you must later call
     * {@link #release(ReadersAndLiveDocs)}.
     */
    public synchronized ReadersAndLiveDocs get(SegmentInfoPerCommit info, boolean create) {

      assert info.info.dir == directory: "info.dir=" + info.info.dir + " vs " + directory;

      ReadersAndLiveDocs rld = readerMap.get(info);
      if (rld == null) {
        if (!create) {
          return null;
        }
        rld = new ReadersAndLiveDocs(IndexWriter.this, info);
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
      Set<String> seen = new HashSet<String>();
      for(SegmentInfoPerCommit info : readerMap.keySet()) {
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
  public int numDeletedDocs(SegmentInfoPerCommit info) {
    ensureOpen(false);
    int delCount = info.getDelCount();

    final ReadersAndLiveDocs rld = readerPool.get(info, false);
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
   * @throws IOException
   *           if the directory cannot be read/written to, or if it does not
   *           exist and <code>conf.getOpenMode()</code> is
   *           <code>OpenMode.APPEND</code> or if there is any other low-level
   *           IO error
   */
  public IndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
    config = new LiveIndexWriterConfig(conf.clone());
    directory = d;
    analyzer = config.getAnalyzer();
    infoStream = config.getInfoStream();
    mergePolicy = config.getMergePolicy();
    mergePolicy.setIndexWriter(this);
    mergeScheduler = config.getMergeScheduler();
    codec = config.getCodec();

    bufferedDeletesStream = new BufferedDeletesStream(infoStream);
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
      docWriter = new DocumentsWriter(codec, config, directory, this, globalFieldNumberMap, bufferedDeletesStream);

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
        try {
          writeLock.release();
        } catch (Throwable t) {
          // don't mask the original exception
        }
        writeLock = null;
      }
    }
  }

  private FieldInfos getFieldInfos(SegmentInfo info) throws IOException {
    Directory cfsDir = null;
    try {
      if (info.getUseCompoundFile()) {
        cfsDir = new CompoundFileDirectory(info.dir,
                                           IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION),
                                           IOContext.READONCE,
                                           false);
      } else {
        cfsDir = info.dir;
      }
      return info.getCodec().fieldInfosFormat().getFieldInfosReader().read(cfsDir,
                                                                                info.name,
                                                                                IOContext.READONCE);
    } finally {
      if (info.getUseCompoundFile() && cfsDir != null) {
        cfsDir.close();
      }
    }
  }

  /**
   * Loads or returns the already loaded the global field number map for this {@link SegmentInfos}.
   * If this {@link SegmentInfos} has no global field number map the returned instance is empty
   */
  private FieldNumbers getFieldNumberMap() throws IOException {
    final FieldNumbers map = new FieldNumbers();

    for(SegmentInfoPerCommit info : segmentInfos) {
      for(FieldInfo fi : getFieldInfos(info.info)) {
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
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "\ndir=" + directory + "\n" +
            "index=" + segString() + "\n" +
            "version=" + Constants.LUCENE_VERSION + "\n" +
            config.toString());
    }
  }

  /**
   * Commits all changes to an index, waits for pending merges
   * to complete, and closes all associated files.  
   * <p>
   * This is a "slow graceful shutdown" which may take a long time
   * especially if a big merge is pending: If you only want to close
   * resources use {@link #rollback()}. If you only want to commit
   * pending changes and close resources see {@link #close(boolean)}.
   * <p>
   * Note that this may be a costly
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
   * <pre class="prettyprint">
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
   * @throws IOException if there is a low-level IO error
   */
  @Override
  public void close() throws IOException {
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
  public void close(boolean waitForMerges) throws IOException {

    // Ensure that only one thread actually gets to do the
    // closing, and make sure no commit is also in progress:
    synchronized(commitLock) {
      if (shouldClose()) {
        // If any methods have hit OutOfMemoryError, then abort
        // on close, in case the internal state of IndexWriter
        // or DocumentsWriter is corrupt
        if (hitOOM) {
          rollbackInternal();
        } else {
          closeInternal(waitForMerges, true);
        }
      }
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
          docWriter.abort(); // already closed -- never sync on IW 
        }
        
      } finally {
        try {
          // clean up merge scheduler in all cases, although flushing may have failed:
          interrupted = Thread.interrupted();
        
          if (waitForMerges) {
            try {
              // Give merge scheduler last chance to run, in case
              // any pending merges are waiting:
              mergeScheduler.merge(this);
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
                finishMerges(waitForMerges && !interrupted);
                break;
              } catch (ThreadInterruptedException tie) {
                // by setting the interrupted status, the
                // next call to finishMerges will pass false,
                // so it will not wait
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
          IOUtils.closeWhileHandlingException(mergePolicy, mergeScheduler);
        }
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now call final commit()");
      }

      if (doFlush) {
        commitInternal();
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "at close: " + segString());
      }
      // used by assert below
      final DocumentsWriter oldWriter = docWriter;
      synchronized(this) {
        readerPool.dropAll(true);
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
      assert oldWriter.perThreadPool.numDeactivatedThreadStates() == oldWriter.perThreadPool.getMaxThreadStates();
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
  public synchronized int numDocs() {
    ensureOpen();
    int count;
    if (docWriter != null)
      count = docWriter.getNumDocs();
    else
      count = 0;

    for (final SegmentInfoPerCommit info : segmentInfos) {
      count += info.info.getDocCount() - numDeletedDocs(info);
    }
    return count;
  }

  /**
   * Returns true if this index has deletions (including buffered deletions).
   */
  public synchronized boolean hasDeletions() {
    ensureOpen();
    if (bufferedDeletesStream.any()) {
      return true;
    }
    if (docWriter.anyDeletions()) {
      return true;
    }
    if (readerPool.anyPendingDeletes()) {
      return true;
    }
    for (final SegmentInfoPerCommit info : segmentInfos) {
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
      boolean anySegmentFlushed = false;
      try {
        anySegmentFlushed = docWriter.updateDocuments(docs, analyzer, delTerm);
        success = true;
      } finally {
        if (!success) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit exception updating document");
          }
        }
      }
      if (anySegmentFlushed) {
        maybeMerge(MergeTrigger.SEGMENT_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
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
  public void deleteDocuments(Term term) throws IOException {
    ensureOpen();
    try {
      docWriter.deleteTerms(term);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Term)");
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
   *  reader you must use the other deleteDocument methods
   *  (e.g., {@link #deleteDocuments(Term)}). */
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
      
    final SegmentInfoPerCommit info = ((SegmentReader) reader).getSegmentInfo();

    // TODO: this is a slow linear search, but, number of
    // segments should be contained unless something is
    // seriously wrong w/ the index, so it should be a minor
    // cost:

    if (segmentInfos.indexOf(info) != -1) {
      ReadersAndLiveDocs rld = readerPool.get(info, false);
      if (rld != null) {
        synchronized(bufferedDeletesStream) {
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
  public void deleteDocuments(Query query) throws IOException {
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
  public void deleteDocuments(Query... queries) throws IOException {
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
      boolean anySegmentFlushed = false;
      try {
        anySegmentFlushed = docWriter.updateDocument(doc, analyzer, term);
        success = true;
      } finally {
        if (!success) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit exception updating document");
          }
        }
      }

      if (anySegmentFlushed) {
        maybeMerge(MergeTrigger.SEGMENT_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
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
      for(SegmentInfoPerCommit info : segmentInfos) {
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

    maybeMerge(MergeTrigger.EXPLICIT, maxNumSegments);

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
                IOException err = new IOException("background merge hit exception: " + merge.segString(directory));
                final Throwable t = merge.getException();
                if (t != null)
                  err.initCause(t);
                throw err;
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

    MergePolicy.MergeSpecification spec;

    synchronized(this) {
      spec = mergePolicy.findForcedDeletesMerges(segmentInfos);
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
    maybeMerge(MergeTrigger.EXPLICIT, UNBOUNDED_MAX_MERGE_SEGMENTS);
  }

  private final void maybeMerge(MergeTrigger trigger, int maxNumSegments) throws IOException {
    ensureOpen(false);
    updatePendingMerges(trigger, maxNumSegments);
    mergeScheduler.merge(this);
  }

  private synchronized void updatePendingMerges(MergeTrigger trigger, int maxNumSegments)
    throws IOException {
    assert maxNumSegments == -1 || maxNumSegments > 0;
    assert trigger != null;
    if (stopMerges) {
      return;
    }

    // Do not start new merges if we've hit OOME
    if (hitOOM) {
      return;
    }

    final MergePolicy.MergeSpecification spec;
    if (maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS) {
      assert trigger == MergeTrigger.EXPLICIT || trigger == MergeTrigger.MERGE_FINISHED :
        "Expected EXPLICT or MERGE_FINISHED as trigger even with maxNumSegments set but was: " + trigger.name();
      spec = mergePolicy.findForcedMerges(segmentInfos, maxNumSegments, Collections.unmodifiableMap(segmentsToMerge));
      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++) {
          final MergePolicy.OneMerge merge = spec.merges.get(i);
          merge.maxNumSegments = maxNumSegments;
        }
      }

    } else {
      spec = mergePolicy.findMerges(trigger, segmentInfos);
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
  public synchronized Collection<SegmentInfoPerCommit> getMergingSegments() {
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
    ensureOpen();

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
        finishMerges(false);
        stopMerges = true;
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "rollback: done finish merges");
      }

      // Must pre-close these two, in case they increment
      // changeCount so that we can then set it to false
      // before calling closeInternal
      mergePolicy.close();
      mergeScheduler.close();

      bufferedDeletesStream.clear();
      docWriter.close(); // mark it as closed first to prevent subsequent indexing actions/flushes 
      docWriter.abort(); // don't sync on IW here
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
      }

      success = true;
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "rollbackInternal");
    } finally {
      synchronized(this) {
        if (!success) {
          closing = false;
          notifyAll();
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit exception during rollback");
          }
        }
      }
    }

    closeInternal(false, false);
  }

  /**
   * Delete all documents in the index.
   *
   * <p>This method will drop all buffered documents and will
   *    remove all segments from the index. This change will not be
   *    visible until a {@link #commit()} has been called. This method
   *    can be rolled back using {@link #rollback()}.</p>
   *
   * <p>NOTE: this method is much faster than using deleteDocuments( new MatchAllDocsQuery() ). 
   *    Yet, this method also has different semantics compared to {@link #deleteDocuments(Query)} 
   *    / {@link #deleteDocuments(Query...)} since internal data-structures are cleared as well 
   *    as all segment information is forcefully dropped anti-viral semantics like omitting norms
   *    are reset or doc value types are cleared. Essentially a call to {@link #deleteAll()} is equivalent
   *    to creating a new {@link IndexWriter} with {@link OpenMode#CREATE} which a delete query only marks
   *    documents as deleted.</p>
   *
   * <p>NOTE: this method will forcefully abort all merges
   *    in progress.  If other threads are running {@link
   *    #forceMerge}, {@link #addIndexes(IndexReader[])} or
   *    {@link #forceMergeDeletes} methods, they may receive
   *    {@link MergePolicy.MergeAbortedException}s.
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
        docWriter.lockAndAbortAll();
        synchronized (this) {
          try {
            // Abort any running merges
            finishMerges(false);
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
        docWriter.unlockAllAfterAbortAll();
      }
    }
  }

  private synchronized void finishMerges(boolean waitForMerges) {
    if (!waitForMerges) {

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
    ensureOpen(false);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "waitForMerges");
    }
    while(pendingMerges.size() > 0 || runningMerges.size() > 0) {
      doWait();
    }

    // sanity check
    assert 0 == mergingSegments.size();

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "waitForMerges done");
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

  /** Called internally if any index state has changed. */
  synchronized void changed() {
    changeCount++;
    segmentInfos.changed();
  }

  synchronized void publishFrozenDeletes(FrozenBufferedDeletes packet) {
    assert packet != null && packet.any();
    synchronized (bufferedDeletesStream) {
      bufferedDeletesStream.push(packet);
    }
  }
  
  /**
   * Atomically adds the segment private delete packet and publishes the flushed
   * segments SegmentInfo to the index writer.
   */
  synchronized void publishFlushedSegment(SegmentInfoPerCommit newSegment,
      FrozenBufferedDeletes packet, FrozenBufferedDeletes globalPacket) throws IOException {
    // Lock order IW -> BDS
    synchronized (bufferedDeletesStream) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "publishFlushedSegment");
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
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "publish sets newSegment delGen=" + nextGen + " seg=" + segString(newSegment));
      }
      newSegment.setBufferedDeletesGen(nextGen);
      segmentInfos.add(newSegment);
      checkpoint();
    }
  }

  synchronized boolean useCompoundFile(SegmentInfoPerCommit segmentInfo) throws IOException {
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
   * starting index (see {@link #forceMerge(int)} for details).
   *
   * <p>
   * <b>NOTE:</b> this method only copies the segments of the incoming indexes
   * and does not merge them. Therefore deleted documents are not removed and
   * the new segments are not merged with the existing ones.
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
  public void addIndexes(Directory... dirs) throws IOException {
    ensureOpen();

    noDupDirs(dirs);

    try {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "flush at addIndexes(Directory...)");
      }

      flush(false, true);

      List<SegmentInfoPerCommit> infos = new ArrayList<SegmentInfoPerCommit>();
      boolean success = false;
      try {
        for (Directory dir : dirs) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "addIndexes: process directory " + dir);
          }
          SegmentInfos sis = new SegmentInfos(); // read infos from dir
          sis.read(dir);
          final Set<String> dsFilesCopied = new HashSet<String>();
          final Map<String, String> dsNames = new HashMap<String, String>();
          final Set<String> copiedFiles = new HashSet<String>();
          for (SegmentInfoPerCommit info : sis) {
            assert !infos.contains(info): "dup info dir=" + info.info.dir + " name=" + info.info.name;

            String newSegName = newSegmentName();

            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "addIndexes: process segment origName=" + info.info.name + " newName=" + newSegName + " info=" + info);
            }

            IOContext context = new IOContext(new MergeInfo(info.info.getDocCount(), info.sizeInBytes(), true, -1));

            for(FieldInfo fi : getFieldInfos(info.info)) {
              globalFieldNumberMap.addOrGet(fi.name, fi.number, fi.getDocValuesType());
            }
            infos.add(copySegmentAsIs(info, newSegName, dsNames, dsFilesCopied, context, copiedFiles));
          }
        }
        success = true;
      } finally {
        if (!success) {
          for(SegmentInfoPerCommit sipc : infos) {
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
          success = true;
        } finally {
          if (!success) {
            for(SegmentInfoPerCommit sipc : infos) {
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

    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addIndexes(Directory...)");
    }
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
      final List<AtomicReader> mergeReaders = new ArrayList<AtomicReader>();
      for (IndexReader indexReader : readers) {
        numDocs += indexReader.numDocs();
        for (AtomicReaderContext ctx : indexReader.leaves()) {
          mergeReaders.add(ctx.reader());
        }
      }
      final IOContext context = new IOContext(new MergeInfo(numDocs, -1, true, -1));

      // TODO: somehow we should fix this merge so it's
      // abortable so that IW.close(false) is able to stop it
      TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(directory);

      SegmentInfo info = new SegmentInfo(directory, Constants.LUCENE_MAIN_VERSION, mergedName, -1,
                                         false, codec, null, null);

      SegmentMerger merger = new SegmentMerger(mergeReaders, info, infoStream, trackingDir, config.getTermIndexInterval(),
                                               MergeState.CheckAbort.NONE, globalFieldNumberMap, context);

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

      SegmentInfoPerCommit infoPerCommit = new SegmentInfoPerCommit(info, 0, -1L);

      info.setFiles(new HashSet<String>(trackingDir.getCreatedFiles()));
      trackingDir.getCreatedFiles().clear();
                                         
      setDiagnostics(info, SOURCE_ADDINDEXES_READERS);

      boolean useCompoundFile;
      synchronized(this) { // Guard segmentInfos
        if (stopMerges) {
          deleter.deleteNewFiles(infoPerCommit.files());
          return;
        }
        ensureOpen();
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, infoPerCommit);
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
  }

  /** Copies the segment files as-is into the IndexWriter's directory. */
  private SegmentInfoPerCommit copySegmentAsIs(SegmentInfoPerCommit info, String segName,
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
    FieldInfos fis = getFieldInfos(info.info);
    
    Set<String> docStoreFiles3xOnly = Lucene3xCodec.getDocStoreFiles(info.info);

    final Map<String,String> attributes;
    // copy the attributes map, we might modify it below.
    // also we need to ensure its read-write, since we will invoke the SIwriter (which might want to set something).
    if (info.info.attributes() == null) {
      attributes = new HashMap<String,String>();
    } else {
      attributes = new HashMap<String,String>(info.info.attributes());
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
                                          info.info.getUseCompoundFile(),
                                          info.info.getCodec(), info.info.getDiagnostics(), attributes);
    SegmentInfoPerCommit newInfoPerCommit = new SegmentInfoPerCommit(newInfo, info.getDelCount(), info.getDelGen());

    Set<String> segFiles = new HashSet<String>();

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
    try {
      newInfo.getCodec().segmentInfoFormat().getSegmentInfoWriter().write(trackingDir, newInfo, fis, context);
    } catch (UnsupportedOperationException uoe) {
      // OK: 3x codec cannot write a new SI file;
      // SegmentInfos will write this on commit
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

        assert !directory.fileExists(newFileName): "file \"" + newFileName + "\" already exists; siFiles=" + siFiles;
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
    prepareCommitInternal();
  }

  private void prepareCommitInternal() throws IOException {
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
          maybeMerge(MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
        }
        success = true;
      } finally {
        if (!success) {
          synchronized (this) {
            deleter.decRef(filesToCommit);
            filesToCommit = null;
          }
        }
      }

      startCommit(toCommit);
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
    segmentInfos.setUserData(new HashMap<String,String>(commitUserData));
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
    commitInternal();
  }

  private final void commitInternal() throws IOException {

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
        prepareCommitInternal();
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
        lastCommitChangeCount = pendingCommitChangeCount;
        segmentInfos.updateGeneration(pendingCommit);
        rollbackSegments = pendingCommit.createBackupSegmentInfos();
        deleter.checkpoint(pendingCommit, true);
      } finally {
        // Matches the incRef done in prepareCommit:
        deleter.decRef(filesToCommit);
        filesToCommit = null;
        pendingCommit = null;
        notifyAll();
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
      maybeMerge(MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
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
          anySegmentFlushed = docWriter.flushAllThreads();
          flushSuccess = true;
        } finally {
          docWriter.finishFullFlush(flushSuccess);
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
      applyAllDeletes();
    } else if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "don't apply deletes now delTermCount=" + bufferedDeletesStream.numTerms() + " bytesUsed=" + bufferedDeletesStream.bytesUsed());
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
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "drop 100% deleted segments: " + segString(result.allDeleted));
      }
      for (SegmentInfoPerCommit info : result.allDeleted) {
        // If a merge has already registered for this
        // segment, we leave it in the readerPool; the
        // merge will skip merging it and will then drop
        // it once it's done:
        if (!mergingSegments.contains(info)) {
          segmentInfos.remove(info);
          readerPool.drop(info);
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

  private synchronized void ensureValidMerge(MergePolicy.OneMerge merge) {
    for(SegmentInfoPerCommit info : merge.segments) {
      if (!segmentInfos.contains(info)) {
        throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.info.name + ") that is not in the current index " + segString(), directory);
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
  synchronized private ReadersAndLiveDocs commitMergedDeletes(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {

    assert testPoint("startCommitMergeDeletes");

    final List<SegmentInfoPerCommit> sourceSegments = merge.segments;

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commitMergeDeletes " + segString(merge.segments));
    }

    // Carefully merge deletes that occurred after we
    // started merging:
    int docUpto = 0;
    long minGen = Long.MAX_VALUE;

    // Lazy init (only when we find a delete to carry over):
    ReadersAndLiveDocs mergedDeletes = null;
    MergePolicy.DocMap docMap = null;

    for(int i=0; i < sourceSegments.size(); i++) {
      SegmentInfoPerCommit info = sourceSegments.get(i);
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
      final int docCount = info.info.getDocCount();
      final Bits prevLiveDocs = merge.readers.get(i).getLiveDocs();
      final Bits currentLiveDocs;
      final ReadersAndLiveDocs rld = readerPool.get(info, false);
      // We hold a ref so it should still be in the pool:
      assert rld != null: "seg=" + info.info.name;
      currentLiveDocs = rld.getLiveDocs();

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
          for(int j=0;j<docCount;j++) {
            if (!prevLiveDocs.get(j)) {
              assert !currentLiveDocs.get(j);
            } else {
              if (!currentLiveDocs.get(j)) {
                if (mergedDeletes == null) {
                  mergedDeletes = readerPool.get(merge.info, true);
                  mergedDeletes.initWritableLiveDocs();
                  docMap = merge.getDocMap(mergeState);
                  assert docMap.isConsistent(merge.info.info.getDocCount());
                }
                mergedDeletes.delete(docMap.map(docUpto));
              }
              docUpto++;
            }
          }
        } else {
          docUpto += info.info.getDocCount() - info.getDelCount() - rld.getPendingDeleteCount();
        }
      } else if (currentLiveDocs != null) {
        assert currentLiveDocs.length() == docCount;
        // This segment had no deletes before but now it
        // does:
        for(int j=0; j<docCount; j++) {
          if (!currentLiveDocs.get(j)) {
            if (mergedDeletes == null) {
              mergedDeletes = readerPool.get(merge.info, true);
              mergedDeletes.initWritableLiveDocs();
              docMap = merge.getDocMap(mergeState);
              assert docMap.isConsistent(merge.info.info.getDocCount());
            }
            mergedDeletes.delete(docMap.map(docUpto));
          }
          docUpto++;
        }
      } else {
        // No deletes before or after
        docUpto += info.info.getDocCount();
      }
    }

    assert docUpto == merge.info.info.getDocCount();

    if (infoStream.isEnabled("IW")) {
      if (mergedDeletes == null) {
        infoStream.message("IW", "no new deletes since merge started");
      } else {
        infoStream.message("IW", mergedDeletes.getPendingDeleteCount() + " new deletes since merge started");
      }
    }

    merge.info.setBufferedDeletesGen(minGen);

    return mergedDeletes;
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
      deleter.deleteNewFiles(merge.info.files());
      return false;
    }

    final ReadersAndLiveDocs mergedDeletes =  merge.info.info.getDocCount() == 0 ? null : commitMergedDeletes(merge, mergeState);

    assert mergedDeletes == null || mergedDeletes.getPendingDeleteCount() != 0;

    // If the doc store we are using has been closed and
    // is in now compound format (but wasn't when we
    // started), then we will switch to the compound
    // format as well:

    assert !segmentInfos.contains(merge.info);

    final boolean allDeleted = merge.segments.size() == 0 ||
      merge.info.info.getDocCount() == 0 ||
      (mergedDeletes != null &&
       mergedDeletes.getPendingDeleteCount() == merge.info.info.getDocCount());

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

    segmentInfos.applyMergeChanges(merge, dropSegment);

    if (mergedDeletes != null) {
      if (dropSegment) {
        mergedDeletes.dropChanges();
      }
      readerPool.release(mergedDeletes);
    }

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
  public void merge(MergePolicy.OneMerge merge) throws IOException {

    boolean success = false;

    final long t0 = System.currentTimeMillis();

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
            updatePendingMerges(MergeTrigger.MERGE_FINISHED, merge.maxNumSegments);
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
    for(SegmentInfoPerCommit info : merge.segments) {
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
      for (SegmentInfoPerCommit info : mergingSegments) {
        builder.append(info.info.name).append(", ");  
      }
      builder.append("]");
      // don't call mergingSegments.toString() could lead to ConcurrentModException
      // since merge updates the segments FieldInfos
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", builder.toString());  
      }
    }
    for(SegmentInfoPerCommit info : merge.segments) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "registerMerge info=" + segString(info));
      }
      mergingSegments.add(info);
    }

    assert merge.estimatedMergeBytes == 0;
    assert merge.totalMergeBytes == 0;
    for(SegmentInfoPerCommit info : merge.segments) {
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
    final BufferedDeletesStream.ApplyDeletesResult result = bufferedDeletesStream.applyDeletes(readerPool, merge.segments);

    if (result.anyDeletes) {
      checkpoint();
    }

    if (!keepFullyDeletedSegments && result.allDeleted != null) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "drop 100% deleted segments: " + result.allDeleted);
      }
      for(SegmentInfoPerCommit info : result.allDeleted) {
        segmentInfos.remove(info);
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
    SegmentInfo si = new SegmentInfo(directory, Constants.LUCENE_MAIN_VERSION, mergeSegmentName, -1, false, codec, null, null);
    Map<String,String> details = new HashMap<String,String>();
    details.put("mergeMaxNumSegments", "" + merge.maxNumSegments);
    details.put("mergeFactor", Integer.toString(merge.segments.size()));
    setDiagnostics(si, SOURCE_MERGE, details);
    merge.setInfo(new SegmentInfoPerCommit(si, 0, -1L));

    // Lock order: IW -> BD
    bufferedDeletesStream.prune(segmentInfos);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "merge seg=" + merge.info.info.name + " " + segString(merge.segments));
    }
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
    diagnostics.put("timestamp", Long.toString(new Date().getTime()));
    if (details != null) {
      diagnostics.putAll(details);
    }
    info.setDiagnostics(diagnostics);
  }

  /** Does fininishing for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance. */
  final synchronized void mergeFinish(MergePolicy.OneMerge merge) {

    // forceMerge, addIndexes or finishMerges may be waiting
    // on merges to finish.
    notifyAll();

    // It's possible we are called twice, eg if there was an
    // exception inside mergeInit
    if (merge.registerDone) {
      final List<SegmentInfoPerCommit> sourceSegments = merge.segments;
      for(SegmentInfoPerCommit info : sourceSegments) {
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
          final ReadersAndLiveDocs rld = readerPool.get(sr.getSegmentInfo(), false);
          // We still hold a ref so it should not have been removed:
          assert rld != null;
          if (drop) {
            rld.dropChanges();
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
    if (!suppressExceptions && th != null) {
      if (th instanceof IOException) throw (IOException) th;
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      throw new RuntimeException(th);
    }
  }

  /** Does the actual (time-consuming) work of the merge,
   *  but without holding synchronized lock on IndexWriter
   *  instance */
  private int mergeMiddle(MergePolicy.OneMerge merge) throws IOException {

    merge.checkAborted(directory);

    final String mergedName = merge.info.info.name;

    List<SegmentInfoPerCommit> sourceSegments = merge.segments;
    
    IOContext context = new IOContext(merge.getMergeInfo());

    final MergeState.CheckAbort checkAbort = new MergeState.CheckAbort(merge, directory);
    final TrackingDirectoryWrapper dirWrapper = new TrackingDirectoryWrapper(directory);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "merging " + segString(merge.segments));
    }

    merge.readers = new ArrayList<SegmentReader>();

    // This is try/finally to make sure merger's readers are
    // closed:
    boolean success = false;
    try {
      int segUpto = 0;
      while(segUpto < sourceSegments.size()) {

        final SegmentInfoPerCommit info = sourceSegments.get(segUpto);

        // Hold onto the "live" reader; we will use this to
        // commit merged deletes
        final ReadersAndLiveDocs rld = readerPool.get(info, true);
        SegmentReader reader = rld.getMergeReader(context);
        assert reader != null;

        // Carefully pull the most recent live docs:
        final Bits liveDocs;
        final int delCount;

        synchronized(this) {
          // Must sync to ensure BufferedDeletesStream
          // cannot change liveDocs/pendingDeleteCount while
          // we pull a copy:
          liveDocs = rld.getReadOnlyLiveDocs();
          delCount = rld.getPendingDeleteCount() + info.getDelCount();

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

          SegmentReader newReader = new SegmentReader(info, reader.core, liveDocs, info.info.getDocCount() - delCount);
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

      // we pass merge.getMergeReaders() instead of merge.readers to allow the
      // OneMerge to return a view over the actual segments to merge
      final SegmentMerger merger = new SegmentMerger(merge.getMergeReaders(),
          merge.info.info, infoStream, dirWrapper, config.getTermIndexInterval(),
          checkAbort, globalFieldNumberMap, context);

      merge.checkAborted(directory);

      // This is where all the work happens:
      MergeState mergeState;
      boolean success3 = false;
      try {
        mergeState = merger.merge();
        success3 = true;
      } finally {
        if (!success3) {
          synchronized(this) {  
            deleter.refresh(merge.info.info.name);
          }
        }
      }
      assert mergeState.segmentInfo == merge.info.info;
      merge.info.info.setFiles(new HashSet<String>(dirWrapper.getCreatedFiles()));

      // Record which codec was used to write the segment

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "merge codec=" + codec + " docCount=" + merge.info.info.getDocCount() + "; merged segment has " +
                           (mergeState.fieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
                           (mergeState.fieldInfos.hasNorms() ? "norms" : "no norms") + "; " + 
                           (mergeState.fieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " + 
                           (mergeState.fieldInfos.hasProx() ? "prox" : "no prox") + "; " + 
                           (mergeState.fieldInfos.hasProx() ? "freqs" : "no freqs"));
      }

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
        final ReadersAndLiveDocs rld = readerPool.get(merge.info, true);
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
        // commitMerge will return false if this merge was aborted
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
  synchronized SegmentInfoPerCommit newestSegment() {
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
  public synchronized String segString(Iterable<SegmentInfoPerCommit> infos) {
    final StringBuilder buffer = new StringBuilder();
    for(final SegmentInfoPerCommit info : infos) {
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
  public synchronized String segString(SegmentInfoPerCommit info) {
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

  // For infoStream output
  synchronized SegmentInfos toLiveInfos(SegmentInfos sis) {
    final SegmentInfos newSIS = new SegmentInfos();
    final Map<SegmentInfoPerCommit,SegmentInfoPerCommit> liveSIS = new HashMap<SegmentInfoPerCommit,SegmentInfoPerCommit>();        
    for(SegmentInfoPerCommit info : segmentInfos) {
      liveSIS.put(info, info);
    }
    for(SegmentInfoPerCommit info : sis) {
      SegmentInfoPerCommit liveInfo = liveSIS.get(info);
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
    directory.makeLock(IndexWriter.WRITE_LOCK_NAME).release();
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
  boolean testPoint(String name) {
    return true;
  }

  synchronized boolean nrtIsCurrent(SegmentInfos infos) {
    //System.out.println("IW.nrtIsCurrent " + (infos.version == segmentInfos.version && !docWriter.anyChanges() && !bufferedDeletesStream.any()));
    ensureOpen();
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "nrtIsCurrent: infoVersion matches: " + (infos.version == segmentInfos.version) + "; DW changes: " + docWriter.anyChanges() + "; BD changes: "+ bufferedDeletesStream.any());
    }
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
    ensureOpen(false);
    deleter.deletePendingFiles();
    deleter.revisitPolicy();
  }

  // Called by DirectoryReader.doClose
  synchronized void deletePendingFiles() throws IOException {
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
    IOException prior = null;
    try {
      for (String file : files) {
        directory.copy(cfsDir, file, file, context);
        checkAbort.work(directory.fileLength(file));
      }
    } catch(IOException ex) {
      prior = ex;
    } finally {
      boolean success = false;
      try {
        IOUtils.closeWhileHandlingException(prior, cfsDir);
        success = true;
      } finally {
        if (!success) {
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
    }

    // Replace all previous files with the CFS/CFE files:
    Set<String> siFiles = new HashSet<String>();
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
}
