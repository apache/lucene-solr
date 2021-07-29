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
package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.index.FieldInfos.FieldNumbers;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockValidatingDirectoryWrapper;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.Version;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

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

  <a name="sequence_numbers"></a>
  <p>Each method that changes the index returns a {@code long} sequence number, which
  expresses the effective order in which each change was applied.
  {@link #commit} also returns a sequence number, describing which
  changes are in the commit point and which are not.  Sequence numbers
  are transient (not saved into the index in any way) and only valid
  within a single {@code IndexWriter} instance.</p>

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
  large RAM buffer.
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
  {@link LockObtainFailedException}.</p>
  
  <a name="deletionPolicy"></a>
  <p>Expert: <code>IndexWriter</code> allows an optional
  {@link IndexDeletionPolicy} implementation to be specified.  You
  can use this to control when prior commits are deleted from
  the index.  The default policy is {@link KeepOnlyLastCommitDeletionPolicy}
  which removes all prior commits as soon as a new commit is
  done.  Creating your own policy can allow you to explicitly
  keep previous "point in time" commits alive in the index for
  some time, either because this is useful for your application,
  or to give readers enough time to refresh to the new commit
  without having the old commit deleted out from under them.
  The latter is necessary when multiple computers take turns opening
  their own {@code IndexWriter} and {@code IndexReader}s
  against a single shared index mounted via remote filesystems
  like NFS which do not support "delete on last close" semantics.
  A single computer accessing an index via NFS is fine with the
  default deletion policy since NFS clients emulate "delete on
  last close" locally.  That said, accessing an index via NFS
  will likely result in poor performance compared to a local IO
  device. </p>

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

  <a name="OOME"></a><p><b>NOTE</b>: if you hit a
  VirtualMachineError, or disaster strikes during a checkpoint
  then IndexWriter will close itself.  This is a
  defensive measure in case any internal state (buffered
  documents, deletions, reference counts) were corrupted.  
  Any subsequent calls will throw an AlreadyClosedException.</p>

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
public class IndexWriter implements Closeable, TwoPhaseCommit, Accountable,
    MergePolicy.MergeContext {

  /** Hard limit on maximum number of documents that may be added to the
   *  index.  If you try to add more than this you'll hit {@code IllegalArgumentException}. */
  // We defensively subtract 128 to be well below the lowest
  // ArrayUtil.MAX_ARRAY_LENGTH on "typical" JVMs.  We don't just use
  // ArrayUtil.MAX_ARRAY_LENGTH here because this can vary across JVMs:
  public static final int MAX_DOCS = Integer.MAX_VALUE - 128;

  /** Maximum value of the token position in an indexed field. */
  public static final int MAX_POSITION = Integer.MAX_VALUE - 128;

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
  
  /** Used only for testing. */
  private final boolean enableTestPoints;

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
  /** Source of a segment which results from a call to {@link #addIndexes(CodecReader...)}. */
  public static final String SOURCE_ADDINDEXES_READERS = "addIndexes(CodecReader...)";

  /**
   * Absolute hard maximum length for a term, in bytes once
   * encoded as UTF8.  If a term arrives from the analyzer
   * longer than this length, an
   * <code>IllegalArgumentException</code>  is thrown
   * and a message is printed to infoStream, if set (see {@link
   * IndexWriterConfig#setInfoStream(InfoStream)}).
   */
  public final static int MAX_TERM_LENGTH =  BYTE_BLOCK_SIZE-2;

  /**
   * Maximum length string for a stored field.
   */
  public final static int MAX_STORED_STRING_LENGTH = ArrayUtil.MAX_ARRAY_LENGTH / UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR;
    
  // when unrecoverable disaster strikes, we populate this with the reason that we had to close IndexWriter
  private final AtomicReference<Throwable> tragedy = new AtomicReference<>(null);

  private final Directory directoryOrig;       // original user directory
  private final Directory directory;           // wrapped with additional checks

  private final AtomicLong changeCount = new AtomicLong(); // increments every time a change is completed
  private volatile long lastCommitChangeCount; // last changeCount that was committed

  private List<SegmentCommitInfo> rollbackSegments;      // list of segmentInfo we will fallback to if the commit fails

  private volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
  private volatile long pendingSeqNo;
  private volatile long pendingCommitChangeCount;

  private Collection<String> filesToCommit;

  private final SegmentInfos segmentInfos;
  final FieldNumbers globalFieldNumberMap;

  final DocumentsWriter docWriter;
  private final EventQueue eventQueue = new EventQueue(this);
  private final MergeScheduler.MergeSource mergeSource = new IndexWriterMergeSource(this);

  private final ReentrantLock writeDocValuesLock = new ReentrantLock();

  static final class EventQueue implements Closeable {
    private volatile boolean closed;
    // we use a semaphore here instead of simply synced methods to allow
    // events to be processed concurrently by multiple threads such that all events
    // for a certain thread are processed once the thread returns from IW
    private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
    private final Queue<Event> queue = new ConcurrentLinkedQueue<>();
    private final IndexWriter writer;

    EventQueue(IndexWriter writer) {
      this.writer = writer;
    }

    private void acquire() {
      if (permits.tryAcquire() == false) {
        throw new AlreadyClosedException("queue is closed");
      }
      if (closed) {
        permits.release();
        throw new AlreadyClosedException("queue is closed");
      }
    }

    boolean add(Event event) {
      acquire();
      try {
        return queue.add(event);
      } finally {
        permits.release();
      }
    }

    void processEvents() throws IOException {
      acquire();
      try {
        processEventsInternal();
      } finally {
        permits.release();
      }
    }

    private void processEventsInternal() throws IOException {
      assert Integer.MAX_VALUE - permits.availablePermits() > 0 : "must acquire a permit before processing events";
      Event event;
      while ((event = queue.poll()) != null) {
        event.process(writer);
      }
    }

    @Override
    public synchronized void close() throws IOException { // synced to prevent double closing
      assert closed == false : "we should never close this twice";
      closed = true;
      // it's possible that we close this queue while we are in a processEvents call
      if (writer.getTragicException() != null) {
        // we are already handling a tragic exception let's drop it all on the floor and return
        queue.clear();
      } else {
        // now we acquire all the permits to ensure we are the only one processing the queue
        try {
          permits.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
          throw new ThreadInterruptedException(e);
        }
        try {
          processEventsInternal();
        } finally {
          permits.release(Integer.MAX_VALUE);
        }
      }
    }
  }

  private final IndexFileDeleter deleter;

  // used by forceMerge to note those needing merging
  private final Map<SegmentCommitInfo,Boolean> segmentsToMerge = new HashMap<>();
  private int mergeMaxNumSegments;

  private Lock writeLock;

  private volatile boolean closed;
  private volatile boolean closing;

  private final AtomicBoolean maybeMerge = new AtomicBoolean();

  private Iterable<Map.Entry<String,String>> commitUserData;

  // Holds all SegmentInfo instances currently involved in
  // merges
  private final HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();
  private final MergeScheduler mergeScheduler;
  private final Set<SegmentMerger> runningAddIndexesMerges = new HashSet<>();
  private final LinkedList<MergePolicy.OneMerge> pendingMerges = new LinkedList<>();
  private final Set<MergePolicy.OneMerge> runningMerges = new HashSet<>();
  private final List<MergePolicy.OneMerge> mergeExceptions = new ArrayList<>();
  private long mergeGen;
  private Merges merges = new Merges();
  private boolean didMessageState;
  private final AtomicInteger flushCount = new AtomicInteger();
  private final AtomicInteger flushDeletesCount = new AtomicInteger();
  private final ReaderPool readerPool;
  private final BufferedUpdatesStream bufferedUpdatesStream;

  /** Counts how many merges have completed; this is used by {@link #forceApply(FrozenBufferedUpdates)}
   *  to handle concurrently apply deletes/updates with merges completing. */
  private final AtomicLong mergeFinishedGen = new AtomicLong();

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
  private final AtomicLong pendingNumDocs = new AtomicLong();
  private final boolean softDeletesEnabled;

  private final DocumentsWriter.FlushNotifications flushNotifications = new DocumentsWriter.FlushNotifications() {
    @Override
    public void deleteUnusedFiles(Collection<String> files) {
      eventQueue.add(w -> w.deleteNewFiles(files));
    }

    @Override
    public void flushFailed(SegmentInfo info) {
      eventQueue.add(w -> w.flushFailed(info));
    }

    @Override
    public void afterSegmentsFlushed() throws IOException {
      publishFlushedSegments(false);
    }

    @Override
    public void onTragicEvent(Throwable event, String message) {
      IndexWriter.this.onTragicEvent(event, message);
    }

    @Override
    public void onDeletesApplied() {
      eventQueue.add(w -> {
          try {
            w.publishFlushedSegments(true);
          } finally {
            flushCount.incrementAndGet();
          }
        }
      );
    }

    @Override
    public void onTicketBacklog() {
      eventQueue.add(w -> w.publishFlushedSegments(true));
    }
  };

  DirectoryReader getReader() throws IOException {
    return getReader(true, false);
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
  DirectoryReader getReader(boolean applyAllDeletes, boolean writeAllDeletes) throws IOException {
    ensureOpen();

    if (writeAllDeletes && applyAllDeletes == false) {
      throw new IllegalArgumentException("applyAllDeletes must be true when writeAllDeletes=true");
    }

    final long tStart = System.currentTimeMillis();

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "flush at getReader");
    }
    // Do this up front before flushing so that the readers
    // obtained during this flush are pooled, the first time
    // this method is called:
    readerPool.enableReaderPooling();
    StandardDirectoryReader r = null;
    doBeforeFlush();
    boolean anyChanges;
    final long maxFullFlushMergeWaitMillis = config.getMaxFullFlushMergeWaitMillis();
    /*
     * for releasing a NRT reader we must ensure that 
     * DW doesn't add any segments or deletes until we are
     * done with creating the NRT DirectoryReader. 
     * We release the two stage full flush after we are done opening the
     * directory reader!
     */
    MergePolicy.MergeSpecification onGetReaderMerges = null;
    final AtomicBoolean stopCollectingMergedReaders = new AtomicBoolean(false);
    final Map<String, SegmentReader> mergedReaders = new HashMap<>();
    final Map<String, SegmentReader> openedReadOnlyClones = new HashMap<>();
    // this function is used to control which SR are opened in order to keep track of them
    // and to reuse them in the case we wait for merges in this getReader call.
    IOUtils.IOFunction<SegmentCommitInfo, SegmentReader> readerFactory = sci -> {
      final ReadersAndUpdates rld = getPooledInstance(sci, true);
      try {
        assert Thread.holdsLock(IndexWriter.this);
        SegmentReader segmentReader = rld.getReadOnlyClone(IOContext.READ);
        if (maxFullFlushMergeWaitMillis > 0) { // only track this if we actually do fullFlush merges
          openedReadOnlyClones.put(sci.info.name, segmentReader);
        }
        return segmentReader;
      } finally {
        release(rld);
      }
    };
    Closeable onGetReaderMergeResources = null;
    SegmentInfos openingSegmentInfos = null;
    boolean success2 = false;
    try {
      /* this is the essential part of the getReader method. We need to take care of the following things:
       *  - flush all currently in-memory DWPTs to disk
       *  - apply all deletes & updates to new and to the existing DWPTs
       *  - prevent flushes and applying deletes of concurrently indexing DWPTs to be applied
       *  - open a SDR on the updated SIS
       *
       * in order to prevent concurrent flushes we call DocumentsWriter#flushAllThreads that swaps out the deleteQueue
       *  (this enforces a happens before relationship between this and the subsequent full flush) and informs the
       * FlushControl (#markForFullFlush()) that it should prevent any new DWPTs from flushing until we are \
       * done (DocumentsWriter#finishFullFlush(boolean)). All this is guarded by the fullFlushLock to prevent multiple
       * full flushes from happening concurrently. Once the DocWriter has initiated a full flush we can sequentially flush
       * and apply deletes & updates to the written segments without worrying about concurrently indexing DWPTs. The important
       * aspect is that it all happens between DocumentsWriter#flushAllThread() and DocumentsWriter#finishFullFlush(boolean)
       * since once the flush is marked as done deletes start to be applied to the segments on disk without guarantees that
       * the corresponding added documents (in the update case) are flushed and visible when opening a SDR.
       *
       */
      boolean success = false;
      synchronized (fullFlushLock) {
        try {
          // TODO: should we somehow make the seqNo available in the returned NRT reader?
          anyChanges = docWriter.flushAllThreads() < 0;
          if (anyChanges == false) {
            // prevent double increment since docWriter#doFlush increments the flushcount
            // if we flushed anything.
            flushCount.incrementAndGet();
          }
          publishFlushedSegments(true);
          processEvents(false);

          if (applyAllDeletes) {
            applyAllDeletesAndUpdates();
          }
          synchronized(this) {

            // NOTE: we cannot carry doc values updates in memory yet, so we always must write them through to disk and re-open each
            // SegmentReader:

            // TODO: we could instead just clone SIS and pull/incref readers in sync'd block, and then do this w/o IW's lock?
            // Must do this sync'd on IW to prevent a merge from completing at the last second and failing to write its DV updates:
            writeReaderPool(writeAllDeletes);

            // Prevent segmentInfos from changing while opening the
            // reader; in theory we could instead do similar retry logic,
            // just like we do when loading segments_N
            r = StandardDirectoryReader.open(this, readerFactory, segmentInfos, applyAllDeletes, writeAllDeletes);
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "return reader version=" + r.getVersion() + " reader=" + r);
            }
            if (maxFullFlushMergeWaitMillis > 0) {
              // we take the SIS from the reader which has already pruned away fully deleted readers
              // this makes pulling the readers below after the merge simpler since we can be safe that
              // they are not closed. Every segment has a corresponding SR in the SDR we opened if we use
              // this SIS
              // we need to do this rather complicated management of SRs and infos since we can't wait for merges
              // while we hold the fullFlushLock since the merge might hit a tragic event and that must not be reported
              // while holding that lock. Merging outside of the lock ie. after calling docWriter.finishFullFlush(boolean) would
              // yield wrong results because deletes might sneak in during the merge
              openingSegmentInfos = r.getSegmentInfos().clone();
              onGetReaderMerges = preparePointInTimeMerge(openingSegmentInfos, stopCollectingMergedReaders::get, MergeTrigger.GET_READER,
                  sci -> {
                    assert stopCollectingMergedReaders.get() == false : "illegal state  merge reader must be not pulled since we already stopped waiting for merges";
                    SegmentReader apply = readerFactory.apply(sci);
                    mergedReaders.put(sci.info.name, apply);
                    // we need to incRef the files of the opened SR otherwise it's possible that another merge
                    // removes the segment before we pass it on to the SDR
                    deleter.incRef(sci.files());
                  });
              onGetReaderMergeResources = () -> {
                // this needs to be closed once after we are done. In the case of an exception it releases
                // all resources, closes the merged readers and decrements the files references.
                // this only happens for readers that haven't been removed from the mergedReaders and release elsewhere
                synchronized (this) {
                  stopCollectingMergedReaders.set(true);
                  IOUtils.close(mergedReaders.values().stream().map(sr -> (Closeable) () -> {
                    try {
                      deleter.decRef(sr.getSegmentInfo().files());
                    } finally {
                      sr.close();
                    }
                  }).collect(Collectors.toList()));
                }
              };
            }
          }
          success = true;
        } finally {
          // Done: finish the full flush!
          assert Thread.holdsLock(fullFlushLock);
          docWriter.finishFullFlush(success);
          if (success) {
            processEvents(false);
            doAfterFlush();
          } else {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception during NRT reader");
            }
          }
        }
      }
      if (onGetReaderMerges != null) { // only relevant if we do merge on getReader
        StandardDirectoryReader mergedReader = finishGetReaderMerge(stopCollectingMergedReaders, mergedReaders,
            openedReadOnlyClones, openingSegmentInfos, applyAllDeletes,
            writeAllDeletes, onGetReaderMerges, maxFullFlushMergeWaitMillis);
        if (mergedReader != null) {
          try {
            r.close();
          } finally {
            r = mergedReader;
          }
        }
      }

      anyChanges |= maybeMerge.getAndSet(false);
      if (anyChanges) {
        maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
      }
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "getReader took " + (System.currentTimeMillis() - tStart) + " msec");
      }
      success2 = true;
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "getReader");
      throw tragedy;
    } finally {
      if (!success2) {
        try {
          IOUtils.closeWhileHandlingException(r, onGetReaderMergeResources);
        } finally {
          maybeCloseOnTragicEvent();
        }
      } else {
        IOUtils.close(onGetReaderMergeResources);
      }
    }
    return r;
  }

  private StandardDirectoryReader finishGetReaderMerge(AtomicBoolean stopCollectingMergedReaders, Map<String, SegmentReader> mergedReaders,
                                                       Map<String, SegmentReader> openedReadOnlyClones, SegmentInfos openingSegmentInfos,
                                                       boolean applyAllDeletes, boolean writeAllDeletes,
                                                       MergePolicy.MergeSpecification pointInTimeMerges, long maxCommitMergeWaitMillis) throws IOException {
    assert openingSegmentInfos != null;
    mergeScheduler.merge(mergeSource, MergeTrigger.GET_READER);
    pointInTimeMerges.await(maxCommitMergeWaitMillis, TimeUnit.MILLISECONDS);
    synchronized (this) {
      stopCollectingMergedReaders.set(true);
      StandardDirectoryReader reader = maybeReopenMergedNRTReader(mergedReaders, openedReadOnlyClones, openingSegmentInfos,
          applyAllDeletes, writeAllDeletes);
      IOUtils.close(mergedReaders.values());
      mergedReaders.clear();
      return reader;
    }
  }

  private StandardDirectoryReader maybeReopenMergedNRTReader(Map<String, SegmentReader> mergedReaders,
                                                             Map<String, SegmentReader> openedReadOnlyClones, SegmentInfos openingSegmentInfos,
                                                             boolean applyAllDeletes, boolean writeAllDeletes) throws IOException {
    assert Thread.holdsLock(this);
    if (mergedReaders.isEmpty() == false) {
      Collection<String> files = new ArrayList<>();
      try {
        return StandardDirectoryReader.open(this,
            sci -> {
              // as soon as we remove the reader and return it the StandardDirectoryReader#open
              // will take care of closing it. We only need to handle the readers that remain in the
              // mergedReaders map and close them.
              SegmentReader remove = mergedReaders.remove(sci.info.name);
              if (remove == null) {
                remove = openedReadOnlyClones.remove(sci.info.name);
                assert remove != null;
                // each of the readers we reuse from the previous reader needs to be incRef'd
                // since we reuse them but don't have an implicit incRef in the SDR:open call
                remove.incRef();
              } else {
                files.addAll(remove.getSegmentInfo().files());
              }
              return remove;
            }, openingSegmentInfos, applyAllDeletes, writeAllDeletes);
      } finally {
        // now the SDR#open call has incRef'd the files so we can let them go
        deleter.decRef(files);
      }
    }
    return null;
  }

  @Override
  public final long ramBytesUsed() {
    ensureOpen();
    return docWriter.ramBytesUsed();
  }

  /**
   * Returns the number of bytes currently being flushed
   */
  public final long getFlushingBytes() {
    ensureOpen();
    return docWriter.getFlushingBytes();
  }

  final void writeSomeDocValuesUpdates() throws IOException {
    if (writeDocValuesLock.tryLock()) {
      try {
        final double ramBufferSizeMB = config.getRAMBufferSizeMB();
        // If the reader pool is > 50% of our IW buffer, then write the updates:
        if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH) {
          long startNS = System.nanoTime();

          long ramBytesUsed = readerPool.ramBytesUsed();
          if (ramBytesUsed > 0.5 * ramBufferSizeMB * 1024 * 1024) {
            if (infoStream.isEnabled("BD")) {
              infoStream.message("BD", String.format(Locale.ROOT, "now write some pending DV updates: %.2f MB used vs IWC Buffer %.2f MB",
                  ramBytesUsed/1024./1024., ramBufferSizeMB));
            }

            // Sort by largest ramBytesUsed:
            final List<ReadersAndUpdates> list = readerPool.getReadersByRam();
            int count = 0;
            for (ReadersAndUpdates rld : list) {

              if (ramBytesUsed <= 0.5 * ramBufferSizeMB * 1024 * 1024) {
                break;
              }
              // We need to do before/after because not all RAM in this RAU is used by DV updates, and
              // not all of those bytes can be written here:
              long bytesUsedBefore = rld.ramBytesUsed.get();
              if (bytesUsedBefore == 0) {
                continue; // nothing to do here - lets not acquire the lock
              }
              // Only acquire IW lock on each write, since this is a time consuming operation.  This way
              // other threads get a chance to run in between our writes.
              synchronized (this) {
                // It's possible that the segment of a reader returned by readerPool#getReadersByRam
                // is dropped before being processed here. If it happens, we need to skip that reader.
                // this is also best effort to free ram, there might be some other thread writing this rld concurrently
                // which wins and then if readerPooling is off this rld will be dropped.
                if (readerPool.get(rld.info, false) == null) {
                  continue;
                }
                if (rld.writeFieldUpdates(directory, globalFieldNumberMap, bufferedUpdatesStream.getCompletedDelGen(), infoStream)) {
                  checkpointNoSIS();
                }
              }
              long bytesUsedAfter = rld.ramBytesUsed.get();
              ramBytesUsed -= bytesUsedBefore - bytesUsedAfter;
              count++;
            }

            if (infoStream.isEnabled("BD")) {
              infoStream.message("BD", String.format(Locale.ROOT, "done write some DV updates for %d segments: now %.2f MB used vs IWC Buffer %.2f MB; took %.2f sec",
                  count, readerPool.ramBytesUsed()/1024./1024., ramBufferSizeMB, ((System.nanoTime() - startNS)/1000000000.)));
            }
          }
        }
      } finally {
        writeDocValuesLock.unlock();
      }
    }
  }

  /**
   * Obtain the number of deleted docs for a pooled reader.
   * If the reader isn't being pooled, the segmentInfo's 
   * delCount is returned.
   */
  @Override
  public int numDeletedDocs(SegmentCommitInfo info) {
    ensureOpen(false);
    validate(info);
    final ReadersAndUpdates rld = getPooledInstance(info, false);
    if (rld != null) {
      return rld.getDelCount(); // get the full count from here since SCI might change concurrently
    } else {
      final int delCount = info.getDelCount(softDeletesEnabled);
      assert delCount <= info.info.maxDoc(): "delCount: " + delCount + " maxDoc: " + info.info.maxDoc();
      return delCount;
    }
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
      throw new AlreadyClosedException("this IndexWriter is closed", tragedy.get());
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

  /**
   * Constructs a new IndexWriter per the settings given in <code>conf</code>.
   * If you want to make "live" changes to this writer instance, use
   * {@link #getConfig()}.
   * 
   * <p>
   * <b>NOTE:</b> after ths writer is created, the given configuration instance
   * cannot be passed to another writer.
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
    enableTestPoints = isEnableTestPoints();
    conf.setIndexWriter(this); // prevent reuse by other instances
    config = conf;
    infoStream = config.getInfoStream();
    softDeletesEnabled = config.getSoftDeletesField() != null;
    // obtain the write.lock. If the user configured a timeout,
    // we wrap with a sleeper and this might take some time.
    writeLock = d.obtainLock(WRITE_LOCK_NAME);
    
    boolean success = false;
    try {
      directoryOrig = d;
      directory = new LockValidatingDirectoryWrapper(d, writeLock);
      mergeScheduler = config.getMergeScheduler();
      mergeScheduler.initialize(infoStream, directoryOrig);
      OpenMode mode = config.getOpenMode();
      final boolean indexExists;
      final boolean create;
      if (mode == OpenMode.CREATE) {
        indexExists = DirectoryReader.indexExists(directory);
        create = true;
      } else if (mode == OpenMode.APPEND) {
        indexExists = true;
        create = false;
      } else {
        // CREATE_OR_APPEND - create only if an index does not exist
        indexExists = DirectoryReader.indexExists(directory);
        create = !indexExists;
      }

      // If index is too old, reading the segments will throw
      // IndexFormatTooOldException.

      String[] files = directory.listAll();

      // Set up our initial SegmentInfos:
      IndexCommit commit = config.getIndexCommit();

      // Set up our initial SegmentInfos:
      StandardDirectoryReader reader;
      if (commit == null) {
        reader = null;
      } else {
        reader = commit.getReader();
      }

      if (create) {

        if (config.getIndexCommit() != null) {
          // We cannot both open from a commit point and create:
          if (mode == OpenMode.CREATE) {
            throw new IllegalArgumentException("cannot use IndexWriterConfig.setIndexCommit() with OpenMode.CREATE");
          } else {
            throw new IllegalArgumentException("cannot use IndexWriterConfig.setIndexCommit() when index has no commit");
          }
        }

        // Try to read first.  This is to allow create
        // against an index that's currently open for
        // searching.  In this case we write the next
        // segments_N file with no segments:
        final SegmentInfos sis = new SegmentInfos(config.getIndexCreatedVersionMajor());
        if (indexExists) {
          final SegmentInfos previous = SegmentInfos.readLatestCommit(directory);
          sis.updateGenerationVersionAndCounter(previous);
        }
        segmentInfos = sis;
        rollbackSegments = segmentInfos.createBackupSegmentInfos();

        // Record that we have a change (zero out all
        // segments) pending:
        changed();

      } else if (reader != null) {
        // Init from an existing already opened NRT or non-NRT reader:
      
        if (reader.directory() != commit.getDirectory()) {
          throw new IllegalArgumentException("IndexCommit's reader must have the same directory as the IndexCommit");
        }

        if (reader.directory() != directoryOrig) {
          throw new IllegalArgumentException("IndexCommit's reader must have the same directory passed to IndexWriter");
        }

        if (reader.segmentInfos.getLastGeneration() == 0) {  
          // TODO: maybe we could allow this?  It's tricky...
          throw new IllegalArgumentException("index must already have an initial commit to open from reader");
        }

        // Must clone because we don't want the incoming NRT reader to "see" any changes this writer now makes:
        segmentInfos = reader.segmentInfos.clone();

        SegmentInfos lastCommit;
        try {
          lastCommit = SegmentInfos.readCommit(directoryOrig, segmentInfos.getSegmentsFileName());
        } catch (IOException ioe) {
          throw new IllegalArgumentException("the provided reader is stale: its prior commit file \"" + segmentInfos.getSegmentsFileName() + "\" is missing from index");
        }

        if (reader.writer != null) {

          // The old writer better be closed (we have the write lock now!):
          assert reader.writer.closed;

          // In case the old writer wrote further segments (which we are now dropping),
          // update SIS metadata so we remain write-once:
          segmentInfos.updateGenerationVersionAndCounter(reader.writer.segmentInfos);
          lastCommit.updateGenerationVersionAndCounter(reader.writer.segmentInfos);
        }

        rollbackSegments = lastCommit.createBackupSegmentInfos();
      } else {
        // Init from either the latest commit point, or an explicit prior commit point:

        String lastSegmentsFile = SegmentInfos.getLastCommitSegmentsFileName(files);
        if (lastSegmentsFile == null) {
          throw new IndexNotFoundException("no segments* file found in " + directory + ": files: " + Arrays.toString(files));
        }

        // Do not use SegmentInfos.read(Directory) since the spooky
        // retrying it does is not necessary here (we hold the write lock):
        segmentInfos = SegmentInfos.readCommit(directoryOrig, lastSegmentsFile);

        if (commit != null) {
          // Swap out all segments, but, keep metadata in
          // SegmentInfos, like version & generation, to
          // preserve write-once.  This is important if
          // readers are open against the future commit
          // points.
          if (commit.getDirectory() != directoryOrig) {
            throw new IllegalArgumentException("IndexCommit's directory doesn't match my directory, expected=" + directoryOrig + ", got=" + commit.getDirectory());
          }
          
          SegmentInfos oldInfos = SegmentInfos.readCommit(directoryOrig, commit.getSegmentsFileName());
          segmentInfos.replace(oldInfos);
          changed();

          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "init: loaded commit \"" + commit.getSegmentsFileName() + "\"");
          }
        }

        rollbackSegments = segmentInfos.createBackupSegmentInfos();
      }



      commitUserData = new HashMap<>(segmentInfos.getUserData()).entrySet();

      pendingNumDocs.set(segmentInfos.totalMaxDoc());

      // start with previous field numbers, but new FieldInfos
      // NOTE: this is correct even for an NRT reader because we'll pull FieldInfos even for the un-committed segments:
      globalFieldNumberMap = getFieldNumberMap();

      validateIndexSort();

      config.getFlushPolicy().init(config);
      bufferedUpdatesStream = new BufferedUpdatesStream(infoStream);
      docWriter = new DocumentsWriter(flushNotifications, segmentInfos.getIndexCreatedVersionMajor(), pendingNumDocs,
          enableTestPoints, this::newSegmentName,
          config, directoryOrig, directory, globalFieldNumberMap);
      readerPool = new ReaderPool(directory, directoryOrig, segmentInfos, globalFieldNumberMap,
          bufferedUpdatesStream::getCompletedDelGen, infoStream, conf.getSoftDeletesField(), reader);
      if (config.getReaderPooling()) {
        readerPool.enableReaderPooling();
      }
      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:

      // Sync'd is silly here, but IFD asserts we sync'd on the IW instance:
      synchronized(this) {
        deleter = new IndexFileDeleter(files, directoryOrig, directory,
                                       config.getIndexDeletionPolicy(),
                                       segmentInfos, infoStream, this,
                                       indexExists, reader != null);

        // We incRef all files when we return an NRT reader from IW, so all files must exist even in the NRT case:
        assert create || filesExist(segmentInfos);
      }

      if (deleter.startingCommitDeleted) {
        // Deletion policy deleted the "head" commit point.
        // We have to mark ourself as changed so that if we
        // are closed w/o any further changes we write a new
        // segments_N file.
        changed();
      }

      if (reader != null) {
        // We always assume we are carrying over incoming changes when opening from reader:
        segmentInfos.changed();
        changed();
      }

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "init: create=" + create + " reader=" + reader);
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

  /** Confirms that the incoming index sort (if any) matches the existing index sort (if any).  */
  private void validateIndexSort() {
    Sort indexSort = config.getIndexSort();
    if (indexSort != null) {
      for(SegmentCommitInfo info : segmentInfos) {
        Sort segmentIndexSort = info.info.getIndexSort();
        if (segmentIndexSort == null || isCongruentSort(indexSort, segmentIndexSort) == false) {
          throw new IllegalArgumentException("cannot change previous indexSort=" + segmentIndexSort + " (from segment=" + info + ") to new indexSort=" + indexSort);
        }
      }
    }
  }

  /**
   * Returns true if <code>indexSort</code> is a prefix of <code>otherSort</code>.
   **/
  static boolean isCongruentSort(Sort indexSort, Sort otherSort) {
    final SortField[] fields1 = indexSort.getSort();
    final SortField[] fields2 = otherSort.getSort();
    if (fields1.length > fields2.length) {
      return false;
    }
    return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
  }

  // reads latest field infos for the commit
  // this is used on IW init and addIndexes(Dir) to create/update the global field map.
  // TODO: fix tests abusing this method!
  static FieldInfos readFieldInfos(SegmentCommitInfo si) throws IOException {
    Codec codec = si.info.getCodec();
    FieldInfosFormat reader = codec.fieldInfosFormat();
    
    if (si.hasFieldUpdates()) {
      // there are updates, we read latest (always outside of CFS)
      final String segmentSuffix = Long.toString(si.getFieldInfosGen(), Character.MAX_RADIX);
      return reader.read(si.info.dir, si.info, segmentSuffix, IOContext.READONCE);
    } else if (si.info.getUseCompoundFile()) {
      // cfs
      try (Directory cfs = codec.compoundFormat().getCompoundReader(si.info.dir, si.info, IOContext.DEFAULT)) {
        return reader.read(cfs, si.info, "", IOContext.READONCE);
      }
    } else {
      // no cfs
      return reader.read(si.info.dir, si.info, "", IOContext.READONCE);
    }
  }

  /**
   * Loads or returns the already loaded the global field number map for this {@link SegmentInfos}.
   * If this {@link SegmentInfos} has no global field number map the returned instance is empty
   */
  private FieldNumbers getFieldNumberMap() throws IOException {
    final FieldNumbers map = new FieldNumbers(config.softDeletesField);

    for(SegmentCommitInfo info : segmentInfos) {
      FieldInfos fis = readFieldInfos(info);
      for(FieldInfo fi : fis) {
        map.addOrGet(fi.name, fi.number, fi.getIndexOptions(), fi.getDocValuesType(), fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField());
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
      infoStream.message("IW", "\ndir=" + directoryOrig + "\n" +
            "index=" + segString() + "\n" +
            "version=" + Version.LATEST.toString() + "\n" +
            config.toString());
      final StringBuilder unmapInfo = new StringBuilder(Boolean.toString(MMapDirectory.UNMAP_SUPPORTED));
      if (!MMapDirectory.UNMAP_SUPPORTED) {
        unmapInfo.append(" (").append(MMapDirectory.UNMAP_NOT_SUPPORTED_REASON).append(")");
      }
      infoStream.message("IW", "MMapDirectory.UNMAP_SUPPORTED=" + unmapInfo);
    }
  }

  /**
   * Gracefully closes (commits, waits for merges), but calls rollback
   * if there's an exc so the IndexWriter is always closed.  This is called
   * from {@link #close} when {@link IndexWriterConfig#commitOnClose} is
   * {@code true}.
   */
  private void shutdown() throws IOException {
    if (pendingCommit != null) {
      throw new IllegalStateException("cannot close: prepareCommit was already called with no corresponding call to commit");
    }
    // Ensure that only one thread actually gets to do the
    // closing
    if (shouldClose(true)) {
      try {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "now flush at close");
        }

        flush(true, true);
        waitForMerges();
        commitInternal(config.getMergePolicy());
      } catch (Throwable t) {
        // Be certain to close the index on any exception
        try {
          rollbackInternal();
        } catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw t;
      }
      rollbackInternal(); // if we got that far lets rollback and close
    }
  }

  /**
   * Closes all open resources and releases the write lock.
   *
   * If {@link IndexWriterConfig#commitOnClose} is <code>true</code>,
   * this will attempt to gracefully shut down by writing any
   * changes, waiting for any running merges, committing, and closing.
   * In this case, note that:
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
    if (config.getCommitOnClose()) {
      shutdown();
    } else {
      rollback();
    }
  }

  // Returns true if this thread should attempt to close, or
  // false if IndexWriter is now closed; else,
  // waits until another thread finishes closing
  synchronized private boolean shouldClose(boolean waitForClose) {
    while (true) {
      if (closed == false) {
        if (closing == false) {
          // We get to close
          closing = true;
          return true;
        } else if (waitForClose == false) {
          return false;
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

  /** Returns the Directory used by this index. */
  public Directory getDirectory() {
    // return the original directory the user supplied, unwrapped.
    return directoryOrig;
  }

  @Override
  public InfoStream getInfoStream() {
    return infoStream;
  }

  /** Returns the analyzer used by this index. */
  public Analyzer getAnalyzer() {
    ensureOpen();
    return config.getAnalyzer();
  }

  /** If {@link SegmentInfos#getVersion} is below {@code newVersion} then update it to this value.
   *
   * @lucene.internal */
  public synchronized void advanceSegmentInfosVersion(long newVersion) {
    ensureOpen();
    if (segmentInfos.getVersion() < newVersion) {
      segmentInfos.setVersion(newVersion);
    }
    changed();
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
    if (bufferedUpdatesStream.any()
        || docWriter.anyDeletions()
        || readerPool.anyDeletions()) {
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
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
    return updateDocument(null, doc);
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
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    return updateDocuments((DocumentsWriterDeleteQueue.Node<?>) null, docs);
  }

  /**
   * Atomically deletes documents matching the provided
   * delTerm and adds a block of documents with sequentially
   * assigned document IDs, such that an external reader
   * will see all or none of the documents. 
   *
   * See {@link #addDocuments(Iterable)}.
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    return updateDocuments(delTerm == null ? null : DocumentsWriterDeleteQueue.newNode(delTerm), docs);
  }

  private long updateDocuments(final DocumentsWriterDeleteQueue.Node<?> delNode, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    ensureOpen();
    boolean success = false;
    try {
      final long seqNo = maybeProcessEvents(docWriter.updateDocuments(docs, delNode));
      success = true;
      return seqNo;
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "updateDocuments");
      throw tragedy;
    } finally {
      if (success == false) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "hit exception updating document");
        }
        maybeCloseOnTragicEvent();
      }
    }
  }

  /**
   * Expert:
   * Atomically updates documents matching the provided
   * term with the given doc-values fields
   * and adds a block of documents with sequentially
   * assigned document IDs, such that an external reader
   * will see all or none of the documents.
   *
   * One use of this API is to retain older versions of
   * documents instead of replacing them. The existing
   * documents can be updated to reflect they are no
   * longer current while atomically adding new documents
   * at the same time.
   *
   * In contrast to {@link #updateDocuments(Term, Iterable)}
   * this method will not delete documents in the index
   * matching the given term but instead update them with
   * the given doc-values fields which can be used as a
   * soft-delete mechanism.
   *
   * See {@link #addDocuments(Iterable)}
   * and {@link #updateDocuments(Term, Iterable)}.
   *
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public long softUpdateDocuments(Term term, Iterable<? extends Iterable<? extends IndexableField>> docs, Field... softDeletes) throws IOException {
    if (term == null) {
      throw new IllegalArgumentException("term must not be null");
    }
    if (softDeletes == null || softDeletes.length == 0) {
      throw new IllegalArgumentException("at least one soft delete must be present");
    }
    return updateDocuments(DocumentsWriterDeleteQueue.newNode(buildDocValuesUpdate(term, softDeletes)), docs);
  }

  /** Expert: attempts to delete by document ID, as long as
   *  the provided reader is a near-real-time reader (from {@link
   *  DirectoryReader#open(IndexWriter)}).  If the
   *  provided reader is an NRT reader obtained from this
   *  writer, and its segment has not been merged away, then
   *  the delete succeeds and this method returns a valid (&gt; 0) sequence
   *  number; else, it returns -1 and the caller must then
   *  separately delete by Term or Query.
   *
   *  <b>NOTE</b>: this method can only delete documents
   *  visible to the currently open NRT reader.  If you need
   *  to delete documents indexed after opening the NRT
   *  reader you must use {@link #deleteDocuments(Term...)}). */
  public synchronized long tryDeleteDocument(IndexReader readerIn, int docID) throws IOException {
    // NOTE: DON'T use docID inside the closure
    return tryModifyDocument(readerIn, docID, (leafDocId, rld) -> {
      if (rld.delete(leafDocId)) {
        if (isFullyDeleted(rld)) {
          dropDeletedSegment(rld.info);
          checkpoint();
        }

        // Must bump changeCount so if no other changes
        // happened, we still commit this change:
        changed();
      }
    });
  }

  /** Expert: attempts to update doc values by document ID, as long as
   *  the provided reader is a near-real-time reader (from {@link
   *  DirectoryReader#open(IndexWriter)}).  If the
   *  provided reader is an NRT reader obtained from this
   *  writer, and its segment has not been merged away, then
   *  the update succeeds and this method returns a valid (&gt; 0) sequence
   *  number; else, it returns -1 and the caller must then
   *  either retry the update and resolve the document again.
   *  If a doc values fields data is <code>null</code> the existing
   *  value is removed from all documents matching the term. This can be used
   *  to un-delete a soft-deleted document since this method will apply the
   *  field update even if the document is marked as deleted.
   *
   *  <b>NOTE</b>: this method can only updates documents
   *  visible to the currently open NRT reader.  If you need
   *  to update documents indexed after opening the NRT
   *  reader you must use {@link #updateDocValues(Term, Field...)}. */
  public synchronized long tryUpdateDocValue(IndexReader readerIn, int docID, Field... fields) throws IOException {
    // NOTE: DON'T use docID inside the closure
    final DocValuesUpdate[] dvUpdates = buildDocValuesUpdate(null, fields);
    return tryModifyDocument(readerIn, docID, (leafDocId, rld) -> {
      long nextGen = bufferedUpdatesStream.getNextGen();
      try {
        Map<String, DocValuesFieldUpdates> fieldUpdatesMap = new HashMap<>();
        for (DocValuesUpdate update : dvUpdates) {
          DocValuesFieldUpdates docValuesFieldUpdates = fieldUpdatesMap.computeIfAbsent(update.field, k -> {
            switch (update.type) {
              case NUMERIC:
                return new NumericDocValuesFieldUpdates(nextGen, k, rld.info.info.maxDoc());
              case BINARY:
                return new BinaryDocValuesFieldUpdates(nextGen, k, rld.info.info.maxDoc());
              default:
                throw new AssertionError("type: " + update.type + " is not supported");
            }
          });
          if (update.hasValue()) {
            switch (update.type) {
              case NUMERIC:
                docValuesFieldUpdates.add(leafDocId, ((NumericDocValuesUpdate) update).getValue());
                break;
              case BINARY:
                docValuesFieldUpdates.add(leafDocId, ((BinaryDocValuesUpdate) update).getValue());
                break;
              default:
                throw new AssertionError("type: " + update.type + " is not supported");
            }
          } else {
            docValuesFieldUpdates.reset(leafDocId);
          }
        }
        for (DocValuesFieldUpdates updates : fieldUpdatesMap.values()) {
          updates.finish();
          rld.addDVUpdate(updates);
        }
      } finally {
        bufferedUpdatesStream.finishedSegment(nextGen);
      }
      // Must bump changeCount so if no other changes
      // happened, we still commit this change:
      changed();
    });
  }

  @FunctionalInterface
  private interface DocModifier {
    void run(int docId, ReadersAndUpdates readersAndUpdates) throws IOException;
  }

  private synchronized long tryModifyDocument(IndexReader readerIn, int docID, DocModifier toApply) throws IOException {
    final LeafReader reader;
    if (readerIn instanceof LeafReader) {
      // Reader is already atomic: use the incoming docID:
      reader = (LeafReader) readerIn;
    } else {
      // Composite reader: lookup sub-reader and re-base docID:
      List<LeafReaderContext> leaves = readerIn.leaves();
      int subIndex = ReaderUtil.subIndex(docID, leaves);
      reader = leaves.get(subIndex).reader();
      docID -= leaves.get(subIndex).docBase;
      assert docID >= 0;
      assert docID < reader.maxDoc();
    }

    if (!(reader instanceof SegmentReader)) {
      throw new IllegalArgumentException("the reader must be a SegmentReader or composite reader containing only SegmentReaders");
    }

    final SegmentCommitInfo info = ((SegmentReader) reader).getOriginalSegmentInfo();

    // TODO: this is a slow linear search, but, number of
    // segments should be contained unless something is
    // seriously wrong w/ the index, so it should be a minor
    // cost:

    if (segmentInfos.indexOf(info) != -1) {
      ReadersAndUpdates rld = getPooledInstance(info, false);
      if (rld != null) {
        synchronized(bufferedUpdatesStream) {
          toApply.run(docID, rld);
          return docWriter.getNextSequenceNumber();
        }
      }
    }
    return -1;
  }

  /** Drops a segment that has 100% deleted documents. */
  private synchronized void dropDeletedSegment(SegmentCommitInfo info) throws IOException {
    // If a merge has already registered for this
    // segment, we leave it in the readerPool; the
    // merge will skip merging it and will then drop
    // it once it's done:
    if (mergingSegments.contains(info) == false) {
      // it's possible that we invoke this method more than once for the same SCI
      // we must only remove the docs once!
      boolean dropPendingDocs = segmentInfos.remove(info);
      try {
        // this is sneaky - we might hit an exception while dropping a reader but then we have already
        // removed the segment for the segmentInfo and we lost the pendingDocs update due to that.
        // therefore we execute the adjustPendingNumDocs in a finally block to account for that.
        dropPendingDocs |= readerPool.drop(info);
      } finally {
        if (dropPendingDocs) {
          adjustPendingNumDocs(-info.info.maxDoc());
        }
      }
    }
  }

  /**
   * Deletes the document(s) containing any of the
   * terms. All given deletes are applied and flushed atomically
   * at the same time.
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @param terms array of terms to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public long deleteDocuments(Term... terms) throws IOException {
    ensureOpen();
    try {
      return maybeProcessEvents(docWriter.deleteTerms(terms));
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "deleteDocuments(Term..)");
      throw tragedy;
    }
  }

  /**
   * Deletes the document(s) matching any of the provided queries.
   * All given deletes are applied and flushed atomically at the same time.
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @param queries array of queries to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public long deleteDocuments(Query... queries) throws IOException {
    ensureOpen();

    // LUCENE-6379: Specialize MatchAllDocsQuery
    for(Query query : queries) {
      if (query.getClass() == MatchAllDocsQuery.class) {
        return deleteAll();
      }
    }

    try {
      return maybeProcessEvents(docWriter.deleteQueries(queries));
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "deleteDocuments(Query..)");
      throw tragedy;
    }
  }

  /**
   * Updates a document by first deleting the document(s)
   * containing <code>term</code> and then adding the new
   * document.  The delete and then add are atomic as seen
   * by a reader on the same index (flush may happen only after
   * the add).
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @param term the term to identify the document(s) to be
   * deleted
   * @param doc the document to be added
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
    return updateDocuments(term == null ? null : DocumentsWriterDeleteQueue.newNode(term), Collections.singletonList(doc));
  }

  /**
   * Expert:
   * Updates a document by first updating the document(s)
   * containing <code>term</code> with the given doc-values fields
   * and then adding the new document.  The doc-values update and
   * then add are atomic as seen by a reader on the same index
   * (flush may happen only after the add).
   *
   * One use of this API is to retain older versions of
   * documents instead of replacing them. The existing
   * documents can be updated to reflect they are no
   * longer current while atomically adding new documents
   * at the same time.
   *
   * In contrast to {@link #updateDocument(Term, Iterable)}
   * this method will not delete documents in the index
   * matching the given term but instead update them with
   * the given doc-values fields which can be used as a
   * soft-delete mechanism.
   *
   * See {@link #addDocuments(Iterable)}
   * and {@link #updateDocuments(Term, Iterable)}.
   *
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @lucene.experimental
   */
  public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
    if (term == null) {
      throw new IllegalArgumentException("term must not be null");
    }
    if (softDeletes == null || softDeletes.length == 0) {
      throw new IllegalArgumentException("at least one soft delete must be present");
    }
    return updateDocuments(DocumentsWriterDeleteQueue.newNode(buildDocValuesUpdate(term, softDeletes)), Collections.singletonList(doc));
  }


  /**
   * Updates a document's {@link NumericDocValues} for <code>field</code> to the
   * given <code>value</code>. You can only update fields that already exist in
   * the index, not add new fields through this method.
   * 
   * @param term
   *          the term to identify the document(s) to be updated
   * @param field
   *          field name of the {@link NumericDocValues} field
   * @param value
   *          new value for the field
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public long updateNumericDocValue(Term term, String field, long value) throws IOException {
    ensureOpen();
    if (!globalFieldNumberMap.contains(field, DocValuesType.NUMERIC)) {
      throw new IllegalArgumentException("can only update existing numeric-docvalues fields!");
    }
    if (config.getIndexSortFields().contains(field)) {
      throw new IllegalArgumentException("cannot update docvalues field involved in the index sort, field=" + field + ", sort=" + config.getIndexSort());
    }
    try {
      return maybeProcessEvents(docWriter.updateDocValues(new NumericDocValuesUpdate(term, field, value)));
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "updateNumericDocValue");
      throw tragedy;
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
   * @param term
   *          the term to identify the document(s) to be updated
   * @param field
   *          field name of the {@link BinaryDocValues} field
   * @param value
   *          new value for the field
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public long updateBinaryDocValue(Term term, String field, BytesRef value) throws IOException {
    ensureOpen();
    if (value == null) {
      throw new IllegalArgumentException("cannot update a field to a null value: " + field);
    }
    if (!globalFieldNumberMap.contains(field, DocValuesType.BINARY)) {
      throw new IllegalArgumentException("can only update existing binary-docvalues fields!");
    }
    try {
      return maybeProcessEvents(docWriter.updateDocValues(new BinaryDocValuesUpdate(term, field, value)));
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "updateBinaryDocValue");
      throw tragedy;
    }
  }
  
  /**
   * Updates documents' DocValues fields to the given values. Each field update
   * is applied to the set of documents that are associated with the
   * {@link Term} to the same value. All updates are atomically applied and
   * flushed together. If a doc values fields data is <code>null</code> the existing
   * value is removed from all documents matching the term.
   *
   * 
   * @param updates
   *          the updates to apply
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   */
  public long updateDocValues(Term term, Field... updates) throws IOException {
    ensureOpen();
    DocValuesUpdate[] dvUpdates = buildDocValuesUpdate(term, updates);
    try {
      return maybeProcessEvents(docWriter.updateDocValues(dvUpdates));
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "updateDocValues");
      throw tragedy;
    }
  }

  private DocValuesUpdate[] buildDocValuesUpdate(Term term, Field[] updates) {
    DocValuesUpdate[] dvUpdates = new DocValuesUpdate[updates.length];
    for (int i = 0; i < updates.length; i++) {
      final Field f = updates[i];
      final DocValuesType dvType = f.fieldType().docValuesType();
      if (dvType == null) {
        throw new NullPointerException("DocValuesType must not be null (field: \"" + f.name() + "\")");
      }
      if (dvType == DocValuesType.NONE) {
        throw new IllegalArgumentException("can only update NUMERIC or BINARY fields! field=" + f.name());
      }
      if (globalFieldNumberMap.contains(f.name(), dvType) == false) {
        // if this field doesn't exists we try to add it. if it exists and the DV type doesn't match we
        // get a consistent error message as if you try to do that during an indexing operation.
        globalFieldNumberMap.addOrGet(f.name(), -1, IndexOptions.NONE, dvType, 0, 0, 0, f.name().equals(config.softDeletesField));
        assert globalFieldNumberMap.contains(f.name(), dvType);
      }
      if (config.getIndexSortFields().contains(f.name())) {
        throw new IllegalArgumentException("cannot update docvalues field involved in the index sort, field=" + f.name() + ", sort=" + config.getIndexSort());
      }

      switch (dvType) {
        case NUMERIC:
          Long value = (Long)f.numericValue();
          dvUpdates[i] = new NumericDocValuesUpdate(term, f.name(), value);
          break;
        case BINARY:
          dvUpdates[i] = new BinaryDocValuesUpdate(term, f.name(), f.binaryValue());
          break;
        default:
          throw new IllegalArgumentException("can only update NUMERIC or BINARY fields: field=" + f.name() + ", type=" + dvType);
      }
    }
    return dvUpdates;
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
  final synchronized int maxDoc(int i) {
    if (i >= 0 && i < segmentInfos.size()) {
      return segmentInfos.info(i).info.maxDoc();
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

  /**
   * Return an unmodifiable set of all field names as visible
   * from this IndexWriter, across all segments of the index.
   * Useful for knowing which fields exist, before {@link #updateDocValues(Term, Field...)} is
   * attempted. We could phase out this method if
   * {@link #updateDocValues(Term, Field...)} could create the non-existent
   * docValues fields as necessary, instead of throwing
   * IllegalArgumentException for attempts to update non-existent
   * docValues fields.
   * @lucene.internal
   * @lucene.experimental
   */
  public Set<String> getFieldNames() {
    return globalFieldNumberMap.getFieldNames(); // FieldNumbers#getFieldNames() returns an unmodifiableSet
  }

  private String newSegmentName() {
    // Cannot synchronize on IndexWriter because that causes
    // deadlock
    synchronized(segmentInfos) {
      // Important to increment changeCount so that the
      // segmentInfos is written on close.  Otherwise we
      // could close, re-open and re-return the same segment
      // name that was previously returned which can cause
      // problems at least with ConcurrentMergeScheduler.
      changeCount.incrementAndGet();
      segmentInfos.changed();
      return "_" + Long.toString(segmentInfos.counter++, Character.MAX_RADIX);
    }
  }

  /** If enabled, information about merges will be printed to this.
   */
  private final InfoStream infoStream;

  /**
   * Forces merge policy to merge segments until there are
   * {@code <= maxNumSegments}.  The actual merges to be
   * executed are determined by the {@link MergePolicy}.
   *
   * <p>This is a horribly costly operation, especially when
   * you pass a small {@code maxNumSegments}; usually you
   * should only call this if the index is static (will no
   * longer be changed).</p>
   *
   * <p>Note that this requires free space that is proportional
   * to the size of the index in your Directory: 2X if you are
   * not using compound file format, and 3X if you are.
   * For example, if your index size is 10 MB then you need
   * an additional 20 MB free for this to complete (30 MB if
   * you're using compound file format). This is also affected
   * by the {@link Codec} that is used to execute the merge,
   * and may result in even a bigger index. Also, it's best
   * to call {@link #commit()} afterwards, to allow IndexWriter
   * to free up disk space.</p>
   *
   * <p>If some but not all readers re-open while merging
   * is underway, this will cause {@code > 2X} temporary
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
   */
  public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
    ensureOpen();

    if (maxNumSegments < 1) {
      throw new IllegalArgumentException("maxNumSegments must be >= 1; got " + maxNumSegments);
    }

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "forceMerge: index now " + segString());
      infoStream.message("IW", "now flush at forceMerge");
    }
    flush(true, true);
    synchronized(this) {
      resetMergeExceptions();
      segmentsToMerge.clear();
      for(SegmentCommitInfo info : segmentInfos) {
        assert info != null;
        segmentsToMerge.put(info, Boolean.TRUE);
      }
      mergeMaxNumSegments = maxNumSegments;

      // Now mark all pending & running merges for forced
      // merge:
      for(final MergePolicy.OneMerge merge  : pendingMerges) {
        merge.maxNumSegments = maxNumSegments;
        if (merge.info != null) {
          // this can be null since we register the merge under lock before we then do the actual merge and
          // set the merge.info in _mergeInit
          segmentsToMerge.put(merge.info, Boolean.TRUE);
        }
      }

      for (final MergePolicy.OneMerge merge: runningMerges) {
        merge.maxNumSegments = maxNumSegments;
        if (merge.info != null) {
          // this can be null since we put the merge on runningMerges before we do the actual merge and
          // set the merge.info in _mergeInit
          segmentsToMerge.put(merge.info, Boolean.TRUE);
        }
      }
    }

    maybeMerge(config.getMergePolicy(), MergeTrigger.EXPLICIT, maxNumSegments);

    if (doWait) {
      synchronized(this) {
        while(true) {
          if (tragedy.get() != null) {
            throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete forceMerge", tragedy.get());
          }

          if (mergeExceptions.size() > 0) {
            // Forward any exceptions in background merge
            // threads to the current thread:
            final int size = mergeExceptions.size();
            for(int i=0;i<size;i++) {
              final MergePolicy.OneMerge merge = mergeExceptions.get(i);
              if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS) {
                throw new IOException("background merge hit exception: " + merge.segString(), merge.getException());
              }
            }
          }

          if (maxNumSegmentsMergesPending()) {
            testPoint("forceMergeBeforeWait");
            doWait();
          } else {
            break;
          }
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
      if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS)
        return true;
    }

    for (final MergePolicy.OneMerge merge : runningMerges) {
      if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS)
        return true;
    }

    return false;
  }

  /** Just like {@link #forceMergeDeletes()}, except you can
   *  specify whether the call should block until the
   *  operation completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads. */
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

    mergeScheduler.merge(mergeSource, MergeTrigger.EXPLICIT);

    if (spec != null && doWait) {
      final int numMerges = spec.merges.size();
      synchronized(this) {
        boolean running = true;
        while(running) {

          if (tragedy.get() != null) {
            throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete forceMergeDeletes", tragedy.get());
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
              throw new IOException("background merge hit exception: " + merge.segString(), t);
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
   */
  public final void maybeMerge() throws IOException {
    maybeMerge(config.getMergePolicy(), MergeTrigger.EXPLICIT, UNBOUNDED_MAX_MERGE_SEGMENTS);
  }

  private final void maybeMerge(MergePolicy mergePolicy, MergeTrigger trigger, int maxNumSegments) throws IOException {
    ensureOpen(false);
    if (updatePendingMerges(mergePolicy, trigger, maxNumSegments) != null) {
      executeMerge(trigger);
    }
  }

  final void executeMerge(MergeTrigger trigger) throws IOException {
    mergeScheduler.merge(mergeSource, trigger);
  }

  private synchronized MergePolicy.MergeSpecification updatePendingMerges(MergePolicy mergePolicy, MergeTrigger trigger, int maxNumSegments)
    throws IOException {

    // In case infoStream was disabled on init, but then enabled at some
    // point, try again to log the config here:
    messageState();

    assert maxNumSegments == UNBOUNDED_MAX_MERGE_SEGMENTS || maxNumSegments > 0;
    assert trigger != null;
    if (merges.areEnabled() == false) {
      return null;
    }

    // Do not start new merges if disaster struck
    if (tragedy.get() != null) {
      return null;
    }

    final MergePolicy.MergeSpecification spec;
    if (maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS) {
      assert trigger == MergeTrigger.EXPLICIT || trigger == MergeTrigger.MERGE_FINISHED :
      "Expected EXPLICT or MERGE_FINISHED as trigger even with maxNumSegments set but was: " + trigger.name();

      spec = mergePolicy.findForcedMerges(segmentInfos, maxNumSegments, Collections.unmodifiableMap(segmentsToMerge), this);
      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++) {
          final MergePolicy.OneMerge merge = spec.merges.get(i);
          merge.maxNumSegments = maxNumSegments;
        }
      }
    } else {
      switch (trigger) {
        case GET_READER:
        case COMMIT:
          spec = mergePolicy.findFullFlushMerges(trigger, segmentInfos, this);
          break;
        default:
          spec = mergePolicy.findMerges(trigger, segmentInfos, this);
      }
    }
    if (spec != null) {
      final int numMerges = spec.merges.size();
      for(int i=0;i<numMerges;i++) {
        registerMerge(spec.merges.get(i));
      }
    }
    return spec;
  }

  /** Expert: to be used by a {@link MergePolicy} to avoid
   *  selecting merges for segments already being merged.
   *  The returned collection is not cloned, and thus is
   *  only safe to access if you hold IndexWriter's lock
   *  (which you do when IndexWriter invokes the
   *  MergePolicy).
   *
   *  <p>The Set is unmodifiable. */
  public synchronized Set<SegmentCommitInfo> getMergingSegments() {
    return Collections.unmodifiableSet(mergingSegments);
  }

  /**
   * Expert: the {@link MergeScheduler} calls this method to retrieve the next
   * merge requested by the MergePolicy
   * 
   * @lucene.experimental
   */
  private synchronized MergePolicy.OneMerge getNextMerge() {
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
    if (shouldClose(true)) {
      rollbackInternal();
    }
  }

  private void rollbackInternal() throws IOException {
    // Make sure no commit is running, else e.g. we can close while another thread is still fsync'ing:
    synchronized(commitLock) {
      rollbackInternalNoCommit();

      assert pendingNumDocs.get() == segmentInfos.totalMaxDoc()
          : "pendingNumDocs " + pendingNumDocs.get() + " != " + segmentInfos.totalMaxDoc() + " totalMaxDoc";
    }
  }

  private void rollbackInternalNoCommit() throws IOException {
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "rollback");
    }
    
    try {
      synchronized (this) {
        // must be synced otherwise register merge might throw and exception if merges
        // changes concurrently, abortMerges is synced as well
        abortMerges(); // this disables merges forever since we are closing and can't reenable them
        assert mergingSegments.isEmpty() : "we aborted all merges but still have merging segments: " + mergingSegments;
      }
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "rollback: done finish merges");
      }

      // Must pre-close in case it increments changeCount so that we can then
      // set it to false before calling rollbackInternal
      mergeScheduler.close();

      docWriter.close(); // mark it as closed first to prevent subsequent indexing actions/flushes
      assert !Thread.holdsLock(this) : "IndexWriter lock should never be hold when aborting";
      docWriter.abort(); // don't sync on IW here
      docWriter.flushControl.waitForFlush(); // wait for all concurrently running flushes
      publishFlushedSegments(true); // empty the flush ticket queue otherwise we might not have cleaned up all resources
      eventQueue.close();
      synchronized (this) {

        if (pendingCommit != null) {
          pendingCommit.rollbackCommit(directory);
          try {
            deleter.decRef(pendingCommit);
          } finally {
            pendingCommit = null;
            notifyAll();
          }
        }
        final int totalMaxDoc = segmentInfos.totalMaxDoc();
        // Keep the same segmentInfos instance but replace all
        // of its SegmentInfo instances so IFD below will remove
        // any segments we flushed since the last commit:
        segmentInfos.rollbackSegmentInfos(rollbackSegments);
        int rollbackMaxDoc = segmentInfos.totalMaxDoc();
        // now we need to adjust this back to the rolled back SI but don't set it to the absolute value
        // otherwise we might hide internal bugsf
        adjustPendingNumDocs(-(totalMaxDoc - rollbackMaxDoc));
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "rollback: infos=" + segString(segmentInfos));
        }

        testPoint("rollback before checkpoint");

        // Ask deleter to locate unreferenced files & remove
        // them ... only when we are not experiencing a tragedy, else
        // these methods throw ACE:
        if (tragedy.get() == null) {
          deleter.checkpoint(segmentInfos, false);
          deleter.refresh();
          deleter.close();
        }

        lastCommitChangeCount = changeCount.get();
        // Don't bother saving any changes in our segmentInfos
        readerPool.close();
        // Must set closed while inside same sync block where we call deleter.refresh, else concurrent threads may try to sneak a flush in,
        // after we leave this sync block and before we enter the sync block in the finally clause below that sets closed:
        closed = true;

        IOUtils.close(writeLock); // release write lock
        writeLock = null;
        closed = true;
        closing = false;
        // So any "concurrently closing" threads wake up and see that the close has now completed:
        notifyAll();
      }
    } catch (Throwable throwable) {
      try {
        // Must not hold IW's lock while closing
        // mergeScheduler: this can lead to deadlock,
        // e.g. TestIW.testThreadInterruptDeadlock
        IOUtils.closeWhileHandlingException(mergeScheduler);
        synchronized (this) {
          // we tried to be nice about it: do the minimum
          // don't leak a segments_N file if there is a pending commit
          if (pendingCommit != null) {
            try {
              pendingCommit.rollbackCommit(directory);
              deleter.decRef(pendingCommit);
            } catch (Throwable t) {
              throwable.addSuppressed(t);
            }
            pendingCommit = null;
          }

          // close all the closeables we can (but important is readerPool and writeLock to prevent leaks)
          IOUtils.closeWhileHandlingException(readerPool, deleter, writeLock);
          writeLock = null;
          closed = true;
          closing = false;

          // So any "concurrently closing" threads wake up and see that the close has now completed:
          notifyAll();
        }
      } catch (Throwable t) {
        throwable.addSuppressed(t);
      } finally {
        if (throwable instanceof VirtualMachineError) {
          try {
            tragicEvent(throwable, "rollbackInternal");
          } catch (Throwable t1){
            throwable.addSuppressed(t1);
          }
        }
      }
      throw throwable;
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
   * threads are running {@link #forceMerge}, {@link #addIndexes(CodecReader[])}
   * or {@link #forceMergeDeletes} methods, they may receive
   * {@link MergePolicy.MergeAbortedException}s.
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   */
  @SuppressWarnings("try")
  public long deleteAll() throws IOException {
    ensureOpen();
    // Remove any buffered docs
    boolean success = false;
    /* hold the full flush lock to prevent concurrency commits / NRT reopens to
     * get in our way and do unnecessary work. -- if we don't lock this here we might
     * get in trouble if */
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
      synchronized (fullFlushLock) {
        try (Closeable finalizer = docWriter.lockAndAbortAll()) {
          processEvents(false);
          synchronized (this) {
            try {
              // Abort any running merges
              try {
                abortMerges();
                assert merges.areEnabled() == false : "merges should be disabled - who enabled them?";
                assert mergingSegments.isEmpty() : "found merging segments but merges are disabled: " + mergingSegments;
              } finally {
                // abortMerges disables all merges and we need to re-enable them here to make sure
                // IW can function properly. An exception in abortMerges() might be fatal for IW but just to be sure
                // lets re-enable merges anyway.
                merges.enable();
              }
              adjustPendingNumDocs(-segmentInfos.totalMaxDoc());
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
              readerPool.dropAll();
              // Mark that the index has changed
              changeCount.incrementAndGet();
              segmentInfos.changed();
              globalFieldNumberMap.clear();
              success = true;
              long seqNo = docWriter.getNextSequenceNumber();
              return seqNo;
            } finally {
              if (success == false) {

                if (infoStream.isEnabled("IW")) {
                  infoStream.message("IW", "hit exception during deleteAll");
                }
              }
            }
          }
        }
      }
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "deleteAll");
      throw tragedy;
    }
  }

  /** Aborts running merges.  Be careful when using this
   *  method: when you abort a long-running merge, you lose
   *  a lot of work that must later be redone. */
  private synchronized void abortMerges() throws IOException {
    merges.disable();
    // Abort all pending & running merges:
    IOUtils.applyToAll(pendingMerges, merge -> {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now abort pending merge " + segString(merge.segments));
      }
      abortOneMerge(merge);
      mergeFinish(merge);
    });
    pendingMerges.clear();

    for (final MergePolicy.OneMerge merge : runningMerges) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now abort running merge " + segString(merge.segments));
      }
      merge.setAborted();
    }

    // We wait here to make all merges stop.  It should not
    // take very long because they periodically check if
    // they are aborted.
    while (runningMerges.size() + runningAddIndexesMerges.size() != 0) {

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "now wait for " + runningMerges.size()
            + " running merge/s to abort; currently running addIndexes: " + runningAddIndexesMerges.size());
      }

      doWait();
    }

    notifyAll();
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
  void waitForMerges() throws IOException {

    // Give merge scheduler last chance to run, in case
    // any pending merges are waiting. We can't hold IW's lock
    // when going into merge because it can lead to deadlock.
    mergeScheduler.merge(mergeSource, MergeTrigger.CLOSING);

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
  private synchronized void checkpoint() throws IOException {
    changed();
    deleter.checkpoint(segmentInfos, false);
  }

  /** Checkpoints with IndexFileDeleter, so it's aware of
   *  new files, and increments changeCount, so on
   *  close/commit we will write a new segments file, but
   *  does NOT bump segmentInfos.version. */
  private synchronized void checkpointNoSIS() throws IOException {
    changeCount.incrementAndGet();
    deleter.checkpoint(segmentInfos, false);
  }

  /** Called internally if any index state has changed. */
  private synchronized void changed() {
    changeCount.incrementAndGet();
    segmentInfos.changed();
  }

  private synchronized long publishFrozenUpdates(FrozenBufferedUpdates packet) {
    assert packet != null && packet.any();
    long nextGen = bufferedUpdatesStream.push(packet);
    // Do this as an event so it applies higher in the stack when we are not holding DocumentsWriterFlushQueue.purgeLock:
    eventQueue.add(w -> {
      try {
        // we call tryApply here since we don't want to block if a refresh or a flush is already applying the
        // packet. The flush will retry this packet anyway to ensure all of them are applied
        tryApply(packet);
      } catch (Throwable t) {
        try {
          w.onTragicEvent(t, "applyUpdatesPacket");
        } catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw t;
      }
      w.flushDeletesCount.incrementAndGet();
    });
    return nextGen;
  }

  /**
   * Atomically adds the segment private delete packet and publishes the flushed
   * segments SegmentInfo to the index writer.
   */
  private synchronized void publishFlushedSegment(SegmentCommitInfo newSegment, FieldInfos fieldInfos,
                                                  FrozenBufferedUpdates packet, FrozenBufferedUpdates globalPacket,
                                                  Sorter.DocMap sortMap) throws IOException {
    boolean published = false;
    try {
      // Lock order IW -> BDS
      ensureOpen(false);

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "publishFlushedSegment " + newSegment);
      }

      if (globalPacket != null && globalPacket.any()) {
        publishFrozenUpdates(globalPacket);
      }

      // Publishing the segment must be sync'd on IW -> BDS to make the sure
      // that no merge prunes away the seg. private delete packet
      final long nextGen;
      if (packet != null && packet.any()) {
        nextGen = publishFrozenUpdates(packet);
      } else {
        // Since we don't have a delete packet to apply we can get a new
        // generation right away
        nextGen = bufferedUpdatesStream.getNextGen();
        // No deletes/updates here, so marked finished immediately:
        bufferedUpdatesStream.finishedSegment(nextGen);
      }
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "publish sets newSegment delGen=" + nextGen + " seg=" + segString(newSegment));
      }
      newSegment.setBufferedDeletesGen(nextGen);
      segmentInfos.add(newSegment);
      published = true;
      checkpoint();
      if (packet != null && packet.any() && sortMap != null) {
        // TODO: not great we do this heavyish op while holding IW's monitor lock,
        // but it only applies if you are using sorted indices and updating doc values:
        ReadersAndUpdates rld = getPooledInstance(newSegment, true);
        rld.sortMap = sortMap;
        // DON't release this ReadersAndUpdates we need to stick with that sortMap
      }
      FieldInfo fieldInfo = fieldInfos.fieldInfo(config.softDeletesField); // will return null if no soft deletes are present
      // this is a corner case where documents delete them-self with soft deletes. This is used to
      // build delete tombstones etc. in this case we haven't seen any updates to the DV in this fresh flushed segment.
      // if we have seen updates the update code checks if the segment is fully deleted.
      boolean hasInitialSoftDeleted = (fieldInfo != null
          && fieldInfo.getDocValuesGen() == -1
          && fieldInfo.getDocValuesType() != DocValuesType.NONE);
      final boolean isFullyHardDeleted = newSegment.getDelCount() == newSegment.info.maxDoc();
      // we either have a fully hard-deleted segment or one or more docs are soft-deleted. In both cases we need
      // to go and check if they are fully deleted. This has the nice side-effect that we now have accurate numbers
      // for the soft delete right after we flushed to disk.
      if (hasInitialSoftDeleted || isFullyHardDeleted){
        // this operation is only really executed if needed an if soft-deletes are not configured it only be executed
        // if we deleted all docs in this newly flushed segment.
        ReadersAndUpdates rld = getPooledInstance(newSegment, true);
        try {
          if (isFullyDeleted(rld)) {
            dropDeletedSegment(newSegment);
            checkpoint();
          }
        } finally {
          release(rld);
        }
      }

    } finally {
      if (published == false) {
        adjustPendingNumDocs(-newSegment.info.maxDoc());
      }
      flushCount.incrementAndGet();
      doAfterFlush();
    }

  }

  private synchronized void resetMergeExceptions() {
    mergeExceptions.clear();
    mergeGen++;
  }

  private void noDupDirs(Directory... dirs) {
    HashSet<Directory> dups = new HashSet<>();
    for(int i=0;i<dirs.length;i++) {
      if (dups.contains(dirs[i]))
        throw new IllegalArgumentException("Directory " + dirs[i] + " appears more than once");
      if (dirs[i] == directoryOrig)
        throw new IllegalArgumentException("Cannot add directory to itself");
      dups.add(dirs[i]);
    }
  }

  /** Acquires write locks on all the directories; be sure
   *  to match with a call to {@link IOUtils#close} in a
   *  finally clause. */
  private List<Lock> acquireWriteLocks(Directory... dirs) throws IOException {
    List<Lock> locks = new ArrayList<>(dirs.length);
    for(int i=0;i<dirs.length;i++) {
      boolean success = false;
      try {
        Lock lock = dirs[i].obtainLock(WRITE_LOCK_NAME);
        locks.add(lock);
        success = true;
      } finally {
        if (success == false) {
          // Release all previously acquired locks:
          // TODO: addSuppressed? it could be many...
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
   * <p>All added indexes must have been created by the same
   * Lucene version as this index.
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @throws IllegalArgumentException if addIndexes would cause
   *   the index to exceed {@link #MAX_DOCS}, or if the indoming
   *   index sort does not match this index's index sort
   */
  public long addIndexes(Directory... dirs) throws IOException {
    ensureOpen();

    noDupDirs(dirs);

    List<Lock> locks = acquireWriteLocks(dirs);

    Sort indexSort = config.getIndexSort();

    boolean successTop = false;

    long seqNo;

    try {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "flush at addIndexes(Directory...)");
      }

      flush(false, true);

      List<SegmentCommitInfo> infos = new ArrayList<>();

      // long so we can detect int overflow:
      long totalMaxDoc = 0;
      List<SegmentInfos> commits = new ArrayList<>(dirs.length);
      for (Directory dir : dirs) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "addIndexes: process directory " + dir);
        }
        SegmentInfos sis = SegmentInfos.readLatestCommit(dir); // read infos from dir
        if (segmentInfos.getIndexCreatedVersionMajor() != sis.getIndexCreatedVersionMajor()) {
          throw new IllegalArgumentException("Cannot use addIndexes(Directory) with indexes that have been created "
              + "by a different Lucene version. The current index was generated by Lucene "
              + segmentInfos.getIndexCreatedVersionMajor()
              + " while one of the directories contains an index that was generated with Lucene "
              + sis.getIndexCreatedVersionMajor());
        }
        totalMaxDoc += sis.totalMaxDoc();
        commits.add(sis);
      }

      // Best-effort up front check:
      testReserveDocs(totalMaxDoc);
        
      boolean success = false;
      try {
        for (SegmentInfos sis : commits) {
          for (SegmentCommitInfo info : sis) {
            assert !infos.contains(info): "dup info dir=" + info.info.dir + " name=" + info.info.name;

            Sort segmentIndexSort = info.info.getIndexSort();

            if (indexSort != null && (segmentIndexSort == null || isCongruentSort(indexSort, segmentIndexSort) == false)) {
              throw new IllegalArgumentException("cannot change index sort from " + segmentIndexSort + " to " + indexSort);
            }

            String newSegName = newSegmentName();

            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "addIndexes: process segment origName=" + info.info.name + " newName=" + newSegName + " info=" + info);
            }

            IOContext context = new IOContext(new FlushInfo(info.info.maxDoc(), info.sizeInBytes()));

            FieldInfos fis = readFieldInfos(info);
            for(FieldInfo fi : fis) {
              // This will throw exceptions if any of the incoming fields have an illegal schema change:
              globalFieldNumberMap.addOrGet(fi.name, fi.number, fi.getIndexOptions(), fi.getDocValuesType(), fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField());
            }
            infos.add(copySegmentAsIs(info, newSegName, context));
          }
        }
        success = true;
      } finally {
        if (!success) {
          for(SegmentCommitInfo sipc : infos) {
            // Safe: these files must exist
            deleteNewFiles(sipc.files());
          }
        }
      }

      synchronized (this) {
        success = false;
        try {
          ensureOpen();

          // Now reserve the docs, just before we update SIS:
          reserveDocs(totalMaxDoc);

          seqNo = docWriter.getNextSequenceNumber();

          success = true;
        } finally {
          if (!success) {
            for(SegmentCommitInfo sipc : infos) {
              // Safe: these files must exist
              deleteNewFiles(sipc.files());
            }
          }
        }
        segmentInfos.addAll(infos);
        checkpoint();
      }

      successTop = true;

    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "addIndexes(Directory...)");
      throw tragedy;
    } finally {
      if (successTop) {
        IOUtils.close(locks);
      } else {
        IOUtils.closeWhileHandlingException(locks);
      }
    }
    maybeMerge();

    return seqNo;
  }

  private void validateMergeReader(CodecReader leaf) {
    LeafMetaData segmentMeta = leaf.getMetaData();
    if (segmentInfos.getIndexCreatedVersionMajor() != segmentMeta.getCreatedVersionMajor()) {
      throw new IllegalArgumentException("Cannot merge a segment that has been created with major version "
          + segmentMeta.getCreatedVersionMajor() + " into this index which has been created by major version "
          + segmentInfos.getIndexCreatedVersionMajor());
    }

    if (segmentInfos.getIndexCreatedVersionMajor() >= 7 && segmentMeta.getMinVersion() == null) {
      throw new IllegalStateException("Indexes created on or after Lucene 7 must record the created version major, but " + leaf + " hides it");
    }

    Sort leafIndexSort = segmentMeta.getSort();
    if (config.getIndexSort() != null &&
          (leafIndexSort == null || isCongruentSort(config.getIndexSort(), leafIndexSort) == false)) {
      throw new IllegalArgumentException("cannot change index sort from " + leafIndexSort + " to " + config.getIndexSort());
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
   * <b>NOTE:</b> empty segments are dropped by this method and not added to this
   * index.
   * 
   * <p>
   * <b>NOTE:</b> this merges all given {@link LeafReader}s in one
   * merge. If you intend to merge a large number of readers, it may be better
   * to call this method multiple times, each time with a small set of readers.
   * In principle, if you use a merge policy with a {@code mergeFactor} or
   * {@code maxMergeAtOnce} parameter, you should pass that many readers in one
   * call.
   * 
   * <p>
   * <b>NOTE:</b> this method does not call or make use of the {@link MergeScheduler},
   * so any custom bandwidth throttling is at the moment ignored.
   * 
   * @return The <a href="#sequence_number">sequence number</a>
   * for this operation
   *
   * @throws CorruptIndexException
   *           if the index is corrupt
   * @throws IOException
   *           if there is a low-level IO error
   * @throws IllegalArgumentException
   *           if addIndexes would cause the index to exceed {@link #MAX_DOCS}
   */
  public long addIndexes(CodecReader... readers) throws IOException {
    ensureOpen();

    // long so we can detect int overflow:
    long numDocs = 0;
    long seqNo;
    try {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "flush at addIndexes(CodecReader...)");
      }
      flush(false, true);

      String mergedName = newSegmentName();
      int numSoftDeleted = 0;
      for (CodecReader leaf : readers) {
        numDocs += leaf.numDocs();
        validateMergeReader(leaf);
        if (softDeletesEnabled) {
            Bits liveDocs = leaf.getLiveDocs();
            numSoftDeleted += PendingSoftDeletes.countSoftDeletes(
            DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(config.getSoftDeletesField(), leaf), liveDocs);
        }
      }
      
      // Best-effort up front check:
      testReserveDocs(numDocs);

      final IOContext context = new IOContext(new MergeInfo(Math.toIntExact(numDocs), -1, false, UNBOUNDED_MAX_MERGE_SEGMENTS));

      // TODO: somehow we should fix this merge so it's
      // abortable so that IW.close(false) is able to stop it
      TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(directory);
      Codec codec = config.getCodec();
      // We set the min version to null for now, it will be set later by SegmentMerger
      SegmentInfo info = new SegmentInfo(directoryOrig, Version.LATEST, null, mergedName, -1,
                                         false, codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), config.getIndexSort());

      SegmentMerger merger = new SegmentMerger(Arrays.asList(readers), info, infoStream, trackingDir,
                                               globalFieldNumberMap, 
                                               context);

      if (!merger.shouldMerge()) {
        return docWriter.getNextSequenceNumber();
      }

      synchronized (this) {
        ensureOpen();
        assert merges.areEnabled();
        runningAddIndexesMerges.add(merger);
      }
      try {
        merger.merge();  // merge 'em
      } finally {
        synchronized (this) {
          runningAddIndexesMerges.remove(merger);
          notifyAll();
        }
      }
      SegmentCommitInfo infoPerCommit = new SegmentCommitInfo(info, 0, numSoftDeleted, -1L, -1L, -1L, StringHelper.randomId());

      info.setFiles(new HashSet<>(trackingDir.getCreatedFiles()));
      trackingDir.clearCreatedFiles();
                                         
      setDiagnostics(info, SOURCE_ADDINDEXES_READERS);

      final MergePolicy mergePolicy = config.getMergePolicy();
      boolean useCompoundFile;
      synchronized(this) { // Guard segmentInfos
        if (merges.areEnabled() == false) {
          // Safe: these files must exist
          deleteNewFiles(infoPerCommit.files());

          return docWriter.getNextSequenceNumber();
        }
        ensureOpen();
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, infoPerCommit, this);
      }

      // Now create the compound file if needed
      if (useCompoundFile) {
        Collection<String> filesToDelete = infoPerCommit.files();
        TrackingDirectoryWrapper trackingCFSDir = new TrackingDirectoryWrapper(directory);
        // TODO: unlike merge, on exception we arent sniping any trash cfs files here?
        // createCompoundFile tries to cleanup, but it might not always be able to...
        try {
          createCompoundFile(infoStream, trackingCFSDir, info, context, this::deleteNewFiles);
        } finally {
          // delete new non cfs files directly: they were never
          // registered with IFD
          deleteNewFiles(filesToDelete);
        }
        info.setUseCompoundFile(true);
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      codec.segmentInfoFormat().write(trackingDir, info, context);

      info.addFiles(trackingDir.getCreatedFiles());

      // Register the new segment
      synchronized(this) {
        if (merges.areEnabled() == false) {
          // Safe: these files must exist
          deleteNewFiles(infoPerCommit.files());

          return docWriter.getNextSequenceNumber();
        }
        ensureOpen();

        // Now reserve the docs, just before we update SIS:
        reserveDocs(numDocs);
      
        segmentInfos.add(infoPerCommit);
        seqNo = docWriter.getNextSequenceNumber();
        checkpoint();
      }
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "addIndexes(CodecReader...)");
      throw tragedy;
    }
    maybeMerge();

    return seqNo;
  }

  /** Copies the segment files as-is into the IndexWriter's directory. */
  private SegmentCommitInfo copySegmentAsIs(SegmentCommitInfo info, String segName, IOContext context) throws IOException {
    
    // Same SI as before but we change directory and name
    SegmentInfo newInfo = new SegmentInfo(directoryOrig, info.info.getVersion(), info.info.getMinVersion(), segName, info.info.maxDoc(),
                                          info.info.getUseCompoundFile(), info.info.getCodec(), 
                                          info.info.getDiagnostics(), info.info.getId(), info.info.getAttributes(), info.info.getIndexSort());
    SegmentCommitInfo newInfoPerCommit = new SegmentCommitInfo(newInfo, info.getDelCount(), info.getSoftDelCount(), info.getDelGen(),
                                                               info.getFieldInfosGen(), info.getDocValuesGen(), info.getId());

    newInfo.setFiles(info.info.files());
    newInfoPerCommit.setFieldInfosFiles(info.getFieldInfosFiles());
    newInfoPerCommit.setDocValuesUpdatesFiles(info.getDocValuesUpdatesFiles());

    boolean success = false;

    Set<String> copiedFiles = new HashSet<>();
    try {
      // Copy the segment's files
      for (String file: info.files()) {
        final String newFileName = newInfo.namedForThisSegment(file);
        directory.copyFrom(info.info.dir, file, newFileName, context);
        copiedFiles.add(newFileName);
      }
      success = true;
    } finally {
      if (!success) {
        // Safe: these files must exist
        deleteNewFiles(copiedFiles);
      }
    }

    assert copiedFiles.equals(newInfoPerCommit.files()): "copiedFiles=" + copiedFiles + " vs " + newInfoPerCommit.files();
    
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
   * @return The <a href="#sequence_number">sequence number</a>
   * of the last operation in the commit.  All sequence numbers &lt;= this value
   * will be reflected in the commit, and all others will not.
   */
  @Override
  public final long prepareCommit() throws IOException {
    ensureOpen();
    pendingSeqNo = prepareCommitInternal();
    // we must do this outside of the commitLock else we can deadlock:
    if (maybeMerge.getAndSet(false)) {
      maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);      
    }
    return pendingSeqNo;
  }

  /**
   * <p>Expert: Flushes the next pending writer per thread buffer if available or the largest active
   * non-pending writer per thread buffer in the calling thread.
   * This can be used to flush documents to disk outside of an indexing thread. In contrast to {@link #flush()}
   * this won't mark all currently active indexing buffers as flush-pending.
   *
   * Note: this method is best-effort and might not flush any segments to disk. If there is a full flush happening
   * concurrently multiple segments might have been flushed.
   * Users of this API can access the IndexWriters current memory consumption via {@link #ramBytesUsed()}
   * </p>
   * @return <code>true</code> iff this method flushed at least on segment to disk.
   * @lucene.experimental
   */
  public final boolean flushNextBuffer() throws IOException {
    try {
      if (docWriter.flushOneDWPT()) {
        processEvents(true);
        return true; // we wrote a segment
      }
      return false;
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "flushNextBuffer");
      throw tragedy;
    } finally {
      maybeCloseOnTragicEvent();
    }
  }

  private long prepareCommitInternal() throws IOException {
    startCommitTime = System.nanoTime();
    synchronized(commitLock) {
      ensureOpen(false);
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "prepareCommit: flush");
        infoStream.message("IW", "  index before flush " + segString());
      }

      if (tragedy.get() != null) {
        throw new IllegalStateException("this writer hit an unrecoverable error; cannot commit", tragedy.get());
      }

      if (pendingCommit != null) {
        throw new IllegalStateException("prepareCommit was already called with no corresponding call to commit");
      }

      doBeforeFlush();
      testPoint("startDoFlush");
      SegmentInfos toCommit = null;
      boolean anyChanges = false;
      long seqNo;
      MergePolicy.MergeSpecification pointInTimeMerges = null;
      AtomicBoolean stopAddingMergedSegments = new AtomicBoolean(false);
      final long maxCommitMergeWaitMillis = config.getMaxFullFlushMergeWaitMillis();
      // This is copied from doFlush, except it's modified to
      // clone & incRef the flushed SegmentInfos inside the
      // sync block:

      try {

        synchronized (fullFlushLock) {
          boolean flushSuccess = false;
          boolean success = false;
          try {
            seqNo = docWriter.flushAllThreads();
            if (seqNo < 0) {
              anyChanges = true;
              seqNo = -seqNo;
            }
            if (anyChanges == false) {
              // prevent double increment since docWriter#doFlush increments the flushcount
              // if we flushed anything.
              flushCount.incrementAndGet();
            }
            publishFlushedSegments(true);
            // cannot pass triggerMerges=true here else it can lead to deadlock:
            processEvents(false);
            
            flushSuccess = true;

            applyAllDeletesAndUpdates();
            synchronized(this) {
              writeReaderPool(true);
              if (changeCount.get() != lastCommitChangeCount) {
                // There are changes to commit, so we will write a new segments_N in startCommit.
                // The act of committing is itself an NRT-visible change (an NRT reader that was
                // just opened before this should see it on reopen) so we increment changeCount
                // and segments version so a future NRT reopen will see the change:
                changeCount.incrementAndGet();
                segmentInfos.changed();
              }

              if (commitUserData != null) {
                Map<String,String> userData = new HashMap<>();
                for(Map.Entry<String,String> ent : commitUserData) {
                  userData.put(ent.getKey(), ent.getValue());
                }
                segmentInfos.setUserData(userData, false);
              }

              // Must clone the segmentInfos while we still
              // hold fullFlushLock and while sync'd so that
              // no partial changes (eg a delete w/o
              // corresponding add from an updateDocument) can
              // sneak into the commit point:
              toCommit = segmentInfos.clone();
              pendingCommitChangeCount = changeCount.get();
              // This protects the segmentInfos we are now going
              // to commit.  This is important in case, eg, while
              // we are trying to sync all referenced files, a
              // merge completes which would otherwise have
              // removed the files we are now syncing.
              deleter.incRef(toCommit.files(false));
              if (anyChanges && maxCommitMergeWaitMillis > 0) {
                // we can safely call preparePointInTimeMerge since writeReaderPool(true) above wrote all
                // necessary files to disk and checkpointed them.
                pointInTimeMerges = preparePointInTimeMerge(toCommit, stopAddingMergedSegments::get, MergeTrigger.COMMIT, sci->{});
              }
            }
            success = true;
          } finally {
            if (!success) {
              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "hit exception during prepareCommit");
              }
            }
            assert Thread.holdsLock(fullFlushLock);
            // Done: finish the full flush!
            docWriter.finishFullFlush(flushSuccess);
            doAfterFlush();
          }
        }
      } catch (VirtualMachineError tragedy) {
        tragicEvent(tragedy, "prepareCommit");
        throw tragedy;
      } finally {
        maybeCloseOnTragicEvent();
      }

      if (pointInTimeMerges != null) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "now run merges during commit: " + pointInTimeMerges.segString(directory));
        }
        mergeScheduler.merge(mergeSource, MergeTrigger.COMMIT);
        pointInTimeMerges.await(maxCommitMergeWaitMillis, TimeUnit.MILLISECONDS);
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "done waiting for merges during commit");
        }
        synchronized (this) {
          // we need to call this under lock since mergeFinished above is also called under the IW lock
          stopAddingMergedSegments.set(true);
        }
      }
      // do this after handling any pointInTimeMerges since the files will have changed if any merges
      // did complete
      filesToCommit = toCommit.files(false);
      try {
        if (anyChanges) {
          maybeMerge.set(true);
        }
        startCommit(toCommit);
        if (pendingCommit == null) {
          return -1;
        } else {
          return seqNo;
        }
      } catch (Throwable t) {
        synchronized (this) {
          if (filesToCommit != null) {
            try {
              deleter.decRef(filesToCommit);
            } catch (Throwable t1) {
              t.addSuppressed(t1);
            } finally {
              filesToCommit = null;
            }
          }
        }
        throw t;
      }
    }
  }

  /**
   * This optimization allows a commit/getReader to wait for merges on smallish segments to
   * reduce the eventual number of tiny segments in the commit point / NRT Reader.  We wrap a {@code OneMerge} to
   * update the {@code mergingSegmentInfos} once the merge has finished. We replace the source segments
   * in the SIS that we are going to commit / open the reader on with the freshly merged segment, but ignore all deletions and updates
   * that are made to documents in the merged segment while it was merging. The updates that are made do not belong to
   * the point-in-time commit point / NRT READER and should therefore not be included. See the clone call in {@code onMergeComplete}
   * below.  We also ensure that we pull the merge readers while holding {@code IndexWriter}'s lock.  Otherwise
   * we could see concurrent deletions/updates applied that do not belong to the segment.
   */
  private MergePolicy.MergeSpecification preparePointInTimeMerge(SegmentInfos mergingSegmentInfos, BooleanSupplier stopCollectingMergeResults,
                                                                 MergeTrigger trigger,
                                                                 IOUtils.IOConsumer<SegmentCommitInfo> mergeFinished) throws IOException {
    assert Thread.holdsLock(this);
    assert trigger == MergeTrigger.GET_READER || trigger == MergeTrigger.COMMIT : "illegal trigger: " + trigger;
    MergePolicy.MergeSpecification pointInTimeMerges = updatePendingMerges(new OneMergeWrappingMergePolicy(config.getMergePolicy(), toWrap ->
        new MergePolicy.OneMerge(toWrap.segments) {
          SegmentCommitInfo origInfo;
          final AtomicBoolean onlyOnce = new AtomicBoolean(false);

          @Override
          public void mergeFinished(boolean committed, boolean segmentDropped) throws IOException {
            assert Thread.holdsLock(IndexWriter.this);

            // includedInCommit will be set (above, by our caller) to false if the allowed max wall clock
            // time (IWC.getMaxCommitMergeWaitMillis()) has elapsed, which means we did not make the timeout
            // and will not commit our merge to the to-be-committed SegmentInfos
            if (segmentDropped == false
                && committed
                && stopCollectingMergeResults.getAsBoolean() == false) {

              // make sure onMergeComplete really was called:
              assert origInfo != null;

              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "now apply merge during commit: " + toWrap.segString());
              }

              if (trigger == MergeTrigger.COMMIT) {
                // if we do this in a getReader call here this is obsolete since we already hold a reader that has
                // incRef'd these files
                deleter.incRef(origInfo.files());
              }
              Set<String> mergedSegmentNames = new HashSet<>();
              for (SegmentCommitInfo sci : segments) {
                mergedSegmentNames.add(sci.info.name);
              }
              List<SegmentCommitInfo> toCommitMergedAwaySegments = new ArrayList<>();
              for (SegmentCommitInfo sci : mergingSegmentInfos) {
                if (mergedSegmentNames.contains(sci.info.name)) {
                  toCommitMergedAwaySegments.add(sci);
                  if (trigger == MergeTrigger.COMMIT) {
                    // if we do this in a getReader call here this is obsolete since we already hold a reader that has
                    // incRef'd these files and will decRef them when it's closed
                    deleter.decRef(sci.files());
                  }
                }
              }
              // Construct a OneMerge that applies to toCommit
              MergePolicy.OneMerge applicableMerge = new MergePolicy.OneMerge(toCommitMergedAwaySegments);
              applicableMerge.info = origInfo;
              long segmentCounter = Long.parseLong(origInfo.info.name.substring(1), Character.MAX_RADIX);
              mergingSegmentInfos.counter = Math.max(mergingSegmentInfos.counter, segmentCounter + 1);
              mergingSegmentInfos.applyMergeChanges(applicableMerge, false);
            } else {
              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "skip apply merge during commit: " + toWrap.segString());
              }
            }
            toWrap.mergeFinished(committed, segmentDropped);
            super.mergeFinished(committed, segmentDropped);
          }

          @Override
          void onMergeComplete() throws IOException {
            assert Thread.holdsLock(IndexWriter.this);
            if (stopCollectingMergeResults.getAsBoolean() == false
                && isAborted() == false
                && info.info.maxDoc() > 0/* never do this if the segment if dropped / empty */) {
              mergeFinished.accept(info);
              // clone the target info to make sure we have the original info without the updated del and update gens
              origInfo = info.clone();
            }
            toWrap.onMergeComplete();
            super.onMergeComplete();
          }

          @Override
          void initMergeReaders(IOUtils.IOFunction<SegmentCommitInfo, MergePolicy.MergeReader> readerFactory) throws IOException {
            if (onlyOnce.compareAndSet(false, true)) {
              // we do this only once below to pull readers as point in time readers with respect to the commit point
              // we try to update
              super.initMergeReaders(readerFactory);
            }
          }

          @Override
          public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            return toWrap.wrapForMerge(reader); // must delegate
          }
        }
    ), trigger, UNBOUNDED_MAX_MERGE_SEGMENTS);
    if (pointInTimeMerges != null) {
      boolean closeReaders = true;
      try {
        for (MergePolicy.OneMerge merge : pointInTimeMerges.merges) {
          IOContext context = new IOContext(merge.getStoreMergeInfo());
          merge.initMergeReaders(
              sci -> {
                final ReadersAndUpdates rld = getPooledInstance(sci, true);
                // calling setIsMerging is important since it causes the RaU to record all DV updates
                // in a separate map in order to be applied to the merged segment after it's done
                rld.setIsMerging();
                return rld.getReaderForMerge(context);
              });
        }
        closeReaders = false;
      } finally {
        if (closeReaders) {
          IOUtils.applyToAll(pointInTimeMerges.merges, merge -> {
            // that merge is broken we need to clean up after it - it's fine we still have the IW lock to do this
            boolean removed = pendingMerges.remove(merge);
            assert removed: "merge should be pending but isn't: " + merge.segString();
            try {
              abortOneMerge(merge);
            } finally {
              mergeFinish(merge);
            }
          });
        }
      }
    }
    return pointInTimeMerges;
  }

  /**
   * Ensures that all changes in the reader-pool are written to disk.
   * @param writeDeletes if <code>true</code> if deletes should be written to disk too.
   */
  private void writeReaderPool(boolean writeDeletes) throws IOException {
    assert Thread.holdsLock(this);
    if (writeDeletes) {
      if (readerPool.commit(segmentInfos)) {
        checkpointNoSIS();
      }
    } else { // only write the docValues
      if (readerPool.writeAllDocValuesUpdates()) {
        checkpoint();
      }
    }
    // now do some best effort to check if a segment is fully deleted
    List<SegmentCommitInfo> toDrop = new ArrayList<>(); // don't modify segmentInfos in-place
    for (SegmentCommitInfo info : segmentInfos) {
      ReadersAndUpdates readersAndUpdates = readerPool.get(info, false);
      if (readersAndUpdates != null) {
        if (isFullyDeleted(readersAndUpdates)) {
          toDrop.add(info);
        }
      }
    }
    for (SegmentCommitInfo info : toDrop) {
      dropDeletedSegment(info);
    }
    if (toDrop.isEmpty() == false) {
      checkpoint();
    }
  }
  
  /**
   * Sets the iterator to provide the commit user data map at commit time.  Calling this method
   * is considered a committable change and will be {@link #commit() committed} even if
   * there are no other changes this writer. Note that you must call this method
   * before {@link #prepareCommit()}.  Otherwise it won't be included in the
   * follow-on {@link #commit()}.
   * <p>
   * <b>NOTE:</b> the iterator is late-binding: it is only visited once all documents for the
   * commit have been written to their segments, before the next segments_N file is written
   */
  public final synchronized void setLiveCommitData(Iterable<Map.Entry<String,String>> commitUserData) {
    setLiveCommitData(commitUserData, true);
  }

  /**
   * Sets the commit user data iterator, controlling whether to advance the {@link SegmentInfos#getVersion}.
   *
   * @see #setLiveCommitData(Iterable)
   *
   * @lucene.internal */
  public final synchronized void setLiveCommitData(Iterable<Map.Entry<String,String>> commitUserData, boolean doIncrementVersion) {
    this.commitUserData = commitUserData;
    if (doIncrementVersion) {
      segmentInfos.changed();
    }
    changeCount.incrementAndGet();
  }
  
  /**
   * Returns the commit user data iterable previously set with {@link #setLiveCommitData(Iterable)}, or null if nothing has been set yet.
   */
  public final synchronized Iterable<Map.Entry<String,String>> getLiveCommitData() {
    return commitUserData;
  }
  
  // Used only by commit and prepareCommit, below; lock
  // order is commitLock -> IW
  private final Object commitLock = new Object();

  /**
   * <p>Commits all pending changes (added and deleted
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
   * file contents and metadata are on stable storage.  For
   * FSDirectory, this calls the OS's fsync.  But, beware:
   * some hardware devices may in fact cache writes even
   * during fsync, and return before the bits are actually
   * on stable storage, to give the appearance of faster
   * performance.  If you have such a device, and it does
   * not have a battery backup (for example) then on power
   * loss it may still lose data.  Lucene cannot guarantee
   * consistency on such devices.  </p>
   *
   * <p> If nothing was committed, because there were no
   * pending changes, this returns -1.  Otherwise, it returns
   * the sequence number such that all indexing operations
   * prior to this sequence will be included in the commit
   * point, and all other operations will not. </p>
   *
   * @see #prepareCommit
   *
   * @return The <a href="#sequence_number">sequence number</a>
   * of the last operation in the commit.  All sequence numbers &lt;= this value
   * will be reflected in the commit, and all others will not.
   */
  @Override
  public final long commit() throws IOException {
    ensureOpen();
    return commitInternal(config.getMergePolicy());
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
    return changeCount.get() != lastCommitChangeCount || hasChangesInRam();
  }

  /**
   * Returns true if there are any changes or deletes that are not flushed or applied.
   */
  boolean hasChangesInRam() {
    return docWriter.anyChanges() || bufferedUpdatesStream.any();
  }

  private long commitInternal(MergePolicy mergePolicy) throws IOException {

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commit: start");
    }

    long seqNo;

    synchronized(commitLock) {
      ensureOpen(false);

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "commit: enter lock");
      }

      if (pendingCommit == null) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "commit: now prepare");
        }
        seqNo = prepareCommitInternal();
      } else {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "commit: already prepared");
        }
        seqNo = pendingSeqNo;
      }

      finishCommit();
    }

    // we must do this outside of the commitLock else we can deadlock:
    if (maybeMerge.getAndSet(false)) {
      maybeMerge(mergePolicy, MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);      
    }
    
    return seqNo;
  }

  @SuppressWarnings("try")
  private void finishCommit() throws IOException {

    boolean commitCompleted = false;
    String committedSegmentsFileName = null;

    try {
      synchronized(this) {
        ensureOpen(false);

        if (tragedy.get() != null) {
          throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete commit", tragedy.get());
        }

        if (pendingCommit != null) {
          final Collection<String> commitFiles = this.filesToCommit;
          try (Closeable finalizer = () -> deleter.decRef(commitFiles)) {

            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "commit: pendingCommit != null");
            }

            committedSegmentsFileName = pendingCommit.finishCommit(directory);

            // we committed, if anything goes wrong after this, we are screwed and it's a tragedy:
            commitCompleted = true;

            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "commit: done writing segments file \"" + committedSegmentsFileName + "\"");
            }

            // NOTE: don't use this.checkpoint() here, because
            // we do not want to increment changeCount:
            deleter.checkpoint(pendingCommit, true);

            // Carry over generation to our master SegmentInfos:
            segmentInfos.updateGeneration(pendingCommit);

            lastCommitChangeCount = pendingCommitChangeCount;
            rollbackSegments = pendingCommit.createBackupSegmentInfos();

          } finally {
            notifyAll();
            pendingCommit = null;
            this.filesToCommit = null;
          }
        } else {
          assert filesToCommit == null;
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "commit: pendingCommit == null; skip");
          }
        }
      }
    } catch (Throwable t) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "hit exception during finishCommit: " + t.getMessage());
      }
      if (commitCompleted) {
        tragicEvent(t, "finishCommit");
      }
      throw t;
    }

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", String.format(Locale.ROOT, "commit: took %.1f msec", (System.nanoTime()-startCommitTime)/1000000.0));
      infoStream.message("IW", "commit: done");
    }
  }

  // Ensures only one flush() is actually flushing segments
  // at a time:
  private final Object fullFlushLock = new Object();
  
  /** Moves all in-memory segments to the {@link Directory}, but does not commit
   *  (fsync) them (call {@link #commit} for that). */
  public final void flush() throws IOException {
    flush(true, true);
  }

  /**
   * Flush all in-memory buffered updates (adds and deletes)
   * to the Directory.
   * @param triggerMerge if true, we may merge segments (if
   *  deletes or docs were flushed) if necessary
   * @param applyAllDeletes whether pending deletes should also
   */
  final void flush(boolean triggerMerge, boolean applyAllDeletes) throws IOException {

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

  /** Returns true a segment was flushed or deletes were applied. */
  private boolean doFlush(boolean applyAllDeletes) throws IOException {
    if (tragedy.get() != null) {
      throw new IllegalStateException("this writer hit an unrecoverable error; cannot flush", tragedy.get());
    }

    doBeforeFlush();
    testPoint("startDoFlush");
    boolean success = false;
    try {

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "  start flush: applyAllDeletes=" + applyAllDeletes);
        infoStream.message("IW", "  index before flush " + segString());
      }
      boolean anyChanges;
      
      synchronized (fullFlushLock) {
        boolean flushSuccess = false;
        try {
          long seqNo = docWriter.flushAllThreads() ;
          if (seqNo < 0) {
            seqNo = -seqNo;
            anyChanges = true;
          } else {
            anyChanges = false;
          }
          if (!anyChanges) {
            // flushCount is incremented in flushAllThreads
            flushCount.incrementAndGet();
          }
          publishFlushedSegments(true);
          flushSuccess = true;
        } finally {
          assert Thread.holdsLock(fullFlushLock);;
          docWriter.finishFullFlush(flushSuccess);
          processEvents(false);
        }
      }

      if (applyAllDeletes) {
        applyAllDeletesAndUpdates();
      }

      anyChanges |= maybeMerge.getAndSet(false);
      
      synchronized(this) {
        writeReaderPool(applyAllDeletes);
        doAfterFlush();
        success = true;
        return anyChanges;
      }
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "doFlush");
      throw tragedy;
    } finally {
      if (!success) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "hit exception during flush");
        }
        maybeCloseOnTragicEvent();
      }
    }
  }
  
  private void applyAllDeletesAndUpdates() throws IOException {
    assert Thread.holdsLock(this) == false;
    flushDeletesCount.incrementAndGet();
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "now apply all deletes for all segments buffered updates bytesUsed=" + bufferedUpdatesStream.ramBytesUsed() + " reader pool bytesUsed=" + readerPool.ramBytesUsed());
    }
    bufferedUpdatesStream.waitApplyAll(this);
  }

  // for testing only
  DocumentsWriter getDocsWriter() {
    return docWriter;
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
        throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.info.name + ") that is not in the current index " + segString());
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
  private synchronized ReadersAndUpdates commitMergedDeletesAndUpdates(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {

    mergeFinishedGen.incrementAndGet();
    
    testPoint("startCommitMergeDeletes");

    final List<SegmentCommitInfo> sourceSegments = merge.segments;

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "commitMergeDeletes " + segString(merge.segments));
    }

    // Carefully merge deletes that occurred after we
    // started merging:
    long minGen = Long.MAX_VALUE;

    // Lazy init (only when we find a delete or update to carry over):
    final ReadersAndUpdates mergedDeletesAndUpdates = getPooledInstance(merge.info, true);
    int numDeletesBefore = mergedDeletesAndUpdates.getDelCount();
    // field -> delGen -> dv field updates
    Map<String,Map<Long,DocValuesFieldUpdates>> mappedDVUpdates = new HashMap<>();

    boolean anyDVUpdates = false;

    assert sourceSegments.size() == mergeState.docMaps.length;
    for (int i = 0; i < sourceSegments.size(); i++) {
      SegmentCommitInfo info = sourceSegments.get(i);
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
      final int maxDoc = info.info.maxDoc();
      final ReadersAndUpdates rld = getPooledInstance(info, false);
      // We hold a ref, from when we opened the readers during mergeInit, so it better still be in the pool:
      assert rld != null: "seg=" + info.info.name;

      MergeState.DocMap segDocMap = mergeState.docMaps[i];
      carryOverHardDeletes(mergedDeletesAndUpdates, maxDoc, mergeState.liveDocs[i],  merge.getMergeReader().get(i).hardLiveDocs, rld.getHardLiveDocs(),
          segDocMap);

      // Now carry over all doc values updates that were resolved while we were merging, remapping the docIDs to the newly merged docIDs.
      // We only carry over packets that finished resolving; if any are still running (concurrently) they will detect that our merge completed
      // and re-resolve against the newly merged segment:
      Map<String,List<DocValuesFieldUpdates>> mergingDVUpdates = rld.getMergingDVUpdates();
      for (Map.Entry<String,List<DocValuesFieldUpdates>> ent : mergingDVUpdates.entrySet()) {

        String field = ent.getKey();

        Map<Long,DocValuesFieldUpdates> mappedField = mappedDVUpdates.get(field);
        if (mappedField == null) {
          mappedField = new HashMap<>();
          mappedDVUpdates.put(field, mappedField);
        }

        for (DocValuesFieldUpdates updates : ent.getValue()) {

          if (bufferedUpdatesStream.stillRunning(updates.delGen)) {
            continue;
          }
              
          // sanity check:
          assert field.equals(updates.field);

          DocValuesFieldUpdates mappedUpdates = mappedField.get(updates.delGen);
          if (mappedUpdates == null) {
            switch (updates.type) {
              case NUMERIC:
                mappedUpdates = new NumericDocValuesFieldUpdates(updates.delGen, updates.field, merge.info.info.maxDoc());
                break;
              case BINARY:
                mappedUpdates = new BinaryDocValuesFieldUpdates(updates.delGen, updates.field, merge.info.info.maxDoc());
                break;
              default:
                throw new AssertionError();
            }
            mappedField.put(updates.delGen, mappedUpdates);
          }

          DocValuesFieldUpdates.Iterator it = updates.iterator();
          int doc;
          while ((doc = it.nextDoc()) != NO_MORE_DOCS) {
            int mappedDoc = segDocMap.get(doc);
            if (mappedDoc != -1) {
              if (it.hasValue()) {
                // not deleted
                mappedUpdates.add(mappedDoc, it);
              } else {
                mappedUpdates.reset(mappedDoc);
              }
              anyDVUpdates = true;
            }
          }
        }
      }
    }

    if (anyDVUpdates) {
      // Persist the merged DV updates onto the RAU for the merged segment:
      for(Map<Long,DocValuesFieldUpdates> d : mappedDVUpdates.values()) {
        for (DocValuesFieldUpdates updates : d.values()) {
          updates.finish();
          mergedDeletesAndUpdates.addDVUpdate(updates);
        }
      }
    }

    if (infoStream.isEnabled("IW")) {
      if (mergedDeletesAndUpdates == null) {
        infoStream.message("IW", "no new deletes or field updates since merge started");
      } else {
        String msg = mergedDeletesAndUpdates.getDelCount() - numDeletesBefore + " new deletes";
        if (anyDVUpdates) {
          msg += " and " + mergedDeletesAndUpdates.getNumDVUpdates() + " new field updates";
          msg += " (" + mergedDeletesAndUpdates.ramBytesUsed.get() + ") bytes";
        }
        msg += " since merge started";
        infoStream.message("IW", msg);
      }
    }

    merge.info.setBufferedDeletesGen(minGen);

    return mergedDeletesAndUpdates;
  }

  /**
   * This method carries over hard-deleted documents that are applied to the source segment during a merge.
   */
  private static void carryOverHardDeletes(ReadersAndUpdates mergedReadersAndUpdates, int maxDoc,
                                           Bits mergeLiveDocs, // the liveDocs used to build the segDocMaps
                                           Bits prevHardLiveDocs, // the hard deletes when the merge reader was pulled
                                           Bits currentHardLiveDocs, // the current hard deletes
                                           MergeState.DocMap segDocMap) throws IOException {

    assert mergeLiveDocs == null || mergeLiveDocs.length() == maxDoc;
    // if we mix soft and hard deletes we need to make sure that we only carry over deletes
    // that were not deleted before. Otherwise the segDocMap doesn't contain a mapping.
    // yet this is also required if any MergePolicy modifies the liveDocs since this is
    // what the segDocMap is build on.
    final IntPredicate carryOverDelete = mergeLiveDocs == null || mergeLiveDocs == prevHardLiveDocs
        ? docId -> currentHardLiveDocs.get(docId) == false
        : docId -> mergeLiveDocs.get(docId) && currentHardLiveDocs.get(docId) == false;
    if (prevHardLiveDocs != null) {
      // If we had deletions on starting the merge we must
      // still have deletions now:
      assert currentHardLiveDocs != null;
      assert mergeLiveDocs != null;
      assert prevHardLiveDocs.length() == maxDoc;
      assert currentHardLiveDocs.length() == maxDoc;

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
      if (currentHardLiveDocs != prevHardLiveDocs) {
        // This means this segment received new deletes
        // since we started the merge, so we
        // must merge them:
        for (int j = 0; j < maxDoc; j++) {
          if (prevHardLiveDocs.get(j) == false) {
            // if the document was deleted before, it better still be deleted!
            assert currentHardLiveDocs.get(j) == false;
          } else if (carryOverDelete.test(j)) {
            // the document was deleted while we were merging:
            mergedReadersAndUpdates.delete(segDocMap.get(j));
          }
        }
      }
    } else if (currentHardLiveDocs != null) {
      assert currentHardLiveDocs.length() == maxDoc;
      // This segment had no deletes before but now it
      // does:
      for (int j = 0; j < maxDoc; j++) {
        if (carryOverDelete.test(j)) {
          mergedReadersAndUpdates.delete(segDocMap.get(j));
        }
      }
    }
  }

  @SuppressWarnings("try")
  private synchronized boolean commitMerge(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {
    merge.onMergeComplete();
    testPoint("startCommitMerge");

    if (tragedy.get() != null) {
      throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete merge", tragedy.get());
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
      // Safe: these files must exist:
      deleteNewFiles(merge.info.files());
      return false;
    }

    final ReadersAndUpdates mergedUpdates = merge.info.info.maxDoc() == 0 ? null : commitMergedDeletesAndUpdates(merge, mergeState);

    // If the doc store we are using has been closed and
    // is in now compound format (but wasn't when we
    // started), then we will switch to the compound
    // format as well:

    assert !segmentInfos.contains(merge.info);

    final boolean allDeleted = merge.segments.size() == 0 ||
      merge.info.info.maxDoc() == 0 ||
      (mergedUpdates != null && isFullyDeleted(mergedUpdates));

    if (infoStream.isEnabled("IW")) {
      if (allDeleted) {
        infoStream.message("IW", "merged segment " + merge.info + " is 100% deleted; skipping insert");
      }
    }

    final boolean dropSegment = allDeleted;

    // If we merged no segments then we better be dropping
    // the new segment:
    assert merge.segments.size() > 0 || dropSegment;

    assert merge.info.info.maxDoc() != 0 || dropSegment;

    if (mergedUpdates != null) {
      boolean success = false;
      try {
        if (dropSegment) {
          mergedUpdates.dropChanges();
        }
        // Pass false for assertInfoLive because the merged
        // segment is not yet live (only below do we commit it
        // to the segmentInfos):
        release(mergedUpdates, false);
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
    int delDocCount;
    if (dropSegment) {
      // if we drop the segment we have to reduce the pendingNumDocs by merge.totalMaxDocs since we never drop
      // the docs when we apply deletes if the segment is currently merged.
      delDocCount = merge.totalMaxDoc;
    } else {
      delDocCount = merge.totalMaxDoc - merge.info.info.maxDoc();
    }
    assert delDocCount >= 0;
    adjustPendingNumDocs(-delDocCount);

    if (dropSegment) {
      assert !segmentInfos.contains(merge.info);
      readerPool.drop(merge.info);
      // Safe: these files must exist
      deleteNewFiles(merge.info.files());
    }

    try (Closeable finalizer = this::checkpoint) {
      // Must close before checkpoint, otherwise IFD won't be
      // able to delete the held-open files from the merge
      // readers:
      closeMergeReaders(merge, false, dropSegment);
    }

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "after commitMerge: " + segString());
    }

    if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS && !dropSegment) {
      // cascade the forceMerge:
      if (!segmentsToMerge.containsKey(merge.info)) {
        segmentsToMerge.put(merge.info, Boolean.FALSE);
      }
    }

    return true;
  }

  private void handleMergeException(Throwable t, MergePolicy.OneMerge merge) throws IOException {

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
      // deleteAll or rollback is called), unless the
      // merge involves segments from external directories,
      // in which case we must throw it so, for example, the
      // rollbackTransaction code in addIndexes* is
      // executed.
      if (merge.isExternal) { // TODO can we simplify this and just throw all the time? this would simplify this a lot
        throw (MergePolicy.MergeAbortedException) t;
      }
    } else {
      assert t != null;
      throw IOUtils.rethrowAlways(t);
    }
  }

  /**
   * Merges the indicated segments, replacing them in the stack with a
   * single segment.
   * 
   * @lucene.experimental
   */
  protected void merge(MergePolicy.OneMerge merge) throws IOException {

    boolean success = false;

    final long t0 = System.currentTimeMillis();

    final MergePolicy mergePolicy = config.getMergePolicy();
    try {
      try {
        try {
          mergeInit(merge);
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
          // Readers are already closed in commitMerge if we didn't hit
          // an exc:
          if (success == false) {
            closeMergeReaders(merge, true, false);
          }
          mergeFinish(merge);

          if (success == false) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception during merge");
            }
          } else if (!merge.isAborted() && (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS || (!closed && !closing))) {
            // This merge (and, generally, any change to the
            // segments) may now enable new merges, so we call
            // merge policy & update pending merges.
            updatePendingMerges(mergePolicy, MergeTrigger.MERGE_FINISHED, merge.maxNumSegments);
          }
        }
      }
    } catch (Throwable t) {
      // Important that tragicEvent is called after mergeFinish, else we hang
      // waiting for our merge thread to be removed from runningMerges:
      tragicEvent(t, "merge");
      throw t;
    }

    if (merge.info != null && merge.isAborted() == false) {
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "merge time " + (System.currentTimeMillis()-t0) + " msec for " + merge.info.info.maxDoc() + " docs");
      }
    }
  }

  /** Hook that's called when the specified merge is complete. */
  protected void mergeSuccess(MergePolicy.OneMerge merge) {}

  private void abortOneMerge(MergePolicy.OneMerge merge) throws IOException {
    merge.setAborted();
    closeMergeReaders(merge, true, false);
  }

  /** Checks whether this merge involves any segments
   *  already participating in a merge.  If not, this merge
   *  is "registered", meaning we record that its segments
   *  are now participating in a merge, and true is
   *  returned.  Else (the merge conflicts) false is
   *  returned. */
  private synchronized boolean registerMerge(MergePolicy.OneMerge merge) throws IOException {

    if (merge.registerDone) {
      return true;
    }
    assert merge.segments.size() > 0;

    if (merges.areEnabled() == false) {
      abortOneMerge(merge);
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
      if (info.info.dir != directoryOrig) {
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
      if (info.info.maxDoc() > 0) {
        final int delCount = numDeletedDocs(info);
        assert delCount <= info.info.maxDoc();
        final double delRatio = ((double) delCount)/info.info.maxDoc();
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
  final void mergeInit(MergePolicy.OneMerge merge) throws IOException {
    assert Thread.holdsLock(this) == false;
    // Make sure any deletes that must be resolved before we commit the merge are complete:
    bufferedUpdatesStream.waitApplyForMerge(merge.segments, this);

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

  private synchronized void _mergeInit(MergePolicy.OneMerge merge) throws IOException {

    testPoint("startMergeInit");

    assert merge.registerDone;
    assert merge.maxNumSegments == UNBOUNDED_MAX_MERGE_SEGMENTS || merge.maxNumSegments > 0;

    if (tragedy.get() != null) {
      throw new IllegalStateException("this writer hit an unrecoverable error; cannot merge", tragedy.get());
    }

    if (merge.info != null) {
      // mergeInit already done
      return;
    }

    merge.mergeInit();

    if (merge.isAborted()) {
      return;
    }

    // TODO: in the non-pool'd case this is somewhat
    // wasteful, because we open these readers, close them,
    // and then open them again for merging.  Maybe  we
    // could pre-pool them somehow in that case...

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "now apply deletes for " + merge.segments.size() + " merging segments");
    }

    // Must move the pending doc values updates to disk now, else the newly merged segment will not see them:
    // TODO: we could fix merging to pull the merged DV iterator so we don't have to move these updates to disk first, i.e. just carry them
    // in memory:
    if (readerPool.writeDocValuesUpdatesForMerge(merge.segments)) {
      checkpoint();
    }
    
    // Bind a new segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.
    final String mergeSegmentName = newSegmentName();
    // We set the min version to null for now, it will be set later by SegmentMerger
    SegmentInfo si = new SegmentInfo(directoryOrig, Version.LATEST, null, mergeSegmentName, -1, false, config.getCodec(),
        Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), config.getIndexSort());
    Map<String,String> details = new HashMap<>();
    details.put("mergeMaxNumSegments", "" + merge.maxNumSegments);
    details.put("mergeFactor", Integer.toString(merge.segments.size()));
    setDiagnostics(si, SOURCE_MERGE, details);
    merge.setMergeInfo(new SegmentCommitInfo(si, 0, 0, -1L, -1L, -1L, StringHelper.randomId()));

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
    // On IBM J9 JVM this is better than java.version which is just 1.7.0 (no update level):
    diagnostics.put("java.runtime.version", System.getProperty("java.runtime.version", "undefined"));
    // Hotspot version, e.g. 2.8 for J9:
    diagnostics.put("java.vm.version", System.getProperty("java.vm.version", "undefined"));
    diagnostics.put("timestamp", Long.toString(new Date().getTime()));
    if (details != null) {
      diagnostics.putAll(details);
    }
    info.setDiagnostics(diagnostics);
  }

  /** Does finishing for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance. */
  private synchronized void mergeFinish(MergePolicy.OneMerge merge) {

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

  @SuppressWarnings("try")
  private synchronized void closeMergeReaders(MergePolicy.OneMerge merge, boolean suppressExceptions, boolean droppedSegment) throws IOException {
    if (merge.hasFinished() == false) {
      final boolean drop = suppressExceptions == false;
      // first call mergeFinished before we potentially drop the reader and the last reference.
      merge.close(suppressExceptions == false, droppedSegment, mr -> {
        final SegmentReader sr = mr.reader;
        final ReadersAndUpdates rld = getPooledInstance(sr.getOriginalSegmentInfo(), false);
        // We still hold a ref so it should not have been removed:
        assert rld != null;
        if (drop) {
          rld.dropChanges();
        } else {
          rld.dropMergingUpdates();
        }
        rld.release(sr);
        release(rld);
        if (drop) {
          readerPool.drop(rld.info);
        }
      });
    } else {
      assert merge.getMergeReader().isEmpty() : "we are done but still have readers: " + merge.getMergeReader();
      assert suppressExceptions : "can't be done and not suppressing exceptions";
    }
  }

  private void countSoftDeletes(CodecReader reader, Bits wrappedLiveDocs, Bits hardLiveDocs, Counter softDeleteCounter,
                                Counter hardDeleteCounter) throws IOException {
    int hardDeleteCount = 0;
    int softDeletesCount = 0;
    DocIdSetIterator softDeletedDocs = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(config.getSoftDeletesField(), reader);
    if (softDeletedDocs != null) {
      int docId;
      while ((docId = softDeletedDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (wrappedLiveDocs == null || wrappedLiveDocs.get(docId)) {
          if (hardLiveDocs == null || hardLiveDocs.get(docId)) {
            softDeletesCount++;
          } else {
            hardDeleteCount++;
          }
        }
      }
    }
    softDeleteCounter.addAndGet(softDeletesCount);
    hardDeleteCounter.addAndGet(hardDeleteCount);
  }

  private boolean assertSoftDeletesCount(CodecReader reader, int expectedCount) throws IOException {
    Counter count = Counter.newCounter(false);
    Counter hardDeletes = Counter.newCounter(false);
    countSoftDeletes(reader, reader.getLiveDocs(), null, count, hardDeletes);
    assert count.get() == expectedCount : "soft-deletes count mismatch expected: "
        + expectedCount  + " but actual: " + count.get() ;
    return true;
  }

  /** Does the actual (time-consuming) work of the merge,
   *  but without holding synchronized lock on IndexWriter
   *  instance */
  private int mergeMiddle(MergePolicy.OneMerge merge, MergePolicy mergePolicy) throws IOException {
    testPoint("mergeMiddleStart");
    merge.checkAborted();

    Directory mergeDirectory = mergeScheduler.wrapForMerge(merge, directory);
    IOContext context = new IOContext(merge.getStoreMergeInfo());

    final TrackingDirectoryWrapper dirWrapper = new TrackingDirectoryWrapper(mergeDirectory);

    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "merging " + segString(merge.segments));
    }

    // This is try/finally to make sure merger's readers are
    // closed:
    boolean success = false;
    try {
      merge.initMergeReaders(sci -> {
        final ReadersAndUpdates rld = getPooledInstance(sci, true);
        rld.setIsMerging();
        return rld.getReaderForMerge(context);
      });
      // Let the merge wrap readers
      List<CodecReader> mergeReaders = new ArrayList<>();
      Counter softDeleteCount = Counter.newCounter(false);
      for (MergePolicy.MergeReader mergeReader : merge.getMergeReader()) {
        SegmentReader reader = mergeReader.reader;
        CodecReader wrappedReader = merge.wrapForMerge(reader);
        validateMergeReader(wrappedReader);
        if (softDeletesEnabled) {
          if (reader != wrappedReader) { // if we don't have a wrapped reader we won't preserve any soft-deletes
            Bits hardLiveDocs = mergeReader.hardLiveDocs;
            if (hardLiveDocs != null) { // we only need to do this accounting if we have mixed deletes
              Bits wrappedLiveDocs = wrappedReader.getLiveDocs();
              Counter hardDeleteCounter = Counter.newCounter(false);
              countSoftDeletes(wrappedReader, wrappedLiveDocs, hardLiveDocs, softDeleteCount, hardDeleteCounter);
              int hardDeleteCount = Math.toIntExact(hardDeleteCounter.get());
              // Wrap the wrapped reader again if we have excluded some hard-deleted docs
              if (hardDeleteCount > 0) {
                Bits liveDocs = wrappedLiveDocs == null ? hardLiveDocs : new Bits() {
                  @Override
                  public boolean get(int index) {
                    return hardLiveDocs.get(index) && wrappedLiveDocs.get(index);
                  }

                  @Override
                  public int length() {
                    return hardLiveDocs.length();
                  }
                };
                wrappedReader = FilterCodecReader.wrapLiveDocs(wrappedReader, liveDocs, wrappedReader.numDocs() - hardDeleteCount);
              }
            } else {
              final int carryOverSoftDeletes = reader.getSegmentInfo().getSoftDelCount() - wrappedReader.numDeletedDocs();
              assert carryOverSoftDeletes >= 0 : "carry-over soft-deletes must be positive";
              assert assertSoftDeletesCount(wrappedReader, carryOverSoftDeletes);
              softDeleteCount.addAndGet(carryOverSoftDeletes);
            }
          }
        }
        mergeReaders.add(wrappedReader);
      }
      final SegmentMerger merger = new SegmentMerger(mergeReaders,
                                                     merge.info.info, infoStream, dirWrapper,
                                                     globalFieldNumberMap,
                                                     context);
      merge.info.setSoftDelCount(Math.toIntExact(softDeleteCount.get()));
      merge.checkAborted();

      merge.mergeStartNS = System.nanoTime();

      // This is where all the work happens:
      if (merger.shouldMerge()) {
        merger.merge();
      }

      MergeState mergeState = merger.mergeState;
      assert mergeState.segmentInfo == merge.info.info;
      merge.info.info.setFiles(new HashSet<>(dirWrapper.getCreatedFiles()));
      Codec codec = config.getCodec();
      if (infoStream.isEnabled("IW")) {
        if (merger.shouldMerge()) {
          String pauseInfo = merge.getMergeProgress().getPauseTimes().entrySet()
            .stream()
            .filter((e) -> e.getValue() > 0)
            .map((e) -> String.format(Locale.ROOT, "%.1f sec %s",
                e.getValue() / 1000000000.,
                e.getKey().name().toLowerCase(Locale.ROOT)))
            .collect(Collectors.joining(", "));
          if (!pauseInfo.isEmpty()) {
            pauseInfo = " (" + pauseInfo + ")";
          }

          long t1 = System.nanoTime();
          double sec = (t1-merge.mergeStartNS)/1000000000.;
          double segmentMB = (merge.info.sizeInBytes()/1024./1024.);
          infoStream.message("IW", "merge codec=" + codec + " maxDoc=" + merge.info.info.maxDoc() + "; merged segment has " +
                             (mergeState.mergeFieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
                             (mergeState.mergeFieldInfos.hasNorms() ? "norms" : "no norms") + "; " +
                             (mergeState.mergeFieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " +
                             (mergeState.mergeFieldInfos.hasProx() ? "prox" : "no prox") + "; " +
                             (mergeState.mergeFieldInfos.hasFreq() ? "freqs" : "no freqs") + "; " +
                             (mergeState.mergeFieldInfos.hasPointValues() ? "points" : "no points") + "; " +
                             String.format(Locale.ROOT,
                                           "%.1f sec%s to merge segment [%.2f MB, %.2f MB/sec]",
                                           sec,
                                           pauseInfo,
                                           segmentMB,
                                           segmentMB / sec));
        } else {
          infoStream.message("IW", "skip merging fully deleted segments");
        }
      }

      if (merger.shouldMerge() == false) {
        // Merge would produce a 0-doc segment, so we do nothing except commit the merge to remove all the 0-doc segments that we "merged":
        assert merge.info.info.maxDoc() == 0;
        success = commitMerge(merge, mergeState);
        return 0;
      }

      assert merge.info.info.maxDoc() > 0;

      // Very important to do this before opening the reader
      // because codec must know if prox was written for
      // this segment:
      boolean useCompoundFile;
      synchronized (this) { // Guard segmentInfos
        useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, merge.info, this);
      }

      if (useCompoundFile) {
        success = false;

        Collection<String> filesToRemove = merge.info.files();
        TrackingDirectoryWrapper trackingCFSDir = new TrackingDirectoryWrapper(mergeDirectory);
        try {
          createCompoundFile(infoStream, trackingCFSDir, merge.info.info, context, this::deleteNewFiles);
          success = true;
        } catch (Throwable t) {
          synchronized(this) {
            if (merge.isAborted()) {
              // This can happen if rollback is called while we were building
              // our CFS -- fall through to logic below to remove the non-CFS
              // merged files:
              if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "hit merge abort exception creating compound file during merge");
              }
              return 0;
            } else {
              handleMergeException(t, merge);
            }
          }
        } finally {
          if (success == false) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception creating compound file during merge");
            }
            // Safe: these files must exist
            deleteNewFiles(merge.info.files());
          }
        }

        // So that, if we hit exc in deleteNewFiles (next)
        // or in commitMerge (later), we close the
        // per-segment readers in the finally clause below:
        success = false;

        synchronized(this) {

          // delete new non cfs files directly: they were never
          // registered with IFD
          deleteNewFiles(filesToRemove);

          if (merge.isAborted()) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "abort merge after building CFS");
            }
            // Safe: these files must exist
            deleteNewFiles(merge.info.files());
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
        codec.segmentInfoFormat().write(directory, merge.info.info, context);
        success2 = true;
      } finally {
        if (!success2) {
          // Safe: these files must exist
          deleteNewFiles(merge.info.files());
        }
      }

      // TODO: ideally we would freeze merge.info here!!
      // because any changes after writing the .si will be
      // lost...

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", String.format(Locale.ROOT, "merged segment size=%.3f MB vs estimate=%.3f MB", merge.info.sizeInBytes()/1024./1024., merge.estimatedMergeBytes/1024/1024.));
      }

      final IndexReaderWarmer mergedSegmentWarmer = config.getMergedSegmentWarmer();
      if (readerPool.isReaderPoolingEnabled() && mergedSegmentWarmer != null) {
        final ReadersAndUpdates rld = getPooledInstance(merge.info, true);
        final SegmentReader sr = rld.getReader(IOContext.READ);
        try {
          mergedSegmentWarmer.warm(sr);
        } finally {
          synchronized(this) {
            rld.release(sr);
            release(rld);
          }
        }
      }

      if (!commitMerge(merge, mergeState)) {
        // commitMerge will return false if this merge was
        // aborted
        return 0;
      }

      success = true;

    } finally {
      // Readers are already closed in commitMerge if we didn't hit
      // an exc:
      if (success == false) {
        closeMergeReaders(merge, true, false);
      }
    }

    return merge.info.info.maxDoc();
  }

  private synchronized void addMergeException(MergePolicy.OneMerge merge) {
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
  synchronized String segString() {
    return segString(segmentInfos);
  }

  synchronized String segString(Iterable<SegmentCommitInfo> infos) {
    return StreamSupport.stream(infos.spliterator(), false)
        .map(this::segString).collect(Collectors.joining(" "));
  }

  /** Returns a string description of the specified
   *  segment, for debugging.
   *
   * @lucene.internal */
  private synchronized String segString(SegmentCommitInfo info) {
    return info.toString(numDeletedDocs(info) - info.getDelCount(softDeletesEnabled));
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

  // called only from assert
  private boolean filesExist(SegmentInfos toSync) throws IOException {
    
    Collection<String> files = toSync.files(false);
    for(final String fileName: files) {
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
    final SegmentInfos newSIS = new SegmentInfos(sis.getIndexCreatedVersionMajor());
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

    testPoint("startStartCommit");
    assert pendingCommit == null;

    if (tragedy.get() != null) {
      throw new IllegalStateException("this writer hit an unrecoverable error; cannot commit", tragedy.get());
    }

    try {

      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "startCommit(): start");
      }

      synchronized(this) {

        if (lastCommitChangeCount > changeCount.get()) {
          throw new IllegalStateException("lastCommitChangeCount=" + lastCommitChangeCount + ",changeCount=" + changeCount);
        }

        if (pendingCommitChangeCount == lastCommitChangeCount) {
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "  skip startCommit(): no changes pending");
          }
          try {
            deleter.decRef(filesToCommit);
          } finally {
            filesToCommit = null;
          }
          return;
        }

        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "startCommit index=" + segString(toLiveInfos(toSync)) + " changeCount=" + changeCount);
        }

        assert filesExist(toSync);
      }

      testPoint("midStartCommit");

      boolean pendingCommitSet = false;

      try {

        testPoint("midStartCommit2");

        synchronized (this) {

          assert pendingCommit == null;

          assert segmentInfos.getGeneration() == toSync.getGeneration();

          // Exception here means nothing is prepared
          // (this method unwinds everything it did on
          // an exception)
          toSync.prepareCommit(directory);
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "startCommit: wrote pending segments file \"" + IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS, "", toSync.getGeneration()) + "\"");
          }

          pendingCommitSet = true;
          pendingCommit = toSync;
        }

        // This call can take a long time -- 10s of seconds
        // or more.  We do it without syncing on this:
        boolean success = false;
        final Collection<String> filesToSync;
        try {
          filesToSync = toSync.files(false);
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

        testPoint("midStartCommitSuccess");
      } catch (Throwable t) {
        synchronized(this) {
          if (!pendingCommitSet) {
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "hit exception committing segments file");
            }
            try {
              // Hit exception
              deleter.decRef(filesToCommit);
            } catch (Throwable t1) {
              t.addSuppressed(t1);
            } finally {
              filesToCommit = null;
            }
          }
        }
        throw t;
      } finally {
        synchronized(this) {
          // Have our master segmentInfos record the
          // generations we just prepared.  We do this
          // on error or success so we don't
          // double-write a segments_N file.
          segmentInfos.updateGeneration(toSync);
        }
      }
    } catch (VirtualMachineError tragedy) {
      tragicEvent(tragedy, "startCommit");
      throw tragedy;
    }
    testPoint("finishStartCommit");
  }

  /** If {@link DirectoryReader#open(IndexWriter)} has
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
   * <p><b>NOTE</b>: {@link #warm(LeafReader)} is called before any
   * deletes have been carried over to the merged segment. */
  @FunctionalInterface
  public interface IndexReaderWarmer {
    /**
     * Invoked on the {@link LeafReader} for the newly
     * merged segment, before that segment is made visible
     * to near-real-time readers.
     */
    void warm(LeafReader reader) throws IOException;
  }

  /**
   * <p>This method should be called on a tragic event ie. if a downstream class of the writer
   * hits an unrecoverable exception. This method does not rethrow the tragic event exception. 
   * <p>Note: This method will not close the writer but can be called from any location without respecting any lock order
   * @lucene.internal
   */
  public void onTragicEvent(Throwable tragedy, String location) {
    // This is not supposed to be tragic: IW is supposed to catch this and
    // ignore, because it means we asked the merge to abort:
    assert tragedy instanceof MergePolicy.MergeAbortedException == false;
    // How can it be a tragedy when nothing happened?
    assert tragedy != null;
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "hit tragic " + tragedy.getClass().getSimpleName() + " inside " + location);
    }
    this.tragedy.compareAndSet(null, tragedy); // only set it once
  }

  /**
   * This method set the tragic exception unless it's already set and closes the writer
   * if necessary. Note this method will not rethrow the throwable passed to it.
   */
  private void tragicEvent(Throwable tragedy, String location) throws IOException {
    try {
      onTragicEvent(tragedy, location);
    } finally {
      maybeCloseOnTragicEvent();
    }
  }

  private void maybeCloseOnTragicEvent() throws IOException {
    // We cannot hold IW's lock here else it can lead to deadlock:
    assert Thread.holdsLock(this) == false;
    assert Thread.holdsLock(fullFlushLock) == false;
    // if we are already closed (e.g. called by rollback), this will be a no-op.
    if (this.tragedy.get() != null && shouldClose(false)) {
      rollbackInternal();
    }
  }

  /** If this {@code IndexWriter} was closed as a side-effect of a tragic exception,
   *  e.g. disk full while flushing a new segment, this returns the root cause exception.
   *  Otherwise (no tragic exception has occurred) it returns null. */
  public Throwable getTragicException() {
    return tragedy.get();
  }

  /** Returns {@code true} if this {@code IndexWriter} is still open. */
  public boolean isOpen() {
    return closing == false && closed == false;
  }

  // Used for testing.  Current points:
  //   startDoFlush
  //   startCommitMerge
  //   startStartCommit
  //   midStartCommit
  //   midStartCommit2
  //   midStartCommitSuccess
  //   finishStartCommit
  //   startCommitMergeDeletes
  //   startMergeInit
  //   DocumentsWriterPerThread addDocuments start
  private void testPoint(String message) {
    if (enableTestPoints) {
      assert infoStream.isEnabled("TP"); // don't enable unless you need them.
      infoStream.message("TP", message);
    }
  }

  synchronized boolean nrtIsCurrent(SegmentInfos infos) {
    ensureOpen();
    boolean isCurrent = infos.getVersion() == segmentInfos.getVersion()
      && docWriter.anyChanges() == false
      && bufferedUpdatesStream.any() == false
      && readerPool.anyDocValuesChanges() == false;
    if (infoStream.isEnabled("IW")) {
      if (isCurrent == false) {
        infoStream.message("IW", "nrtIsCurrent: infoVersion matches: " + (infos.getVersion() == segmentInfos.getVersion()) + "; DW changes: " + docWriter.anyChanges() + "; BD changes: "+ bufferedUpdatesStream.any());
      }
    }
    return isCurrent;
  }

  synchronized boolean isClosed() {
    return closed;
  }

  boolean isDeleterClosed() {
    return deleter.isClosed();
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
    // TODO: should we remove this method now that it's the Directory's job to retry deletions?  Except, for the super expert IDP use case
    // it's still needed?
    ensureOpen(false);
    deleter.revisitPolicy();
  }

  /**
   * NOTE: this method creates a compound file for all files returned by
   * info.files(). While, generally, this may include separate norms and
   * deletion files, this SegmentInfo must not reference such files when this
   * method is called, because they are not allowed within a compound file.
   */
  static void createCompoundFile(InfoStream infoStream, TrackingDirectoryWrapper directory, final SegmentInfo info, IOContext context, IOUtils.IOConsumer<Collection<String>> deleteFiles) throws IOException {

    // maybe this check is not needed, but why take the risk?
    if (!directory.getCreatedFiles().isEmpty()) {
      throw new IllegalStateException("pass a clean trackingdir for CFS creation");
    }
    
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "create compound file");
    }
    // Now merge all added files    
    boolean success = false;
    try {
      info.getCodec().compoundFormat().write(directory, info, context);
      success = true;
    } finally {
      if (!success) {
        // Safe: these files must exist
        deleteFiles.accept(directory.getCreatedFiles());
      }
    }

    // Replace all previous files with the CFS/CFE files:
    info.setFiles(new HashSet<>(directory.getCreatedFiles()));
  }
  
  /**
   * Tries to delete the given files if unreferenced
   * @param files the files to delete
   * @throws IOException if an {@link IOException} occurs
   * @see IndexFileDeleter#deleteNewFiles(Collection)
   */
  private synchronized void deleteNewFiles(Collection<String> files) throws IOException {
    deleter.deleteNewFiles(files);
  }
  /**
   * Cleans up residuals from a segment that could not be entirely flushed due to an error
   */
  private synchronized void flushFailed(SegmentInfo info) throws IOException {
    // TODO: this really should be a tragic
    Collection<String> files;
    try {
      files = info.files();
    } catch (IllegalStateException ise) {
      // OK
      files = null;
    }
    if (files != null) {
      deleter.deleteNewFiles(files);
    }
  }

  /**
   * Publishes the flushed segment, segment-private deletes (if any) and its
   * associated global delete (if present) to IndexWriter.  The actual
   * publishing operation is synced on {@code IW -> BDS} so that the {@link SegmentInfo}'s
   * delete generation is always GlobalPacket_deleteGeneration + 1
   * @param forced if <code>true</code> this call will block on the ticket queue if the lock is held by another thread.
   *               if <code>false</code> the call will try to acquire the queue lock and exits if it's held by another thread.
   *
   */
  private void publishFlushedSegments(boolean forced) throws IOException {
    docWriter.purgeFlushTickets(forced, ticket -> {
      DocumentsWriterPerThread.FlushedSegment newSegment = ticket.getFlushedSegment();
      FrozenBufferedUpdates bufferedUpdates = ticket.getFrozenUpdates();
      ticket.markPublished();
      if (newSegment == null) { // this is a flushed global deletes package - not a segments
        if (bufferedUpdates != null && bufferedUpdates.any()) { // TODO why can this be null?
          publishFrozenUpdates(bufferedUpdates);
          if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "flush: push buffered updates: " + bufferedUpdates);
          }
        }
      } else {
        assert newSegment.segmentInfo != null;
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "publishFlushedSegment seg-private updates=" + newSegment.segmentUpdates);
        }
        if (newSegment.segmentUpdates != null && infoStream.isEnabled("DW")) {
          infoStream.message("IW", "flush: push buffered seg private updates: " + newSegment.segmentUpdates);
        }
        // now publish!
        publishFlushedSegment(newSegment.segmentInfo, newSegment.fieldInfos, newSegment.segmentUpdates,
            bufferedUpdates, newSegment.sortMap);
      }
    });
  }

  /** Record that the files referenced by this {@link SegmentInfos} are still in use.
   *
   * @lucene.internal */
  public synchronized void incRefDeleter(SegmentInfos segmentInfos) throws IOException {
    ensureOpen();
    deleter.incRef(segmentInfos, false);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "incRefDeleter for NRT reader version=" + segmentInfos.getVersion() + " segments=" + segString(segmentInfos));
    }
  }

  /** Record that the files referenced by this {@link SegmentInfos} are no longer in use.  Only call this if you are sure you previously
   *  called {@link #incRefDeleter}.
   *
  * @lucene.internal */
  public synchronized void decRefDeleter(SegmentInfos segmentInfos) throws IOException {
    ensureOpen();
    deleter.decRef(segmentInfos);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", "decRefDeleter for NRT reader version=" + segmentInfos.getVersion() + " segments=" + segString(segmentInfos));
    }
  }

  /**
   * Processes all events and might trigger a merge if the given seqNo is negative
   * @param seqNo if the seqNo is less than 0 this method will process events otherwise it's a no-op.
   * @return the given seqId inverted if negative.
   */
  private long maybeProcessEvents(long seqNo) throws IOException {
    if (seqNo < 0) {
      seqNo = -seqNo;
      processEvents(true);
    }
    return seqNo;
  }
  
  private void processEvents(boolean triggerMerge) throws IOException {
    if (tragedy.get() == null) {
     eventQueue.processEvents();
    }
    if (triggerMerge) {
      maybeMerge(getConfig().getMergePolicy(), MergeTrigger.SEGMENT_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
    }
  }

  /**
   * Interface for internal atomic events. See {@link DocumentsWriter} for details. Events are executed concurrently and no order is guaranteed.
   * Each event should only rely on the serializeability within its process method. All actions that must happen before or after a certain action must be
   * encoded inside the {@link #process(IndexWriter)} method.
   *
   */
  @FunctionalInterface
  interface Event {
    /**
     * Processes the event. This method is called by the {@link IndexWriter}
     * passed as the first argument.
     * 
     * @param writer
     *          the {@link IndexWriter} that executes the event.
     * @throws IOException
     *           if an {@link IOException} occurs
     */
    void process(IndexWriter writer) throws IOException;
  }

  /** Anything that will add N docs to the index should reserve first to
   *  make sure it's allowed.  This will throw {@code
   *  IllegalArgumentException} if it's not allowed. */ 
  private void reserveDocs(long addedNumDocs) {
    assert addedNumDocs >= 0;
    if (adjustPendingNumDocs(addedNumDocs) > actualMaxDocs) {
      // Reserve failed: put the docs back and throw exc:
      adjustPendingNumDocs(-addedNumDocs);
      tooManyDocs(addedNumDocs);
    }
  }

  /** Does a best-effort check, that the current index would accept this many additional docs, but does not actually reserve them.
   *
   * @throws IllegalArgumentException if there would be too many docs */
  private void testReserveDocs(long addedNumDocs) {
    assert addedNumDocs >= 0;
    if (pendingNumDocs.get() + addedNumDocs > actualMaxDocs) {
      tooManyDocs(addedNumDocs);
    }
  }

  private void tooManyDocs(long addedNumDocs) {
    assert addedNumDocs >= 0;
    throw new IllegalArgumentException("number of documents in the index cannot exceed " + actualMaxDocs + " (current document count is " + pendingNumDocs.get() + "; added numDocs is " + addedNumDocs + ")");
  }

  /**
   * Returns the number of documents in the index including documents are being added (i.e., reserved).
   * @lucene.experimental
   */
  public long getPendingNumDocs() {
    return pendingNumDocs.get();
  }

  /** Returns the highest <a href="#sequence_number">sequence number</a> across
   *  all completed operations, or 0 if no operations have finished yet.  Still
   *  in-flight operations (in other threads) are not counted until they finish.
   *
   * @lucene.experimental */
  public long getMaxCompletedSequenceNumber() {
    ensureOpen();
    return docWriter.getMaxCompletedSequenceNumber();
  }

  private long adjustPendingNumDocs(long numDocs) {
    long count = pendingNumDocs.addAndGet(numDocs);
    assert count >= 0 : "pendingNumDocs is negative: " + count;
    return count;
  }

  final boolean isFullyDeleted(ReadersAndUpdates readersAndUpdates) throws IOException {
    if (readersAndUpdates.isFullyDeleted()) {
      assert Thread.holdsLock(this);
      return readersAndUpdates.keepFullyDeletedSegment(config.getMergePolicy()) == false;
    }
    return false;
  }

  /**
   * Returns the number of deletes a merge would claim back if the given segment is merged.
   * @see MergePolicy#numDeletesToMerge(SegmentCommitInfo, int, org.apache.lucene.util.IOSupplier)
   * @param info the segment to get the number of deletes for
   * @lucene.experimental
   */
  @Override
  public final int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
    ensureOpen(false);
    validate(info);
    MergePolicy mergePolicy = config.getMergePolicy();
    final ReadersAndUpdates rld = getPooledInstance(info, false);
    int numDeletesToMerge;
    if (rld != null) {
      numDeletesToMerge = rld.numDeletesToMerge(mergePolicy);
    } else {
      // if we don't have a  pooled instance lets just return the hard deletes, this is safe!
      numDeletesToMerge = info.getDelCount();
    }
    assert numDeletesToMerge <= info.info.maxDoc() :
        "numDeletesToMerge: " + numDeletesToMerge + " > maxDoc: " + info.info.maxDoc();
    return numDeletesToMerge;
  }

  void release(ReadersAndUpdates readersAndUpdates) throws IOException {
    release(readersAndUpdates, true);
  }

  private void release(ReadersAndUpdates readersAndUpdates, boolean assertLiveInfo) throws IOException {
    assert Thread.holdsLock(this);
    if (readerPool.release(readersAndUpdates, assertLiveInfo)) {
      // if we write anything here we have to hold the lock otherwise IDF will delete files underneath us
      assert Thread.holdsLock(this);
      checkpointNoSIS();
    }
  }

  ReadersAndUpdates getPooledInstance(SegmentCommitInfo info, boolean create) {
    ensureOpen(false);
    return readerPool.get(info, create);
  }

// FrozenBufferedUpdates
  /**
   * Translates a frozen packet of delete term/query, or doc values
   * updates, into their actual docIDs in the index, and applies the change.  This is a heavy
   * operation and is done concurrently by incoming indexing threads.
   * This method will return immediately without blocking if another thread is currently
   * applying the package. In order to ensure the packet has been applied,
   * {@link IndexWriter#forceApply(FrozenBufferedUpdates)} must be called.
   */
  @SuppressWarnings("try")
  final boolean tryApply(FrozenBufferedUpdates updates) throws IOException {
    if (updates.tryLock()) {
      try {
        forceApply(updates);
        return true;
      } finally {
        updates.unlock();
      }
    }
    return false;
  }

  /**
   * Translates a frozen packet of delete term/query, or doc values
   * updates, into their actual docIDs in the index, and applies the change.  This is a heavy
   * operation and is done concurrently by incoming indexing threads.
   */
  final void forceApply(FrozenBufferedUpdates updates) throws IOException {
    updates.lock();
    try {
      if (updates.isApplied()) {
        // already done
        return;
      }
      long startNS = System.nanoTime();

      assert updates.any();

      Set<SegmentCommitInfo> seenSegments = new HashSet<>();

      int iter = 0;
      int totalSegmentCount = 0;
      long totalDelCount = 0;

      boolean finished = false;

      // Optimistic concurrency: assume we are free to resolve the deletes against all current segments in the index, despite that
      // concurrent merges are running.  Once we are done, we check to see if a merge completed while we were running.  If so, we must retry
      // resolving against the newly merged segment(s).  Eventually no merge finishes while we were running and we are done.
      while (true) {
        String messagePrefix;
        if (iter == 0) {
          messagePrefix = "";
        } else {
          messagePrefix = "iter " + iter;
        }

        long iterStartNS = System.nanoTime();

        long mergeGenStart = mergeFinishedGen.get();

        Set<String> delFiles = new HashSet<>();
        BufferedUpdatesStream.SegmentState[] segStates;

        synchronized (this) {
          List<SegmentCommitInfo> infos = getInfosToApply(updates);
          if (infos == null) {
            break;
          }

          for (SegmentCommitInfo info : infos) {
            delFiles.addAll(info.files());
          }

          // Must open while holding IW lock so that e.g. segments are not merged
          // away, dropped from 100% deletions, etc., before we can open the readers
          segStates = openSegmentStates(infos, seenSegments, updates.delGen());

          if (segStates.length == 0) {

            if (infoStream.isEnabled("BD")) {
              infoStream.message("BD", "packet matches no segments");
            }
            break;
          }

          if (infoStream.isEnabled("BD")) {
            infoStream.message("BD", String.format(Locale.ROOT,
                messagePrefix + "now apply del packet (%s) to %d segments, mergeGen %d",
                this, segStates.length, mergeGenStart));
          }

          totalSegmentCount += segStates.length;

          // Important, else IFD may try to delete our files while we are still using them,
          // if e.g. a merge finishes on some of the segments we are resolving on:
          deleter.incRef(delFiles);
        }

        AtomicBoolean success = new AtomicBoolean();
        long delCount;
        try (Closeable finalizer = () -> finishApply(segStates, success.get(), delFiles)) {
          assert finalizer != null; // access the finalizer to prevent a warning
          // don't hold IW monitor lock here so threads are free concurrently resolve deletes/updates:
          delCount = updates.apply(segStates);
          success.set(true);
        }

        // Since we just resolved some more deletes/updates, now is a good time to write them:
        writeSomeDocValuesUpdates();

        // It's OK to add this here, even if the while loop retries, because delCount only includes newly
        // deleted documents, on the segments we didn't already do in previous iterations:
        totalDelCount += delCount;

        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", String.format(Locale.ROOT,
              messagePrefix + "done inner apply del packet (%s) to %d segments; %d new deletes/updates; took %.3f sec",
              this, segStates.length, delCount, (System.nanoTime() - iterStartNS) / 1000000000.));
        }
        if (updates.privateSegment != null) {
          // No need to retry for a segment-private packet: the merge that folds in our private segment already waits for all deletes to
          // be applied before it kicks off, so this private segment must already not be in the set of merging segments

          break;
        }

        // Must sync on writer here so that IW.mergeCommit is not running concurrently, so that if we exit, we know mergeCommit will succeed
        // in pulling all our delGens into a merge:
        synchronized (this) {
          long mergeGenCur = mergeFinishedGen.get();

          if (mergeGenCur == mergeGenStart) {

            // Must do this while still holding IW lock else a merge could finish and skip carrying over our updates:

            // Record that this packet is finished:
            bufferedUpdatesStream.finished(updates);

            finished = true;

            // No merge finished while we were applying, so we are done!
            break;
          }
        }

        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", messagePrefix + "concurrent merges finished; move to next iter");
        }

        // A merge completed while we were running.  In this case, that merge may have picked up some of the updates we did, but not
        // necessarily all of them, so we cycle again, re-applying all our updates to the newly merged segment.

        iter++;
      }

      if (finished == false) {
        // Record that this packet is finished:
        bufferedUpdatesStream.finished(updates);
      }

      if (infoStream.isEnabled("BD")) {
        String message = String.format(Locale.ROOT,
            "done apply del packet (%s) to %d segments; %d new deletes/updates; took %.3f sec",
            this, totalSegmentCount, totalDelCount, (System.nanoTime() - startNS) / 1000000000.);
        if (iter > 0) {
          message += "; " + (iter + 1) + " iters due to concurrent merges";
        }
        message += "; " + bufferedUpdatesStream.getPendingUpdatesCount() + " packets remain";
        infoStream.message("BD", message);
      }
    } finally {
      updates.unlock();
    }
  }

  /** Returns the {@link SegmentCommitInfo} that this packet is supposed to apply its deletes to, or null
   *  if the private segment was already merged away. */
  private synchronized List<SegmentCommitInfo> getInfosToApply(FrozenBufferedUpdates updates) {
    final List<SegmentCommitInfo> infos;
    if (updates.privateSegment != null) {
      if (segmentInfos.contains(updates.privateSegment)) {
        infos = Collections.singletonList(updates.privateSegment);
      }else {
        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "private segment already gone; skip processing updates");
        }
        infos = null;
      }
    } else {
      infos = segmentInfos.asList();
    }
    return infos;
  }

  private void finishApply(BufferedUpdatesStream.SegmentState[] segStates,
                           boolean success, Set<String> delFiles) throws IOException {
    synchronized (this) {

      BufferedUpdatesStream.ApplyDeletesResult result;
      try {
        result = closeSegmentStates(segStates, success);
      } finally {
        // Matches the incRef we did above, but we must do the decRef after closing segment states else
        // IFD can't delete still-open files
        deleter.decRef(delFiles);
      }

      if (result.anyDeletes) {
        maybeMerge.set(true);
        checkpoint();
      }

      if (result.allDeleted != null) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "drop 100% deleted segments: " + segString(result.allDeleted));
        }
        for (SegmentCommitInfo info : result.allDeleted) {
          dropDeletedSegment(info);
        }
        checkpoint();
      }
    }
  }

  /** Close segment states previously opened with openSegmentStates. */
  private BufferedUpdatesStream.ApplyDeletesResult closeSegmentStates(BufferedUpdatesStream.SegmentState[] segStates, boolean success) throws IOException {
    List<SegmentCommitInfo> allDeleted = null;
    long totDelCount = 0;
    try {
      for (BufferedUpdatesStream.SegmentState segState : segStates) {
        if (success) {
          totDelCount += segState.rld.getDelCount() - segState.startDelCount;
          int fullDelCount = segState.rld.getDelCount();
          assert fullDelCount <= segState.rld.info.info.maxDoc() : fullDelCount + " > " + segState.rld.info.info.maxDoc();
          if (segState.rld.isFullyDeleted() && getConfig().getMergePolicy().keepFullyDeletedSegment(() -> segState.reader) == false) {
            if (allDeleted == null) {
              allDeleted = new ArrayList<>();
            }
            allDeleted.add(segState.reader.getOriginalSegmentInfo());
          }
        }
      }
    } finally {
      IOUtils.close(segStates);
    }
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "closeSegmentStates: " + totDelCount + " new deleted documents; pool " + bufferedUpdatesStream.getPendingUpdatesCount() +  " packets; bytesUsed=" + readerPool.ramBytesUsed());
    }

    return new BufferedUpdatesStream.ApplyDeletesResult(totDelCount > 0, allDeleted);
  }

  /** Opens SegmentReader and inits SegmentState for each segment. */
  private BufferedUpdatesStream.SegmentState[] openSegmentStates(List<SegmentCommitInfo> infos,
                                                                 Set<SegmentCommitInfo> alreadySeenSegments, long delGen) throws IOException {
    List<BufferedUpdatesStream.SegmentState> segStates = new ArrayList<>();
    try {
      for (SegmentCommitInfo info : infos) {
        if (info.getBufferedDeletesGen() <= delGen && alreadySeenSegments.contains(info) == false) {
          segStates.add(new BufferedUpdatesStream.SegmentState(getPooledInstance(info, true), this::release, info));
          alreadySeenSegments.add(info);
        }
      }
    } catch (Throwable t) {
      try {
        IOUtils.close(segStates);
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }

    return segStates.toArray(new BufferedUpdatesStream.SegmentState[0]);
  }

  /**
   * Tests should override this to enable test points. Default is <code>false</code>.
   */
  protected boolean isEnableTestPoints() {
    return false;
  }

  private void validate(SegmentCommitInfo info) {
    if (info.info.dir != directoryOrig) {
      throw new IllegalArgumentException("SegmentCommitInfo must be from the same directory");
    }
  }

  /** Tests should use this method to snapshot the current segmentInfos to have a consistent view */
  final synchronized SegmentInfos cloneSegmentInfos() {
    return segmentInfos.clone();
  }

  /**
   * Returns accurate {@link DocStats} form this writer. The numDoc for instance can change after maxDoc is fetched
   * that causes numDocs to be greater than maxDoc which makes it hard to get accurate document stats from IndexWriter.
   */
  public synchronized DocStats getDocStats() {
    ensureOpen();
    int numDocs = docWriter.getNumDocs();
    int maxDoc = numDocs;
    for (final SegmentCommitInfo info : segmentInfos) {
      maxDoc += info.info.maxDoc();
      numDocs += info.info.maxDoc() - numDeletedDocs(info);
    }
    assert maxDoc >= numDocs : "maxDoc is less than numDocs: " + maxDoc + " < " + numDocs;
    return new DocStats(maxDoc, numDocs);
  }

  /**
   * DocStats for this index
   */
  public static final class DocStats {
    /**
     * The total number of docs in this index, including
     * docs not yet flushed (still in the RAM buffer),
     * not counting deletions.
     */
    public final int maxDoc;
    /**
     * The total number of docs in this index, including
     * docs not yet flushed (still in the RAM buffer), and
     * including deletions.  <b>NOTE:</b> buffered deletions
     * are not counted.  If you really need these to be
     * counted you should call {@link IndexWriter#commit()} first.
     */
    public final int numDocs;

    private DocStats(int maxDoc, int numDocs) {
      this.maxDoc = maxDoc;
      this.numDocs = numDocs;
    }
  }

  private static class IndexWriterMergeSource implements MergeScheduler.MergeSource {
    private final IndexWriter writer;

    private IndexWriterMergeSource(IndexWriter writer) {
      this.writer = writer;
    }

    @Override
    public MergePolicy.OneMerge getNextMerge() {
      MergePolicy.OneMerge nextMerge = writer.getNextMerge();
      if (nextMerge != null) {
        if (writer.mergeScheduler.verbose()) {
          writer.mergeScheduler.message("  checked out merge " + writer.segString(nextMerge.segments));
        }
      }
      return nextMerge;
    }

    @Override
    public void onMergeFinished(MergePolicy.OneMerge merge) {
      writer.mergeFinish(merge);
    }

    @Override
    public boolean hasPendingMerges() {
      return writer.hasPendingMerges();
    }

    @Override
    public void merge(MergePolicy.OneMerge merge) throws IOException {
      assert Thread.holdsLock(writer) == false;
      writer.merge(merge);
    }

    public String toString() {
      return writer.segString();
    }
  }

  private class Merges {
    private boolean mergesEnabled = true;

    boolean areEnabled() {
      assert Thread.holdsLock(IndexWriter.this);
      return mergesEnabled;
    }

    void disable() {
      assert Thread.holdsLock(IndexWriter.this);
      mergesEnabled = false;
    }

    void enable() {
      ensureOpen();
      assert Thread.holdsLock(IndexWriter.this);
      mergesEnabled = true;
    }
  }
}
