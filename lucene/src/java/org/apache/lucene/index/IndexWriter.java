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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.PayloadProcessorProvider.DirPayloadProcessor;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Closeable;
import java.io.PrintStream;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;

/**
  An <code>IndexWriter</code> creates and maintains an index.

  <p>The <code>create</code> argument to the {@link
  #IndexWriter(Directory, Analyzer, boolean, MaxFieldLength) constructor} determines 
  whether a new index is created, or whether an existing index is
  opened.  Note that you can open an index with <code>create=true</code>
  even while readers are using the index.  The old readers will 
  continue to search the "point in time" snapshot they had opened, 
  and won't see the newly created index until they re-open.  There are
  also {@link #IndexWriter(Directory, Analyzer, MaxFieldLength) constructors}
  with no <code>create</code> argument which will create a new index
  if there is not already an index at the provided path and otherwise 
  open the existing index.</p>

  <p>In either case, documents are added with {@link #addDocument(Document)
  addDocument} and removed with {@link #deleteDocuments(Term)} or {@link
  #deleteDocuments(Query)}. A document can be updated with {@link
  #updateDocument(Term, Document) updateDocument} (which just deletes
  and then adds the entire document). When finished adding, deleting 
  and updating documents, {@link #close() close} should be called.</p>

  <a name="flush"></a>
  <p>These changes are buffered in memory and periodically
  flushed to the {@link Directory} (during the above method
  calls).  A flush is triggered when there are enough
  buffered deletes (see {@link #setMaxBufferedDeleteTerms})
  or enough added documents since the last flush, whichever
  is sooner.  For the added documents, flushing is triggered
  either by RAM usage of the documents (see {@link
  #setRAMBufferSizeMB}) or the number of added documents.
  The default is to flush when RAM usage hits 16 MB.  For
  best indexing speed you should flush by RAM usage with a
  large RAM buffer.  Note that flushing just moves the
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
public class IndexWriter implements Closeable {

  /**
   * Default value for the write lock timeout (1,000).
   * @see #setDefaultWriteLockTimeout
   * @deprecated use {@link IndexWriterConfig#WRITE_LOCK_TIMEOUT} instead
   */
  public static long WRITE_LOCK_TIMEOUT = IndexWriterConfig.WRITE_LOCK_TIMEOUT;

  private long writeLockTimeout;

  /**
   * Name of the write lock in the index.
   */
  public static final String WRITE_LOCK_NAME = "write.lock";

  /**
   * Value to denote a flush trigger is disabled
   * @deprecated use {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} instead
   */
  public final static int DISABLE_AUTO_FLUSH = IndexWriterConfig.DISABLE_AUTO_FLUSH;

  /**
   * Disabled by default (because IndexWriter flushes by RAM usage
   * by default). Change using {@link #setMaxBufferedDocs(int)}.
   * @deprecated use {@link IndexWriterConfig#DEFAULT_MAX_BUFFERED_DOCS} instead.
   */
  public final static int DEFAULT_MAX_BUFFERED_DOCS = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;

  /**
   * Default value is 16 MB (which means flush when buffered
   * docs consume 16 MB RAM).  Change using {@link #setRAMBufferSizeMB}.
   * @deprecated use {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB} instead.
   */
  public final static double DEFAULT_RAM_BUFFER_SIZE_MB = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;

  /**
   * Disabled by default (because IndexWriter flushes by RAM usage
   * by default). Change using {@link #setMaxBufferedDeleteTerms(int)}.
   * @deprecated use {@link IndexWriterConfig#DEFAULT_MAX_BUFFERED_DELETE_TERMS} instead
   */
  public final static int DEFAULT_MAX_BUFFERED_DELETE_TERMS = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS;

  /**
   * Default value is 10,000. Change using {@link #setMaxFieldLength(int)}.
   * 
   * @deprecated see {@link IndexWriterConfig}
   */
  public final static int DEFAULT_MAX_FIELD_LENGTH = 10000;

  /**
   * Default value is 128. Change using {@link #setTermIndexInterval(int)}.
   * @deprecated use {@link IndexWriterConfig#DEFAULT_TERM_INDEX_INTERVAL} instead.
   */
  public final static int DEFAULT_TERM_INDEX_INTERVAL = IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL;

  /**
   * Absolute hard maximum length for a term, in bytes once
   * encoded as UTF8.  If a term arrives from the analyzer
   * longer than this length, it is skipped and a message is
   * printed to infoStream, if set (see {@link
   * #setInfoStream}).
   */
  public final static int MAX_TERM_LENGTH = DocumentsWriter.MAX_TERM_LENGTH_UTF8;

  // The normal read buffer size defaults to 1024, but
  // increasing this during merging seems to yield
  // performance gains.  However we don't want to increase
  // it too much because there are quite a few
  // BufferedIndexInputs created during merging.  See
  // LUCENE-888 for details.
  private final static int MERGE_READ_BUFFER_SIZE = 4096;

  // Used for printing messages
  private static Object MESSAGE_ID_LOCK = new Object();
  private static int MESSAGE_ID = 0;
  private int messageID = -1;
  volatile private boolean hitOOM;

  private final Directory directory;  // where this index resides
  private final Analyzer analyzer;    // how to analyze text

  // TODO 4.0: this should be made final once the setter is out
  private /*final*/Similarity similarity = Similarity.getDefault(); // how to normalize

  private volatile long changeCount; // increments every time a change is completed
  private long lastCommitChangeCount; // last changeCount that was committed

  private SegmentInfos rollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails
  private HashMap<SegmentInfo,Integer> rollbackSegments;

  volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
  volatile long pendingCommitChangeCount;

  private SegmentInfos segmentInfos = new SegmentInfos();       // the segments

  private DocumentsWriter docWriter;
  private IndexFileDeleter deleter;

  private Set<SegmentInfo> segmentsToOptimize = new HashSet<SegmentInfo>();           // used by optimize to note those needing optimization

  private Lock writeLock;

  // TODO 4.0: this should be made final once the setter is out
  private /*final*/int termIndexInterval;

  private boolean closed;
  private boolean closing;

  // Holds all SegmentInfo instances currently involved in
  // merges
  private HashSet<SegmentInfo> mergingSegments = new HashSet<SegmentInfo>();

  private MergePolicy mergePolicy;
  // TODO 4.0: this should be made final once the setter is removed
  private /*final*/MergeScheduler mergeScheduler;
  private LinkedList<MergePolicy.OneMerge> pendingMerges = new LinkedList<MergePolicy.OneMerge>();
  private Set<MergePolicy.OneMerge> runningMerges = new HashSet<MergePolicy.OneMerge>();
  private List<MergePolicy.OneMerge> mergeExceptions = new ArrayList<MergePolicy.OneMerge>();
  private long mergeGen;
  private boolean stopMerges;

  private int flushCount;
  private int flushDeletesCount;

  final ReaderPool readerPool = new ReaderPool();
  
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

  /**
   * Expert: returns a readonly reader, covering all
   * committed as well as un-committed changes to the index.
   * This provides "near real-time" searching, in that
   * changes made during an IndexWriter session can be
   * quickly made available for searching without closing
   * the writer nor calling {@link #commit}.
   *
   * <p>Note that this is functionally equivalent to calling
   * {#commit} and then using {@link IndexReader#open} to
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
  public IndexReader getReader() throws IOException {
    return getReader(config.getReaderTermsIndexDivisor());
  }

  /** Expert: like {@link #getReader}, except you can
   *  specify which termInfosIndexDivisor should be used for
   *  any newly opened readers.
   * @param termInfosIndexDivisor Subsamples which indexed
   *  terms are loaded into RAM. This has the same effect as {@link
   *  IndexWriter#setTermIndexInterval} except that setting
   *  must be done at indexing time while this setting can be
   *  set per reader.  When set to N, then one in every
   *  N*termIndexInterval terms in the index is loaded into
   *  memory.  By setting this to a value > 1 you can reduce
   *  memory usage, at the expense of higher latency when
   *  loading a TermInfo.  The default value is 1.  Set this
   *  to -1 to skip loading the terms index entirely. */
  public IndexReader getReader(int termInfosIndexDivisor) throws IOException {

    ensureOpen();

    if (infoStream != null) {
      message("flush at getReader");
    }

    // Do this up front before flushing so that the readers
    // obtained during this flush are pooled, the first time
    // this method is called:
    poolReaders = true;

    flush(true, true, false);
    
    // Prevent segmentInfos from changing while opening the
    // reader; in theory we could do similar retry logic,
    // just like we do when loading segments_N
    synchronized(this) {
      applyDeletes();
      final IndexReader r = new ReadOnlyDirectoryReader(this, segmentInfos, termInfosIndexDivisor, codecs);
      if (infoStream != null) {
        message("return reader version=" + r.getVersion() + " reader=" + r);
      }
      return r;
    }
  }

  /** Holds shared SegmentReader instances. IndexWriter uses
   *  SegmentReaders for 1) applying deletes, 2) doing
   *  merges, 3) handing out a real-time reader.  This pool
   *  reuses instances of the SegmentReaders in all these
   *  places if it is in "near real-time mode" (getReader()
   *  has been called on this instance). */

  class ReaderPool {

    private final Map<SegmentInfo,SegmentReader> readerMap = new HashMap<SegmentInfo,SegmentReader>();

    /** Forcefully clear changes for the specified segments,
     *  and remove from the pool.   This is called on successful merge. */
    synchronized void clear(SegmentInfos infos) throws IOException {
      if (infos == null) {
        for (Map.Entry<SegmentInfo,SegmentReader> ent: readerMap.entrySet()) {
          ent.getValue().hasChanges = false;
        }
      } else {
        for (final SegmentInfo info: infos) {
          if (readerMap.containsKey(info)) {
            readerMap.get(info).hasChanges = false;
          }
        }     
      }
    }
    
    // used only by asserts
    public synchronized boolean infoIsLive(SegmentInfo info) {
      int idx = segmentInfos.indexOf(info);
      assert idx != -1;
      assert segmentInfos.get(idx) == info;
      return true;
    }

    public synchronized SegmentInfo mapToLive(SegmentInfo info) {
      int idx = segmentInfos.indexOf(info);
      if (idx != -1) {
        info = segmentInfos.get(idx);
      }
      return info;
    }
    
    /**
     * Release the segment reader (i.e. decRef it and close if there
     * are no more references.
     * @param sr
     * @throws IOException
     */
    public synchronized void release(SegmentReader sr) throws IOException {
      release(sr, false);
    }
    
    /**
     * Release the segment reader (i.e. decRef it and close if there
     * are no more references.
     * @param sr
     * @throws IOException
     */
    public synchronized void release(SegmentReader sr, boolean drop) throws IOException {

      final boolean pooled = readerMap.containsKey(sr.getSegmentInfo());

      assert !pooled || readerMap.get(sr.getSegmentInfo()) == sr;

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
        sr.hasChanges &= !drop;

        final boolean hasChanges = sr.hasChanges;

        // Drop our ref -- this will commit any pending
        // changes to the dir
        sr.close();

        // We are the last ref to this reader; since we're
        // not pooling readers, we release it:
        readerMap.remove(sr.getSegmentInfo());

        if (hasChanges) {
          // Must checkpoint w/ deleter, because this
          // segment reader will have created new _X_N.del
          // file.
          deleter.checkpoint(segmentInfos, false);
        }
      }
    }
    
    /** Remove all our references to readers, and commits
     *  any pending changes. */
    synchronized void close() throws IOException {
      // We invoke deleter.checkpoint below, so we must be
      // sync'd on IW:
      assert Thread.holdsLock(IndexWriter.this);

      Iterator<Map.Entry<SegmentInfo,SegmentReader>> iter = readerMap.entrySet().iterator();
      while (iter.hasNext()) {
        
        Map.Entry<SegmentInfo,SegmentReader> ent = iter.next();

        SegmentReader sr = ent.getValue();
        if (sr.hasChanges) {
          assert infoIsLive(sr.getSegmentInfo());
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
    synchronized void commit() throws IOException {

      // We invoke deleter.checkpoint below, so we must be
      // sync'd on IW:
      assert Thread.holdsLock(IndexWriter.this);

      for (Map.Entry<SegmentInfo,SegmentReader> ent : readerMap.entrySet()) {

        SegmentReader sr = ent.getValue();
        if (sr.hasChanges) {
          assert infoIsLive(sr.getSegmentInfo());
          sr.doCommit(null);

          // Must checkpoint w/ deleter, because this
          // segment reader will have created new _X_N.del
          // file.
          deleter.checkpoint(segmentInfos, false);
        }
      }
    }
    
    /**
     * Returns a ref to a clone.  NOTE: this clone is not
     * enrolled in the pool, so you should simply close()
     * it when you're done (ie, do not call release()).
     */
    public synchronized SegmentReader getReadOnlyClone(SegmentInfo info, boolean doOpenStores, int termInfosIndexDivisor) throws IOException {
      SegmentReader sr = get(info, doOpenStores, BufferedIndexInput.BUFFER_SIZE, termInfosIndexDivisor);
      try {
        return (SegmentReader) sr.clone(true);
      } finally {
        sr.decRef();
      }
    }
   
    /**
     * Obtain a SegmentReader from the readerPool.  The reader
     * must be returned by calling {@link #release(SegmentReader)}
     * @see #release(SegmentReader)
     * @param info
     * @param doOpenStores
     * @throws IOException
     */
    public synchronized SegmentReader get(SegmentInfo info, boolean doOpenStores) throws IOException {
      return get(info, doOpenStores, BufferedIndexInput.BUFFER_SIZE, config.getReaderTermsIndexDivisor());
    }

    /**
     * Obtain a SegmentReader from the readerPool.  The reader
     * must be returned by calling {@link #release(SegmentReader)}
     * 
     * @see #release(SegmentReader)
     * @param info
     * @param doOpenStores
     * @param readBufferSize
     * @param termsIndexDivisor
     * @throws IOException
     */
    public synchronized SegmentReader get(SegmentInfo info, boolean doOpenStores, int readBufferSize, int termsIndexDivisor) throws IOException {

      if (poolReaders) {
        readBufferSize = BufferedIndexInput.BUFFER_SIZE;
      }

      SegmentReader sr = readerMap.get(info);
      if (sr == null) {
        // TODO: we may want to avoid doing this while
        // synchronized
        // Returns a ref, which we xfer to readerMap:
        sr = SegmentReader.get(false, info.dir, info, readBufferSize, doOpenStores, termsIndexDivisor, codecs);

        if (info.dir == directory) {
          // Only pool if reader is not external
          readerMap.put(info, sr);
        }
      } else {
        if (doOpenStores) {
          sr.openDocStores();
        }
        if (termsIndexDivisor != -1) {
          // If this reader was originally opened because we
          // needed to merge it, we didn't load the terms
          // index.  But now, if the caller wants the terms
          // index (eg because it's doing deletes, or an NRT
          // reader is being opened) we ask the reader to
          // load its terms index.
          sr.loadTermsIndex(termsIndexDivisor);
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
      SegmentReader sr = readerMap.get(info);
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
        return reader.numDeletedDocs();
      } else {
        return info.getDelCount();
      }
    } finally {
      if (reader != null) {
        readerPool.release(reader);
      }
    }
  }
  
  /**
   * Used internally to throw an {@link
   * AlreadyClosedException} if this IndexWriter has been
   * closed.
   * @throws AlreadyClosedException if this IndexWriter is
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
      infoStream.println("IW " + messageID + " [" + Thread.currentThread().getName() + "]: " + message);
  }

  private synchronized void setMessageID(PrintStream infoStream) {
    if (infoStream != null && messageID == -1) {
      synchronized(MESSAGE_ID_LOCK) {
        messageID = MESSAGE_ID++;
      }
    }
    this.infoStream = infoStream;
  }

  /**
   * Casts current mergePolicy to LogMergePolicy, and throws
   * an exception if the mergePolicy is not a LogMergePolicy.
   */
  private LogMergePolicy getLogMergePolicy() {
    if (mergePolicy instanceof LogMergePolicy)
      return (LogMergePolicy) mergePolicy;
    else
      throw new IllegalArgumentException("this method can only be called when the merge policy is the default LogMergePolicy");
  }

  /** <p>Get the current setting of whether newly flushed
   *  segments will use the compound file format.  Note that
   *  this just returns the value previously set with
   *  setUseCompoundFile(boolean), or the default value
   *  (true).  You cannot use this to query the status of
   *  previously flushed segments.</p>
   *
   *  <p>Note that this method is a convenience method: it
   *  just calls mergePolicy.getUseCompoundFile as long as
   *  mergePolicy is an instance of {@link LogMergePolicy}.
   *  Otherwise an IllegalArgumentException is thrown.</p>
   *
   *  @see #setUseCompoundFile(boolean)
   *  @deprecated use {@link LogMergePolicy#getUseCompoundDocStore()} and
   *  {@link LogMergePolicy#getUseCompoundFile()} directly.
   */
  public boolean getUseCompoundFile() {
    return getLogMergePolicy().getUseCompoundFile();
  }

  /**
   * <p>
   * Setting to turn on usage of a compound file. When on, multiple files for
   * each segment are merged into a single file when a new segment is flushed.
   * </p>
   * 
   * <p>
   * Note that this method is a convenience method: it just calls
   * mergePolicy.setUseCompoundFile as long as mergePolicy is an instance of
   * {@link LogMergePolicy}. Otherwise an IllegalArgumentException is thrown.
   * </p>
   * 
   * @deprecated use {@link LogMergePolicy#setUseCompoundDocStore(boolean)} and
   *             {@link LogMergePolicy#setUseCompoundFile(boolean)} directly.
   *             Note that this method set the given value on both, therefore
   *             you should consider doing the same.
   */
  public void setUseCompoundFile(boolean value) {
    getLogMergePolicy().setUseCompoundFile(value);
    getLogMergePolicy().setUseCompoundDocStore(value);
  }

  /** Expert: Set the Similarity implementation used by this IndexWriter.
   *
   * @see Similarity#setDefault(Similarity)
   * @deprecated use {@link IndexWriterConfig#setSimilarity(Similarity)} instead
   */
  public void setSimilarity(Similarity similarity) {
    ensureOpen();
    this.similarity = similarity;
    docWriter.setSimilarity(similarity);
    // Required so config.getSimilarity returns the right value. But this will
    // go away together with the method in 4.0.
    config.setSimilarity(similarity);
  }

  /** Expert: Return the Similarity implementation used by this IndexWriter.
   *
   * <p>This defaults to the current value of {@link Similarity#getDefault()}.
   * @deprecated use {@link IndexWriterConfig#getSimilarity()} instead
   */
  public Similarity getSimilarity() {
    ensureOpen();
    return similarity;
  }

  /** Expert: Set the interval between indexed terms.  Large values cause less
   * memory to be used by IndexReader, but slow random-access to terms.  Small
   * values cause more memory to be used by an IndexReader, and speed
   * random-access to terms.
   *
   * This parameter determines the amount of computation required per query
   * term, regardless of the number of documents that contain that term.  In
   * particular, it is the maximum number of other terms that must be
   * scanned before a term is located and its frequency and position information
   * may be processed.  In a large index with user-entered query terms, query
   * processing time is likely to be dominated not by term lookup but rather
   * by the processing of frequency and positional data.  In a small index
   * or when many uncommon query terms are generated (e.g., by wildcard
   * queries) term lookup may become a dominant cost.
   *
   * In particular, <code>numUniqueTerms/interval</code> terms are read into
   * memory by an IndexReader, and, on average, <code>interval/2</code> terms
   * must be scanned for each random term access.
   *
   * @see #DEFAULT_TERM_INDEX_INTERVAL
   * @deprecated use {@link IndexWriterConfig#setTermIndexInterval(int)}
   */
  public void setTermIndexInterval(int interval) {
    ensureOpen();
    this.termIndexInterval = interval;
    // Required so config.getTermIndexInterval returns the right value. But this
    // will go away together with the method in 4.0.
    config.setTermIndexInterval(interval);
  }

  /** Expert: Return the interval between indexed terms.
   *
   * @see #setTermIndexInterval(int)
   * @deprecated use {@link IndexWriterConfig#getTermIndexInterval()}
   */
  public int getTermIndexInterval() {
    // We pass false because this method is called by SegmentMerger while we are in the process of closing
    ensureOpen(false);
    return termIndexInterval;
  }

  /**
   * Constructs an IndexWriter for the index in <code>d</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>d</code>, replacing the index already there, if any.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param mfl Maximum field length in number of terms/tokens: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   *  @deprecated use {@link #IndexWriter(Directory, IndexWriterConfig)} instead
   */
  public IndexWriter(Directory d, Analyzer a, boolean create, MaxFieldLength mfl)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    this(d, new IndexWriterConfig(Version.LUCENE_31, a).setOpenMode(
        create ? OpenMode.CREATE : OpenMode.APPEND).setMaxFieldLength(
        mfl.getLimit()));
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>d</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param mfl Maximum field length in number of terms/tokens: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   *  @deprecated use {@link #IndexWriter(Directory, IndexWriterConfig)} instead
   */
  public IndexWriter(Directory d, Analyzer a, MaxFieldLength mfl)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    this(d, new IndexWriterConfig(Version.LUCENE_31, a)
        .setMaxFieldLength(mfl.getLimit()));
  }

  /**
   * Expert: constructs an IndexWriter with a custom {@link
   * IndexDeletionPolicy}, for the index in <code>d</code>,
   * first creating it if it does not already exist.  Text
   * will be analyzed with <code>a</code>.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param deletionPolicy see <a href="#deletionPolicy">above</a>
   * @param mfl whether or not to limit field lengths
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   *  @deprecated use {@link #IndexWriter(Directory, IndexWriterConfig)} instead
   */
  public IndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    this(d, new IndexWriterConfig(Version.LUCENE_31, a).setMaxFieldLength(
        mfl.getLimit()).setIndexDeletionPolicy(deletionPolicy));
  }

  /**
   * Expert: constructs an IndexWriter with a custom {@link
   * IndexDeletionPolicy}, for the index in <code>d</code>.
   * Text will be analyzed with <code>a</code>.  If
   * <code>create</code> is true, then a new, empty index
   * will be created in <code>d</code>, replacing the index
   * already there, if any.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param deletionPolicy see <a href="#deletionPolicy">above</a>
   * @param mfl {@link org.apache.lucene.index.IndexWriter.MaxFieldLength}, whether or not to limit field lengths.  Value is in number of terms/tokens
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   *  @deprecated use {@link #IndexWriter(Directory, IndexWriterConfig)} instead
   */
  public IndexWriter(Directory d, Analyzer a, boolean create, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    this(d, new IndexWriterConfig(Version.LUCENE_31, a).setOpenMode(
        create ? OpenMode.CREATE : OpenMode.APPEND).setMaxFieldLength(
        mfl.getLimit()).setIndexDeletionPolicy(deletionPolicy));
  }
  
  /**
   * Expert: constructs an IndexWriter on specific commit
   * point, with a custom {@link IndexDeletionPolicy}, for
   * the index in <code>d</code>.  Text will be analyzed
   * with <code>a</code>.
   *
   * <p> This is only meaningful if you've used a {@link
   * IndexDeletionPolicy} in that past that keeps more than
   * just the last commit.
   * 
   * <p>This operation is similar to {@link #rollback()},
   * except that method can only rollback what's been done
   * with the current instance of IndexWriter since its last
   * commit, whereas this method can rollback to an
   * arbitrary commit point from the past, assuming the
   * {@link IndexDeletionPolicy} has preserved past
   * commits.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param deletionPolicy see <a href="#deletionPolicy">above</a>
   * @param mfl whether or not to limit field lengths, value is in number of terms/tokens.  See {@link org.apache.lucene.index.IndexWriter.MaxFieldLength}.
   * @param commit which commit to open
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   *  @deprecated use {@link #IndexWriter(Directory, IndexWriterConfig)} instead
   */
  public IndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IndexCommit commit)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    this(d, new IndexWriterConfig(Version.LUCENE_31, a)
        .setOpenMode(OpenMode.APPEND).setMaxFieldLength(mfl.getLimit())
        .setIndexDeletionPolicy(deletionPolicy).setIndexCommit(commit));
  }
  
  CodecProvider codecs;

  /**
   * Constructs a new IndexWriter per the settings given in <code>conf</code>.
   * Note that the passed in {@link IndexWriterConfig} is cloned and thus making
   * changes to it after IndexWriter has been instantiated will not affect
   * IndexWriter. Additionally, calling {@link #getConfig()} and changing the
   * parameters does not affect that IndexWriter instance.
   * <p>
   * <b>NOTE:</b> by default, {@link IndexWriterConfig#getMaxFieldLength()}
   * returns {@link IndexWriterConfig#UNLIMITED_FIELD_LENGTH}. Pay attention to
   * whether this setting fits your application.
   * 
   * @param d
   *          the index directory. The index is either created or appended
   *          according <code>conf.getOpenMode()</code>.
   * @param conf
   *          the configuration settings according to which IndexWriter should
   *          be initalized.
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
    setMessageID(defaultInfoStream);
    maxFieldLength = conf.getMaxFieldLength();
    termIndexInterval = conf.getTermIndexInterval();
    writeLockTimeout = conf.getWriteLockTimeout();
    similarity = conf.getSimilarity();
    mergePolicy = conf.getMergePolicy();
    mergePolicy.setIndexWriter(this);
    mergeScheduler = conf.getMergeScheduler();
    mergedSegmentWarmer = conf.getMergedSegmentWarmer();
    codecs = conf.getCodecProvider();
    
    poolReaders = conf.getReaderPooling();

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

    if (create) {
      // Clear the write lock in case it's leftover:
      directory.clearLock(WRITE_LOCK_NAME);
    }

    writeLock = directory.makeLock(WRITE_LOCK_NAME);

    if (!writeLock.obtain(writeLockTimeout)) // obtain write lock
      throw new LockObtainFailedException("Index locked for write: " + writeLock);

    boolean success = false;

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
          SegmentInfos oldInfos = new SegmentInfos();
          oldInfos.read(directory, commit.getSegmentsFileName(), codecs);
          segmentInfos.replace(oldInfos);
          changeCount++;
          if (infoStream != null)
            message("init: loaded commit \"" + commit.getSegmentsFileName() + "\"");
        }
      }

      setRollbackSegmentInfos(segmentInfos);

      docWriter = new DocumentsWriter(directory, this, conf.getIndexingChain(), conf.getMaxThreadStates());
      docWriter.setInfoStream(infoStream);
      docWriter.setMaxFieldLength(maxFieldLength);

      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:
      deleter = new IndexFileDeleter(directory,
                                     conf.getIndexDeletionPolicy(),
                                     segmentInfos, infoStream, docWriter, this.codecs);

      if (deleter.startingCommitDeleted)
        // Deletion policy deleted the "head" commit point.
        // We have to mark ourself as changed so that if we
        // are closed w/o any further changes we write a new
        // segments_N file.
        changeCount++;

      docWriter.setMaxBufferedDeleteTerms(conf.getMaxBufferedDeleteTerms());
      docWriter.setRAMBufferSizeMB(conf.getRAMBufferSizeMB());
      docWriter.setMaxBufferedDocs(conf.getMaxBufferedDocs());
      pushMaxBufferedDocs();

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
  
  private synchronized void setRollbackSegmentInfos(SegmentInfos infos) {
    rollbackSegmentInfos = (SegmentInfos) infos.clone();
    rollbackSegments = new HashMap<SegmentInfo,Integer>();
    final int size = rollbackSegmentInfos.size();
    for(int i=0;i<size;i++)
      rollbackSegments.put(rollbackSegmentInfos.info(i), Integer.valueOf(i));
  }

  /**
   * Returns the {@link IndexWriterConfig} that was passed to
   * {@link #IndexWriter(Directory, IndexWriterConfig)}. This allows querying
   * IndexWriter's settings.
   * <p>
   * <b>NOTE:</b> setting any parameter on the returned instance has not effect
   * on the IndexWriter instance. If you need to change those settings after
   * IndexWriter has been created, you need to instantiate a new IndexWriter.
   */
  public IndexWriterConfig getConfig() {
    return config;
  }
  
  /**
   * Expert: set the merge policy used by this writer.
   * 
   * @deprecated use {@link IndexWriterConfig#setMergePolicy(MergePolicy)} instead.
   */
  public void setMergePolicy(MergePolicy mp) {
    ensureOpen();
    if (mp == null)
      throw new NullPointerException("MergePolicy must be non-null");

    if (mergePolicy != mp)
      mergePolicy.close();
    mergePolicy = mp;
    mergePolicy.setIndexWriter(this);
    pushMaxBufferedDocs();
    if (infoStream != null)
      message("setMergePolicy " + mp);
    // Required so config.getMergePolicy returns the right value. But this will
    // go away together with the method in 4.0.
    config.setMergePolicy(mp);
  }

  /**
   * Expert: returns the current MergePolicy in use by this writer.
   * @see #setMergePolicy
   * 
   * @deprecated use {@link IndexWriterConfig#getMergePolicy()} instead
   */
  public MergePolicy getMergePolicy() {
    ensureOpen();
    return mergePolicy;
  }

  /**
   * Expert: set the merge scheduler used by this writer.
   * @deprecated use {@link IndexWriterConfig#setMergeScheduler(MergeScheduler)} instead
   */
  synchronized public void setMergeScheduler(MergeScheduler mergeScheduler) throws CorruptIndexException, IOException {
    ensureOpen();
    if (mergeScheduler == null)
      throw new NullPointerException("MergeScheduler must be non-null");

    if (this.mergeScheduler != mergeScheduler) {
      finishMerges(true);
      this.mergeScheduler.close();
    }
    this.mergeScheduler = mergeScheduler;
    if (infoStream != null)
      message("setMergeScheduler " + mergeScheduler);
    // Required so config.getMergeScheduler returns the right value. But this will
    // go away together with the method in 4.0.
    config.setMergeScheduler(mergeScheduler);
  }

  /**
   * Expert: returns the current MergeScheduler in use by this
   * writer.
   * @see #setMergeScheduler(MergeScheduler)
   * @deprecated use {@link IndexWriterConfig#getMergeScheduler()} instead
   */
  public MergeScheduler getMergeScheduler() {
    ensureOpen();
    return mergeScheduler;
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
   * <p>Note that this method is a convenience method: it
   * just calls mergePolicy.setMaxMergeDocs as long as
   * mergePolicy is an instance of {@link LogMergePolicy}.
   * Otherwise an IllegalArgumentException is thrown.</p>
   *
   * <p>The default merge policy ({@link
   * LogByteSizeMergePolicy}) also allows you to set this
   * limit by net size (in MB) of the segment, using {@link
   * LogByteSizeMergePolicy#setMaxMergeMB}.</p>
   * @deprecated use {@link LogMergePolicy#setMaxMergeDocs(int)} directly.
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    getLogMergePolicy().setMaxMergeDocs(maxMergeDocs);
  }

  /**
   * <p>Returns the largest segment (measured by document
   * count) that may be merged with other segments.</p>
   *
   * <p>Note that this method is a convenience method: it
   * just calls mergePolicy.getMaxMergeDocs as long as
   * mergePolicy is an instance of {@link LogMergePolicy}.
   * Otherwise an IllegalArgumentException is thrown.</p>
   *
   * @see #setMaxMergeDocs
   * @deprecated use {@link LogMergePolicy#getMaxMergeDocs()} directly.
   */
  public int getMaxMergeDocs() {
    return getLogMergePolicy().getMaxMergeDocs();
  }

  /**
   * The maximum number of terms that will be indexed for a single field in a
   * document.  This limits the amount of memory required for indexing, so that
   * collections with very large files will not crash the indexing process by
   * running out of memory.  This setting refers to the number of running terms,
   * not to the number of different terms.<p/>
   * <strong>Note:</strong> this silently truncates large documents, excluding from the
   * index all terms that occur further in the document.  If you know your source
   * documents are large, be sure to set this value high enough to accomodate
   * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
   * is your memory, but you should anticipate an OutOfMemoryError.<p/>
   * By default, no more than {@link #DEFAULT_MAX_FIELD_LENGTH} terms
   * will be indexed for a field.
   * @deprecated use {@link IndexWriterConfig#setMaxFieldLength(int)} instead
   */
  public void setMaxFieldLength(int maxFieldLength) {
    ensureOpen();
    this.maxFieldLength = maxFieldLength;
    docWriter.setMaxFieldLength(maxFieldLength);
    if (infoStream != null)
      message("setMaxFieldLength " + maxFieldLength);
    // Required so config.getMaxFieldLength returns the right value. But this
    // will go away together with the method in 4.0.
    config.setMaxFieldLength(maxFieldLength);
  }

  /**
   * Returns the maximum number of terms that will be
   * indexed for a single field in a document.
   * @see #setMaxFieldLength
   * @deprecated use {@link IndexWriterConfig#getMaxFieldLength()} instead
   */
  public int getMaxFieldLength() {
    ensureOpen();
    return maxFieldLength;
  }

  /** Determines the minimal number of documents required
   * before the buffered in-memory documents are flushed as
   * a new Segment.  Large values generally gives faster
   * indexing.
   *
   * <p>When this is set, the writer will flush every
   * maxBufferedDocs added documents.  Pass in {@link
   * #DISABLE_AUTO_FLUSH} to prevent triggering a flush due
   * to number of buffered documents.  Note that if flushing
   * by RAM usage is also enabled, then the flush will be
   * triggered by whichever comes first.</p>
   *
   * <p>Disabled by default (writer flushes by RAM usage).</p>
   *
   * @throws IllegalArgumentException if maxBufferedDocs is
   * enabled but smaller than 2, or it disables maxBufferedDocs
   * when ramBufferSize is already disabled
   * @see #setRAMBufferSizeMB
   * @deprecated use {@link IndexWriterConfig#setMaxBufferedDocs(int)} instead.
   */
  public void setMaxBufferedDocs(int maxBufferedDocs) {
    ensureOpen();
    if (maxBufferedDocs != DISABLE_AUTO_FLUSH && maxBufferedDocs < 2)
      throw new IllegalArgumentException(
          "maxBufferedDocs must at least be 2 when enabled");
    if (maxBufferedDocs == DISABLE_AUTO_FLUSH
        && getRAMBufferSizeMB() == DISABLE_AUTO_FLUSH)
      throw new IllegalArgumentException(
          "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    docWriter.setMaxBufferedDocs(maxBufferedDocs);
    pushMaxBufferedDocs();
    if (infoStream != null)
      message("setMaxBufferedDocs " + maxBufferedDocs);
    // Required so config.getMaxBufferedDocs returns the right value. But this
    // will go away together with the method in 4.0.
    config.setMaxBufferedDocs(maxBufferedDocs);
  }

  /**
   * If we are flushing by doc count (not by RAM usage), and
   * using LogDocMergePolicy then push maxBufferedDocs down
   * as its minMergeDocs, to keep backwards compatibility.
   */
  private void pushMaxBufferedDocs() {
    if (docWriter.getMaxBufferedDocs() != DISABLE_AUTO_FLUSH) {
      final MergePolicy mp = mergePolicy;
      if (mp instanceof LogDocMergePolicy) {
        LogDocMergePolicy lmp = (LogDocMergePolicy) mp;
        final int maxBufferedDocs = docWriter.getMaxBufferedDocs();
        if (lmp.getMinMergeDocs() != maxBufferedDocs) {
          if (infoStream != null)
            message("now push maxBufferedDocs " + maxBufferedDocs + " to LogDocMergePolicy");
          lmp.setMinMergeDocs(maxBufferedDocs);
        }
      }
    }
  }

  /**
   * Returns the number of buffered added documents that will
   * trigger a flush if enabled.
   * @see #setMaxBufferedDocs
   * @deprecated use {@link IndexWriterConfig#getMaxBufferedDocs()} instead.
   */
  public int getMaxBufferedDocs() {
    ensureOpen();
    return docWriter.getMaxBufferedDocs();
  }

  /** Determines the amount of RAM that may be used for
   * buffering added documents and deletions before they are
   * flushed to the Directory.  Generally for faster
   * indexing performance it's best to flush by RAM usage
   * instead of document count and use as large a RAM buffer
   * as you can.
   *
   * <p>When this is set, the writer will flush whenever
   * buffered documents and deletions use this much RAM.
   * Pass in {@link #DISABLE_AUTO_FLUSH} to prevent
   * triggering a flush due to RAM usage.  Note that if
   * flushing by document count is also enabled, then the
   * flush will be triggered by whichever comes first.</p>
   *
   * <p> <b>NOTE</b>: the account of RAM usage for pending
   * deletions is only approximate.  Specifically, if you
   * delete by Query, Lucene currently has no way to measure
   * the RAM usage if individual Queries so the accounting
   * will under-estimate and you should compensate by either
   * calling commit() periodically yourself, or by using
   * {@link #setMaxBufferedDeleteTerms} to flush by count
   * instead of RAM usage (each buffered delete Query counts
   * as one).
   *
   * <p> <b>NOTE</b>: because IndexWriter uses
   * <code>int</code>s when managing its internal storage,
   * the absolute maximum value for this setting is somewhat
   * less than 2048 MB.  The precise limit depends on
   * various factors, such as how large your documents are,
   * how many fields have norms, etc., so it's best to set
   * this value comfortably under 2048.</p>
   *
   * <p> The default value is {@link #DEFAULT_RAM_BUFFER_SIZE_MB}.</p>
   * 
   * @throws IllegalArgumentException if ramBufferSize is
   * enabled but non-positive, or it disables ramBufferSize
   * when maxBufferedDocs is already disabled
   * @deprecated use {@link IndexWriterConfig#setRAMBufferSizeMB(double)} instead.
   */
  public void setRAMBufferSizeMB(double mb) {
    if (mb > 2048.0) {
      throw new IllegalArgumentException("ramBufferSize " + mb + " is too large; should be comfortably less than 2048");
    }
    if (mb != DISABLE_AUTO_FLUSH && mb <= 0.0)
      throw new IllegalArgumentException(
          "ramBufferSize should be > 0.0 MB when enabled");
    if (mb == DISABLE_AUTO_FLUSH && getMaxBufferedDocs() == DISABLE_AUTO_FLUSH)
      throw new IllegalArgumentException(
          "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    docWriter.setRAMBufferSizeMB(mb);
    if (infoStream != null)
      message("setRAMBufferSizeMB " + mb);
    // Required so config.getRAMBufferSizeMB returns the right value. But this
    // will go away together with the method in 4.0.
    config.setRAMBufferSizeMB(mb);
  }

  /**
   * Returns the value set by {@link #setRAMBufferSizeMB} if enabled.
   * @deprecated use {@link IndexWriterConfig#getRAMBufferSizeMB()} instead.
   */
  public double getRAMBufferSizeMB() {
    return docWriter.getRAMBufferSizeMB();
  }

  /**
   * <p>Determines the minimal number of delete terms required before the buffered
   * in-memory delete terms are applied and flushed. If there are documents
   * buffered in memory at the time, they are merged and a new segment is
   * created.</p>

   * <p>Disabled by default (writer flushes by RAM usage).</p>
   * 
   * @throws IllegalArgumentException if maxBufferedDeleteTerms
   * is enabled but smaller than 1
   * @see #setRAMBufferSizeMB
   * @deprecated use {@link IndexWriterConfig#setMaxBufferedDeleteTerms(int)} instead.
   */
  public void setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    ensureOpen();
    if (maxBufferedDeleteTerms != DISABLE_AUTO_FLUSH
        && maxBufferedDeleteTerms < 1)
      throw new IllegalArgumentException(
          "maxBufferedDeleteTerms must at least be 1 when enabled");
    docWriter.setMaxBufferedDeleteTerms(maxBufferedDeleteTerms);
    if (infoStream != null)
      message("setMaxBufferedDeleteTerms " + maxBufferedDeleteTerms);
    // Required so config.getMaxBufferedDeleteTerms returns the right value. But
    // this will go away together with the method in 4.0.
    config.setMaxBufferedDeleteTerms(maxBufferedDeleteTerms);
  }

  /**
   * Returns the number of buffered deleted terms that will
   * trigger a flush if enabled.
   * @see #setMaxBufferedDeleteTerms
   * @deprecated use {@link IndexWriterConfig#getMaxBufferedDeleteTerms()} instead
   */
  public int getMaxBufferedDeleteTerms() {
    ensureOpen();
    return docWriter.getMaxBufferedDeleteTerms();
  }

  /** Determines how often segment indices are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indices are faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while searches on unoptimized
   * indices are slower, indexing is faster.  Thus larger values (> 10) are best
   * for batch index creation, and smaller values (< 10) for indices that are
   * interactively maintained.
   *
   * <p>Note that this method is a convenience method: it
   * just calls mergePolicy.setMergeFactor as long as
   * mergePolicy is an instance of {@link LogMergePolicy}.
   * Otherwise an IllegalArgumentException is thrown.</p>
   *
   * <p>This must never be less than 2.  The default value is 10.
   * @deprecated use {@link LogMergePolicy#setMergeFactor(int)} directly.
   */
  public void setMergeFactor(int mergeFactor) {
    getLogMergePolicy().setMergeFactor(mergeFactor);
  }

  /**
   * <p>Returns the number of segments that are merged at
   * once and also controls the total number of segments
   * allowed to accumulate in the index.</p>
   *
   * <p>Note that this method is a convenience method: it
   * just calls mergePolicy.getMergeFactor as long as
   * mergePolicy is an instance of {@link LogMergePolicy}.
   * Otherwise an IllegalArgumentException is thrown.</p>
   *
   * @see #setMergeFactor
   * @deprecated use {@link LogMergePolicy#getMergeFactor()} directly.
   */
  public int getMergeFactor() {
    return getLogMergePolicy().getMergeFactor();
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
  public void setInfoStream(PrintStream infoStream) {
    ensureOpen();
    setMessageID(infoStream);
    docWriter.setInfoStream(infoStream);
    deleter.setInfoStream(infoStream);
    if (infoStream != null)
      messageState();
  }

  private void messageState() {
    message("\ndir=" + directory + "\n" +
            "mergePolicy=" + mergePolicy + "\n" + 
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
   * Sets the maximum time to wait for a write lock (in milliseconds) for this instance of IndexWriter.  @see
   * @see #setDefaultWriteLockTimeout to change the default value for all instances of IndexWriter.
   * @deprecated use {@link IndexWriterConfig#setWriteLockTimeout(long)} instead
   */
  public void setWriteLockTimeout(long writeLockTimeout) {
    ensureOpen();
    this.writeLockTimeout = writeLockTimeout;
    // Required so config.getWriteLockTimeout returns the right value. But this
    // will go away together with the method in 4.0.
    config.setWriteLockTimeout(writeLockTimeout);
  }

  /**
   * Returns allowed timeout when acquiring the write lock.
   * @see #setWriteLockTimeout
   * @deprecated use {@link IndexWriterConfig#getWriteLockTimeout()}
   */
  public long getWriteLockTimeout() {
    ensureOpen();
    return writeLockTimeout;
  }

  /**
   * Sets the default (for any instance of IndexWriter) maximum time to wait for a write lock (in
   * milliseconds).
   * @deprecated use {@link IndexWriterConfig#setDefaultWriteLockTimeout(long)} instead
   */
  public static void setDefaultWriteLockTimeout(long writeLockTimeout) {
    IndexWriterConfig.setDefaultWriteLockTimeout(writeLockTimeout);
  }

  /**
   * Returns default write lock timeout for newly
   * instantiated IndexWriters.
   * @see #setDefaultWriteLockTimeout
   * @deprecated use {@link IndexWriterConfig#getDefaultWriteLockTimeout()} instead
   */
  public static long getDefaultWriteLockTimeout() {
    return IndexWriterConfig.getDefaultWriteLockTimeout();
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

    docWriter.pauseAllThreads();

    try {
      if (infoStream != null)
        message("now flush at close");

      docWriter.close();

      // Only allow a new merge to be triggered if we are
      // going to wait for merges:
      if (!hitOOM) {
        flush(waitForMerges, true, true);
      }

      if (waitForMerges)
        // Give merge scheduler last chance to run, in case
        // any pending merges are waiting:
        mergeScheduler.merge(this);

      mergePolicy.close();

      finishMerges(waitForMerges);
      stopMerges = true;

      mergeScheduler.close();

      if (infoStream != null)
        message("now call final commit()");
      
      if (!hitOOM) {
        commit(0);
      }

      if (infoStream != null)
        message("at close: " + segString());

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
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "closeInternal");
    } finally {
      synchronized(this) {
        closing = false;
        notifyAll();
        if (!closed) {
          if (docWriter != null)
            docWriter.resumeAllThreads();
          if (infoStream != null)
            message("hit exception while closing");
        }
      }
    }
  }

  /** Tells the docWriter to close its currently open shared
   *  doc stores (stored fields & vectors files).
   *  Return value specifices whether new doc store files are compound or not.
   */
  private synchronized boolean flushDocStores() throws IOException {

    boolean useCompoundDocStore = false;

    String docStoreSegment;

    boolean success = false;
    try {
      docStoreSegment = docWriter.closeDocStore();
      success = true;
    } finally {
      if (!success && infoStream != null) {
        message("hit exception closing doc store segment");
      }
    }

    useCompoundDocStore = mergePolicy.useCompoundDocStore(segmentInfos);
      
    if (useCompoundDocStore && docStoreSegment != null && docWriter.closedFiles().size() != 0) {
      // Now build compound doc store file

      if (infoStream != null) {
        message("create compound file " + IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION));
      }

      success = false;

      final int numSegments = segmentInfos.size();
      final String compoundFileName = IndexFileNames.segmentFileName(docStoreSegment, "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION);

      try {
        CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, compoundFileName);
        for (final String file :  docWriter.closedFiles() ) {
          cfsWriter.addFile(file);
        }
      
        // Perform the merge
        cfsWriter.close();
        success = true;

      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception building compound file doc store for segment " + docStoreSegment);
          deleter.deleteFile(compoundFileName);
          docWriter.abort();
        }
      }

      for(int i=0;i<numSegments;i++) {
        SegmentInfo si = segmentInfos.info(i);
        if (si.getDocStoreOffset() != -1 &&
            si.getDocStoreSegment().equals(docStoreSegment))
          si.setDocStoreIsCompoundFile(true);
      }

      checkpoint();

      // In case the files we just merged into a CFS were
      // not previously checkpointed:
      deleter.deleteNewFiles(docWriter.closedFiles());
    }

    return useCompoundDocStore;
  }

  /** Returns the Directory used by this index. */
  public Directory getDirectory() {     
    // Pass false because the flush during closing calls getDirectory
    ensureOpen(false);
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
      count = docWriter.getNumDocsInRAM();
    else
      count = 0;

    for (int i = 0; i < segmentInfos.size(); i++)
      count += segmentInfos.info(i).docCount;
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
      count = docWriter.getNumDocsInRAM();
    else
      count = 0;

    for (int i = 0; i < segmentInfos.size(); i++) {
      final SegmentInfo info = segmentInfos.info(i);
      count += info.docCount - info.getDelCount();
    }
    return count;
  }

  public synchronized boolean hasDeletions() throws IOException {
    ensureOpen();
    if (docWriter.hasDeletes())
      return true;
    for (int i = 0; i < segmentInfos.size(); i++)
      if (segmentInfos.info(i).hasDeletions())
        return true;
    return false;
  }

  /**
   * The maximum number of terms that will be indexed for a single field in a
   * document.  This limits the amount of memory required for indexing, so that
   * collections with very large files will not crash the indexing process by
   * running out of memory.<p/>
   * Note that this effectively truncates large documents, excluding from the
   * index terms that occur further in the document.  If you know your source
   * documents are large, be sure to set this value high enough to accommodate
   * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
   * is your memory, but you should anticipate an OutOfMemoryError.<p/>
   * By default, no more than 10,000 terms will be indexed for a field.
   *
   * @see MaxFieldLength
   */
  private int maxFieldLength;

  /**
   * Adds a document to this index.  If the document contains more than
   * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
   * discarded.
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
  public void addDocument(Document doc) throws CorruptIndexException, IOException {
    addDocument(doc, analyzer);
  }

  /**
   * Adds a document to this index, using the provided analyzer instead of the
   * value of {@link #getAnalyzer()}.  If the document contains more than
   * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
   * discarded.
   *
   * <p>See {@link #addDocument(Document)} for details on
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
  public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {
    ensureOpen();
    boolean doFlush = false;
    boolean success = false;
    try {
      try {
        doFlush = docWriter.addDocument(doc, analyzer);
        success = true;
      } finally {
        if (!success) {

          if (infoStream != null)
            message("hit exception adding document");

          synchronized (this) {
            // If docWriter has some aborted files that were
            // never incref'd, then we clean them up here
            if (docWriter != null) {
              final Collection<String> files = docWriter.abortedFiles();
              if (files != null)
                deleter.deleteNewFiles(files);
            }
          }
        }
      }
      if (doFlush)
        flush(true, false, false);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addDocument");
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
      boolean doFlush = docWriter.bufferDeleteTerm(term);
      if (doFlush)
        flush(true, false, false);
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteDocuments(Term)");
    }
  }

  /**
   * Deletes the document(s) containing any of the
   * terms. All deletes are flushed at the same time.
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
      boolean doFlush = docWriter.bufferDeleteTerms(terms);
      if (doFlush)
        flush(true, false, false);
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
    boolean doFlush = docWriter.bufferDeleteQuery(query);
    if (doFlush)
      flush(true, false, false);
  }

  /**
   * Deletes the document(s) matching any of the provided queries.
   * All deletes are flushed at the same time.
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
    boolean doFlush = docWriter.bufferDeleteQueries(queries);
    if (doFlush)
      flush(true, false, false);
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
  public void updateDocument(Term term, Document doc) throws CorruptIndexException, IOException {
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
  public void updateDocument(Term term, Document doc, Analyzer analyzer)
      throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      boolean doFlush = false;
      boolean success = false;
      try {
        doFlush = docWriter.updateDocument(term, doc, analyzer);
        success = true;
      } finally {
        if (!success) {

          if (infoStream != null)
            message("hit exception updating document");

          synchronized (this) {
            // If docWriter has some aborted files that were
            // never incref'd, then we clean them up here
            final Collection<String> files = docWriter.abortedFiles();
            if (files != null)
              deleter.deleteNewFiles(files);
          }
        }
      }
      if (doFlush)
        flush(true, false, false);
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
    return docWriter.getNumDocsInRAM();
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
  final synchronized int getFlushCount() {
    return flushCount;
  }

  // for test purpose
  final synchronized int getFlushDeletesCount() {
    return flushDeletesCount;
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
      return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
    }
  }

  /** If non-null, information about merges will be printed to this.
   */
  private PrintStream infoStream = null;
  private static PrintStream defaultInfoStream = null;

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
   * space in your Directory.  For example, if your index
   * size is 10 MB then you need 20 MB free for optimize to
   * complete.  Also, it's best to call {@link #commit()}
   * after the optimize completes to allow IndexWriter to
   * free up disk space.</p>
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
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @see LogMergePolicy#findMergesForOptimize
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

    if (infoStream != null)
      message("optimize: index now " + segString());

    flush(true, false, true);

    synchronized(this) {
      resetMergeExceptions();
      segmentsToOptimize = new HashSet<SegmentInfo>(segmentInfos);
      
      // Now mark all pending & running merges as optimize
      // merge:
      for(final MergePolicy.OneMerge merge  : pendingMerges) {
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = maxNumSegments;
      }

      for ( final MergePolicy.OneMerge merge: runningMerges ) {
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = maxNumSegments;
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
   */
  public void expungeDeletes(boolean doWait)
    throws CorruptIndexException, IOException {
    ensureOpen();

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

    if (stopMerges)
      return;

    // Do not start new merges if we've hit OOME
    if (hitOOM) {
      return;
    }

    final MergePolicy.MergeSpecification spec;
    if (optimize) {
      spec = mergePolicy.findMergesForOptimize(segmentInfos, maxNumSegmentsOptimize, segmentsToOptimize);

      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++) {
          final MergePolicy.OneMerge merge = ( spec.merges.get(i));
          merge.optimize = true;
          merge.maxNumSegmentsOptimize = maxNumSegmentsOptimize;
        }
      }

    } else
      spec = mergePolicy.findMerges(segmentInfos);

    if (spec != null) {
      final int numMerges = spec.merges.size();
      for(int i=0;i<numMerges;i++)
        registerMerge(spec.merges.get(i));
    }
  }

  /** Expert: the {@link MergeScheduler} calls this method
   *  to retrieve the next merge requested by the
   *  MergePolicy */
  synchronized MergePolicy.OneMerge getNextMerge() {
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

    docWriter.pauseAllThreads();

    try {
      finishMerges(false);

      // Must pre-close these two, in case they increment
      // changeCount so that we can then set it to false
      // before calling closeInternal
      mergePolicy.close();
      mergeScheduler.close();

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
        segmentInfos.clear();
        segmentInfos.addAll(rollbackSegmentInfos);

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
          docWriter.resumeAllThreads();
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
   *    #optimize()} or any of the addIndexes methods, they
   *    will receive {@link MergePolicy.MergeAbortedException}s.
   */
  public synchronized void deleteAll() throws IOException {
    docWriter.pauseAllThreads();
    try {

      // Abort any running merges
      finishMerges(false);

      // Remove any buffered docs
      docWriter.abort();
      docWriter.setFlushedDocCount(0);

      // Remove all segments
      segmentInfos.clear();

      // Ask deleter to locate unreferenced files & remove them:
      deleter.checkpoint(segmentInfos, false);
      deleter.refresh();

      // Don't bother saving any changes in our segmentInfos
      readerPool.clear(null);      

      // Mark that the index has changed
      ++changeCount;
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "deleteAll");
    } finally {
      docWriter.resumeAllThreads();
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
    while(pendingMerges.size() > 0 || runningMerges.size() > 0) {
      doWait();
    }

    // sanity check
    assert 0 == mergingSegments.size();
  }

  /**
   * Called whenever the SegmentInfos has been updated and
   * the index files referenced exist (correctly) in the
   * index directory.
   */
  private synchronized void checkpoint() throws IOException {
    changeCount++;
    deleter.checkpoint(segmentInfos, false);
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
   * the new segments are not merged with the existing ones. Also, the segments 
   * are copied as-is, meaning they are not converted to CFS if they aren't, 
   * and vice-versa. If you wish to do that, you can call {@link #maybeMerge} 
   * or {@link #optimize} afterwards.
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
      flush(true, false, true);

      int docCount = 0;
      List<SegmentInfo> infos = new ArrayList<SegmentInfo>();
      for (Directory dir : dirs) {
        if (infoStream != null) {
          message("process directory " + dir);
        }
        SegmentInfos sis = new SegmentInfos(); // read infos from dir
        sis.read(dir);
        Map<String, String> dsNames = new HashMap<String, String>();
        for (SegmentInfo info : sis) {
          assert !infos.contains(info): "dup info dir=" + info.dir + " name=" + info.name;

          if (infoStream != null) {
            message("process segment=" + info.name);
          }
          docCount += info.docCount;
          String newSegName = newSegmentName();
          String dsName = info.getDocStoreSegment();

          // Determine if the doc store of this segment needs to be copied. It's
          // only relevant for segments who share doc store with others, because
          // the DS might have been copied already, in which case we just want
          // to update the DS name of this SegmentInfo.
          // NOTE: pre-3x segments include a null DSName if they don't share doc
          // store. So the following code ensures we don't accidentally insert
          // 'null' to the map.
          String newDsName = newSegName;
          boolean docStoreCopied = false;
          if (dsNames.containsKey(dsName)) {
            newDsName = dsNames.get(dsName);
            docStoreCopied = true;
          } else if (dsName != null) {
            dsNames.put(dsName, newSegName);
            docStoreCopied = false;
          }

          // Copy the segment files
          for (String file : info.files()) {
            if (docStoreCopied && IndexFileNames.isDocStoreFile(file)) {
              continue;
            } 
            dir.copy(directory, file, newSegName + IndexFileNames.stripSegmentName(file));
          }

          // Update SI appropriately
          info.setDocStore(info.getDocStoreOffset(), newDsName, info.getDocStoreIsCompoundFile());
          info.dir = directory;
          info.name = newSegName;

          infos.add(info);
        }
      }      

      synchronized (this) {
        ensureOpen();
        segmentInfos.addAll(infos);
        // Notify DocumentsWriter that the flushed count just increased
        docWriter.updateFlushedDocCount(docCount);

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
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addIndexes(IndexReader... readers)
    throws CorruptIndexException, IOException {
    ensureOpen();

    try {
      String mergedName = newSegmentName();
      SegmentMerger merger = new SegmentMerger(directory, termIndexInterval,
          mergedName, null, codecs, payloadProcessorProvider);
      
      for (IndexReader reader : readers)      // add new indexes
        merger.add(reader);
      
      int docCount = merger.merge();                // merge 'em
      
      SegmentInfo info = null;
      synchronized(this) {
        info = new SegmentInfo(mergedName, docCount, directory, false, -1,
            null, false, merger.hasProx(), merger.getCodec());
        setDiagnostics(info, "addIndexes(IndexReader...)");
        segmentInfos.add(info);
        checkpoint();
        
        // Notify DocumentsWriter that the flushed count just increased
        docWriter.updateFlushedDocCount(docCount);
      }
      
      // Now create the compound file if needed
      if (mergePolicy instanceof LogMergePolicy && getUseCompoundFile()) {

        List<String> files = null;

        synchronized(this) {
          // Must incRef our files so that if another thread
          // is running merge/optimize, it doesn't delete our
          // segment's files before we have a chance to
          // finish making the compound file.
          if (segmentInfos.contains(info)) {
            files = info.files();
            deleter.incRef(files);
          }
        }

        if (files != null) {
          try {
            merger.createCompoundFile(mergedName + ".cfs", info);
            synchronized(this) {
              info.setUseCompoundFile(true);
            }
          } finally {
            deleter.decRef(files);
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "addIndexes(IndexReader...)");
    }
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

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot commit");
    }

    if (pendingCommit != null)
      throw new IllegalStateException("prepareCommit was already called with no corresponding call to commit");

    if (infoStream != null)
      message("prepareCommit: flush");

    flush(true, true, true);

    startCommit(0, commitUserData);
  }

  // Used only by commit, below; lock order is commitLock -> IW
  private final Object commitLock = new Object();

  private void commit(long sizeInBytes) throws IOException {
    synchronized(commitLock) {
      startCommit(sizeInBytes, null);
      finishCommit();
    }
  }

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
        setRollbackSegmentInfos(pendingCommit);
        deleter.checkpoint(pendingCommit, true);
      } finally {
        deleter.decRef(pendingCommit);
        pendingCommit = null;
        notifyAll();
      }

    } else if (infoStream != null)
        message("commit: pendingCommit == null; skip");

    if (infoStream != null)
      message("commit: done");
  }

  /**
   * Flush all in-memory buffered udpates (adds and deletes)
   * to the Directory.
   * @param triggerMerge if true, we may merge segments (if
   *  deletes or docs were flushed) if necessary
   * @param flushDocStores if false we are allowed to keep
   *  doc stores open to share with the next segment
   * @param flushDeletes whether pending deletes should also
   *  be flushed
   */
  protected final void flush(boolean triggerMerge, boolean flushDocStores, boolean flushDeletes) throws CorruptIndexException, IOException {
    // We can be called during close, when closing==true, so we must pass false to ensureOpen:
    ensureOpen(false);
    if (doFlush(flushDocStores, flushDeletes) && triggerMerge)
      maybeMerge();
  }

  // TODO: this method should not have to be entirely
  // synchronized, ie, merges should be allowed to commit
  // even while a flush is happening
  private synchronized final boolean doFlush(boolean flushDocStores, boolean flushDeletes) throws CorruptIndexException, IOException {
    try {
      try {
        return doFlushInternal(flushDocStores, flushDeletes);
      } finally {
        docWriter.balanceRAM();
      }
    } finally {
      docWriter.clearFlushPending();
    }
  }

  // TODO: this method should not have to be entirely
  // synchronized, ie, merges should be allowed to commit
  // even while a flush is happening
  private synchronized final boolean doFlushInternal(boolean flushDocStores, boolean flushDeletes) throws CorruptIndexException, IOException {

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot flush");
    }

    ensureOpen(false);

    assert testPoint("startDoFlush");

    doBeforeFlush();
    
    flushCount++;

    // If we are flushing because too many deletes
    // accumulated, then we should apply the deletes to free
    // RAM:
    flushDeletes |= docWriter.doApplyDeletes();

    // Make sure no threads are actively adding a document.
    // Returns true if docWriter is currently aborting, in
    // which case we skip flushing this segment
    if (infoStream != null) {
      message("flush: now pause all indexing threads");
    }
    if (docWriter.pauseAllThreads()) {
      docWriter.resumeAllThreads();
      return false;
    }

    try {

      SegmentInfo newSegment = null;

      final int numDocs = docWriter.getNumDocsInRAM();

      // Always flush docs if there are any
      boolean flushDocs = numDocs > 0;

      String docStoreSegment = docWriter.getDocStoreSegment();

      assert docStoreSegment != null || numDocs == 0: "dss=" + docStoreSegment + " numDocs=" + numDocs;

      if (docStoreSegment == null)
        flushDocStores = false;

      int docStoreOffset = docWriter.getDocStoreOffset();

      boolean docStoreIsCompoundFile = false;

      if (infoStream != null) {
        message("  flush: segment=" + docWriter.getSegment() +
                " docStoreSegment=" + docWriter.getDocStoreSegment() +
                " docStoreOffset=" + docStoreOffset +
                " flushDocs=" + flushDocs +
                " flushDeletes=" + flushDeletes +
                " flushDocStores=" + flushDocStores +
                " numDocs=" + numDocs +
                " numBufDelTerms=" + docWriter.getNumBufferedDeleteTerms());
        message("  index before flush " + segString());
      }

      // Check if the doc stores must be separately flushed
      // because other segments, besides the one we are about
      // to flush, reference it
      if (flushDocStores && (!flushDocs || !docWriter.getSegment().equals(docWriter.getDocStoreSegment()))) {
        // We must separately flush the doc store
        if (infoStream != null)
          message("  flush shared docStore segment " + docStoreSegment);
      
        docStoreIsCompoundFile = flushDocStores();
        flushDocStores = false;
      }

      String segment = docWriter.getSegment();

      // If we are flushing docs, segment must not be null:
      assert segment != null || !flushDocs;

      if (flushDocs) {

        boolean success = false;
        final int flushedDocCount;

        try {
          flushedDocCount = docWriter.flush(flushDocStores);
          success = true;
        } finally {
          if (!success) {
            if (infoStream != null)
              message("hit exception flushing segment " + segment);
            deleter.refresh(segment);
          }
        }
        
        if (0 == docStoreOffset && flushDocStores) {
          // This means we are flushing private doc stores
          // with this segment, so it will not be shared
          // with other segments
          assert docStoreSegment != null;
          assert docStoreSegment.equals(segment);
          docStoreOffset = -1;
          docStoreIsCompoundFile = false;
          docStoreSegment = null;
        }

        // Create new SegmentInfo, but do not add to our
        // segmentInfos until deletes are flushed
        // successfully.
        newSegment = new SegmentInfo(segment,
                                     flushedDocCount,
                                     directory, false, docStoreOffset,
                                     docStoreSegment, docStoreIsCompoundFile,
                                     docWriter.hasProx(),    
                                     docWriter.getCodec());

        if (infoStream != null) {
          message("flush codec=" + docWriter.getCodec().name);
        }
        setDiagnostics(newSegment, "flush");
      }

      docWriter.pushDeletes();

      if (flushDocs) {
        segmentInfos.add(newSegment);
        checkpoint();
      }

      if (flushDocs && mergePolicy.useCompoundFile(segmentInfos, newSegment)) {
        // Now build compound file
        boolean success = false;
        try {
          docWriter.createCompoundFile(segment);
          success = true;
        } finally {
          if (!success) {
            if (infoStream != null)
              message("hit exception creating compound file for newly flushed segment " + segment);
            deleter.deleteFile(IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
          }
        }

        newSegment.setUseCompoundFile(true);
        checkpoint();
      }

      if (flushDeletes) {
        applyDeletes();
      }
      
      if (flushDocs)
        checkpoint();

      doAfterFlush();

      return flushDocs;

    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "doFlush");
      // never hit
      return false;
    } finally {
      docWriter.clearFlushPending();
      docWriter.resumeAllThreads();
    }
  }

  /** Expert:  Return the total size of all index files currently cached in memory.
   * Useful for size management with flushRamDocs()
   */
  public final long ramSizeInBytes() {
    ensureOpen();
    return docWriter.getRAMUsed();
  }

  /** Expert:  Return the number of documents currently
   *  buffered in RAM. */
  public final synchronized int numRamDocs() {
    ensureOpen();
    return docWriter.getNumDocsInRAM();
  }

  private int ensureContiguousMerge(MergePolicy.OneMerge merge) {

    int first = segmentInfos.indexOf(merge.segments.info(0));
    if (first == -1)
      throw new MergePolicy.MergeException("could not find segment " + merge.segments.info(0).name + " in current index " + segString(), directory);

    final int numSegments = segmentInfos.size();
    
    final int numSegmentsToMerge = merge.segments.size();
    for(int i=0;i<numSegmentsToMerge;i++) {
      final SegmentInfo info = merge.segments.info(i);

      if (first + i >= numSegments || !segmentInfos.info(first+i).equals(info)) {
        if (segmentInfos.indexOf(info) == -1)
          throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.name + ") that is not in the current index " + segString(), directory);
        else
          throw new MergePolicy.MergeException("MergePolicy selected non-contiguous segments to merge (" + merge.segString(directory) + " vs " + segString() + "), which IndexWriter (currently) cannot handle",
                                               directory);
      }
    }

    return first;
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
  synchronized private void commitMergedDeletes(MergePolicy.OneMerge merge, SegmentReader mergeReader) throws IOException {

    assert testPoint("startCommitMergeDeletes");

    final SegmentInfos sourceSegments = merge.segments;

    if (infoStream != null)
      message("commitMergeDeletes " + merge.segString(directory));

    // Carefully merge deletes that occurred after we
    // started merging:
    int docUpto = 0;
    int delCount = 0;

    for(int i=0; i < sourceSegments.size(); i++) {
      SegmentInfo info = sourceSegments.info(i);
      int docCount = info.docCount;
      SegmentReader previousReader = merge.readersClone[i];
      SegmentReader currentReader = merge.readers[i];
      if (previousReader.hasDeletions()) {

        // There were deletes on this segment when the merge
        // started.  The merge has collapsed away those
        // deletes, but, if new deletes were flushed since
        // the merge started, we must now carefully keep any
        // newly flushed deletes but mapping them to the new
        // docIDs.

        if (currentReader.numDeletedDocs() > previousReader.numDeletedDocs()) {
          // This means this segment has had new deletes
          // committed since we started the merge, so we
          // must merge them:
          for(int j=0;j<docCount;j++) {
            if (previousReader.isDeleted(j))
              assert currentReader.isDeleted(j);
            else {
              if (currentReader.isDeleted(j)) {
                mergeReader.doDelete(docUpto);
                delCount++;
              }
              docUpto++;
            }
          }
        } else {
          docUpto += docCount - previousReader.numDeletedDocs();
        }
      } else if (currentReader.hasDeletions()) {
        // This segment had no deletes before but now it
        // does:
        for(int j=0; j<docCount; j++) {
          if (currentReader.isDeleted(j)) {
            mergeReader.doDelete(docUpto);
            delCount++;
          }
          docUpto++;
        }
      } else
        // No deletes before or after
        docUpto += info.docCount;
    }

    assert mergeReader.numDeletedDocs() == delCount;

    mergeReader.hasChanges = delCount >= 0;
  }

  /* FIXME if we want to support non-contiguous segment merges */
  synchronized private boolean commitMerge(MergePolicy.OneMerge merge, SegmentMerger merger, int mergedDocCount, SegmentReader mergedReader) throws IOException {

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

      deleter.refresh(merge.info.name);
      return false;
    }

    final int start = ensureContiguousMerge(merge);

    commitMergedDeletes(merge, mergedReader);
    docWriter.remapDeletes(segmentInfos, merger.getDocMaps(), merger.getDelCounts(), merge, mergedDocCount);
      
    setMergeDocStoreIsCompoundFile(merge);
    merge.info.setHasProx(merger.hasProx());

    segmentInfos.subList(start, start + merge.segments.size()).clear();
    assert !segmentInfos.contains(merge.info);
    segmentInfos.add(start, merge.info);

    // Must note the change to segmentInfos so any commits
    // in-flight don't lose it:
    checkpoint();

    // If the merged segments had pending changes, clear
    // them so that they don't bother writing them to
    // disk, updating SegmentInfo, etc.:
    readerPool.clear(merge.segments);

    if (merge.optimize)
      segmentsToOptimize.add(merge.info);
    return true;
  }
  
  private synchronized void decrefMergeSegments(MergePolicy.OneMerge merge) throws IOException {
    assert merge.increfDone;
    merge.increfDone = false;
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
   */

  final void merge(MergePolicy.OneMerge merge)
    throws CorruptIndexException, IOException {

    boolean success = false;

    final long t0 = System.currentTimeMillis();

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
          if (success && !merge.isAborted() && !closed && !closing)
            updatePendingMerges(merge.maxNumSegmentsOptimize, merge.optimize);
        }
      }
    } catch (OutOfMemoryError oom) {
      handleOOM(oom, "merge");
    }
    if (infoStream != null && merge.info != null) {
      message("merge time " + (System.currentTimeMillis()-t0) + " msec for " + merge.info.docCount + " docs");
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
  final synchronized boolean registerMerge(MergePolicy.OneMerge merge) throws MergePolicy.MergeAbortedException {

    if (merge.registerDone)
      return true;

    if (stopMerges) {
      merge.abort();
      throw new MergePolicy.MergeAbortedException("merge is aborted: " + merge.segString(directory));
    }

    final int count = merge.segments.size();
    boolean isExternal = false;
    for(int i=0;i<count;i++) {
      final SegmentInfo info = merge.segments.info(i);
      if (mergingSegments.contains(info))
        return false;
      if (segmentInfos.indexOf(info) == -1)
        return false;
      if (info.dir != directory)
        isExternal = true;
    }

    ensureContiguousMerge(merge);

    pendingMerges.add(merge);

    if (infoStream != null)
      message("add merge to pendingMerges: " + merge.segString(directory) + " [total " + pendingMerges.size() + " pending]");

    merge.mergeGen = mergeGen;
    merge.isExternal = isExternal;

    // OK it does not conflict; now record that this merge
    // is running (while synchronized) to avoid race
    // condition where two conflicting merges from different
    // threads, start
    for(int i=0;i<count;i++)
      mergingSegments.add(merge.segments.info(i));

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
        mergeFinish(merge);
      }
    }
  }

  final synchronized private void _mergeInit(MergePolicy.OneMerge merge) throws IOException {

    assert testPoint("startMergeInit");

    assert merge.registerDone;
    assert !merge.optimize || merge.maxNumSegmentsOptimize > 0;

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot merge");
    }

    if (merge.info != null)
      // mergeInit already done
      return;

    if (merge.isAborted())
      return;

    applyDeletes();

    final SegmentInfos sourceSegments = merge.segments;
    final int end = sourceSegments.size();

    // Check whether this merge will allow us to skip
    // merging the doc stores (stored field & vectors).
    // This is a very substantial optimization (saves tons
    // of IO).

    Directory lastDir = directory;
    String lastDocStoreSegment = null;
    int next = -1;

    boolean mergeDocStores = false;
    boolean doFlushDocStore = false;
    final String currentDocStoreSegment = docWriter.getDocStoreSegment();

    // Test each segment to be merged: check if we need to
    // flush/merge doc stores
    for (int i = 0; i < end; i++) {
      SegmentInfo si = sourceSegments.info(i);

      // If it has deletions we must merge the doc stores
      if (si.hasDeletions())
        mergeDocStores = true;

      // If it has its own (private) doc stores we must
      // merge the doc stores
      if (-1 == si.getDocStoreOffset())
        mergeDocStores = true;

      // If it has a different doc store segment than
      // previous segments, we must merge the doc stores
      String docStoreSegment = si.getDocStoreSegment();
      if (docStoreSegment == null)
        mergeDocStores = true;
      else if (lastDocStoreSegment == null)
        lastDocStoreSegment = docStoreSegment;
      else if (!lastDocStoreSegment.equals(docStoreSegment))
        mergeDocStores = true;

      // Segments' docScoreOffsets must be in-order,
      // contiguous.  For the default merge policy now
      // this will always be the case but for an arbitrary
      // merge policy this may not be the case
      if (-1 == next)
        next = si.getDocStoreOffset() + si.docCount;
      else if (next != si.getDocStoreOffset())
        mergeDocStores = true;
      else
        next = si.getDocStoreOffset() + si.docCount;
      
      // If the segment comes from a different directory
      // we must merge
      if (lastDir != si.dir)
        mergeDocStores = true;

      // If the segment is referencing the current "live"
      // doc store outputs then we must merge
      if (si.getDocStoreOffset() != -1 && currentDocStoreSegment != null && si.getDocStoreSegment().equals(currentDocStoreSegment)) {
        doFlushDocStore = true;
      }
    }

    final int docStoreOffset;
    final String docStoreSegment;
    final boolean docStoreIsCompoundFile;

    if (mergeDocStores) {
      docStoreOffset = -1;
      docStoreSegment = null;
      docStoreIsCompoundFile = false;
    } else {
      SegmentInfo si = sourceSegments.info(0);        
      docStoreOffset = si.getDocStoreOffset();
      docStoreSegment = si.getDocStoreSegment();
      docStoreIsCompoundFile = si.getDocStoreIsCompoundFile();
    }

    if (mergeDocStores && doFlushDocStore) {
      // SegmentMerger intends to merge the doc stores
      // (stored fields, vectors), and at least one of the
      // segments to be merged refers to the currently
      // live doc stores.

      // TODO: if we know we are about to merge away these
      // newly flushed doc store files then we should not
      // make compound file out of them...
      if (infoStream != null)
        message("now flush at merge");
      doFlush(true, false);
      updatePendingMerges(1, false);
    }

    merge.increfDone = true;

    merge.mergeDocStores = mergeDocStores;

    // Bind a new segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.
    merge.info = new SegmentInfo(newSegmentName(), 0,
                                 directory, false, docStoreOffset,
                                 docStoreSegment,
                                 docStoreIsCompoundFile,
                                 false,
                                 null);


    Map<String,String> details = new HashMap<String,String>();
    details.put("optimize", Boolean.toString(merge.optimize));
    details.put("mergeFactor", Integer.toString(end));
    details.put("mergeDocStores", Boolean.toString(mergeDocStores));
    setDiagnostics(merge.info, "merge", details);

    // Also enroll the merged segment into mergingSegments;
    // this prevents it from getting selected for a merge
    // after our merge is done but while we are building the
    // CFS:
    mergingSegments.add(merge.info);
  }

  private void setDiagnostics(SegmentInfo info, String source) {
    setDiagnostics(info, source, null);
  }

  private void setDiagnostics(SegmentInfo info, String source, Map<String,String> details) {
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

    if (merge.increfDone)
      decrefMergeSegments(merge);

    if (merge.mergeFiles != null) {
      deleter.decRef(merge.mergeFiles);
      merge.mergeFiles = null;
    }

    // It's possible we are called twice, eg if there was an
    // exception inside mergeInit
    if (merge.registerDone) {
      final SegmentInfos sourceSegments = merge.segments;
      final int end = sourceSegments.size();
      for(int i=0;i<end;i++)
        mergingSegments.remove(sourceSegments.info(i));
      mergingSegments.remove(merge.info);
      merge.registerDone = false;
    }

    runningMerges.remove(merge);
  }

  private synchronized void setMergeDocStoreIsCompoundFile(MergePolicy.OneMerge merge) {
    final String mergeDocStoreSegment = merge.info.getDocStoreSegment(); 
    if (mergeDocStoreSegment != null && !merge.info.getDocStoreIsCompoundFile()) {
      final int size = segmentInfos.size();
      for(int i=0;i<size;i++) {
        final SegmentInfo info = segmentInfos.info(i);
        final String docStoreSegment = info.getDocStoreSegment();
        if (docStoreSegment != null &&
            docStoreSegment.equals(mergeDocStoreSegment) && 
            info.getDocStoreIsCompoundFile()) {
          merge.info.setDocStoreIsCompoundFile(true);
          break;
        }
      }
    }
  }        

  /** Does the actual (time-consuming) work of the merge,
   *  but without holding synchronized lock on IndexWriter
   *  instance */
  final private int mergeMiddle(MergePolicy.OneMerge merge) 
    throws CorruptIndexException, IOException {
    
    merge.checkAborted(directory);

    final String mergedName = merge.info.name;
    
    SegmentMerger merger = null;

    int mergedDocCount = 0;

    SegmentInfos sourceSegments = merge.segments;
    final int numSegments = sourceSegments.size();

    if (infoStream != null)
      message("merging " + merge.segString(directory));

    merger = new SegmentMerger(directory, termIndexInterval, mergedName, merge, codecs, payloadProcessorProvider);

    merge.readers = new SegmentReader[numSegments];
    merge.readersClone = new SegmentReader[numSegments];

    boolean mergeDocStores = false;

    final Set<String> dss = new HashSet<String>();
    
    // This is try/finally to make sure merger's readers are
    // closed:
    boolean success = false;
    try {
      int totDocCount = 0;

      for (int i = 0; i < numSegments; i++) {

        final SegmentInfo info = sourceSegments.info(i);

        // Hold onto the "live" reader; we will use this to
        // commit merged deletes
        SegmentReader reader = merge.readers[i] = readerPool.get(info, merge.mergeDocStores,
                                                                 MERGE_READ_BUFFER_SIZE,
                                                                 -config.getReaderTermsIndexDivisor());

        // We clone the segment readers because other
        // deletes may come in while we're merging so we
        // need readers that will not change
        SegmentReader clone = merge.readersClone[i] = (SegmentReader) reader.clone(true);
        merger.add(clone);

        if (clone.hasDeletions()) {
          mergeDocStores = true;
        }
        
        if (info.getDocStoreOffset() != -1) {
          dss.add(info.getDocStoreSegment());
        }

        totDocCount += clone.numDocs();
      }

      if (infoStream != null) {
        message("merge: total "+totDocCount+" docs");
      }

      merge.checkAborted(directory);

      // If deletions have arrived and it has now become
      // necessary to merge doc stores, go and open them:
      if (mergeDocStores && !merge.mergeDocStores) {
        merge.mergeDocStores = true;
        synchronized(this) {

          // If 1) we must now merge doc stores, and 2) at
          // least one of the segments we are merging uses
          // the doc store we are now writing to, we must at
          // this point force this doc store closed (by
          // calling flush).  If we didn't do this then the
          // readers will attempt to open an IndexInput
          // on files that have still-open IndexOutputs
          // against them:
          if (dss.contains(docWriter.getDocStoreSegment())) {
            if (infoStream != null)
              message("now flush at mergeMiddle");
            doFlush(true, false);
            updatePendingMerges(1, false);
          }
        }

        for(int i=0;i<numSegments;i++) {
          merge.readersClone[i].openDocStores();
        }

        // Clear DSS
        synchronized(this) {
          merge.info.setDocStore(-1, null, false);
        }
      }

      // This is where all the work happens:
      mergedDocCount = merge.info.docCount = merger.merge(merge.mergeDocStores);

      // Record which codec was used to write the segment
      merge.info.setCodec(merger.getCodec());

      if (infoStream != null) {
        message("merge codec=" + merger.getCodec().name);
      }
      
      assert mergedDocCount == totDocCount;

      // Very important to do this before opening the reader
      // because codec must know if prox was written for
      // this segment:
      //System.out.println("merger set hasProx=" + merger.hasProx() + " seg=" + merge.info.name);
      merge.info.setHasProx(merger.hasProx());

      // TODO: in the non-realtime case, we may want to only
      // keep deletes (it's costly to open entire reader
      // when we just need deletes)

      final int termsIndexDivisor;
      final boolean loadDocStores;

      synchronized(this) {
        // If the doc store we are using has been closed and
        // is in now compound format (but wasn't when we
        // started), then we will switch to the compound
        // format as well:
        setMergeDocStoreIsCompoundFile(merge);
        assert merge.mergeFiles == null;
        merge.mergeFiles = merge.info.files();
        deleter.incRef(merge.mergeFiles);
      }

      if (poolReaders && mergedSegmentWarmer != null) {
        // Load terms index & doc stores so the segment
        // warmer can run searches, load documents/term
        // vectors
        termsIndexDivisor = config.getReaderTermsIndexDivisor();
        loadDocStores = true;
      } else {
        termsIndexDivisor = -1;
        loadDocStores = false;
      }

      final SegmentReader mergedReader = readerPool.get(merge.info, loadDocStores, BufferedIndexInput.BUFFER_SIZE, termsIndexDivisor);
      try {
        if (poolReaders && mergedSegmentWarmer != null) {
          mergedSegmentWarmer.warm(mergedReader);
        }
        if (!commitMerge(merge, merger, mergedDocCount, mergedReader))
          // commitMerge will return false if this merge was aborted
          return 0;
      } finally {
        synchronized(this) {
          readerPool.release(mergedReader);
        }
      }

      success = true;
    } finally {
      synchronized(this) {
        if (!success) {
          // Suppress any new exceptions so we throw the
          // original cause
          for (int i=0;i<numSegments;i++) {
            if (merge.readers[i] != null) {
              try {
                readerPool.release(merge.readers[i], false);
              } catch (Throwable t) {
              }
            }

            if (merge.readersClone[i] != null) {
              try {
                merge.readersClone[i].close();
              } catch (Throwable t) {
              }
              // This was a private clone and we had the
              // only reference
              assert merge.readersClone[i].getRefCount() == 0: "refCount should be 0 but is " + merge.readersClone[i].getRefCount();
            }
          }
        } else {
          for (int i=0;i<numSegments;i++) {
            if (merge.readers[i] != null) {
              readerPool.release(merge.readers[i], true);
            }

            if (merge.readersClone[i] != null) {
              merge.readersClone[i].close();
              // This was a private clone and we had the only reference
              assert merge.readersClone[i].getRefCount() == 0;
            }
          }
        }
      }
    }

    // Must checkpoint before decrefing so any newly
    // referenced files in the new merge.info are incref'd
    // first:
    synchronized(this) {
      deleter.checkpoint(segmentInfos, false);
    }
    decrefMergeSegments(merge);

    if (merge.useCompoundFile) {

      success = false;
      final String compoundFileName = IndexFileNames.segmentFileName(mergedName, "", IndexFileNames.COMPOUND_FILE_EXTENSION);

      try {
        merger.createCompoundFile(compoundFileName, merge.info);
        success = true;
      } catch (IOException ioe) {
        synchronized(this) {
          if (merge.isAborted()) {
            // This can happen if rollback or close(false)
            // is called -- fall through to logic below to
            // remove the partially created CFS:
            success = true;
          } else
            handleMergeException(ioe, merge);
        }
      } catch (Throwable t) {
        handleMergeException(t, merge);
      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception creating compound file during merge");
          synchronized(this) {
            deleter.deleteFile(compoundFileName);
          }
        }
      }

      if (merge.isAborted()) {
        if (infoStream != null)
          message("abort merge after building CFS");
        deleter.deleteFile(compoundFileName);
        return 0;
      }

      synchronized(this) {
        if (segmentInfos.indexOf(merge.info) == -1 || merge.isAborted()) {
          // Our segment (committed in non-compound
          // format) got merged away while we were
          // building the compound format.
          deleter.deleteFile(compoundFileName);
        } else {
          merge.info.setUseCompoundFile(true);
          checkpoint();
        }
      }
    }

    return mergedDocCount;
  }

  synchronized void addMergeException(MergePolicy.OneMerge merge) {
    assert merge.getException() != null;
    if (!mergeExceptions.contains(merge) && mergeGen == merge.mergeGen)
      mergeExceptions.add(merge);
  }

  // Apply buffered deletes to all segments.
  private final synchronized boolean applyDeletes() throws CorruptIndexException, IOException {
    assert testPoint("startApplyDeletes");
    if (infoStream != null) {
      message("applyDeletes");
    }
    flushDeletesCount++;
    boolean success = false;
    boolean changed;
    try {
      changed = docWriter.applyDeletes(segmentInfos);
      success = true;
    } finally {
      if (!success && infoStream != null) {
        message("hit exception flushing deletes");
      }
    }

    if (changed)
      checkpoint();
    return changed;
  }

  // For test purposes.
  final synchronized int getBufferedDeleteTermsSize() {
    return docWriter.getBufferedDeleteTerms().size();
  }

  // For test purposes.
  final synchronized int getNumBufferedDeleteTerms() {
    return docWriter.getNumBufferedDeleteTerms();
  }

  // utility routines for tests
  SegmentInfo newestSegment() {
    return segmentInfos.size() > 0 ? segmentInfos.info(segmentInfos.size()-1) : null;
  }

  public synchronized String segString() {
    return segString(segmentInfos);
  }

  private synchronized String segString(SegmentInfos infos) {
    StringBuilder buffer = new StringBuilder();
    final int count = infos.size();
    for(int i = 0; i < count; i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      final SegmentInfo info = infos.info(i);
      buffer.append(info.toString(directory, 0));
      if (info.dir != directory)
        buffer.append("**");
    }
    return buffer.toString();
  }

  private synchronized void doWait() {
    // NOTE: the callers of this method should in theory
    // be able to do simply wait(), but, as a defense
    // against thread timing hazards where notifyAll()
    // falls to be called, we wait for at most 1 second
    // and then return so caller can check if wait
    // conditions are satisfied:
    try {
      wait(1000);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }

  /** Walk through all files referenced by the current
   *  segmentInfos and ask the Directory to sync each file,
   *  if it wasn't already.  If that succeeds, then we
   *  prepare a new segments_N file but do not fully commit
   *  it. */
  private void startCommit(long sizeInBytes, Map<String,String> commitUserData) throws IOException {

    assert testPoint("startStartCommit");

    // TODO: as of LUCENE-2095, we can simplify this method,
    // since only 1 thread can be in here at once

    if (hitOOM) {
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot commit");
    }

    try {

      if (infoStream != null)
        message("startCommit(): start sizeInBytes=" + sizeInBytes);

      SegmentInfos toSync = null;
      final long myChangeCount;

      synchronized(this) {

        assert lastCommitChangeCount <= changeCount;
        
        if (changeCount == lastCommitChangeCount) {
          if (infoStream != null)
            message("  skip startCommit(): no changes pending");
          return;
        }
        
        // First, we clone & incref the segmentInfos we intend
        // to sync, then, without locking, we sync() each file
        // referenced by toSync, in the background.  Multiple
        // threads can be doing this at once, if say a large
        // merge and a small merge finish at the same time:
        
        if (infoStream != null)
          message("startCommit index=" + segString(segmentInfos) + " changeCount=" + changeCount);
        
        readerPool.commit();
        
        toSync = (SegmentInfos) segmentInfos.clone();
        
        if (commitUserData != null)
          toSync.setUserData(commitUserData);
        
        deleter.incRef(toSync, false);
        myChangeCount = changeCount;
        
        Collection<String> files = toSync.files(directory, false);
        for(final String fileName: files) {
          assert directory.fileExists(fileName): "file " + fileName + " does not exist";

          // If this trips it means we are missing a call to
          // .checkpoint somewhere, because by the time we
          // are called, deleter should know about every
          // file referenced by the current head
          // segmentInfos:
          assert deleter.exists(fileName);
        }
      }

      assert testPoint("midStartCommit");

      boolean setPending = false;

      try {
        directory.sync(toSync.files(directory, false));

        assert testPoint("midStartCommit2");

        synchronized(this) {
          // If someone saved a newer version of segments file
          // since I first started syncing my version, I can
          // safely skip saving myself since I've been
          // superseded:

          while(true) {
            if (myChangeCount <= lastCommitChangeCount) {
              if (infoStream != null) {
                message("sync superseded by newer infos");
              }
              break;
            } else if (pendingCommit == null) {
              // My turn to commit

              if (segmentInfos.getGeneration() > toSync.getGeneration())
                toSync.updateGeneration(segmentInfos);

              boolean success = false;
              try {

                // Exception here means nothing is prepared
                // (this method unwinds everything it did on
                // an exception)
                try {
                  toSync.prepareCommit(directory);
                } finally {
                  // Have our master segmentInfos record the
                  // generations we just prepared.  We do this
                  // on error or success so we don't
                  // double-write a segments_N file.
                  segmentInfos.updateGeneration(toSync);
                }

                assert pendingCommit == null;
                setPending = true;
                pendingCommit = toSync;
                pendingCommitChangeCount = myChangeCount;
                success = true;
              } finally {
                if (!success && infoStream != null)
                  message("hit exception committing segments file");
              }
              break;
            } else {
              // Must wait for other commit to complete
              doWait();
            }
          }
        }

        if (infoStream != null)
          message("done all syncs");

        assert testPoint("midStartCommitSuccess");

      } finally {
        synchronized(this) {
          if (!setPending)
            deleter.decRef(toSync);
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

  /**
   * Specifies maximum field length (in number of tokens/terms) in
   * {@link IndexWriter} constructors. {@link #setMaxFieldLength(int)} overrides
   * the value set by the constructor.
   * 
   * @deprecated use {@link IndexWriterConfig} and pass
   *             {@link IndexWriterConfig#UNLIMITED_FIELD_LENGTH} or your own
   *             value.
   */
  public static final class MaxFieldLength {

    private int limit;
    private String name;

    /**
     * Private type-safe-enum-pattern constructor.
     * 
     * @param name instance name
     * @param limit maximum field length
     */
    private MaxFieldLength(String name, int limit) {
      this.name = name;
      this.limit = limit;
    }

    /**
     * Public constructor to allow users to specify the maximum field size limit.
     * 
     * @param limit The maximum field length
     */
    public MaxFieldLength(int limit) {
      this("User-specified", limit);
    }
    
    public int getLimit() {
      return limit;
    }
    
    @Override
    public String toString()
    {
      return name + ":" + limit;
    }

    /** Sets the maximum field length to {@link Integer#MAX_VALUE}. */
    public static final MaxFieldLength UNLIMITED
        = new MaxFieldLength("UNLIMITED", Integer.MAX_VALUE);

    /**
     *  Sets the maximum field length to 
     * {@link #DEFAULT_MAX_FIELD_LENGTH} 
     * */
    public static final MaxFieldLength LIMITED
        = new MaxFieldLength("LIMITED", DEFAULT_MAX_FIELD_LENGTH);
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

  private IndexReaderWarmer mergedSegmentWarmer;

  /**
   * Set the merged segment warmer. See {@link IndexReaderWarmer}.
   * 
   * @deprecated use
   *             {@link IndexWriterConfig#setMergedSegmentWarmer}
   *             instead.
   */
  public void setMergedSegmentWarmer(IndexReaderWarmer warmer) {
    mergedSegmentWarmer = warmer;
    // Required so config.getMergedSegmentWarmer returns the right value. But
    // this will go away together with the method in 4.0.
    config.setMergedSegmentWarmer(mergedSegmentWarmer);
  }

  /**
   * Returns the current merged segment warmer. See {@link IndexReaderWarmer}.
   * 
   * @deprecated use {@link IndexWriterConfig#getMergedSegmentWarmer()} instead.
   */
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return mergedSegmentWarmer;
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
  //   startApplyDeletes
  //   DocumentsWriter.ThreadState.init start
  boolean testPoint(String name) {
    return true;
  }

  synchronized boolean nrtIsCurrent(SegmentInfos infos) {
    if (!infos.equals(segmentInfos)) {
      // if any structural changes (new segments), we are
      // stale
      return false;
    } else {
      return !docWriter.anyChanges();
    }
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
