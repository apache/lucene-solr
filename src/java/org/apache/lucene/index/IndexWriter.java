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
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Constants;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Iterator;

/**
  An <code>IndexWriter</code> creates and maintains an index.

  <p>The <code>create</code> argument to the 
  <a href="#IndexWriter(org.apache.lucene.store.Directory, org.apache.lucene.analysis.Analyzer, boolean)"><b>constructor</b></a>
  determines whether a new index is created, or whether an existing index is
  opened.  Note that you
  can open an index with <code>create=true</code> even while readers are
  using the index.  The old readers will continue to search
  the "point in time" snapshot they had opened, and won't
  see the newly created index until they re-open.  There are
  also <a href="#IndexWriter(org.apache.lucene.store.Directory, org.apache.lucene.analysis.Analyzer)"><b>constructors</b></a>
  with no <code>create</code> argument which
  will create a new index if there is not already an index at the
  provided path and otherwise open the existing index.</p>

  <p>In either case, documents are added with <a
  href="#addDocument(org.apache.lucene.document.Document)"><b>addDocument</b></a>
  and removed with <a
  href="#deleteDocuments(org.apache.lucene.index.Term)"><b>deleteDocuments(Term)</b></a>
  or <a
  href="#deleteDocuments(org.apache.lucene.search.Query)"><b>deleteDocuments(Query)</b></a>.
  A document can be updated with <a href="#updateDocument(org.apache.lucene.index.Term, org.apache.lucene.document.Document)"><b>updateDocument</b></a> 
  (which just deletes and then adds the entire document).
  When finished adding, deleting and updating documents, <a href="#close()"><b>close</b></a> should be called.</p>

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

  <a name="autoCommit"></a>
  <p>The optional <code>autoCommit</code> argument to the <a
  href="#IndexWriter(org.apache.lucene.store.Directory,
  boolean,
  org.apache.lucene.analysis.Analyzer)"><b>constructors</b></a>
  controls visibility of the changes to {@link IndexReader}
  instances reading the same index.  When this is
  <code>false</code>, changes are not visible until {@link
  #close()} or {@link #commit()} is called.  Note that changes will still be
  flushed to the {@link org.apache.lucene.store.Directory}
  as new files, but are not committed (no new
  <code>segments_N</code> file is written referencing the
  new files, nor are the files sync'd to stable storage)
  until {@link #close()} or {@link #commit()} is called.  If something
  goes terribly wrong (for example the JVM crashes), then
  the index will reflect none of the changes made since the
  last commit, or the starting state if commit was not called.
  You can also call {@link #rollback}, which closes the writer
  without committing any changes, and removes any index
  files that had been flushed but are now unreferenced.
  This mode is useful for preventing readers from refreshing
  at a bad time (for example after you've done all your
  deletes but before you've done your adds).  It can also be
  used to implement simple single-writer transactional
  semantics ("all or none").  You can do a two-phase commit
  by calling {@link #prepareCommit()}
  followed by {@link #commit()}. This is necessary when
  Lucene is working with an external resource (for example,
  a database) and both must either commit or rollback the
  transaction.</p>

  <p>When <code>autoCommit</code> is <code>true</code> then
  the writer will periodically commit on its own.  [<b>Deprecated</b>: Note that in 3.0, IndexWriter will
  no longer accept autoCommit=true (it will be hardwired to
  false).  You can always call {@link #commit()} yourself
  when needed]. There is
  no guarantee when exactly an auto commit will occur (it
  used to be after every flush, but it is now after every
  completed merge, as of 2.4).  If you want to force a
  commit, call {@link #commit()}, or, close the writer.  Once
  a commit has finished, newly opened {@link IndexReader} instances will
  see the changes to the index as of that commit.  When
  running in this mode, be careful not to refresh your
  readers while optimize or segment merges are taking place
  as this can tie up substantial disk space.</p>
  
  <p>Regardless of <code>autoCommit</code>, an {@link
  IndexReader} or {@link org.apache.lucene.search.IndexSearcher} will only see the
  index as of the "point in time" that it was opened.  Any
  changes committed to the index after the reader was opened
  are not visible until the reader is re-opened.</p>

  <p>If an index will not have more documents added for a while and optimal search
  performance is desired, then either the full <a href="#optimize()"><b>optimize</b></a>
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
*/

/*
 * Clarification: Check Points (and commits)
 * Being able to set autoCommit=false allows IndexWriter to flush and 
 * write new index files to the directory without writing a new segments_N
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
 * With autoCommit=true, every checkPoint is also a CommitPoint.
 * With autoCommit=false, some checkPoints may not be commits.
 * 
 * A new checkpoint always replaces the previous checkpoint and 
 * becomes the new "front" of the index. This allows the IndexFileDeleter 
 * to delete files that are referenced only by stale checkpoints.
 * (files that were created since the last commit, but are no longer
 * referenced by the "front" of the index). For this, IndexFileDeleter 
 * keeps track of the last non commit checkpoint.
 */
public class IndexWriter {

  /**
   * Default value for the write lock timeout (1,000).
   * @see #setDefaultWriteLockTimeout
   */
  public static long WRITE_LOCK_TIMEOUT = 1000;

  private long writeLockTimeout = WRITE_LOCK_TIMEOUT;

  /**
   * Name of the write lock in the index.
   */
  public static final String WRITE_LOCK_NAME = "write.lock";

  /**
   * @deprecated
   * @see LogMergePolicy#DEFAULT_MERGE_FACTOR
   */
  public final static int DEFAULT_MERGE_FACTOR = LogMergePolicy.DEFAULT_MERGE_FACTOR;

  /**
   * Value to denote a flush trigger is disabled
   */
  public final static int DISABLE_AUTO_FLUSH = -1;

  /**
   * Disabled by default (because IndexWriter flushes by RAM usage
   * by default). Change using {@link #setMaxBufferedDocs(int)}.
   */
  public final static int DEFAULT_MAX_BUFFERED_DOCS = DISABLE_AUTO_FLUSH;

  /**
   * Default value is 16 MB (which means flush when buffered
   * docs consume 16 MB RAM).  Change using {@link #setRAMBufferSizeMB}.
   */
  public final static double DEFAULT_RAM_BUFFER_SIZE_MB = 16.0;

  /**
   * Disabled by default (because IndexWriter flushes by RAM usage
   * by default). Change using {@link #setMaxBufferedDeleteTerms(int)}.
   */
  public final static int DEFAULT_MAX_BUFFERED_DELETE_TERMS = DISABLE_AUTO_FLUSH;

  /**
   * @deprecated
   * @see LogDocMergePolicy#DEFAULT_MAX_MERGE_DOCS
   */
  public final static int DEFAULT_MAX_MERGE_DOCS = LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS;

  /**
   * Default value is 10,000. Change using {@link #setMaxFieldLength(int)}.
   */
  public final static int DEFAULT_MAX_FIELD_LENGTH = 10000;

  /**
   * Default value is 128. Change using {@link #setTermIndexInterval(int)}.
   */
  public final static int DEFAULT_TERM_INDEX_INTERVAL = 128;

  /**
   * Absolute hard maximum length for a term.  If a term
   * arrives from the analyzer longer than this length, it
   * is skipped and a message is printed to infoStream, if
   * set (see {@link #setInfoStream}).
   */
  public final static int MAX_TERM_LENGTH = DocumentsWriter.MAX_TERM_LENGTH;

  /**
   * Default for {@link #getMaxSyncPauseSeconds}.  On
   * Windows this defaults to 10.0 seconds; elsewhere it's
   * 0.
   */
  public final static double DEFAULT_MAX_SYNC_PAUSE_SECONDS;
  static {
    if (Constants.WINDOWS)
      DEFAULT_MAX_SYNC_PAUSE_SECONDS = 10.0;
    else
      DEFAULT_MAX_SYNC_PAUSE_SECONDS = 0.0;
  }

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

  private Directory directory;  // where this index resides
  private Analyzer analyzer;    // how to analyze text

  private Similarity similarity = Similarity.getDefault(); // how to normalize

  private volatile long changeCount; // increments every time a change is completed
  private long lastCommitChangeCount; // last changeCount that was committed

  private SegmentInfos rollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails
  private HashMap rollbackSegments;

  volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
  volatile long pendingCommitChangeCount;

  private SegmentInfos localRollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails
  private boolean localAutoCommit;                // saved autoCommit during local transaction
  private int localFlushedDocCount;               // saved docWriter.getFlushedDocCount during local transaction
  private boolean autoCommit = true;              // false if we should commit only on close

  private SegmentInfos segmentInfos = new SegmentInfos();       // the segments

  private DocumentsWriter docWriter;
  private IndexFileDeleter deleter;

  private Set segmentsToOptimize = new HashSet();           // used by optimize to note those needing optimization

  private Lock writeLock;

  private int termIndexInterval = DEFAULT_TERM_INDEX_INTERVAL;

  private boolean closeDir;
  private boolean closed;
  private boolean closing;

  // Holds all SegmentInfo instances currently involved in
  // merges
  private HashSet mergingSegments = new HashSet();

  private MergePolicy mergePolicy = new LogByteSizeMergePolicy();
  private MergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
  private LinkedList pendingMerges = new LinkedList();
  private Set runningMerges = new HashSet();
  private List mergeExceptions = new ArrayList();
  private long mergeGen;
  private boolean stopMerges;

  private int flushCount;
  private int flushDeletesCount;
  private double maxSyncPauseSeconds = DEFAULT_MAX_SYNC_PAUSE_SECONDS;

  // Used to only allow one addIndexes to proceed at once
  // TODO: use ReadWriteLock once we are on 5.0
  private int readCount;                          // count of how many threads are holding read lock
  private Thread writeThread;                     // non-null if any thread holds write lock
  private int upgradeCount;

  synchronized void acquireWrite() {
    assert writeThread != Thread.currentThread();
    while(writeThread != null || readCount > 0)
      doWait();

    // We could have been closed while we were waiting:
    ensureOpen();

    writeThread = Thread.currentThread();
  }

  synchronized void releaseWrite() {
    assert Thread.currentThread() == writeThread;
    writeThread = null;
    notifyAll();
  }

  synchronized void acquireRead() {
    final Thread current = Thread.currentThread();
    while(writeThread != null && writeThread != current)
      doWait();

    readCount++;
  }

  // Allows one readLock to upgrade to a writeLock even if
  // there are other readLocks as long as all other
  // readLocks are also blocked in this method:
  synchronized void upgradeReadToWrite() {
    assert readCount > 0;
    upgradeCount++;
    while(readCount > upgradeCount || writeThread != null) {
      doWait();
    }
    
    writeThread = Thread.currentThread();
    readCount--;
    upgradeCount--;
  }

  synchronized void releaseRead() {
    readCount--;
    assert readCount >= 0;
    notifyAll();
  }

  /**
   * Used internally to throw an {@link
   * AlreadyClosedException} if this IndexWriter has been
   * closed.
   * @throws AlreadyClosedException if this IndexWriter is
   */
  protected synchronized final void ensureOpen(boolean includePendingClose) throws AlreadyClosedException {
    if (closed || (includePendingClose && closing)) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  protected synchronized final void ensureOpen() throws AlreadyClosedException {
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
   */
  public boolean getUseCompoundFile() {
    return getLogMergePolicy().getUseCompoundFile();
  }

  /** <p>Setting to turn on usage of a compound file. When on,
   *  multiple files for each segment are merged into a
   *  single file when a new segment is flushed.</p>
   *
   *  <p>Note that this method is a convenience method: it
   *  just calls mergePolicy.setUseCompoundFile as long as
   *  mergePolicy is an instance of {@link LogMergePolicy}.
   *  Otherwise an IllegalArgumentException is thrown.</p>
   */
  public void setUseCompoundFile(boolean value) {
    getLogMergePolicy().setUseCompoundFile(value);
    getLogMergePolicy().setUseCompoundDocStore(value);
  }

  /** Expert: Set the Similarity implementation used by this IndexWriter.
   *
   * @see Similarity#setDefault(Similarity)
   */
  public void setSimilarity(Similarity similarity) {
    ensureOpen();
    this.similarity = similarity;
    docWriter.setSimilarity(similarity);
  }

  /** Expert: Return the Similarity implementation used by this IndexWriter.
   *
   * <p>This defaults to the current value of {@link Similarity#getDefault()}.
   */
  public Similarity getSimilarity() {
    ensureOpen();
    return this.similarity;
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
   */
  public void setTermIndexInterval(int interval) {
    ensureOpen();
    this.termIndexInterval = interval;
  }

  /** Expert: Return the interval between indexed terms.
   *
   * @see #setTermIndexInterval(int)
   */
  public int getTermIndexInterval() {
    // We pass false because this method is called by SegmentMerger while we are in the process of closing
    ensureOpen(false);
    return termIndexInterval;
  }

  /**
   * Constructs an IndexWriter for the index in <code>path</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>path</code>, replacing the index already there,
   * if any.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param mfl Maximum field length: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   */
  public IndexWriter(String path, Analyzer a, boolean create, MaxFieldLength mfl)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, create, true, null, false, mfl.getLimit());
  }

  /**
   * Constructs an IndexWriter for the index in <code>path</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>path</code>, replacing the index already there, if any.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(String,Analyzer,boolean,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(String path, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, create, true, null, true, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in <code>path</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>path</code>, replacing the index already there, if any.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param mfl Maximum field length: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   */
  public IndexWriter(File path, Analyzer a, boolean create, MaxFieldLength mfl)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, create, true, null, false, mfl.getLimit());
  }

  /**
   * Constructs an IndexWriter for the index in <code>path</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>path</code>, replacing the index already there, if any.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(File,Analyzer,boolean,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(File path, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, create, true, null, true, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in <code>d</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>d</code>, replacing the index already there, if any.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param mfl Maximum field length: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   */
  public IndexWriter(Directory d, Analyzer a, boolean create, MaxFieldLength mfl)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, null, false, mfl.getLimit());
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
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0
   *  release, and call {@link #commit()} when needed.
   *  Use {@link #IndexWriter(Directory,Analyzer,boolean,MaxFieldLength)} instead.
   */
  public IndexWriter(Directory d, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, null, true, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>path</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param mfl Maximum field length: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   */
  public IndexWriter(String path, Analyzer a, MaxFieldLength mfl)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, true, null, false, mfl.getLimit());
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>path</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0
   *  release, and call {@link #commit()} when needed.
   *  Use {@link #IndexWriter(String,Analyzer,MaxFieldLength)} instead.
   */
  public IndexWriter(String path, Analyzer a)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, true, null, true, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>path</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @param mfl Maximum field length: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   */
  public IndexWriter(File path, Analyzer a, MaxFieldLength mfl)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, true, null, false, mfl.getLimit());
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>path</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * @param path the path to the index directory
   * @param a the analyzer to use
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link #IndexWriter(File,Analyzer,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(File path, Analyzer a)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, true, null, true, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>d</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param mfl Maximum field length: LIMITED, UNLIMITED, or user-specified
   *   via the MaxFieldLength constructor.
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   */
  public IndexWriter(Directory d, Analyzer a, MaxFieldLength mfl)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, null, false, mfl.getLimit());
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>d</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(Directory,Analyzer,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(Directory d, Analyzer a)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, null, true, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in
   * <code>d</code>, first creating it if it does not
   * already exist.  Text will be analyzed with
   * <code>a</code>.
   *
   * @param d the index directory
   * @param autoCommit see <a href="#autoCommit">above</a>
   * @param a the analyzer to use
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(Directory,Analyzer,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, null, autoCommit, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Constructs an IndexWriter for the index in <code>d</code>.
   * Text will be analyzed with <code>a</code>.  If <code>create</code>
   * is true, then a new, empty index will be created in
   * <code>d</code>, replacing the index already there, if any.
   *
   * @param d the index directory
   * @param autoCommit see <a href="#autoCommit">above</a>
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(Directory,Analyzer,boolean,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, null, autoCommit, DEFAULT_MAX_FIELD_LENGTH);
  }

  /**
   * Expert: constructs an IndexWriter with a custom {@link
   * IndexDeletionPolicy}, for the index in <code>d</code>,
   * first creating it if it does not already exist.  Text
   * will be analyzed with <code>a</code>.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
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
   */
  public IndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, deletionPolicy, false, mfl.getLimit());
  }

  /**
   * Expert: constructs an IndexWriter with a custom {@link
   * IndexDeletionPolicy}, for the index in <code>d</code>,
   * first creating it if it does not already exist.  Text
   * will be analyzed with <code>a</code>.
   *
   * @param d the index directory
   * @param autoCommit see <a href="#autoCommit">above</a>
   * @param a the analyzer to use
   * @param deletionPolicy see <a href="#deletionPolicy">above</a>
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be
   *  read/written to or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(Directory,Analyzer,IndexDeletionPolicy,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a, IndexDeletionPolicy deletionPolicy)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, deletionPolicy, autoCommit, DEFAULT_MAX_FIELD_LENGTH);
  }
  
  /**
   * Expert: constructs an IndexWriter with a custom {@link
   * IndexDeletionPolicy}, for the index in <code>d</code>.
   * Text will be analyzed with <code>a</code>.  If
   * <code>create</code> is true, then a new, empty index
   * will be created in <code>d</code>, replacing the index
   * already there, if any.
   *
   * <p><b>NOTE</b>: autoCommit (see <a
   * href="#autoCommit">above</a>) is set to false with this
   * constructor.
   *
   * @param d the index directory
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param deletionPolicy see <a href="#deletionPolicy">above</a>
   * @param mfl whether or not to limit field lengths
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   */
  public IndexWriter(Directory d, Analyzer a, boolean create, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, deletionPolicy, false, mfl.getLimit());
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
   * @param autoCommit see <a href="#autoCommit">above</a>
   * @param a the analyzer to use
   * @param create <code>true</code> to create the index or overwrite
   *  the existing one; <code>false</code> to append to the existing
   *  index
   * @param deletionPolicy see <a href="#deletionPolicy">above</a>
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if the directory cannot be read/written to, or
   *  if it does not exist and <code>create</code> is
   *  <code>false</code> or if there is any other low-level
   *  IO error
   * @deprecated This constructor will be removed in the 3.0 release.
   *  Use {@link
   *  #IndexWriter(Directory,Analyzer,boolean,IndexDeletionPolicy,MaxFieldLength)}
   *  instead, and call {@link #commit()} when needed.
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a, boolean create, IndexDeletionPolicy deletionPolicy)
          throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, deletionPolicy, autoCommit, DEFAULT_MAX_FIELD_LENGTH);
  }

  private void init(Directory d, Analyzer a, boolean closeDir, IndexDeletionPolicy deletionPolicy, boolean autoCommit, int maxFieldLength)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    if (IndexReader.indexExists(d)) {
      init(d, a, false, closeDir, deletionPolicy, autoCommit, maxFieldLength);
    } else {
      init(d, a, true, closeDir, deletionPolicy, autoCommit, maxFieldLength);
    }
  }

  private void init(Directory d, Analyzer a, final boolean create, boolean closeDir, IndexDeletionPolicy deletionPolicy, boolean autoCommit, int maxFieldLength)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    this.closeDir = closeDir;
    directory = d;
    analyzer = a;
    setMessageID(defaultInfoStream);
    this.maxFieldLength = maxFieldLength;

    if (create) {
      // Clear the write lock in case it's leftover:
      directory.clearLock(WRITE_LOCK_NAME);
    }

    Lock writeLock = directory.makeLock(WRITE_LOCK_NAME);
    if (!writeLock.obtain(writeLockTimeout)) // obtain write lock
      throw new LockObtainFailedException("Index locked for write: " + writeLock);
    this.writeLock = writeLock;                   // save it

    try {
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
        }
        segmentInfos.commit(directory);
      } else {
        segmentInfos.read(directory);

        // We assume that this segments_N was previously
        // properly sync'd:
        for(int i=0;i<segmentInfos.size();i++) {
          final SegmentInfo info = segmentInfos.info(i);
          List files = info.files();
          for(int j=0;j<files.size();j++)
            synced.add(files.get(j));
        }
      }

      this.autoCommit = autoCommit;
      setRollbackSegmentInfos(segmentInfos);

      docWriter = new DocumentsWriter(directory, this);
      docWriter.setInfoStream(infoStream);
      docWriter.setMaxFieldLength(maxFieldLength);

      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:
      deleter = new IndexFileDeleter(directory,
                                     deletionPolicy == null ? new KeepOnlyLastCommitDeletionPolicy() : deletionPolicy,
                                     segmentInfos, infoStream, docWriter);

      pushMaxBufferedDocs();

      if (infoStream != null) {
        message("init: create=" + create);
        messageState();
      }

    } catch (IOException e) {
      this.writeLock.release();
      this.writeLock = null;
      throw e;
    }
  }

  private synchronized void setRollbackSegmentInfos(SegmentInfos infos) {
    rollbackSegmentInfos = (SegmentInfos) infos.clone();
    assert !hasExternalSegments(rollbackSegmentInfos);
    rollbackSegments = new HashMap();
    final int size = rollbackSegmentInfos.size();
    for(int i=0;i<size;i++)
      rollbackSegments.put(rollbackSegmentInfos.info(i), new Integer(i));
  }

  /**
   * Expert: set the merge policy used by this writer.
   */
  public void setMergePolicy(MergePolicy mp) {
    ensureOpen();
    if (mp == null)
      throw new NullPointerException("MergePolicy must be non-null");

    if (mergePolicy != mp)
      mergePolicy.close();
    mergePolicy = mp;
    pushMaxBufferedDocs();
    if (infoStream != null)
      message("setMergePolicy " + mp);
  }

  /**
   * Expert: returns the current MergePolicy in use by this writer.
   * @see #setMergePolicy
   */
  public MergePolicy getMergePolicy() {
    ensureOpen();
    return mergePolicy;
  }

  /**
   * Expert: set the merge scheduler used by this writer.
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
  }

  /**
   * Expert: returns the current MergePolicy in use by this
   * writer.
   * @see #setMergePolicy
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
   */
  public void setMaxFieldLength(int maxFieldLength) {
    ensureOpen();
    this.maxFieldLength = maxFieldLength;
    docWriter.setMaxFieldLength(maxFieldLength);
    if (infoStream != null)
      message("setMaxFieldLength " + maxFieldLength);
  }

  /**
   * Returns the maximum number of terms that will be
   * indexed for a single field in a document.
   * @see #setMaxFieldLength
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
   */
  public int getMaxBufferedDocs() {
    ensureOpen();
    return docWriter.getMaxBufferedDocs();
  }

  /** Determines the amount of RAM that may be used for
   * buffering added documents before they are flushed as a
   * new Segment.  Generally for faster indexing performance
   * it's best to flush by RAM usage instead of document
   * count and use as large a RAM buffer as you can.
   *
   * <p>When this is set, the writer will flush whenever
   * buffered documents use this much RAM.  Pass in {@link
   * #DISABLE_AUTO_FLUSH} to prevent triggering a flush due
   * to RAM usage.  Note that if flushing by document count
   * is also enabled, then the flush will be triggered by
   * whichever comes first.</p>
   *
   * <p> The default value is {@link #DEFAULT_RAM_BUFFER_SIZE_MB}.</p>
   * 
   * @throws IllegalArgumentException if ramBufferSize is
   * enabled but non-positive, or it disables ramBufferSize
   * when maxBufferedDocs is already disabled
   */
  public void setRAMBufferSizeMB(double mb) {
    if (mb != DISABLE_AUTO_FLUSH && mb <= 0.0)
      throw new IllegalArgumentException(
          "ramBufferSize should be > 0.0 MB when enabled");
    if (mb == DISABLE_AUTO_FLUSH && getMaxBufferedDocs() == DISABLE_AUTO_FLUSH)
      throw new IllegalArgumentException(
          "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    docWriter.setRAMBufferSizeMB(mb);
    if (infoStream != null)
      message("setRAMBufferSizeMB " + mb);
  }

  /**
   * Returns the value set by {@link #setRAMBufferSizeMB} if enabled.
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
  }

  /**
   * Returns the number of buffered deleted terms that will
   * trigger a flush if enabled.
   * @see #setMaxBufferedDeleteTerms
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
   */
  public int getMergeFactor() {
    return getLogMergePolicy().getMergeFactor();
  }

  /**
   * Expert: returns max delay inserted before syncing a
   * commit point.  On Windows, at least, pausing before
   * syncing can increase net indexing throughput.  The
   * delay is variable based on size of the segment's files,
   * and is only inserted when using
   * ConcurrentMergeScheduler for merges.
   * @deprecated This will be removed in 3.0, when
   * autoCommit=true is removed from IndexWriter.
   */
  public double getMaxSyncPauseSeconds() {
    return maxSyncPauseSeconds;
  }

  /**
   * Expert: sets the max delay before syncing a commit
   * point.
   * @see #getMaxSyncPauseSeconds
   * @deprecated This will be removed in 3.0, when
   * autoCommit=true is removed from IndexWriter.
   */
  public void setMaxSyncPauseSeconds(double seconds) {
    maxSyncPauseSeconds = seconds;
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
    message("setInfoStream: dir=" + directory +
            " autoCommit=" + autoCommit +
            " mergePolicy=" + mergePolicy +
            " mergeScheduler=" + mergeScheduler +
            " ramBufferSizeMB=" + docWriter.getRAMBufferSizeMB() +
            " maxBufferedDocs=" + docWriter.getMaxBufferedDocs() +
            " maxBuffereDeleteTerms=" + docWriter.getMaxBufferedDeleteTerms() +
            " maxFieldLength=" + maxFieldLength +
            " index=" + segString());
  }

  /**
   * Returns the current infoStream in use by this writer.
   * @see #setInfoStream
   */
  public PrintStream getInfoStream() {
    ensureOpen();
    return infoStream;
  }

  /**
   * Sets the maximum time to wait for a write lock (in milliseconds) for this instance of IndexWriter.  @see
   * @see #setDefaultWriteLockTimeout to change the default value for all instances of IndexWriter.
   */
  public void setWriteLockTimeout(long writeLockTimeout) {
    ensureOpen();
    this.writeLockTimeout = writeLockTimeout;
  }

  /**
   * Returns allowed timeout when acquiring the write lock.
   * @see #setWriteLockTimeout
   */
  public long getWriteLockTimeout() {
    ensureOpen();
    return writeLockTimeout;
  }

  /**
   * Sets the default (for any instance of IndexWriter) maximum time to wait for a write lock (in
   * milliseconds).
   */
  public static void setDefaultWriteLockTimeout(long writeLockTimeout) {
    IndexWriter.WRITE_LOCK_TIMEOUT = writeLockTimeout;
  }

  /**
   * Returns default write lock timeout for newly
   * instantiated IndexWriters.
   * @see #setDefaultWriteLockTimeout
   */
  public static long getDefaultWriteLockTimeout() {
    return IndexWriter.WRITE_LOCK_TIMEOUT;
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
      flush(waitForMerges, true, true);

      if (waitForMerges)
        // Give merge scheduler last chance to run, in case
        // any pending merges are waiting:
        mergeScheduler.merge(this);

      mergePolicy.close();

      finishMerges(waitForMerges);

      mergeScheduler.close();

      if (infoStream != null)
        message("now call final commit()");
      
      commit(0);

      if (infoStream != null)
        message("at close: " + segString());

      synchronized(this) {
        docWriter = null;
        deleter.close();
      }
      
      if (closeDir)
        directory.close();

      if (writeLock != null) {
        writeLock.release();                          // release write lock
        writeLock = null;
      }
      synchronized(this) {
        closed = true;
      }
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
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
      if (!success) {
        if (infoStream != null)
          message("hit exception closing doc store segment");
      }
    }

    useCompoundDocStore = mergePolicy.useCompoundDocStore(segmentInfos);
      
    if (useCompoundDocStore && docStoreSegment != null && docWriter.closedFiles().size() != 0) {
      // Now build compound doc store file

      success = false;

      final int numSegments = segmentInfos.size();
      final String compoundFileName = docStoreSegment + "." + IndexFileNames.COMPOUND_FILE_STORE_EXTENSION;

      try {
        CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, compoundFileName);
        final Iterator it = docWriter.closedFiles().iterator();
        while(it.hasNext())
          cfsWriter.addFile((String) it.next());
      
        // Perform the merge
        cfsWriter.close();
        success = true;

      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception building compound file doc store for segment " + docStoreSegment);
          deleter.deleteFile(compoundFileName);
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

  /** Release the write lock, if needed. */
  protected void finalize() throws Throwable {
    try {
      if (writeLock != null) {
        writeLock.release();                        // release write lock
        writeLock = null;
      }
    } finally {
      super.finalize();
    }
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

  /** Returns the number of documents currently in this
   *  index, not counting deletions.
   * @deprecated Please use {@link #maxDoc()} (same as this
   * method) or {@link #numDocs()} (also takes deletions
   * into account), instead. */
  public synchronized int docCount() {
    ensureOpen();
    return maxDoc();
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
   * documents are large, be sure to set this value high enough to accomodate
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
              final Collection files = docWriter.abortedFiles();
              if (files != null)
                deleter.deleteNewFiles(files);
            }
          }
        }
      }
      if (doFlush)
        flush(true, false, false);
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
    }
  }

  /**
   * Deletes the document(s) containing <code>term</code>.
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
      hitOOM = true;
      throw oom;
    }
  }

  /**
   * Deletes the document(s) containing any of the
   * terms. All deletes are flushed at the same time.
   * @param terms array of terms to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Term[] terms) throws CorruptIndexException, IOException {
    ensureOpen();
    try {
      boolean doFlush = docWriter.bufferDeleteTerms(terms);
      if (doFlush)
        flush(true, false, false);
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
    }
  }

  /**
   * Deletes the document(s) matching the provided query.
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
   * @param queries array of queries to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Query[] queries) throws CorruptIndexException, IOException {
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
            final Collection files = docWriter.abortedFiles();
            if (files != null)
              deleter.deleteNewFiles(files);
          }
        }
      }
      if (doFlush)
        flush(true, false, false);
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
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
   * default merge policy, but individaul merge policies may implement
   * optimize in different ways.
   *
   * @see LogMergePolicy#findMergesForOptimize
   *
   * <p>It is recommended that this method be called upon completion of indexing.  In
   * environments with frequent updates, optimize is best done during low volume times, if at all. 
   * 
   * </p>
   * <p>See http://www.gossamer-threads.com/lists/lucene/java-dev/47895 for more discussion. </p>
   *
   * <p>Note that this can require substantial temporary free
   * space in the Directory (see <a target="_top"
   * href="http://issues.apache.org/jira/browse/LUCENE-764">LUCENE-764</a>
   * for details):</p>
   *
   * <ul>
   * <li>
   * 
   * <p>If no readers/searchers are open against the index,
   * then free space required is up to 1X the total size of
   * the starting index.  For example, if the starting
   * index is 10 GB, then you must have up to 10 GB of free
   * space before calling optimize.</p>
   *
   * <li>
   * 
   * <p>If readers/searchers are using the index, then free
   * space required is up to 2X the size of the starting
   * index.  This is because in addition to the 1X used by
   * optimize, the original 1X of the starting index is
   * still consuming space in the Directory as the readers
   * are holding the segments files open.  Even on Unix,
   * where it will appear as if the files are gone ("ls"
   * won't list them), they still consume storage due to
   * "delete on last close" semantics.</p>
   * 
   * <p>Furthermore, if some but not all readers re-open
   * while the optimize is underway, this will cause > 2X
   * temporary space to be consumed as those new readers
   * will then hold open the partially optimized segments at
   * that time.  It is best not to re-open readers while
   * optimize is running.</p>
   *
   * </ul>
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
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
  */
  public void optimize() throws CorruptIndexException, IOException {
    optimize(true);
  }

  /**
   * Optimize the index down to <= maxNumSegments.  If
   * maxNumSegments==1 then this is the same as {@link
   * #optimize()}.
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
   *  background threads. */
  public void optimize(boolean doWait) throws CorruptIndexException, IOException {
    optimize(1, doWait);
  }

  /** Just like {@link #optimize(int)}, except you can
   *  specify whether the call should block until the
   *  optimize completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads. */
  public void optimize(int maxNumSegments, boolean doWait) throws CorruptIndexException, IOException {
    ensureOpen();

    if (maxNumSegments < 1)
      throw new IllegalArgumentException("maxNumSegments must be >= 1; got " + maxNumSegments);

    if (infoStream != null)
      message("optimize: index now " + segString());

    flush(true, false, true);

    synchronized(this) {
      resetMergeExceptions();
      segmentsToOptimize = new HashSet();
      final int numSegments = segmentInfos.size();
      for(int i=0;i<numSegments;i++)
        segmentsToOptimize.add(segmentInfos.info(i));
      
      // Now mark all pending & running merges as optimize
      // merge:
      Iterator it = pendingMerges.iterator();
      while(it.hasNext()) {
        final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) it.next();
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = maxNumSegments;
      }

      it = runningMerges.iterator();
      while(it.hasNext()) {
        final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) it.next();
        merge.optimize = true;
        merge.maxNumSegmentsOptimize = maxNumSegments;
      }
    }

    maybeMerge(maxNumSegments, true);

    if (doWait) {
      synchronized(this) {
        while(true) {
          if (mergeExceptions.size() > 0) {
            // Forward any exceptions in background merge
            // threads to the current thread:
            final int size = mergeExceptions.size();
            for(int i=0;i<size;i++) {
              final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) mergeExceptions.get(0);
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
    Iterator it = pendingMerges.iterator();
    while(it.hasNext())
      if (((MergePolicy.OneMerge) it.next()).optimize)
        return true;

    it = runningMerges.iterator();
    while(it.hasNext())
      if (((MergePolicy.OneMerge) it.next()).optimize)
        return true;

    return false;
  }

  /** Just like {@link #expungeDeletes()}, except you can
   *  specify whether the call should block until the
   *  operation completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads. */
  public void expungeDeletes(boolean doWait)
    throws CorruptIndexException, IOException {
    ensureOpen();

    if (infoStream != null)
      message("expungeDeletes: index now " + segString());

    MergePolicy.MergeSpecification spec;

    synchronized(this) {
      spec = mergePolicy.findMergesToExpungeDeletes(segmentInfos, this);
      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++)
          registerMerge((MergePolicy.OneMerge) spec.merges.get(i));
      }
    }

    mergeScheduler.merge(this);

    if (spec != null && doWait) {
      final int numMerges = spec.merges.size();
      synchronized(this) {
        boolean running = true;
        while(running) {

          // Check each merge that MergePolicy asked us to
          // do, to see if any of them are still running and
          // if any of them have hit an exception.
          running = false;
          for(int i=0;i<numMerges;i++) {
            final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) spec.merges.get(i);
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
   *  See also {@link #expungeDeletes(boolean)} */
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

    final MergePolicy.MergeSpecification spec;
    if (optimize) {
      spec = mergePolicy.findMergesForOptimize(segmentInfos, this, maxNumSegmentsOptimize, segmentsToOptimize);

      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++) {
          final MergePolicy.OneMerge merge = ((MergePolicy.OneMerge) spec.merges.get(i));
          merge.optimize = true;
          merge.maxNumSegmentsOptimize = maxNumSegmentsOptimize;
        }
      }

    } else
      spec = mergePolicy.findMerges(segmentInfos, this);

    if (spec != null) {
      final int numMerges = spec.merges.size();
      for(int i=0;i<numMerges;i++)
        registerMerge((MergePolicy.OneMerge) spec.merges.get(i));
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
      MergePolicy.OneMerge merge = (MergePolicy.OneMerge) pendingMerges.removeFirst();
      runningMerges.add(merge);
      return merge;
    }
  }

  /** Like getNextMerge() except only returns a merge if it's
   *  external. */
  private synchronized MergePolicy.OneMerge getNextExternalMerge() {
    if (pendingMerges.size() == 0)
      return null;
    else {
      Iterator it = pendingMerges.iterator();
      while(it.hasNext()) {
        MergePolicy.OneMerge merge = (MergePolicy.OneMerge) it.next();
        if (merge.isExternal) {
          // Advance the merge from pending to running
          it.remove();
          runningMerges.add(merge);
          return merge;
        }
      }

      // All existing merges do not involve external segments
      return null;
    }
  }

  /*
   * Begin a transaction.  During a transaction, any segment
   * merges that happen (or ram segments flushed) will not
   * write a new segments file and will not remove any files
   * that were present at the start of the transaction.  You
   * must make a matched (try/finally) call to
   * commitTransaction() or rollbackTransaction() to finish
   * the transaction.
   *
   * Note that buffered documents and delete terms are not handled
   * within the transactions, so they must be flushed before the
   * transaction is started.
   */
  private synchronized void startTransaction(boolean haveReadLock) throws IOException {

    boolean success = false;
    try {
      if (infoStream != null)
        message("now start transaction");

      assert docWriter.getNumBufferedDeleteTerms() == 0 :
      "calling startTransaction with buffered delete terms not supported: numBufferedDeleteTerms=" + docWriter.getNumBufferedDeleteTerms();
      assert docWriter.getNumDocsInRAM() == 0 :
      "calling startTransaction with buffered documents not supported: numDocsInRAM=" + docWriter.getNumDocsInRAM();

      ensureOpen();

      // If a transaction is trying to roll back (because
      // addIndexes hit an exception) then wait here until
      // that's done:
      synchronized(this) {
        while(stopMerges)
          doWait();
      }
      success = true;
    } finally {
      // Release the write lock if our caller held it, on
      // hitting an exception
      if (!success && haveReadLock)
        releaseRead();
    }

    if (haveReadLock) {
      upgradeReadToWrite();
    } else {
      acquireWrite();
    }

    success = false;
    try {
      localRollbackSegmentInfos = (SegmentInfos) segmentInfos.clone();

      assert !hasExternalSegments(segmentInfos);

      localAutoCommit = autoCommit;
      localFlushedDocCount = docWriter.getFlushedDocCount();

      if (localAutoCommit) {

        if (infoStream != null)
          message("flush at startTransaction");

        flush(true, false, false);

        // Turn off auto-commit during our local transaction:
        autoCommit = false;
      } else
        // We must "protect" our files at this point from
        // deletion in case we need to rollback:
        deleter.incRef(segmentInfos, false);

      success = true;
    } finally {
      if (!success)
        finishAddIndexes();
    }
  }

  /*
   * Rolls back the transaction and restores state to where
   * we were at the start.
   */
  private synchronized void rollbackTransaction() throws IOException {

    if (infoStream != null)
      message("now rollback transaction");

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;
    docWriter.setFlushedDocCount(localFlushedDocCount);

    // Must finish merges before rolling back segmentInfos
    // so merges don't hit exceptions on trying to commit
    // themselves, don't get files deleted out from under
    // them, etc:
    finishMerges(false);

    // Keep the same segmentInfos instance but replace all
    // of its SegmentInfo instances.  This is so the next
    // attempt to commit using this instance of IndexWriter
    // will always write to a new generation ("write once").
    segmentInfos.clear();
    segmentInfos.addAll(localRollbackSegmentInfos);
    localRollbackSegmentInfos = null;

    // This must come after we rollback segmentInfos, so
    // that if a commit() kicks off it does not see the
    // segmentInfos with external segments
    finishAddIndexes();

    // Ask deleter to locate unreferenced files we had
    // created & remove them:
    deleter.checkpoint(segmentInfos, false);

    if (!autoCommit)
      // Remove the incRef we did in startTransaction:
      deleter.decRef(segmentInfos);

    // Also ask deleter to remove any newly created files
    // that were never incref'd; this "garbage" is created
    // when a merge kicks off but aborts part way through
    // before it had a chance to incRef the files it had
    // partially created
    deleter.refresh();
    
    notifyAll();

    assert !hasExternalSegments();
  }

  /*
   * Commits the transaction.  This will write the new
   * segments file and remove and pending deletions we have
   * accumulated during the transaction
   */
  private synchronized void commitTransaction() throws IOException {

    if (infoStream != null)
      message("now commit transaction");

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;

    // Give deleter a chance to remove files now:
    checkpoint();

    if (autoCommit) {
      boolean success = false;
      try {
        commit(0);
        success = true;
      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception committing transaction");
          rollbackTransaction();
        }
      }
    } else
      // Remove the incRef we did in startTransaction.
      deleter.decRef(localRollbackSegmentInfos);

    localRollbackSegmentInfos = null;

    assert !hasExternalSegments();

    finishAddIndexes();
  }

  /**
   * @deprecated Please use {@link #rollback} instead.
   */
  public void abort() throws IOException {
    rollback();
  }

  /**
   * Close the <code>IndexWriter</code> without committing
   * any changes that have occurred since the last commit
   * (or since it was opened, if commit hasn't been called).
   * This removes any temporary files that had been
   * created, after which the state of the index will be the
   * same as it was when this writer was first opened.  This
   * can only be called when this IndexWriter was opened
   * with <code>autoCommit=false</code>.  This also clears a
   * previous call to {@link #prepareCommit}.
   * @throws IllegalStateException if this is called when
   *  the writer was opened with <code>autoCommit=true</code>.
   * @throws IOException if there is a low-level IO error
   */
  public void rollback() throws IOException {
    ensureOpen();
    if (autoCommit)
      throw new IllegalStateException("rollback() can only be called when IndexWriter was opened with autoCommit=false");

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

        assert !hasExternalSegments();
        
        docWriter.abort();

        assert testPoint("rollback before checkpoint");

        // Ask deleter to locate unreferenced files & remove
        // them:
        deleter.checkpoint(segmentInfos, false);
        deleter.refresh();
      }

      lastCommitChangeCount = changeCount;

      success = true;
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
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

  private synchronized void finishMerges(boolean waitForMerges) throws IOException {
    if (!waitForMerges) {

      stopMerges = true;

      // Abort all pending & running merges:
      Iterator it = pendingMerges.iterator();
      while(it.hasNext()) {
        final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) it.next();
        if (infoStream != null)
          message("now abort pending merge " + merge.segString(directory));
        merge.abort();
        mergeFinish(merge);
      }
      pendingMerges.clear();
      
      it = runningMerges.iterator();
      while(it.hasNext()) {
        final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) it.next();
        if (infoStream != null)
          message("now abort running merge " + merge.segString(directory));
        merge.abort();
      }

      // Ensure any running addIndexes finishes.  It's fine
      // if a new one attempts to start because its merges
      // will quickly see the stopMerges == true and abort.
      acquireRead();
      releaseRead();

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
      // Ensure any running addIndexes finishes.  It's fine
      // if a new one attempts to start because from our
      // caller above the call will see that we are in the
      // process of closing, and will throw an
      // AlreadyClosedException.
      acquireRead();
      releaseRead();
      while(pendingMerges.size() > 0 || runningMerges.size() > 0)
        doWait();
      assert 0 == mergingSegments.size();
    }
  }
 
  /*
   * Called whenever the SegmentInfos has been updated and
   * the index files referenced exist (correctly) in the
   * index directory.
   */
  private synchronized void checkpoint() throws IOException {
    changeCount++;
    deleter.checkpoint(segmentInfos, false);
  }

  private void finishAddIndexes() {
    releaseWrite();
  }

  private void blockAddIndexes(boolean includePendingClose) {

    acquireRead();

    boolean success = false;
    try {

      // Make sure we are still open since we could have
      // waited quite a while for last addIndexes to finish
      ensureOpen(includePendingClose);
      success = true;
    } finally {
      if (!success)
        releaseRead();
    }
  }

  private void resumeAddIndexes() {
    releaseRead();
  }

  /** Merges all segments from an array of indexes into this index.
   * @deprecated Use {@link #addIndexesNoOptimize} instead,
   * then separately call {@link #optimize} afterwards if
   * you need to.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addIndexes(Directory[] dirs)
    throws CorruptIndexException, IOException {

    ensureOpen();
    
    noDupDirs(dirs);

    // Do not allow add docs or deletes while we are running:
    docWriter.pauseAllThreads();

    try {

      if (infoStream != null)
        message("flush at addIndexes");
      flush(true, false, true);

      boolean success = false;

      startTransaction(false);

      try {

        int docCount = 0;
        synchronized(this) {
          ensureOpen();
          for (int i = 0; i < dirs.length; i++) {
            SegmentInfos sis = new SegmentInfos();	  // read infos from dir
            sis.read(dirs[i]);
            for (int j = 0; j < sis.size(); j++) {
              final SegmentInfo info = sis.info(j);
              docCount += info.docCount;
              assert !segmentInfos.contains(info);
              segmentInfos.add(info);	  // add each info
            }
          }
        }

        // Notify DocumentsWriter that the flushed count just increased
        docWriter.updateFlushedDocCount(docCount);

        optimize();

        success = true;
      } finally {
        if (success) {
          commitTransaction();
        } else {
          rollbackTransaction();
        }
      }
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
    } finally {
      docWriter.resumeAllThreads();
    }
  }

  private synchronized void resetMergeExceptions() {
    mergeExceptions = new ArrayList();
    mergeGen++;
  }

  private void noDupDirs(Directory[] dirs) {
    HashSet dups = new HashSet();
    for(int i=0;i<dirs.length;i++) {
      if (dups.contains(dirs[i]))
        throw new IllegalArgumentException("Directory " + dirs[i] + " appears more than once");
      if (dirs[i] == directory)
        throw new IllegalArgumentException("Cannot add directory to itself");
      dups.add(dirs[i]);
    }
  }

  /**
   * Merges all segments from an array of indexes into this
   * index.
   *
   * <p>This may be used to parallelize batch indexing.  A large document
   * collection can be broken into sub-collections.  Each sub-collection can be
   * indexed in parallel, on a different thread, process or machine.  The
   * complete index can then be created by merging sub-collection indexes
   * with this method.
   *
   * <p><b>NOTE:</b> the index in each Directory must not be
   * changed (opened by a writer) while this method is
   * running.  This method does not acquire a write lock in
   * each input Directory, so it is up to the caller to
   * enforce this.
   *
   * <p><b>NOTE:</b> while this is running, any attempts to
   * add or delete documents (with another thread) will be
   * paused until this method completes.
   *
   * <p>This method is transactional in how Exceptions are
   * handled: it does not commit a new segments_N file until
   * all indexes are added.  This means if an Exception
   * occurs (for example disk full), then either no indexes
   * will have been added or they all will have been.</p>
   *
   * <p>Note that this requires temporary free space in the
   * Directory up to 2X the sum of all input indexes
   * (including the starting index).  If readers/searchers
   * are open against the starting index, then temporary
   * free space required will be higher by the size of the
   * starting index (see {@link #optimize()} for details).
   * </p>
   *
   * <p>Once this completes, the final size of the index
   * will be less than the sum of all input index sizes
   * (including the starting index).  It could be quite a
   * bit smaller (if there were many pending deletes) or
   * just slightly smaller.</p>
   * 
   * <p>
   * This requires this index not be among those to be added.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addIndexesNoOptimize(Directory[] dirs)
      throws CorruptIndexException, IOException {

    ensureOpen();

    noDupDirs(dirs);

    // Do not allow add docs or deletes while we are running:
    docWriter.pauseAllThreads();

    try {
      if (infoStream != null)
        message("flush at addIndexesNoOptimize");
      flush(true, false, true);

      boolean success = false;

      startTransaction(false);

      try {

        int docCount = 0;
        synchronized(this) {
          ensureOpen();

          for (int i = 0; i < dirs.length; i++) {
            if (directory == dirs[i]) {
              // cannot add this index: segments may be deleted in merge before added
              throw new IllegalArgumentException("Cannot add this index to itself");
            }

            SegmentInfos sis = new SegmentInfos(); // read infos from dir
            sis.read(dirs[i]);
            for (int j = 0; j < sis.size(); j++) {
              SegmentInfo info = sis.info(j);
              assert !segmentInfos.contains(info): "dup info dir=" + info.dir + " name=" + info.name;
              docCount += info.docCount;
              segmentInfos.add(info); // add each info
            }
          }
        }

        // Notify DocumentsWriter that the flushed count just increased
        docWriter.updateFlushedDocCount(docCount);

        maybeMerge();

        ensureOpen();

        // If after merging there remain segments in the index
        // that are in a different directory, just copy these
        // over into our index.  This is necessary (before
        // finishing the transaction) to avoid leaving the
        // index in an unusable (inconsistent) state.
        resolveExternalSegments();

        ensureOpen();

        success = true;

      } finally {
        if (success) {
          commitTransaction();
        } else {
          rollbackTransaction();
        }
      }
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
    } finally {
      docWriter.resumeAllThreads();
    }
  }

  private boolean hasExternalSegments() {
    return hasExternalSegments(segmentInfos);
  }

  private boolean hasExternalSegments(SegmentInfos infos) {
    final int numSegments = infos.size();
    for(int i=0;i<numSegments;i++)
      if (infos.info(i).dir != directory)
        return true;
    return false;
  }

  /* If any of our segments are using a directory != ours
   * then we have to either copy them over one by one, merge
   * them (if merge policy has chosen to) or wait until
   * currently running merges (in the background) complete.
   * We don't return until the SegmentInfos has no more
   * external segments.  Currently this is only used by
   * addIndexesNoOptimize(). */
  private void resolveExternalSegments() throws CorruptIndexException, IOException {

    boolean any = false;

    boolean done = false;

    while(!done) {
      SegmentInfo info = null;
      MergePolicy.OneMerge merge = null;
      synchronized(this) {

        if (stopMerges)
          throw new MergePolicy.MergeAbortedException("rollback() was called or addIndexes* hit an unhandled exception");

        final int numSegments = segmentInfos.size();

        done = true;
        for(int i=0;i<numSegments;i++) {
          info = segmentInfos.info(i);
          if (info.dir != directory) {
            done = false;
            final MergePolicy.OneMerge newMerge = new MergePolicy.OneMerge(segmentInfos.range(i, 1+i), info.getUseCompoundFile());

            // Returns true if no running merge conflicts
            // with this one (and, records this merge as
            // pending), ie, this segment is not currently
            // being merged:
            if (registerMerge(newMerge)) {
              merge = newMerge;

              // If this segment is not currently being
              // merged, then advance it to running & run
              // the merge ourself (below):
              pendingMerges.remove(merge);
              runningMerges.add(merge);
              break;
            }
          }
        }

        if (!done && merge == null)
          // We are not yet done (external segments still
          // exist in segmentInfos), yet, all such segments
          // are currently "covered" by a pending or running
          // merge.  We now try to grab any pending merge
          // that involves external segments:
          merge = getNextExternalMerge();

        if (!done && merge == null)
          // We are not yet done, and, all external segments
          // fall under merges that the merge scheduler is
          // currently running.  So, we now wait and check
          // back to see if the merge has completed.
          doWait();
      }

      if (merge != null) {
        any = true;
        merge(merge);
      }
    }

    if (any)
      // Sometimes, on copying an external segment over,
      // more merges may become necessary:
      mergeScheduler.merge(this);
  }

  /** Merges the provided indexes into this index.
   * <p>After this completes, the index is optimized. </p>
   * <p>The provided IndexReaders are not closed.</p>

   * <p><b>NOTE:</b> the index in each Directory must not be
   * changed (opened by a writer) while this method is
   * running.  This method does not acquire a write lock in
   * each input Directory, so it is up to the caller to
   * enforce this.
   *
   * <p><b>NOTE:</b> while this is running, any attempts to
   * add or delete documents (with another thread) will be
   * paused until this method completes.
   *
   * <p>See {@link #addIndexesNoOptimize(Directory[])} for
   * details on transactional semantics, temporary free
   * space required in the Directory, and non-CFS segments
   * on an Exception.</p>
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void addIndexes(IndexReader[] readers)
    throws CorruptIndexException, IOException {

    ensureOpen();

    // Do not allow add docs or deletes while we are running:
    docWriter.pauseAllThreads();

    // We must pre-acquire a read lock here (and upgrade to
    // write lock in startTransaction below) so that no
    // other addIndexes is allowed to start up after we have
    // flushed & optimized but before we then start our
    // transaction.  This is because the merging below
    // requires that only one segment is present in the
    // index:
    acquireRead();

    try {

      SegmentInfo info = null;
      String mergedName = null;
      SegmentMerger merger = null;

      boolean success = false;

      try {
        flush(true, false, true);
        optimize();					  // start with zero or 1 seg
        success = true;
      } finally {
        // Take care to release the read lock if we hit an
        // exception before starting the transaction
        if (!success)
          releaseRead();
      }

      // true means we already have a read lock; if this
      // call hits an exception it will release the write
      // lock:
      startTransaction(true);

      try {
        mergedName = newSegmentName();
        merger = new SegmentMerger(this, mergedName, null);

        IndexReader sReader = null;
        synchronized(this) {
          if (segmentInfos.size() == 1) { // add existing index, if any
            sReader = SegmentReader.get(true, segmentInfos.info(0));
          }
        }
        
        success = false;

        try {
          if (sReader != null)
            merger.add(sReader);

          for (int i = 0; i < readers.length; i++)      // add new indexes
            merger.add(readers[i]);

          int docCount = merger.merge();                // merge 'em

          if(sReader != null) {
            sReader.close();
            sReader = null;
          }

          synchronized(this) {
            segmentInfos.clear();                      // pop old infos & add new
            info = new SegmentInfo(mergedName, docCount, directory, false, true,
                                   -1, null, false, merger.hasProx());
            segmentInfos.add(info);
          }

          // Notify DocumentsWriter that the flushed count just increased
          docWriter.updateFlushedDocCount(docCount);

          success = true;

        } finally {
          if (sReader != null) {
            sReader.close();
          }
        }
      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception in addIndexes during merge");
          rollbackTransaction();
        } else {
          commitTransaction();
        }
      }
    
      if (mergePolicy instanceof LogMergePolicy && getUseCompoundFile()) {

        List files = null;

        synchronized(this) {
          // Must incRef our files so that if another thread
          // is running merge/optimize, it doesn't delete our
          // segment's files before we have a change to
          // finish making the compound file.
          if (segmentInfos.contains(info)) {
            files = info.files();
            deleter.incRef(files);
          }
        }

        if (files != null) {

          success = false;

          startTransaction(false);

          try {
            merger.createCompoundFile(mergedName + ".cfs");
            synchronized(this) {
              info.setUseCompoundFile(true);
            }
          
            success = true;
          
          } finally {

            deleter.decRef(files);

            if (!success) {
              if (infoStream != null)
                message("hit exception building compound file in addIndexes during merge");

              rollbackTransaction();
            } else {
              commitTransaction();
            }
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
    } finally {
      docWriter.resumeAllThreads();
    }
  }

  // This is called after pending added and deleted
  // documents have been flushed to the Directory but before
  // the change is committed (new segments_N file written).
  void doAfterFlush()
    throws IOException {
  }

  /**
   * Flush all in-memory buffered updates (adds and deletes)
   * to the Directory. 
   * <p>Note: while this will force buffered docs to be
   * pushed into the index, it will not make these docs
   * visible to a reader.  Use {@link #commit()} instead
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated please call {@link #commit()}) instead
   */
  public final void flush() throws CorruptIndexException, IOException {  
    if (hitOOM)
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot flush");

    flush(true, false, true);
  }

  /** <p>Expert: prepare for commit.  This does the first
   *  phase of 2-phase commit.  You can only call this when
   *  autoCommit is false.  This method does all steps
   *  necessary to commit changes since this writer was
   *  opened: flushes pending added and deleted docs, syncs
   *  the index files, writes most of next segments_N file.
   *  After calling this you must call either {@link
   *  #commit()} to finish the commit, or {@link
   *  #rollback()} to revert the commit and undo all changes
   *  done since the writer was opened.</p>
   *
   * You can also just call {@link #commit()} directly
   * without prepareCommit first in which case that method
   * will internally call prepareCommit.
   */
  public final void prepareCommit() throws CorruptIndexException, IOException {
    ensureOpen();
    prepareCommit(false);
  }

  private final void prepareCommit(boolean internal) throws CorruptIndexException, IOException {

    if (hitOOM)
      throw new IllegalStateException("this writer hit an OutOfMemoryError; cannot commit");

    if (autoCommit && !internal)
      throw new IllegalStateException("this method can only be used when autoCommit is false");

    if (!autoCommit && pendingCommit != null)
      throw new IllegalStateException("prepareCommit was already called with no corresponding call to commit");

    message("prepareCommit: flush");

    flush(true, true, true);

    startCommit(0);
  }

  private void commit(long sizeInBytes) throws IOException {
    startCommit(sizeInBytes);
    finishCommit();
  }

  /**
   * <p>Commits all pending updates (added & deleted
   * documents) to the index, and syncs all referenced index
   * files, such that a reader will see the changes and the
   * index updates will survive an OS or machine crash or
   * power loss.  Note that this does not wait for any
   * running background merges to finish.  This may be a
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
   * @see #prepareCommit
   */

  public final void commit() throws CorruptIndexException, IOException {

    ensureOpen();

    if (infoStream != null)
      message("commit: start");

    if (autoCommit || pendingCommit == null) {
      if (infoStream != null)
        message("commit: now prepare");
      prepareCommit(true);
    } else if (infoStream != null)
      message("commit: already prepared");

    finishCommit();
  }

  private synchronized final void finishCommit() throws CorruptIndexException, IOException {

    if (pendingCommit != null) {
      try {
        message("commit: pendingCommit != null");
        pendingCommit.finishCommit(directory);
        lastCommitChangeCount = pendingCommitChangeCount;
        segmentInfos.updateGeneration(pendingCommit);
        setRollbackSegmentInfos(pendingCommit);
        deleter.checkpoint(pendingCommit, true);
      } finally {
        deleter.decRef(pendingCommit);
        pendingCommit = null;
        notifyAll();
      }

    } else
      message("commit: pendingCommit == null; skip");

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

    ensureOpen(false);

    assert testPoint("startDoFlush");

    flushCount++;

    flushDeletes |= docWriter.deletesFull();

    // When autoCommit=true we must always flush deletes
    // when flushing a segment; otherwise deletes may become
    // visible before their corresponding added document
    // from an updateDocument call
    flushDeletes |= autoCommit;

    // Make sure no threads are actively adding a document.
    // Returns true if docWriter is currently aborting, in
    // which case we skip flushing this segment
    if (docWriter.pauseAllThreads()) {
      docWriter.resumeAllThreads();
      return false;
    }

    try {

      SegmentInfo newSegment = null;

      final int numDocs = docWriter.getNumDocsInRAM();

      // Always flush docs if there are any
      boolean flushDocs = numDocs > 0;

      // With autoCommit=true we always must flush the doc
      // stores when we flush
      flushDocStores |= autoCommit;
      String docStoreSegment = docWriter.getDocStoreSegment();
      if (docStoreSegment == null)
        flushDocStores = false;

      int docStoreOffset = docWriter.getDocStoreOffset();

      // docStoreOffset should only be non-zero when
      // autoCommit == false
      assert !autoCommit || 0 == docStoreOffset;

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
                                     directory, false, true,
                                     docStoreOffset, docStoreSegment,
                                     docStoreIsCompoundFile,    
                                     docWriter.hasProx());
      }

      docWriter.pushDeletes();

      if (flushDocs)
        segmentInfos.add(newSegment);

      if (flushDeletes) {
        flushDeletesCount++;
        applyDeletes();
      }
      
      doAfterFlush();

      if (flushDocs)
        checkpoint();

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
            deleter.deleteFile(segment + "." + IndexFileNames.COMPOUND_FILE_EXTENSION);
          }
        }

        newSegment.setUseCompoundFile(true);
        checkpoint();
      }

      return flushDocs;

    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
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
      throw new MergePolicy.MergeException("could not find segment " + merge.segments.info(0).name + " in current segments", directory);

    final int numSegments = segmentInfos.size();
    
    final int numSegmentsToMerge = merge.segments.size();
    for(int i=0;i<numSegmentsToMerge;i++) {
      final SegmentInfo info = merge.segments.info(i);

      if (first + i >= numSegments || !segmentInfos.info(first+i).equals(info)) {
        if (segmentInfos.indexOf(info) == -1)
          throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.name + ") that is not in the index", directory);
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
  synchronized private void commitMergedDeletes(MergePolicy.OneMerge merge) throws IOException {

    assert testPoint("startCommitMergeDeletes");

    final SegmentInfos sourceSegmentsClone = merge.segmentsClone;
    final SegmentInfos sourceSegments = merge.segments;

    if (infoStream != null)
      message("commitMergeDeletes " + merge.segString(directory));

    // Carefully merge deletes that occurred after we
    // started merging:

    BitVector deletes = null;
    int docUpto = 0;
    int delCount = 0;

    final int numSegmentsToMerge = sourceSegments.size();
    for(int i=0;i<numSegmentsToMerge;i++) {
      final SegmentInfo previousInfo = sourceSegmentsClone.info(i);
      final SegmentInfo currentInfo = sourceSegments.info(i);

      assert currentInfo.docCount == previousInfo.docCount;

      final int docCount = currentInfo.docCount;

      if (previousInfo.hasDeletions()) {

        // There were deletes on this segment when the merge
        // started.  The merge has collapsed away those
        // deletes, but, if new deletes were flushed since
        // the merge started, we must now carefully keep any
        // newly flushed deletes but mapping them to the new
        // docIDs.

        assert currentInfo.hasDeletions();

        // Load deletes present @ start of merge, for this segment:
        BitVector previousDeletes = new BitVector(previousInfo.dir, previousInfo.getDelFileName());

        if (!currentInfo.getDelFileName().equals(previousInfo.getDelFileName())) {
          // This means this segment has had new deletes
          // committed since we started the merge, so we
          // must merge them:
          if (deletes == null)
            deletes = new BitVector(merge.info.docCount);

          BitVector currentDeletes = new BitVector(currentInfo.dir, currentInfo.getDelFileName());
          for(int j=0;j<docCount;j++) {
            if (previousDeletes.get(j))
              assert currentDeletes.get(j);
            else {
              if (currentDeletes.get(j)) {
                deletes.set(docUpto);
                delCount++;
              }
              docUpto++;
            }
          }
        } else
          docUpto += docCount - previousDeletes.count();
        
      } else if (currentInfo.hasDeletions()) {
        // This segment had no deletes before but now it
        // does:
        if (deletes == null)
          deletes = new BitVector(merge.info.docCount);
        BitVector currentDeletes = new BitVector(directory, currentInfo.getDelFileName());

        for(int j=0;j<docCount;j++) {
          if (currentDeletes.get(j)) {
            deletes.set(docUpto);
            delCount++;
          }
          docUpto++;
        }
            
      } else
        // No deletes before or after
        docUpto += currentInfo.docCount;
    }

    if (deletes != null) {
      merge.info.advanceDelGen();
      message("commit merge deletes to " + merge.info.getDelFileName());
      deletes.write(directory, merge.info.getDelFileName());
      merge.info.setDelCount(delCount);
      assert delCount == deletes.count();
    }
  }

  /* FIXME if we want to support non-contiguous segment merges */
  synchronized private boolean commitMerge(MergePolicy.OneMerge merge, SegmentMerger merger, int mergedDocCount) throws IOException {

    assert testPoint("startCommitMerge");

    if (hitOOM)
      return false;

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

    commitMergedDeletes(merge);

    docWriter.remapDeletes(segmentInfos, merger.getDocMaps(), merger.getDelCounts(), merge, mergedDocCount);
      
    // Simple optimization: if the doc store we are using
    // has been closed and is in now compound format (but
    // wasn't when we started), then we will switch to the
    // compound format as well:
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

    merge.info.setHasProx(merger.hasProx());

    segmentInfos.subList(start, start + merge.segments.size()).clear();
    assert !segmentInfos.contains(merge.info);
    segmentInfos.add(start, merge.info);

    // Must checkpoint before decrefing so any newly
    // referenced files in the new merge.info are incref'd
    // first:
    checkpoint();

    decrefMergeSegments(merge);

    if (merge.optimize)
      segmentsToOptimize.add(merge.info);
    return true;
  }

  private void decrefMergeSegments(MergePolicy.OneMerge merge) throws IOException {
    final SegmentInfos sourceSegmentsClone = merge.segmentsClone;
    final int numSegmentsToMerge = sourceSegmentsClone.size();
    assert merge.increfDone;
    merge.increfDone = false;
    for(int i=0;i<numSegmentsToMerge;i++) {
      final SegmentInfo previousInfo = sourceSegmentsClone.info(i);
      // Decref all files for this SegmentInfo (this
      // matches the incref in mergeInit):
      if (previousInfo.dir == directory)
        deleter.decRef(previousInfo.files());
    }
  }

  final private void handleMergeException(Throwable t, MergePolicy.OneMerge merge) throws IOException {
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

    try {
      try {
        try {
          mergeInit(merge);

          if (infoStream != null)
            message("now merge\n  merge=" + merge.segString(directory) + "\n  merge=" + merge + "\n  index=" + segString());

          mergeMiddle(merge);
          success = true;
        } catch (Throwable t) {
          handleMergeException(t, merge);
        }
      } finally {
        synchronized(this) {
          try {

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
          } finally {
            runningMerges.remove(merge);
          }
        }
      }
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
    }
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
        runningMerges.remove(merge);
      }
    }
  }

  final synchronized private void _mergeInit(MergePolicy.OneMerge merge) throws IOException {

    assert testPoint("startMergeInit");

    assert merge.registerDone;
    assert !merge.optimize || merge.maxNumSegmentsOptimize > 0;

    if (merge.info != null)
      // mergeInit already done
      return;

    if (merge.isAborted())
      return;

    boolean changed = applyDeletes();

    // If autoCommit == true then all deletes should have
    // been flushed when we flushed the last segment
    assert !changed || !autoCommit;

    final SegmentInfos sourceSegments = merge.segments;
    final int end = sourceSegments.size();

    // Check whether this merge will allow us to skip
    // merging the doc stores (stored field & vectors).
    // This is a very substantial optimization (saves tons
    // of IO) that can only be applied with
    // autoCommit=false.

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
      //flush(false, true, false);
    }

    // We must take a full copy at this point so that we can
    // properly merge deletes in commitMerge()
    merge.segmentsClone = (SegmentInfos) merge.segments.clone();

    for (int i = 0; i < end; i++) {
      SegmentInfo si = merge.segmentsClone.info(i);

      // IncRef all files for this segment info to make sure
      // they are not removed while we are trying to merge.
      if (si.dir == directory)
        deleter.incRef(si.files());
    }

    merge.increfDone = true;

    merge.mergeDocStores = mergeDocStores;

    // Bind a new segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.
    merge.info = new SegmentInfo(newSegmentName(), 0,
                                 directory, false, true,
                                 docStoreOffset,
                                 docStoreSegment,
                                 docStoreIsCompoundFile,
                                 false);

    // Also enroll the merged segment into mergingSegments;
    // this prevents it from getting selected for a merge
    // after our merge is done but while we are building the
    // CFS:
    mergingSegments.add(merge.info);
  }

  /** This is called after merging a segment and before
   *  building its CFS.  Return true if the files should be
   *  sync'd.  If you return false, then the source segment
   *  files that were merged cannot be deleted until the CFS
   *  file is built & sync'd.  So, returning false consumes
   *  more transient disk space, but saves performance of
   *  not having to sync files which will shortly be deleted
   *  anyway.
   * @deprecated -- this will be removed in 3.0 when
   * autoCommit is hardwired to false */
  private synchronized boolean doCommitBeforeMergeCFS(MergePolicy.OneMerge merge) throws IOException {
    long freeableBytes = 0;
    final int size = merge.segments.size();
    for(int i=0;i<size;i++) {
      final SegmentInfo info = merge.segments.info(i);
      // It's only important to sync if the most recent
      // commit actually references this segment, because if
      // it doesn't, even without syncing we will free up
      // the disk space:
      Integer loc = (Integer) rollbackSegments.get(info);
      if (loc != null) {
        final SegmentInfo oldInfo = rollbackSegmentInfos.info(loc.intValue());
        if (oldInfo.getUseCompoundFile() != info.getUseCompoundFile())
          freeableBytes += info.sizeInBytes();
      }
    }
    // If we would free up more than 1/3rd of the index by
    // committing now, then do so:
    long totalBytes = 0;
    final int numSegments = segmentInfos.size();
    for(int i=0;i<numSegments;i++)
      totalBytes += segmentInfos.info(i).sizeInBytes();
    if (3*freeableBytes > totalBytes)
      return true;
    else
      return false;
  }

  /** Does fininishing for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance. */
  final synchronized void mergeFinish(MergePolicy.OneMerge merge) throws IOException {
    
    // Optimize, addIndexes or finishMerges may be waiting
    // on merges to finish.
    notifyAll();

    if (merge.increfDone)
      decrefMergeSegments(merge);

    assert merge.registerDone;

    final SegmentInfos sourceSegments = merge.segments;
    final int end = sourceSegments.size();
    for(int i=0;i<end;i++)
      mergingSegments.remove(sourceSegments.info(i));
    mergingSegments.remove(merge.info);
    merge.registerDone = false;
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
    SegmentInfos sourceSegmentsClone = merge.segmentsClone;
    final int numSegments = sourceSegments.size();

    if (infoStream != null)
      message("merging " + merge.segString(directory));

    merger = new SegmentMerger(this, mergedName, merge);
    
    // This is try/finally to make sure merger's readers are
    // closed:
    try {
      int totDocCount = 0;

      for (int i = 0; i < numSegments; i++) {
        SegmentInfo si = sourceSegmentsClone.info(i);
        IndexReader reader = SegmentReader.get(true, si, MERGE_READ_BUFFER_SIZE, merge.mergeDocStores); // no need to set deleter (yet)
        merger.add(reader);
        totDocCount += reader.numDocs();
      }
      if (infoStream != null) {
        message("merge: total "+totDocCount+" docs");
      }

      merge.checkAborted(directory);

      // This is where all the work happens:
      mergedDocCount = merge.info.docCount = merger.merge(merge.mergeDocStores);

      assert mergedDocCount == totDocCount;

    } finally {
      // close readers before we attempt to delete
      // now-obsolete segments
      if (merger != null) {
        merger.closeReaders();
      }
    }

    if (!commitMerge(merge, merger, mergedDocCount))
      // commitMerge will return false if this merge was aborted
      return 0;

    if (merge.useCompoundFile) {

      // Maybe force a sync here to allow reclaiming of the
      // disk space used by the segments we just merged:
      if (autoCommit && doCommitBeforeMergeCFS(merge)) {
        final long size;
        synchronized(this) {
          size = merge.info.sizeInBytes();
        }
        commit(size);
      }
      
      boolean success = false;
      final String compoundFileName = mergedName + "." + IndexFileNames.COMPOUND_FILE_EXTENSION;

      try {
        merger.createCompoundFile(compoundFileName);
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

    // Force a sync after commiting the merge.  Once this
    // sync completes then all index files referenced by the
    // current segmentInfos are on stable storage so if the
    // OS/machine crashes, or power cord is yanked, the
    // index will be intact.  Note that this is just one
    // (somewhat arbitrary) policy; we could try other
    // policies like only sync if it's been > X minutes or
    // more than Y bytes have been written, etc.
    if (autoCommit) {
      final long size;
      synchronized(this) {
        size = merge.info.sizeInBytes();
      }
      commit(size);
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
    SegmentInfos rollback = (SegmentInfos) segmentInfos.clone();
    boolean success = false;
    boolean changed;
    try {
      changed = docWriter.applyDeletes(segmentInfos);
      success = true;
    } finally {
      if (!success) {
        if (infoStream != null)
          message("hit exception flushing deletes");

        // Carefully remove any partially written .del
        // files
        final int size = rollback.size();
        for(int i=0;i<size;i++) {
          final String newDelFileName = segmentInfos.info(i).getDelFileName();
          final String delFileName = rollback.info(i).getDelFileName();
          if (newDelFileName != null && !newDelFileName.equals(delFileName))
            deleter.deleteFile(newDelFileName);
        }

        // Fully replace the segmentInfos since flushed
        // deletes could have changed any of the
        // SegmentInfo instances:
        segmentInfos.clear();
        segmentInfos.addAll(rollback);
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
    return segmentInfos.info(segmentInfos.size()-1);
  }

  public synchronized String segString() {
    return segString(segmentInfos);
  }

  private synchronized String segString(SegmentInfos infos) {
    StringBuffer buffer = new StringBuffer();
    final int count = infos.size();
    for(int i = 0; i < count; i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      final SegmentInfo info = infos.info(i);
      buffer.append(info.segString(directory));
      if (info.dir != directory)
        buffer.append("**");
    }
    return buffer.toString();
  }

  // Files that have been sync'd already
  private HashSet synced = new HashSet();

  // Files that are now being sync'd
  private HashSet syncing = new HashSet();

  private boolean startSync(String fileName, Collection pending) {
    synchronized(synced) {
      if (!synced.contains(fileName)) {
        if (!syncing.contains(fileName)) {
          syncing.add(fileName);
          return true;
        } else {
          pending.add(fileName);
          return false;
        }
      } else
        return false;
    }
  }

  private void finishSync(String fileName, boolean success) {
    synchronized(synced) {
      assert syncing.contains(fileName);
      syncing.remove(fileName);
      if (success)
        synced.add(fileName);
      synced.notifyAll();
    }
  }

  /** Blocks until all files in syncing are sync'd */
  private boolean waitForAllSynced(Collection syncing) throws IOException {
    synchronized(synced) {
      Iterator it = syncing.iterator();
      while(it.hasNext()) {
        final String fileName = (String) it.next();
        while(!synced.contains(fileName)) {
          if (!syncing.contains(fileName))
            // There was an error because a file that was
            // previously syncing failed to appear in synced
            return false;
          else
            try {
              synced.wait();
            } catch (InterruptedException ie) {
              continue;
            }
        }
      }
      return true;
    }
  }

  /** Pauses before syncing.  On Windows, at least, it's
   *  best (performance-wise) to pause in order to let OS
   *  flush writes to disk on its own, before forcing a
   *  sync.
   * @deprecated -- this will be removed in 3.0 when
   * autoCommit is hardwired to false */
  private void syncPause(long sizeInBytes) {
    if (mergeScheduler instanceof ConcurrentMergeScheduler && maxSyncPauseSeconds > 0) {
      // Rough heuristic: for every 10 MB, we pause for 1
      // second, up until the max
      long pauseTime = (long) (1000*sizeInBytes/10/1024/1024);
      final long maxPauseTime = (long) (maxSyncPauseSeconds*1000);
      if (pauseTime > maxPauseTime)
        pauseTime = maxPauseTime;
      final int sleepCount = (int) (pauseTime / 100);
      for(int i=0;i<sleepCount;i++) {
        synchronized(this) {
          if (stopMerges || closing)
            break;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private synchronized void doWait() {
    try {
      // NOTE: the callers of this method should in theory
      // be able to do simply wait(), but, as a defense
      // against thread timing hazards where notifyAll()
      // falls to be called, we wait for at most 1 second
      // and then return so caller can check if wait
      // conditions are satisified:
      wait(1000);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /** Walk through all files referenced by the current
   *  segmentInfos and ask the Directory to sync each file,
   *  if it wasn't already.  If that succeeds, then we
   *  prepare a new segments_N file but do not fully commit
   *  it. */
  private void startCommit(long sizeInBytes) throws IOException {

    assert testPoint("startStartCommit");

    if (hitOOM)
      return;

    try {

      if (infoStream != null)
        message("startCommit(): start sizeInBytes=" + sizeInBytes);

      if (sizeInBytes > 0)
        syncPause(sizeInBytes);

      SegmentInfos toSync = null;
      final long myChangeCount;

      synchronized(this) {

        // sizeInBytes > 0 means this is an autoCommit at
        // the end of a merge.  If at this point stopMerges
        // is true (which means a rollback() or
        // rollbackTransaction() is waiting for us to
        // finish), we skip the commit to avoid deadlock
        if (sizeInBytes > 0 && stopMerges)
          return;

        // Wait for any running addIndexes to complete
        // first, then block any from running until we've
        // copied the segmentInfos we intend to sync:
        blockAddIndexes(false);

        assert !hasExternalSegments();

        try {

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

          toSync = (SegmentInfos) segmentInfos.clone();
          deleter.incRef(toSync, false);
          myChangeCount = changeCount;
        } finally {
          resumeAddIndexes();
        }
      }

      assert testPoint("midStartCommit");

      boolean setPending = false;

      try {

        // Loop until all files toSync references are sync'd:
        while(true) {

          final Collection pending = new ArrayList();

          for(int i=0;i<toSync.size();i++) {
            final SegmentInfo info = toSync.info(i);
            final List files = info.files();
            for(int j=0;j<files.size();j++) {
              final String fileName = (String) files.get(j);
              if (startSync(fileName, pending)) {
                boolean success = false;
                try {
                  // Because we incRef'd this commit point, above,
                  // the file had better exist:
                  assert directory.fileExists(fileName): "file '" + fileName + "' does not exist dir=" + directory;
                  message("now sync " + fileName);
                  directory.sync(fileName);
                  success = true;
                } finally {
                  finishSync(fileName, success);
                }
              }
            }
          }

          // All files that I require are either synced or being
          // synced by other threads.  If they are being synced,
          // we must at this point block until they are done.
          // If this returns false, that means an error in
          // another thread resulted in failing to actually
          // sync one of our files, so we repeat:
          if (waitForAllSynced(pending))
            break;
        }

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

        message("done all syncs");

        assert testPoint("midStartCommitSuccess");

      } finally {
        synchronized(this) {
          if (!setPending)
            deleter.decRef(toSync);
        }
      }
    } catch (OutOfMemoryError oom) {
      hitOOM = true;
      throw oom;
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
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   * @param directory the directory to check for a lock
   * @throws IOException if there is a low-level IO error
   */
  public static boolean isLocked(String directory) throws IOException {
    Directory dir = FSDirectory.getDirectory(directory);
    try {
      return isLocked(dir);
    } finally {
      dir.close();
    }
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
   * Specifies maximum field length in {@link IndexWriter} constructors.
   * {@link #setMaxFieldLength(int)} overrides the value set by
   * the constructor.
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
}
