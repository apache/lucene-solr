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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BitVector;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map.Entry;

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
  href="#deleteDocuments(org.apache.lucene.index.Term)"><b>deleteDocuments</b></a>.
  A document can be updated with <a href="#updateDocument(org.apache.lucene.index.Term, org.apache.lucene.document.Document)"><b>updateDocument</b></a> 
  (which just deletes and then adds the entire document).
  When finished adding, deleting and updating documents, <a href="#close()"><b>close</b></a> should be called.</p>

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
  large RAM buffer.  You can also force a flush by calling
  {@link #flush}.  When a flush occurs, both pending deletes
  and added documents are flushed to the index.  A flush may
  also trigger one or more segment merges which by default
  run with a background thread so as not to block the
  addDocument calls (see <a href="#mergePolicy">below</a>
  for changing the {@link MergeScheduler}).</p>

  <a name="autoCommit"></a>
  <p>The optional <code>autoCommit</code> argument to the
  <a href="#IndexWriter(org.apache.lucene.store.Directory, boolean, org.apache.lucene.analysis.Analyzer)"><b>constructors</b></a>
  controls visibility of the changes to {@link IndexReader} instances reading the same index.
  When this is <code>false</code>, changes are not
  visible until {@link #close()} is called.
  Note that changes will still be flushed to the
  {@link org.apache.lucene.store.Directory} as new files,
  but are not committed (no new <code>segments_N</code> file
  is written referencing the new files) until {@link #close} is
  called.  If something goes terribly wrong (for example the
  JVM crashes) before {@link #close()}, then
  the index will reflect none of the changes made (it will
  remain in its starting state).
  You can also call {@link #abort()}, which closes the writer without committing any
  changes, and removes any index
  files that had been flushed but are now unreferenced.
  This mode is useful for preventing readers from refreshing
  at a bad time (for example after you've done all your
  deletes but before you've done your adds).
  It can also be used to implement simple single-writer
  transactional semantics ("all or none").</p>

  <p>When <code>autoCommit</code> is <code>true</code> then
  every flush is also a commit ({@link IndexReader}
  instances will see each flush as changes to the index).
  This is the default, to match the behavior before 2.2.
  When running in this mode, be careful not to refresh your
  readers while optimize or segment merges are taking place
  as this can tie up substantial disk space.</p>
  
  <p>Regardless of <code>autoCommit</code>, an {@link
  IndexReader} or {@link org.apache.lucene.search.IndexSearcher} will only see the
  index as of the "point in time" that it was opened.  Any
  changes committed to the index after the reader was opened
  are not visible until the reader is re-opened.</p>

  <p>If an index will not have more documents added for a while and optimal search
  performance is desired, then the <a href="#optimize()"><b>optimize</b></a>
  method should be called before the index is closed.</p>

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
 * IndexCommitPoint.
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

  private Directory directory;  // where this index resides
  private Analyzer analyzer;    // how to analyze text

  private Similarity similarity = Similarity.getDefault(); // how to normalize

  private boolean commitPending; // true if segmentInfos has changes not yet committed
  private SegmentInfos rollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails

  private SegmentInfos localRollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails
  private boolean localAutoCommit;                // saved autoCommit during local transaction
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

  /**
   * Used internally to throw an {@link
   * AlreadyClosedException} if this IndexWriter has been
   * closed.
   * @throws AlreadyClosedException if this IndexWriter is
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  private void message(String message) {
    infoStream.println("IW " + messageID + " [" + Thread.currentThread().getName() + "]: " + message);
  }

  private synchronized void setMessageID() {
    if (infoStream != null && messageID == -1) {
      synchronized(MESSAGE_ID_LOCK) {
        messageID = MESSAGE_ID++;
      }
    }
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
    ensureOpen();
    return termIndexInterval;
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
   */
  public IndexWriter(String path, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, create, true, null, true);
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
   */
  public IndexWriter(File path, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, create, true, null, true);
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
   */
  public IndexWriter(Directory d, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, null, true);
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
   */
  public IndexWriter(String path, Analyzer a) 
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, true, null, true);
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
   */
  public IndexWriter(File path, Analyzer a) 
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(FSDirectory.getDirectory(path), a, true, null, true);
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
   */
  public IndexWriter(Directory d, Analyzer a) 
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, null, true);
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
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a) 
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, null, autoCommit);
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
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a, boolean create)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, null, autoCommit);
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
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a, IndexDeletionPolicy deletionPolicy) 
    throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, false, deletionPolicy, autoCommit);
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
   */
  public IndexWriter(Directory d, boolean autoCommit, Analyzer a, boolean create, IndexDeletionPolicy deletionPolicy)
       throws CorruptIndexException, LockObtainFailedException, IOException {
    init(d, a, create, false, deletionPolicy, autoCommit);
  }

  private void init(Directory d, Analyzer a, boolean closeDir, IndexDeletionPolicy deletionPolicy, boolean autoCommit)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    if (IndexReader.indexExists(d)) {
      init(d, a, false, closeDir, deletionPolicy, autoCommit);
    } else {
      init(d, a, true, closeDir, deletionPolicy, autoCommit);
    }
  }

  private void init(Directory d, Analyzer a, final boolean create, boolean closeDir, IndexDeletionPolicy deletionPolicy, boolean autoCommit)
    throws CorruptIndexException, LockObtainFailedException, IOException {
    this.closeDir = closeDir;
    directory = d;
    analyzer = a;
    this.infoStream = defaultInfoStream;
    setMessageID();

    if (create) {
      // Clear the write lock in case it's leftover:
      directory.clearLock(IndexWriter.WRITE_LOCK_NAME);
    }

    Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME);
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
        segmentInfos.write(directory);
      } else {
        segmentInfos.read(directory);
      }

      this.autoCommit = autoCommit;
      if (!autoCommit) {
        rollbackSegmentInfos = (SegmentInfos) segmentInfos.clone();
      }

      docWriter = new DocumentsWriter(directory, this);
      docWriter.setInfoStream(infoStream);

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
  public void setMergeScheduler(MergeScheduler mergeScheduler) throws CorruptIndexException, IOException {
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

  /** Determines the largest number of documents ever merged by addDocument().
   * Small values (e.g., less than 10,000) are best for interactive indexing,
   * as this limits the length of pauses while indexing to a few seconds.
   * Larger values are best for batched indexing and speedier searches.
   *
   * <p>The default value is {@link Integer#MAX_VALUE}.
   *
   * <p>Note that this method is a convenience method: it
   * just calls mergePolicy.setMaxMergeDocs as long as
   * mergePolicy is an instance of {@link LogMergePolicy}.
   * Otherwise an IllegalArgumentException is thrown.</p>
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    getLogMergePolicy().setMaxMergeDocs(maxMergeDocs);
  }

   /**
   * Returns the largest number of documents allowed in a
   * single segment.
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
   * By default, no more than 10,000 terms will be indexed for a field.
   */
  public void setMaxFieldLength(int maxFieldLength) {
    ensureOpen();
    this.maxFieldLength = maxFieldLength;
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
    this.infoStream = infoStream;
    setMessageID();
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
            " maxBuffereDocs=" + docWriter.getMaxBufferedDocs() +
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
   * Flushes all changes to an index and closes all
   * associated files.
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
   *   if (IndexReader.isLocked(directory)) {
   *     IndexReader.unlock(directory);
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
   * until all merges complete; else, it will abort all
   * running merges and return right away
   */
  public void close(boolean waitForMerges) throws CorruptIndexException, IOException {
    boolean doClose;
    synchronized(this) {
      // Ensure that only one thread actually gets to do the closing:
      if (!closing) {
        doClose = true;
        closing = true;
      } else
        doClose = false;
    }
    if (doClose)
      closeInternal(waitForMerges);
    else
      // Another thread beat us to it (is actually doing the
      // close), so we will block until that other thread
      // has finished closing
      waitForClose();
  }

  synchronized private void waitForClose() {
    while(!closed && closing) {
      try {
        wait();
      } catch (InterruptedException ie) {
      }
    }
  }

  private void closeInternal(boolean waitForMerges) throws CorruptIndexException, IOException {
    try {
      if (infoStream != null)
        message("now flush at close");

      flush(true, true);

      mergePolicy.close();

      finishMerges(waitForMerges);

      mergeScheduler.close();

      if (commitPending) {
        boolean success = false;
        try {
          segmentInfos.write(directory);         // now commit changes
          success = true;
        } finally {
          if (!success) {
            if (infoStream != null)
              message("hit exception committing segments file during close");
            deletePartialSegmentsFile();
          }
        }
        if (infoStream != null)
          message("close: wrote segments file \"" + segmentInfos.getCurrentSegmentFileName() + "\"");
        synchronized(this) {
          deleter.checkpoint(segmentInfos, true);
        }
        commitPending = false;
        rollbackSegmentInfos = null;
      }

      if (infoStream != null)
        message("at close: " + segString());

      if (writeLock != null) {
        writeLock.release();                          // release write lock
        writeLock = null;
      }
      closed = true;
      docWriter = null;

      synchronized(this) {
        deleter.close();
      }
      
      if (closeDir)
        directory.close();
    } finally {
      synchronized(this) {
        if (!closed)
          closing = false;
        notifyAll();
      }
    }
  }

  /** Tells the docWriter to close its currently open shared
   *  doc stores (stored fields & vectors files).
   *  Return value specifices whether new doc store files are compound or not.
   */
  private synchronized boolean flushDocStores() throws IOException {

    List files = docWriter.files();

    boolean useCompoundDocStore = false;

    if (files.size() > 0) {
      String docStoreSegment;

      boolean success = false;
      try {
        docStoreSegment = docWriter.closeDocStore();
        success = true;
      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception closing doc store segment");
          docWriter.abort();
        }
      }

      useCompoundDocStore = mergePolicy.useCompoundDocStore(segmentInfos);
      
      if (useCompoundDocStore && docStoreSegment != null) {
        // Now build compound doc store file

        success = false;

        final int numSegments = segmentInfos.size();
        final String compoundFileName = docStoreSegment + "." + IndexFileNames.COMPOUND_FILE_STORE_EXTENSION;

        try {
          CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, compoundFileName);
          final int size = files.size();
          for(int i=0;i<size;i++)
            cfsWriter.addFile((String) files.get(i));
      
          // Perform the merge
          cfsWriter.close();

          for(int i=0;i<numSegments;i++) {
            SegmentInfo si = segmentInfos.info(i);
            if (si.getDocStoreOffset() != -1 &&
                si.getDocStoreSegment().equals(docStoreSegment))
              si.setDocStoreIsCompoundFile(true);
          }
          checkpoint();
          success = true;
        } finally {
          if (!success) {

            if (infoStream != null)
              message("hit exception building compound file doc store for segment " + docStoreSegment);
            
            // Rollback to no compound file
            for(int i=0;i<numSegments;i++) {
              SegmentInfo si = segmentInfos.info(i);
              if (si.getDocStoreOffset() != -1 &&
                  si.getDocStoreSegment().equals(docStoreSegment))
                si.setDocStoreIsCompoundFile(false);
            }
            deleter.deleteFile(compoundFileName);
            deletePartialSegmentsFile();
          }
        }

        deleter.checkpoint(segmentInfos, false);
      }
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
    ensureOpen();
    return directory;
  }

  /** Returns the analyzer used by this index. */
  public Analyzer getAnalyzer() {
    ensureOpen();
    return analyzer;
  }

  /** Returns the number of documents currently in this index. */
  public synchronized int docCount() {
    ensureOpen();
    int count = docWriter.getNumDocsInRAM();
    for (int i = 0; i < segmentInfos.size(); i++) {
      SegmentInfo si = segmentInfos.info(i);
      count += si.docCount;
    }
    return count;
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
   */
  private int maxFieldLength = DEFAULT_MAX_FIELD_LENGTH;

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
   * to the Directory (every {@link #setMaxBufferedDocs}),
   * and also periodically merges segments in the index
   * (every {@link #setMergeFactor} flushes).  When this
   * occurs, the method will take more time to run (possibly
   * a long time if the index is large), and will require
   * free temporary space in the Directory to do the
   * merging.</p>
   *
   * <p>The amount of free space required when a merge is triggered is
   * up to 1X the size of all segments being merged, when no
   * readers/searchers are open against the index, and up to 2X the
   * size of all segments being merged when readers/searchers are open
   * against the index (see {@link #optimize()} for details). The
   * sequence of primitive merge operations performed is governed by
   * the merge policy.
   *
   * <p>Note that each term in the document can be no longer
   * than 16383 characters, otherwise an
   * IllegalArgumentException will be thrown.</p>
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
      doFlush = docWriter.addDocument(doc, analyzer);
      success = true;
    } finally {
      if (!success) {

        if (infoStream != null)
          message("hit exception adding document");

        synchronized (this) {
          // If docWriter has some aborted files that were
          // never incref'd, then we clean them up here
          final List files = docWriter.abortedFiles();
          if (files != null)
            deleter.deleteNewFiles(files);
        }
      }
    }
    if (doFlush)
      flush(true, false);
  }

  /**
   * Deletes the document(s) containing <code>term</code>.
   * @param term the term to identify the documents to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
    ensureOpen();
    boolean doFlush = docWriter.bufferDeleteTerm(term);
    if (doFlush)
      flush(true, false);
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
    boolean doFlush = docWriter.bufferDeleteTerms(terms);
    if (doFlush)
      flush(true, false);
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
          final List files = docWriter.abortedFiles();
          if (files != null)
            deleter.deleteNewFiles(files);
        }
      }
    }
    if (doFlush)
      flush(true, false);
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

  final String newSegmentName() {
    // Cannot synchronize on IndexWriter because that causes
    // deadlock
    synchronized(segmentInfos) {
      // Important to set commitPending so that the
      // segmentInfos is written on close.  Otherwise we
      // could close, re-open and re-return the same segment
      // name that was previously returned which can cause
      // problems at least with ConcurrentMergeScheduler.
      commitPending = true;
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

  /** Just like {@link #optimize()}, except you can specify
   *  whether the call should block until the optimize
   *  completes.  This is only meaningful with a
   *  {@link MergeScheduler} that is able to run merges in
   *  background threads. */
  public void optimize(boolean doWait) throws CorruptIndexException, IOException {
    ensureOpen();

    if (infoStream != null)
      message("optimize: index now " + segString());

    flush();

    synchronized(this) {
      resetMergeExceptions();
      segmentsToOptimize = new HashSet();
      final int numSegments = segmentInfos.size();
      for(int i=0;i<numSegments;i++)
        segmentsToOptimize.add(segmentInfos.info(i));
      
      // Now mark all pending & running merges as optimize
      // merge:
      Iterator it = pendingMerges.iterator();
      while(it.hasNext())
        ((MergePolicy.OneMerge) it.next()).optimize = true;

      it = runningMerges.iterator();
      while(it.hasNext())
        ((MergePolicy.OneMerge) it.next()).optimize = true;
    }

    maybeMerge(true);

    if (doWait) {
      synchronized(this) {
        while(optimizeMergesPending()) {
          try {
            wait();
          } catch (InterruptedException ie) {
          }

          if (mergeExceptions.size() > 0) {
            // Forward any exceptions in background merge
            // threads to the current thread:
            final int size = mergeExceptions.size();
            for(int i=0;i<size;i++) {
              final MergePolicy.OneMerge merge = (MergePolicy.OneMerge) mergeExceptions.get(0);
              if (merge.optimize) {
                IOException err = new IOException("background merge hit exception: " + merge.segString(directory));
                err.initCause(merge.getException());
                throw err;
              }
            }
          }
        }
      }
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
    updatePendingMerges(optimize);
    mergeScheduler.merge(this);
  }

  private synchronized void updatePendingMerges(boolean optimize)
    throws CorruptIndexException, IOException {

    final MergePolicy.MergeSpecification spec;
    if (optimize) {
      // Currently hardwired to 1, but once we add method to
      // IndexWriter to allow "optimizing to <= N segments"
      // then we will change this.
      final int maxSegmentCount = 1;
      spec = mergePolicy.findMergesForOptimize(segmentInfos, this, maxSegmentCount, segmentsToOptimize);

      if (spec != null) {
        final int numMerges = spec.merges.size();
        for(int i=0;i<numMerges;i++)
          ((MergePolicy.OneMerge) spec.merges.get(i)).optimize = true;
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
  private void startTransaction() throws IOException {

    if (infoStream != null)
      message("now start transaction");

    assert docWriter.getNumBufferedDeleteTerms() == 0 :
           "calling startTransaction with buffered delete terms not supported";
    assert docWriter.getNumDocsInRAM() == 0 :
           "calling startTransaction with buffered documents not supported";

    localRollbackSegmentInfos = (SegmentInfos) segmentInfos.clone();
    localAutoCommit = autoCommit;
    if (localAutoCommit) {

      if (infoStream != null)
        message("flush at startTransaction");

      flush();
      // Turn off auto-commit during our local transaction:
      autoCommit = false;
    } else
      // We must "protect" our files at this point from
      // deletion in case we need to rollback:
      deleter.incRef(segmentInfos, false);
  }

  /*
   * Rolls back the transaction and restores state to where
   * we were at the start.
   */
  private void rollbackTransaction() throws IOException {

    if (infoStream != null)
      message("now rollback transaction");

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;

    // Keep the same segmentInfos instance but replace all
    // of its SegmentInfo instances.  This is so the next
    // attempt to commit using this instance of IndexWriter
    // will always write to a new generation ("write once").
    segmentInfos.clear();
    segmentInfos.addAll(localRollbackSegmentInfos);
    localRollbackSegmentInfos = null;

    // Ask deleter to locate unreferenced files we had
    // created & remove them:
    deleter.checkpoint(segmentInfos, false);

    if (!autoCommit)
      // Remove the incRef we did in startTransaction:
      deleter.decRef(segmentInfos);

    deleter.refresh();
    finishMerges(false);
  }

  /*
   * Commits the transaction.  This will write the new
   * segments file and remove and pending deletions we have
   * accumulated during the transaction
   */
  private void commitTransaction() throws IOException {

    if (infoStream != null)
      message("now commit transaction");

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;

    boolean success = false;
    try {
      checkpoint();
      success = true;
    } finally {
      if (!success) {
        if (infoStream != null)
          message("hit exception committing transaction");

        rollbackTransaction();
      }
    }

    if (!autoCommit)
      // Remove the incRef we did in startTransaction.
      deleter.decRef(localRollbackSegmentInfos);

    localRollbackSegmentInfos = null;

    // Give deleter a chance to remove files now:
    deleter.checkpoint(segmentInfos, autoCommit);
  }

  /**
   * Close the <code>IndexWriter</code> without committing
   * any of the changes that have occurred since it was
   * opened. This removes any temporary files that had been
   * created, after which the state of the index will be the
   * same as it was when this writer was first opened.  This
   * can only be called when this IndexWriter was opened
   * with <code>autoCommit=false</code>.
   * @throws IllegalStateException if this is called when
   *  the writer was opened with <code>autoCommit=true</code>.
   * @throws IOException if there is a low-level IO error
   */
  public void abort() throws IOException {
    ensureOpen();
    if (autoCommit)
      throw new IllegalStateException("abort() can only be called when IndexWriter was opened with autoCommit=false");

    boolean doClose;
    synchronized(this) {
      // Ensure that only one thread actually gets to do the closing:
      if (!closing) {
        doClose = true;
        closing = true;
      } else
        doClose = false;
    }

    if (doClose) {

      finishMerges(false);

      // Must pre-close these two, in case they set
      // commitPending=true, so that we can then set it to
      // false before calling closeInternal
      mergePolicy.close();
      mergeScheduler.close();

      synchronized(this) {
        // Keep the same segmentInfos instance but replace all
        // of its SegmentInfo instances.  This is so the next
        // attempt to commit using this instance of IndexWriter
        // will always write to a new generation ("write
        // once").
        segmentInfos.clear();
        segmentInfos.addAll(rollbackSegmentInfos);

        docWriter.abort();

        // Ask deleter to locate unreferenced files & remove
        // them:
        deleter.checkpoint(segmentInfos, false);
        deleter.refresh();
        finishMerges(false);
      }

      commitPending = false;
      closeInternal(false);
    } else
      waitForClose();
  }

  private synchronized void finishMerges(boolean waitForMerges) {
    if (!waitForMerges) {
      // Abort all pending & running merges:
      Iterator it = pendingMerges.iterator();
      while(it.hasNext())
        ((MergePolicy.OneMerge) it.next()).abort();

      pendingMerges.clear();
      it = runningMerges.iterator();
      while(it.hasNext())
        ((MergePolicy.OneMerge) it.next()).abort();

      runningMerges.clear();
      mergingSegments.clear();
      notifyAll();
    } else {
      while(pendingMerges.size() > 0 || runningMerges.size() > 0) {
        try {
          wait();
        } catch (InterruptedException ie) {
        }
      }
      assert 0 == mergingSegments.size();
    }
  }
 
  /*
   * Called whenever the SegmentInfos has been updated and
   * the index files referenced exist (correctly) in the
   * index directory.  If we are in autoCommit mode, we
   * commit the change immediately.  Else, we mark
   * commitPending.
   */
  private synchronized void checkpoint() throws IOException {
    if (autoCommit) {
      segmentInfos.write(directory);
      commitPending = false;
      if (infoStream != null)
        message("checkpoint: wrote segments file \"" + segmentInfos.getCurrentSegmentFileName() + "\"");
    } else {
      commitPending = true;
    }
  }

  /** Merges all segments from an array of indexes into this index.
   *
   * <p>This may be used to parallelize batch indexing.  A large document
   * collection can be broken into sub-collections.  Each sub-collection can be
   * indexed in parallel, on a different thread, process or machine.  The
   * complete index can then be created by merging sub-collection indexes
   * with this method.
   *
   * <p>After this completes, the index is optimized.
   *
   * <p>This method is transactional in how Exceptions are
   * handled: it does not commit a new segments_N file until
   * all indexes are added.  This means if an Exception
   * occurs (for example disk full), then either no indexes
   * will have been added or they all will have been.</p>
   *
   * <p>If an Exception is hit, it's still possible that all
   * indexes were successfully added.  This happens when the
   * Exception is hit when trying to build a CFS file.  In
   * this case, one segment in the index will be in non-CFS
   * format, even when using compound file format.</p>
   *
   * <p>Also note that on an Exception, the index may still
   * have been partially or fully optimized even though none
   * of the input indexes were added. </p>
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
   * <p>See <a target="_top"
   * href="http://issues.apache.org/jira/browse/LUCENE-702">LUCENE-702</a>
   * for details.</p>
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void addIndexes(Directory[] dirs)
    throws CorruptIndexException, IOException {

    ensureOpen();
    if (infoStream != null)
      message("flush at addIndexes");
    flush();

    int start = segmentInfos.size();

    boolean success = false;

    startTransaction();

    try {
      for (int i = 0; i < dirs.length; i++) {
        SegmentInfos sis = new SegmentInfos();	  // read infos from dir
        sis.read(dirs[i]);
        for (int j = 0; j < sis.size(); j++) {
          segmentInfos.addElement(sis.info(j));	  // add each info
        }
      }

      optimize();

      success = true;
    } finally {
      if (success) {
        commitTransaction();
      } else {
        rollbackTransaction();
      }
    }
  }

  private synchronized void resetMergeExceptions() {
    mergeExceptions = new ArrayList();
    mergeGen++;
  }

  /**
   * Merges all segments from an array of indexes into this index.
   * <p>
   * This is similar to addIndexes(Directory[]). However, no optimize()
   * is called either at the beginning or at the end. Instead, merges
   * are carried out as necessary.
   * <p>
   * This requires this index not be among those to be added, and the
   * upper bound* of those segment doc counts not exceed maxMergeDocs.
   *
   * <p>See {@link #addIndexes(Directory[])} for
   * details on transactional semantics, temporary free
   * space required in the Directory, and non-CFS segments
   * on an Exception.</p>
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void addIndexesNoOptimize(Directory[] dirs)
      throws CorruptIndexException, IOException {

    ensureOpen();
    if (infoStream != null)
      message("flush at addIndexesNoOptimize");
    flush();

    /* new merge policy
    if (startUpperBound == 0)
      startUpperBound = 10;
    */

    boolean success = false;

    startTransaction();

    try {

      for (int i = 0; i < dirs.length; i++) {
        if (directory == dirs[i]) {
          // cannot add this index: segments may be deleted in merge before added
          throw new IllegalArgumentException("Cannot add this index to itself");
        }

        SegmentInfos sis = new SegmentInfos(); // read infos from dir
        sis.read(dirs[i]);
        for (int j = 0; j < sis.size(); j++) {
          SegmentInfo info = sis.info(j);
          segmentInfos.addElement(info); // add each info
        }
      }

      maybeMerge();

      // If after merging there remain segments in the index
      // that are in a different directory, just copy these
      // over into our index.  This is necessary (before
      // finishing the transaction) to avoid leaving the
      // index in an unusable (inconsistent) state.
      copyExternalSegments();

      success = true;

    } finally {
      if (success) {
        commitTransaction();
      } else {
        rollbackTransaction();
      }
    }
  }

  /* If any of our segments are using a directory != ours
   * then copy them over.  Currently this is only used by
   * addIndexesNoOptimize(). */
  private synchronized void copyExternalSegments() throws CorruptIndexException, IOException {
    final int numSegments = segmentInfos.size();
    for(int i=0;i<numSegments;i++) {
      SegmentInfo info = segmentInfos.info(i);
      if (info.dir != directory) {
        MergePolicy.OneMerge merge = new MergePolicy.OneMerge(segmentInfos.range(i, 1+i), info.getUseCompoundFile());
        if (registerMerge(merge)) {
          pendingMerges.remove(merge);
          runningMerges.add(merge);
          merge(merge);
        } else
          // This means there is a bug in the
          // MergeScheduler.  MergeSchedulers in general are
          // not allowed to run a merge involving segments
          // external to this IndexWriter's directory in the
          // background because this would put the index
          // into an inconsistent state (where segmentInfos
          // has been written with such external segments
          // that an IndexReader would fail to load).
          throw new MergePolicy.MergeException("segment \"" + info.name + " exists in external directory yet the MergeScheduler executed the merge in a separate thread");
      }
    }
  }

  /** Merges the provided indexes into this index.
   * <p>After this completes, the index is optimized. </p>
   * <p>The provided IndexReaders are not closed.</p>

   * <p>See {@link #addIndexes(Directory[])} for
   * details on transactional semantics, temporary free
   * space required in the Directory, and non-CFS segments
   * on an Exception.</p>
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void addIndexes(IndexReader[] readers)
    throws CorruptIndexException, IOException {

    ensureOpen();
    optimize();					  // start with zero or 1 seg

    final String mergedName = newSegmentName();
    SegmentMerger merger = new SegmentMerger(this, mergedName);

    SegmentInfo info;

    IndexReader sReader = null;
    try {
      if (segmentInfos.size() == 1){ // add existing index, if any
        sReader = SegmentReader.get(segmentInfos.info(0));
        merger.add(sReader);
      }

      for (int i = 0; i < readers.length; i++)      // add new indexes
        merger.add(readers[i]);

      boolean success = false;

      startTransaction();

      try {
        int docCount = merger.merge();                // merge 'em

        if(sReader != null) {
          sReader.close();
          sReader = null;
        }

        segmentInfos.setSize(0);                      // pop old infos & add new
        info = new SegmentInfo(mergedName, docCount, directory, false, true,
                               -1, null, false);
        segmentInfos.addElement(info);

        success = true;

      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception in addIndexes during merge");

          rollbackTransaction();
        } else {
          commitTransaction();
        }
      }
    } finally {
      if (sReader != null) {
        sReader.close();
      }
    }
    
    if (mergePolicy instanceof LogMergePolicy && getUseCompoundFile()) {

      boolean success = false;

      startTransaction();

      try {
        merger.createCompoundFile(mergedName + ".cfs");
        info.setUseCompoundFile(true);
      } finally {
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

  // This is called after pending added and deleted
  // documents have been flushed to the Directory but before
  // the change is committed (new segments_N file written).
  void doAfterFlush()
    throws IOException {
  }

  /**
   * Flush all in-memory buffered updates (adds and deletes)
   * to the Directory. 
   * <p>Note: if <code>autoCommit=false</code>, flushed data would still 
   * not be visible to readers, until {@link #close} is called.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public final void flush() throws CorruptIndexException, IOException {  
    flush(true, false);
  }

  /**
   * Flush all in-memory buffered udpates (adds and deletes)
   * to the Directory.
   * @param triggerMerge if true, we may merge segments (if
   *  deletes or docs were flushed) if necessary
   * @param flushDocStores if false we are allowed to keep
   *  doc stores open to share with the next segment
   */
  protected final void flush(boolean triggerMerge, boolean flushDocStores) throws CorruptIndexException, IOException {
    ensureOpen();

    if (doFlush(flushDocStores) && triggerMerge)
      maybeMerge();
  }

  private synchronized final boolean doFlush(boolean flushDocStores) throws CorruptIndexException, IOException {

    // Make sure no threads are actively adding a document
    docWriter.pauseAllThreads();

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

      // Always flush deletes if there are any delete terms.
      // TODO: when autoCommit=false we don't have to flush
      // deletes with every flushed segment; we can save
      // CPU/IO by buffering longer & flushing deletes only
      // when they are full or writer is being closed.  We
      // have to fix the "applyDeletesSelectively" logic to
      // apply to more than just the last flushed segment
      boolean flushDeletes = docWriter.hasDeletes();

      if (infoStream != null) {
        message("  flush: segment=" + docWriter.getSegment() +
                " docStoreSegment=" + docWriter.getDocStoreSegment() +
                " docStoreOffset=" + docWriter.getDocStoreOffset() +
                " flushDocs=" + flushDocs +
                " flushDeletes=" + flushDeletes +
                " flushDocStores=" + flushDocStores +
                " numDocs=" + numDocs +
                " numBufDelTerms=" + docWriter.getNumBufferedDeleteTerms());
        message("  index before flush " + segString());
      }

      int docStoreOffset = docWriter.getDocStoreOffset();
      boolean docStoreIsCompoundFile = false;

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

      if (flushDocs || flushDeletes) {

        SegmentInfos rollback = null;

        if (flushDeletes)
          rollback = (SegmentInfos) segmentInfos.clone();

        boolean success = false;

        try {
          if (flushDocs) {

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

            int flushedDocCount = docWriter.flush(flushDocStores);
          
            newSegment = new SegmentInfo(segment,
                                         flushedDocCount,
                                         directory, false, true,
                                         docStoreOffset, docStoreSegment,
                                         docStoreIsCompoundFile);
            segmentInfos.addElement(newSegment);
          }

          if (flushDeletes) {
            // we should be able to change this so we can
            // buffer deletes longer and then flush them to
            // multiple flushed segments, when
            // autoCommit=false
            int delCount = applyDeletes(flushDocs);
            if (infoStream != null)
              infoStream.println("flushed " + delCount + " deleted documents");
            doAfterFlush();
          }

          checkpoint();
          success = true;
        } finally {
          if (!success) {

            if (infoStream != null)
              message("hit exception flushing segment " + segment);
                
            if (flushDeletes) {

              // Carefully check if any partial .del files
              // should be removed:
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
              
            } else {
              // Remove segment we added, if any:
              if (newSegment != null && 
                  segmentInfos.size() > 0 && 
                  segmentInfos.info(segmentInfos.size()-1) == newSegment)
                segmentInfos.remove(segmentInfos.size()-1);
            }
            if (flushDocs)
              docWriter.abort();
            deletePartialSegmentsFile();
            deleter.checkpoint(segmentInfos, false);

            if (segment != null)
              deleter.refresh(segment);
          }
        }

        deleter.checkpoint(segmentInfos, autoCommit);

        if (flushDocs && mergePolicy.useCompoundFile(segmentInfos,
                                                     newSegment)) {
          success = false;
          try {
            docWriter.createCompoundFile(segment);
            newSegment.setUseCompoundFile(true);
            checkpoint();
            success = true;
          } finally {
            if (!success) {
              if (infoStream != null)
                message("hit exception creating compound file for newly flushed segment " + segment);
              newSegment.setUseCompoundFile(false);
              deleter.deleteFile(segment + "." + IndexFileNames.COMPOUND_FILE_EXTENSION);
              deletePartialSegmentsFile();
            }
          }

          deleter.checkpoint(segmentInfos, autoCommit);
        }
      
        return true;
      } else {
        return false;
      }

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

  /** Expert:  Return the number of documents whose segments are currently cached in memory.
   * Useful when calling flush()
   */
  public final synchronized int numRamDocs() {
    ensureOpen();
    return docWriter.getNumDocsInRAM();
  }

  private int ensureContiguousMerge(MergePolicy.OneMerge merge) {

    int first = segmentInfos.indexOf(merge.segments.info(0));
    if (first == -1)
      throw new MergePolicy.MergeException("could not find segment " + merge.segments.info(0).name + " in current segments");

    final int numSegments = segmentInfos.size();
    
    final int numSegmentsToMerge = merge.segments.size();
    for(int i=0;i<numSegmentsToMerge;i++) {
      final SegmentInfo info = merge.segments.info(i);

      if (first + i >= numSegments || !segmentInfos.info(first+i).equals(info)) {
        if (segmentInfos.indexOf(info) == -1)
          throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.name + ") that is not in the index");
        else
          throw new MergePolicy.MergeException("MergePolicy selected non-contiguous segments to merge (" + merge + " vs " + segString() + "), which IndexWriter (currently) cannot handle");
      }
    }

    return first;
  }

  /* FIXME if we want to support non-contiguous segment merges */
  synchronized private boolean commitMerge(MergePolicy.OneMerge merge) throws IOException {

    assert merge.registerDone;

    // If merge was explicitly aborted, or, if abort() or
    // rollbackTransaction() had been called since our merge
    // started (which results in an unqualified
    // deleter.refresh() call that will remove any index
    // file that current segments does not reference), we
    // abort this merge
    if (merge.isAborted()) {

      if (infoStream != null) {
        if (merge.isAborted())
          message("commitMerge: skipping merge " + merge.segString(directory) + ": it was aborted");
      }

      assert merge.increfDone;
      decrefMergeSegments(merge);
      deleter.refresh(merge.info.name);
      return false;
    }

    boolean success = false;

    int start;

    try {
      SegmentInfos sourceSegmentsClone = merge.segmentsClone;
      SegmentInfos sourceSegments = merge.segments;
      final int numSegments = segmentInfos.size();

      start = ensureContiguousMerge(merge);
      if (infoStream != null)
        message("commitMerge " + merge.segString(directory));

      // Carefully merge deletes that occurred after we
      // started merging:

      BitVector deletes = null;
      int docUpto = 0;

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
                if (currentDeletes.get(j))
                  deletes.set(docUpto);
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
            if (currentDeletes.get(j))
              deletes.set(docUpto);
            docUpto++;
          }
            
        } else
          // No deletes before or after
          docUpto += currentInfo.docCount;
      }

      if (deletes != null) {
        merge.info.advanceDelGen();
        deletes.write(directory, merge.info.getDelFileName());
      }
      success = true;
    } finally {
      if (!success) {
        if (infoStream != null)
          message("hit exception creating merged deletes file");
        deleter.refresh(merge.info.name);
      }
    }

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

    success = false;
    SegmentInfos rollback = null;
    try {
      rollback = (SegmentInfos) segmentInfos.clone();
      segmentInfos.subList(start, start + merge.segments.size()).clear();
      segmentInfos.add(start, merge.info);
      checkpoint();
      success = true;
    } finally {
      if (!success && rollback != null) {
        if (infoStream != null)
          message("hit exception when checkpointing after merge");
        segmentInfos.clear();
        segmentInfos.addAll(rollback);
        deletePartialSegmentsFile();
        deleter.refresh(merge.info.name);
      }
    }

    if (merge.optimize)
      segmentsToOptimize.add(merge.info);

    // Must checkpoint before decrefing so any newly
    // referenced files in the new merge.info are incref'd
    // first:
    deleter.checkpoint(segmentInfos, autoCommit);

    decrefMergeSegments(merge);

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

  /**
   * Merges the indicated segments, replacing them in the stack with a
   * single segment.
   */

  final void merge(MergePolicy.OneMerge merge)
    throws CorruptIndexException, IOException {

    assert merge.registerDone;

    int mergedDocCount;
    boolean success = false;

    try {

      if (merge.info == null)
        mergeInit(merge);

      if (infoStream != null)
        message("now merge\n  merge=" + merge.segString(directory) + "\n  index=" + segString());

      mergedDocCount = mergeMiddle(merge);

      success = true;
    } finally {
      synchronized(this) {
        if (!success && infoStream != null)
          message("hit exception during merge");

        mergeFinish(merge);

        // This merge (and, generally, any change to the
        // segments) may now enable new merges, so we call
        // merge policy & update pending merges.
        if (success && !merge.isAborted() && !closed && !closing)
          updatePendingMerges(merge.optimize);

        runningMerges.remove(merge);

        // Optimize may be waiting on the final optimize
        // merge to finish; and finishMerges() may be
        // waiting for all merges to finish:
        notifyAll();
      }
    }
  }

  /** Checks whether this merge involves any segments
   *  already participating in a merge.  If not, this merge
   *  is "registered", meaning we record that its segments
   *  are now participating in a merge, and true is
   *  returned.  Else (the merge conflicts) false is
   *  returned. */
  final synchronized boolean registerMerge(MergePolicy.OneMerge merge) {

    if (merge.registerDone)
      return true;

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
   *  the synchronized lock on IndexWriter instance. */
  final synchronized void mergeInit(MergePolicy.OneMerge merge) throws IOException {

    // Bind a new segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.

    assert merge.registerDone;

    final SegmentInfos sourceSegments = merge.segments;
    final int end = sourceSegments.size();
    final int numSegments = segmentInfos.size();

    final int start = ensureContiguousMerge(merge);

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
      if (si.getDocStoreOffset() != -1 && currentDocStoreSegment != null && si.getDocStoreSegment().equals(currentDocStoreSegment))
        doFlushDocStore = true;
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
        message("flush at merge");
      flush(false, true);
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
    merge.info = new SegmentInfo(newSegmentName(), 0,
                                 directory, false, true,
                                 docStoreOffset,
                                 docStoreSegment,
                                 docStoreIsCompoundFile);
  }

  /** Does fininishing for a merge, which is fast but holds
   *  the synchronized lock on IndexWriter instance. */
  final synchronized void mergeFinish(MergePolicy.OneMerge merge) throws IOException {

    if (merge.increfDone)
      decrefMergeSegments(merge);

    assert merge.registerDone;

    final SegmentInfos sourceSegments = merge.segments;
    final SegmentInfos sourceSegmentsClone = merge.segmentsClone;
    final int end = sourceSegments.size();
    for(int i=0;i<end;i++)
      mergingSegments.remove(sourceSegments.info(i));
    merge.registerDone = false;
  }

  /** Does the actual (time-consuming) work of the merge,
   *  but without holding synchronized lock on IndexWriter
   *  instance */
  final private int mergeMiddle(MergePolicy.OneMerge merge) 
    throws CorruptIndexException, IOException {

    final String mergedName = merge.info.name;
    
    SegmentMerger merger = null;

    int mergedDocCount = 0;

    SegmentInfos sourceSegments = merge.segments;
    SegmentInfos sourceSegmentsClone = merge.segmentsClone;
    final int numSegments = sourceSegments.size();

    if (infoStream != null)
      message("merging " + merge.segString(directory));

    merger = new SegmentMerger(this, mergedName);

    // This is try/finally to make sure merger's readers are
    // closed:

    boolean success = false;

    try {
      int totDocCount = 0;
      for (int i = 0; i < numSegments; i++) {
        SegmentInfo si = sourceSegmentsClone.info(i);
        IndexReader reader = SegmentReader.get(si, MERGE_READ_BUFFER_SIZE, merge.mergeDocStores); // no need to set deleter (yet)
        merger.add(reader);
        if (infoStream != null)
          totDocCount += reader.numDocs();
      }
      if (infoStream != null) {
        message("merge: total "+totDocCount+" docs");
      }

      mergedDocCount = merge.info.docCount = merger.merge(merge.mergeDocStores);

      if (infoStream != null)
        assert mergedDocCount == totDocCount;

      success = true;

    } finally {
      // close readers before we attempt to delete
      // now-obsolete segments
      if (merger != null) {
        merger.closeReaders();
      }
      if (!success) {
        if (infoStream != null)
          message("hit exception during merge; now refresh deleter on segment " + mergedName);
        synchronized(this) {
          addMergeException(merge);
          deleter.refresh(mergedName);
        }
      }
    }

    if (!commitMerge(merge))
      // commitMerge will return false if this merge was aborted
      return 0;

    if (merge.useCompoundFile) {
      
      success = false;
      boolean skip = false;
      final String compoundFileName = mergedName + "." + IndexFileNames.COMPOUND_FILE_EXTENSION;

      try {
        try {
          merger.createCompoundFile(compoundFileName);
          success = true;
        } catch (IOException ioe) {
          synchronized(this) {
            if (segmentInfos.indexOf(merge.info) == -1) {
              // If another merge kicked in and merged our
              // new segment away while we were trying to
              // build the compound file, we can hit a
              // FileNotFoundException and possibly
              // IOException over NFS.  We can tell this has
              // happened because our SegmentInfo is no
              // longer in the segments; if this has
              // happened it is safe to ignore the exception
              // & skip finishing/committing our compound
              // file creating.
              if (infoStream != null)
                message("hit exception creating compound file; ignoring it because our info (segment " + merge.info.name + ") has been merged away");
              skip = true;
            } else
              throw ioe;
          }
        }
      } finally {
        if (!success) {
          if (infoStream != null)
            message("hit exception creating compound file during merge: skip=" + skip);

          synchronized(this) {
            if (!skip)
              addMergeException(merge);
            deleter.deleteFile(compoundFileName);
          }
        }
      }

      if (!skip) {

        synchronized(this) {
          if (skip || segmentInfos.indexOf(merge.info) == -1 || merge.isAborted()) {
            // Our segment (committed in non-compound
            // format) got merged away while we were
            // building the compound format.
            deleter.deleteFile(compoundFileName);
          } else {
            success = false;
            try {
              merge.info.setUseCompoundFile(true);
              checkpoint();
              success = true;
            } finally {
              if (!success) {  
                if (infoStream != null)
                  message("hit exception checkpointing compound file during merge");

                // Must rollback:
                addMergeException(merge);
                merge.info.setUseCompoundFile(false);
                deletePartialSegmentsFile();
                deleter.deleteFile(compoundFileName);
              }
            }
      
            // Give deleter a chance to remove files now.
            deleter.checkpoint(segmentInfos, autoCommit);
          }
        }
      }
    }

    return mergedDocCount;
  }

  void addMergeException(MergePolicy.OneMerge merge) {
    if (!mergeExceptions.contains(merge) && mergeGen == merge.mergeGen)
      mergeExceptions.add(merge);
  }

  private void deletePartialSegmentsFile() throws IOException  {
    if (segmentInfos.getLastGeneration() != segmentInfos.getGeneration()) {
      String segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                     "",
                                                                     segmentInfos.getGeneration());
      if (infoStream != null)
        message("now delete partial segments file \"" + segmentFileName + "\"");

      deleter.deleteFile(segmentFileName);
    }
  }

  // Called during flush to apply any buffered deletes.  If
  // flushedNewSegment is true then a new segment was just
  // created and flushed from the ram segments, so we will
  // selectively apply the deletes to that new segment.
  private final int applyDeletes(boolean flushedNewSegment) throws CorruptIndexException, IOException {

    final HashMap bufferedDeleteTerms = docWriter.getBufferedDeleteTerms();

    int delCount = 0;
    if (bufferedDeleteTerms.size() > 0) {
      if (infoStream != null)
        message("flush " + docWriter.getNumBufferedDeleteTerms() + " buffered deleted terms on "
                + segmentInfos.size() + " segments.");

      if (flushedNewSegment) {
        IndexReader reader = null;
        try {
          // Open readers w/o opening the stored fields /
          // vectors because these files may still be held
          // open for writing by docWriter
          reader = SegmentReader.get(segmentInfos.info(segmentInfos.size() - 1), false);

          // Apply delete terms to the segment just flushed from ram
          // apply appropriately so that a delete term is only applied to
          // the documents buffered before it, not those buffered after it.
          delCount += applyDeletesSelectively(bufferedDeleteTerms, reader);
        } finally {
          if (reader != null) {
            try {
              reader.doCommit();
            } finally {
              reader.doClose();
            }
          }
        }
      }

      int infosEnd = segmentInfos.size();
      if (flushedNewSegment) {
        infosEnd--;
      }

      for (int i = 0; i < infosEnd; i++) {
        IndexReader reader = null;
        try {
          reader = SegmentReader.get(segmentInfos.info(i), false);

          // Apply delete terms to disk segments
          // except the one just flushed from ram.
          delCount += applyDeletes(bufferedDeleteTerms, reader);
        } finally {
          if (reader != null) {
            try {
              reader.doCommit();
            } finally {
              reader.doClose();
            }
          }
        }
      }

      // Clean up bufferedDeleteTerms.
      docWriter.clearBufferedDeleteTerms();
    }

    return delCount;
  }

  // For test purposes.
  final synchronized int getBufferedDeleteTermsSize() {
    return docWriter.getBufferedDeleteTerms().size();
  }

  // For test purposes.
  final synchronized int getNumBufferedDeleteTerms() {
    return docWriter.getNumBufferedDeleteTerms();
  }

  // Apply buffered delete terms to the segment just flushed from ram
  // apply appropriately so that a delete term is only applied to
  // the documents buffered before it, not those buffered after it.
  private final int applyDeletesSelectively(HashMap deleteTerms,
      IndexReader reader) throws CorruptIndexException, IOException {
    Iterator iter = deleteTerms.entrySet().iterator();
    int delCount = 0;
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      Term term = (Term) entry.getKey();

      TermDocs docs = reader.termDocs(term);
      if (docs != null) {
        int num = ((DocumentsWriter.Num) entry.getValue()).getNum();
        try {
          while (docs.next()) {
            int doc = docs.doc();
            if (doc >= num) {
              break;
            }
            reader.deleteDocument(doc);
            delCount++;
          }
        } finally {
          docs.close();
        }
      }
    }
    return delCount;
  }

  // Apply buffered delete terms to this reader.
  private final int applyDeletes(HashMap deleteTerms, IndexReader reader)
      throws CorruptIndexException, IOException {
    Iterator iter = deleteTerms.entrySet().iterator();
    int delCount = 0;
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      delCount += reader.deleteDocuments((Term) entry.getKey());
    }
    return delCount;
  }

  // utility routines for tests
  SegmentInfo newestSegment() {
    return segmentInfos.info(segmentInfos.size()-1);
  }

  public synchronized String segString() {
    StringBuffer buffer = new StringBuffer();
    for(int i = 0; i < segmentInfos.size(); i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      buffer.append(segmentInfos.info(i).segString(directory));
    }

    return buffer.toString();
  }
}
