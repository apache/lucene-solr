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
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
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
  (which just deletes and then adds). When finished adding, deleting and updating documents, <a href="#close()"><b>close</b></a> should be called.</p>

  <p>These changes are buffered in memory and periodically
  flushed to the {@link Directory} (during the above method calls).  A flush is triggered when there are
  enough buffered deletes (see {@link
  #setMaxBufferedDeleteTerms}) or enough added documents
  (see {@link #setMaxBufferedDocs}) since the last flush,
  whichever is sooner.  When a flush occurs, both pending
  deletes and added documents are flushed to the index.  A
  flush may also trigger one or more segment merges.</p>

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
   * Default value is 10. Change using {@link #setMergeFactor(int)}.
   */
  public final static int DEFAULT_MERGE_FACTOR = 10;

  /**
   * Default value is 10. Change using {@link #setMaxBufferedDocs(int)}.
   */
  public final static int DEFAULT_MAX_BUFFERED_DOCS = 10;

  /**
   * Default value is 1000. Change using {@link #setMaxBufferedDeleteTerms(int)}.
   */
  public final static int DEFAULT_MAX_BUFFERED_DELETE_TERMS = 1000;

  /**
   * Default value is {@link Integer#MAX_VALUE}. Change using {@link #setMaxMergeDocs(int)}.
   */
  public final static int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;

  /**
   * Default value is 10,000. Change using {@link #setMaxFieldLength(int)}.
   */
  public final static int DEFAULT_MAX_FIELD_LENGTH = 10000;

  /**
   * Default value is 128. Change using {@link #setTermIndexInterval(int)}.
   */
  public final static int DEFAULT_TERM_INDEX_INTERVAL = 128;
  
  private Directory directory;  // where this index resides
  private Analyzer analyzer;    // how to analyze text

  private Similarity similarity = Similarity.getDefault(); // how to normalize

  private boolean commitPending; // true if segmentInfos has changes not yet committed
  private SegmentInfos rollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails

  private SegmentInfos localRollbackSegmentInfos;      // segmentInfos we will fallback to if the commit fails
  private boolean localAutoCommit;                // saved autoCommit during local transaction
  private boolean autoCommit = true;              // false if we should commit only on close

  SegmentInfos segmentInfos = new SegmentInfos();       // the segments
  SegmentInfos ramSegmentInfos = new SegmentInfos();    // the segments in ramDirectory
  private final RAMDirectory ramDirectory = new RAMDirectory(); // for temp segs
  private IndexFileDeleter deleter;

  private Lock writeLock;

  private int termIndexInterval = DEFAULT_TERM_INDEX_INTERVAL;

  // The max number of delete terms that can be buffered before
  // they must be flushed to disk.
  private int maxBufferedDeleteTerms = DEFAULT_MAX_BUFFERED_DELETE_TERMS;

  // This Hashmap buffers delete terms in ram before they are applied.
  // The key is delete term; the value is number of ram
  // segments the term applies to.
  private HashMap bufferedDeleteTerms = new HashMap();
  private int numBufferedDeleteTerms = 0;

  /** Use compound file setting. Defaults to true, minimizing the number of
   * files used.  Setting this to false may improve indexing performance, but
   * may also cause file handle problems.
   */
  private boolean useCompoundFile = true;

  private boolean closeDir;
  private boolean closed;

  /**
   * @throws AlreadyClosedException if this IndexWriter is closed
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  /** Get the current setting of whether to use the compound file format.
   *  Note that this just returns the value you set with setUseCompoundFile(boolean)
   *  or the default. You cannot use this to query the status of an existing index.
   *  @see #setUseCompoundFile(boolean)
   */
  public boolean getUseCompoundFile() {
    ensureOpen();
    return useCompoundFile;
  }

  /** Setting to turn on usage of a compound file. When on, multiple files
   *  for each segment are merged into a single file once the segment creation
   *  is finished. This is done regardless of what directory is in use.
   */
  public void setUseCompoundFile(boolean value) {
    ensureOpen();
    useCompoundFile = value;
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

      // Default deleter (for backwards compatibility) is
      // KeepOnlyLastCommitDeleter:
      deleter = new IndexFileDeleter(directory,
                                     deletionPolicy == null ? new KeepOnlyLastCommitDeletionPolicy() : deletionPolicy,
                                     segmentInfos, infoStream);

    } catch (IOException e) {
      this.writeLock.release();
      this.writeLock = null;
      throw e;
    }
  }

  /** Determines the largest number of documents ever merged by addDocument().
   * Small values (e.g., less than 10,000) are best for interactive indexing,
   * as this limits the length of pauses while indexing to a few seconds.
   * Larger values are best for batched indexing and speedier searches.
   *
   * <p>The default value is {@link Integer#MAX_VALUE}.
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    ensureOpen();
    this.maxMergeDocs = maxMergeDocs;
  }

  /**
   * @see #setMaxMergeDocs
   */
  public int getMaxMergeDocs() {
    ensureOpen();
    return maxMergeDocs;
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
   */
  public void setMaxFieldLength(int maxFieldLength) {
    ensureOpen();
    this.maxFieldLength = maxFieldLength;
  }

  /**
   * @see #setMaxFieldLength
   */
  public int getMaxFieldLength() {
    ensureOpen();
    return maxFieldLength;
  }

  /** Determines the minimal number of documents required before the buffered
   * in-memory documents are merged and a new Segment is created.
   * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
   * large value gives faster indexing.  At the same time, mergeFactor limits
   * the number of files open in a FSDirectory.
   *
   * <p> The default value is 10.
   *
   * @throws IllegalArgumentException if maxBufferedDocs is smaller than 2
   */
  public void setMaxBufferedDocs(int maxBufferedDocs) {
    ensureOpen();
    if (maxBufferedDocs < 2)
      throw new IllegalArgumentException("maxBufferedDocs must at least be 2");
    this.minMergeDocs = maxBufferedDocs;
  }

  /**
   * @see #setMaxBufferedDocs
   */
  public int getMaxBufferedDocs() {
    ensureOpen();
    return minMergeDocs;
  }

  /**
   * <p>Determines the minimal number of delete terms required before the buffered
   * in-memory delete terms are applied and flushed. If there are documents
   * buffered in memory at the time, they are merged and a new segment is
   * created.</p>

   * <p>The default value is {@link #DEFAULT_MAX_BUFFERED_DELETE_TERMS}.
   * @throws IllegalArgumentException if maxBufferedDeleteTerms is smaller than 1</p>
   */
  public void setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    ensureOpen();
    if (maxBufferedDeleteTerms < 1)
      throw new IllegalArgumentException("maxBufferedDeleteTerms must at least be 1");
    this.maxBufferedDeleteTerms = maxBufferedDeleteTerms;
  }

  /**
   * @see #setMaxBufferedDeleteTerms
   */
  public int getMaxBufferedDeleteTerms() {
    ensureOpen();
    return maxBufferedDeleteTerms;
  }

  /** Determines how often segment indices are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indices are faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while searches on unoptimized
   * indices are slower, indexing is faster.  Thus larger values (> 10) are best
   * for batch index creation, and smaller values (< 10) for indices that are
   * interactively maintained.
   *
   * <p>This must never be less than 2.  The default value is 10.
   */
  public void setMergeFactor(int mergeFactor) {
    ensureOpen();
    if (mergeFactor < 2)
      throw new IllegalArgumentException("mergeFactor cannot be less than 2");
    this.mergeFactor = mergeFactor;
  }

  /**
   * @see #setMergeFactor
   */
  public int getMergeFactor() {
    ensureOpen();
    return mergeFactor;
  }

  /** If non-null, this will be the default infoStream used
   * by a newly instantiated IndexWriter.
   * @see #setInfoStream
   */
  public static void setDefaultInfoStream(PrintStream infoStream) {
    IndexWriter.defaultInfoStream = infoStream;
  }

  /**
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
    deleter.setInfoStream(infoStream);
  }

  /**
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
  public synchronized void close() throws CorruptIndexException, IOException {
    if (!closed) {
      flushRamSegments();

      if (commitPending) {
        segmentInfos.write(directory);         // now commit changes
        deleter.checkpoint(segmentInfos, true);
        commitPending = false;
        rollbackSegmentInfos = null;
      }

      ramDirectory.close();
      if (writeLock != null) {
        writeLock.release();                          // release write lock
        writeLock = null;
      }
      closed = true;

      if(closeDir)
        directory.close();
    }
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
    int count = ramSegmentInfos.size();
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
   * <p>The amount of free space required when a merge is
   * triggered is up to 1X the size of all segments being
   * merged, when no readers/searchers are open against the
   * index, and up to 2X the size of all segments being
   * merged when readers/searchers are open against the
   * index (see {@link #optimize()} for details).  Most
   * merges are small (merging the smallest segments
   * together), but whenever a full merge occurs (all
   * segments in the index, which is the worst case for
   * temporary space usage) then the maximum free disk space
   * required is the same as {@link #optimize}.</p>
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
    SegmentInfo newSegmentInfo = buildSingleDocSegment(doc, analyzer);
    synchronized (this) {
      ramSegmentInfos.addElement(newSegmentInfo);
      maybeFlushRamSegments();
    }
  }

  SegmentInfo buildSingleDocSegment(Document doc, Analyzer analyzer)
      throws CorruptIndexException, IOException {
    DocumentWriter dw = new DocumentWriter(ramDirectory, analyzer, this);
    dw.setInfoStream(infoStream);
    String segmentName = newRamSegmentName();
    dw.addDocument(segmentName, doc);
    SegmentInfo si = new SegmentInfo(segmentName, 1, ramDirectory, false, false);
    si.setNumFields(dw.getNumFields());
    return si;
  }

  /**
   * Deletes the document(s) containing <code>term</code>.
   * @param term the term to identify the documents to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void deleteDocuments(Term term) throws CorruptIndexException, IOException {
    ensureOpen();
    bufferDeleteTerm(term);
    maybeFlushRamSegments();
  }

  /**
   * Deletes the document(s) containing any of the
   * terms. All deletes are flushed at the same time.
   * @param terms array of terms to identify the documents
   * to be deleted
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void deleteDocuments(Term[] terms) throws CorruptIndexException, IOException {
    ensureOpen();
    for (int i = 0; i < terms.length; i++) {
      bufferDeleteTerm(terms[i]);
    }
    maybeFlushRamSegments();
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
    SegmentInfo newSegmentInfo = buildSingleDocSegment(doc, analyzer);
    synchronized (this) {
      bufferDeleteTerm(term);
      ramSegmentInfos.addElement(newSegmentInfo);
      maybeFlushRamSegments();
    }
  }

  final synchronized String newRamSegmentName() {
    return "_ram_" + Integer.toString(ramSegmentInfos.counter++, Character.MAX_RADIX);
  }

  // for test purpose
  final synchronized int getSegmentCount(){
    return segmentInfos.size();
  }

  // for test purpose
  final synchronized int getRamSegmentCount(){
    return ramSegmentInfos.size();
  }

  // for test purpose
  final synchronized int getDocCount(int i) {
    if (i >= 0 && i < segmentInfos.size()) {
      return segmentInfos.info(i).docCount;
    } else {
      return -1;
    }
  }

  final synchronized String newSegmentName() {
    return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
  }

  /** Determines how often segment indices are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indices are faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while searches on unoptimized
   * indices are slower, indexing is faster.  Thus larger values (> 10) are best
   * for batch index creation, and smaller values (< 10) for indices that are
   * interactively maintained.
   *
   * <p>This must never be less than 2.  The default value is {@link #DEFAULT_MERGE_FACTOR}.

   */
  private int mergeFactor = DEFAULT_MERGE_FACTOR;

  /** Determines the minimal number of documents required before the buffered
   * in-memory documents are merging and a new Segment is created.
   * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
   * large value gives faster indexing.  At the same time, mergeFactor limits
   * the number of files open in a FSDirectory.
   *
   * <p> The default value is {@link #DEFAULT_MAX_BUFFERED_DOCS}.

   */
  private int minMergeDocs = DEFAULT_MAX_BUFFERED_DOCS;


  /** Determines the largest number of documents ever merged by addDocument().
   * Small values (e.g., less than 10,000) are best for interactive indexing,
   * as this limits the length of pauses while indexing to a few seconds.
   * Larger values are best for batched indexing and speedier searches.
   *
   * <p>The default value is {@link #DEFAULT_MAX_MERGE_DOCS}.

   */
  private int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;

  /** If non-null, information about merges will be printed to this.

   */
  private PrintStream infoStream = null;
  private static PrintStream defaultInfoStream = null;

  /** Merges all segments together into a single segment,
   * optimizing an index for search.
   * 
   * <p>Note that this requires substantial temporary free
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
   * <p>Once the optimize completes, the total size of the
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
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
  */
  public synchronized void optimize() throws CorruptIndexException, IOException {
    ensureOpen();
    flushRamSegments();
    while (segmentInfos.size() > 1 ||
           (segmentInfos.size() == 1 &&
            (SegmentReader.hasDeletions(segmentInfos.info(0)) ||
             SegmentReader.hasSeparateNorms(segmentInfos.info(0)) ||
             segmentInfos.info(0).dir != directory ||
             (useCompoundFile &&
              (!SegmentReader.usesCompoundFile(segmentInfos.info(0))))))) {
      int minSegment = segmentInfos.size() - mergeFactor;
      mergeSegments(segmentInfos, minSegment < 0 ? 0 : minSegment, segmentInfos.size());
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
   */
  private void startTransaction() throws IOException {
    localRollbackSegmentInfos = (SegmentInfos) segmentInfos.clone();
    localAutoCommit = autoCommit;
    if (localAutoCommit) {
      flushRamSegments();
      // Turn off auto-commit during our local transaction:
      autoCommit = false;
    }
  }

  /*
   * Rolls back the transaction and restores state to where
   * we were at the start.
   */
  private void rollbackTransaction() throws IOException {

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
    deleter.refresh();
  }

  /*
   * Commits the transaction.  This will write the new
   * segments file and remove and pending deletions we have
   * accumulated during the transaction
   */
  private void commitTransaction() throws IOException {

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;

    boolean success = false;
    try {
      checkpoint();
      success = true;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
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
  public synchronized void abort() throws IOException {
    ensureOpen();
    if (!autoCommit) {

      // Keep the same segmentInfos instance but replace all
      // of its SegmentInfo instances.  This is so the next
      // attempt to commit using this instance of IndexWriter
      // will always write to a new generation ("write once").
      segmentInfos.clear();
      segmentInfos.addAll(rollbackSegmentInfos);

      // Ask deleter to locate unreferenced files & remove
      // them:
      deleter.checkpoint(segmentInfos, false);
      deleter.refresh();

      ramSegmentInfos = new SegmentInfos();
      bufferedDeleteTerms.clear();
      numBufferedDeleteTerms = 0;

      commitPending = false;
      close();

    } else {
      throw new IllegalStateException("abort() can only be called when IndexWriter was opened with autoCommit=false");
    }
  }
 
  /*
   * Called whenever the SegmentInfos has been updated and
   * the index files referenced exist (correctly) in the
   * index directory.  If we are in autoCommit mode, we
   * commit the change immediately.  Else, we mark
   * commitPending.
   */
  private void checkpoint() throws IOException {
    if (autoCommit) {
      segmentInfos.write(directory);
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
    optimize();					  // start with zero or 1 seg

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

      // merge newly added segments in log(n) passes
      while (segmentInfos.size() > start+mergeFactor) {
        for (int base = start; base < segmentInfos.size(); base++) {
          int end = Math.min(segmentInfos.size(), base+mergeFactor);
          if (end-base > 1) {
            mergeSegments(segmentInfos, base, end);
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        commitTransaction();
      } else {
        rollbackTransaction();
      }
    }

    optimize();					  // final cleanup
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
    // Adding indexes can be viewed as adding a sequence of segments S to
    // a sequence of segments T. Segments in T follow the invariants but
    // segments in S may not since they could come from multiple indexes.
    // Here is the merge algorithm for addIndexesNoOptimize():
    //
    // 1 Flush ram segments.
    // 2 Consider a combined sequence with segments from T followed
    //   by segments from S (same as current addIndexes(Directory[])).
    // 3 Assume the highest level for segments in S is h. Call
    //   maybeMergeSegments(), but instead of starting w/ lowerBound = -1
    //   and upperBound = maxBufferedDocs, start w/ lowerBound = -1 and
    //   upperBound = upperBound of level h. After this, the invariants
    //   are guaranteed except for the last < M segments whose levels <= h.
    // 4 If the invariants hold for the last < M segments whose levels <= h,
    //   if some of those < M segments are from S (not merged in step 3),
    //   properly copy them over*, otherwise done.
    //   Otherwise, simply merge those segments. If the merge results in
    //   a segment of level <= h, done. Otherwise, it's of level h+1 and call
    //   maybeMergeSegments() starting w/ upperBound = upperBound of level h+1.
    //
    // * Ideally, we want to simply copy a segment. However, directory does
    // not support copy yet. In addition, source may use compound file or not
    // and target may use compound file or not. So we use mergeSegments() to
    // copy a segment, which may cause doc count to change because deleted
    // docs are garbage collected.

    // 1 flush ram segments

    ensureOpen();
    flushRamSegments();

    // 2 copy segment infos and find the highest level from dirs
    int startUpperBound = minMergeDocs;

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
          
          while (startUpperBound < info.docCount) {
            startUpperBound *= mergeFactor; // find the highest level from dirs
            if (startUpperBound > maxMergeDocs) {
              // upper bound cannot exceed maxMergeDocs
              throw new IllegalArgumentException("Upper bound cannot exceed maxMergeDocs");
            }
          }
        }
      }

      // 3 maybe merge segments starting from the highest level from dirs
      maybeMergeSegments(startUpperBound);

      // get the tail segments whose levels <= h
      int segmentCount = segmentInfos.size();
      int numTailSegments = 0;
      while (numTailSegments < segmentCount
             && startUpperBound >= segmentInfos.info(segmentCount - 1 - numTailSegments).docCount) {
        numTailSegments++;
      }
      if (numTailSegments == 0) {
        success = true;
        return;
      }

      // 4 make sure invariants hold for the tail segments whose levels <= h
      if (checkNonDecreasingLevels(segmentCount - numTailSegments)) {
        // identify the segments from S to be copied (not merged in 3)
        int numSegmentsToCopy = 0;
        while (numSegmentsToCopy < segmentCount
               && directory != segmentInfos.info(segmentCount - 1 - numSegmentsToCopy).dir) {
          numSegmentsToCopy++;
        }
        if (numSegmentsToCopy == 0) {
          success = true;
          return;
        }

        // copy those segments from S
        for (int i = segmentCount - numSegmentsToCopy; i < segmentCount; i++) {
          mergeSegments(segmentInfos, i, i + 1);
        }
        if (checkNonDecreasingLevels(segmentCount - numSegmentsToCopy)) {
          success = true;
          return;
        }
      }

      // invariants do not hold, simply merge those segments
      mergeSegments(segmentInfos, segmentCount - numTailSegments, segmentCount);

      // maybe merge segments again if necessary
      if (segmentInfos.info(segmentInfos.size() - 1).docCount > startUpperBound) {
        maybeMergeSegments(startUpperBound * mergeFactor);
      }

      success = true;
    } finally {
      if (success) {
        commitTransaction();
      } else {
        rollbackTransaction();
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
        info = new SegmentInfo(mergedName, docCount, directory, false, true);
        segmentInfos.addElement(info);

        success = true;

      } finally {
        if (!success) {
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
    
    if (useCompoundFile) {

      boolean success = false;

      startTransaction();

      try {
        merger.createCompoundFile(mergedName + ".cfs");
        info.setUseCompoundFile(true);
      } finally {
        if (!success) {
          rollbackTransaction();
        } else {
          commitTransaction();
        }
      }
    }
  }

  // Overview of merge policy:
  //
  // A flush is triggered either by close() or by the number of ram segments
  // reaching maxBufferedDocs. After a disk segment is created by the flush,
  // further merges may be triggered.
  //
  // LowerBound and upperBound set the limits on the doc count of a segment
  // which may be merged. Initially, lowerBound is set to 0 and upperBound
  // to maxBufferedDocs. Starting from the rightmost* segment whose doc count
  // > lowerBound and <= upperBound, count the number of consecutive segments
  // whose doc count <= upperBound.
  //
  // Case 1: number of worthy segments < mergeFactor, no merge, done.
  // Case 2: number of worthy segments == mergeFactor, merge these segments.
  //         If the doc count of the merged segment <= upperBound, done.
  //         Otherwise, set lowerBound to upperBound, and multiply upperBound
  //         by mergeFactor, go through the process again.
  // Case 3: number of worthy segments > mergeFactor (in the case mergeFactor
  //         M changes), merge the leftmost* M segments. If the doc count of
  //         the merged segment <= upperBound, consider the merged segment for
  //         further merges on this same level. Merge the now leftmost* M
  //         segments, and so on, until number of worthy segments < mergeFactor.
  //         If the doc count of all the merged segments <= upperBound, done.
  //         Otherwise, set lowerBound to upperBound, and multiply upperBound
  //         by mergeFactor, go through the process again.
  // Note that case 2 can be considerd as a special case of case 3.
  //
  // This merge policy guarantees two invariants if M does not change and
  // segment doc count is not reaching maxMergeDocs:
  // B for maxBufferedDocs, f(n) defined as ceil(log_M(ceil(n/B)))
  //      1: If i (left*) and i+1 (right*) are two consecutive segments of doc
  //         counts x and y, then f(x) >= f(y).
  //      2: The number of committed segments on the same level (f(n)) <= M.

  // This is called after pending added and deleted
  // documents have been flushed to the Directory but before
  // the change is committed (new segments_N file written).
  void doAfterFlush()
    throws IOException {
  }

  protected final void maybeFlushRamSegments() throws CorruptIndexException, IOException {
    // A flush is triggered if enough new documents are buffered or
    // if enough delete terms are buffered
    if (ramSegmentInfos.size() >= minMergeDocs || numBufferedDeleteTerms >= maxBufferedDeleteTerms) {
      flushRamSegments();
    }
  }

  /** Expert:  Flushes all RAM-resident segments (buffered documents), then may merge segments. */
  private final synchronized void flushRamSegments() throws CorruptIndexException, IOException {
    if (ramSegmentInfos.size() > 0 || bufferedDeleteTerms.size() > 0) {
      mergeSegments(ramSegmentInfos, 0, ramSegmentInfos.size());
      maybeMergeSegments(minMergeDocs);
    }
  }

  /**
   * Flush all in-memory buffered updates (adds and deletes)
   * to the Directory. 
   * <p>Note: if <code>autoCommit=false</code>, flushed data would still 
   * not be visible to readers, until {@link #close} is called.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public final synchronized void flush() throws CorruptIndexException, IOException {
    ensureOpen();
    flushRamSegments();
  }

  /** Expert:  Return the total size of all index files currently cached in memory.
   * Useful for size management with flushRamDocs()
   */
  public final long ramSizeInBytes() {
    ensureOpen();
    return ramDirectory.sizeInBytes();
  }

  /** Expert:  Return the number of documents whose segments are currently cached in memory.
   * Useful when calling flushRamSegments()
   */
  public final synchronized int numRamDocs() {
    ensureOpen();
    return ramSegmentInfos.size();
  }
  
  /** Incremental segment merger.  */
  private final void maybeMergeSegments(int startUpperBound) throws CorruptIndexException, IOException {
    long lowerBound = -1;
    long upperBound = startUpperBound;

    while (upperBound < maxMergeDocs) {
      int minSegment = segmentInfos.size();
      int maxSegment = -1;

      // find merge-worthy segments
      while (--minSegment >= 0) {
        SegmentInfo si = segmentInfos.info(minSegment);

        if (maxSegment == -1 && si.docCount > lowerBound && si.docCount <= upperBound) {
          // start from the rightmost* segment whose doc count is in bounds
          maxSegment = minSegment;
        } else if (si.docCount > upperBound) {
          // until the segment whose doc count exceeds upperBound
          break;
        }
      }

      minSegment++;
      maxSegment++;
      int numSegments = maxSegment - minSegment;

      if (numSegments < mergeFactor) {
        break;
      } else {
        boolean exceedsUpperLimit = false;

        // number of merge-worthy segments may exceed mergeFactor when
        // mergeFactor and/or maxBufferedDocs change(s)
        while (numSegments >= mergeFactor) {
          // merge the leftmost* mergeFactor segments

          int docCount = mergeSegments(segmentInfos, minSegment, minSegment + mergeFactor);
          numSegments -= mergeFactor;

          if (docCount > upperBound) {
            // continue to merge the rest of the worthy segments on this level
            minSegment++;
            exceedsUpperLimit = true;
          } else {
            // if the merged segment does not exceed upperBound, consider
            // this segment for further merges on this same level
            numSegments++;
          }
        }

        if (!exceedsUpperLimit) {
          // if none of the merged segments exceed upperBound, done
          break;
        }
      }

      lowerBound = upperBound;
      upperBound *= mergeFactor;
    }
  }

  /**
   * Merges the named range of segments, replacing them in the stack with a
   * single segment.
   */
  private final int mergeSegments(SegmentInfos sourceSegments, int minSegment, int end)
    throws CorruptIndexException, IOException {

    // We may be called solely because there are deletes
    // pending, in which case doMerge is false:
    boolean doMerge = end > 0;
    final String mergedName = newSegmentName();
    SegmentMerger merger = null;

    final List ramSegmentsToDelete = new ArrayList();

    SegmentInfo newSegment = null;

    int mergedDocCount = 0;
    boolean anyDeletes = (bufferedDeleteTerms.size() != 0);

    // This is try/finally to make sure merger's readers are closed:
    try {

      if (doMerge) {
        if (infoStream != null) infoStream.print("merging segments");
        merger = new SegmentMerger(this, mergedName);

        for (int i = minSegment; i < end; i++) {
          SegmentInfo si = sourceSegments.info(i);
          if (infoStream != null)
            infoStream.print(" " + si.name + " (" + si.docCount + " docs)");
          IndexReader reader = SegmentReader.get(si); // no need to set deleter (yet)
          merger.add(reader);
          if (reader.directory() == this.ramDirectory) {
            ramSegmentsToDelete.add(si);
          }
        }
      }

      SegmentInfos rollback = null;
      boolean success = false;

      // This is try/finally to rollback our internal state
      // if we hit exception when doing the merge:
      try {

        if (doMerge) {
          mergedDocCount = merger.merge();

          if (infoStream != null) {
            infoStream.println(" into "+mergedName+" ("+mergedDocCount+" docs)");
          }

          newSegment = new SegmentInfo(mergedName, mergedDocCount,
                                       directory, false, true);
        }
        
        if (sourceSegments != ramSegmentInfos || anyDeletes) {
          // Now save the SegmentInfo instances that
          // we are replacing:
          rollback = (SegmentInfos) segmentInfos.clone();
        }

        if (doMerge) {
          if (sourceSegments == ramSegmentInfos) {
            segmentInfos.addElement(newSegment);
          } else {
            for (int i = end-1; i > minSegment; i--)     // remove old infos & add new
              sourceSegments.remove(i);

            segmentInfos.set(minSegment, newSegment);
          }
        }

        if (sourceSegments == ramSegmentInfos) {
          maybeApplyDeletes(doMerge);
          doAfterFlush();
        }
        
        checkpoint();

        success = true;

      } finally {

        if (success) {
          // The non-ram-segments case is already committed
          // (above), so all the remains for ram segments case
          // is to clear the ram segments:
          if (sourceSegments == ramSegmentInfos) {
            ramSegmentInfos.removeAllElements();
          }
        } else {

          // Must rollback so our state matches index:
          if (sourceSegments == ramSegmentInfos && !anyDeletes) {
            // Simple case: newSegment may or may not have
            // been added to the end of our segment infos,
            // so just check & remove if so:
            if (newSegment != null && 
                segmentInfos.size() > 0 && 
                segmentInfos.info(segmentInfos.size()-1) == newSegment) {
              segmentInfos.remove(segmentInfos.size()-1);
            }
          } else if (rollback != null) {
            // Rollback the individual SegmentInfo
            // instances, but keep original SegmentInfos
            // instance (so we don't try to write again the
            // same segments_N file -- write once):
            segmentInfos.clear();
            segmentInfos.addAll(rollback);
          }

          // Delete any partially created and now unreferenced files:
          deleter.refresh();
        }
      }
    } finally {
      // close readers before we attempt to delete now-obsolete segments
      if (doMerge) merger.closeReaders();
    }

    // Delete the RAM segments
    deleter.deleteDirect(ramDirectory, ramSegmentsToDelete);

    // Give deleter a chance to remove files now.
    deleter.checkpoint(segmentInfos, autoCommit);

    if (useCompoundFile && doMerge) {

      boolean success = false;

      try {

        merger.createCompoundFile(mergedName + ".cfs");
        newSegment.setUseCompoundFile(true);
        checkpoint();
        success = true;

      } finally {
        if (!success) {  
          // Must rollback:
          newSegment.setUseCompoundFile(false);
          deleter.refresh();
        }
      }
      
      // Give deleter a chance to remove files now.
      deleter.checkpoint(segmentInfos, autoCommit);
    }

    return mergedDocCount;
  }

  // Called during flush to apply any buffered deletes.  If
  // doMerge is true then a new segment was just created and
  // flushed from the ram segments.
  private final void maybeApplyDeletes(boolean doMerge) throws CorruptIndexException, IOException {

    if (bufferedDeleteTerms.size() > 0) {
      if (infoStream != null)
        infoStream.println("flush " + numBufferedDeleteTerms + " buffered deleted terms on "
                           + segmentInfos.size() + " segments.");

      if (doMerge) {
        IndexReader reader = null;
        try {
          reader = SegmentReader.get(segmentInfos.info(segmentInfos.size() - 1));

          // Apply delete terms to the segment just flushed from ram
          // apply appropriately so that a delete term is only applied to
          // the documents buffered before it, not those buffered after it.
          applyDeletesSelectively(bufferedDeleteTerms, reader);
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
      if (doMerge) {
        infosEnd--;
      }

      for (int i = 0; i < infosEnd; i++) {
        IndexReader reader = null;
        try {
          reader = SegmentReader.get(segmentInfos.info(i));

          // Apply delete terms to disk segments
          // except the one just flushed from ram.
          applyDeletes(bufferedDeleteTerms, reader);
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
      bufferedDeleteTerms.clear();
      numBufferedDeleteTerms = 0;
    }
  }

  private final boolean checkNonDecreasingLevels(int start) {
    int lowerBound = -1;
    int upperBound = minMergeDocs;

    for (int i = segmentInfos.size() - 1; i >= start; i--) {
      int docCount = segmentInfos.info(i).docCount;
      if (docCount <= lowerBound) {
        return false;
      }

      while (docCount > upperBound) {
        lowerBound = upperBound;
        upperBound *= mergeFactor;
      }
    }
    return true;
  }

  // For test purposes.
  final synchronized int getBufferedDeleteTermsSize() {
    return bufferedDeleteTerms.size();
  }

  // For test purposes.
  final synchronized int getNumBufferedDeleteTerms() {
    return numBufferedDeleteTerms;
  }

  // Number of ram segments a delete term applies to.
  private static class Num {
    private int num;

    Num(int num) {
      this.num = num;
    }

    int getNum() {
      return num;
    }

    void setNum(int num) {
      this.num = num;
    }
  }

  // Buffer a term in bufferedDeleteTerms, which records the
  // current number of documents buffered in ram so that the
  // delete term will be applied to those ram segments as
  // well as the disk segments.
  private void bufferDeleteTerm(Term term) {
    Num num = (Num) bufferedDeleteTerms.get(term);
    if (num == null) {
      bufferedDeleteTerms.put(term, new Num(ramSegmentInfos.size()));
    } else {
      num.setNum(ramSegmentInfos.size());
    }
    numBufferedDeleteTerms++;
  }

  // Apply buffered delete terms to the segment just flushed from ram
  // apply appropriately so that a delete term is only applied to
  // the documents buffered before it, not those buffered after it.
  private final void applyDeletesSelectively(HashMap deleteTerms,
      IndexReader reader) throws CorruptIndexException, IOException {
    Iterator iter = deleteTerms.entrySet().iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      Term term = (Term) entry.getKey();

      TermDocs docs = reader.termDocs(term);
      if (docs != null) {
        int num = ((Num) entry.getValue()).getNum();
        try {
          while (docs.next()) {
            int doc = docs.doc();
            if (doc >= num) {
              break;
            }
            reader.deleteDocument(doc);
          }
        } finally {
          docs.close();
        }
      }
    }
  }

  // Apply buffered delete terms to this reader.
  private final void applyDeletes(HashMap deleteTerms, IndexReader reader)
      throws CorruptIndexException, IOException {
    Iterator iter = deleteTerms.entrySet().iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      reader.deleteDocuments((Term) entry.getKey());
    }
  }
}
