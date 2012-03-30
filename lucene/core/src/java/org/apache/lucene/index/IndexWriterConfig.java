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

import java.io.PrintStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

/**
 * Holds all the configuration of {@link IndexWriter}.  You
 * should instantiate this class, call the setters to set
 * your configuration, then pass it to {@link IndexWriter}.
 * Note that {@link IndexWriter} makes a private clone; if
 * you need to subsequently change settings use {@link
 * IndexWriter#getConfig}.
 *
 * <p>
 * All setter methods return {@link IndexWriterConfig} to allow chaining
 * settings conveniently, for example:
 * 
 * <pre>
 * IndexWriterConfig conf = new IndexWriterConfig(analyzer);
 * conf.setter1().setter2();
 * </pre>
 *
 * @since 3.1
 */
public final class IndexWriterConfig implements Cloneable {

  /**
   * Specifies the open mode for {@link IndexWriter}:
   * <ul>
   * {@link #CREATE} - creates a new index or overwrites an existing one.
   * {@link #CREATE_OR_APPEND} - creates a new index if one does not exist,
   * otherwise it opens the index and documents will be appended.
   * {@link #APPEND} - opens an existing index.
   * </ul>
   */
  public static enum OpenMode { CREATE, APPEND, CREATE_OR_APPEND }

  /** Default value is 32. Change using {@link #setTermIndexInterval(int)}. */
  public static final int DEFAULT_TERM_INDEX_INTERVAL = 32; // TODO: this should be private to the codec, not settable here

  /** Denotes a flush trigger is disabled. */
  public final static int DISABLE_AUTO_FLUSH = -1;

  /** Disabled by default (because IndexWriter flushes by RAM usage by default). */
  public final static int DEFAULT_MAX_BUFFERED_DELETE_TERMS = DISABLE_AUTO_FLUSH;

  /** Disabled by default (because IndexWriter flushes by RAM usage by default). */
  public final static int DEFAULT_MAX_BUFFERED_DOCS = DISABLE_AUTO_FLUSH;

  /**
   * Default value is 16 MB (which means flush when buffered docs consume
   * approximately 16 MB RAM).
   */
  public final static double DEFAULT_RAM_BUFFER_SIZE_MB = 16.0;

  /**
   * Default value for the write lock timeout (1,000 ms).
   *
   * @see #setDefaultWriteLockTimeout(long)
   */
  public static long WRITE_LOCK_TIMEOUT = 1000;

  /** Default setting for {@link #setReaderPooling}. */
  public final static boolean DEFAULT_READER_POOLING = false;

  /** Default value is 1. Change using {@link #setReaderTermsIndexDivisor(int)}. */
  public static final int DEFAULT_READER_TERMS_INDEX_DIVISOR = DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR;

  /** Default value is 1945. Change using {@link #setRAMPerThreadHardLimitMB(int)} */
  public static final int DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB = 1945;
  
  /** The maximum number of simultaneous threads that may be
   *  indexing documents at once in IndexWriter; if more
   *  than this many threads arrive they will wait for
   *  others to finish. Default value is 8. */
  public final static int DEFAULT_MAX_THREAD_STATES = 8;
  
  /**
   * Sets the default (for any instance) maximum time to wait for a write lock
   * (in milliseconds).
   */
  public static void setDefaultWriteLockTimeout(long writeLockTimeout) {
    WRITE_LOCK_TIMEOUT = writeLockTimeout;
  }

  /**
   * Returns the default write lock timeout for newly instantiated
   * IndexWriterConfigs.
   *
   * @see #setDefaultWriteLockTimeout(long)
   */
  public static long getDefaultWriteLockTimeout() {
    return WRITE_LOCK_TIMEOUT;
  }

  private final Analyzer analyzer;
  private volatile IndexDeletionPolicy delPolicy;
  private volatile IndexCommit commit;
  private volatile OpenMode openMode;
  private volatile Similarity similarity;
  private volatile int termIndexInterval; // TODO: this should be private to the codec, not settable here
  private volatile MergeScheduler mergeScheduler;
  private volatile long writeLockTimeout;
  private volatile int maxBufferedDeleteTerms;
  private volatile double ramBufferSizeMB;
  private volatile int maxBufferedDocs;
  private volatile IndexingChain indexingChain;
  private volatile IndexReaderWarmer mergedSegmentWarmer;
  private volatile Codec codec;
  private volatile InfoStream infoStream;
  private volatile MergePolicy mergePolicy;
  private volatile DocumentsWriterPerThreadPool indexerThreadPool;
  private volatile boolean readerPooling;
  private volatile int readerTermsIndexDivisor;
  private volatile FlushPolicy flushPolicy;
  private volatile int perThreadHardLimitMB;

  private Version matchVersion;

  /**
   * Creates a new config that with defaults that match the specified
   * {@link Version} as well as the default {@link
   * Analyzer}. If matchVersion is >= {@link
   * Version#LUCENE_32}, {@link TieredMergePolicy} is used
   * for merging; else {@link LogByteSizeMergePolicy}.
   * Note that {@link TieredMergePolicy} is free to select
   * non-contiguous merges, which means docIDs may not
   * remain montonic over time.  If this is a problem you
   * should switch to {@link LogByteSizeMergePolicy} or
   * {@link LogDocMergePolicy}.
   */
  public IndexWriterConfig(Version matchVersion, Analyzer analyzer) {
    this.matchVersion = matchVersion;
    this.analyzer = analyzer;
    delPolicy = new KeepOnlyLastCommitDeletionPolicy();
    commit = null;
    openMode = OpenMode.CREATE_OR_APPEND;
    similarity = IndexSearcher.getDefaultSimilarity();
    termIndexInterval = DEFAULT_TERM_INDEX_INTERVAL; // TODO: this should be private to the codec, not settable here
    mergeScheduler = new ConcurrentMergeScheduler();
    writeLockTimeout = WRITE_LOCK_TIMEOUT;
    maxBufferedDeleteTerms = DEFAULT_MAX_BUFFERED_DELETE_TERMS;
    ramBufferSizeMB = DEFAULT_RAM_BUFFER_SIZE_MB;
    maxBufferedDocs = DEFAULT_MAX_BUFFERED_DOCS;
    indexingChain = DocumentsWriterPerThread.defaultIndexingChain;
    mergedSegmentWarmer = null;
    codec = Codec.getDefault();
    infoStream = InfoStream.getDefault();
    if (matchVersion.onOrAfter(Version.LUCENE_32)) {
      mergePolicy = new TieredMergePolicy();
    } else {
      mergePolicy = new LogByteSizeMergePolicy();
    }
    flushPolicy = new FlushByRamOrCountsPolicy();
    readerPooling = DEFAULT_READER_POOLING;
    indexerThreadPool = new ThreadAffinityDocumentsWriterThreadPool(DEFAULT_MAX_THREAD_STATES);
    readerTermsIndexDivisor = DEFAULT_READER_TERMS_INDEX_DIVISOR;
    perThreadHardLimitMB = DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB;
  }

  @Override
  public IndexWriterConfig clone() {
    // Shallow clone is the only thing that's possible, since parameters like
    // analyzer, index commit etc. do not implement Cloneable.
    try {
      return (IndexWriterConfig)super.clone();
    } catch (CloneNotSupportedException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }

  /** Returns the default analyzer to use for indexing documents. */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /** Specifies {@link OpenMode} of the index.
   * 
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setOpenMode(OpenMode openMode) {
    this.openMode = openMode;
    return this;
  }

  /** Returns the {@link OpenMode} set by {@link #setOpenMode(OpenMode)}. */
  public OpenMode getOpenMode() {
    return openMode;
  }

  /**
   * Expert: allows an optional {@link IndexDeletionPolicy} implementation to be
   * specified. You can use this to control when prior commits are deleted from
   * the index. The default policy is {@link KeepOnlyLastCommitDeletionPolicy}
   * which removes all prior commits as soon as a new commit is done (this
   * matches behavior before 2.2). Creating your own policy can allow you to
   * explicitly keep previous "point in time" commits alive in the index for
   * some time, to allow readers to refresh to the new commit without having the
   * old commit deleted out from under them. This is necessary on filesystems
   * like NFS that do not support "delete on last close" semantics, which
   * Lucene's "point in time" search normally relies on.
   * <p>
   * <b>NOTE:</b> the deletion policy cannot be null. If <code>null</code> is
   * passed, the deletion policy will be set to the default.
   *
   * <p>Only takes effect when IndexWriter is first created. 
   */
  public IndexWriterConfig setIndexDeletionPolicy(IndexDeletionPolicy delPolicy) {
    this.delPolicy = delPolicy == null ? new KeepOnlyLastCommitDeletionPolicy() : delPolicy;
    return this;
  }

  /**
   * Returns the {@link IndexDeletionPolicy} specified in
   * {@link #setIndexDeletionPolicy(IndexDeletionPolicy)} or the default
   * {@link KeepOnlyLastCommitDeletionPolicy}/
   */
  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return delPolicy;
  }

  /**
   * Expert: allows to open a certain commit point. The default is null which
   * opens the latest commit point.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setIndexCommit(IndexCommit commit) {
    this.commit = commit;
    return this;
  }

  /**
   * Returns the {@link IndexCommit} as specified in
   * {@link #setIndexCommit(IndexCommit)} or the default, <code>null</code>
   * which specifies to open the latest index commit point.
   */
  public IndexCommit getIndexCommit() {
    return commit;
  }

  /**
   * Expert: set the {@link Similarity} implementation used by this IndexWriter.
   * <p>
   * <b>NOTE:</b> the similarity cannot be null. If <code>null</code> is passed,
   * the similarity will be set to the default implementation (unspecified).
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setSimilarity(Similarity similarity) {
    this.similarity = similarity == null ? IndexSearcher.getDefaultSimilarity() : similarity;
    return this;
  }

  /**
   * Expert: returns the {@link Similarity} implementation used by this
   * IndexWriter.
   */
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Expert: set the interval between indexed terms. Large values cause less
   * memory to be used by IndexReader, but slow random-access to terms. Small
   * values cause more memory to be used by an IndexReader, and speed
   * random-access to terms.
   * <p>
   * This parameter determines the amount of computation required per query
   * term, regardless of the number of documents that contain that term. In
   * particular, it is the maximum number of other terms that must be scanned
   * before a term is located and its frequency and position information may be
   * processed. In a large index with user-entered query terms, query processing
   * time is likely to be dominated not by term lookup but rather by the
   * processing of frequency and positional data. In a small index or when many
   * uncommon query terms are generated (e.g., by wildcard queries) term lookup
   * may become a dominant cost.
   * <p>
   * In particular, <code>numUniqueTerms/interval</code> terms are read into
   * memory by an IndexReader, and, on average, <code>interval/2</code> terms
   * must be scanned for each random term access.
   *
   * @see #DEFAULT_TERM_INDEX_INTERVAL
   *
   * <p>Takes effect immediately, but only applies to newly
   *  flushed/merged segments. */
  public IndexWriterConfig setTermIndexInterval(int interval) { // TODO: this should be private to the codec, not settable here
    this.termIndexInterval = interval;
    return this;
  }

  /**
   * Returns the interval between indexed terms.
   *
   * @see #setTermIndexInterval(int)
   */
  public int getTermIndexInterval() { // TODO: this should be private to the codec, not settable here
    return termIndexInterval;
  }

  /**
   * Expert: sets the merge scheduler used by this writer. The default is
   * {@link ConcurrentMergeScheduler}.
   * <p>
   * <b>NOTE:</b> the merge scheduler cannot be null. If <code>null</code> is
   * passed, the merge scheduler will be set to the default.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setMergeScheduler(MergeScheduler mergeScheduler) {
    this.mergeScheduler = mergeScheduler == null ? new ConcurrentMergeScheduler() : mergeScheduler;
    return this;
  }

  /**
   * Returns the {@link MergeScheduler} that was set by
   * {@link #setMergeScheduler(MergeScheduler)}
   */
  public MergeScheduler getMergeScheduler() {
    return mergeScheduler;
  }

  /**
   * Sets the maximum time to wait for a write lock (in milliseconds) for this
   * instance. You can change the default value for all instances by calling
   * {@link #setDefaultWriteLockTimeout(long)}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setWriteLockTimeout(long writeLockTimeout) {
    this.writeLockTimeout = writeLockTimeout;
    return this;
  }

  /**
   * Returns allowed timeout when acquiring the write lock.
   *
   * @see #setWriteLockTimeout(long)
   */
  public long getWriteLockTimeout() {
    return writeLockTimeout;
  }

  /**
   * Determines the minimal number of delete terms required before the buffered
   * in-memory delete terms and queries are applied and flushed.
   * <p>Disabled by default (writer flushes by RAM usage).</p>
   * <p>
   * NOTE:  This setting won't trigger a segment flush.
   * </p>
   * 
   * @throws IllegalArgumentException if maxBufferedDeleteTerms
   * is enabled but smaller than 1
   * @see #setRAMBufferSizeMB
   * @see #setFlushPolicy(FlushPolicy)
   *
   * <p>Takes effect immediately, but only the next time a
   * document is added, updated or deleted.
   */
  public IndexWriterConfig setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    if (maxBufferedDeleteTerms != DISABLE_AUTO_FLUSH
        && maxBufferedDeleteTerms < 1)
      throw new IllegalArgumentException(
          "maxBufferedDeleteTerms must at least be 1 when enabled");
    this.maxBufferedDeleteTerms = maxBufferedDeleteTerms;
    return this;
  }

  /**
   * Returns the number of buffered deleted terms that will trigger a flush of all
   * buffered deletes if enabled.
   *
   * @see #setMaxBufferedDeleteTerms(int)
   */
  public int getMaxBufferedDeleteTerms() {
    return maxBufferedDeleteTerms;
  }

  /**
   * Determines the amount of RAM that may be used for buffering added documents
   * and deletions before they are flushed to the Directory. Generally for
   * faster indexing performance it's best to flush by RAM usage instead of
   * document count and use as large a RAM buffer as you can.
   * <p>
   * When this is set, the writer will flush whenever buffered documents and
   * deletions use this much RAM. Pass in {@link #DISABLE_AUTO_FLUSH} to prevent
   * triggering a flush due to RAM usage. Note that if flushing by document
   * count is also enabled, then the flush will be triggered by whichever comes
   * first.
   * <p>
   * The maximum RAM limit is inherently determined by the JVMs available memory.
   * Yet, an {@link IndexWriter} session can consume a significantly larger amount
   * of memory than the given RAM limit since this limit is just an indicator when
   * to flush memory resident documents to the Directory. Flushes are likely happen
   * concurrently while other threads adding documents to the writer. For application
   * stability the available memory in the JVM should be significantly larger than
   * the RAM buffer used for indexing.
   * <p>
   * <b>NOTE</b>: the account of RAM usage for pending deletions is only
   * approximate. Specifically, if you delete by Query, Lucene currently has no
   * way to measure the RAM usage of individual Queries so the accounting will
   * under-estimate and you should compensate by either calling commit()
   * periodically yourself, or by using {@link #setMaxBufferedDeleteTerms(int)}
   * to flush and apply buffered deletes by count instead of RAM usage
   * (for each buffered delete Query a constant number of bytes is used to estimate
   * RAM usage). Note that enabling {@link #setMaxBufferedDeleteTerms(int)} will
   * not trigger any segment flushes.
   * <p>
   * <b>NOTE</b>: It's not guaranteed that all memory resident documents are flushed 
   * once this limit is exceeded. Depending on the configured {@link FlushPolicy} only a
   * subset of the buffered documents are flushed and therefore only parts of the RAM
   * buffer is released.    
   * <p>
   * 
   * The default value is {@link #DEFAULT_RAM_BUFFER_SIZE_MB}.
   * @see #setFlushPolicy(FlushPolicy)
   * @see #setRAMPerThreadHardLimitMB(int)
   *
   * <p>Takes effect immediately, but only the next time a
   * document is added, updated or deleted.
   *
   * @throws IllegalArgumentException
   *           if ramBufferSize is enabled but non-positive, or it disables
   *           ramBufferSize when maxBufferedDocs is already disabled
   *           
   */
  public IndexWriterConfig setRAMBufferSizeMB(double ramBufferSizeMB) {
    if (ramBufferSizeMB != DISABLE_AUTO_FLUSH && ramBufferSizeMB <= 0.0)
      throw new IllegalArgumentException(
          "ramBufferSize should be > 0.0 MB when enabled");
    if (ramBufferSizeMB == DISABLE_AUTO_FLUSH && maxBufferedDocs == DISABLE_AUTO_FLUSH)
      throw new IllegalArgumentException(
          "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    this.ramBufferSizeMB = ramBufferSizeMB;
    return this;
  }

  /** Returns the value set by {@link #setRAMBufferSizeMB(double)} if enabled. */
  public double getRAMBufferSizeMB() {
    return ramBufferSizeMB;
  }

  /**
   * Determines the minimal number of documents required before the buffered
   * in-memory documents are flushed as a new Segment. Large values generally
   * give faster indexing.
   *
   * <p>
   * When this is set, the writer will flush every maxBufferedDocs added
   * documents. Pass in {@link #DISABLE_AUTO_FLUSH} to prevent triggering a
   * flush due to number of buffered documents. Note that if flushing by RAM
   * usage is also enabled, then the flush will be triggered by whichever comes
   * first.
   *
   * <p>
   * Disabled by default (writer flushes by RAM usage).
   *
   * <p>Takes effect immediately, but only the next time a
   * document is added, updated or deleted.
   *
   * @see #setRAMBufferSizeMB(double)
   * @see #setFlushPolicy(FlushPolicy)
   * @throws IllegalArgumentException
   *           if maxBufferedDocs is enabled but smaller than 2, or it disables
   *           maxBufferedDocs when ramBufferSize is already disabled
   */
  public IndexWriterConfig setMaxBufferedDocs(int maxBufferedDocs) {
    if (maxBufferedDocs != DISABLE_AUTO_FLUSH && maxBufferedDocs < 2)
      throw new IllegalArgumentException(
          "maxBufferedDocs must at least be 2 when enabled");
    if (maxBufferedDocs == DISABLE_AUTO_FLUSH
        && ramBufferSizeMB == DISABLE_AUTO_FLUSH)
      throw new IllegalArgumentException(
          "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    this.maxBufferedDocs = maxBufferedDocs;
    return this;
  }

  /**
   * Returns the number of buffered added documents that will trigger a flush if
   * enabled.
   *
   * @see #setMaxBufferedDocs(int)
   */
  public int getMaxBufferedDocs() {
    return maxBufferedDocs;
  }

  /** Set the merged segment warmer. See {@link IndexReaderWarmer}.
   *
   * <p>Takes effect on the next merge. */
  public IndexWriterConfig setMergedSegmentWarmer(IndexReaderWarmer mergeSegmentWarmer) {
    this.mergedSegmentWarmer = mergeSegmentWarmer;
    return this;
  }

  /** Returns the current merged segment warmer. See {@link IndexReaderWarmer}. */
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return mergedSegmentWarmer;
  }

  /**
   * Expert: {@link MergePolicy} is invoked whenever there are changes to the
   * segments in the index. Its role is to select which merges to do, if any,
   * and return a {@link MergePolicy.MergeSpecification} describing the merges.
   * It also selects merges to do for forceMerge. (The default is
   * {@link LogByteSizeMergePolicy}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    this.mergePolicy = mergePolicy == null ? new LogByteSizeMergePolicy() : mergePolicy;
    return this;
  }

  /** Set the Codec. See {@link Codec}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setCodec(Codec codec) {
    this.codec = codec;
    return this;
  }

  /** Returns the current Codec. See {@link Codec}. */
  public Codec getCodec() {
    return codec;
  }


  /**
   * Returns the current MergePolicy in use by this writer.
   *
   * @see #setMergePolicy(MergePolicy)
   */
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }

  /** Expert: Sets the {@link DocumentsWriterPerThreadPool} instance used by the
   * IndexWriter to assign thread-states to incoming indexing threads. If no
   * {@link DocumentsWriterPerThreadPool} is set {@link IndexWriter} will use
   * {@link ThreadAffinityDocumentsWriterThreadPool} with max number of
   * thread-states set to {@link #DEFAULT_MAX_THREAD_STATES} (see
   * {@link #DEFAULT_MAX_THREAD_STATES}).
   * </p>
   * <p>
   * NOTE: The given {@link DocumentsWriterPerThreadPool} instance must not be used with
   * other {@link IndexWriter} instances once it has been initialized / associated with an
   * {@link IndexWriter}.
   * </p>
   * <p>
   * NOTE: This only takes effect when IndexWriter is first created.</p>*/
  public IndexWriterConfig setIndexerThreadPool(DocumentsWriterPerThreadPool threadPool) {
    if(threadPool == null) {
      throw new IllegalArgumentException("DocumentsWriterPerThreadPool must not be nul");
    }
    this.indexerThreadPool = threadPool;
    return this;
  }

  /** Returns the configured {@link DocumentsWriterPerThreadPool} instance.
   * @see #setIndexerThreadPool(DocumentsWriterPerThreadPool)
   * @return the configured {@link DocumentsWriterPerThreadPool} instance.*/
  public DocumentsWriterPerThreadPool getIndexerThreadPool() {
    return this.indexerThreadPool;
  }

  /** By default, IndexWriter does not pool the
   *  SegmentReaders it must open for deletions and
   *  merging, unless a near-real-time reader has been
   *  obtained by calling {@link IndexWriter#getReader}.
   *  This method lets you enable pooling without getting a
   *  near-real-time reader.  NOTE: if you set this to
   *  false, IndexWriter will still pool readers once
   *  {@link IndexWriter#getReader} is called.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setReaderPooling(boolean readerPooling) {
    this.readerPooling = readerPooling;
    return this;
  }

  /** Returns true if IndexWriter should pool readers even
   *  if {@link IndexWriter#getReader} has not been called. */
  public boolean getReaderPooling() {
    return readerPooling;
  }

  /** Expert: sets the {@link DocConsumer} chain to be used to process documents.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  IndexWriterConfig setIndexingChain(IndexingChain indexingChain) {
    this.indexingChain = indexingChain == null ? DocumentsWriterPerThread.defaultIndexingChain : indexingChain;
    return this;
  }

  /** Returns the indexing chain set on {@link #setIndexingChain(IndexingChain)}. */
  IndexingChain getIndexingChain() {
    return indexingChain;
  }

  /** Sets the termsIndexDivisor passed to any readers that
   *  IndexWriter opens, for example when applying deletes
   *  or creating a near-real-time reader in {@link
   *  IndexWriter#getReader}. If you pass -1, the terms index 
   *  won't be loaded by the readers. This is only useful in 
   *  advanced situations when you will only .next() through 
   *  all terms; attempts to seek will hit an exception.
   *
   * <p>Takes effect immediately, but only applies to
   * readers opened after this call */
  public IndexWriterConfig setReaderTermsIndexDivisor(int divisor) {
    if (divisor <= 0 && divisor != -1) {
      throw new IllegalArgumentException("divisor must be >= 1, or -1 (got " + divisor + ")");
    }
    readerTermsIndexDivisor = divisor;
    return this;
  }

  /** @see #setReaderTermsIndexDivisor(int) */
  public int getReaderTermsIndexDivisor() {
    return readerTermsIndexDivisor;
  }
  
  /**
   * Expert: Controls when segments are flushed to disk during indexing.
   * The {@link FlushPolicy} initialized during {@link IndexWriter} instantiation and once initialized
   * the given instance is bound to this {@link IndexWriter} and should not be used with another writer.
   * @see #setMaxBufferedDeleteTerms(int)
   * @see #setMaxBufferedDocs(int)
   * @see #setRAMBufferSizeMB(double)
   */
  public IndexWriterConfig setFlushPolicy(FlushPolicy flushPolicy) {
    this.flushPolicy = flushPolicy;
    return this;
  }

  /**
   * Expert: Sets the maximum memory consumption per thread triggering a forced
   * flush if exceeded. A {@link DocumentsWriterPerThread} is forcefully flushed
   * once it exceeds this limit even if the {@link #getRAMBufferSizeMB()} has
   * not been exceeded. This is a safety limit to prevent a
   * {@link DocumentsWriterPerThread} from address space exhaustion due to its
   * internal 32 bit signed integer based memory addressing.
   * The given value must be less that 2GB (2048MB)
   * 
   * @see #DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB
   */
  public IndexWriterConfig setRAMPerThreadHardLimitMB(int perThreadHardLimitMB) {
    if (perThreadHardLimitMB <= 0 || perThreadHardLimitMB >= 2048) {
      throw new IllegalArgumentException("PerThreadHardLimit must be greater than 0 and less than 2048MB");
    }
    this.perThreadHardLimitMB = perThreadHardLimitMB;
    return this;
  }

  /**
   * Returns the max amount of memory each {@link DocumentsWriterPerThread} can
   * consume until forcefully flushed.
   * @see #setRAMPerThreadHardLimitMB(int) 
   */
  public int getRAMPerThreadHardLimitMB() {
    return perThreadHardLimitMB;
  }
  /**
   * @see #setFlushPolicy(FlushPolicy)
   */
  public FlushPolicy getFlushPolicy() {
    return flushPolicy;
  }
  
  /**
   * @see #setInfoStream(InfoStream)
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  /** If non-null, information about merges, deletes and a
   * message when maxFieldLength is reached will be printed
   * to this.
   */
  public IndexWriterConfig setInfoStream(InfoStream infoStream) {
    if (infoStream == null) {
      throw new IllegalArgumentException("Cannot set InfoStream implementation to null. "+
        "To disable logging use InfoStream.NO_OUTPUT");
    }
    this.infoStream = infoStream;
    return this;
  }
  
  /**
   * Convenience method that uses {@link PrintStreamInfoStream}
   */
  public IndexWriterConfig setInfoStream(PrintStream printStream) {
    return setInfoStream(printStream == null ? InfoStream.NO_OUTPUT : new PrintStreamInfoStream(printStream));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("matchVersion=").append(matchVersion).append("\n");
    sb.append("analyzer=").append(analyzer == null ? "null" : analyzer.getClass().getName()).append("\n");
    sb.append("delPolicy=").append(delPolicy.getClass().getName()).append("\n");
    sb.append("commit=").append(commit == null ? "null" : commit).append("\n");
    sb.append("openMode=").append(openMode).append("\n");
    sb.append("similarity=").append(similarity.getClass().getName()).append("\n");
    sb.append("termIndexInterval=").append(termIndexInterval).append("\n"); // TODO: this should be private to the codec, not settable here
    sb.append("mergeScheduler=").append(mergeScheduler.getClass().getName()).append("\n");
    sb.append("default WRITE_LOCK_TIMEOUT=").append(WRITE_LOCK_TIMEOUT).append("\n");
    sb.append("writeLockTimeout=").append(writeLockTimeout).append("\n");
    sb.append("maxBufferedDeleteTerms=").append(maxBufferedDeleteTerms).append("\n");
    sb.append("ramBufferSizeMB=").append(ramBufferSizeMB).append("\n");
    sb.append("maxBufferedDocs=").append(maxBufferedDocs).append("\n");
    sb.append("mergedSegmentWarmer=").append(mergedSegmentWarmer).append("\n");
    sb.append("codec=").append(codec).append("\n");
    sb.append("infoStream=").append(infoStream.getClass().getName()).append("\n");
    sb.append("mergePolicy=").append(mergePolicy).append("\n");
    sb.append("indexerThreadPool=").append(indexerThreadPool).append("\n");
    sb.append("readerPooling=").append(readerPooling).append("\n");
    sb.append("readerTermsIndexDivisor=").append(readerTermsIndexDivisor).append("\n");
    sb.append("flushPolicy=").append(flushPolicy).append("\n");
    sb.append("perThreadHardLimitMB=").append(perThreadHardLimitMB).append("\n");

    return sb.toString();
  }

}
