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


import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

/**
 * Holds all the configuration used by {@link IndexWriter} with few setters for
 * settings that can be changed on an {@link IndexWriter} instance "live".
 * 
 * @since 4.0
 */
public class LiveIndexWriterConfig {
  
  private final Analyzer analyzer;
  
  private volatile int maxBufferedDocs;
  private volatile double ramBufferSizeMB;
  private volatile IndexReaderWarmer mergedSegmentWarmer;

  // modified by IndexWriterConfig
  /** {@link IndexDeletionPolicy} controlling when commit
   *  points are deleted. */
  protected volatile IndexDeletionPolicy delPolicy;

  /** {@link IndexCommit} that {@link IndexWriter} is
   *  opened on. */
  protected volatile IndexCommit commit;

  /** {@link OpenMode} that {@link IndexWriter} is opened
   *  with. */
  protected volatile OpenMode openMode;

  /** Compatibility version to use for this index. */
  protected int createdVersionMajor = Version.LATEST.major;

  /** {@link Similarity} to use when encoding norms. */
  protected volatile Similarity similarity;

  /** {@link MergeScheduler} to use for running merges. */
  protected volatile MergeScheduler mergeScheduler;

  /** {@link IndexingChain} that determines how documents are
   *  indexed. */
  protected volatile IndexingChain indexingChain;

  /** {@link Codec} used to write new segments. */
  protected volatile Codec codec;

  /** {@link InfoStream} for debugging messages. */
  protected volatile InfoStream infoStream;

  /** {@link MergePolicy} for selecting merges. */
  protected volatile MergePolicy mergePolicy;

  /** True if readers should be pooled. */
  protected volatile boolean readerPooling;

  /** {@link FlushPolicy} to control when segments are
   *  flushed. */
  protected volatile FlushPolicy flushPolicy;

  /** Sets the hard upper bound on RAM usage for a single
   *  segment, after which the segment is forced to flush. */
  protected volatile int perThreadHardLimitMB;

  /** True if segment flushes should use compound file format */
  protected volatile boolean useCompoundFile = IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM;
  
  /** True if calls to {@link IndexWriter#close()} should first do a commit. */
  protected boolean commitOnClose = IndexWriterConfig.DEFAULT_COMMIT_ON_CLOSE;

  /** The sort order to use to write merged segments. */
  protected Sort indexSort = null;

  /** The comparator for sorting leaf readers. */
  protected Comparator<LeafReader> leafSorter;

  /** The field names involved in the index sort */
  protected Set<String> indexSortFields = Collections.emptySet();

  /** if an indexing thread should check for pending flushes on update in order to help out on a full flush*/
  protected volatile boolean checkPendingFlushOnUpdate = true;

  /** soft deletes field */
  protected String softDeletesField = null;

  /** Amount of time to wait for merges returned by MergePolicy.findFullFlushMerges(...) */
  protected volatile long maxFullFlushMergeWaitMillis;

  // used by IndexWriterConfig
  LiveIndexWriterConfig(Analyzer analyzer) {
    this.analyzer = analyzer;
    ramBufferSizeMB = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
    maxBufferedDocs = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;
    mergedSegmentWarmer = null;
    delPolicy = new KeepOnlyLastCommitDeletionPolicy();
    commit = null;
    useCompoundFile = IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM;
    openMode = OpenMode.CREATE_OR_APPEND;
    similarity = IndexSearcher.getDefaultSimilarity();
    mergeScheduler = new ConcurrentMergeScheduler();
    indexingChain = DocumentsWriterPerThread.defaultIndexingChain;
    codec = Codec.getDefault();
    if (codec == null) {
      throw new NullPointerException();
    }
    infoStream = InfoStream.getDefault();
    mergePolicy = new TieredMergePolicy();
    flushPolicy = new FlushByRamOrCountsPolicy();
    readerPooling = IndexWriterConfig.DEFAULT_READER_POOLING;
    perThreadHardLimitMB = IndexWriterConfig.DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB;
    maxFullFlushMergeWaitMillis = IndexWriterConfig.DEFAULT_MAX_FULL_FLUSH_MERGE_WAIT_MILLIS;
  }
  
  /** Returns the default analyzer to use for indexing documents. */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /**
   * Determines the amount of RAM that may be used for buffering added documents
   * and deletions before they are flushed to the Directory. Generally for
   * faster indexing performance it's best to flush by RAM usage instead of
   * document count and use as large a RAM buffer as you can.
   * <p>
   * When this is set, the writer will flush whenever buffered documents and
   * deletions use this much RAM. Pass in
   * {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} to prevent triggering a flush
   * due to RAM usage. Note that if flushing by document count is also enabled,
   * then the flush will be triggered by whichever comes first.
   * <p>
   * The maximum RAM limit is inherently determined by the JVMs available
   * memory. Yet, an {@link IndexWriter} session can consume a significantly
   * larger amount of memory than the given RAM limit since this limit is just
   * an indicator when to flush memory resident documents to the Directory.
   * Flushes are likely happen concurrently while other threads adding documents
   * to the writer. For application stability the available memory in the JVM
   * should be significantly larger than the RAM buffer used for indexing.
   * <p>
   * <b>NOTE</b>: the account of RAM usage for pending deletions is only
   * approximate. Specifically, if you delete by Query, Lucene currently has no
   * way to measure the RAM usage of individual Queries so the accounting will
   * under-estimate and you should compensate by either calling commit() or refresh()
   * periodically yourself.
   * <p>
   * <b>NOTE</b>: It's not guaranteed that all memory resident documents are
   * flushed once this limit is exceeded. Depending on the configured
   * {@link FlushPolicy} only a subset of the buffered documents are flushed and
   * therefore only parts of the RAM buffer is released.
   * <p>
   * 
   * The default value is {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB}.
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted.
   * 
   * @see IndexWriterConfig#setRAMPerThreadHardLimitMB(int)
   * 
   * @throws IllegalArgumentException
   *           if ramBufferSize is enabled but non-positive, or it disables
   *           ramBufferSize when maxBufferedDocs is already disabled
   */
  public synchronized LiveIndexWriterConfig setRAMBufferSizeMB(double ramBufferSizeMB) {
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && ramBufferSizeMB <= 0.0) {
      throw new IllegalArgumentException("ramBufferSize should be > 0.0 MB when enabled");
    }
    if (ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && maxBufferedDocs == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      throw new IllegalArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
    }
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
   * documents. Pass in {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} to prevent
   * triggering a flush due to number of buffered documents. Note that if
   * flushing by RAM usage is also enabled, then the flush will be triggered by
   * whichever comes first.
   * 
   * <p>
   * Disabled by default (writer flushes by RAM usage).
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted.
   * 
   * @see #setRAMBufferSizeMB(double)
   * @throws IllegalArgumentException
   *           if maxBufferedDocs is enabled but smaller than 2, or it disables
   *           maxBufferedDocs when ramBufferSize is already disabled
   */
  public synchronized LiveIndexWriterConfig setMaxBufferedDocs(int maxBufferedDocs) {
    if (maxBufferedDocs != IndexWriterConfig.DISABLE_AUTO_FLUSH && maxBufferedDocs < 2) {
      throw new IllegalArgumentException("maxBufferedDocs must at least be 2 when enabled");
    }
    if (maxBufferedDocs == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      throw new IllegalArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
    }
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

  /**
   * Expert: {@link MergePolicy} is invoked whenever there are changes to the
   * segments in the index. Its role is to select which merges to do, if any,
   * and return a {@link MergePolicy.MergeSpecification} describing the merges.
   * It also selects merges to do for forceMerge.
   * 
   * <p>
   * Takes effect on subsequent merge selections. Any merges in flight or any
   * merges already registered by the previous {@link MergePolicy} are not
   * affected.
   */
  public LiveIndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    if (mergePolicy == null) {
      throw new IllegalArgumentException("mergePolicy must not be null");
    }
    this.mergePolicy = mergePolicy;
    return this;
  }

  /**
   * Set the merged segment warmer. See {@link IndexReaderWarmer}.
   * 
   * <p>
   * Takes effect on the next merge.
   */
  public LiveIndexWriterConfig setMergedSegmentWarmer(IndexReaderWarmer mergeSegmentWarmer) {
    this.mergedSegmentWarmer = mergeSegmentWarmer;
    return this;
  }

  /** Returns the current merged segment warmer. See {@link IndexReaderWarmer}. */
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return mergedSegmentWarmer;
  }
  
  /** Returns the {@link OpenMode} set by {@link IndexWriterConfig#setOpenMode(OpenMode)}. */
  public OpenMode getOpenMode() {
    return openMode;
  }

  /**
   * Return the compatibility version to use for this index.
   * @see IndexWriterConfig#setIndexCreatedVersionMajor
   */
  public int getIndexCreatedVersionMajor() {
    return createdVersionMajor;
  }

  /**
   * Returns the {@link IndexDeletionPolicy} specified in
   * {@link IndexWriterConfig#setIndexDeletionPolicy(IndexDeletionPolicy)} or
   * the default {@link KeepOnlyLastCommitDeletionPolicy}/
   */
  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return delPolicy;
  }
  
  /**
   * Returns the {@link IndexCommit} as specified in
   * {@link IndexWriterConfig#setIndexCommit(IndexCommit)} or the default,
   * {@code null} which specifies to open the latest index commit point.
   */
  public IndexCommit getIndexCommit() {
    return commit;
  }

  /**
   * Expert: returns the {@link Similarity} implementation used by this
   * {@link IndexWriter}.
   */
  public Similarity getSimilarity() {
    return similarity;
  }
  
  /**
   * Returns the {@link MergeScheduler} that was set by
   * {@link IndexWriterConfig#setMergeScheduler(MergeScheduler)}.
   */
  public MergeScheduler getMergeScheduler() {
    return mergeScheduler;
  }
  
  /** Returns the current {@link Codec}. */
  public Codec getCodec() {
    return codec;
  }

  /**
   * Returns the current MergePolicy in use by this writer.
   *
   * @see IndexWriterConfig#setMergePolicy(MergePolicy)
   */
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }
  
  /**
   * Returns {@code true} if {@link IndexWriter} should pool readers even if
   * {@link DirectoryReader#open(IndexWriter)} has not been called.
   */
  public boolean getReaderPooling() {
    return readerPooling;
  }

  /**
   * Returns the indexing chain.
   */
  IndexingChain getIndexingChain() {
    return indexingChain;
  }

  /**
   * Returns the max amount of memory each {@link DocumentsWriterPerThread} can
   * consume until forcefully flushed.
   * 
   * @see IndexWriterConfig#setRAMPerThreadHardLimitMB(int)
   */
  public int getRAMPerThreadHardLimitMB() {
    return perThreadHardLimitMB;
  }
  
  /**
   * @see IndexWriterConfig#setFlushPolicy(FlushPolicy)
   */
  FlushPolicy getFlushPolicy() {
    return flushPolicy;
  }
  
  /** Returns {@link InfoStream} used for debugging.
   *
   * @see IndexWriterConfig#setInfoStream(InfoStream)
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  /**
   * Sets if the {@link IndexWriter} should pack newly written segments in a
   * compound file. Default is <code>true</code>.
   * <p>
   * Use <code>false</code> for batch indexing with very large ram buffer
   * settings.
   * </p>
   * <p>
   * <b>Note: To control compound file usage during segment merges see
   * {@link MergePolicy#setNoCFSRatio(double)} and
   * {@link MergePolicy#setMaxCFSSegmentSizeMB(double)}. This setting only
   * applies to newly created segments.</b>
   * </p>
   */
  public LiveIndexWriterConfig setUseCompoundFile(boolean useCompoundFile) {
    this.useCompoundFile = useCompoundFile;
    return this;
  }
  
  /**
   * Returns <code>true</code> iff the {@link IndexWriter} packs
   * newly written segments in a compound file. Default is <code>true</code>.
   */
  public boolean getUseCompoundFile() {
    return useCompoundFile ;
  }
  
  /**
   * Returns <code>true</code> if {@link IndexWriter#close()} should first commit before closing.
   */
  public boolean getCommitOnClose() {
    return commitOnClose;
  }

  /**
   * Get the index-time {@link Sort} order, applied to all (flushed and merged) segments.
   */
  public Sort getIndexSort() {
    return indexSort;
  }

  /**
   * Returns the field names involved in the index sort
   */
  public Set<String> getIndexSortFields() {
    return indexSortFields;
  }

  /**
   * Returns a comparator for sorting leaf readers. If not {@code null}, this comparator is used to
   * sort leaf readers within {@code DirectoryReader} opened from the {@code IndexWriter} of this
   * configuration.
   *
   * @return a comparator for sorting leaf readers
   */
  public Comparator<LeafReader> getLeafSorter() {
    return leafSorter;
  }

  /**
   * Expert: Returns if indexing threads check for pending flushes on update in order
   * to help our flushing indexing buffers to disk
   * @lucene.experimental
   */
  public boolean isCheckPendingFlushOnUpdate() {
    return checkPendingFlushOnUpdate;
  }

  /**
   * Expert: sets if indexing threads check for pending flushes on update in order
   * to help our flushing indexing buffers to disk. As a consequence, threads calling
   * {@link DirectoryReader#openIfChanged(DirectoryReader, IndexWriter)} or {@link IndexWriter#flush()} will
   * be the only thread writing segments to disk unless flushes are falling behind. If indexing is stalled
   * due to too many pending flushes indexing threads will help our writing pending segment flushes to disk.
   *
   * @lucene.experimental
   */
  public LiveIndexWriterConfig setCheckPendingFlushUpdate(boolean checkPendingFlushOnUpdate) {
    this.checkPendingFlushOnUpdate = checkPendingFlushOnUpdate;
    return this;
  }

  /**
   * Returns the soft deletes field or <code>null</code> if soft-deletes are disabled.
   * See {@link IndexWriterConfig#setSoftDeletesField(String)} for details.
   */
  public String getSoftDeletesField() {
    return softDeletesField;
  }

  /**
   * Expert: return the amount of time to wait for merges returned by by MergePolicy.findFullFlushMerges(...).
   * If this time is reached, we proceed with the commit based on segments merged up to that point.
   * The merges are not cancelled, and may still run to completion independent of the commit.
   */
  public long getMaxFullFlushMergeWaitMillis() {
    return maxFullFlushMergeWaitMillis;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("analyzer=").append(analyzer == null ? "null" : analyzer.getClass().getName()).append("\n");
    sb.append("ramBufferSizeMB=").append(getRAMBufferSizeMB()).append("\n");
    sb.append("maxBufferedDocs=").append(getMaxBufferedDocs()).append("\n");
    sb.append("mergedSegmentWarmer=").append(getMergedSegmentWarmer()).append("\n");
    sb.append("delPolicy=").append(getIndexDeletionPolicy().getClass().getName()).append("\n");
    IndexCommit commit = getIndexCommit();
    sb.append("commit=").append(commit == null ? "null" : commit).append("\n");
    sb.append("openMode=").append(getOpenMode()).append("\n");
    sb.append("similarity=").append(getSimilarity().getClass().getName()).append("\n");
    sb.append("mergeScheduler=").append(getMergeScheduler()).append("\n");
    sb.append("codec=").append(getCodec()).append("\n");
    sb.append("infoStream=").append(getInfoStream().getClass().getName()).append("\n");
    sb.append("mergePolicy=").append(getMergePolicy()).append("\n");
    sb.append("readerPooling=").append(getReaderPooling()).append("\n");
    sb.append("perThreadHardLimitMB=").append(getRAMPerThreadHardLimitMB()).append("\n");
    sb.append("useCompoundFile=").append(getUseCompoundFile()).append("\n");
    sb.append("commitOnClose=").append(getCommitOnClose()).append("\n");
    sb.append("indexSort=").append(getIndexSort()).append("\n");
    sb.append("checkPendingFlushOnUpdate=").append(isCheckPendingFlushOnUpdate()).append("\n");
    sb.append("softDeletesField=").append(getSoftDeletesField()).append("\n");
    sb.append("maxFullFlushMergeWaitMillis=").append(getMaxFullFlushMergeWaitMillis()).append("\n");
    sb.append("leafSorter=").append(getLeafSorter()).append("\n");
    return sb.toString();
  }
}
