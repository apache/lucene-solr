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


import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.apache.lucene.util.Version;

/**
 * Holds all the configuration that is used to create an {@link IndexWriter}.
 * Once {@link IndexWriter} has been created with this object, changes to this
 * object will not affect the {@link IndexWriter} instance. For that, use
 * {@link LiveIndexWriterConfig} that is returned from {@link IndexWriter#getConfig()}.
 * 
 * <p>
 * All setter methods return {@link IndexWriterConfig} to allow chaining
 * settings conveniently, for example:
 * 
 * <pre class="prettyprint">
 * IndexWriterConfig conf = new IndexWriterConfig(analyzer);
 * conf.setter1().setter2();
 * </pre>
 * 
 * @see IndexWriter#getConfig()
 * 
 * @since 3.1
 */
public final class IndexWriterConfig extends LiveIndexWriterConfig {

  /**
   * Specifies the open mode for {@link IndexWriter}.
   */
  public enum OpenMode {
    /** 
     * Creates a new index or overwrites an existing one. 
     */
    CREATE,
    
    /** 
     * Opens an existing index. 
     */
    APPEND,
    
    /** 
     * Creates a new index if one does not exist,
     * otherwise it opens the index and documents will be appended. 
     */
    CREATE_OR_APPEND 
  }

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

  /** Default setting (true) for {@link #setReaderPooling}. */
  // We changed this default to true with concurrent deletes/updates (LUCENE-7868),
  // because we will otherwise need to open and close segment readers more frequently.
  // False is still supported, but will have worse performance since readers will
  // be forced to aggressively move all state to disk.
  public final static boolean DEFAULT_READER_POOLING = true;

  /** Default value is 1945. Change using {@link #setRAMPerThreadHardLimitMB(int)} */
  public static final int DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB = 1945;
  
  /** Default value for compound file system for newly written segments
   *  (set to <code>true</code>). For batch indexing with very large 
   *  ram buffers use <code>false</code> */
  public final static boolean DEFAULT_USE_COMPOUND_FILE_SYSTEM = true;
  
  /** Default value for whether calls to {@link IndexWriter#close()} include a commit. */
  public final static boolean DEFAULT_COMMIT_ON_CLOSE = true;

  /** Default value for time to wait for merges on commit or getReader (when using a {@link MergePolicy} that implements {@link MergePolicy#findFullFlushMerges}). */
  public static final long DEFAULT_MAX_FULL_FLUSH_MERGE_WAIT_MILLIS = 0;
  
  // indicates whether this config instance is already attached to a writer.
  // not final so that it can be cloned properly.
  private SetOnce<IndexWriter> writer = new SetOnce<>();
  
  /**
   * Sets the {@link IndexWriter} this config is attached to.
   * 
   * @throws AlreadySetException
   *           if this config is already attached to a writer.
   */
  IndexWriterConfig setIndexWriter(IndexWriter writer) {
    if (this.writer.get() != null) {
      throw new IllegalStateException("do not share IndexWriterConfig instances across IndexWriters");
    }
    this.writer.set(writer);
    return this;
  }
  
  /**
   * Creates a new config, using {@link StandardAnalyzer} as the
   * analyzer.  By default, {@link TieredMergePolicy} is used
   * for merging;
   * Note that {@link TieredMergePolicy} is free to select
   * non-contiguous merges, which means docIDs may not
   * remain monotonic over time.  If this is a problem you
   * should switch to {@link LogByteSizeMergePolicy} or
   * {@link LogDocMergePolicy}.
   */
  public IndexWriterConfig() {
    this(new StandardAnalyzer());
  }
  
  /**
   * Creates a new config that with the provided {@link
   * Analyzer}. By default, {@link TieredMergePolicy} is used
   * for merging;
   * Note that {@link TieredMergePolicy} is free to select
   * non-contiguous merges, which means docIDs may not
   * remain monotonic over time.  If this is a problem you
   * should switch to {@link LogByteSizeMergePolicy} or
   * {@link LogDocMergePolicy}.
   */
  public IndexWriterConfig(Analyzer analyzer) {
    super(analyzer);
  }

  /** Specifies {@link OpenMode} of the index.
   * 
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setOpenMode(OpenMode openMode) {
    if (openMode == null) {
      throw new IllegalArgumentException("openMode must not be null");
    }
    this.openMode = openMode;
    return this;
  }

  @Override
  public OpenMode getOpenMode() {
    return openMode;
  }

  /**
   * Expert: set the compatibility version to use for this index. In case the
   * index is created, it will use the given major version for compatibility.
   * It is sometimes useful to set the previous major version for compatibility
   * due to the fact that {@link IndexWriter#addIndexes} only accepts indices
   * that have been written with the same major version as the current index.
   * If the index already exists, then this value is ignored.
   * Default value is the {@link Version#major major} of the
   * {@link Version#LATEST latest version}.
   * <p><b>NOTE</b>: Changing the creation version reduces backward
   * compatibility guarantees. For instance an index created with Lucene 8 with
   * a compatibility version of 7 can't be read with Lucene 9 due to the fact
   * that Lucene only supports reading indices created with the current or
   * previous major release.
   * @param indexCreatedVersionMajor the major version to use for compatibility
   */
  public IndexWriterConfig setIndexCreatedVersionMajor(int indexCreatedVersionMajor) {
    if (indexCreatedVersionMajor > Version.LATEST.major) {
      throw new IllegalArgumentException("indexCreatedVersionMajor may not be in the future: current major version is " +
          Version.LATEST.major + ", but got: " + indexCreatedVersionMajor);
    }
    if (indexCreatedVersionMajor < Version.LATEST.major - 1) {
      throw new IllegalArgumentException("indexCreatedVersionMajor may not be less than the minimum supported version: " +
          (Version.LATEST.major-1) + ", but got: " + indexCreatedVersionMajor);
    }
    this.createdVersionMajor = indexCreatedVersionMajor;
    return this;
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
   * <b>NOTE:</b> the deletion policy must not be null.
   *
   * <p>Only takes effect when IndexWriter is first created. 
   */
  public IndexWriterConfig setIndexDeletionPolicy(IndexDeletionPolicy delPolicy) {
    if (delPolicy == null) {
      throw new IllegalArgumentException("indexDeletionPolicy must not be null");
    }
    this.delPolicy = delPolicy;
    return this;
  }

  @Override
  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return delPolicy;
  }

  /**
   * Expert: allows to open a certain commit point. The default is null which
   * opens the latest commit point.  This can also be used to open {@link IndexWriter}
   * from a near-real-time reader, if you pass the reader's
   * {@link DirectoryReader#getIndexCommit}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setIndexCommit(IndexCommit commit) {
    this.commit = commit;
    return this;
  }

  @Override
  public IndexCommit getIndexCommit() {
    return commit;
  }

  /**
   * Expert: set the {@link Similarity} implementation used by this IndexWriter.
   * <p>
   * <b>NOTE:</b> the similarity must not be null.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setSimilarity(Similarity similarity) {
    if (similarity == null) {
      throw new IllegalArgumentException("similarity must not be null");
    }
    this.similarity = similarity;
    return this;
  }

  @Override
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Expert: sets the merge scheduler used by this writer. The default is
   * {@link ConcurrentMergeScheduler}.
   * <p>
   * <b>NOTE:</b> the merge scheduler must not be null.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setMergeScheduler(MergeScheduler mergeScheduler) {
    if (mergeScheduler == null) {
      throw new IllegalArgumentException("mergeScheduler must not be null");
    }
    this.mergeScheduler = mergeScheduler;
    return this;
  }

  @Override
  public MergeScheduler getMergeScheduler() {
    return mergeScheduler;
  }

  /**
   * Set the {@link Codec}.
   * 
   * <p>
   * Only takes effect when IndexWriter is first created.
   */
  public IndexWriterConfig setCodec(Codec codec) {
    if (codec == null) {
      throw new IllegalArgumentException("codec must not be null");
    }
    this.codec = codec;
    return this;
  }

  @Override
  public Codec getCodec() {
    return codec;
  }


  @Override
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }

  /** By default, IndexWriter does not pool the
   *  SegmentReaders it must open for deletions and
   *  merging, unless a near-real-time reader has been
   *  obtained by calling {@link DirectoryReader#open(IndexWriter)}.
   *  This method lets you enable pooling without getting a
   *  near-real-time reader.  NOTE: if you set this to
   *  false, IndexWriter will still pool readers once
   *  {@link DirectoryReader#open(IndexWriter)} is called.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setReaderPooling(boolean readerPooling) {
    this.readerPooling = readerPooling;
    return this;
  }

  @Override
  public boolean getReaderPooling() {
    return readerPooling;
  }

  /**
   * Expert: Controls when segments are flushed to disk during indexing.
   * The {@link FlushPolicy} initialized during {@link IndexWriter} instantiation and once initialized
   * the given instance is bound to this {@link IndexWriter} and should not be used with another writer.
   * @see #setMaxBufferedDocs(int)
   * @see #setRAMBufferSizeMB(double)
   */
  IndexWriterConfig setFlushPolicy(FlushPolicy flushPolicy) {
    if (flushPolicy == null) {
      throw new IllegalArgumentException("flushPolicy must not be null");
    }
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

  @Override
  public int getRAMPerThreadHardLimitMB() {
    return perThreadHardLimitMB;
  }
  
  @Override
  FlushPolicy getFlushPolicy() {
    return flushPolicy;
  }
  
  @Override
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  @Override
  public Analyzer getAnalyzer() {
    return super.getAnalyzer();
  }
  
  @Override
  public int getMaxBufferedDocs() {
    return super.getMaxBufferedDocs();
  }
  
  @Override
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return super.getMergedSegmentWarmer();
  }
  
  @Override
  public double getRAMBufferSizeMB() {
    return super.getRAMBufferSizeMB();
  }
  
  /** 
   * Information about merges, deletes and a
   * message when maxFieldLength is reached will be printed
   * to this. Must not be null, but {@link InfoStream#NO_OUTPUT} 
   * may be used to suppress output.
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
   * Convenience method that uses {@link PrintStreamInfoStream}.  Must not be null.
   */
  public IndexWriterConfig setInfoStream(PrintStream printStream) {
    if (printStream == null) {
      throw new IllegalArgumentException("printStream must not be null");
    }
    return setInfoStream(new PrintStreamInfoStream(printStream));
  }
  
  @Override
  public IndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    return (IndexWriterConfig) super.setMergePolicy(mergePolicy);
  }
  
  @Override
  public IndexWriterConfig setMaxBufferedDocs(int maxBufferedDocs) {
    return (IndexWriterConfig) super.setMaxBufferedDocs(maxBufferedDocs);
  }
  
  @Override
  public IndexWriterConfig setMergedSegmentWarmer(IndexReaderWarmer mergeSegmentWarmer) {
    return (IndexWriterConfig) super.setMergedSegmentWarmer(mergeSegmentWarmer);
  }
  
  @Override
  public IndexWriterConfig setRAMBufferSizeMB(double ramBufferSizeMB) {
    return (IndexWriterConfig) super.setRAMBufferSizeMB(ramBufferSizeMB);
  }
  
  @Override
  public IndexWriterConfig setUseCompoundFile(boolean useCompoundFile) {
    return (IndexWriterConfig) super.setUseCompoundFile(useCompoundFile);
  }

  /**
   * Sets if calls {@link IndexWriter#close()} should first commit
   * before closing.  Use <code>true</code> to match behavior of Lucene 4.x.
   */
  public IndexWriterConfig setCommitOnClose(boolean commitOnClose) {
    this.commitOnClose = commitOnClose;
    return this;
  }

  /**
   * Expert: sets the amount of time to wait for merges (during {@link IndexWriter#commit}
   * or {@link IndexWriter#getReader(boolean, boolean)}) returned by
   * MergePolicy.findFullFlushMerges(...).
   * If this time is reached, we proceed with the commit based on segments merged up to that point.
   * The merges are not aborted, and will still run to completion independent of the commit or getReader call,
   * like natural segment merges. The default is <code>{@value IndexWriterConfig#DEFAULT_MAX_FULL_FLUSH_MERGE_WAIT_MILLIS}</code>.
   *
   * Note: This settings has no effect unless {@link MergePolicy#findFullFlushMerges(MergeTrigger, SegmentInfos, MergePolicy.MergeContext)}
   * has an implementation that actually returns merges which by default doesn't return any merges.
   */
  public IndexWriterConfig setMaxFullFlushMergeWaitMillis(long maxFullFlushMergeWaitMillis) {
    this.maxFullFlushMergeWaitMillis = maxFullFlushMergeWaitMillis;
    return this;
  }

  /**
   * Set the {@link Sort} order to use for all (flushed and merged) segments.
   */
  public IndexWriterConfig setIndexSort(Sort sort) {
    for (SortField sortField : sort.getSort()) {
      if (sortField.getIndexSorter() == null) {
        throw new IllegalArgumentException("Cannot sort index with sort field " + sortField);
      }
    }
    this.indexSort = sort;
    this.indexSortFields = Arrays.stream(sort.getSort()).map(SortField::getField).collect(Collectors.toSet());
    return this;
  }

  /**
   * Set the comparator for sorting leaf readers. A DirectoryReader opened from a IndexWriter with
   * this configuration will have its leaf readers sorted with the provided leaf sorter.
   *
   * @param leafSorter – a comparator for sorting leaf readers
   * @return IndexWriterConfig with leafSorter set.
   */
  public IndexWriterConfig setLeafSorter(Comparator<LeafReader> leafSorter) {
    this.leafSorter = leafSorter;
    return this;
  }

    @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append("writer=").append(writer.get()).append("\n");
    return sb.toString();
  }

  @Override
  public IndexWriterConfig setCheckPendingFlushUpdate(boolean checkPendingFlushOnUpdate) {
    return (IndexWriterConfig) super.setCheckPendingFlushUpdate(checkPendingFlushOnUpdate);
  }

  /**
   * Sets the soft deletes field. A soft delete field in lucene is a doc-values field that marks a document as soft-deleted if a
   * document has at least one value in that field. If a document is marked as soft-deleted the document is treated as
   * if it has been hard-deleted through the IndexWriter API ({@link IndexWriter#deleteDocuments(Term...)}.
   * Merges will reclaim soft-deleted as well as hard-deleted documents and index readers obtained from the IndexWriter
   * will reflect all deleted documents in it's live docs. If soft-deletes are used documents must be indexed via
   * {@link IndexWriter#softUpdateDocument(Term, Iterable, Field...)}. Deletes are applied via
   * {@link IndexWriter#updateDocValues(Term, Field...)}.
   *
   * Soft deletes allow to retain documents across merges if the merge policy modifies the live docs of a merge reader.
   * {@link SoftDeletesRetentionMergePolicy} for instance allows to specify an arbitrary query to mark all documents
   * that should survive the merge. This can be used to for example keep all document modifications for a certain time
   * interval or the last N operations if some kind of sequence ID is available in the index.
   *
   * Currently there is no API support to un-delete a soft-deleted document. In oder to un-delete the document must be
   * re-indexed using {@link IndexWriter#softUpdateDocument(Term, Iterable, Field...)}.
   *
   * The default value for this is <code>null</code> which disables soft-deletes. If soft-deletes are enabled documents
   * can still be hard-deleted. Hard-deleted documents will won't considered as soft-deleted even if they have
   * a value in the soft-deletes field.
   *
   * @see #getSoftDeletesField()
   */
  public IndexWriterConfig setSoftDeletesField(String softDeletesField) {
    this.softDeletesField = softDeletesField;
    return this;
  }
}
