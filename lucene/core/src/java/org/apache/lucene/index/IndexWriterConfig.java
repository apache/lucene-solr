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

import java.io.PrintStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
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
public final class IndexWriterConfig extends LiveIndexWriterConfig implements Cloneable {

  /**
   * Specifies the open mode for {@link IndexWriter}.
   */
  public static enum OpenMode {
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

  /**
   * Creates a new config that with defaults that match the specified
   * {@link Version} as well as the default {@link
   * Analyzer}. If matchVersion is >= {@link
   * Version#LUCENE_32}, {@link TieredMergePolicy} is used
   * for merging; else {@link LogByteSizeMergePolicy}.
   * Note that {@link TieredMergePolicy} is free to select
   * non-contiguous merges, which means docIDs may not
   * remain monotonic over time.  If this is a problem you
   * should switch to {@link LogByteSizeMergePolicy} or
   * {@link LogDocMergePolicy}.
   */
  public IndexWriterConfig(Version matchVersion, Analyzer analyzer) {
    super(analyzer, matchVersion);
  }

  @Override
  public IndexWriterConfig clone() {
    try {
      IndexWriterConfig clone = (IndexWriterConfig) super.clone();
      
      // Mostly shallow clone, but do a deepish clone of
      // certain objects that have state that cannot be shared
      // across IW instances:
      clone.delPolicy = delPolicy.clone();
      clone.flushPolicy = flushPolicy.clone();
      clone.indexerThreadPool = indexerThreadPool.clone();
      // we clone the infoStream because some impls might have state variables
      // such as line numbers, message throughput, ...
      clone.infoStream = infoStream.clone();
      clone.mergePolicy = mergePolicy.clone();
      clone.mergeScheduler = mergeScheduler.clone();
      
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
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
   * <b>NOTE:</b> the deletion policy cannot be null.
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
   * opens the latest commit point.
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
   * <b>NOTE:</b> the similarity cannot be null.
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
   * <b>NOTE:</b> the merge scheduler cannot be null.
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
   * Sets the maximum time to wait for a write lock (in milliseconds) for this
   * instance. You can change the default value for all instances by calling
   * {@link #setDefaultWriteLockTimeout(long)}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setWriteLockTimeout(long writeLockTimeout) {
    this.writeLockTimeout = writeLockTimeout;
    return this;
  }

  @Override
  public long getWriteLockTimeout() {
    return writeLockTimeout;
  }

  /**
   * Expert: {@link MergePolicy} is invoked whenever there are changes to the
   * segments in the index. Its role is to select which merges to do, if any,
   * and return a {@link MergePolicy.MergeSpecification} describing the merges.
   * It also selects merges to do for forceMerge.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    if (mergePolicy == null) {
      throw new IllegalArgumentException("mergePolicy must not be null");
    }
    this.mergePolicy = mergePolicy;
    return this;
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
  IndexWriterConfig setIndexerThreadPool(DocumentsWriterPerThreadPool threadPool) {
    if (threadPool == null) {
      throw new IllegalArgumentException("threadPool must not be null");
    }
    this.indexerThreadPool = threadPool;
    return this;
  }

  @Override
  DocumentsWriterPerThreadPool getIndexerThreadPool() {
    return indexerThreadPool;
  }

  /**
   * Sets the max number of simultaneous threads that may be indexing documents
   * at once in IndexWriter. Values &lt; 1 are invalid and if passed
   * <code>maxThreadStates</code> will be set to
   * {@link #DEFAULT_MAX_THREAD_STATES}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setMaxThreadStates(int maxThreadStates) {
    this.indexerThreadPool = new ThreadAffinityDocumentsWriterThreadPool(maxThreadStates);
    return this;
  }

  @Override
  public int getMaxThreadStates() {
    try {
      return ((ThreadAffinityDocumentsWriterThreadPool) indexerThreadPool).getMaxThreadStates();
    } catch (ClassCastException cce) {
      throw new IllegalStateException(cce);
    }
  }

  /** By default, IndexWriter does not pool the
   *  SegmentReaders it must open for deletions and
   *  merging, unless a near-real-time reader has been
   *  obtained by calling {@link DirectoryReader#open(IndexWriter, boolean)}.
   *  This method lets you enable pooling without getting a
   *  near-real-time reader.  NOTE: if you set this to
   *  false, IndexWriter will still pool readers once
   *  {@link DirectoryReader#open(IndexWriter, boolean)} is called.
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

  /** Expert: sets the {@link DocConsumer} chain to be used to process documents.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  IndexWriterConfig setIndexingChain(IndexingChain indexingChain) {
    if (indexingChain == null) {
      throw new IllegalArgumentException("indexingChain must not be null");
    }
    this.indexingChain = indexingChain;
    return this;
  }

  @Override
  IndexingChain getIndexingChain() {
    return indexingChain;
  }

  /**
   * Expert: Controls when segments are flushed to disk during indexing.
   * The {@link FlushPolicy} initialized during {@link IndexWriter} instantiation and once initialized
   * the given instance is bound to this {@link IndexWriter} and should not be used with another writer.
   * @see #setMaxBufferedDeleteTerms(int)
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
  public int getMaxBufferedDeleteTerms() {
    return super.getMaxBufferedDeleteTerms();
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
  
  @Override
  public int getReaderTermsIndexDivisor() {
    return super.getReaderTermsIndexDivisor();
  }
  
  @Override
  public int getTermIndexInterval() {
    return super.getTermIndexInterval();
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
  
  /** Convenience method that uses {@link PrintStreamInfoStream} */
  public IndexWriterConfig setInfoStream(PrintStream printStream) {
    if (printStream == null) {
      throw new IllegalArgumentException("printStream must not be null");
    }
    return setInfoStream(new PrintStreamInfoStream(printStream));
  }
  
  @Override
  public IndexWriterConfig setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    return (IndexWriterConfig) super.setMaxBufferedDeleteTerms(maxBufferedDeleteTerms);
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
  public IndexWriterConfig setReaderTermsIndexDivisor(int divisor) {
    return (IndexWriterConfig) super.setReaderTermsIndexDivisor(divisor);
  }
  
  @Override
  public IndexWriterConfig setTermIndexInterval(int interval) {
    return (IndexWriterConfig) super.setTermIndexInterval(interval);
  }

}
