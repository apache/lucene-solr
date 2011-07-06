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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.search.FieldCache; // javadocs
import org.apache.lucene.search.Similarity;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.store.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;         // for javadocs

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** IndexReader is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable.

 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link
 #open(Directory, boolean)}.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral--they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 <p> An IndexReader can be opened on a directory for which an IndexWriter is
 opened already, but it cannot be used to delete documents from the index then.

 <p>
 <b>NOTE</b>: for backwards API compatibility, several methods are not listed 
 as abstract, but have no useful implementations in this base class and 
 instead always throw UnsupportedOperationException.  Subclasses are 
 strongly encouraged to override these methods, but in many cases may not 
 need to.
 </p>

 <p>

 <b>NOTE</b>: as of 2.4, it's possible to open a read-only
 IndexReader using the static open methods that accept the 
 boolean readOnly parameter.  Such a reader may have better 
 concurrency.  You must specify false if you want to 
 make changes with the resulting IndexReader.
 </p>

 <a name="thread-safety"></a><p><b>NOTE</b>: {@link
 IndexReader} instances are completely thread
 safe, meaning multiple threads can call any of its methods,
 concurrently.  If your application requires external
 synchronization, you should <b>not</b> synchronize on the
 <code>IndexReader</code> instance; use your own
 (non-Lucene) objects instead.
*/
public abstract class IndexReader implements Cloneable,Closeable {

  /**
   * A custom listener that's invoked when the IndexReader
   * is finished.
   *
   * <p>For a SegmentReader, this listener is called only
   * once all SegmentReaders sharing the same core are
   * closed.  At this point it is safe for apps to evict
   * this reader from any caches keyed on {@link
   * #getCoreCacheKey}.  This is the same interface that
   * {@link FieldCache} uses, internally, to evict
   * entries.</p>
   *
   * <p>For other readers, this listener is called when they
   * are closed.</p>
   *
   * @lucene.experimental
   */
  public static interface ReaderFinishedListener {
    public void finished(IndexReader reader);
  }

  // Impls must set this if they may call add/removeReaderFinishedListener:
  protected volatile Collection<ReaderFinishedListener> readerFinishedListeners;

  /** Expert: adds a {@link ReaderFinishedListener}.  The
   * provided listener is also added to any sub-readers, if
   * this is a composite reader.  Also, any reader reopened
   * or cloned from this one will also copy the listeners at
   * the time of reopen.
   *
   * @lucene.experimental */
  public void addReaderFinishedListener(ReaderFinishedListener listener) {
    readerFinishedListeners.add(listener);
  }

  /** Expert: remove a previously added {@link ReaderFinishedListener}.
   *
   * @lucene.experimental */
  public void removeReaderFinishedListener(ReaderFinishedListener listener) {
    readerFinishedListeners.remove(listener);
  }

  protected void notifyReaderFinishedListeners() {
    // Defensive (should never be null -- all impls must set
    // this):
    if (readerFinishedListeners != null) {
      for(ReaderFinishedListener listener : readerFinishedListeners) {
        listener.finished(this);
      }
    }
  }

  protected void readerFinished() {
    notifyReaderFinishedListeners();
  }

  /**
   * Constants describing field properties, for example used for
   * {@link IndexReader#getFieldNames(FieldOption)}.
   */
  public static final class FieldOption {
    private String option;
    private FieldOption() { }
    private FieldOption(String option) {
      this.option = option;
    }
    @Override
    public String toString() {
      return this.option;
    }
    /** All fields */
    public static final FieldOption ALL = new FieldOption ("ALL");
    /** All indexed fields */
    public static final FieldOption INDEXED = new FieldOption ("INDEXED");
    /** All fields that store payloads */
    public static final FieldOption STORES_PAYLOADS = new FieldOption ("STORES_PAYLOADS");
    /** All fields that omit tf */
    public static final FieldOption OMIT_TERM_FREQ_AND_POSITIONS = new FieldOption ("OMIT_TERM_FREQ_AND_POSITIONS");
    /** All fields which are not indexed */
    public static final FieldOption UNINDEXED = new FieldOption ("UNINDEXED");
    /** All fields which are indexed with termvectors enabled */
    public static final FieldOption INDEXED_WITH_TERMVECTOR = new FieldOption ("INDEXED_WITH_TERMVECTOR");
    /** All fields which are indexed but don't have termvectors enabled */
    public static final FieldOption INDEXED_NO_TERMVECTOR = new FieldOption ("INDEXED_NO_TERMVECTOR");
    /** All fields with termvectors enabled. Please note that only standard termvector fields are returned */
    public static final FieldOption TERMVECTOR = new FieldOption ("TERMVECTOR");
    /** All fields with termvectors with position values enabled */
    public static final FieldOption TERMVECTOR_WITH_POSITION = new FieldOption ("TERMVECTOR_WITH_POSITION");
    /** All fields with termvectors with offset values enabled */
    public static final FieldOption TERMVECTOR_WITH_OFFSET = new FieldOption ("TERMVECTOR_WITH_OFFSET");
    /** All fields with termvectors with offset values and position values enabled */
    public static final FieldOption TERMVECTOR_WITH_POSITION_OFFSET = new FieldOption ("TERMVECTOR_WITH_POSITION_OFFSET");
    /** All fields holding doc values */
    public static final FieldOption DOC_VALUES = new FieldOption ("DOC_VALUES");

  }

  private boolean closed;
  protected boolean hasChanges;
  
  private final AtomicInteger refCount = new AtomicInteger();

  static int DEFAULT_TERMS_INDEX_DIVISOR = 1;

  /** Expert: returns the current refCount for this reader */
  public int getRefCount() {
    return refCount.get();
  }
  
  /**
   * Expert: increments the refCount of this IndexReader
   * instance.  RefCounts are used to determine when a
   * reader can be closed safely, i.e. as soon as there are
   * no more references.  Be sure to always call a
   * corresponding {@link #decRef}, in a finally clause;
   * otherwise the reader may never be closed.  Note that
   * {@link #close} simply calls decRef(), which means that
   * the IndexReader will not really be closed until {@link
   * #decRef} has been called for all outstanding
   * references.
   *
   * @see #decRef
   */
  public void incRef() {
    ensureOpen();
    refCount.incrementAndGet();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    if (hasChanges) {
      buffer.append('*');
    }
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final IndexReader[] subReaders = getSequentialSubReaders();
    if ((subReaders != null) && (subReaders.length > 0)) {
      buffer.append(subReaders[0]);
      for (int i = 1; i < subReaders.length; ++i) {
        buffer.append(" ").append(subReaders[i]);
      }
    }
    buffer.append(')');
    return buffer.toString();
  }

  /**
   * Expert: decreases the refCount of this IndexReader
   * instance.  If the refCount drops to 0, then pending
   * changes (if any) are committed to the index and this
   * reader is closed.  If an exception is hit, the refCount
   * is unchanged.
   *
   * @throws IOException in case an IOException occurs in commit() or doClose()
   *
   * @see #incRef
   */
  public void decRef() throws IOException {
    ensureOpen();
    if (refCount.getAndDecrement() == 1) {
      boolean success = false;
      try {
        commit();
        doClose();
        success = true;
      } finally {
        if (!success) {
          // Put reference back on failure
          refCount.incrementAndGet();
        }
      }
      readerFinished();
    }
  }
  
  protected IndexReader() { 
    refCount.set(1);
  }
  
  /**
   * @throws AlreadyClosedException if this IndexReader is closed
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (refCount.get() <= 0) {
      throw new AlreadyClosedException("this IndexReader is closed");
    }
  }
  
  /** Returns a IndexReader reading the index in the given
   *  Directory, with readOnly=true.
   * @param directory the index directory
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory) throws CorruptIndexException, IOException {
    return open(directory, null, null, true, DEFAULT_TERMS_INDEX_DIVISOR, null);
  }

  /** Returns an IndexReader reading the index in the given
   *  Directory.  You should pass readOnly=true, since it
   *  gives much better concurrent performance, unless you
   *  intend to do write operations (delete documents or
   *  change norms) with the reader.
   * @param directory the index directory
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, boolean readOnly) throws CorruptIndexException, IOException {
    return open(directory, null, null, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, null);
  }

  /**
   * Open a near real time IndexReader from the {@link org.apache.lucene.index.IndexWriter}.
   *
   * @param writer The IndexWriter to open from
   * @param applyAllDeletes If true, all buffered deletes will
   * be applied (made visible) in the returned reader.  If
   * false, the deletes are not applied but remain buffered
   * (in IndexWriter) so that they will be applied in the
   * future.  Applying deletes can be costly, so if your app
   * can tolerate deleted documents being returned you might
   * gain some performance by passing false.
   * @return The new IndexReader
   * @throws CorruptIndexException
   * @throws IOException if there is a low-level IO error
   *
   * @see #reopen(IndexWriter,boolean)
   *
   * @lucene.experimental
   */
  public static IndexReader open(final IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
    return writer.getReader(applyAllDeletes);
  }

  /** Expert: returns an IndexReader reading the index in the given
   *  {@link IndexCommit}.  You should pass readOnly=true, since it
   *  gives much better concurrent performance, unless you
   *  intend to do write operations (delete documents or
   *  change norms) with the reader.
   * @param commit the commit point to open
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final IndexCommit commit, boolean readOnly) throws CorruptIndexException, IOException {
    return open(commit.getDirectory(), null, commit, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, null);
  }

  /** Expert: returns an IndexReader reading the index in
   *  the given Directory, with a custom {@link
   *  IndexDeletionPolicy}.  You should pass readOnly=true,
   *  since it gives much better concurrent performance,
   *  unless you intend to do write operations (delete
   *  documents or change norms) with the reader.
   * @param directory the index directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, IndexDeletionPolicy deletionPolicy, boolean readOnly) throws CorruptIndexException, IOException {
    return open(directory, deletionPolicy, null, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, null);
  }

  /** Expert: returns an IndexReader reading the index in
   *  the given Directory, with a custom {@link
   *  IndexDeletionPolicy}.  You should pass readOnly=true,
   *  since it gives much better concurrent performance,
   *  unless you intend to do write operations (delete
   *  documents or change norms) with the reader.
   * @param directory the index directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @param termInfosIndexDivisor Subsamples which indexed
   *  terms are loaded into RAM. This has the same effect as {@link
   *  IndexWriterConfig#setTermIndexInterval} except that setting
   *  must be done at indexing time while this setting can be
   *  set per reader.  When set to N, then one in every
   *  N*termIndexInterval terms in the index is loaded into
   *  memory.  By setting this to a value > 1 you can reduce
   *  memory usage, at the expense of higher latency when
   *  loading a TermInfo.  The default value is 1.  Set this
   *  to -1 to skip loading the terms index entirely.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, IndexDeletionPolicy deletionPolicy, boolean readOnly, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return open(directory, deletionPolicy, null, readOnly, termInfosIndexDivisor, null);
  }

  /** Expert: returns an IndexReader reading the index in
   *  the given Directory, using a specific commit and with
   *  a custom {@link IndexDeletionPolicy}.  You should pass
   *  readOnly=true, since it gives much better concurrent
   *  performance, unless you intend to do write operations
   *  (delete documents or change norms) with the reader.
   * @param commit the specific {@link IndexCommit} to open;
   * see {@link IndexReader#listCommits} to list all commits
   * in a directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final IndexCommit commit, IndexDeletionPolicy deletionPolicy, boolean readOnly) throws CorruptIndexException, IOException {
    return open(commit.getDirectory(), deletionPolicy, commit, readOnly, DEFAULT_TERMS_INDEX_DIVISOR, null);
  }

  /** Expert: returns an IndexReader reading the index in
   *  the given Directory, using a specific commit and with
   *  a custom {@link IndexDeletionPolicy}.  You should pass
   *  readOnly=true, since it gives much better concurrent
   *  performance, unless you intend to do write operations
   *  (delete documents or change norms) with the reader.
   * @param commit the specific {@link IndexCommit} to open;
   * see {@link IndexReader#listCommits} to list all commits
   * in a directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @param termInfosIndexDivisor Subsamples which indexed
   *  terms are loaded into RAM. This has the same effect as {@link
   *  IndexWriterConfig#setTermIndexInterval} except that setting
   *  must be done at indexing time while this setting can be
   *  set per reader.  When set to N, then one in every
   *  N*termIndexInterval terms in the index is loaded into
   *  memory.  By setting this to a value > 1 you can reduce
   *  memory usage, at the expense of higher latency when
   *  loading a TermInfo.  The default value is 1.  Set this
   *  to -1 to skip loading the terms index entirely. This is only useful in 
   *  advanced situations when you will only .next() through all terms; 
   *  attempts to seek will hit an exception.
   *  
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final IndexCommit commit, IndexDeletionPolicy deletionPolicy, boolean readOnly, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return open(commit.getDirectory(), deletionPolicy, commit, readOnly, termInfosIndexDivisor, null);
  }

  /** Expert: returns an IndexReader reading the index in
   *  the given Directory, with a custom {@link
   *  IndexDeletionPolicy}, and specified {@link CodecProvider}.
   *  You should pass readOnly=true, since it gives much
   *  better concurrent performance, unless you intend to do
   *  write operations (delete documents or change norms)
   *  with the reader.
   * @param directory the index directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @param termInfosIndexDivisor Subsamples which indexed
   *  terms are loaded into RAM. This has the same effect as {@link
   *  IndexWriterConfig#setTermIndexInterval} except that setting
   *  must be done at indexing time while this setting can be
   *  set per reader.  When set to N, then one in every
   *  N*termIndexInterval terms in the index is loaded into
   *  memory.  By setting this to a value > 1 you can reduce
   *  memory usage, at the expense of higher latency when
   *  loading a TermInfo.  The default value is 1.  Set this
   *  to -1 to skip loading the terms index entirely.
   * @param codecs CodecProvider to use when opening index
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, IndexDeletionPolicy deletionPolicy, boolean readOnly, int termInfosIndexDivisor, CodecProvider codecs) throws CorruptIndexException, IOException {
    return open(directory, deletionPolicy, null, readOnly, termInfosIndexDivisor, codecs);
  }

  /** Expert: returns an IndexReader reading the index in
   *  the given Directory, using a specific commit and with
   *  a custom {@link IndexDeletionPolicy} and specified
   *  {@link CodecProvider}.  You should pass readOnly=true, since
   *  it gives much better concurrent performance, unless
   *  you intend to do write operations (delete documents or
   *  change norms) with the reader.

   * @param commit the specific {@link IndexCommit} to open;
   * see {@link IndexReader#listCommits} to list all commits
   * in a directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @param termInfosIndexDivisor Subsamples which indexed
   *  terms are loaded into RAM. This has the same effect as {@link
   *  IndexWriterConfig#setTermIndexInterval} except that setting
   *  must be done at indexing time while this setting can be
   *  set per reader.  When set to N, then one in every
   *  N*termIndexInterval terms in the index is loaded into
   *  memory.  By setting this to a value > 1 you can reduce
   *  memory usage, at the expense of higher latency when
   *  loading a TermInfo.  The default value is 1.  Set this
   *  to -1 to skip loading the terms index entirely.
   * @param codecs CodecProvider to use when opening index
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final IndexCommit commit, IndexDeletionPolicy deletionPolicy, boolean readOnly, int termInfosIndexDivisor, CodecProvider codecs) throws CorruptIndexException, IOException {
    return open(commit.getDirectory(), deletionPolicy, commit, readOnly, termInfosIndexDivisor, codecs);
  }

  private static IndexReader open(final Directory directory, final IndexDeletionPolicy deletionPolicy, final IndexCommit commit, final boolean readOnly, int termInfosIndexDivisor,
      CodecProvider codecs) throws CorruptIndexException, IOException {
    if (codecs == null) {
      codecs = CodecProvider.getDefault();
    }
    return DirectoryReader.open(directory, deletionPolicy, commit, readOnly, termInfosIndexDivisor, codecs);
  }

  /**
   * Refreshes an IndexReader if the index has changed since this instance 
   * was (re)opened. 
   * <p>
   * Opening an IndexReader is an expensive operation. This method can be used
   * to refresh an existing IndexReader to reduce these costs. This method 
   * tries to only load segments that have changed or were created after the 
   * IndexReader was (re)opened.
   * <p>
   * If the index has not changed since this instance was (re)opened, then this
   * call is a NOOP and returns this instance. Otherwise, a new instance is 
   * returned. The old instance is <b>not</b> closed and remains usable.<br>
   * <p>   
   * If the reader is reopened, even though they share
   * resources internally, it's safe to make changes
   * (deletions, norms) with the new reader.  All shared
   * mutable state obeys "copy on write" semantics to ensure
   * the changes are not seen by other readers.
   * <p>
   * You can determine whether a reader was actually reopened by comparing the
   * old instance with the instance returned by this method: 
   * <pre>
   * IndexReader reader = ... 
   * ...
   * IndexReader newReader = r.reopen();
   * if (newReader != reader) {
   * ...     // reader was reopened
   *   reader.close(); 
   * }
   * reader = newReader;
   * ...
   * </pre>
   *
   * Be sure to synchronize that code so that other threads,
   * if present, can never use reader after it has been
   * closed and before it's switched to newReader.
   *
   * <p><b>NOTE</b>: If this reader is a near real-time
   * reader (obtained from {@link IndexWriter#getReader()},
   * reopen() will simply call writer.getReader() again for
   * you, though this may change in the future.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */  
  public synchronized IndexReader reopen() throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support reopen().");
  }
  

  /** Just like {@link #reopen()}, except you can change the
   *  readOnly of the original reader.  If the index is
   *  unchanged but readOnly is different then a new reader
   *  will be returned. */
  public synchronized IndexReader reopen(boolean openReadOnly) throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support reopen().");
  }
  
  /** Expert: reopen this reader on a specific commit point.
   *  This always returns a readOnly reader.  If the
   *  specified commit point matches what this reader is
   *  already on, and this reader is already readOnly, then
   *  this same instance is returned; if it is not already
   *  readOnly, a readOnly clone is returned. */
  public synchronized IndexReader reopen(final IndexCommit commit) throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support reopen(IndexCommit).");
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
   * {#flush} (an internal IndexWriter operation) and then using {@link IndexReader#open} to
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
   * @return IndexReader that covers entire index plus all
   * changes made so far by this IndexWriter instance
   *
   * @param writer The IndexWriter to open from
   * @param applyAllDeletes If true, all buffered deletes will
   * be applied (made visible) in the returned reader.  If
   * false, the deletes are not applied but remain buffered
   * (in IndexWriter) so that they will be applied in the
   * future.  Applying deletes can be costly, so if your app
   * can tolerate deleted documents being returned you might
   * gain some performance by passing false.
   *
   * @throws IOException
   *
   * @lucene.experimental
   */
  public IndexReader reopen(IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
    return writer.getReader(applyAllDeletes);
  }

  /**
   * Efficiently clones the IndexReader (sharing most
   * internal state).
   * <p>
   * On cloning a reader with pending changes (deletions,
   * norms), the original reader transfers its write lock to
   * the cloned reader.  This means only the cloned reader
   * may make further changes to the index, and commit the
   * changes to the index on close, but the old reader still
   * reflects all changes made up until it was cloned.
   * <p>
   * Like {@link #reopen()}, it's safe to make changes to
   * either the original or the cloned reader: all shared
   * mutable state obeys "copy on write" semantics to ensure
   * the changes are not seen by other readers.
   * <p>
   */
  @Override
  public synchronized Object clone() {
    throw new UnsupportedOperationException("This reader does not implement clone()");
  }
  
  /**
   * Clones the IndexReader and optionally changes readOnly.  A readOnly 
   * reader cannot open a writeable reader.  
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized IndexReader clone(boolean openReadOnly) throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not implement clone()");
  }

  /** 
   * Returns the directory associated with this index.  The Default 
   * implementation returns the directory specified by subclasses when 
   * delegating to the IndexReader(Directory) constructor, or throws an 
   * UnsupportedOperationException if one was not specified.
   * @throws UnsupportedOperationException if no directory
   */
  public Directory directory() {
    ensureOpen();
    throw new UnsupportedOperationException("This reader does not support this method.");  
  }

  /**
   * Returns the time the index in the named directory was last modified. 
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long lastModified(final Directory directory2) throws CorruptIndexException, IOException {
    return ((Long) new SegmentInfos.FindSegmentsFile(directory2) {
        @Override
        public Object doBody(String segmentFileName) throws IOException {
          return Long.valueOf(directory2.fileModified(segmentFileName));
        }
      }.run()).longValue();
  }

  /**
   * Reads version number from segments files. The version number is
   * initialized with a timestamp and then increased by one for each change of
   * the index.
   * 
   * @param directory where the index resides.
   * @return version number.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long getCurrentVersion(Directory directory) throws CorruptIndexException, IOException {
    return getCurrentVersion(directory, CodecProvider.getDefault());
  }
  
  /**
   * Reads version number from segments files. The version number is
   * initialized with a timestamp and then increased by one for each change of
   * the index.
   * 
   * @param directory where the index resides.
   * @param codecs the {@link CodecProvider} holding all {@link Codec}s required to open the index
   * @return version number.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long getCurrentVersion(Directory directory, CodecProvider codecs) throws CorruptIndexException, IOException {
    return SegmentInfos.readCurrentVersion(directory, codecs);
  }

  /**
   * Reads commitUserData, previously passed to {@link
   * IndexWriter#commit(Map)}, from current index
   * segments file.  This will return null if {@link
   * IndexWriter#commit(Map)} has never been called for
   * this index.
   * 
   * @param directory where the index resides.
   * @return commit userData.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @see #getCommitUserData()
   */
  public static Map<String,String> getCommitUserData(Directory directory) throws CorruptIndexException, IOException {
    return getCommitUserData(directory,  CodecProvider.getDefault());
  }
  
  
  /**
   * Reads commitUserData, previously passed to {@link
   * IndexWriter#commit(Map)}, from current index
   * segments file.  This will return null if {@link
   * IndexWriter#commit(Map)} has never been called for
   * this index.
   * 
   * @param directory where the index resides.
   * @param codecs the {@link CodecProvider} provider holding all {@link Codec}s required to open the index
   * @return commit userData.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @see #getCommitUserData()
   */
  public static Map<String, String> getCommitUserData(Directory directory, CodecProvider codecs) throws CorruptIndexException, IOException {
    return SegmentInfos.readCurrentUserData(directory, codecs);
  }

  /**
   * Version number when this IndexReader was opened. Not
   * implemented in the IndexReader base class.
   *
   * <p>If this reader is based on a Directory (ie, was
   * created by calling {@link #open}, or {@link #reopen} on
   * a reader based on a Directory), then this method
   * returns the version recorded in the commit that the
   * reader opened.  This version is advanced every time
   * {@link IndexWriter#commit} is called.</p>
   *
   * <p>If instead this reader is a near real-time reader
   * (ie, obtained by a call to {@link
   * IndexWriter#getReader}, or by calling {@link #reopen}
   * on a near real-time reader), then this method returns
   * the version of the last commit done by the writer.
   * Note that even as further changes are made with the
   * writer, the version will not changed until a commit is
   * completed.  Thus, you should not rely on this method to
   * determine when a near real-time reader should be
   * opened.  Use {@link #isCurrent} instead.</p>
   *
   * @throws UnsupportedOperationException unless overridden in subclass
   */
  public long getVersion() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }

  /**
   * Retrieve the String userData optionally passed to
   * IndexWriter#commit.  This will return null if {@link
   * IndexWriter#commit(Map)} has never been called for
   * this index.
   *
   * @see #getCommitUserData(Directory)
   */
  public Map<String,String> getCommitUserData() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }


  /**
   * Check whether any new changes have occurred to the
   * index since this reader was opened.
   *
   * <p>If this reader is based on a Directory (ie, was
   * created by calling {@link #open}, or {@link #reopen} on
   * a reader based on a Directory), then this method checks
   * if any further commits (see {@link IndexWriter#commit}
   * have occurred in that directory).</p>
   *
   * <p>If instead this reader is a near real-time reader
   * (ie, obtained by a call to {@link
   * IndexWriter#getReader}, or by calling {@link #reopen}
   * on a near real-time reader), then this method checks if
   * either a new commmit has occurred, or any new
   * uncommitted changes have taken place via the writer.
   * Note that even if the writer has only performed
   * merging, this method will still return false.</p>
   *
   * <p>In any event, if this returns false, you should call
   * {@link #reopen} to get a new reader that sees the
   * changes.</p>
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException           if there is a low-level IO error
   * @throws UnsupportedOperationException unless overridden in subclass
   */
  public boolean isCurrent() throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }

  /**
   * Checks is the index is optimized (if it has a single segment and 
   * no deletions).  Not implemented in the IndexReader base class.
   * @return <code>true</code> if the index is optimized; <code>false</code> otherwise
   * @throws UnsupportedOperationException unless overridden in subclass
   */
  public boolean isOptimized() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }
  
  /**
   * Return an array of term frequency vectors for the specified document.
   * The array contains a vector for each vectorized field in the document.
   * Each vector contains terms and frequencies for all terms in a given vectorized field.
   * If no such fields existed, the method returns null. The term vectors that are
   * returned may either be of type {@link TermFreqVector}
   * or of type {@link TermPositionVector} if
   * positions or offsets have been stored.
   * 
   * @param docNumber document for which term frequency vectors are returned
   * @return array of term frequency vectors. May be null if no term vectors have been
   *  stored for the specified document.
   * @throws IOException if index cannot be accessed
   * @see org.apache.lucene.document.Field.TermVector
   */
  abstract public TermFreqVector[] getTermFreqVectors(int docNumber)
          throws IOException;


  /**
   * Return a term frequency vector for the specified document and field. The
   * returned vector contains terms and frequencies for the terms in
   * the specified field of this document, if the field had the storeTermVector
   * flag set. If termvectors had been stored with positions or offsets, a 
   * {@link TermPositionVector} is returned.
   * 
   * @param docNumber document for which the term frequency vector is returned
   * @param field field for which the term frequency vector is returned.
   * @return term frequency vector May be null if field does not exist in the specified
   * document or term vector was not stored.
   * @throws IOException if index cannot be accessed
   * @see org.apache.lucene.document.Field.TermVector
   */
  abstract public TermFreqVector getTermFreqVector(int docNumber, String field)
          throws IOException;

  /**
   * Load the Term Vector into a user-defined data structure instead of relying on the parallel arrays of
   * the {@link TermFreqVector}.
   * @param docNumber The number of the document to load the vector for
   * @param field The name of the field to load
   * @param mapper The {@link TermVectorMapper} to process the vector.  Must not be null
   * @throws IOException if term vectors cannot be accessed or if they do not exist on the field and doc. specified.
   * 
   */
  abstract public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException;

  /**
   * Map all the term vectors for all fields in a Document
   * @param docNumber The number of the document to load the vector for
   * @param mapper The {@link TermVectorMapper} to process the vector.  Must not be null
   * @throws IOException if term vectors cannot be accessed or if they do not exist on the field and doc. specified.
   */
  abstract public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException;

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean indexExists(Directory directory) throws IOException {
    try {
      new SegmentInfos().read(directory);
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * @param  directory the directory to check for an index
   * @param  codecProvider provides a CodecProvider in case the index uses non-core codecs
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean indexExists(Directory directory, CodecProvider codecProvider) throws IOException {
    try {
      new SegmentInfos().read(directory, codecProvider);
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }

  /** Returns the number of documents in this index. */
  public abstract int numDocs();

  /** Returns one greater than the largest possible document number.
   * This may be used to, e.g., determine how big to allocate an array which
   * will have an element for every document number in an index.
   */
  public abstract int maxDoc();

  /** Returns the number of deleted documents. */
  public int numDeletedDocs() {
    return maxDoc() - numDocs();
  }

  /**
   * Returns the stored fields of the <code>n</code><sup>th</sup>
   * <code>Document</code> in this index.
   * <p>
   * <b>NOTE:</b> for performance reasons, this method does not check if the
   * requested document is deleted, and therefore asking for a deleted document
   * may yield unspecified results. Usually this is not required, however you
   * can test if the doc is deleted by checking the {@link
   * Bits} returned from {@link MultiFields#getLiveDocs}.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public Document document(int n) throws CorruptIndexException, IOException {
    ensureOpen();
    return document(n, null);
  }

  /**
   * Get the {@link org.apache.lucene.document.Document} at the <code>n</code>
   * <sup>th</sup> position. The {@link FieldSelector} may be used to determine
   * what {@link org.apache.lucene.document.Field}s to load and how they should
   * be loaded. <b>NOTE:</b> If this Reader (more specifically, the underlying
   * <code>FieldsReader</code>) is closed before the lazy
   * {@link org.apache.lucene.document.Field} is loaded an exception may be
   * thrown. If you want the value of a lazy
   * {@link org.apache.lucene.document.Field} to be available after closing you
   * must explicitly load it or fetch the Document again with a new loader.
   * <p>
   * <b>NOTE:</b> for performance reasons, this method does not check if the
   * requested document is deleted, and therefore asking for a deleted document
   * may yield unspecified results. Usually this is not required, however you
   * can test if the doc is deleted by checking the {@link
   * Bits} returned from {@link MultiFields#getLiveDocs}.
   * 
   * @param n Get the document at the <code>n</code><sup>th</sup> position
   * @param fieldSelector The {@link FieldSelector} to use to determine what
   *        Fields should be loaded on the Document. May be null, in which case
   *        all Fields will be loaded.
   * @return The stored fields of the
   *         {@link org.apache.lucene.document.Document} at the nth position
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @see org.apache.lucene.document.Fieldable
   * @see org.apache.lucene.document.FieldSelector
   * @see org.apache.lucene.document.SetBasedFieldSelector
   * @see org.apache.lucene.document.LoadFirstFieldSelector
   */
  // TODO (1.5): When we convert to JDK 1.5 make this Set<String>
  public abstract Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException;
  
  /** Returns true if any documents have been deleted */
  public abstract boolean hasDeletions();

  /** Returns true if there are norms stored for this field. */
  public boolean hasNorms(String field) throws IOException {
    // backward compatible implementation.
    // SegmentReader has an efficient implementation.
    ensureOpen();
    return norms(field) != null;
  }

  /** Returns the byte-encoded normalization factor for the named field of
   *  every document.  This is used by the search code to score documents.
   *  Returns null if norms were not indexed for this field.
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   */
  public abstract byte[] norms(String field) throws IOException;

  /** Expert: Resets the normalization factor for the named field of the named
   * document.  The norm represents the product of the field's {@link
   * org.apache.lucene.document.Fieldable#setBoost(float) boost} and its
   * length normalization}.  Thus, to preserve the length normalization
   * values when resetting this, one should base the new value upon the old.
   *
   * <b>NOTE:</b> If this field does not index norms, then
   * this method throws {@link IllegalStateException}.
   *
   * @see #norms(String)
   * @see Similarity#decodeNormValue(byte)
   * @throws StaleReaderException if the index has changed
   *  since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if there is a low-level IO error
   * @throws IllegalStateException if the field does not index norms
   */
  public synchronized  void setNorm(int doc, String field, byte value)
          throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    ensureOpen();
    acquireWriteLock();
    hasChanges = true;
    doSetNorm(doc, field, value);
  }

  /** Implements setNorm in subclass.*/
  protected abstract void doSetNorm(int doc, String field, byte value)
          throws CorruptIndexException, IOException;

  /**
   * Returns {@link Fields} for this reader.
   * This method may return null if the reader has no
   * postings.
   *
   * <p><b>NOTE</b>: if this is a multi reader ({@link
   * #getSequentialSubReaders} is not null) then this
   * method will throw UnsupportedOperationException.  If
   * you really need a {@link Fields} for such a reader,
   * use {@link MultiFields#getFields}.  However, for
   * performance reasons, it's best to get all sub-readers
   * using {@link ReaderUtil#gatherSubReaders} and iterate
   * through them yourself. */
  public abstract Fields fields() throws IOException;
  
  /**
   * Returns {@link PerDocValues} for this reader.
   * This method may return null if the reader has no per-document
   * values stored.
   *
   * <p><b>NOTE</b>: if this is a multi reader ({@link
   * #getSequentialSubReaders} is not null) then this
   * method will throw UnsupportedOperationException.  If
   * you really need {@link PerDocValues} for such a reader,
   * use {@link MultiPerDocValues#getPerDocs(IndexReader)}.  However, for
   * performance reasons, it's best to get all sub-readers
   * using {@link ReaderUtil#gatherSubReaders} and iterate
   * through them yourself. */
  public abstract PerDocValues perDocValues() throws IOException;

  public int docFreq(Term term) throws IOException {
    return docFreq(term.field(), term.bytes());
  }

  /** Returns the number of documents containing the term
   * <code>t</code>.  This method returns 0 if the term or
   * field does not exists.  This method does not take into
   * account deleted documents that have not yet been merged
   * away. */
  public int docFreq(String field, BytesRef term) throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return 0;
    }
    final Terms terms = fields.terms(field);
    if (terms == null) {
      return 0;
    }
    return terms.docFreq(term);
  }

  /** Returns the number of documents containing the term
   * <code>t</code>.  This method returns 0 if the term or
   * field does not exists.  This method does not take into
   * account deleted documents that have not yet been merged
   * away. */
  public long totalTermFreq(String field, BytesRef term) throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return 0;
    }
    final Terms terms = fields.terms(field);
    if (terms == null) {
      return 0;
    }
    return terms.totalTermFreq(term);
  }

  /** This may return null if the field does not exist.*/
  public Terms terms(String field) throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return null;
    }
    return fields.terms(field);
  }

  /** Returns {@link DocsEnum} for the specified field &
   *  term.  This may return null, if either the field or
   *  term does not exist. */
  public DocsEnum termDocsEnum(Bits liveDocs, String field, BytesRef term) throws IOException {
    assert field != null;
    assert term != null;
    final Fields fields = fields();
    if (fields == null) {
      return null;
    }
    final Terms terms = fields.terms(field);
    if (terms != null) {
      return terms.docs(liveDocs, term, null);
    } else {
      return null;
    }
  }

  /** Returns {@link DocsAndPositionsEnum} for the specified
   *  field & term.  This may return null, if either the
   *  field or term does not exist, or, positions were not
   *  stored for this term. */
  public DocsAndPositionsEnum termPositionsEnum(Bits liveDocs, String field, BytesRef term) throws IOException {
    assert field != null;
    assert term != null;
    final Fields fields = fields();
    if (fields == null) {
      return null;
    }
    final Terms terms = fields.terms(field);
    if (terms != null) {
      return terms.docsAndPositions(liveDocs, term, null);
    } else {
      return null;
    }
  }
  
  /**
   * Returns {@link DocsEnum} for the specified field and
   * {@link TermState}. This may return null, if either the field or the term
   * does not exists or the {@link TermState} is invalid for the underlying
   * implementation.*/
  public DocsEnum termDocsEnum(Bits liveDocs, String field, BytesRef term, TermState state) throws IOException {
    assert state != null;
    assert field != null;
    final Fields fields = fields();
    if (fields == null) {
      return null;
    }
    final Terms terms = fields.terms(field);
    if (terms != null) {
      return terms.docs(liveDocs, term, state, null);
    } else {
      return null;
    }
  }
  
  /**
   * Returns {@link DocsAndPositionsEnum} for the specified field and
   * {@link TermState}. This may return null, if either the field or the term
   * does not exists, the {@link TermState} is invalid for the underlying
   * implementation, or positions were not stored for this term.*/
  public DocsAndPositionsEnum termPositionsEnum(Bits liveDocs, String field, BytesRef term, TermState state) throws IOException {
    assert state != null;
    assert field != null;
    final Fields fields = fields();
    if (fields == null) {
      return null;
    }
    final Terms terms = fields.terms(field);
    if (terms != null) {
      return terms.docsAndPositions(liveDocs, term, state, null);
    } else {
      return null;
    }
  }


  /** Deletes the document numbered <code>docNum</code>.  Once a document is
   * deleted it will not appear in TermDocs or TermPositions enumerations.
   * Attempts to read its field with the {@link #document}
   * method will result in an error.  The presence of this document may still be
   * reflected in the {@link #docFreq} statistic, though
   * this will be corrected eventually as the index is further modified.
   *
   * @throws StaleReaderException if the index has changed
   * since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void deleteDocument(int docNum) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    ensureOpen();
    acquireWriteLock();
    hasChanges = true;
    doDelete(docNum);
  }


  /** Implements deletion of the document numbered <code>docNum</code>.
   * Applications should call {@link #deleteDocument(int)} or {@link #deleteDocuments(Term)}.
   */
  protected abstract void doDelete(int docNum) throws CorruptIndexException, IOException;


  /** Deletes all documents that have a given <code>term</code> indexed.
   * This is useful if one uses a document field to hold a unique ID string for
   * the document.  Then to delete such a document, one merely constructs a
   * term with the appropriate field and the unique ID string as its text and
   * passes it to this method.
   * See {@link #deleteDocument(int)} for information about when this deletion will 
   * become effective.
   *
   * @return the number of documents deleted
   * @throws StaleReaderException if the index has changed
   *  since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if there is a low-level IO error
   */
  public int deleteDocuments(Term term) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    ensureOpen();
    DocsEnum docs = MultiFields.getTermDocsEnum(this,
                                                MultiFields.getLiveDocs(this),
                                                term.field(),
                                                term.bytes());
    if (docs == null) return 0;
    int n = 0;
    int doc;
    while ((doc = docs.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
      deleteDocument(doc);
      n++;
    }
    return n;
  }

  /** Undeletes all documents currently marked as deleted in
   * this index.
   *
   * <p>NOTE: this method can only recover documents marked
   * for deletion but not yet removed from the index; when
   * and how Lucene removes deleted documents is an
   * implementation detail, subject to change from release
   * to release.  However, you can use {@link
   * #numDeletedDocs} on the current IndexReader instance to
   * see how many documents will be un-deleted.
   *
   * @throws StaleReaderException if the index has changed
   *  since this reader was opened
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized void undeleteAll() throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    ensureOpen();
    acquireWriteLock();
    hasChanges = true;
    doUndeleteAll();
  }

  /** Implements actual undeleteAll() in subclass. */
  protected abstract void doUndeleteAll() throws CorruptIndexException, IOException;

  /** Does nothing by default. Subclasses that require a write lock for
   *  index modifications must implement this method. */
  protected synchronized void acquireWriteLock() throws IOException {
    /* NOOP */
  }
  
  /**
   * 
   * @throws IOException
   */
  public final synchronized void flush() throws IOException {
    ensureOpen();
    commit();
  }

  /**
   * @param commitUserData Opaque Map (String -> String)
   *  that's recorded into the segments file in the index,
   *  and retrievable by {@link
   *  IndexReader#getCommitUserData}.
   * @throws IOException
   */
  public final synchronized void flush(Map<String, String> commitUserData) throws IOException {
    ensureOpen();
    commit(commitUserData);
  }
  
  /**
   * Commit changes resulting from delete, undeleteAll, or
   * setNorm operations
   *
   * If an exception is hit, then either no changes or all
   * changes will have been committed to the index
   * (transactional semantics).
   * @throws IOException if there is a low-level IO error
   */
  protected final synchronized void commit() throws IOException {
    commit(null);
  }
  
  /**
   * Commit changes resulting from delete, undeleteAll, or
   * setNorm operations
   *
   * If an exception is hit, then either no changes or all
   * changes will have been committed to the index
   * (transactional semantics).
   * @throws IOException if there is a low-level IO error
   */
  public final synchronized void commit(Map<String, String> commitUserData) throws IOException {
    doCommit(commitUserData);
    hasChanges = false;
  }

  /** Implements commit.  */
  protected abstract void doCommit(Map<String, String> commitUserData) throws IOException;

  /**
   * Closes files associated with this index.
   * Also saves any new deletions to disk.
   * No other methods should be called after this has been called.
   * @throws IOException if there is a low-level IO error
   */
  public final synchronized void close() throws IOException {
    if (!closed) {
      decRef();
      closed = true;
    }
  }
  
  /** Implements close. */
  protected abstract void doClose() throws IOException;


  /**
   * Get a list of unique field names that exist in this index and have the specified
   * field option information.
   * @param fldOption specifies which field option should be available for the returned fields
   * @return Collection of Strings indicating the names of the fields.
   * @see IndexReader.FieldOption
   */
  public abstract Collection<String> getFieldNames(FieldOption fldOption);

  /** Returns the {@link Bits} representing live (not
   *  deleted) docs.  A set bit indicates the doc ID has not
   *  been deleted.  If this method returns null it means
   *  there are no deleted documents (all documents are
   *  live).
   *
   *  The returned instance has been safely published for
   *  use by multiple threads without additional
   *  synchronization.
   * @lucene.experimental */
  public abstract Bits getLiveDocs();

  /**
   * Expert: return the IndexCommit that this reader has
   * opened.  This method is only implemented by those
   * readers that correspond to a Directory with its own
   * segments_N file.
   *
   * @lucene.experimental
   */
  public IndexCommit getIndexCommit() throws IOException {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }
  
  /**
   * Prints the filename and size of each file within a given compound file.
   * Add the -extract flag to extract files to the current working directory.
   * In order to make the extracted version of the index work, you have to copy
   * the segments file from the compound index into the directory where the extracted files are stored.
   * @param args Usage: org.apache.lucene.index.IndexReader [-extract] &lt;cfsfile&gt;
   */
  public static void main(String [] args) {
    String filename = null;
    boolean extract = false;

    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("-extract")) {
        extract = true;
      } else if (filename == null) {
        filename = args[i];
      }
    }

    if (filename == null) {
      System.out.println("Usage: org.apache.lucene.index.IndexReader [-extract] <cfsfile>");
      return;
    }

    Directory dir = null;
    CompoundFileDirectory cfr = null;

    try {
      File file = new File(filename);
      String dirname = file.getAbsoluteFile().getParent();
      filename = file.getName();
      dir = FSDirectory.open(new File(dirname));
      cfr = dir.openCompoundInput(filename, BufferedIndexInput.BUFFER_SIZE);

      String [] files = cfr.listAll();
      ArrayUtil.mergeSort(files);   // sort the array of filename so that the output is more readable

      for (int i = 0; i < files.length; ++i) {
        long len = cfr.fileLength(files[i]);

        if (extract) {
          System.out.println("extract " + files[i] + " with " + len + " bytes to local directory...");
          IndexInput ii = cfr.openInput(files[i]);

          FileOutputStream f = new FileOutputStream(files[i]);

          // read and write with a small buffer, which is more effective than reading byte by byte
          byte[] buffer = new byte[1024];
          int chunk = buffer.length;
          while(len > 0) {
            final int bufLen = (int) Math.min(chunk, len);
            ii.readBytes(buffer, 0, bufLen);
            f.write(buffer, 0, bufLen);
            len -= bufLen;
          }

          f.close();
          ii.close();
        }
        else
          System.out.println(files[i] + ": " + len + " bytes");
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    finally {
      try {
        if (dir != null)
          dir.close();
        if (cfr != null)
          cfr.close();
      }
      catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  /** Returns all commit points that exist in the Directory.
   *  Normally, because the default is {@link
   *  KeepOnlyLastCommitDeletionPolicy}, there would be only
   *  one commit point.  But if you're using a custom {@link
   *  IndexDeletionPolicy} then there could be many commits.
   *  Once you have a given commit, you can open a reader on
   *  it by calling {@link IndexReader#open(IndexCommit,boolean)}
   *  There must be at least one commit in
   *  the Directory, else this method throws {@link
   *  IndexNotFoundException}.  Note that if a commit is in
   *  progress while this method is running, that commit
   *  may or may not be returned.
   *  
   *  @return a sorted list of {@link IndexCommit}s, from oldest 
   *  to latest. */
  public static List<IndexCommit> listCommits(Directory dir) throws IOException {
    return DirectoryReader.listCommits(dir);
  }

  /** Expert: returns the sequential sub readers that this
   *  reader is logically composed of. If this reader is not composed
   *  of sequential child readers, it should return null.
   *  If this method returns an empty array, that means this
   *  reader is a null reader (for example a MultiReader
   *  that has no sub readers).
   *  <p>
   *  NOTE: You should not try using sub-readers returned by
   *  this method to make any changes (setNorm, deleteDocument,
   *  etc.). While this might succeed for one composite reader
   *  (like MultiReader), it will most likely lead to index
   *  corruption for other readers (like DirectoryReader obtained
   *  through {@link #open}. Use the parent reader directly. */
  public IndexReader[] getSequentialSubReaders() {
    return null;
  }
  
  /**
   * Expert: Returns a the root {@link ReaderContext} for this
   * {@link IndexReader}'s sub-reader tree. Iff this reader is composed of sub
   * readers ,ie. this reader being a composite reader, this method returns a
   * {@link CompositeReaderContext} holding the reader's direct children as well as a
   * view of the reader tree's atomic leaf contexts. All sub-
   * {@link ReaderContext} instances referenced from this readers top-level
   * context are private to this reader and are not shared with another context
   * tree. For example, IndexSearcher uses this API to drive searching by one
   * atomic leaf reader at a time. If this reader is not composed of child
   * readers, this method returns an {@link AtomicReaderContext}.
   * <p>
   * Note: Any of the sub-{@link CompositeReaderContext} instances reference from this
   * top-level context holds a <code>null</code> {@link CompositeReaderContext#leaves}
   * reference. Only the top-level context maintains the convenience leaf-view
   * for performance reasons.
   * <p>
   * NOTE: You should not try using sub-readers returned by this method to make
   * any changes (setNorm, deleteDocument, etc.). While this might succeed for
   * one composite reader (like MultiReader), it will most likely lead to index
   * corruption for other readers (like DirectoryReader obtained through
   * {@link #open}. Use the top-level context's reader directly.
   * 
   * @lucene.experimental
   */
  public abstract ReaderContext getTopReaderContext();

  /** Expert */
  public Object getCoreCacheKey() {
    return this;
  }

  /** Returns the number of unique terms (across all fields)
   *  in this reader.
   *
   *  @throws UnsupportedOperationException if this count
   *  cannot be easily determined (eg Multi*Readers).
   *  Instead, you should call {@link
   *  #getSequentialSubReaders} and ask each sub reader for
   *  its unique term count. */
  public long getUniqueTermCount() throws IOException {
    long numTerms = 0;
    final Fields fields = fields();
    if (fields == null) {
      return 0;
    }
    FieldsEnum it = fields.iterator();
    while(true) {
      String field = it.next();
      if (field == null) {
        break;
      }
      numTerms += fields.terms(field).getUniqueTermCount();
    }
    return numTerms;
  }

  /** For IndexReader implementations that use
   *  TermInfosReader to read terms, this returns the
   *  current indexDivisor as specified when the reader was
   *  opened.
   */
  public int getTermInfosIndexDivisor() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }
  
  public final IndexDocValues docValues(String field) throws IOException {
    final PerDocValues perDoc = perDocValues();
    if (perDoc == null) {
      return null;
    }
    return perDoc.docValues(field);
  }

  private volatile Fields fields;

  /** @lucene.internal */
  void storeFields(Fields fields) {
    this.fields = fields;
  }

  /** @lucene.internal */
  Fields retrieveFields() {
    return fields;
  }
  
  private volatile PerDocValues perDocValues;
  
  /** @lucene.internal */
  void storePerDoc(PerDocValues perDocValues) {
    this.perDocValues = perDocValues;
  }

  /** @lucene.internal */
  PerDocValues retrievePerDoc() {
    return perDocValues;
  }  
  

  /**
   * A struct like class that represents a hierarchical relationship between
   * {@link IndexReader} instances. 
   * @lucene.experimental
   */
  public static abstract class ReaderContext {
    /** The reader context for this reader's immediate parent, or null if none */
    public final ReaderContext parent;
    /** The actual reader */
    public final IndexReader reader;
    /** <code>true</code> iff the reader is an atomic reader */
    public final boolean isAtomic;
    /** <code>true</code> if this context struct represents the top level reader within the hierarchical context */
    public final boolean isTopLevel;
    /** the doc base for this reader in the parent, <tt>0</tt> if parent is null */
    public final int docBaseInParent;
    /** the ord for this reader in the parent, <tt>0</tt> if parent is null */
    public final int ordInParent;
    
    ReaderContext(ReaderContext parent, IndexReader reader,
        boolean isAtomic, int ordInParent, int docBaseInParent) {
      this.parent = parent;
      this.reader = reader;
      this.isAtomic = isAtomic;
      this.docBaseInParent = docBaseInParent;
      this.ordInParent = ordInParent;
      this.isTopLevel = parent==null;
    }
    
    /**
     * Returns the context's leaves if this context is a top-level context
     * otherwise <code>null</code>.
     * <p>
     * Note: this is convenience method since leaves can always be obtained by
     * walking the context tree.
     */
    public AtomicReaderContext[] leaves() {
      return null;
    }
    
    /**
     * Returns the context's children iff this context is a composite context
     * otherwise <code>null</code>.
     * <p>
     * Note: this method is a convenience method to prevent
     * <code>instanceof</code> checks and type-casts to
     * {@link CompositeReaderContext}.
     */
    public ReaderContext[] children() {
      return null;
    }
  }
  
  /**
   * {@link ReaderContext} for composite {@link IndexReader} instance.
   * @lucene.experimental
   */
  public static final class CompositeReaderContext extends ReaderContext {
    /** the composite readers immediate children */
    public final ReaderContext[] children;
    /** the composite readers leaf reader contexts if this is the top level reader in this context */
    public final AtomicReaderContext[] leaves;

    /**
     * Creates a {@link CompositeReaderContext} for intermediate readers that aren't
     * not top-level readers in the current context
     */
    public CompositeReaderContext(ReaderContext parent, IndexReader reader,
        int ordInParent, int docbaseInParent, ReaderContext[] children) {
      this(parent, reader, ordInParent, docbaseInParent, children, null);
    }
    
    /**
     * Creates a {@link CompositeReaderContext} for top-level readers with parent set to <code>null</code>
     */
    public CompositeReaderContext(IndexReader reader, ReaderContext[] children, AtomicReaderContext[] leaves) {
      this(null, reader, 0, 0, children, leaves);
    }
    
    private CompositeReaderContext(ReaderContext parent, IndexReader reader,
        int ordInParent, int docbaseInParent, ReaderContext[] children,
        AtomicReaderContext[] leaves) {
      super(parent, reader, false, ordInParent, docbaseInParent);
      this.children = children;
      this.leaves = leaves;
    }

    @Override
    public AtomicReaderContext[] leaves() {
      return leaves;
    }
    
    
    @Override
    public ReaderContext[] children() {
      return children;
    }
  }
  
  /**
   * {@link ReaderContext} for atomic {@link IndexReader} instances
   * @lucene.experimental
   */
  public static final class AtomicReaderContext extends ReaderContext {
    /** The readers ord in the top-level's leaves array */
    public final int ord;
    /** The readers absolute doc base */
    public final int docBase;
    /**
     * Creates a new {@link AtomicReaderContext} 
     */    
    public AtomicReaderContext(ReaderContext parent, IndexReader reader,
        int ord, int docBase, int leafOrd, int leafDocBase) {
      super(parent, reader, true, ord, docBase);
      assert reader.getSequentialSubReaders() == null : "Atomic readers must not have subreaders";
      this.ord = leafOrd;
      this.docBase = leafDocBase;
    }
    
    /**
     * Creates a new {@link AtomicReaderContext} for a atomic reader without an immediate
     * parent.
     */
    public AtomicReaderContext(IndexReader atomicReader) {
      this(null, atomicReader, 0, 0, 0, 0);
    }
  }
}
