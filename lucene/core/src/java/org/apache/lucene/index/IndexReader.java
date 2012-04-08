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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.WeakHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.search.SearcherManager; // javadocs
import org.apache.lucene.store.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;         // for javadocs

/** IndexReader is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable.

 <p>There are two different types of IndexReaders:
 <ul>
  <li>{@link AtomicReader}: These indexes do not consist of several sub-readers,
  they are atomic. They support retrieval of stored fields, doc values, terms,
  and postings.
  <li>{@link CompositeReader}: Instances (like {@link DirectoryReader})
  of this reader can only
  be used to get stored fields from the underlying AtomicReaders,
  but it is not possible to directly retrieve postings. To do that, get
  the sub-readers via {@link CompositeReader#getSequentialSubReaders}.
  Alternatively, you can mimic an {@link AtomicReader} (with a serious slowdown),
  by wrapping composite readers with {@link SlowCompositeReaderWrapper}.
 </ul>
 
 <p>IndexReader instances for indexes on disk are usually constructed
 with a call to one of the static <code>DirectoryReader,open()</code> methods,
 e.g. {@link DirectoryReader#open(Directory)}. {@link DirectoryReader} implements
 the {@link CompositeReader} interface, it is not possible to directly get postings.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral -- they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 <p>
 <a name="thread-safety"></a><p><b>NOTE</b>: {@link
 IndexReader} instances are completely thread
 safe, meaning multiple threads can call any of its methods,
 concurrently.  If your application requires external
 synchronization, you should <b>not</b> synchronize on the
 <code>IndexReader</code> instance; use your own
 (non-Lucene) objects instead.
*/
public abstract class IndexReader implements Closeable {
  
  private boolean closed = false;
  private boolean closedByChild = false;
  private final AtomicInteger refCount = new AtomicInteger(1);

  IndexReader() {
    if (!(this instanceof CompositeReader || this instanceof AtomicReader))
      throw new Error("IndexReader should never be directly extended, subclass AtomicReader or CompositeReader instead.");
  }
  
  /**
   * A custom listener that's invoked when the IndexReader
   * is closed.
   *
   * @lucene.experimental
   */
  public static interface ReaderClosedListener {
    public void onClose(IndexReader reader);
  }

  private final Set<ReaderClosedListener> readerClosedListeners = 
      Collections.synchronizedSet(new LinkedHashSet<ReaderClosedListener>());

  private final Set<IndexReader> parentReaders = 
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<IndexReader,Boolean>()));

  /** Expert: adds a {@link ReaderClosedListener}.  The
   * provided listener will be invoked when this reader is closed.
   *
   * @lucene.experimental */
  public final void addReaderClosedListener(ReaderClosedListener listener) {
    ensureOpen();
    readerClosedListeners.add(listener);
  }

  /** Expert: remove a previously added {@link ReaderClosedListener}.
   *
   * @lucene.experimental */
  public final void removeReaderClosedListener(ReaderClosedListener listener) {
    ensureOpen();
    readerClosedListeners.remove(listener);
  }
  
  /** Expert: This method is called by {@code IndexReader}s which wrap other readers
   * (e.g. {@link CompositeReader} or {@link FilterAtomicReader}) to register the parent
   * at the child (this reader) on construction of the parent. When this reader is closed,
   * it will mark all registered parents as closed, too. The references to parent readers
   * are weak only, so they can be GCed once they are no longer in use.
   * @lucene.experimental */
  public final void registerParentReader(IndexReader reader) {
    ensureOpen();
    parentReaders.add(reader);
  }

  private void notifyReaderClosedListeners() {
    synchronized(readerClosedListeners) {
      for(ReaderClosedListener listener : readerClosedListeners) {
        listener.onClose(this);
      }
    }
  }

  private void reportCloseToParentReaders() {
    synchronized(parentReaders) {
      for(IndexReader parent : parentReaders) {
        parent.closedByChild = true;
        // cross memory barrier by a fake write:
        parent.refCount.addAndGet(0);
        // recurse:
        parent.reportCloseToParentReaders();
      }
    }
  }

  /** Expert: returns the current refCount for this reader */
  public final int getRefCount() {
    // NOTE: don't ensureOpen, so that callers can see
    // refCount is 0 (reader is closed)
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
   * @see #tryIncRef
   */
  public final void incRef() {
    ensureOpen();
    refCount.incrementAndGet();
  }
  
  /**
   * Expert: increments the refCount of this IndexReader
   * instance only if the IndexReader has not been closed yet
   * and returns <code>true</code> iff the refCount was
   * successfully incremented, otherwise <code>false</code>.
   * If this method returns <code>false</code> the reader is either
   * already closed or is currently been closed. Either way this
   * reader instance shouldn't be used by an application unless
   * <code>true</code> is returned.
   * <p>
   * RefCounts are used to determine when a
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
   * @see #incRef
   */
  public final boolean tryIncRef() {
    int count;
    while ((count = refCount.get()) > 0) {
      if (refCount.compareAndSet(count, count+1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Expert: decreases the refCount of this IndexReader
   * instance.  If the refCount drops to 0, then this
   * reader is closed.  If an exception is hit, the refCount
   * is unchanged.
   *
   * @throws IOException in case an IOException occurs in  doClose()
   *
   * @see #incRef
   */
  public final void decRef() throws IOException {
    // only check refcount here (don't call ensureOpen()), so we can
    // still close the reader if it was made invalid by a child:
    if (refCount.get() <= 0) {
      throw new AlreadyClosedException("this IndexReader is closed");
    }
    
    final int rc = refCount.decrementAndGet();
    if (rc == 0) {
      boolean success = false;
      try {
        doClose();
        success = true;
      } finally {
        if (!success) {
          // Put reference back on failure
          refCount.incrementAndGet();
        }
      }
      reportCloseToParentReaders();
      notifyReaderClosedListeners();
    } else if (rc < 0) {
      throw new IllegalStateException("too many decRef calls: refCount is " + rc + " after decrement");
    }
  }
  
  /**
   * @throws AlreadyClosedException if this IndexReader is closed
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (refCount.get() <= 0) {
      throw new AlreadyClosedException("this IndexReader is closed");
    }
    // the happens before rule on reading the refCount, which must be after the fake write,
    // ensures that we see the value:
    if (closedByChild) {
      throw new AlreadyClosedException("this IndexReader cannot be used anymore as one of its child readers was closed");
    }
  }
  
  /** {@inheritDoc}
   * <p>For caching purposes, {@code IndexReader} subclasses are not allowed
   * to implement equals/hashCode, so methods are declared final.
   * To lookup instances from caches use {@link #getCoreCacheKey} and 
   * {@link #getCombinedCoreAndDeletesKey}.
   */
  @Override
  public final boolean equals(Object obj) {
    return (this == obj);
  }
  
  /** {@inheritDoc}
   * <p>For caching purposes, {@code IndexReader} subclasses are not allowed
   * to implement equals/hashCode, so methods are declared final.
   * To lookup instances from caches use {@link #getCoreCacheKey} and 
   * {@link #getCombinedCoreAndDeletesKey}.
   */
  @Override
  public final int hashCode() {
    return System.identityHashCode(this);
  }
  
  /** Returns a IndexReader reading the index in the given
   *  Directory
   * @param directory the index directory
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated Use {@link DirectoryReader#open(Directory)}
   */
  @Deprecated
  public static DirectoryReader open(final Directory directory) throws CorruptIndexException, IOException {
    return DirectoryReader.open(directory);
  }
  
  /** Expert: Returns a IndexReader reading the index in the given
   *  Directory with the given termInfosIndexDivisor.
   * @param directory the index directory
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
   * @deprecated Use {@link DirectoryReader#open(Directory,int)}
   */
  @Deprecated
  public static DirectoryReader open(final Directory directory, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return DirectoryReader.open(directory, termInfosIndexDivisor);
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
   * @see DirectoryReader#openIfChanged(DirectoryReader,IndexWriter,boolean)
   *
   * @lucene.experimental
   * @deprecated Use {@link DirectoryReader#open(IndexWriter,boolean)}
   */
  @Deprecated
  public static DirectoryReader open(final IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
    return DirectoryReader.open(writer, applyAllDeletes);
  }

  /** Expert: returns an IndexReader reading the index in the given
   *  {@link IndexCommit}.
   * @param commit the commit point to open
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated Use {@link DirectoryReader#open(IndexCommit)}
   */
  @Deprecated
  public static DirectoryReader open(final IndexCommit commit) throws CorruptIndexException, IOException {
    return DirectoryReader.open(commit);
  }


  /** Expert: returns an IndexReader reading the index in the given
   *  {@link IndexCommit} and termInfosIndexDivisor.
   * @param commit the commit point to open
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
   * @deprecated Use {@link DirectoryReader#open(IndexCommit,int)}
   */
  @Deprecated
  public static DirectoryReader open(final IndexCommit commit, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return DirectoryReader.open(commit, termInfosIndexDivisor);
  }

  /** Retrieve term vectors for this document, or null if
   *  term vectors were not indexed.  The returned Fields
   *  instance acts like a single-document inverted index
   *  (the docID will be 0). */
  public abstract Fields getTermVectors(int docID)
          throws IOException;

  /** Retrieve term vector for this document and field, or
   *  null if term vectors were not indexed.  The returned
   *  Fields instance acts like a single-document inverted
   *  index (the docID will be 0). */
  public final Terms getTermVector(int docID, String field)
    throws IOException {
    Fields vectors = getTermVectors(docID);
    if (vectors == null) {
      return null;
    }
    return vectors.terms(field);
  }

  /** Returns the number of documents in this index. */
  public abstract int numDocs();

  /** Returns one greater than the largest possible document number.
   * This may be used to, e.g., determine how big to allocate an array which
   * will have an element for every document number in an index.
   */
  public abstract int maxDoc();

  /** Returns the number of deleted documents. */
  public final int numDeletedDocs() {
    return maxDoc() - numDocs();
  }

  /** Expert: visits the fields of a stored document, for
   *  custom processing/loading of each field.  If you
   *  simply want to load all fields, use {@link
   *  #document(int)}.  If you want to load a subset, use
   *  {@link DocumentStoredFieldVisitor}.  */
  public abstract void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException;
  
  /**
   * Returns the stored fields of the <code>n</code><sup>th</sup>
   * <code>Document</code> in this index.  This is just
   * sugar for using {@link DocumentStoredFieldVisitor}.
   * <p>
   * <b>NOTE:</b> for performance reasons, this method does not check if the
   * requested document is deleted, and therefore asking for a deleted document
   * may yield unspecified results. Usually this is not required, however you
   * can test if the doc is deleted by checking the {@link
   * Bits} returned from {@link MultiFields#getLiveDocs}.
   *
   * <b>NOTE:</b> only the content of a field is returned,
   * if that field was stored during indexing.  Metadata
   * like boost, omitNorm, IndexOptions, tokenized, etc.,
   * are not preserved.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  // TODO: we need a separate StoredField, so that the
  // Document returned here contains that class not
  // IndexableField
  public final Document document(int docID) throws CorruptIndexException, IOException {
    final DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
    document(docID, visitor);
    return visitor.getDocument();
  }

  /**
   * Like {@link #document(int)} but only loads the specified
   * fields.  Note that this is simply sugar for {@link
   * DocumentStoredFieldVisitor#DocumentStoredFieldVisitor(Set)}.
   */
  public final Document document(int docID, Set<String> fieldsToLoad) throws CorruptIndexException, IOException {
    final DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(fieldsToLoad);
    document(docID, visitor);
    return visitor.getDocument();
  }

  /** Returns true if any documents have been deleted */
  public abstract boolean hasDeletions();

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
   * Expert: Returns a the root {@link IndexReaderContext} for this
   * {@link IndexReader}'s sub-reader tree. Iff this reader is composed of sub
   * readers ,ie. this reader being a composite reader, this method returns a
   * {@link CompositeReaderContext} holding the reader's direct children as well as a
   * view of the reader tree's atomic leaf contexts. All sub-
   * {@link IndexReaderContext} instances referenced from this readers top-level
   * context are private to this reader and are not shared with another context
   * tree. For example, IndexSearcher uses this API to drive searching by one
   * atomic leaf reader at a time. If this reader is not composed of child
   * readers, this method returns an {@link AtomicReaderContext}.
   * <p>
   * Note: Any of the sub-{@link CompositeReaderContext} instances reference from this
   * top-level context holds a <code>null</code> {@link CompositeReaderContext#leaves}
   * reference. Only the top-level context maintains the convenience leaf-view
   * for performance reasons.
   * 
   * @lucene.experimental
   */
  public abstract IndexReaderContext getTopReaderContext();

  /** Expert: Returns a key for this IndexReader, so FieldCache/CachingWrapperFilter can find
   * it again.
   * This key must not have equals()/hashCode() methods, so &quot;equals&quot; means &quot;identical&quot;. */
  public Object getCoreCacheKey() {
    // Don't can ensureOpen since FC calls this (to evict)
    // on close
    return this;
  }

  /** Expert: Returns a key for this IndexReader that also includes deletions,
   * so FieldCache/CachingWrapperFilter can find it again.
   * This key must not have equals()/hashCode() methods, so &quot;equals&quot; means &quot;identical&quot;. */
  public Object getCombinedCoreAndDeletesKey() {
    // Don't can ensureOpen since FC calls this (to evict)
    // on close
    return this;
  }
  
  /** Returns the number of documents containing the 
   * <code>term</code>.  This method returns 0 if the term or
   * field does not exists.  This method does not take into
   * account deleted documents that have not yet been merged
   * away. */
  public final int docFreq(Term term) throws IOException {
    return docFreq(term.field(), term.bytes());
  }

  /** Returns the number of documents containing the
   * <code>term</code>.  This method returns 0 if the term or
   * field does not exists.  This method does not take into
   * account deleted documents that have not yet been merged
   * away. */
  public abstract int docFreq(String field, BytesRef term) throws IOException;
}
