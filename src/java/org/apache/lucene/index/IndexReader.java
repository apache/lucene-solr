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
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** IndexReader is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable.

 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link #open(String)}.

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
 IndexReader using one of the static open methods that
 accepts the boolean readOnly parameter.  Such a reader has
 better concurrency as it's not necessary to synchronize on
 the isDeleted method.  Currently the default for readOnly
 is false, meaning if not specified you will get a
 read/write IndexReader.  But in 3.0 this default will
 change to true, meaning you must explicitly specify false
 if you want to make changes with the resulting IndexReader.
 </p>

 @version $Id$
*/
public abstract class IndexReader {

  // NOTE: in 3.0 this will change to true
  final static boolean READ_ONLY_DEFAULT = false;

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
    public static final FieldOption OMIT_TF = new FieldOption ("OMIT_TF");
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
  }

  private boolean closed;
  protected boolean hasChanges;
  
  private volatile int refCount;
  
  // for testing
  synchronized int getRefCount() {
    return refCount;
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
  public synchronized void incRef() {
    assert refCount > 0;
    ensureOpen();
    refCount++;
  }

  /**
   * Expert: decreases the refCount of this IndexReader
   * instance.  If the refCount drops to 0, then pending
   * changes (if any) are committed to the index and this
   * reader is closed.
   * 
   * @throws IOException in case an IOException occurs in commit() or doClose()
   *
   * @see #incRef
   */
  public synchronized void decRef() throws IOException {
    assert refCount > 0;
    ensureOpen();
    if (refCount == 1) {
      commit();
      doClose();
    }
    refCount--;
  }
  
  /** 
   * @deprecated will be deleted when IndexReader(Directory) is deleted
   * @see #directory()
   */
  private Directory directory;

  /**
   * Legacy Constructor for backwards compatibility.
   *
   * <p>
   * This Constructor should not be used, it exists for backwards 
   * compatibility only to support legacy subclasses that did not "own" 
   * a specific directory, but needed to specify something to be returned 
   * by the directory() method.  Future subclasses should delegate to the 
   * no arg constructor and implement the directory() method as appropriate.
   * 
   * @param directory Directory to be returned by the directory() method
   * @see #directory()
   * @deprecated - use IndexReader()
   */
  protected IndexReader(Directory directory) {
    this();
    this.directory = directory;
  }
  
  protected IndexReader() { 
    refCount = 1;
  }
  
  /**
   * @throws AlreadyClosedException if this IndexReader is closed
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (refCount <= 0) {
      throw new AlreadyClosedException("this IndexReader is closed");
    }
  }

  /** Returns a read/write IndexReader reading the index in an FSDirectory in the named
   path.  <b>NOTE</b>: starting in 3.0 this will return a readOnly IndexReader.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @param path the path to the index directory */
  public static IndexReader open(String path) throws CorruptIndexException, IOException {
    return open(FSDirectory.getDirectory(path), true, null, null, READ_ONLY_DEFAULT);
  }

  /** Returns a read/write IndexReader reading the index in an FSDirectory in the named
   * path.  <b>NOTE</b>: starting in 3.0 this will return a readOnly IndexReader.
   * @param path the path to the index directory
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(File path) throws CorruptIndexException, IOException {
    return open(FSDirectory.getDirectory(path), true, null, null, READ_ONLY_DEFAULT);
  }

  /** Returns a read/write IndexReader reading the index in
   * the given Directory. <b>NOTE</b>: starting in 3.0 this
   * will return a readOnly IndexReader.
   * @param directory the index directory
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory) throws CorruptIndexException, IOException {
    return open(directory, false, null, null, READ_ONLY_DEFAULT);
  }

  /** Returns a read/write or read only IndexReader reading the index in the given Directory.
   * @param directory the index directory
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, boolean readOnly) throws CorruptIndexException, IOException {
    return open(directory, false, null, null, readOnly);
  }

  /** Expert: returns a read/write IndexReader reading the index in the given
   * {@link IndexCommit}.  <b>NOTE</b>: starting in 3.0 this
   * will return a readOnly IndexReader.
   * @param commit the commit point to open
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final IndexCommit commit) throws CorruptIndexException, IOException {
    return open(commit.getDirectory(), false, null, commit, READ_ONLY_DEFAULT);
  }

  /** Expert: returns a read/write IndexReader reading the index in the given
   * Directory, with a custom {@link IndexDeletionPolicy}.
   * <b>NOTE</b>: starting in 3.0 this will return a
   * readOnly IndexReader.
   * @param directory the index directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, IndexDeletionPolicy deletionPolicy) throws CorruptIndexException, IOException {
    return open(directory, false, deletionPolicy, null, READ_ONLY_DEFAULT);
  }

  /** Expert: returns a read/write or read only IndexReader reading the index in the given
   * Directory, with a custom {@link IndexDeletionPolicy}.
   * <b>NOTE</b>: starting in 3.0 this will return a
   * readOnly IndexReader.
   * @param directory the index directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @param readOnly true if no changes (deletions, norms) will be made with this IndexReader
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final Directory directory, IndexDeletionPolicy deletionPolicy, boolean readOnly) throws CorruptIndexException, IOException {
    return open(directory, false, deletionPolicy, null, readOnly);
  }

  /** Expert: returns a read/write IndexReader reading the index in the given
   * Directory, using a specific commit and with a custom
   * {@link IndexDeletionPolicy}.  <b>NOTE</b>: starting in
   * 3.0 this will return a readOnly IndexReader.
   * @param commit the specific {@link IndexCommit} to open;
   * see {@link IndexReader#listCommits} to list all commits
   * in a directory
   * @param deletionPolicy a custom deletion policy (only used
   *  if you use this reader to perform deletes or to set
   *  norms); see {@link IndexWriter} for details.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static IndexReader open(final IndexCommit commit, IndexDeletionPolicy deletionPolicy) throws CorruptIndexException, IOException {
    return open(commit.getDirectory(), false, deletionPolicy, commit, READ_ONLY_DEFAULT);
  }

  /** Expert: returns a read/write or read only IndexReader reading the index in the given
   * Directory, using a specific commit and with a custom {@link IndexDeletionPolicy}.
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
    return open(commit.getDirectory(), false, deletionPolicy, commit, readOnly);
  }

  private static IndexReader open(final Directory directory, final boolean closeDirectory, final IndexDeletionPolicy deletionPolicy, final IndexCommit commit, final boolean readOnly) throws CorruptIndexException, IOException {
    return DirectoryIndexReader.open(directory, closeDirectory, deletionPolicy, commit, readOnly);
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
   * <b>Note:</b> The re-opened reader instance and the old instance might share
   * the same resources. For this reason no index modification operations 
   * (e. g. {@link #deleteDocument(int)}, {@link #setNorm(int, String, byte)}) 
   * should be performed using one of the readers until the old reader instance
   * is closed. <b>Otherwise, the behavior of the readers is undefined.</b> 
   * <p>   
   * You can determine whether a reader was actually reopened by comparing the
   * old instance with the instance returned by this method: 
   * <pre>
   * IndexReader reader = ... 
   * ...
   * IndexReader new = r.reopen();
   * if (new != reader) {
   *   ...     // reader was reopened
   *   reader.close(); 
   * }
   * reader = new;
   * ...
   * </pre>
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */  
  public synchronized IndexReader reopen() throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support reopen().");
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
    if (null != directory) {
      return directory;
    } else {
      throw new UnsupportedOperationException("This reader does not support this method.");  
    }
  }

  /**
   * Returns the time the index in the named directory was last modified.
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long lastModified(String directory) throws CorruptIndexException, IOException {
    return lastModified(new File(directory));
  }

  /**
   * Returns the time the index in the named directory was last modified. 
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long lastModified(File fileDirectory) throws CorruptIndexException, IOException {
    return ((Long) new SegmentInfos.FindSegmentsFile(fileDirectory) {
        public Object doBody(String segmentFileName) {
          return new Long(FSDirectory.fileModified(fileDirectory, segmentFileName));
        }
      }.run()).longValue();
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
        public Object doBody(String segmentFileName) throws IOException {
          return new Long(directory2.fileModified(segmentFileName));
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
  public static long getCurrentVersion(String directory) throws CorruptIndexException, IOException {
    return getCurrentVersion(new File(directory));
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
  public static long getCurrentVersion(File directory) throws CorruptIndexException, IOException {
    Directory dir = FSDirectory.getDirectory(directory);
    long version = getCurrentVersion(dir);
    dir.close();
    return version;
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
    return SegmentInfos.readCurrentVersion(directory);
  }

  /**
   * Reads commitUserData, previously passed to {@link
   * IndexWriter#commit(String)}, from current index
   * segments file.  This will return null if {@link
   * IndexWriter#commit(String)} has never been called for
   * this index.
   * 
   * @param directory where the index resides.
   * @return commit userData.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   *
   * @see #getCommitUserData()
   */
  public static String getCommitUserData(Directory directory) throws CorruptIndexException, IOException {
    return SegmentInfos.readCurrentUserData(directory);
  }

  /**
   * Version number when this IndexReader was opened. Not implemented in the IndexReader base class.
   * @throws UnsupportedOperationException unless overridden in subclass
   */
  public long getVersion() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }

  /**
   * Retrieve the String userData optionally passed to
   * IndexWriter#commit.  This will return null if {@link
   * IndexWriter#commit(String)} has never been called for
   * this index.
   *
   * @see #getCommitUserData(Directory)
   */
  public String getCommitUserData() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }

  /**<p>For IndexReader implementations that use
   * TermInfosReader to read terms, this sets the
   * indexDivisor to subsample the number of indexed terms
   * loaded into memory.  This has the same effect as {@link
   * IndexWriter#setTermIndexInterval} except that setting
   * must be done at indexing time while this setting can be
   * set per reader.  When set to N, then one in every
   * N*termIndexInterval terms in the index is loaded into
   * memory.  By setting this to a value > 1 you can reduce
   * memory usage, at the expense of higher latency when
   * loading a TermInfo.  The default value is 1.</p>
   *
   * <b>NOTE:</b> you must call this before the term
   * index is loaded.  If the index is already loaded, 
   * an IllegalStateException is thrown.
   * @throws IllegalStateException if the term index has already been loaded into memory
   */
  public void setTermInfosIndexDivisor(int indexDivisor) throws IllegalStateException {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }

  /** <p>For IndexReader implementations that use
   *  TermInfosReader to read terms, this returns the
   *  current indexDivisor.
   *  @see #setTermInfosIndexDivisor */
  public int getTermInfosIndexDivisor() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }

  /**
   * Check whether this IndexReader is still using the
   * current (i.e., most recently committed) version of the
   * index.  If a writer has committed any changes to the
   * index since this reader was opened, this will return
   * <code>false</code>, in which case you must open a new
   * IndexReader in order to see the changes.  See the
   * description of the <a href="IndexWriter.html#autoCommit"><code>autoCommit</code></a>
   * flag which controls when the {@link IndexWriter}
   * actually commits changes to the index.
   * 
   * <p>
   * Not implemented in the IndexReader base class.
   * </p>
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
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
   *  Return an array of term frequency vectors for the specified document.
   *  The array contains a vector for each vectorized field in the document.
   *  Each vector contains terms and frequencies for all terms in a given vectorized field.
   *  If no such fields existed, the method returns null. The term vectors that are
   * returned my either be of type TermFreqVector or of type TermPositionsVector if
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
   *  Return a term frequency vector for the specified document and field. The
   *  returned vector contains terms and frequencies for the terms in
   *  the specified field of this document, if the field had the storeTermVector
   *  flag set. If termvectors had been stored with positions or offsets, a 
   *  TermPositionsVector is returned.
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
   * If the directory does not exist or if there is no index in it.
   * <code>false</code> is returned.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   */
  public static boolean indexExists(String directory) {
    return indexExists(new File(directory));
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   */

  public static boolean indexExists(File directory) {
    return SegmentInfos.getCurrentSegmentGeneration(directory.list()) != -1;
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean indexExists(Directory directory) throws IOException {
    return SegmentInfos.getCurrentSegmentGeneration(directory) != -1;
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

  /** Returns the stored fields of the <code>n</code><sup>th</sup>
   <code>Document</code> in this index.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public Document document(int n) throws CorruptIndexException, IOException {
    ensureOpen();
    return document(n, null);
  }

  /**
   * Get the {@link org.apache.lucene.document.Document} at the <code>n</code><sup>th</sup> position. The {@link org.apache.lucene.document.FieldSelector}
   * may be used to determine what {@link org.apache.lucene.document.Field}s to load and how they should be loaded.
   * 
   * <b>NOTE:</b> If this Reader (more specifically, the underlying <code>FieldsReader</code>) is closed before the lazy {@link org.apache.lucene.document.Field} is
   * loaded an exception may be thrown.  If you want the value of a lazy {@link org.apache.lucene.document.Field} to be available after closing you must
   * explicitly load it or fetch the Document again with a new loader.
   * 
   *  
   * @param n Get the document at the <code>n</code><sup>th</sup> position
   * @param fieldSelector The {@link org.apache.lucene.document.FieldSelector} to use to determine what Fields should be loaded on the Document.  May be null, in which case all Fields will be loaded.
   * @return The stored fields of the {@link org.apache.lucene.document.Document} at the nth position
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * 
   * @see org.apache.lucene.document.Fieldable
   * @see org.apache.lucene.document.FieldSelector
   * @see org.apache.lucene.document.SetBasedFieldSelector
   * @see org.apache.lucene.document.LoadFirstFieldSelector
   */
  //When we convert to JDK 1.5 make this Set<String>
  public abstract Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException;
  
  

  /** Returns true if document <i>n</i> has been deleted */
  public abstract boolean isDeleted(int n);

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
   * every document.  This is used by the search code to score documents.
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   */
  public abstract byte[] norms(String field) throws IOException;

  /** Reads the byte-encoded normalization factor for the named field of every
   *  document.  This is used by the search code to score documents.
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   */
  public abstract void norms(String field, byte[] bytes, int offset)
    throws IOException;

  /** Expert: Resets the normalization factor for the named field of the named
   * document.  The norm represents the product of the field's {@link
   * org.apache.lucene.document.Fieldable#setBoost(float) boost} and its {@link Similarity#lengthNorm(String,
   * int) length normalization}.  Thus, to preserve the length normalization
   * values when resetting this, one should base the new value upon the old.
   *
   * @see #norms(String)
   * @see Similarity#decodeNorm(byte)
   * @throws StaleReaderException if the index has changed
   *  since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if there is a low-level IO error
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

  /** Expert: Resets the normalization factor for the named field of the named
   * document.
   *
   * @see #norms(String)
   * @see Similarity#decodeNorm(byte)
   * 
   * @throws StaleReaderException if the index has changed
   *  since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if there is a low-level IO error
   */
  public void setNorm(int doc, String field, float value)
          throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    ensureOpen();
    setNorm(doc, field, Similarity.encodeNorm(value));
  }

  /** Returns an enumeration of all the terms in the index. The
   * enumeration is ordered by Term.compareTo(). Each term is greater
   * than all that precede it in the enumeration. Note that after
   * calling terms(), {@link TermEnum#next()} must be called
   * on the resulting enumeration before calling other methods such as
   * {@link TermEnum#term()}.
   * @throws IOException if there is a low-level IO error
   */
  public abstract TermEnum terms() throws IOException;

  /** Returns an enumeration of all terms starting at a given term. If
   * the given term does not exist, the enumeration is positioned at the
   * first term greater than the supplied term. The enumeration is
   * ordered by Term.compareTo(). Each term is greater than all that
   * precede it in the enumeration.
   * @throws IOException if there is a low-level IO error
   */
  public abstract TermEnum terms(Term t) throws IOException;

  /** Returns the number of documents containing the term <code>t</code>.
   * @throws IOException if there is a low-level IO error
   */
  public abstract int docFreq(Term t) throws IOException;

  /** Returns an enumeration of all the documents which contain
   * <code>term</code>. For each document, the document number, the frequency of
   * the term in that document is also provided, for use in search scoring.
   * Thus, this method implements the mapping:
   * <p><ul>
   * Term &nbsp;&nbsp; =&gt; &nbsp;&nbsp; &lt;docNum, freq&gt;<sup>*</sup>
   * </ul>
   * <p>The enumeration is ordered by document number.  Each document number
   * is greater than all that precede it in the enumeration.
   * @throws IOException if there is a low-level IO error
   */
  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    TermDocs termDocs = termDocs();
    termDocs.seek(term);
    return termDocs;
  }

  /** Returns an unpositioned {@link TermDocs} enumerator.
   * @throws IOException if there is a low-level IO error
   */
  public abstract TermDocs termDocs() throws IOException;

  /** Returns an enumeration of all the documents which contain
   * <code>term</code>.  For each document, in addition to the document number
   * and frequency of the term in that document, a list of all of the ordinal
   * positions of the term in the document is available.  Thus, this method
   * implements the mapping:
   *
   * <p><ul>
   * Term &nbsp;&nbsp; =&gt; &nbsp;&nbsp; &lt;docNum, freq,
   * &lt;pos<sub>1</sub>, pos<sub>2</sub>, ...
   * pos<sub>freq-1</sub>&gt;
   * &gt;<sup>*</sup>
   * </ul>
   * <p> This positional information facilitates phrase and proximity searching.
   * <p>The enumeration is ordered by document number.  Each document number is
   * greater than all that precede it in the enumeration.
   * @throws IOException if there is a low-level IO error
   */
  public TermPositions termPositions(Term term) throws IOException {
    ensureOpen();
    TermPositions termPositions = termPositions();
    termPositions.seek(term);
    return termPositions;
  }

  /** Returns an unpositioned {@link TermPositions} enumerator.
   * @throws IOException if there is a low-level IO error
   */
  public abstract TermPositions termPositions() throws IOException;



  /** Deletes the document numbered <code>docNum</code>.  Once a document is
   * deleted it will not appear in TermDocs or TermPostitions enumerations.
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
    TermDocs docs = termDocs(term);
    if (docs == null) return 0;
    int n = 0;
    try {
      while (docs.next()) {
        deleteDocument(docs.doc());
        n++;
      }
    } finally {
      docs.close();
    }
    return n;
  }

  /** Undeletes all documents currently marked as deleted in this index.
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
   * Commit changes resulting from delete, undeleteAll, or
   * setNorm operations
   *
   * If an exception is hit, then either no changes or all
   * changes will have been committed to the index
   * (transactional semantics).
   * @throws IOException if there is a low-level IO error
   */
  protected final synchronized void commit() throws IOException {
    if(hasChanges){
      doCommit();
    }
    hasChanges = false;
  }

  /** Implements commit. */
  protected abstract void doCommit() throws IOException;

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
  public abstract Collection getFieldNames(FieldOption fldOption);

  /**
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   * @param directory the directory to check for a lock
   * @throws IOException if there is a low-level IO error
   * @deprecated Please use {@link IndexWriter#isLocked(Directory)} instead
   */
  public static boolean isLocked(Directory directory) throws IOException {
    return
      directory.makeLock(IndexWriter.WRITE_LOCK_NAME).isLocked();
  }

  /**
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   * @param directory the directory to check for a lock
   * @throws IOException if there is a low-level IO error
   * @deprecated Please use {@link IndexWriter#isLocked(String)} instead
   */
  public static boolean isLocked(String directory) throws IOException {
    Directory dir = FSDirectory.getDirectory(directory);
    boolean result = isLocked(dir);
    dir.close();
    return result;
  }

  /**
   * Forcibly unlocks the index in the named directory.
   * <P>
   * Caution: this should only be used by failure recovery code,
   * when it is known that no other process nor thread is in fact
   * currently accessing this index.
   * @deprecated Please use {@link IndexWriter#unlock(Directory)} instead
   */
  public static void unlock(Directory directory) throws IOException {
    directory.makeLock(IndexWriter.WRITE_LOCK_NAME).release();
  }

  /**
   * Expert: return the IndexCommit that this reader has
   * opened.  This method is only implemented by those
   * readers that correspond to a Directory with its own
   * segments_N file.
   *
   * <p><b>WARNING</b>: this API is new and experimental and
   * may suddenly change.</p>
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
    CompoundFileReader cfr = null;

    try {
      File file = new File(filename);
      String dirname = file.getAbsoluteFile().getParent();
      filename = file.getName();
      dir = FSDirectory.getDirectory(dirname);
      cfr = new CompoundFileReader(dir, filename);

      String [] files = cfr.list();
      Arrays.sort(files);   // sort the array of filename so that the output is more readable

      for (int i = 0; i < files.length; ++i) {
        long len = cfr.fileLength(files[i]);

        if (extract) {
          System.out.println("extract " + files[i] + " with " + len + " bytes to local directory...");
          IndexInput ii = cfr.openInput(files[i]);

          FileOutputStream f = new FileOutputStream(files[i]);

          // read and write with a small buffer, which is more effectiv than reading byte by byte
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
   *  it by calling {@link IndexReader#open(IndexCommit)}
   *  There must be at least one commit in
   *  the Directory, else this method throws {@link
   *  java.io.IOException}.  Note that if a commit is in
   *  progress while this method is running, that commit
   *  may or may not be returned array.  */
  public static Collection listCommits(Directory dir) throws IOException {
    return DirectoryIndexReader.listCommits(dir);
  }
}
