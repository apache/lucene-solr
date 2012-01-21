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
import java.util.List;
import java.util.Map;
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

 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link
 #open(Directory)}.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral--they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 <p>
 <b>NOTE</b>: for backwards API compatibility, several methods are not listed 
 as abstract, but have no useful implementations in this base class and 
 instead always throw UnsupportedOperationException.  Subclasses are 
 strongly encouraged to override these methods, but in many cases may not 
 need to.
 </p>

 <p>

 <a name="thread-safety"></a><p><b>NOTE</b>: {@link
 IndexReader} instances are completely thread
 safe, meaning multiple threads can call any of its methods,
 concurrently.  If your application requires external
 synchronization, you should <b>not</b> synchronize on the
 <code>IndexReader</code> instance; use your own
 (non-Lucene) objects instead.
*/
public abstract class CompositeIndexReader extends IndexReader {

  protected CompositeIndexReader() { 
    super();
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
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

  @Override
  public abstract CompositeReaderContext getTopReaderContext();

  /**
   * If the index has changed since it was opened, open and return a new reader;
   * else, return {@code null}.
   * 
   * @see #openIfChanged(IndexReader)
   */
  protected CompositeIndexReader doOpenIfChanged() throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support reopen().");
  }
  
  /**
   * If the index has changed since it was opened, open and return a new reader;
   * else, return {@code null}.
   * 
   * @see #openIfChanged(IndexReader, IndexCommit)
   */
  protected CompositeIndexReader doOpenIfChanged(final IndexCommit commit) throws CorruptIndexException, IOException {
    throw new UnsupportedOperationException("This reader does not support reopen(IndexCommit).");
  }

  /**
   * If the index has changed since it was opened, open and return a new reader;
   * else, return {@code null}.
   * 
   * @see #openIfChanged(IndexReader, IndexWriter, boolean)
   */
  protected CompositeIndexReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
    return writer.getReader(applyAllDeletes);
  }

  /**
   * Version number when this IndexReader was opened. Not
   * implemented in the IndexReader base class.
   *
   * <p>If this reader is based on a Directory (ie, was
   * created by calling {@link #open}, or {@link #openIfChanged} on
   * a reader based on a Directory), then this method
   * returns the version recorded in the commit that the
   * reader opened.  This version is advanced every time
   * {@link IndexWriter#commit} is called.</p>
   *
   * <p>If instead this reader is a near real-time reader
   * (ie, obtained by a call to {@link
   * IndexWriter#getReader}, or by calling {@link #openIfChanged}
   * on a near real-time reader), then this method returns
   * the version of the last commit done by the writer.
   * Note that even as further changes are made with the
   * writer, the version will not changed until a commit is
   * completed.  Thus, you should not rely on this method to
   * determine when a near real-time reader should be
   * opened.  Use {@link #isCurrent} instead.</p>
   */
  public abstract long getVersion();

  /**
   * Check whether any new changes have occurred to the
   * index since this reader was opened.
   *
   * <p>If this reader is based on a Directory (ie, was
   * created by calling {@link #open}, or {@link #openIfChanged} on
   * a reader based on a Directory), then this method checks
   * if any further commits (see {@link IndexWriter#commit}
   * have occurred in that directory).</p>
   *
   * <p>If instead this reader is a near real-time reader
   * (ie, obtained by a call to {@link
   * IndexWriter#getReader}, or by calling {@link #openIfChanged}
   * on a near real-time reader), then this method checks if
   * either a new commit has occurred, or any new
   * uncommitted changes have taken place via the writer.
   * Note that even if the writer has only performed
   * merging, this method will still return false.</p>
   *
   * <p>In any event, if this returns false, you should call
   * {@link #openIfChanged} to get a new reader that sees the
   * changes.</p>
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException           if there is a low-level IO error
   * @throws UnsupportedOperationException unless overridden in subclass
   */
  public abstract boolean isCurrent() throws CorruptIndexException, IOException;

  /**
   * Returns the time the index in the named directory was last modified. 
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long lastModified(final Directory directory) throws CorruptIndexException, IOException {
    return ((Long) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      public Object doBody(String segmentFileName) throws IOException {
        return Long.valueOf(directory.fileModified(segmentFileName));
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
    return SegmentInfos.readCurrentVersion(directory);
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
  public static Map<String, String> getCommitUserData(Directory directory) throws CorruptIndexException, IOException {
    return SegmentInfos.readCurrentUserData(directory);
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
  
  /** Expert: returns the sequential sub readers that this
   *  reader is logically composed of. If this reader is not composed
   *  of sequential child readers, it should return null.
   *  If this method returns an empty array, that means this
   *  reader is a null reader (for example a MultiReader
   *  that has no sub readers).
   */
  public abstract IndexReader[] getSequentialSubReaders();
  
  /** For IndexReader implementations that use
   *  TermInfosReader to read terms, this returns the
   *  current indexDivisor as specified when the reader was
   *  opened.
   */
  public int getTermInfosIndexDivisor() {
    throw new UnsupportedOperationException("This reader does not support this method.");
  }
  
}
