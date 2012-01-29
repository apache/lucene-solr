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

import org.apache.lucene.index.AtomicReader.AtomicReaderContext;
import org.apache.lucene.search.SearcherManager; // javadocs
import org.apache.lucene.store.*;
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
public abstract class CompositeReader extends IndexReader {

  private CompositeReaderContext readerContext = null; // lazy init

  protected CompositeReader() { 
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
  public final CompositeReaderContext getTopReaderContext() {
    ensureOpen();
    // lazy init without thread safety for perf resaons: Building the readerContext twice does not hurt!
    if (readerContext == null) {
      assert getSequentialSubReaders() != null;
      readerContext = (CompositeReaderContext) ReaderUtil.buildReaderContext(this);
    }
    return readerContext;
  }
  
  /** Expert: returns the sequential sub readers that this
   *  reader is logically composed of. It contrast to previous
   *  Lucene versions may not return null.
   *  If this method returns an empty array, that means this
   *  reader is a null reader (for example a MultiReader
   *  that has no sub readers).
   */
  public abstract IndexReader[] getSequentialSubReaders();
  
  /**
   * {@link ReaderContext} for {@link CompositeReader} instance.
   * @lucene.experimental
   */
  public static final class CompositeReaderContext extends ReaderContext {
    private final ReaderContext[] children;
    private final AtomicReaderContext[] leaves;
    private final CompositeReader reader;

    /**
     * Creates a {@link CompositeReaderContext} for intermediate readers that aren't
     * not top-level readers in the current context
     */
    public CompositeReaderContext(CompositeReaderContext parent, CompositeReader reader,
        int ordInParent, int docbaseInParent, ReaderContext[] children) {
      this(parent, reader, ordInParent, docbaseInParent, children, null);
    }
    
    /**
     * Creates a {@link CompositeReaderContext} for top-level readers with parent set to <code>null</code>
     */
    public CompositeReaderContext(CompositeReader reader, ReaderContext[] children, AtomicReaderContext[] leaves) {
      this(null, reader, 0, 0, children, leaves);
    }
    
    private CompositeReaderContext(CompositeReaderContext parent, CompositeReader reader,
        int ordInParent, int docbaseInParent, ReaderContext[] children,
        AtomicReaderContext[] leaves) {
      super(parent, ordInParent, docbaseInParent);
      this.children = children;
      this.leaves = leaves;
      this.reader = reader;
    }

    @Override
    public AtomicReaderContext[] leaves() {
      return leaves;
    }
    
    
    @Override
    public ReaderContext[] children() {
      return children;
    }
    
    @Override
    public CompositeReader reader() {
      return reader;
    }
  }
}
