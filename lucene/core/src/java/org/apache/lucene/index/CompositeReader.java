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

import org.apache.lucene.search.SearcherManager; // javadocs
import org.apache.lucene.store.*;

/** Instances of this reader type can only
  be used to get stored fields from the underlying AtomicReaders,
  but it is not possible to directly retrieve postings. To do that, get
  the sub-readers via {@link #getSequentialSubReaders}.
  Alternatively, you can mimic an {@link AtomicReader} (with a serious slowdown),
  by wrapping composite readers with {@link SlowCompositeReaderWrapper}.
 
 <p>IndexReader instances for indexes on disk are usually constructed
 with a call to one of the static <code>DirectoryReader,open()</code> methods,
 e.g. {@link DirectoryReader#open(Directory)}. {@link DirectoryReader} implements
 the {@code CompositeReader} interface, it is not possible to directly get postings.
 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link
 #open(Directory)}.

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
public abstract class CompositeReader extends IndexReader {

  private volatile CompositeReaderContext readerContext = null; // lazy init

  protected CompositeReader() { 
    super();
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final IndexReader[] subReaders = getSequentialSubReaders();
    assert subReaders != null;
    if (subReaders.length > 0) {
      buffer.append(subReaders[0]);
      for (int i = 1; i < subReaders.length; ++i) {
        buffer.append(" ").append(subReaders[i]);
      }
    }
    buffer.append(')');
    return buffer.toString();
  }
  
  /** Expert: returns the sequential sub readers that this
   *  reader is logically composed of. It contrast to previous
   *  Lucene versions may not return null.
   *  If this method returns an empty array, that means this
   *  reader is a null reader (for example a MultiReader
   *  that has no sub readers).
   *  <p><b>Warning:</b> Don't modify the returned array!
   *  Doing so will corrupt the internal structure of this
   *  {@code CompositeReader}.
   */
  public abstract IndexReader[] getSequentialSubReaders();

  @Override
  public final CompositeReaderContext getTopReaderContext() {
    ensureOpen();
    // lazy init without thread safety for perf reasons: Building the readerContext twice does not hurt!
    if (readerContext == null) {
      assert getSequentialSubReaders() != null;
      readerContext = CompositeReaderContext.create(this);
    }
    return readerContext;
  }
}
