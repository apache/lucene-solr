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

import java.io.IOException;

import org.apache.lucene.search.SearcherManager; // javadocs
import org.apache.lucene.store.*;
import org.apache.lucene.util.ReaderUtil;

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
  
  /** Expert: returns the sequential sub readers that this
   *  reader is logically composed of. It contrast to previous
   *  Lucene versions may not return null.
   *  If this method returns an empty array, that means this
   *  reader is a null reader (for example a MultiReader
   *  that has no sub readers).
   */
  public abstract IndexReader[] getSequentialSubReaders();

  @Override
  public final CompositeReaderContext getTopReaderContext() {
    ensureOpen();
    // lazy init without thread safety for perf reasons: Building the readerContext twice does not hurt!
    if (readerContext == null) {
      assert getSequentialSubReaders() != null;
      readerContext = new ReaderContextBuilder(this).build();
    }
    return readerContext;
  }
  
  private static class ReaderContextBuilder {
    private final CompositeReader reader;
    private final AtomicReaderContext[] leaves;
    private int leafOrd = 0;
    private int leafDocBase = 0;
    public ReaderContextBuilder(CompositeReader reader) {
      this.reader = reader;
      leaves = new AtomicReaderContext[numLeaves(reader)];
    }
    
    public CompositeReaderContext build() {
      return (CompositeReaderContext) build(null, reader, 0, 0);
    }
    
    private IndexReaderContext build(CompositeReaderContext parent, IndexReader reader, int ord, int docBase) {
      if (reader instanceof AtomicReader) {
        final AtomicReader ar = (AtomicReader) reader;
        final AtomicReaderContext atomic = new AtomicReaderContext(parent, ar, ord, docBase, leafOrd, leafDocBase);
        leaves[leafOrd++] = atomic;
        leafDocBase += reader.maxDoc();
        return atomic;
      } else {
        final CompositeReader cr = (CompositeReader) reader;
        final IndexReader[] sequentialSubReaders = cr.getSequentialSubReaders();
        final IndexReaderContext[] children = new IndexReaderContext[sequentialSubReaders.length];
        final CompositeReaderContext newParent;
        if (parent == null) {
          newParent = new CompositeReaderContext(cr, children, leaves);
        } else {
          newParent = new CompositeReaderContext(parent, cr, ord, docBase, children);
        }
        int newDocBase = 0;
        for (int i = 0; i < sequentialSubReaders.length; i++) {
          children[i] = build(newParent, sequentialSubReaders[i], i, newDocBase);
          newDocBase += sequentialSubReaders[i].maxDoc();
        }
        return newParent;
      }
    }
    
    private int numLeaves(IndexReader reader) {
      final int[] numLeaves = new int[1];
      try {
        new ReaderUtil.Gather(reader) {
          @Override
          protected void add(int base, AtomicReader r) {
            numLeaves[0]++;
          }
        }.run();
      } catch (IOException ioe) {
        // won't happen
        throw new RuntimeException(ioe);
      }
      return numLeaves[0];
    }
    
  }
}
