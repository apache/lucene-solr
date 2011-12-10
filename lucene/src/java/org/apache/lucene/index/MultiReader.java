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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.codecs.PerDocProducer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.MapBackedSet;

/** An IndexReader which reads multiple indexes, appending
 *  their content. */
public class MultiReader extends IndexReader implements Cloneable {
  protected IndexReader[] subReaders;
  private final ReaderContext topLevelContext;
  private int[] starts;                           // 1st docno for each segment
  private boolean[] decrefOnClose;                // remember which subreaders to decRef on close
  private int maxDoc = 0;
  private int numDocs = -1;
  private boolean hasDeletions = false;
  
 /**
  * <p>Construct a MultiReader aggregating the named set of (sub)readers.
  * <p>Note that all subreaders are closed if this Multireader is closed.</p>
  * @param subReaders set of (sub)readers
  */
  public MultiReader(IndexReader... subReaders) throws IOException {
    topLevelContext = initialize(subReaders, true);
  }

  /**
   * <p>Construct a MultiReader aggregating the named set of (sub)readers.
   * @param closeSubReaders indicates whether the subreaders should be closed
   * when this MultiReader is closed
   * @param subReaders set of (sub)readers
   */
  public MultiReader(IndexReader[] subReaders, boolean closeSubReaders) throws IOException {
    topLevelContext = initialize(subReaders, closeSubReaders);
  }
  
  private ReaderContext initialize(IndexReader[] subReaders, boolean closeSubReaders) throws IOException {
    this.subReaders =  subReaders.clone();
    starts = new int[subReaders.length + 1];    // build starts array
    decrefOnClose = new boolean[subReaders.length];
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();      // compute maxDocs

      if (!closeSubReaders) {
        subReaders[i].incRef();
        decrefOnClose[i] = true;
      } else {
        decrefOnClose[i] = false;
      }
      
      if (subReaders[i].hasDeletions()) {
        hasDeletions = true;
      }
    }
    starts[subReaders.length] = maxDoc;
    readerFinishedListeners = new MapBackedSet<ReaderFinishedListener>(new ConcurrentHashMap<ReaderFinishedListener,Boolean>());
    return ReaderUtil.buildReaderContext(this);
  }

  @Override
  public Fields fields() throws IOException {
    throw new UnsupportedOperationException("please use MultiFields.getFields, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Fields");
  }

  /**
   * Tries to reopen the subreaders.
   * <br>
   * If one or more subreaders could be re-opened (i. e. IndexReader.openIfChanged(subReader) 
   * returned a new instance), then a new MultiReader instance 
   * is returned, otherwise this instance is returned.
   * <p>
   * A re-opened instance might share one or more subreaders with the old 
   * instance. Index modification operations result in undefined behavior
   * when performed before the old instance is closed.
   * (see {@link IndexReader#openIfChanged}).
   * <p>
   * If subreaders are shared, then the reference count of those
   * readers is increased to ensure that the subreaders remain open
   * until the last referring reader is closed.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error 
   */
  @Override
  protected synchronized IndexReader doOpenIfChanged() throws CorruptIndexException, IOException {
    return doOpenIfChanged(false);
  }
  
  /**
   * Clones the subreaders.
   * (see {@link IndexReader#clone()}).
   * <br>
   * <p>
   * If subreaders are shared, then the reference count of those
   * readers is increased to ensure that the subreaders remain open
   * until the last referring reader is closed.
   */
  @Override
  public synchronized Object clone() {
    try {
      return doOpenIfChanged(true);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  @Override
  public Bits getLiveDocs() {
    throw new UnsupportedOperationException("please use MultiFields.getLiveDocs, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Bits liveDocs");
  }

  /**
   * If clone is true then we clone each of the subreaders
   * @param doClone
   * @return New IndexReader, or null if open/clone is not necessary
   * @throws CorruptIndexException
   * @throws IOException
   */
  protected IndexReader doOpenIfChanged(boolean doClone) throws CorruptIndexException, IOException {
    ensureOpen();
    
    boolean changed = false;
    IndexReader[] newSubReaders = new IndexReader[subReaders.length];
    
    boolean success = false;
    try {
      for (int i = 0; i < subReaders.length; i++) {
        if (doClone) {
          newSubReaders[i] = (IndexReader) subReaders[i].clone();
          changed = true;
        } else {
          final IndexReader newSubReader = IndexReader.openIfChanged(subReaders[i]);
          if (newSubReader != null) {
            newSubReaders[i] = newSubReader;
            changed = true;
          } else {
            newSubReaders[i] = subReaders[i];
          }
        }
      }
      success = true;
    } finally {
      if (!success && changed) {
        for (int i = 0; i < newSubReaders.length; i++) {
          if (newSubReaders[i] != subReaders[i]) {
            try {
              newSubReaders[i].close();
            } catch (IOException ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }

    if (changed) {
      boolean[] newDecrefOnClose = new boolean[subReaders.length];
      for (int i = 0; i < subReaders.length; i++) {
        if (newSubReaders[i] == subReaders[i]) {
          newSubReaders[i].incRef();
          newDecrefOnClose[i] = true;
        }
      }
      MultiReader mr = new MultiReader(newSubReaders);
      mr.decrefOnClose = newDecrefOnClose;
      return mr;
    } else {
      return null;
    }
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    int i = readerIndex(docID);        // find segment num
    return subReaders[i].getTermVectors(docID - starts[i]); // dispatch to segment
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    // NOTE: multiple threads may wind up init'ing
    // numDocs... but that's harmless
    if (numDocs == -1) {        // check cache
      int n = 0;                // cache miss--recompute
      for (int i = 0; i < subReaders.length; i++)
        n += subReaders[i].numDocs();      // sum from readers
      numDocs = n;
    }
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    int i = readerIndex(docID);                          // find segment num
    subReaders[i].document(docID - starts[i], visitor);    // dispatch to segment reader
  }

  @Override
  public boolean hasDeletions() {
    ensureOpen();
    return hasDeletions;
  }

  private int readerIndex(int n) {    // find reader for doc n:
    return DirectoryReader.readerIndex(n, this.starts, this.subReaders.length);
  }
  
  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    for (int i = 0; i < subReaders.length; i++) {
      if (subReaders[i].hasNorms(field)) return true;
    }
    return false;
  }
  
  @Override
  public synchronized byte[] norms(String field) throws IOException {
    throw new UnsupportedOperationException("please use MultiNorms.norms, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level norms");
  }
  
  @Override
  public int docFreq(String field, BytesRef t) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++) {
      total += subReaders[i].docFreq(field, t);
    }
    return total;
  }

  @Override
  protected synchronized void doClose() throws IOException {
    for (int i = 0; i < subReaders.length; i++) {
      if (decrefOnClose[i]) {
        subReaders[i].decRef();
      } else {
        subReaders[i].close();
      }
    }
  }
  
  @Override
  public Collection<String> getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    return DirectoryReader.getFieldNames(fieldNames, this.subReaders);
  }  
  
  /**
   * Checks recursively if all subreaders are up to date. 
   */
  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    for (int i = 0; i < subReaders.length; i++) {
      if (!subReaders[i].isCurrent()) {
        return false;
      }
    }
    
    // all subreaders are up to date
    return true;
  }
  
  /** Not implemented.
   * @throws UnsupportedOperationException
   */
  @Override
  public long getVersion() {
    throw new UnsupportedOperationException("MultiReader does not support this method.");
  }
  
  @Override
  public IndexReader[] getSequentialSubReaders() {
    return subReaders;
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    ensureOpen();
    return topLevelContext;
  }

  @Override
  public void addReaderFinishedListener(ReaderFinishedListener listener) {
    super.addReaderFinishedListener(listener);
    for(IndexReader sub : subReaders) {
      sub.addReaderFinishedListener(listener);
    }
  }

  @Override
  public void removeReaderFinishedListener(ReaderFinishedListener listener) {
    super.removeReaderFinishedListener(listener);
    for(IndexReader sub : subReaders) {
      sub.removeReaderFinishedListener(listener);
    }
  }

  @Override
  public DocValues docValues(String field) throws IOException {
    throw new UnsupportedOperationException("please use MultiDocValues#getDocValues, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level DocValues");
  }
}
