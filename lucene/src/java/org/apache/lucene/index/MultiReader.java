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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MapBackedSet;

/** An IndexReader which reads multiple indexes, appending
 *  their content. */
public class MultiReader extends BaseMultiReader<IndexReader> {
  private boolean[] decrefOnClose; // remember which subreaders to decRef on close
  
 /**
  * <p>Construct a MultiReader aggregating the named set of (sub)readers.
  * <p>Note that all subreaders are closed if this Multireader is closed.</p>
  * @param subReaders set of (sub)readers
  */
  public MultiReader(IndexReader... subReaders) throws IOException {
    this(subReaders, true);
  }

  /**
   * <p>Construct a MultiReader aggregating the named set of (sub)readers.
   * @param subReaders set of (sub)readers
   * @param closeSubReaders indicates whether the subreaders should be closed
   * when this MultiReader is closed
   */
  public MultiReader(IndexReader[] subReaders, boolean closeSubReaders) throws IOException {
    super(subReaders.clone());
    this.readerFinishedListeners = new MapBackedSet<ReaderFinishedListener>(new ConcurrentHashMap<ReaderFinishedListener,Boolean>());
    decrefOnClose = new boolean[subReaders.length];
    for (int i = 0; i < subReaders.length; i++) {
      if (!closeSubReaders) {
        subReaders[i].incRef();
        decrefOnClose[i] = true;
      } else {
        decrefOnClose[i] = false;
      }
    }
  }

  @Override
  protected synchronized IndexReader doOpenIfChanged() throws CorruptIndexException, IOException {
    return doReopen(false);
  }
  
  @Override
  public synchronized Object clone() {
    try {
      return doReopen(true);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private IndexReader doReopen(boolean doClone) throws CorruptIndexException, IOException {
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
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (int i = 0; i < subReaders.length; i++) {
      try {
        if (decrefOnClose[i]) {
          subReaders[i].decRef();
        } else {
          subReaders[i].close();
        }
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }
  
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

}
