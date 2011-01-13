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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ReaderUtil; // javadoc

import org.apache.lucene.index.DirectoryReader; // javadoc
import org.apache.lucene.index.MultiReader; // javadoc

/**
 * This class forces a composite reader (eg a {@link
 * MultiReader} or {@link DirectoryReader} or any other
 * IndexReader subclass that returns non-null from {@link
 * IndexReader#getSequentialSubReaders}) to emulate an
 * atomic reader.  This requires implementing the postings
 * APIs on-the-fly, using the static methods in {@link
 * MultiFields}, by stepping through the sub-readers to
 * merge fields/terms, appending docs, etc.
 *
 * <p>If you ever hit an UnsupportedOperationException saying
 * "please use MultiFields.XXX instead", the simple
 * but non-performant workaround is to wrap your reader
 * using this class.</p>
 *
 * <p><b>NOTE</b>: this class almost always results in a
 * performance hit.  If this is important to your use case,
 * it's better to get the sequential sub readers (see {@link
 * ReaderUtil#gatherSubReaders}, instead, and iterate through them
 * yourself.</p>
 */

public final class SlowMultiReaderWrapper extends FilterIndexReader {

  private final ReaderContext readerContext;
  private final Map<String,byte[]> normsCache = new HashMap<String,byte[]>();
  
  public SlowMultiReaderWrapper(IndexReader other) {
    super(other);
    readerContext = new AtomicReaderContext(this); // emulate atomic reader!
  }

  @Override
  public Fields fields() throws IOException {
    return MultiFields.getFields(in);
  }

  @Override
  public Bits getDeletedDocs() {
    return MultiFields.getDeletedDocs(in);
  }

  
  @Override
  public IndexReader[] getSequentialSubReaders() {
    return null;
  }

  @Override
  public synchronized byte[] norms(String field) throws IOException {
    ensureOpen();
    byte[] bytes = normsCache.get(field);
    if (bytes != null)
      return bytes;
    if (!hasNorms(field))
      return null;
    if (normsCache.containsKey(field)) // cached omitNorms, not missing key
      return null;
    
    bytes = MultiNorms.norms(in, field);
    normsCache.put(field, bytes);
    return bytes;
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    return readerContext;
  }
  
  @Override
  protected void doSetNorm(int n, String field, byte value)
      throws CorruptIndexException, IOException {
    synchronized(normsCache) {
      normsCache.remove(field);
    }
    in.doSetNorm(n, field, value);
  }
}
