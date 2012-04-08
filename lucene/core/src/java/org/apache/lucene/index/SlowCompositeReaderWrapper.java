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
 * MultiReader} or {@link DirectoryReader}) to emulate an
 * atomic reader.  This requires implementing the postings
 * APIs on-the-fly, using the static methods in {@link
 * MultiFields}, {@link MultiDocValues}, 
 * by stepping through the sub-readers to merge fields/terms, 
 * appending docs, etc.
 *
 * <p><b>NOTE</b>: this class almost always results in a
 * performance hit.  If this is important to your use case,
 * it's better to get the sequential sub readers (see {@link
 * ReaderUtil#gatherSubReaders}, instead, and iterate through them
 * yourself.</p>
 */

public final class SlowCompositeReaderWrapper extends AtomicReader {

  private final CompositeReader in;
  private final Map<String, DocValues> normsCache = new HashMap<String, DocValues>();
  private final Fields fields;
  private final Bits liveDocs;
  
  /** This method is sugar for getting an {@link AtomicReader} from
   * an {@link IndexReader} of any kind. If the reader is already atomic,
   * it is returned unchanged, otherwise wrapped by this class.
   */
  public static AtomicReader wrap(IndexReader reader) throws IOException {
    if (reader instanceof CompositeReader) {
      return new SlowCompositeReaderWrapper((CompositeReader) reader);
    } else {
      assert reader instanceof AtomicReader;
      return (AtomicReader) reader;
    }
  }
  
  public SlowCompositeReaderWrapper(CompositeReader reader) throws IOException {
    super();
    in = reader;
    fields = MultiFields.getFields(in);
    liveDocs = MultiFields.getLiveDocs(in);
    in.registerParentReader(this);
  }

  @Override
  public String toString() {
    return "SlowCompositeReaderWrapper(" + in + ")";
  }

  @Override
  public Fields fields() throws IOException {
    ensureOpen();
    return fields;
  }

  @Override
  public DocValues docValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getDocValues(in, field);
  }
  
  @Override
  public synchronized DocValues normValues(String field) throws IOException {
    ensureOpen();
    DocValues values = normsCache.get(field);
    if (values == null) {
      values = MultiDocValues.getNormDocValues(in, field);
      normsCache.put(field, values);
    }
    return values;
  }
  
  @Override
  public Fields getTermVectors(int docID)
          throws IOException {
    ensureOpen();
    return in.getTermVectors(docID);
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return liveDocs;
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    return MultiFields.getMergedFieldInfos(in);
  }
  
  @Override
  public boolean hasDeletions() {
    ensureOpen();
    return liveDocs != null;
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }

  @Override
  protected void doClose() throws IOException {
    // TODO: as this is a wrapper, should we really close the delegate?
    in.close();
  }
}
