package org.apache.lucene.index;

/*
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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.util.Bits;

/**  A <code>FilterLeafReader</code> contains another LeafReader, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * <code>FilterLeafReader</code> itself simply implements all abstract methods
 * of <code>IndexReader</code> with versions that pass all requests to the
 * contained index reader. Subclasses of <code>FilterLeafReader</code> may
 * further override some of these methods and may also provide additional
 * methods and fields.
 * <p><b>NOTE</b>: If you override {@link #getLiveDocs()}, you will likely need
 * to override {@link #numDocs()} as well and vice-versa.
 * <p><b>NOTE</b>: If this {@link FilterLeafReader} does not change the
 * content the contained reader, you could consider overriding
 * {@link #getCoreCacheKey()} so that
 * {@link CachingWrapperFilter} shares the same entries for this atomic reader
 * and the wrapped one.
 */
public class FilterLeafReader2 extends LeafReader2 {

  /** Get the wrapped instance by <code>reader</code> as long as this reader is
   *  an instance of {@link FilterLeafReader}.  */
  public static LeafReader2 unwrap(LeafReader2 reader) {
    while (reader instanceof FilterLeafReader2) {
      reader = ((FilterLeafReader2) reader).in;
    }
    return reader;
  }
  
  /** The underlying LeafReader. */
  protected final LeafReader2 in;
  
  /**
   * <p>Construct a FilterLeafReader based on the specified base reader.
   * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
   * @param in specified base reader.
   */
  public FilterLeafReader2(LeafReader2 in) {
    super();
    if (in == null) {
      throw new NullPointerException("incoming LeafReader cannot be null");
    }
    this.in = in;
    in.registerParentReader(this);
  }
  
  @Override
  protected TermVectorsReader getTermVectorsReader() {
    return in.getTermVectorsReader();
  }

  @Override
  protected StoredFieldsReader getFieldsReader() {
    return in.getFieldsReader();
  }

  @Override
  protected NormsProducer getNormsReader() {
    return in.getNormsReader();
  }

  @Override
  protected DocValuesProducer getDocValuesReader() {
    return in.getDocValuesReader();
  }

  @Override
  protected FieldsProducer getPostingsReader() {
    return in.getPostingsReader();
  }

  @Override
  public void addCoreClosedListener(CoreClosedListener listener) {
    in.addCoreClosedListener(listener);
  }

  @Override
  public void removeCoreClosedListener(CoreClosedListener listener) {
    in.removeCoreClosedListener(listener);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return in.getLiveDocs();
  }
  
  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
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
  protected void doClose() throws IOException {
    in.close();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("FilterLeafReader(");
    buffer.append(in);
    buffer.append(')');
    return buffer.toString();
  }
}
