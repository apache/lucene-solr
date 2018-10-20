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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;

/** 
 * A <code>FilterCodecReader</code> contains another CodecReader, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality.
 * <p><b>NOTE</b>: If this {@link FilterCodecReader} does not change the
 * content the contained reader, you could consider delegating calls to
 * {@link #getCoreCacheHelper()} and {@link #getReaderCacheHelper()}.
 */
public abstract class FilterCodecReader extends CodecReader {

  /** Get the wrapped instance by <code>reader</code> as long as this reader is
   *  an instance of {@link FilterCodecReader}.  */
  public static CodecReader unwrap(CodecReader reader) {
    while (reader instanceof FilterCodecReader) {
      reader = ((FilterCodecReader) reader).getDelegate();
    }
    return reader;
  }
  /**
   * The underlying CodecReader instance.
   */
  protected final CodecReader in;
  
  /**
   * Creates a new FilterCodecReader.
   * @param in the underlying CodecReader instance.
   */
  public FilterCodecReader(CodecReader in) {
    this.in = Objects.requireNonNull(in);
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    return in.getFieldsReader();
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    return in.getTermVectorsReader();
  }

  @Override
  public NormsProducer getNormsReader() {
    return in.getNormsReader();
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    return in.getDocValuesReader();
  }

  @Override
  public FieldsProducer getPostingsReader() {
    return in.getPostingsReader();
  }

  @Override
  public Bits getLiveDocs() {
    return in.getLiveDocs();
  }

  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
  }

  @Override
  public PointsReader getPointsReader() {
    return in.getPointsReader();
  }

  @Override
  public int numDocs() {
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    return in.maxDoc();
  }

  @Override
  public LeafMetaData getMetaData() {
    return in.getMetaData();
  }

  @Override
  protected void doClose() throws IOException {
    in.doClose();
  }

  @Override
  public long ramBytesUsed() {
    return in.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return in.getChildResources();
  }

  @Override
  public void checkIntegrity() throws IOException {
    in.checkIntegrity();
  }

  /** Returns the wrapped {@link CodecReader}. */
  public CodecReader getDelegate() {
    return in;
  }

  /**
   * Returns a filtered codec reader with the given live docs and numDocs.
   */
  static FilterCodecReader wrapLiveDocs(CodecReader reader, Bits liveDocs, int numDocs) {
    return new FilterCodecReader(reader) {
      @Override
      public CacheHelper getCoreCacheHelper() {
        return reader.getCoreCacheHelper();
      }
      @Override
      public CacheHelper getReaderCacheHelper() {
        return null; // we are altering live docs
      }
      @Override
      public Bits getLiveDocs() {
        return liveDocs;
      }
      @Override
      public int numDocs() {
        return numDocs;
      }
    };
  }
}
