package org.apache.lucene.codecs.lucene40.values;

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
import java.util.Comparator;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.PerDocProducerBase;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.lucene40.values.Writer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.DocValues.Type; // javadoc
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Abstract base class for PerDocConsumer implementations
 *
 * @lucene.experimental
 */
public abstract class DocValuesWriterBase extends PerDocConsumer {
  /** Segment name to use when writing files. */
  protected final String segmentName;
  private final Counter bytesUsed;

  /** {@link IOContext} to use when writing files. */
  protected final IOContext context;

  private final float acceptableOverheadRatio;

  /**
   * Filename extension for index files
   */
  public static final String INDEX_EXTENSION = "idx";
  
  /**
   * Filename extension for data files.
   */
  public static final String DATA_EXTENSION = "dat";

  /**
   * Creates {@code DocValuesWriterBase}, using {@link
   * PackedInts#FAST}.
   * @param state The state to initiate a {@link PerDocConsumer} instance
   */
  protected DocValuesWriterBase(PerDocWriteState state) {
    this(state, PackedInts.FAST);
  }

  /**
   * Creates {@code DocValuesWriterBase}.
   * @param state The state to initiate a {@link PerDocConsumer} instance
   * @param acceptableOverheadRatio
   *          how to trade space for speed. This option is only applicable for
   *          docvalues of type {@link Type#BYTES_FIXED_SORTED} and
   *          {@link Type#BYTES_VAR_SORTED}.
   * @see PackedInts#getReader(org.apache.lucene.store.DataInput)
   */
  protected DocValuesWriterBase(PerDocWriteState state, float acceptableOverheadRatio) {
    this.segmentName = state.segmentInfo.name;
    this.bytesUsed = state.bytesUsed;
    this.context = state.context;
    this.acceptableOverheadRatio = acceptableOverheadRatio;
  }

  /** Returns the {@link Directory} that files should be
   *  written to. */
  protected abstract Directory getDirectory() throws IOException;
  
  @Override
  public void close() throws IOException {   
  }

  @Override
  public DocValuesConsumer addValuesField(Type valueType, FieldInfo field) throws IOException {
    return Writer.create(valueType,
        PerDocProducerBase.docValuesId(segmentName, field.number), 
        getDirectory(), getComparator(), bytesUsed, context, acceptableOverheadRatio);
  }


  /** Returns the comparator used to sort {@link BytesRef}
   *  values. */
  public Comparator<BytesRef> getComparator() throws IOException {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }
}
