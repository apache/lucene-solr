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
package org.apache.lucene.codecs.memory;


import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.packed.PackedInts;

/** In-memory docvalues format */
public class MemoryDocValuesFormat extends DocValuesFormat {

  /** Maximum length for each binary doc values field. */
  public static final int MAX_BINARY_FIELD_LENGTH = (1 << 15) - 2;
  
  final float acceptableOverheadRatio;
  
  /** 
   * Calls {@link #MemoryDocValuesFormat(float) 
   * MemoryDocValuesFormat(PackedInts.DEFAULT)} 
   */
  public MemoryDocValuesFormat() {
    this(PackedInts.DEFAULT);
  }
  
  /**
   * Creates a new MemoryDocValuesFormat with the specified
   * <code>acceptableOverheadRatio</code> for NumericDocValues.
   * @param acceptableOverheadRatio compression parameter for numerics. 
   *        Currently this is only used when the number of unique values is small.
   *        
   * @lucene.experimental
   */
  public MemoryDocValuesFormat(float acceptableOverheadRatio) {
    super("Memory");
    this.acceptableOverheadRatio = acceptableOverheadRatio;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new MemoryDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION, acceptableOverheadRatio);
  }
  
  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new MemoryDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  static final String DATA_CODEC = "MemoryDocValuesData";
  static final String DATA_EXTENSION = "mdvd";
  static final String METADATA_CODEC = "MemoryDocValuesMetadata";
  static final String METADATA_EXTENSION = "mdvm";
}
