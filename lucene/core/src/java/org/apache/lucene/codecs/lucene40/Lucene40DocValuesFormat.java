package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

// NOTE: not registered in SPI, doesnt respect segment suffix, etc
// for back compat only!
public class Lucene40DocValuesFormat extends DocValuesFormat {
  
  public Lucene40DocValuesFormat() {
    super("Lucene40");
  }
  
  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
  
  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    String filename = IndexFileNames.segmentFileName(state.segmentInfo.name, 
                                                     "dv", 
                                                     IndexFileNames.COMPOUND_FILE_EXTENSION);
    return new Lucene40DocValuesReader(state, filename, Lucene40FieldInfosReader.LEGACY_DV_TYPE_KEY);
  }
  
  // constants for VAR_INTS
  static final String VAR_INTS_CODEC_NAME = "PackedInts";
  static final int VAR_INTS_VERSION_START = 0;
  static final int VAR_INTS_VERSION_CURRENT = VAR_INTS_VERSION_START;
  
  static final byte VAR_INTS_PACKED = 0x00;
  static final byte VAR_INTS_FIXED_64 = 0x01;
  
  // constants for FIXED_INTS_8, FIXED_INTS_16, FIXED_INTS_32, FIXED_INTS_64
  static final String INTS_CODEC_NAME = "Ints";
  static final int INTS_VERSION_START = 0;
  static final int INTS_VERSION_CURRENT = INTS_VERSION_START;
  
  // constants for FLOAT_32, FLOAT_64
  static final String FLOATS_CODEC_NAME = "Floats";
  static final int FLOATS_VERSION_START = 0;
  static final int FLOATS_VERSION_CURRENT = FLOATS_VERSION_START;
}
