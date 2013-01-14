package org.apache.lucene.codecs.diskdv;

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

import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

// nocommit fix this
/**
 * Internally there are only 2 field types:
 * BINARY: a big byte[]
 * NUMERIC: packed ints
 *
 * NumericField = NUMERIC
 * fixedLength BinaryField = BINARY
 * variableLength BinaryField = BINARY + NUMERIC (addresses)
 * fixedLength SortedField = BINARY + NUMERIC (ords)
 * variableLength SortedField = BINARY + NUMERIC (addresses) + NUMERIC (ords) 
 */
public class DiskDocValuesFormat extends SimpleDocValuesFormat {

  public DiskDocValuesFormat() {
    super("Disk");
  }

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new DiskDocValuesConsumer(state);
  }

  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new DiskDocValuesProducer(state);
  }
  
  static final String DATA_CODEC = "DiskDocValuesData";
  static final String METADATA_CODEC = "DiskDocValuesMetadata";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
}
