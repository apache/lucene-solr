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

import org.apache.lucene.codecs.lucene45.Lucene45DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;

class DiskDocValuesProducer extends Lucene45DocValuesProducer {

  DiskDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    super(state, dataCodec, dataExtension, metaCodec, metaExtension);
  }

  @Override
  protected MonotonicBlockPackedReader getAddressInstance(IndexInput data, FieldInfo field, BinaryEntry bytes) throws IOException {
    data.seek(bytes.addressesOffset);
    return new MonotonicBlockPackedReader(data.clone(), bytes.packedIntsVersion, bytes.blockSize, bytes.count, true);
  }

  @Override
  protected MonotonicBlockPackedReader getIntervalInstance(IndexInput data, FieldInfo field, BinaryEntry bytes) throws IOException {
    throw new AssertionError();
  }

  @Override
  protected MonotonicBlockPackedReader getOrdIndexInstance(IndexInput data, FieldInfo field, NumericEntry entry) throws IOException {
    data.seek(entry.offset);
    return new MonotonicBlockPackedReader(data.clone(), entry.packedIntsVersion, entry.blockSize, entry.count, true);
  }
}
