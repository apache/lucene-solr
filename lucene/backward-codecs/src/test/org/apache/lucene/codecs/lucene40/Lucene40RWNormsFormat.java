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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Read-write version of 4.0 norms format for testing
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene40RWNormsFormat extends Lucene40NormsFormat {

  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    String filename = IndexFileNames.segmentFileName(state.segmentInfo.name, 
        "nrm", 
        Lucene40CompoundFormat.COMPOUND_FILE_EXTENSION);
    final Lucene40DocValuesWriter impl = new Lucene40DocValuesWriter(state, filename, Lucene40FieldInfosFormat.LEGACY_NORM_TYPE_KEY);
    return new NormsConsumer() {
      @Override
      public void addNormsField(FieldInfo field, Iterable<Number> values) throws IOException {
        impl.addNumericField(field, values);
      }
      
      @Override
      public void close() throws IOException {
        impl.close();
      }
    };
  }
}
