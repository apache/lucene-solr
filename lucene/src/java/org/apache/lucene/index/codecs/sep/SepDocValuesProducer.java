package org.apache.lucene.index.codecs.sep;

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
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.DocValuesReaderBase;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.util.IOUtils;

/**
 * Implementation of PerDocValues that uses separate files.
 * @lucene.experimental
 */
public class SepDocValuesProducer extends DocValuesReaderBase {
  private final TreeMap<String, IndexDocValues> docValues;

  /**
   * Creates a new {@link SepDocValuesProducer} instance and loads all
   * {@link IndexDocValues} instances for this segment and codec.
   */
  public SepDocValuesProducer(SegmentReadState state) throws IOException {
    docValues = load(state.fieldInfos, state.segmentInfo.name, state.segmentInfo.docCount, state.dir, state.context);
  }
  
  @Override
  protected Map<String,IndexDocValues> docValues() {
    return docValues;
  }
  
  @Override
  protected void closeInternal(Collection<? extends Closeable> closeables) throws IOException {
    IOUtils.close(closeables);
  }
}
