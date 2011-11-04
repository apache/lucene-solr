package org.apache.lucene.index.codecs;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Default PerDocValues implementation that uses compound file.
 * @lucene.experimental
 */
public class DefaultDocValuesProducer extends DocValuesReaderBase {
  protected final TreeMap<String,IndexDocValues> docValues;
  private final Directory cfs;

  /**
   * Creates a new {@link DefaultDocValuesProducer} instance and loads all
   * {@link IndexDocValues} instances for this segment and codec.
   */
  public DefaultDocValuesProducer(SegmentReadState state) throws IOException {
    if (state.fieldInfos.anyDocValuesFields()) {
      cfs = new CompoundFileDirectory(state.dir, 
                                      IndexFileNames.segmentFileName(state.segmentInfo.name,
                                                                     DefaultDocValuesConsumer.DOC_VALUES_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_EXTENSION), 
                                      state.context, false);
      docValues = load(state.fieldInfos, state.segmentInfo.name, state.segmentInfo.docCount, cfs, state.context);
    } else {
      cfs = null;
      docValues = new TreeMap<String,IndexDocValues>();
    }
  }
  
  @Override
  protected Map<String,IndexDocValues> docValues() {
    return docValues;
  }

  @Override
  protected void closeInternal(Collection<? extends Closeable> closeables) throws IOException {
    if (cfs != null) {
      final ArrayList<Closeable> list = new ArrayList<Closeable>(closeables);
      list.add(cfs);
      IOUtils.close(list);
    } else {
      IOUtils.close(closeables);
    }
  }
}
