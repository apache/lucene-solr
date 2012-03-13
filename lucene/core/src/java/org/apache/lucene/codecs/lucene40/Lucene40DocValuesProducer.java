package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.PerDocProducerBase;
import org.apache.lucene.codecs.lucene40.values.Bytes;
import org.apache.lucene.codecs.lucene40.values.Floats;
import org.apache.lucene.codecs.lucene40.values.Ints;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

/**
 * Default PerDocProducer implementation that uses compound file.
 * @lucene.experimental
 */
public class Lucene40DocValuesProducer extends PerDocProducerBase {
  protected final TreeMap<String,DocValues> docValues;
  private final Directory cfs;
  /**
   * Creates a new {@link Lucene40DocValuesProducer} instance and loads all
   * {@link DocValues} instances for this segment and codec.
   */
  public Lucene40DocValuesProducer(SegmentReadState state, String segmentSuffix) throws IOException {
    if (anyDocValuesFields(state.fieldInfos)) {
      cfs = new CompoundFileDirectory(state.dir, 
                                      IndexFileNames.segmentFileName(state.segmentInfo.name,
                                                                     segmentSuffix, IndexFileNames.COMPOUND_FILE_EXTENSION), 
                                      state.context, false);
      docValues = load(state.fieldInfos, state.segmentInfo.name, state.segmentInfo.docCount, cfs, state.context);
    } else {
      cfs = null;
      docValues = new TreeMap<String,DocValues>();
    }
  }
  
  @Override
  protected Map<String,DocValues> docValues() {
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

  @Override
  protected DocValues loadDocValues(int docCount, Directory dir, String id,
      Type type, IOContext context) throws IOException {
      switch (type) {
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FIXED_INTS_8:
      case VAR_INTS:
        return Ints.getValues(dir, id, docCount, type, context);
      case FLOAT_32:
        return Floats.getValues(dir, id, docCount, context, type);
      case FLOAT_64:
        return Floats.getValues(dir, id, docCount, context, type);
      case BYTES_FIXED_STRAIGHT:
        return Bytes.getValues(dir, id, Bytes.Mode.STRAIGHT, true, docCount, getComparator(), context);
      case BYTES_FIXED_DEREF:
        return Bytes.getValues(dir, id, Bytes.Mode.DEREF, true, docCount, getComparator(), context);
      case BYTES_FIXED_SORTED:
        return Bytes.getValues(dir, id, Bytes.Mode.SORTED, true, docCount, getComparator(), context);
      case BYTES_VAR_STRAIGHT:
        return Bytes.getValues(dir, id, Bytes.Mode.STRAIGHT, false, docCount, getComparator(), context);
      case BYTES_VAR_DEREF:
        return Bytes.getValues(dir, id, Bytes.Mode.DEREF, false, docCount, getComparator(), context);
      case BYTES_VAR_SORTED:
        return Bytes.getValues(dir, id, Bytes.Mode.SORTED, false, docCount, getComparator(), context);
      default:
        throw new IllegalStateException("unrecognized index values mode " + type);
      }
    }
}
