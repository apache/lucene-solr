package org.apache.lucene.codecs.lucene41.values;

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
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.codecs.PerDocProducerBase;
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;


public class Lucene41SimpleDocValuesFormat extends SimpleDocValuesFormat {
  
  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new Lucene41DocValuesConsumer(state.directory, state.segmentInfo, state.context);
  }
  
  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    // nocommit fixme
    // return new Lucene41PerdocProducer(state);
    return null;
  }

  //nocommit this is equivalent to sep - we should pack in CFS
  private static final class Lucene41DocValuesReader extends PerDocProducerBase {
    private final TreeMap<String, DocValues> docValues;

    /**
     * Creates a new {@link Lucene41PerDocProducer} instance and loads all
     * {@link DocValues} instances for this segment and codec.
     */
    public Lucene41DocValuesReader(SegmentReadState state) throws IOException {
      docValues = load(state.fieldInfos, state.segmentInfo.name, state.segmentInfo.getDocCount(), state.dir, state.context);
    }
    
    @Override
    protected Map<String,DocValues> docValues() {
      return docValues;
    }
    
    @Override
    protected void closeInternal(Collection<? extends Closeable> closeables) throws IOException {
      IOUtils.close(closeables);
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
        case FLOAT_32:
        case FLOAT_64:
          return new Lucene41NumericDocValuesProducer(dir.openInput( IndexFileNames.segmentFileName(
        id, Lucene41DocValuesConsumer.DV_SEGMENT_SUFFIX, Lucene41DocValuesConsumer.DATA_EXTENSION), context), docCount);
        
        case BYTES_FIXED_STRAIGHT:
        case BYTES_FIXED_DEREF:
        case BYTES_VAR_STRAIGHT:
        case BYTES_VAR_DEREF:
          //nocommit cose in case of an exception
          IndexInput dataIn = dir.openInput(IndexFileNames.segmentFileName(id,
              Lucene41DocValuesConsumer.DV_SEGMENT_SUFFIX,
              Lucene41DocValuesConsumer.DATA_EXTENSION), context);
          IndexInput indexIn = dir.openInput(IndexFileNames.segmentFileName(id,
              Lucene41DocValuesConsumer.DV_SEGMENT_SUFFIX,
              Lucene41DocValuesConsumer.INDEX_EXTENSION), context);
          return new Lucene41BinaryDocValuesProducer(dataIn, indexIn);
        case BYTES_VAR_SORTED:
        case BYTES_FIXED_SORTED:
        default:
          throw new IllegalStateException("unrecognized index values mode " + type);
        }
      }
  }
  
}
