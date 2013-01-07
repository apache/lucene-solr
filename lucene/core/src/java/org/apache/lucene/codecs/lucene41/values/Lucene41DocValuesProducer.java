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

import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

// nocommit 
public class Lucene41DocValuesProducer extends SimpleDVProducer {
  
  private final CompoundFileDirectory cfs;
  // nocommit: remove this
  private final SegmentInfo info;
  private final IOContext context;
  
  public Lucene41DocValuesProducer(SegmentReadState state) throws IOException {
    final String suffix;
    if (state.segmentSuffix.length() == 0) {
      suffix = Lucene41DocValuesConsumer.DV_SEGMENT_SUFFIX;
    } else {
      suffix = state.segmentSuffix + "_" + Lucene41DocValuesConsumer.DV_SEGMENT_SUFFIX;
    }
    String cfsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, suffix,
                                                        IndexFileNames.COMPOUND_FILE_EXTENSION);
    this.cfs = new CompoundFileDirectory(state.directory, cfsFileName, state.context, false);
    this.info = state.segmentInfo;
    this.context = state.context;
  }
  
  @Override
  public void close() throws IOException {
    IOUtils.close(cfs);
  }

  @Override
  public SimpleDVProducer clone() {
    return this; // nocommit ? actually safe since we open new each time from cfs?
  }
  
  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    // nocommit
    return null;
  }
  
  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    // nocommit
    return null;
  }
  
  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    if (DocValues.isSortedBytes(field.getDocValuesType())) {
      return new Lucene41SortedDocValues.Factory(this.cfs, this.info, field, context).getDirect();
    } else {
      return null;
    }
  }
    
  public static abstract class DocValuesFactory<T> implements Closeable {
    
    public abstract T getDirect() throws IOException;
    
    public abstract T getInMemory() throws IOException;
  }
  
}
