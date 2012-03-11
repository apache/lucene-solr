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
import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;

/**
 * Norms Format for the default codec. 
 * @lucene.experimental
 */
public class Lucene40NormsFormat extends NormsFormat {
  private final static String NORMS_SEGMENT_SUFFIX = "nrm";
  
  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return new Lucene40NormsDocValuesConsumer(state, NORMS_SEGMENT_SUFFIX);
  }

  @Override
  public PerDocProducer docsProducer(SegmentReadState state) throws IOException {
    return new Lucene40NormsDocValuesProducer(state, NORMS_SEGMENT_SUFFIX);
  }

  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    Lucene40NormsDocValuesConsumer.files(info, files);
  }
 
  public static class Lucene40NormsDocValuesProducer extends Lucene40DocValuesProducer {

    public Lucene40NormsDocValuesProducer(SegmentReadState state,
        String segmentSuffix) throws IOException {
      super(state, segmentSuffix);
    }

    @Override
    protected boolean canLoad(FieldInfo info) {
      return info.hasNorms();
    }

    @Override
    protected Type getDocValuesType(FieldInfo info) {
      return info.getNormType();
    }

    @Override
    protected boolean anyDocValuesFields(FieldInfos infos) {
      return infos.hasNorms();
    }
    
  }
  
  public static class Lucene40NormsDocValuesConsumer extends Lucene40DocValuesConsumer {

    public Lucene40NormsDocValuesConsumer(PerDocWriteState state,
        String segmentSuffix) throws IOException {
      super(state, segmentSuffix);
    }

    @Override
    protected DocValues getDocValuesForMerge(AtomicReader reader, FieldInfo info)
        throws IOException {
      return reader.normValues(info.name);
    }

    @Override
    protected boolean canMerge(FieldInfo info) {
      return info.hasNorms();
    }

    @Override
    protected Type getDocValuesType(FieldInfo info) {
      return info.getNormType();
    }
    
    public static void files(SegmentInfo segmentInfo, Set<String> files) throws IOException {
      final String normsFileName = IndexFileNames.segmentFileName(segmentInfo.name, NORMS_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_EXTENSION);
      FieldInfos fieldInfos = segmentInfo.getFieldInfos();
      for (FieldInfo fieldInfo : fieldInfos) {
        if (fieldInfo.hasNorms()) {
          final String normsEntriesFileName = IndexFileNames.segmentFileName(segmentInfo.name, NORMS_SEGMENT_SUFFIX, IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION);
          files.add(normsFileName);
          files.add(normsEntriesFileName);
          return;
        }
      }
    }
  }

}
