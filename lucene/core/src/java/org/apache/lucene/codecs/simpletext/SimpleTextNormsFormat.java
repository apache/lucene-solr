package org.apache.lucene.codecs.simpletext;

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * plain-text norms format
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * 
 * @lucene.experimental
 */
public class SimpleTextNormsFormat extends NormsFormat {
  private static final String NORMS_SEG_SUFFIX = "len";
  
  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return new SimpleTextNormsPerDocConsumer(state, NORMS_SEG_SUFFIX);
  }
  
  @Override
  public PerDocProducer docsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextNormsPerDocProducer(state,
        BytesRef.getUTF8SortedAsUnicodeComparator(), NORMS_SEG_SUFFIX);
  }
  
  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    SimpleTextNormsPerDocConsumer.files(info, files);
  }
  
  public static class SimpleTextNormsPerDocProducer extends
      SimpleTextPerDocProducer {
    
    public SimpleTextNormsPerDocProducer(SegmentReadState state,
        Comparator<BytesRef> comp, String segmentSuffix) throws IOException {
      super(state, comp, segmentSuffix);
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
  
  public static class SimpleTextNormsPerDocConsumer extends
      SimpleTextPerDocConsumer {
    
    public SimpleTextNormsPerDocConsumer(PerDocWriteState state,
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
    
    @Override
    public void abort() {
      Set<String> files = new HashSet<String>();
      filesInternal(state.fieldInfos, state.segmentName, files, segmentSuffix);
      IOUtils.deleteFilesIgnoringExceptions(state.directory,
          files.toArray(new String[0]));
    }
    
    public static void files(SegmentInfo segmentInfo, Set<String> files)
        throws IOException {
      filesInternal(segmentInfo.getFieldInfos(), segmentInfo.name, files,
          NORMS_SEG_SUFFIX);
    }
    
    public static void filesInternal(FieldInfos fieldInfos, String segmentName,
        Set<String> files, String segmentSuffix) {
      for (FieldInfo fieldInfo : fieldInfos) {
        if (fieldInfo.hasNorms()) {
          String id = docValuesId(segmentName, fieldInfo.number);
          files.add(IndexFileNames.segmentFileName(id, "",
              segmentSuffix));
        }
      }
    }
  }
}
