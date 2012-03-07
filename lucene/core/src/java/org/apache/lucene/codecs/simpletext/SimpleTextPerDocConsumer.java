package org.apache.lucene.codecs.simpletext;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * @lucene.experimental
 */
class SimpleTextPerDocConsumer extends PerDocConsumer {

  protected final PerDocWriteState state;
  protected final String segmentSuffix;
  public SimpleTextPerDocConsumer(PerDocWriteState state, String segmentSuffix)
      throws IOException {
    this.state = state;
    this.segmentSuffix = segmentSuffix;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public DocValuesConsumer addValuesField(Type type, FieldInfo field)
      throws IOException {
    return new SimpleTextDocValuesConsumer(SimpleTextDocValuesFormat.docValuesId(state.segmentName,
        field.number), state.directory, state.context, type, segmentSuffix);
  }

  @Override
  public void abort() {
    Set<String> files = new HashSet<String>();
    files(state.directory, state.fieldInfos, state.segmentName, files, segmentSuffix);
    IOUtils.deleteFilesIgnoringExceptions(state.directory,
        files.toArray(new String[0]));
  }
  
  
  static void files(SegmentInfo info, Set<String> files, String segmentSuffix) throws IOException {
    files(info.dir, info.getFieldInfos(), info.name, files, segmentSuffix);
  }
  
  static String docValuesId(String segmentsName, int fieldId) {
    return segmentsName + "_" + fieldId;
  }

  @SuppressWarnings("fallthrough")
  private static void files(Directory dir, FieldInfos fieldInfos,
      String segmentName, Set<String> files, String segmentSuffix) {
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.hasDocValues()) {
        String filename = docValuesId(segmentName, fieldInfo.number);
        files.add(IndexFileNames.segmentFileName(filename, "",
            segmentSuffix));
        try {
          assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
              segmentSuffix));
        } catch (IOException e) {
          // don't throw checked exception - dir is only used in assert
          throw new RuntimeException(e);
        }
      }
    }
  }

}