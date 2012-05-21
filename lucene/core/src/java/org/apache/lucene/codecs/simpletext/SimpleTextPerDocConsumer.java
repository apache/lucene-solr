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
    files(state.directory, state.segmentName, files, segmentSuffix);
    IOUtils.deleteFilesIgnoringExceptions(state.directory,
                                          SegmentInfo.findMatchingFiles(state.segmentName, state.directory, files).toArray(new String[0]));
  }
  
  static String docValuesId(String segmentsName, int fieldId) {
    return segmentsName + "_" + fieldId;
  }

  static String docValuesIdRegexp(String segmentsName) {
    return segmentsName + "_\\d+";
  }

  @SuppressWarnings("fallthrough")
  private static void files(Directory dir,
      String segmentName, Set<String> files, String segmentSuffix) {
    files.add(IndexFileNames.segmentFileName(docValuesIdRegexp(segmentName), "",
                                               segmentSuffix));
  }
}