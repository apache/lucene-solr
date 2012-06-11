package org.apache.lucene.codecs.simpletext;

/*
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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.DocValues.Type;

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
    return new SimpleTextDocValuesConsumer(SimpleTextDocValuesFormat.docValuesId(state.segmentInfo.name,
        field.number), state.directory, state.context, type, segmentSuffix);
  }

  @Override
  public void abort() {
    // We don't have to remove files here: IndexFileDeleter
    // will do so
  }
  
  static String docValuesId(String segmentsName, int fieldId) {
    return segmentsName + "_" + fieldId;
  }
}
