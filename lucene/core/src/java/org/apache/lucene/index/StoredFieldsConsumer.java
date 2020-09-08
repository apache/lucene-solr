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

package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

class StoredFieldsConsumer {
  final Codec codec;
  final Directory directory;
  final SegmentInfo info;
  StoredFieldsWriter writer;
  private int lastDoc;

  StoredFieldsConsumer(Codec codec, Directory directory, SegmentInfo info) {
    this.codec = codec;
    this.directory = directory;
    this.info = info;
    this.lastDoc = -1;
  }

  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) { // TODO can we allocate this in the ctor? we call start document for every doc anyway
      this.writer = codec.storedFieldsFormat().fieldsWriter(directory, info, IOContext.DEFAULT);
    }
  }

  void startDocument(int docID) throws IOException {
    assert lastDoc < docID;
    initStoredFieldsWriter();
    while (++lastDoc < docID) {
      writer.startDocument();
      writer.finishDocument();
    }
    writer.startDocument();
  }

  void writeField(FieldInfo info, IndexableField field) throws IOException {
    writer.writeField(info, field);
  }

  void finishDocument() throws IOException {
    writer.finishDocument();
  }

  void finish(int maxDoc) throws IOException {
    while (lastDoc < maxDoc-1) {
      startDocument(lastDoc);
      finishDocument();
      ++lastDoc;
    }
  }

  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    try {
      writer.finish(state.fieldInfos, state.segmentInfo.maxDoc());
    } finally {
      IOUtils.close(writer);
      writer = null;
    }
  }

  void abort() {
    if (writer != null) {
      IOUtils.closeWhileHandlingException(writer);
      writer = null;
    }
  }

  long ramBytesUsed() {
    return writer == null ? 0 : writer.ramBytesUsed();
  }
}
