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
import java.io.Reader;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

final class SortingStoredFieldsConsumer extends StoredFieldsConsumer {
  TrackingTmpOutputDirectoryWrapper tmpDirectory;

  SortingStoredFieldsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter);
  }

  @Override
  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) {
      this.tmpDirectory = new TrackingTmpOutputDirectoryWrapper(docWriter.directory);
      this.writer = docWriter.codec.storedFieldsFormat().fieldsWriter(tmpDirectory, docWriter.getSegmentInfo(),
          IOContext.DEFAULT);
    }
  }

  @Override
  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    super.flush(state, sortMap);
    if (sortMap == null) {
      // we're lucky the index is already sorted, just rename the temporary file and return
      for (Map.Entry<String, String> entry : tmpDirectory.getTemporaryFiles().entrySet()) {
        tmpDirectory.rename(entry.getValue(), entry.getKey());
      }
      return;
    }
    StoredFieldsReader reader = docWriter.codec.storedFieldsFormat()
        .fieldsReader(tmpDirectory, state.segmentInfo, state.fieldInfos, IOContext.DEFAULT);
    StoredFieldsReader mergeReader = reader.getMergeInstance();
    StoredFieldsWriter sortWriter = docWriter.codec.storedFieldsFormat()
        .fieldsWriter(state.directory, state.segmentInfo, IOContext.DEFAULT);
    try {
      reader.checkIntegrity();
      CopyVisitor visitor = new CopyVisitor(sortWriter);
      for (int docID = 0; docID < state.segmentInfo.maxDoc(); docID++) {
        sortWriter.startDocument();
        mergeReader.visitDocument(sortMap.newToOld(docID), visitor);
        sortWriter.finishDocument();
      }
      sortWriter.finish(state.fieldInfos, state.segmentInfo.maxDoc());
    } finally {
      IOUtils.close(reader, sortWriter);
      IOUtils.deleteFiles(tmpDirectory,
          tmpDirectory.getTemporaryFiles().values());
    }
  }

  @Override
  void abort() {
    try {
      super.abort();
    } finally {
      if (tmpDirectory != null) {
        IOUtils.deleteFilesIgnoringExceptions(tmpDirectory,
            tmpDirectory.getTemporaryFiles().values());
      }
    }
  }

  /**
   * A visitor that copies every field it sees in the provided {@link StoredFieldsWriter}.
   */
  private static class CopyVisitor extends StoredFieldVisitor implements IndexableField {
    final StoredFieldsWriter writer;
    BytesRef binaryValue;
    String stringValue;
    Number numericValue;
    FieldInfo currentField;


    CopyVisitor(StoredFieldsWriter writer) {
      this.writer = writer;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      reset(fieldInfo);
      // TODO: can we avoid new BR here?
      binaryValue = new BytesRef(value);
      write();
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      reset(fieldInfo);
      stringValue = Objects.requireNonNull(value, "String value should not be null");
      write();
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      reset(fieldInfo);
      numericValue = value;
      write();
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      reset(fieldInfo);
      numericValue = value;
      write();
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      reset(fieldInfo);
      numericValue = value;
      write();
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      reset(fieldInfo);
      numericValue = value;
      write();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return Status.YES;
    }

    @Override
    public String name() {
      return currentField.name;
    }

    @Override
    public IndexableFieldType fieldType() {
      return StoredField.TYPE;
    }

    @Override
    public BytesRef binaryValue() {
      return binaryValue;
    }

    @Override
    public String stringValue() {
      return stringValue;
    }

    @Override
    public Number numericValue() {
      return numericValue;
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
      return null;
    }

    void reset(FieldInfo field) {
      currentField = field;
      binaryValue = null;
      stringValue = null;
      numericValue = null;
    }

    void write() throws IOException {
      writer.writeField(currentField, this);
    }
  }
}
