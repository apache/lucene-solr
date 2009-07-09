package org.apache.lucene.index;

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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.document.Fieldable;

final class StoredFieldsWriterPerThread {

  final FieldsWriter localFieldsWriter;
  final StoredFieldsWriter storedFieldsWriter;
  final DocumentsWriter.DocState docState;

  StoredFieldsWriter.PerDoc doc;

  public StoredFieldsWriterPerThread(DocumentsWriter.DocState docState, StoredFieldsWriter storedFieldsWriter) throws IOException {
    this.storedFieldsWriter = storedFieldsWriter;
    this.docState = docState;
    localFieldsWriter = new FieldsWriter((IndexOutput) null, (IndexOutput) null, storedFieldsWriter.fieldInfos);
  }

  public void startDocument() {
    if (doc != null) {
      // Only happens if previous document hit non-aborting
      // exception while writing stored fields into
      // localFieldsWriter:
      doc.reset();
      doc.docID = docState.docID;
    }
  }

  public void addField(Fieldable field, FieldInfo fieldInfo) throws IOException {
    if (doc == null) {
      doc = storedFieldsWriter.getPerDoc();
      doc.docID = docState.docID;
      localFieldsWriter.setFieldsStream(doc.fdt);
      assert doc.numStoredFields == 0: "doc.numStoredFields=" + doc.numStoredFields;
      assert 0 == doc.fdt.length();
      assert 0 == doc.fdt.getFilePointer();
    }

    localFieldsWriter.writeField(fieldInfo, field);
    assert docState.testPoint("StoredFieldsWriterPerThread.processFields.writeField");
    doc.numStoredFields++;
  }

  public DocumentsWriter.DocWriter finishDocument() {
    // If there were any stored fields in this doc, doc will
    // be non-null; else it's null.
    try {
      return doc;
    } finally {
      doc = null;
    }
  }

  public void abort() {
    if (doc != null) {
      doc.abort();
      doc = null;
    }
  }
}
