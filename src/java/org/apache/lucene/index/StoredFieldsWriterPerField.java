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
import org.apache.lucene.document.Fieldable;

final class StoredFieldsWriterPerField extends DocFieldConsumerPerField {

  final StoredFieldsWriterPerThread perThread;
  final FieldInfo fieldInfo;
  final DocumentsWriter.DocState docState;

  public StoredFieldsWriterPerField(StoredFieldsWriterPerThread perThread, FieldInfo fieldInfo) {
    this.perThread = perThread;
    this.fieldInfo = fieldInfo;
    docState = perThread.docState;
  }

  // Process all occurrences of a single field in one doc;
  // count is 1 if a given field occurs only once in the
  // Document, which is the "typical" case
  public void processFields(Fieldable[] fields, int count) throws IOException {

    final StoredFieldsWriter.PerDoc doc;
    if (perThread.doc == null) {
      doc = perThread.doc = perThread.storedFieldsWriter.getPerDoc();
      doc.docID = docState.docID;
      perThread.localFieldsWriter.setFieldsStream(doc.fdt);
      assert doc.numStoredFields == 0: "doc.numStoredFields=" + doc.numStoredFields;
      assert 0 == doc.fdt.length();
      assert 0 == doc.fdt.getFilePointer();
    } else {
      doc = perThread.doc;
      assert doc.docID == docState.docID: "doc.docID=" + doc.docID + " docState.docID=" + docState.docID;
    }

    for(int i=0;i<count;i++) {
      final Fieldable field = fields[i];
      if (field.isStored()) {
        perThread.localFieldsWriter.writeField(fieldInfo, field);
        assert docState.testPoint("StoredFieldsWriterPerField.processFields.writeField");
        doc.numStoredFields++;
      }
    }
  }

  void abort() {
  }
}

