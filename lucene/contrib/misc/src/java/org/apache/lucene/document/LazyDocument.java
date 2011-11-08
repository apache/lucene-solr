package org.apache.lucene.document;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;

/** Defers actually loading a field's value until you ask
 *  for it.  You must not use the returned Field instances
 *  after the provided reader has been closed. */

public class LazyDocument {
  private IndexReader reader;
  private final int docID;

  // null until first field is loaded
  private Document doc;

  private Map<String,LazyField> fields = new HashMap<String,LazyField>();

  public LazyDocument(IndexReader reader, int docID) {
    this.reader = reader;
    this.docID = docID;
  }

  public Field getField(FieldInfo fieldInfo) {  
    LazyField f = fields.get(fieldInfo.name);
    if (f == null) {
      final FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setStoreTermVectors(fieldInfo.storeTermVector);
      ft.setStoreTermVectorPositions(fieldInfo.storePositionWithTermVector);
      ft.setStoreTermVectorOffsets(fieldInfo.storeOffsetWithTermVector);
      ft.setStoreTermVectors(fieldInfo.storeTermVector);
      ft.setIndexed(fieldInfo.isIndexed);
      ft.setOmitNorms(fieldInfo.omitNorms);
      ft.setIndexOptions(fieldInfo.indexOptions);
      f = new LazyField(fieldInfo.name, ft);
      fields.put(fieldInfo.name, f);
    }
    return f;
  }

  private synchronized Document getDocument() {
    if (doc == null) {
      try {
        doc = reader.document(docID);
      } catch (IOException ioe) {
        throw new IllegalStateException("unable to load document", ioe);
      }
      reader = null;
    }
    return doc;
  }

  private class LazyField extends Field {
    public LazyField(String name, FieldType ft) {
      super(name, ft);
    }

    @Override
    public Number numericValue() {
      return null;
    }

    @Override
    public NumericField.DataType numericDataType() {
      return null;
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public String stringValue() {
      return getDocument().get(name);
    }

    @Override
    public BytesRef binaryValue() {
      return getDocument().getBinaryValue(name);
    }
  }
}
