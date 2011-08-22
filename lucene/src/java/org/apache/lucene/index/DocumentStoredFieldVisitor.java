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
import java.util.Set;
import java.util.HashSet;

import org.apache.lucene.document.BinaryField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.IndexInput;

/** A {@link StoredFieldVisitor} that creates a {@link
 *  Document} containing all stored fields, or only specific
 *  requested fields provided to {@link #DocumentStoredFieldVisitor(Set)}
 *  This is used by {@link IndexReader#document(int)} to load a
 *  document.
 *
 * @lucene.experimental */

public class DocumentStoredFieldVisitor extends StoredFieldVisitor {
  private final Document doc = new Document();
  private final Set<String> fieldsToAdd;

  /** Load only fields named in the provided <code>Set&lt;String&gt;</code>. */
  public DocumentStoredFieldVisitor(Set<String> fieldsToAdd) {
    this.fieldsToAdd = fieldsToAdd;
  }

  /** Load only fields named in the provided <code>Set&lt;String&gt;</code>. */
  public DocumentStoredFieldVisitor(String... fields) {
    fieldsToAdd = new HashSet<String>(fields.length);
    for(String field : fields) {
      fieldsToAdd.add(field);
    }
  }

  /** Load all stored fields. */
  public DocumentStoredFieldVisitor() {
    this.fieldsToAdd = null;
  }

  @Override
  public boolean binaryField(FieldInfo fieldInfo, IndexInput in, int numBytes) throws IOException {
    if (accept(fieldInfo)) {
      final byte[] b = new byte[numBytes];
      in.readBytes(b, 0, b.length);
      doc.add(new BinaryField(fieldInfo.name, b));
    } else {
      in.seek(in.getFilePointer() + numBytes);
    }
    return false;
  }

  @Override
  public boolean stringField(FieldInfo fieldInfo, IndexInput in, int numUTF8Bytes) throws IOException {
    if (accept(fieldInfo)) {
      final byte[] b = new byte[numUTF8Bytes];
      in.readBytes(b, 0, b.length);
      FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setStoreTermVectors(fieldInfo.storeTermVector);
      ft.setStoreTermVectorPositions(fieldInfo.storePositionWithTermVector);
      ft.setStoreTermVectorOffsets(fieldInfo.storeOffsetWithTermVector);
      ft.setStoreTermVectors(fieldInfo.storeTermVector);
      ft.setOmitNorms(fieldInfo.omitNorms);
      ft.setIndexOptions(fieldInfo.indexOptions);
      doc.add(new Field(fieldInfo.name,
                        ft,
                        new String(b, "UTF-8")));
    } else {
      in.seek(in.getFilePointer() + numUTF8Bytes);
    }
    return false;
  }

  @Override
  public boolean intField(FieldInfo fieldInfo, int value) {
    if (accept(fieldInfo)) {
      FieldType ft = new FieldType(NumericField.TYPE_STORED);
      ft.setIndexed(fieldInfo.isIndexed);
      doc.add(new NumericField(fieldInfo.name, ft).setIntValue(value));
    }
    return false;
  }

  @Override
  public boolean longField(FieldInfo fieldInfo, long value) {
    if (accept(fieldInfo)) {
      FieldType ft = new FieldType(NumericField.TYPE_STORED);
      ft.setIndexed(fieldInfo.isIndexed);
      doc.add(new NumericField(fieldInfo.name, ft).setLongValue(value));
    }
    return false;
  }

  @Override
  public boolean floatField(FieldInfo fieldInfo, float value) {
    if (accept(fieldInfo)) {
      FieldType ft = new FieldType(NumericField.TYPE_STORED);
      ft.setIndexed(fieldInfo.isIndexed);
      doc.add(new NumericField(fieldInfo.name, ft).setFloatValue(value));
    }
    return false;
  }

  @Override
  public boolean doubleField(FieldInfo fieldInfo, double value) {
    if (accept(fieldInfo)) {
      FieldType ft = new FieldType(NumericField.TYPE_STORED);
      ft.setIndexed(fieldInfo.isIndexed);
      doc.add(new NumericField(fieldInfo.name, ft).setDoubleValue(value));
    }
    return false;
  }

  private boolean accept(FieldInfo fieldInfo) {
    return fieldsToAdd == null || fieldsToAdd.contains(fieldInfo.name);
  }

  public Document getDocument() {
    return doc;
  }
}
