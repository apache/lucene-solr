package org.apache.lucene.document;

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

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;

/** A {@link StoredFieldVisitor} that creates a {@link
 *  Document} containing all stored fields, or only specific
 *  requested fields provided to {@link #DocumentStoredFieldVisitor(Set)}.
 *  <p>
 *  This is used by {@link IndexReader#document(int)} to load a
 *  document.
 *
 * @lucene.experimental */

public class DocumentStoredFieldVisitor extends StoredFieldVisitor {
  private final Document doc;
  private final Set<String> fieldsToAdd;
  private final FieldTypes fieldTypes;

  /** 
   * Load only fields named in the provided <code>Set&lt;String&gt;</code>. 
   * @param fieldsToAdd Set of fields to load, or <code>null</code> (all fields).
   */
  public DocumentStoredFieldVisitor(FieldTypes fieldTypes, Set<String> fieldsToAdd) {
    doc = new Document(fieldTypes, false);
    this.fieldTypes = fieldTypes;
    this.fieldsToAdd = fieldsToAdd;
  }

  /** Load only fields named in the provided fields. */
  public DocumentStoredFieldVisitor(FieldTypes fieldTypes, String... fields) {
    doc = new Document(fieldTypes, false);
    this.fieldTypes = fieldTypes;
    fieldsToAdd = new HashSet<>(fields.length);
    for(String field : fields) {
      fieldsToAdd.add(field);
    }
  }

  /** Load all stored fields. */
  public DocumentStoredFieldVisitor(FieldTypes fieldTypes) {
    doc = new Document(fieldTypes, false);
    this.fieldTypes = fieldTypes;
    this.fieldsToAdd = null;
  }

  private FieldTypes.FieldType getFieldType(String fieldName) {
    if (fieldTypes != null) {
      try {
        return fieldTypes.getFieldType(fieldName);
      } catch (IllegalArgumentException iae) {
      }
    }
    return null;
  }

  @Override
  public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
    FieldTypes.FieldType fieldType = getFieldType(fieldInfo.name);
    if (fieldType != null && fieldType.valueType == FieldTypes.ValueType.INET_ADDRESS) {
      doc.addInetAddress(fieldInfo.name, InetAddress.getByAddress(value));
    } else if (fieldType != null && fieldType.valueType == FieldTypes.ValueType.BIG_INT) {
      doc.addBigInteger(fieldInfo.name, new BigInteger(value));
    } else {
      doc.addBinary(fieldInfo.name, new BytesRef(value));
    }
  }

  @Override
  public void stringField(FieldInfo fieldInfo, String value) throws IOException {
    doc.addLargeText(fieldInfo.name, value);
  }

  // nocommit it's odd that this API differentiates which number it was, vs doc values which always uses long:
  @Override
  public void intField(FieldInfo fieldInfo, int value) {
    FieldTypes.FieldType fieldType = getFieldType(fieldInfo.name);
    if (fieldType != null && fieldType.valueType == FieldTypes.ValueType.BOOLEAN) {
      // nocommit real check?
      assert value == 0 || value == 1;
      doc.addBoolean(fieldInfo.name, Boolean.valueOf(value == 1));
    } else {
      doc.addInt(fieldInfo.name, value);
    }
  }

  @Override
  public void longField(FieldInfo fieldInfo, long value) {
    FieldTypes.FieldType fieldType = getFieldType(fieldInfo.name);
    if (fieldType != null && fieldType.valueType == FieldTypes.ValueType.DATE) {
      doc.addDate(fieldInfo.name, new Date(value));
    } else {
      doc.addLong(fieldInfo.name, value);
    }
  }

  @Override
  public void floatField(FieldInfo fieldInfo, float value) {
    doc.addFloat(fieldInfo.name, value);
  }

  @Override
  public void doubleField(FieldInfo fieldInfo, double value) {
    doc.addDouble(fieldInfo.name, value);
  }

  @Override
  public Status needsField(FieldInfo fieldInfo) throws IOException {
    return fieldsToAdd == null || fieldsToAdd.contains(fieldInfo.name) ? Status.YES : Status.NO;
  }

  /**
   * Retrieve the visited document.
   * @return {@link Document} populated with stored fields. Note that only
   *         the stored information in the field instances is valid,
   *         data such as indexing options, term vector options,
   *         etc is not set.
   */
  public Document getDocument() {
    return doc;
  }
}
