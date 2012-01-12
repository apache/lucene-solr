package org.apache.lucene.document;

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
import java.util.Comparator;

import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Type; // javadocs
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * This class provides a {@link Field} that enables storing of typed
 * per-document values for scoring, sorting or value retrieval. Here's an
 * example usage, adding an int value:
 * 
 * <pre>
 * DocValuesField field = new DocValuesField(name, DocValues.Type.VAR_INTS);
 * field.setInt(value);
 * document.add(field);
 * </pre>
 * 
 * For optimal performance, re-use the <code>DocValuesField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre>
 *  DocValuesField field = new DocValuesField(name, DocValues.Type.VAR_INTS);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setInt(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 * 
 * <p>
 * If doc values are stored in addition to an indexed ({@link FieldType#setIndexed(boolean)}) or stored
 * ({@link FieldType#setStored(boolean)}) value it's recommended to pass the appropriate {@link FieldType}
 * when creating the field:
 * 
 * <pre>
 *  DocValuesField field = new DocValuesField(name, StringField.TYPE_STORED);
 *  Document document = new Document();
 *  document.add(field);
 *  for(all documents) {
 *    ...
 *    field.setInt(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 * 
 * */

// nocommit -- how to sugar this...?

public class DocValuesField extends Field {

  protected Comparator<BytesRef> bytesComparator;

  public DocValuesField(String name, BytesRef bytes, DocValues.Type docValueType) {
    super(name, new FieldType());
    // nocommit use enumset
    if (docValueType != DocValues.Type.BYTES_FIXED_STRAIGHT &&
        docValueType != DocValues.Type.BYTES_FIXED_DEREF &&
        docValueType != DocValues.Type.BYTES_VAR_STRAIGHT &&
        docValueType != DocValues.Type.BYTES_VAR_DEREF &&
        docValueType != DocValues.Type.BYTES_FIXED_SORTED &&
        docValueType != DocValues.Type.BYTES_VAR_SORTED) {
      throw new IllegalArgumentException("docValueType must be BYTE_FIXED_STRAIGHT, BYTE_FIXED_DEREF, BYTES_VAR_STRAIGHT, BYTES_VAR_DEREF, BYTES_FIXED_SORTED or BYTES_VAR_SORTED; got " + docValueType);
    }
    fieldsData = bytes;
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
  }

  public DocValuesField(String name, byte value, DocValues.Type docValueType) {
    super(name, new FieldType());
    // nocommit use enumset
    if (docValueType != DocValues.Type.VAR_INTS &&
        docValueType != DocValues.Type.FIXED_INTS_8 &&
        docValueType != DocValues.Type.FIXED_INTS_16 &&
        docValueType != DocValues.Type.FIXED_INTS_32 &&
        docValueType != DocValues.Type.FIXED_INTS_64) {
      throw new IllegalArgumentException("docValueType must be VAR_INTS, FIXED_INTS_8/16/32/64; got " + docValueType);
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
    fieldsData = Byte.valueOf(value);
  }

  public DocValuesField(String name, short value, DocValues.Type docValueType) {
    super(name, new FieldType());
    // nocommit use enumset
    if (docValueType != DocValues.Type.VAR_INTS &&
        docValueType != DocValues.Type.FIXED_INTS_8 &&
        docValueType != DocValues.Type.FIXED_INTS_16 &&
        docValueType != DocValues.Type.FIXED_INTS_32 &&
        docValueType != DocValues.Type.FIXED_INTS_64) {
      throw new IllegalArgumentException("docValueType must be VAR_INTS, FIXED_INTS_8/16/32/64; got " + docValueType);
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
    fieldsData = Short.valueOf(value);
  }

  public DocValuesField(String name, int value, DocValues.Type docValueType) {
    super(name, new FieldType());
    // nocommit use enumset
    if (docValueType != DocValues.Type.VAR_INTS &&
        docValueType != DocValues.Type.FIXED_INTS_8 &&
        docValueType != DocValues.Type.FIXED_INTS_16 &&
        docValueType != DocValues.Type.FIXED_INTS_32 &&
        docValueType != DocValues.Type.FIXED_INTS_64) {
      throw new IllegalArgumentException("docValueType must be VAR_INTS, FIXED_INTS_8/16/32/64; got " + docValueType);
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
    fieldsData = Integer.valueOf(value);
  }

  public DocValuesField(String name, long value, DocValues.Type docValueType) {
    super(name, new FieldType());
    // nocommit use enumset
    if (docValueType != DocValues.Type.VAR_INTS &&
        docValueType != DocValues.Type.FIXED_INTS_8 &&
        docValueType != DocValues.Type.FIXED_INTS_16 &&
        docValueType != DocValues.Type.FIXED_INTS_32 &&
        docValueType != DocValues.Type.FIXED_INTS_64) {
      throw new IllegalArgumentException("docValueType must be VAR_INTS, FIXED_INTS_8/16/32/64; got " + docValueType);
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
    fieldsData = Long.valueOf(value);
  }

  public DocValuesField(String name, float value, DocValues.Type docValueType) {
    super(name, new FieldType());
    if (docValueType != DocValues.Type.FLOAT_32 &&
        docValueType != DocValues.Type.FLOAT_64) {
      throw new IllegalArgumentException("docValueType must be FLOAT_32/64; got " + docValueType);
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
    fieldsData = Float.valueOf(value);
  }

  public DocValuesField(String name, double value, DocValues.Type docValueType) {
    super(name, new FieldType());
    if (docValueType != DocValues.Type.FLOAT_32 &&
        docValueType != DocValues.Type.FLOAT_64) {
      throw new IllegalArgumentException("docValueType must be FLOAT_32/64; got " + docValueType);
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
    fieldsData = Double.valueOf(value);
  }

  // nocommit need static or dynamic type checking here:
  public DocValuesField(String name, Object value, IndexableFieldType type) {
    super(name, type);
    if (type.docValueType() == null) {
      throw new NullPointerException("docValueType cannot be null");
    }
    fieldsData = value;
  }
}
