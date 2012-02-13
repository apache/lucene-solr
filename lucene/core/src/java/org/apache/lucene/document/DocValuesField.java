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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DocValues.Type; // javadocs
import org.apache.lucene.index.DocValues;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * This class provides a {@link Field} that enables storing of typed
 * per-document values for scoring, sorting or value retrieval. Here's an
 * example usage, adding an int value (<code>22</code>):
 * 
 * <pre>
 *   document.add(new DocValuesField(name, 22, DocValues.Type.VAR_INTS));
 * </pre>
 * 
 * For optimal performance, re-use the <code>DocValuesField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre>
 *  DocValuesField field = new DocValuesField(name, 0, DocValues.Type.VAR_INTS);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setValue(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * */

public class DocValuesField extends Field {

  private static final Map<DocValues.Type,FieldType> types = new HashMap<DocValues.Type,FieldType>();
  static {
    for(DocValues.Type type : DocValues.Type.values()) {
      final FieldType ft = new FieldType();
      ft.setDocValueType(type);
      ft.freeze();
      types.put(type, ft);
    }
  }

  private static EnumSet<Type> BYTES = EnumSet.of(
                     Type.BYTES_FIXED_DEREF,
                     Type.BYTES_FIXED_STRAIGHT,
                     Type.BYTES_VAR_DEREF,
                     Type.BYTES_VAR_STRAIGHT,
                     Type.BYTES_FIXED_SORTED,
                     Type.BYTES_VAR_SORTED);

  private static EnumSet<Type> INTS = EnumSet.of(
                     Type.VAR_INTS,
                     Type.FIXED_INTS_8,
                     Type.FIXED_INTS_16,
                     Type.FIXED_INTS_32,
                     Type.FIXED_INTS_64);

  public static FieldType getFieldType(DocValues.Type type) {
    return types.get(type);
  }

  public DocValuesField(String name, BytesRef bytes, DocValues.Type docValueType) {
    super(name, getFieldType(docValueType));
    if (!BYTES.contains(docValueType)) {
      throw new IllegalArgumentException("docValueType must be one of: " + BYTES + "; got " + docValueType);
    }
    fieldsData = bytes;
  }

  public DocValuesField(String name, int value, DocValues.Type docValueType) {
    super(name, getFieldType(docValueType));
    if (!INTS.contains(docValueType)) {
      throw new IllegalArgumentException("docValueType must be one of: " + INTS +"; got " + docValueType);
    }
    fieldsData = Integer.valueOf(value);
  }

  public DocValuesField(String name, long value, DocValues.Type docValueType) {
    super(name, getFieldType(docValueType));
    if (!INTS.contains(docValueType)) {
      throw new IllegalArgumentException("docValueType must be one of: " + INTS +"; got " + docValueType);
    }
    fieldsData = Long.valueOf(value);
  }

  public DocValuesField(String name, float value, DocValues.Type docValueType) {
    super(name, getFieldType(docValueType));
    if (docValueType != DocValues.Type.FLOAT_32 &&
        docValueType != DocValues.Type.FLOAT_64) {
      throw new IllegalArgumentException("docValueType must be FLOAT_32/64; got " + docValueType);
    }
    fieldsData = Float.valueOf(value);
  }

  public DocValuesField(String name, double value, DocValues.Type docValueType) {
    super(name, getFieldType(docValueType));
    if (docValueType != DocValues.Type.FLOAT_32 &&
        docValueType != DocValues.Type.FLOAT_64) {
      throw new IllegalArgumentException("docValueType must be FLOAT_32/64; got " + docValueType);
    }
    fieldsData = Double.valueOf(value);
  }
}
