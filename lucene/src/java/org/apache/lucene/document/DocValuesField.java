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
import java.io.Reader;
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
 * document.add(new DocValuesField(name).setInt(value));
 * </pre>
 * 
 * For optimal performance, re-use the <code>DocValuesField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre>
 *  DocValuesField field = new DocValuesField(name);
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

  protected BytesRef bytes;
  protected Number numberValue;
  protected Comparator<BytesRef> bytesComparator;

  // nocommit sugar ctors taking byte, short, int, etc.?

  /**
   * Creates a new {@link DocValuesField} with the given name.
   */
  public DocValuesField(String name, DocValues.Type docValueType) {
    super(name, new FieldType());
    if (docValueType == null) {
      throw new NullPointerException("docValueType cannot be null");
    }
    FieldType ft = (FieldType) type;
    ft.setDocValueType(docValueType);
    ft.freeze();
  }

  public DocValuesField(String name, IndexableFieldType type) {
    super(name, type);
    if (type.docValueType() == null) {
      throw new NullPointerException("docValueType cannot be null");
    }
  }

  /**
   * Sets the given <code>long</code> value.
   */
  public void setInt(long value) {
    // nocommit assert type matches
    numberValue = value;
  }

  /**
   * Sets the given <code>int</code> value.
   */
  public void setInt(int value) {
    // nocommit assert type matches
    numberValue = value;
  }

  /**
   * Sets the given <code>short</code> value.
   */
  public void setInt(short value) {
    // nocommit assert type matches
    numberValue = value;
  }

  /**
   * Sets the given <code>byte</code> value.
   */
  public void setInt(byte value) {
    // nocommit assert type matches
    numberValue = value;
  }

  /**
   * Sets the given <code>float</code> value.
   */
  public void setFloat(float value) {
    // nocommit assert type matches
    numberValue = value;
  }

  /**
   * Sets the given <code>double</code> value.
   */
  public void setFloat(double value) {
    // nocommit assert type matches
    numberValue = value;
  }

  /**
   * Sets the given {@link BytesRef} value.
   */
  public void setBytes(BytesRef value) {
    bytes = value;
  }

  /**
   * Sets the given {@link BytesRef} value, the field's {@link Type} and the
   * field's comparator. If the {@link Comparator} is set to <code>null</code>
   * the default for the given {@link Type} is used instead.
   * 
   * @throws IllegalArgumentException
   *           if the value or the type are null
   */
  // nocommit what to do w/ comparator...
  /*
  public void setBytes(BytesRef value, DocValues.Type type, Comparator<BytesRef> comp) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    setDocValuesType(type);
    if (bytes == null) {
      bytes = BytesRef.deepCopyOf(value);
    } else {
      bytes.copyBytes(value);
    }
    bytesComparator = comp;
  }
  */

  @Override
  public BytesRef binaryValue() {
    return bytes;
  }

  /**
   * Returns the set {@link BytesRef} comparator or <code>null</code> if not set
   */
  /*
  public Comparator<BytesRef> bytesComparator() {
    return bytesComparator;
  }
  */

  @Override
  public Number numericValue() {
    return numberValue;
  }

  /**
   * Sets the {@link BytesRef} comparator for this field. If the field has a
   * numeric {@link Type} the comparator will be ignored.
   */
  /*
  public void setBytesComparator(Comparator<BytesRef> comp) {
    this.bytesComparator = comp;
  }
  */

  /**
   * Returns always <code>null</code>
   */
  @Override
  public Reader readerValue() {
    return null;
  }

  @Override
  public String toString() {
    final String value;
    switch (type.docValueType()) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_STRAIGHT:
    case BYTES_FIXED_SORTED:
    case BYTES_VAR_SORTED:
      // don't use to unicode string this is not necessarily unicode here
      value = "bytes: " + bytes.toString();
      break;
    case FIXED_INTS_16:
      value = "int16: " + numberValue;
      break;
    case FIXED_INTS_32:
      value = "int32: " + numberValue;
      break;
    case FIXED_INTS_64:
      value = "int64: " + numberValue;
      break;
    case FIXED_INTS_8:
      value = "int8: " + numberValue;
      break;
    case VAR_INTS:
      value = "vint: " + numberValue;
      break;
    case FLOAT_32:
      value = "float32: " + numberValue;
      break;
    case FLOAT_64:
      value = "float64: " + numberValue;
      break;
    default:
      throw new IllegalArgumentException("unknown type: " + type);
    }
    return "<" + name() + ": DocValuesField " + value + ">";
  }
}
