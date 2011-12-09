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
import org.apache.lucene.index.PerDocFieldValues;
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
public class DocValuesField extends Field implements PerDocFieldValues {

  protected BytesRef bytes;
  protected double doubleValue;
  protected long longValue;
  protected DocValues.Type type;
  protected Comparator<BytesRef> bytesComparator;

  /**
   * Creates a new {@link DocValuesField} with the given name.
   */
  public DocValuesField(String name) {
    this(name, new FieldType());
  }

  public DocValuesField(String name, IndexableFieldType type) {
    this(name, type, null);
  }

  public DocValuesField(String name, IndexableFieldType type, String value) {
    super(name, type);
    fieldsData = value;
  }

  @Override
  public PerDocFieldValues docValues() {
    return this;
  }

  /**
   * Sets the given <code>long</code> value and sets the field's {@link Type} to
   * {@link Type#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(DocValues.Type)}.
   */
  public void setInt(long value) {
    setInt(value, false);
  }
  
  /**
   * Sets the given <code>long</code> value as a 64 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link Type#FIXED_INTS_64} is used
   *          otherwise {@link Type#VAR_INTS}
   */
  public void setInt(long value, boolean fixed) {
    if (type == null) {
      type = fixed ? DocValues.Type.FIXED_INTS_64 : DocValues.Type.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>int</code> value and sets the field's {@link Type} to
   * {@link Type#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(DocValues.Type)}.
   */
  public void setInt(int value) {
    setInt(value, false);
  }

  /**
   * Sets the given <code>int</code> value as a 32 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link Type#FIXED_INTS_32} is used
   *          otherwise {@link Type#VAR_INTS}
   */
  public void setInt(int value, boolean fixed) {
    if (type == null) {
      type = fixed ? DocValues.Type.FIXED_INTS_32 : DocValues.Type.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>short</code> value and sets the field's {@link Type} to
   * {@link Type#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(DocValues.Type)}.
   */
  public void setInt(short value) {
    setInt(value, false);
  }

  /**
   * Sets the given <code>short</code> value as a 16 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link Type#FIXED_INTS_16} is used
   *          otherwise {@link Type#VAR_INTS}
   */
  public void setInt(short value, boolean fixed) {
    if (type == null) {
      type = fixed ? DocValues.Type.FIXED_INTS_16 : DocValues.Type.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>byte</code> value and sets the field's {@link Type} to
   * {@link Type#VAR_INTS} unless already set. If you want to change the
   * default type use {@link #setDocValuesType(DocValues.Type)}.
   */
  public void setInt(byte value) {
    setInt(value, false);
  }

  /**
   * Sets the given <code>byte</code> value as a 8 bit signed integer.
   * 
   * @param value
   *          the value to set
   * @param fixed
   *          if <code>true</code> {@link Type#FIXED_INTS_8} is used
   *          otherwise {@link Type#VAR_INTS}
   */
  public void setInt(byte value, boolean fixed) {
    if (type == null) {
      type = fixed ? DocValues.Type.FIXED_INTS_8 : DocValues.Type.VAR_INTS;
    }
    longValue = value;
  }

  /**
   * Sets the given <code>float</code> value and sets the field's {@link Type}
   * to {@link Type#FLOAT_32} unless already set. If you want to
   * change the type use {@link #setDocValuesType(DocValues.Type)}.
   */
  public void setFloat(float value) {
    if (type == null) {
      type = DocValues.Type.FLOAT_32;
    }
    doubleValue = value;
  }

  /**
   * Sets the given <code>double</code> value and sets the field's {@link Type}
   * to {@link Type#FLOAT_64} unless already set. If you want to
   * change the default type use {@link #setDocValuesType(DocValues.Type)}.
   */
  public void setFloat(double value) {
    if (type == null) {
      type = DocValues.Type.FLOAT_64;
    }
    doubleValue = value;
  }

  /**
   * Sets the given {@link BytesRef} value and the field's {@link Type}. The
   * comparator for this field is set to <code>null</code>. If a
   * <code>null</code> comparator is set the default comparator for the given
   * {@link Type} is used.
   */
  public void setBytes(BytesRef value, DocValues.Type type) {
    setBytes(value, type, null);
  }

  /**
   * Sets the given {@link BytesRef} value, the field's {@link Type} and the
   * field's comparator. If the {@link Comparator} is set to <code>null</code>
   * the default for the given {@link Type} is used instead.
   * 
   * @throws IllegalArgumentException
   *           if the value or the type are null
   */
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

  /**
   * Returns the set {@link BytesRef} or <code>null</code> if not set.
   */
  public BytesRef getBytes() {
    return bytes;
  }

  /**
   * Returns the set {@link BytesRef} comparator or <code>null</code> if not set
   */
  public Comparator<BytesRef> bytesComparator() {
    return bytesComparator;
  }

  /**
   * Returns the set floating point value or <code>0.0d</code> if not set.
   */
  public double getFloat() {
    return doubleValue;
  }

  /**
   * Returns the set <code>long</code> value of <code>0</code> if not set.
   */
  public long getInt() {
    return longValue;
  }

  /**
   * Sets the {@link BytesRef} comparator for this field. If the field has a
   * numeric {@link Type} the comparator will be ignored.
   */
  public void setBytesComparator(Comparator<BytesRef> comp) {
    this.bytesComparator = comp;
  }

  /**
   * Sets the {@link Type} for this field.
   */
  public void setDocValuesType(DocValues.Type type) {
    if (type == null) {
      throw new IllegalArgumentException("Type must not be null");
    }
    this.type = type;
  }

  /**
   * Returns always <code>null</code>
   */
  public Reader readerValue() {
    return null;
  }

  @Override
  public DocValues.Type docValuesType() {
    return type;
  }

  @Override
  public String toString() {
    final String value;
    switch (type) {
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
      value = "int16: " + longValue;
      break;
    case FIXED_INTS_32:
      value = "int32: " + longValue;
      break;
    case FIXED_INTS_64:
      value = "int64: " + longValue;
      break;
    case FIXED_INTS_8:
      value = "int8: " + longValue;
      break;
    case VAR_INTS:
      value = "vint: " + longValue;
      break;
    case FLOAT_32:
      value = "float32: " + doubleValue;
      break;
    case FLOAT_64:
      value = "float64: " + doubleValue;
      break;
    default:
      throw new IllegalArgumentException("unknown type: " + type);
    }
    return "<" + name() + ": DocValuesField " + value + ">";
  }

  /**
   * Returns an DocValuesField holding the value from
   * the provided string field, as the specified type.  The
   * incoming field must have a string value.  The name, {@link
   * FieldType} and string value are carried over from the
   * incoming Field.
   */
  public static DocValuesField build(Field field, DocValues.Type type) {
    if (field instanceof DocValuesField) {
      return (DocValuesField) field;
    }
    final DocValuesField valField = new DocValuesField(field.name(), field.fieldType(), field.stringValue());
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_STRAIGHT:
    case BYTES_FIXED_SORTED:
    case BYTES_VAR_SORTED:
      BytesRef ref = field.isBinary() ? field.binaryValue() : new BytesRef(field.stringValue());
      valField.setBytes(ref, type);
      break;
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
    case VAR_INTS:
      valField.setInt(Long.parseLong(field.stringValue()));
      break;
    case FLOAT_32:
      valField.setFloat(Float.parseFloat(field.stringValue()));
      break;
    case FLOAT_64:
      valField.setFloat(Double.parseDouble(field.stringValue()));
      break;
    default:
      throw new IllegalArgumentException("unknown type: " + type);
    }
    return valField;
  }
}
