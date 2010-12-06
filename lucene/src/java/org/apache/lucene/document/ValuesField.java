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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.Values;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
@SuppressWarnings("serial")
public class ValuesField extends AbstractField implements PerDocFieldValues {

  protected BytesRef bytes;
  protected double doubleValue;
  protected long longValue;
  protected Values type;
  protected Comparator<BytesRef> bytesComparator;
  
  public ValuesField(String name) {
    super(name, Store.NO, Index.NO, TermVector.NO);
    setDocValues(this);
  }

  ValuesField() {
    this("");
  }
  
  public void setInt(long value) {
    type = Values.PACKED_INTS;
    longValue = value;
  }

  public void setFloat(float value) {
    type = Values.SIMPLE_FLOAT_4BYTE;
    doubleValue = value;
  }

  public void setFloat(double value) {
    type = Values.SIMPLE_FLOAT_8BYTE;
    doubleValue = value;
  }

  public void setBytes(BytesRef value, Values type) {
    setBytes(value, type, null);

  }

  public void setBytes(BytesRef value, Values type, Comparator<BytesRef> comp) {
    this.type = type;
    if (bytes == null) {
      this.bytes = new BytesRef();
    }
    bytes.copy(value);
    bytesComparator = comp;
  }

  public BytesRef getBytes() {
    return bytes;
  }

  public Comparator<BytesRef> bytesComparator() {
    return bytesComparator;
  }

  public double getFloat() {
    return doubleValue;
  }

  public long getInt() {
    return longValue;
  }

  public void setBytesComparator(Comparator<BytesRef> comp) {
    this.bytesComparator = comp;
  }

  public void setType(Values type) {
    this.type = type;
  }

  public Values type() {
    return type;
  }

  public Reader readerValue() {
    return null;
  }

  public String stringValue() {
    return null;
  }

  public TokenStream tokenStreamValue() {
    return tokenStream;
  }

  public <T extends AbstractField> T set(T field) {
    field.setDocValues(this);
    return field;
  }

  public static <T extends AbstractField> T set(T field, Values type) {
    if (field instanceof ValuesField)
      return field;
    final ValuesField valField = new ValuesField();
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      BytesRef ref = field.isBinary() ? new BytesRef(field.getBinaryValue(),
          field.getBinaryOffset(), field.getBinaryLength()) : new BytesRef(
          field.stringValue());
      valField.setBytes(ref, type);
      break;
    case PACKED_INTS:
      valField.setInt(Long.parseLong(field.stringValue()));
      break;
    case SIMPLE_FLOAT_4BYTE:
      valField.setFloat(Float.parseFloat(field.stringValue()));
      break;
    case SIMPLE_FLOAT_8BYTE:
      valField.setFloat(Double.parseDouble(field.stringValue()));
      break;
    default:
      throw new IllegalArgumentException("unknown type: " + type);
    }
    return valField.set(field);
  }

}
