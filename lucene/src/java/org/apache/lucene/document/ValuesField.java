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
import java.io.IOException;
import java.io.Reader;
import java.util.Comparator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.values.Values;
import org.apache.lucene.index.values.ValuesAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
@SuppressWarnings("serial")
public class ValuesField extends AbstractField {
  private final ValuesAttribute attr;
  private final AttributeSource fieldAttributes;
  

  public ValuesField(String name) {
    super(name, Store.NO, Index.NO, TermVector.NO);
    fieldAttributes = getFieldAttributes();
    attr = fieldAttributes.addAttribute(ValuesAttribute.class);
  }
  
  ValuesField() {
    this("");
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

  public void setInt(long value) {
    attr.setType(Values.PACKED_INTS);
    attr.ints().set(value);
  }

  public void setFloat(float value) {
    attr.setType(Values.SIMPLE_FLOAT_4BYTE);
    attr.floats().set(value);
  }

  public void setFloat(double value) {
    attr.setType(Values.SIMPLE_FLOAT_8BYTE);
    attr.floats().set(value);
  }

  public void setBytes(BytesRef value, Values type) {
    setBytes(value, type, null);

  }

  public void setBytes(BytesRef value, Values type, Comparator<BytesRef> comp) {
    attr.setType(type);
    attr.bytes().copy(value);
    attr.setBytesComparator(comp);
  }

  public ValuesAttribute values() {
    return attr;
  }
  
  public <T extends Fieldable> T set(T field) {
    AttributeSource src = field.getFieldAttributes();
    src.addAttribute(ValuesAttribute.class);
    fieldAttributes.copyTo(field.getFieldAttributes());
    return field;
  }
  
  public static ValuesAttribute values(Fieldable fieldable) {
    return fieldable.getFieldAttributes().addAttribute(ValuesAttribute.class);
  }

  public static <T extends Fieldable> T set(T field, Values type) {
    if(field instanceof ValuesField)
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
          field.getBinaryOffset(), field.getBinaryLength()) : new BytesRef(field
          .stringValue());
      valField.setBytes(ref, type);
      break;
    case PACKED_INTS:
    case PACKED_INTS_FIXED:
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
