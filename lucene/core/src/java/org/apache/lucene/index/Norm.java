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
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

/**
 * Stores the normalization value computed in
 * {@link Similarity#computeNorm(FieldInvertState, Norm)} per field.
 * Normalization values must be consistent within a single field, different
 * value types are not permitted within a single field. All values set must be
 * fixed size values ie. all values passed to {@link Norm#setBytes(BytesRef)}
 * must have the same length per field.
 * 
 * @lucene.experimental
 * @lucene.internal
 */
public final class Norm  {
  private DocValuesField field;
  private BytesRef spare;
  
  /**
   * Returns the {@link IndexableField} representation for this norm
   */
  public IndexableField field() {
    return field;
  }
  
  /**
   * Returns the {@link Type} for this norm.
   */
  public Type type() {
    return field == null? null : field.fieldType().docValueType();
  }
  
  /**
   * Returns a spare {@link BytesRef} 
   */
  public BytesRef getSpare() {
    if (spare == null) {
      spare = new BytesRef();
    }
    return spare;
  }

  /**
   * Sets a float norm value
   */
  public void setFloat(float norm) {
    setType(Type.FLOAT_32);
    this.field.setFloatValue(norm);
  }

  /**
   * Sets a double norm value
   */
  public void setDouble(double norm) {
    setType(Type.FLOAT_64);
    this.field.setDoubleValue(norm);
  }

  /**
   * Sets a short norm value
   */
  public void setShort(short norm) {
    setType(Type.FIXED_INTS_16);
    this.field.setIntValue(norm);
    
  }

  /**
   * Sets a int norm value
   */
  public void setInt(int norm) {
    setType(Type.FIXED_INTS_32);
    this.field.setIntValue(norm);
  }

  /**
   * Sets a long norm value
   */
  public void setLong(long norm) {
    setType(Type.FIXED_INTS_64);
    this.field.setLongValue(norm);
  }

  /**
   * Sets a byte norm value
   */
  public void setByte(byte norm) {
    setType(Type.FIXED_INTS_8);
    this.field.setIntValue(norm);
  }

  /**
   * Sets a fixed byte array norm value
   */
  public void setBytes(BytesRef norm) {
    setType(Type.BYTES_FIXED_STRAIGHT);
    this.field.setBytesValue(norm);
  }

  
  private void setType(Type type) {
    if (field != null) {
      if (type != field.fieldType().docValueType()) {
        throw new IllegalArgumentException("FieldType missmatch - expected "+type+" but was " + field.fieldType().docValueType());
      }
    } else {
      switch (type) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        this.field = new DocValuesField("", new BytesRef(), type);
        break;

      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FIXED_INTS_8:
      case VAR_INTS:
        this.field = new DocValuesField("", 0, type);
        break;
      case FLOAT_32:
      case FLOAT_64:
        this.field = new DocValuesField("", 0f, type);
        break;
      default:
        throw new IllegalArgumentException("unknown Type: " + type);
      }
    }
  }

}