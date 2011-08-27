package org.apache.lucene.index.values;

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

import org.apache.lucene.document.IndexDocValuesField;
import org.apache.lucene.index.codecs.DocValuesConsumer;
import org.apache.lucene.util.BytesRef;

/**
 * Per document and field values consumed by {@link DocValuesConsumer}. 
 * @see IndexDocValuesField
 * 
 * @lucene.experimental
 */
public interface PerDocFieldValues {

  /**
   * Sets the given <code>long</code> value.
   */
  public void setInt(long value);

  /**
   * Sets the given <code>float</code> value.
   */
  public void setFloat(float value);

  /**
   * Sets the given <code>double</code> value.
   */
  public void setFloat(double value);

  /**
   * Sets the given {@link BytesRef} value and the field's {@link ValueType}. The
   * comparator for this field is set to <code>null</code>. If a
   * <code>null</code> comparator is set the default comparator for the given
   * {@link ValueType} is used.
   */
  public void setBytes(BytesRef value, ValueType type);

  /**
   * Sets the given {@link BytesRef} value, the field's {@link ValueType} and the
   * field's comparator. If the {@link Comparator} is set to <code>null</code>
   * the default for the given {@link ValueType} is used instead.
   */
  public void setBytes(BytesRef value, ValueType type, Comparator<BytesRef> comp);

  /**
   * Returns the set {@link BytesRef} or <code>null</code> if not set.
   */
  public BytesRef getBytes();

  /**
   * Returns the set {@link BytesRef} comparator or <code>null</code> if not set
   */
  public Comparator<BytesRef> bytesComparator();

  /**
   * Returns the set floating point value or <code>0.0d</code> if not set.
   */
  public double getFloat();

  /**
   * Returns the set <code>long</code> value of <code>0</code> if not set.
   */
  public long getInt();

  /**
   * Sets the {@link BytesRef} comparator for this field. If the field has a
   * numeric {@link ValueType} the comparator will be ignored.
   */
  public void setBytesComparator(Comparator<BytesRef> comp);

  /**
   * Sets the {@link ValueType}
   */
  public void setDocValuesType(ValueType type);

  /**
  * Returns the {@link ValueType}
  */
  public ValueType docValuesType();
}
