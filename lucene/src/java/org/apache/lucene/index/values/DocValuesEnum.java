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
import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;

/**
 * {@link DocValuesEnum} is a {@link DocIdSetIterator} iterating <tt>byte[]</tt>
 * , <tt>long</tt> and <tt>double</tt> stored per document. Depending on the
 * enum's {@link ValueType} ({@link #type()}) the enum might skip over documents that
 * have no value stored. Types like {@link ValueType#BYTES_VAR_STRAIGHT} might not
 * skip over documents even if there is no value associated with a document. The
 * value for document without values again depends on the types implementation
 * although a reference for a {@link ValueType} returned from a accessor method
 * {@link #getFloat()}, {@link #getInt()} or {@link #bytes()} will never be
 * <code>null</code> even if a document has no value.
 * <p>
 * Note: Only the reference for the enum's type are initialized to non
 * <code>null</code> ie. {@link #getInt()} will always return <code>null</code>
 * if the enum's Type is {@link ValueType#FLOAT_32}.
 * 
 * @lucene.experimental
 */
public abstract class DocValuesEnum extends DocIdSetIterator {
  private AttributeSource source;
  private final ValueType enumType;
  protected BytesRef bytesRef;
  protected FloatsRef floatsRef;
  protected LongsRef intsRef;

  /**
   * Creates a new {@link DocValuesEnum} for the given type. The
   * {@link AttributeSource} for this enum is set to <code>null</code>
   */
  protected DocValuesEnum(ValueType enumType) {
    this(null, enumType);
  }

  /**
   * Creates a new {@link DocValuesEnum} for the given type.
   */
  protected DocValuesEnum(AttributeSource source, ValueType enumType) {
    this.source = source;
    this.enumType = enumType;
    switch (enumType) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      bytesRef = new BytesRef();
      break;
    case INTS:
      intsRef = new LongsRef(1);
      break;
    case FLOAT_32:
    case FLOAT_64:
      floatsRef = new FloatsRef(1);
      break;
    }
  }

  /**
   * Returns the type of this enum
   */
  public ValueType type() {
    return enumType;
  }

  /**
   * Returns a {@link BytesRef} or <code>null</code> if this enum doesn't
   * enumerate byte[] values
   */
  public BytesRef bytes() {
    return bytesRef;
  }

  /**
   * Returns a {@link FloatsRef} or <code>null</code> if this enum doesn't
   * enumerate floating point values
   */
  public FloatsRef getFloat() {
    return floatsRef;
  }

  /**
   * Returns a {@link LongsRef} or <code>null</code> if this enum doesn't
   * enumerate integer values.
   */
  public LongsRef getInt() {
    return intsRef;
  }

  /**
   * Copies the internal state from the given enum
   */
  protected void copyFrom(DocValuesEnum valuesEnum) {
    intsRef = valuesEnum.intsRef;
    floatsRef = valuesEnum.floatsRef;
    bytesRef = valuesEnum.bytesRef;
    source = valuesEnum.source;
  }

  /**
   * Returns the {@link AttributeSource} associated with this enum.
   * <p>
   * Note: this method might create a new AttribueSource if no
   * {@link AttributeSource} has been provided during enum creation.
   */
  public AttributeSource attributes() {
    if (source == null) {
      source = new AttributeSource();
    }
    return source;
  }

  /**
   * Closes the enum
   * 
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public abstract void close() throws IOException;

  /**
   * Returns an empty {@link DocValuesEnum} for the given {@link ValueType}.
   */
  public static DocValuesEnum emptyEnum(ValueType type) {
    return new DocValuesEnum(type) {
      @Override
      public int nextDoc() throws IOException {
        return NO_MORE_DOCS;
      }

      @Override
      public int docID() {
        return NO_MORE_DOCS;
      }

      @Override
      public int advance(int target) throws IOException {
        return NO_MORE_DOCS;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }

}
