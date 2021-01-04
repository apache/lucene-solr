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
package org.apache.lucene.index;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.io.IOException;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** An in-place update to a DocValues field. */
abstract class DocValuesUpdate {

  /* Rough logic: OBJ_HEADER + 3*PTR + INT
   * Term: OBJ_HEADER + 2*PTR
   *   Term.field: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   *   Term.bytes: 2*OBJ_HEADER + 2*INT + PTR + bytes.length
   * String: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   * T: OBJ_HEADER
   */
  private static final int RAW_SIZE_IN_BYTES =
      8 * NUM_BYTES_OBJECT_HEADER + 8 * NUM_BYTES_OBJECT_REF + 8 * Integer.BYTES;

  final DocValuesType type;
  final Term term;
  final String field;
  // used in BufferedDeletes to apply this update only to a slice of docs. It's initialized to
  // BufferedUpdates.MAX_INT
  // since it's safe and most often used this way we safe object creations.
  final int docIDUpTo;
  final boolean hasValue;

  /**
   * Constructor.
   *
   * @param term the {@link Term} which determines the documents that will be updated
   * @param field the {@link NumericDocValuesField} to update
   */
  protected DocValuesUpdate(
      DocValuesType type, Term term, String field, int docIDUpTo, boolean hasValue) {
    assert docIDUpTo >= 0 : docIDUpTo + "must be >= 0";
    this.type = type;
    this.term = term;
    this.field = field;
    this.docIDUpTo = docIDUpTo;
    this.hasValue = hasValue;
  }

  abstract long valueSizeInBytes();

  final long sizeInBytes() {
    long sizeInBytes = RAW_SIZE_IN_BYTES;
    sizeInBytes += term.field.length() * Character.BYTES;
    sizeInBytes += term.bytes.bytes.length;
    sizeInBytes += field.length() * Character.BYTES;
    sizeInBytes += valueSizeInBytes();
    sizeInBytes += 1; // hasValue
    return sizeInBytes;
  }

  protected abstract String valueToString();

  abstract void writeTo(DataOutput output) throws IOException;

  boolean hasValue() {
    return hasValue;
  }

  @Override
  public String toString() {
    return "term="
        + term
        + ",field="
        + field
        + ",value="
        + valueToString()
        + ",docIDUpTo="
        + docIDUpTo;
  }

  /** An in-place update to a binary DocValues field */
  static final class BinaryDocValuesUpdate extends DocValuesUpdate {
    private final BytesRef value;

    /* Size of BytesRef: 2*INT + ARRAY_HEADER + PTR */
    private static final long RAW_VALUE_SIZE_IN_BYTES =
        2L * Integer.BYTES + NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF;

    BinaryDocValuesUpdate(Term term, String field, BytesRef value) {
      this(term, field, value, BufferedUpdates.MAX_INT);
    }

    private BinaryDocValuesUpdate(Term term, String field, BytesRef value, int docIDUpTo) {
      super(DocValuesType.BINARY, term, field, docIDUpTo, value != null);
      this.value = value;
    }

    BinaryDocValuesUpdate prepareForApply(int docIDUpTo) {
      if (docIDUpTo == this.docIDUpTo) {
        return this; // it's a final value so we can safely reuse this instance
      }
      return new BinaryDocValuesUpdate(term, field, value, docIDUpTo);
    }

    @Override
    long valueSizeInBytes() {
      return RAW_VALUE_SIZE_IN_BYTES + (value == null ? 0 : value.bytes.length);
    }

    @Override
    protected String valueToString() {
      return value.toString();
    }

    BytesRef getValue() {
      assert hasValue : "getValue should only be called if this update has a value";
      return value;
    }

    @Override
    void writeTo(DataOutput out) throws IOException {
      assert hasValue;
      out.writeVInt(value.length);
      out.writeBytes(value.bytes, value.offset, value.length);
    }

    static BytesRef readFrom(DataInput in, BytesRef scratch) throws IOException {
      scratch.length = in.readVInt();
      if (scratch.bytes.length < scratch.length) {
        scratch.bytes = ArrayUtil.grow(scratch.bytes, scratch.length);
      }
      in.readBytes(scratch.bytes, 0, scratch.length);
      return scratch;
    }
  }

  /** An in-place update to a numeric DocValues field */
  static final class NumericDocValuesUpdate extends DocValuesUpdate {
    private final long value;

    NumericDocValuesUpdate(Term term, String field, long value) {
      this(term, field, value, BufferedUpdates.MAX_INT, true);
    }

    NumericDocValuesUpdate(Term term, String field, Long value) {
      this(term, field, value != null ? value : -1, BufferedUpdates.MAX_INT, value != null);
    }

    private NumericDocValuesUpdate(
        Term term, String field, long value, int docIDUpTo, boolean hasValue) {
      super(DocValuesType.NUMERIC, term, field, docIDUpTo, hasValue);
      this.value = value;
    }

    NumericDocValuesUpdate prepareForApply(int docIDUpTo) {
      if (docIDUpTo == this.docIDUpTo) {
        return this;
      }
      return new NumericDocValuesUpdate(term, field, value, docIDUpTo, hasValue);
    }

    @Override
    long valueSizeInBytes() {
      return Long.BYTES;
    }

    @Override
    protected String valueToString() {
      return hasValue ? Long.toString(value) : "null";
    }

    @Override
    void writeTo(DataOutput out) throws IOException {
      assert hasValue;
      out.writeZLong(value);
    }

    static long readFrom(DataInput in) throws IOException {
      return in.readZLong();
    }

    long getValue() {
      assert hasValue : "getValue should only be called if this update has a value";
      return value;
    }
  }
}
