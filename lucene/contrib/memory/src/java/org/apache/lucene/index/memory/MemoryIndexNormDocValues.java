package org.apache.lucene.index.memory;
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

import org.apache.lucene.index.Norm;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.util.BytesRef;

/**
 * 
 * @lucene.internal
 */
class MemoryIndexNormDocValues extends DocValues {

  private final Source source;

  MemoryIndexNormDocValues(Source source) {
    this.source = source;
  }
  @Override
  public Source load() throws IOException {
    return source;
  }

  @Override
  public Source getDirectSource() throws IOException {
    return source;
  }

  @Override
  public Type getType() {
    return source.getType();
  }
  
  @Override
  public int getValueSize() {
    return 1;
  }

  public static class SingleValueSource extends Source {

    private final Number numericValue;
    private final BytesRef binaryValue;

    protected SingleValueSource(Norm norm) {
      super(norm.type());
      this.numericValue = norm.field().numericValue();
      this.binaryValue = norm.field().binaryValue();
    }

    @Override
    public long getInt(int docID) {
      switch (type) {
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FIXED_INTS_8:
      case VAR_INTS:
        assert numericValue != null;
        return numericValue.longValue();
      }
      return super.getInt(docID);
    }

    @Override
    public double getFloat(int docID) {
      switch (type) {
      case FLOAT_32:
      case FLOAT_64:
        assert numericValue != null;
        return numericValue.floatValue();
      }
      return super.getFloat(docID);
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      switch (type) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        assert binaryValue != null;
        ref.copyBytes(binaryValue);
        return ref;
      }
      return super.getBytes(docID, ref);
    }

    @Override
    public boolean hasArray() {
      return true;
    }

    @Override
    public Object getArray() {
      switch (type) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        return binaryValue.bytes;
      case FIXED_INTS_16:
        return new short[] { numericValue.shortValue() };
      case FIXED_INTS_32:
        return new int[] { numericValue.intValue() };
      case FIXED_INTS_64:
        return new long[] { numericValue.longValue() };
      case FIXED_INTS_8:
        return new byte[] { numericValue.byteValue() };
      case VAR_INTS:
        return new long[] { numericValue.longValue() };
      case FLOAT_32:
        return new float[] { numericValue.floatValue() };
      case FLOAT_64:
        return new double[] { numericValue.doubleValue() };
      default:
        throw new IllegalArgumentException("unknown type " + type);
      }

    }
  }

}
