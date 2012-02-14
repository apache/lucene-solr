package org.apache.lucene.codecs.lucene40.values;

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

import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * Base class for disk resident source implementations
 * @lucene.internal
 */
abstract class DirectSource extends Source {

  protected final IndexInput data;
  private final ToNumeric toNumeric;
  protected final long baseOffset;

  public DirectSource(IndexInput input, Type type) {
    super(type);
    this.data = input;
    baseOffset = input.getFilePointer();
    switch (type) {
    case FIXED_INTS_16:
      toNumeric = new ShortToLong();
      break;
    case FLOAT_32:
    case FIXED_INTS_32:
      toNumeric = new IntToLong();
      break;
    case FIXED_INTS_8:
      toNumeric = new ByteToLong();
      break;
    default:
      toNumeric = new LongToLong();
    }
  }

  @Override
  public BytesRef getBytes(int docID, BytesRef ref) {
    try {
      final int sizeToRead = position(docID);
      ref.grow(sizeToRead);
      data.readBytes(ref.bytes, 0, sizeToRead);
      ref.length = sizeToRead;
      ref.offset = 0;
      return ref;
    } catch (IOException ex) {
      throw new IllegalStateException("failed to get value for docID: " + docID, ex);
    }
  }

  @Override
  public long getInt(int docID) {
    try {
      position(docID);
      return toNumeric.toLong(data);
    } catch (IOException ex) {
      throw new IllegalStateException("failed to get value for docID: " + docID, ex);
    }
  }

  @Override
  public double getFloat(int docID) {
    try {
      position(docID);
      return toNumeric.toDouble(data);
    } catch (IOException ex) {
      throw new IllegalStateException("failed to get value for docID: " + docID, ex);
    }
  }

  protected abstract int position(int docID) throws IOException;

  private abstract static class ToNumeric {
    abstract long toLong(IndexInput input) throws IOException;

    double toDouble(IndexInput input) throws IOException {
      return toLong(input);
    }
  }

  private static final class ByteToLong extends ToNumeric {
    @Override
    long toLong(IndexInput input) throws IOException {
      return input.readByte();
    }

  }

  private static final class ShortToLong extends ToNumeric {
    @Override
    long toLong(IndexInput input) throws IOException {
      return input.readShort();
    }
  }

  private static final class IntToLong extends ToNumeric {
    @Override
    long toLong(IndexInput input) throws IOException {
      return input.readInt();
    }

    double toDouble(IndexInput input) throws IOException {
      return Float.intBitsToFloat(input.readInt());
    }
  }

  private static final class LongToLong extends ToNumeric {
    @Override
    long toLong(IndexInput input) throws IOException {
      return input.readLong();
    }

    double toDouble(IndexInput input) throws IOException {
      return Double.longBitsToDouble(input.readLong());
    }
  }

}
