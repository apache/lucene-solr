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
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.codecs.docvalues.DocValuesConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * @lucene.experimental
 */
public abstract class Writer extends DocValuesConsumer {

  protected Writer(AtomicLong bytesUsed) {
    super(bytesUsed);
  }

  public static final String INDEX_EXTENSION = "idx";
  public static final String DATA_EXTENSION = "dat";

  /** Records the specfied value for the docID */
  public void add(int docID, long value) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Records the specfied value for the docID */
  public void add(int docID, double value) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Records the specfied value for the docID */
  public void add(int docID, BytesRef value) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Records the specfied value for the docID */
  protected abstract void add(int docID) throws IOException;
  
  protected abstract void setNextEnum(DocValuesEnum valuesEnum);

  /** Finish writing, close any files */
  public abstract void finish(int docCount) throws IOException;

  // enables bulk copies in subclasses per MergeState
  @Override
  protected void merge(MergeState state) throws IOException {
    final DocValuesEnum valEnum = state.reader.getEnum();
    assert valEnum != null;
    try {
      setNextEnum(valEnum);
      int docID = state.docBase;
      final Bits bits = state.bits;
      final int docCount = state.docCount;
      int currentDocId;
      if ((currentDocId = valEnum.advance(0)) != DocValuesEnum.NO_MORE_DOCS) {
        for (int i = 0; i < docCount; i++) {
          if (bits == null || !bits.get(i)) {
            if (currentDocId < i) {
              if ((currentDocId = valEnum.advance(i)) == DocValuesEnum.NO_MORE_DOCS) {
                break; // advance can jump over default values
              }
            }
            if (currentDocId == i) { // we are on the doc to merge
              add(docID);
            }
            ++docID;
          }
        }
      }
    } finally {
      valEnum.close();
    }
  }

  public static Writer create(Type v, String id, Directory directory,
      Comparator<BytesRef> comp, AtomicLong bytesUsed) throws IOException {
    switch (v) {
    case PACKED_INTS:
      return Ints.getWriter(directory, id, true, bytesUsed);
    case SIMPLE_FLOAT_4BYTE:
      return Floats.getWriter(directory, id, 4, bytesUsed);
    case SIMPLE_FLOAT_8BYTE:
      return Floats.getWriter(directory, id, 8, bytesUsed);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, comp, true, bytesUsed);
    case BYTES_FIXED_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, comp, true, bytesUsed);
    case BYTES_FIXED_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, comp, true, bytesUsed);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, comp, false, bytesUsed);
    case BYTES_VAR_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, comp, false, bytesUsed);
    case BYTES_VAR_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, comp, false, bytesUsed);
    default:
      throw new IllegalArgumentException("Unknown Values: " + v);
    }
  }
}
