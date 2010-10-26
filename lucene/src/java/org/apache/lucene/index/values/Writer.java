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

import org.apache.lucene.index.codecs.docvalues.DocValuesConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public abstract class Writer extends DocValuesConsumer {
  
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

  protected abstract void setNextAttribute(ValuesAttribute attr);

  /** Finish writing, close any files */
  public abstract void finish(int docCount) throws IOException;

  // enables bulk copies in subclasses per MergeState
  @Override
  protected void merge(MergeState state) throws IOException {
    final ValuesEnum valEnum = state.reader.getEnum();
    assert valEnum != null;
    try {
      final ValuesAttribute attr = valEnum.addAttribute(ValuesAttribute.class);
      setNextAttribute(attr);
      int docID = state.docBase;
      final Bits bits = state.bits;
      final int docCount = state.docCount;
      for (int i = 0; i < docCount; i++) {
        if (bits == null || !bits.get(i)) {
          if (valEnum.advance(i) == ValuesEnum.NO_MORE_DOCS)
            break;
          add(docID++);
        }
      }
    } finally {
      valEnum.close();
    }
  }
  
  public static Writer create(Values v, String id,
      Directory directory, Comparator<BytesRef> comp) throws IOException {
    switch (v) {
    case PACKED_INTS:
    case PACKED_INTS_FIXED:
      return Ints.getWriter(directory, id, true);
    case SIMPLE_FLOAT_4BYTE:
      return Floats.getWriter(directory, id, 4);
    case SIMPLE_FLOAT_8BYTE:
      return Floats.getWriter(directory, id, 8);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, comp, true);
    case BYTES_FIXED_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, comp, true);
    case BYTES_FIXED_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, comp, true);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, comp, false);
    case BYTES_VAR_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, comp, false);
    case BYTES_VAR_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, comp, false);
    default:
      throw new IllegalArgumentException("Unknown Values: " + v);
    }
  }
}
