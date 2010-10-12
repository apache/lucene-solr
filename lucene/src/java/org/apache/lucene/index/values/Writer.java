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
import java.util.List;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public abstract class Writer {

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

  public static class MergeState {
    public final Reader reader;
    public final int docBase;
    public final int docCount;
    public final Bits bits;

    public MergeState(Reader reader, int docBase, int docCount, Bits bits) {
      assert reader != null;
      this.reader = reader;
      this.docBase = docBase;
      this.docCount = docCount;
      this.bits = bits;
    }
  }

  public void add(List<MergeState> states) throws IOException {
    for (MergeState state : states) {
      merge(state);
    }
  }

  // enables bulk copies in subclasses per MergeState
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
}
