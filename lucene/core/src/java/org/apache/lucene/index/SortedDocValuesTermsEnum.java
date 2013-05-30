package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Implements a {@link TermsEnum} wrapping a provided
 * {@link SortedDocValues}. */

class SortedDocValuesTermsEnum extends TermsEnum {
  private final SortedDocValues values;
  private int currentOrd = -1;
  private final BytesRef term = new BytesRef();

  /** Creates a new TermsEnum over the provided values */
  public SortedDocValuesTermsEnum(SortedDocValues values) {
    this.values = values;
  }

  @Override
  public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {
    int ord = values.lookupTerm(text);
    if (ord >= 0) {
      currentOrd = ord;
      term.offset = 0;
      // TODO: is there a cleaner way?
      // term.bytes may be pointing to codec-private byte[]
      // storage, so we must force new byte[] allocation:
      term.bytes = new byte[text.length];
      term.copyBytes(text);
      return SeekStatus.FOUND;
    } else {
      currentOrd = -ord-1;
      if (currentOrd == values.getValueCount()) {
        return SeekStatus.END;
      } else {
        // TODO: hmm can we avoid this "extra" lookup?:
        values.lookupOrd(currentOrd, term);
        return SeekStatus.NOT_FOUND;
      }
    }
  }

  @Override
  public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
    int ord = values.lookupTerm(text);
    if (ord >= 0) {
      term.offset = 0;
      // TODO: is there a cleaner way?
      // term.bytes may be pointing to codec-private byte[]
      // storage, so we must force new byte[] allocation:
      term.bytes = new byte[text.length];
      term.copyBytes(text);
      currentOrd = ord;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void seekExact(long ord) throws IOException {
    assert ord >= 0 && ord < values.getValueCount();
    currentOrd = (int) ord;
    values.lookupOrd(currentOrd, term);
  }

  @Override
  public BytesRef next() throws IOException {
    currentOrd++;
    if (currentOrd >= values.getValueCount()) {
      return null;
    }
    values.lookupOrd(currentOrd, term);
    return term;
  }

  @Override
  public BytesRef term() throws IOException {
    return term;
  }

  @Override
  public long ord() throws IOException {
    return currentOrd;
  }

  @Override
  public int docFreq() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long totalTermFreq() {
    return -1;
  }

  @Override
  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }

  @Override
  public void seekExact(BytesRef term, TermState state) throws IOException {
    assert state != null && state instanceof OrdTermState;
    this.seekExact(((OrdTermState)state).ord);
  }

  @Override
  public TermState termState() throws IOException {
    OrdTermState state = new OrdTermState();
    state.ord = currentOrd;
    return state;
  }
}

