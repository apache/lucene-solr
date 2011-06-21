package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class InstantiatedTermsEnum extends TermsEnum {
  private final String field;
  private final InstantiatedTerm[] terms;
  private final BytesRef br = new BytesRef();
  private final int start;
  private int upto;

  public InstantiatedTermsEnum(InstantiatedTerm[] terms, int start, String field) {
    this.start = start;
    upto = start-1;
    this.field = field;
    this.terms = terms;
  }

  @Override
  public SeekStatus seek(BytesRef text, boolean useCache) {
    final Term t = new Term(field, text);
    int loc = Arrays.binarySearch(terms, t, InstantiatedTerm.termComparator);
    if (loc < 0) {
      upto = -loc - 1;
      if (upto >= terms.length) {
        return SeekStatus.END;
      } else {
        br.copy(terms[upto].getTerm().bytes());
        return SeekStatus.NOT_FOUND;
      }
    } else {
      upto = loc;
      br.copy(text);
      return SeekStatus.FOUND;
    }
  }

  @Override
  public SeekStatus seek(long ord) {
    upto = start + (int) ord;
    if (upto >= terms.length) {
      return SeekStatus.END;
    }

    if (terms[upto].field().equals(field)) {
      return SeekStatus.FOUND;
    } else {
      return SeekStatus.END;
    }
  }

  @Override
  public BytesRef next() {
    upto++;
    if (upto >= terms.length) {
      return null;
    }
    if (terms[upto].field().equals(field)) {
      br.copy(terms[upto].getTerm().text());
      return br;
    } else {
      return null;
    }
  }

  @Override
  public BytesRef term() {
    return br;
  }

  @Override
  public long ord() {
    return upto - start;
  }

  @Override
  public int docFreq() {
    return terms[upto].getAssociatedDocuments().length;
  }

  @Override
  public long totalTermFreq() {
    final long v = terms[upto].getTotalTermFreq();
    return v == 0 ? -1 : v;
  }

  @Override
  public DocsEnum docs(Bits skipDocs, DocsEnum reuse) {
    if (reuse == null || !(reuse instanceof InstantiatedDocsEnum)) {
      reuse = new InstantiatedDocsEnum();
    }
    return ((InstantiatedDocsEnum) reuse).reset(skipDocs, terms[upto]);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) {
    if (reuse == null || !(reuse instanceof InstantiatedDocsAndPositionsEnum)) {
      reuse = new InstantiatedDocsAndPositionsEnum();
    }
    return ((InstantiatedDocsAndPositionsEnum) reuse).reset(skipDocs, terms[upto]);
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }

  @Override
  public TermState termState() throws IOException {
    final OrdTermState state = new OrdTermState();
    state.ord = upto - start;
    return state;
  }

  @Override
  public void seek(BytesRef term, TermState state) throws IOException {
    assert state != null && state instanceof OrdTermState;
    seek(((OrdTermState)state).ord); // just use the ord for simplicity
  }
}

