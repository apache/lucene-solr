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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link org.apache.lucene.index.TermEnum} navigating an {@link org.apache.lucene.store.instantiated.InstantiatedIndexReader}.
 */
public class InstantiatedTermEnum
    extends TermEnum {

  private final InstantiatedIndexReader reader;

  public InstantiatedTermEnum(InstantiatedIndexReader reader) {
    this.nextTermIndex = 0;
    this.reader = reader;
  }

  public InstantiatedTermEnum(InstantiatedIndexReader reader, int startPosition) {
    this.reader = reader;
    this.nextTermIndex = startPosition;
    next();
  }

  private int nextTermIndex;
  private InstantiatedTerm term;

  /**
   * Increments the enumeration to the next element.  True if one exists.
   */
  public boolean next() {
    if (reader.getIndex().getOrderedTerms().length <= nextTermIndex) {
      return false;
    } else {
      term = reader.getIndex().getOrderedTerms()[nextTermIndex];
      nextTermIndex++;
      return true;
    }
  }

  /**
   * Returns the current Term in the enumeration.
   */
  public Term term() {
    return term == null ? null : term.getTerm();
  }

  /**
   * Returns the docFreq of the current Term in the enumeration.
   */
  public int docFreq() {
    return term.getAssociatedDocuments().length;
  }

  /**
   * Closes the enumeration to further activity, freeing resources.
   */
  public void close() {
  }


  public boolean skipTo(Term target) throws IOException {

    // this method is not known to be used by anything
    // in lucene for many years now, so there is
    // very to gain by optimizing this method more,

    InstantiatedTerm term = reader.getIndex().findTerm(target);
    if (term != null) {
      this.term = term;
      nextTermIndex = term.getTermIndex() + 1;
      return true;
    } else {
      int pos = Arrays.binarySearch(reader.getIndex().getOrderedTerms(), target, InstantiatedTerm.termComparator);
      if (pos < 0) {
        pos = -1 - pos;
      }

      if (pos > reader.getIndex().getOrderedTerms().length) {
        return false;
      }
      this.term = reader.getIndex().getOrderedTerms()[pos];
      nextTermIndex = pos + 1;
      return true;
    }
  }
}



