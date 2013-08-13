package org.apache.lucene.queries;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A filter that includes documents that match with a specific term.
 */
final public class TermFilter extends Filter {

  private final Term term;

  /**
   * @param term The term documents need to have in order to be a match for this filter.
   */
  public TermFilter(Term term) {
    if (term == null) {
      throw new IllegalArgumentException("Term must not be null");
    } else if (term.field() == null) {
      throw new IllegalArgumentException("Field must not be null");
    }
    this.term = term;
  }

  /**
   * @return The term this filter includes documents with.
   */
  public Term getTerm() {
    return term;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    Terms terms = context.reader().terms(term.field());
    if (terms == null) {
      return null;
    }

    final TermsEnum termsEnum = terms.iterator(null);
    if (!termsEnum.seekExact(term.bytes())) {
      return null;
    }
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return termsEnum.docs(acceptDocs, null, DocsEnum.FLAG_NONE);
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TermFilter that = (TermFilter) o;

    if (term != null ? !term.equals(that.term) : that.term != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return term != null ? term.hashCode() : 0;
  }

  @Override
  public String toString() {
    return term.field() + ":" + term.text();
  }

}
