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
package org.apache.lucene.search.join;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.FieldTerms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

import java.io.IOException;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 *
 * @lucene.experimental
 */
class TermsQuery extends MultiTermQuery {

  private final BytesRefHash terms;
  private final int[] ords;
  private final Query fromQuery; // Used for equals() only

  /**
   * @param field The field that should contain terms that are specified in the previous parameter
   * @param terms The terms that matching documents should have. The terms must be sorted by natural order.
   */
  TermsQuery(String field, Query fromQuery, BytesRefHash terms) {
    super(field);
    this.fromQuery = fromQuery;
    this.terms = terms;
    ords = terms.sort();
  }

  @Override
  protected TermsEnum getTermsEnum(FieldTerms terms, AttributeSource atts) throws IOException {
    if (this.terms.size() == 0) {
      return TermsEnum.EMPTY;
    }

    return new SeekingTermSetTermsEnum(terms.iterator(), this.terms, ords);
  }

  @Override
  public String toString(String string) {
    return "TermsQuery{" +
        "field=" + field +
        '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } if (!super.equals(obj)) {
      return false;
    } if (getClass() != obj.getClass()) {
      return false;
    }

    TermsQuery other = (TermsQuery) obj;
    if (!fromQuery.equals(other.fromQuery)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result += prime * fromQuery.hashCode();
    return result;
  }

  static class SeekingTermSetTermsEnum extends FilteredTermsEnum {

    private final BytesRefHash terms;
    private final int[] ords;
    private final int lastElement;

    private final BytesRef lastTerm;
    private final BytesRef spare = new BytesRef();

    private BytesRef seekTerm;
    private int upto = 0;

    SeekingTermSetTermsEnum(TermsEnum tenum, BytesRefHash terms, int[] ords) {
      super(tenum);
      this.terms = terms;
      this.ords = ords;
      lastElement = terms.size() - 1;
      lastTerm = terms.get(ords[lastElement], new BytesRef());
      seekTerm = terms.get(ords[upto], spare);
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
      BytesRef temp = seekTerm;
      seekTerm = null;
      return temp;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      if (term.compareTo(lastTerm) > 0) {
        return AcceptStatus.END;
      }

      BytesRef currentTerm = terms.get(ords[upto], spare);
      if (term.compareTo(currentTerm) == 0) {
        if (upto == lastElement) {
          return AcceptStatus.YES;
        } else {
          seekTerm = terms.get(ords[++upto], spare);
          return AcceptStatus.YES_AND_SEEK;
        }
      } else {
        if (upto == lastElement) {
          return AcceptStatus.NO;
        } else { // Our current term doesn't match the the given term.
          int cmp;
          do { // We maybe are behind the given term by more than one step. Keep incrementing till we're the same or higher.
            if (upto == lastElement) {
              return AcceptStatus.NO;
            }
            // typically the terms dict is a superset of query's terms so it's unusual that we have to skip many of
            // our terms so we don't do a binary search here
            seekTerm = terms.get(ords[++upto], spare);
          } while ((cmp = seekTerm.compareTo(term)) < 0);
          if (cmp == 0) {
            if (upto == lastElement) {
              return AcceptStatus.YES;
            }
            seekTerm = terms.get(ords[++upto], spare);
            return AcceptStatus.YES_AND_SEEK;
          } else {
            return AcceptStatus.NO_AND_SEEK;
          }
        }
      }
    }

  }

}
