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

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 *
 * @lucene.experimental
 */
class TermsQuery extends MultiTermQuery {

  private final BytesRefHash terms;
  private final int[] ords;

  // These fields are used for equals() and hashcode() only
  private final String fromField;
  private final Query fromQuery;
  // id of the context rather than the context itself in order not to hold references to index readers
  private final Object indexReaderContextId;

  /**
   * @param toField               The field that should contain terms that are specified in the next parameter.
   * @param terms                 The terms that matching documents should have. The terms must be sorted by natural order.
   * @param indexReaderContextId  Refers to the top level index reader used to create the set of terms in the previous parameter.
   */
  TermsQuery(String toField, BytesRefHash terms, String fromField, Query fromQuery, Object indexReaderContextId) {
    super(toField);
    this.terms = terms;
    ords = terms.sort();
    this.fromField = fromField;
    this.fromQuery = fromQuery;
    this.indexReaderContextId = indexReaderContextId;
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    if (this.terms.size() == 0) {
      return TermsEnum.EMPTY;
    }

    return new SeekingTermSetTermsEnum(terms.iterator(), this.terms, ords);
  }

  @Override
  public String toString(String string) {
    return "TermsQuery{" +
        "field=" + field +
        "fromQuery=" + fromQuery.toString(field) +
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
    return Objects.equals(field, other.field) &&
        Objects.equals(fromField, other.fromField) &&
        Objects.equals(fromQuery, other.fromQuery) &&
        Objects.equals(indexReaderContextId, other.indexReaderContextId);
  }

  @Override
  public int hashCode() {
    return classHash() + Objects.hash(field, fromField, fromQuery, indexReaderContextId);
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
