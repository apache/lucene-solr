package org.apache.lucene.search.join;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.search.MultiTermQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 *
 * @lucene.experimental
 */
class TermsQuery extends MultiTermQuery {

  private static final FilteredTermEnum EMPTY = new FilteredTermEnum() {

    public boolean next() throws IOException {
      return false;
    }

    public Term term() {
      return null;
    }

    public int docFreq() {
      throw new IllegalStateException("this method should never be called");
    }

    public void close() throws IOException {
    }

    protected boolean termCompare(Term term) {
      throw new IllegalStateException("this method should never be called");
    }

    public float difference() {
      throw new IllegalStateException("this method should never be called");
    }

    protected boolean endEnum() {
      throw new IllegalStateException("this method should never be called");
    }
  };

  private final String[] terms;
  private final String field;

  /**
   * @param field The field that should contain terms that are specified in the previous parameter
   * @param terms A set terms that matching documents should have.
   */
  TermsQuery(String field, Set<String> terms) {
    this.field = field;
    this.terms = terms.toArray(new String[terms.size()]);
    Arrays.sort(this.terms);
  }

  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    if (terms.length == 0) {
      return EMPTY;
    }

    TermEnum termEnum = reader.terms(new Term(field, terms[0]));
    Term firstTerm = termEnum.term();
    if (firstTerm == null || field != firstTerm.field()) { // interned comparison
      return EMPTY;
    }

    return new SeekingTermsEnum(termEnum, firstTerm, field, terms);
  }

  public String toString(String string) {
    return "TermsQuery{" +
        "field=" + field +
        '}';
  }

  static class SeekingTermsEnum extends FilteredTermEnum {

    private final String[] terms;
    private final String field;
    private final int lastPosition;

    private boolean endEnum;
    private int upto;

    SeekingTermsEnum(TermEnum termEnum, Term firstTerm, String field, String[] terms) throws IOException {
      this.terms = terms;
      this.field = field;
      this.lastPosition = terms.length - 1;

      upto = Arrays.binarySearch(terms, firstTerm.text());
      if (upto < 0) {
        upto = 0;
      }
      endEnum = upto == lastPosition;
      setEnum(termEnum);
    }

    protected boolean termCompare(Term term) {
      if (term == null || term.field() != field) { // interned comparison
        endEnum = true;
        return false;
      }

      int cmp = terms[upto].compareTo(term.text());
      if (cmp < 0) {
        if (upto == lastPosition) {
          return false;
        }

        while ((cmp = terms[++upto].compareTo(term.text())) < 0) {
          if (upto == lastPosition) {
            endEnum = true;
            return false;
          }
        }
        assert cmp >= 0 : "cmp cannot be lower than zero";
        if (cmp == 0) {
          upto++;
          endEnum = upto > lastPosition;
          return true;
        } else {
          return false;
        }
      } else if (cmp > 0) {
        return false;
      } else {
        upto++;
        endEnum = upto > lastPosition;
        return true;
      }
    }

    public float difference() {
      return 1.0f;
    }

    protected boolean endEnum() {
      return endEnum;
    }

  }

}
