package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ToStringUtils;

/**
 * A {@link Query} that matches documents containing a subset of terms provided
 * by a {@link FilteredTermEnum} enumeration.
 * <P>
 * <code>MultiTermQuery</code> is not designed to be used by itself.
 * <BR>
 * The reason being that it is not intialized with a {@link FilteredTermEnum}
 * enumeration. A {@link FilteredTermEnum} enumeration needs to be provided.
 * <P>
 * For example, {@link WildcardQuery} and {@link FuzzyQuery} extend
 * <code>MultiTermQuery</code> to provide {@link WildcardTermEnum} and
 * {@link FuzzyTermEnum}, respectively.
 */
public abstract class MultiTermQuery extends Query {
    private Term term;

    /** Constructs a query for terms matching <code>term</code>. */
    public MultiTermQuery(Term term) {
        this.term = term;
    }

    /** Returns the pattern term. */
    public Term getTerm() { return term; }

    /** Construct the enumeration to be used, expanding the pattern term. */
    protected abstract FilteredTermEnum getEnum(IndexReader reader)
      throws IOException;

    public Query rewrite(IndexReader reader) throws IOException {
      FilteredTermEnum enumerator = getEnum(reader);
      BooleanQuery query = new BooleanQuery(true);
      try {
        do {
          Term t = enumerator.term();
          if (t != null) {
            TermQuery tq = new TermQuery(t);      // found a match
            tq.setBoost(getBoost() * enumerator.difference()); // set the boost
            query.add(tq, BooleanClause.Occur.SHOULD);          // add to query
          }
        } while (enumerator.next());
      } finally {
        enumerator.close();
      }
      return query;
    }

    /** Prints a user-readable version of this query. */
    public String toString(String field) {
        StringBuffer buffer = new StringBuffer();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MultiTermQuery)) return false;

      final MultiTermQuery multiTermQuery = (MultiTermQuery) o;

      if (!term.equals(multiTermQuery.term)) return false;

      return getBoost() == multiTermQuery.getBoost();
    }

    public int hashCode() {
      return term.hashCode() + Float.floatToRawIntBits(getBoost());
    }
}
