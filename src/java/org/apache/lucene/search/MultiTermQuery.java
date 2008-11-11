package org.apache.lucene.search;

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
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.ToStringUtils;

/**
 * A {@link Query} that matches documents containing a subset of terms provided
 * by a {@link FilteredTermEnum} enumeration.
 * <P>
 * <code>MultiTermQuery</code> is not designed to be used by itself. <BR>
 * The reason being that it is not intialized with a {@link FilteredTermEnum}
 * enumeration. A {@link FilteredTermEnum} enumeration needs to be provided.
 * <P>
 * For example, {@link WildcardQuery} and {@link FuzzyQuery} extend
 * <code>MultiTermQuery</code> to provide {@link WildcardTermEnum} and
 * {@link FuzzyTermEnum}, respectively.
 * 
 * The pattern Term may be null. A query that uses a null pattern Term should
 * override equals and hashcode.
 */
public abstract class MultiTermQuery extends Query {
  protected Term term;
  protected boolean constantScoreRewrite = false;

  /** Constructs a query for terms matching <code>term</code>. */
  public MultiTermQuery(Term term) {
    this.term = term;
  }

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public MultiTermQuery() {
  }

  /** Returns the pattern term. */
  public Term getTerm() {
    return term;
  }

  /** Construct the enumeration to be used, expanding the pattern term. */
  protected abstract FilteredTermEnum getEnum(IndexReader reader)
      throws IOException;

  protected Filter getFilter() {
    return new MultiTermFilter(this);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    if (!constantScoreRewrite) {
      FilteredTermEnum enumerator = getEnum(reader);
      BooleanQuery query = new BooleanQuery(true);
      try {
        do {
          Term t = enumerator.term();
          if (t != null) {
            TermQuery tq = new TermQuery(t); // found a match
            tq.setBoost(getBoost() * enumerator.difference()); // set the boost
            query.add(tq, BooleanClause.Occur.SHOULD); // add to query
          }
        } while (enumerator.next());
      } finally {
        enumerator.close();
      }
      return query;
    } else {
      Query query = new ConstantScoreQuery(getFilter());
      query.setBoost(getBoost());
      return query;
    }
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (term != null) {
      if (!term.field().equals(field)) {
        buffer.append(term.field());
        buffer.append(":");
      }
      buffer.append(term.text());
    } else {
      buffer.append("termPattern:unknown");
    }
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  public boolean getConstantScoreRewrite() {
    return constantScoreRewrite;
  }

  public void setConstantScoreRewrite(boolean constantScoreRewrite) {
    this.constantScoreRewrite = constantScoreRewrite;
  }

  public boolean equals(Object o) {
    if (o == null || term == null) {
      throw new UnsupportedOperationException(
          "MultiTermQuerys that do not use a pattern term need to override equals/hashcode");
    }

    if (this == o)
      return true;
    if (!(o instanceof MultiTermQuery))
      return false;

    final MultiTermQuery multiTermQuery = (MultiTermQuery) o;

    if (!term.equals(multiTermQuery.term))
      return false;

    return getBoost() == multiTermQuery.getBoost();
  }

  public int hashCode() {
    if (term == null) {
      throw new UnsupportedOperationException(
          "MultiTermQuerys that do not use a pattern term need to override equals/hashcode");
    }
    return term.hashCode() + Float.floatToRawIntBits(getBoost());
  }

  static class MultiTermFilter extends Filter {
    MultiTermQuery mtq;

    abstract class TermGenerator {
      public void generate(IndexReader reader) throws IOException {
        TermEnum enumerator = mtq.getEnum(reader);
        TermDocs termDocs = reader.termDocs();
        try {
          do {
            Term term = enumerator.term();
            if (term == null)
              break;
            termDocs.seek(term);
            while (termDocs.next()) {
              handleDoc(termDocs.doc());
            }
          } while (enumerator.next());
        } finally {
          termDocs.close();
          enumerator.close();
        }
      }
      abstract public void handleDoc(int doc);
    }
    
    public MultiTermFilter(MultiTermQuery mtq) {
      this.mtq = mtq;
    }

    public BitSet bits(IndexReader reader) throws IOException {
      final BitSet bitSet = new BitSet(reader.maxDoc());
      new TermGenerator() {
        public void handleDoc(int doc) {
          bitSet.set(doc);
        }
      }.generate(reader);
      return bitSet;
    }

    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
      final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
      new TermGenerator() {
        public void handleDoc(int doc) {
          bitSet.set(doc);
        }
      }.generate(reader);

      return bitSet;
    }
      
    public boolean equals(Object o) {

      if (this == o)
        return true;
      if (!(o instanceof MultiTermFilter))
        return false;

      final MultiTermFilter filter = (MultiTermFilter) o;
      return mtq.equals(filter.mtq);
    }
      
    public int hashCode() {
      return mtq.hashCode();
    }
  }
}
