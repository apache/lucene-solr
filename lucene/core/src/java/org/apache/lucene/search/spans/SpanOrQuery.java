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
package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/** Matches the union of its clauses.
 */
public final class SpanOrQuery extends SpanQuery {
  private List<SpanQuery> clauses;
  private String field;

  /** Construct a SpanOrQuery merging the provided clauses.
   * All clauses must have the same field.
   */
  public SpanOrQuery(SpanQuery... clauses) {
    this.clauses = new ArrayList<>(clauses.length);
    for (SpanQuery seq : clauses) {
      addClause(seq);
    }
  }

  /** Adds a clause to this query */
  private final void addClause(SpanQuery clause) {
    if (field == null) {
      field = clause.getField();
    } else if (clause.getField() != null && !clause.getField().equals(field)) {
      throw new IllegalArgumentException("Clauses must have same field.");
    }
    this.clauses.add(clause);
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  @Override
  public String getField() { return field; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanOrQuery rewritten = new SpanOrQuery();
    boolean actuallyRewritten = false;
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      actuallyRewritten |= query != c;
      rewritten.addClause(query);
    }
    if (actuallyRewritten) {
      return rewritten;
    }
    return super.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanOr([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           clauses.equals(((SpanOrQuery) other).clauses);
  }

  @Override
  public int hashCode() {
    return classHash() ^ clauses.hashCode();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    List<SpanWeight> subWeights = new ArrayList<>(clauses.size());
    for (SpanQuery q : clauses) {
      subWeights.add(q.createWeight(searcher, needsScores, boost));
    }
    return new SpanOrWeight(searcher,
                            needsScores ? getTermContexts(subWeights) : null,
                            subWeights,
                            needsScores,
                            boost);
  }

  public class SpanOrWeight extends SpanWeight {

    final List<SpanWeight> subWeights;

    public SpanOrWeight(IndexSearcher searcher,
                        Map<Term, TermContext> terms,
                        List<SpanWeight> subWeights,
                        boolean needsScores,
                        float boost) throws IOException
    {
      super(SpanOrQuery.this, searcher, terms, boost);
      this.subWeights = subWeights;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (final SpanWeight w: subWeights) {
        w.extractTerms(terms);
      }
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      for (SpanWeight w : subWeights) {
        w.extractTermContexts(contexts);
      }
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings)
        throws IOException {

      ArrayList<Spans> subSpans = new ArrayList<>(clauses.size());

      for (SpanWeight w : subWeights) {
        Spans spans = w.getSpans(context, requiredPostings);
        if (spans != null) {
          subSpans.add(spans);
        }
      }

      if (subSpans.size() == 0) {
        return null;
      } else if (subSpans.size() == 1) {
        return subSpans.get(0);
      }
      return new DisjunctionSpans(SpanOrQuery.this, subSpans);
    }
  }
}

