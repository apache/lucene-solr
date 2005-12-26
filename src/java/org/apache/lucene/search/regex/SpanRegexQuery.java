package org.apache.lucene.search.regex;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;

public class SpanRegexQuery extends SpanQuery {
  private Term term;

  public SpanRegexQuery(Term term) {
    this.term = term;
  }

  public Term getTerm() { return term; }

  public Query rewrite(IndexReader reader) throws IOException {
    Query orig = new RegexQuery(term).rewrite(reader);

    // RegexQuery (via MultiTermQuery).rewrite always returns a BooleanQuery
    BooleanQuery bq = (BooleanQuery) orig;

    BooleanClause[] clauses = bq.getClauses();
    SpanQuery[] sqs = new SpanQuery[clauses.length];
    for (int i = 0; i < clauses.length; i++) {
      BooleanClause clause = clauses[i];

      // Clauses from RegexQuery.rewrite are always TermQuery's
      TermQuery tq = (TermQuery) clause.getQuery();

      sqs[i] = new SpanTermQuery(tq.getTerm());
      sqs[i].setBoost(tq.getBoost());
    }

    SpanOrQuery query = new SpanOrQuery(sqs);
    query.setBoost(orig.getBoost());

    return query;
  }

  public Spans getSpans(IndexReader reader) throws IOException {
    throw new UnsupportedOperationException("Query should have been rewritten");
  }

  public String getField() {
    return term.field();
  }

  public Collection getTerms() {
    Collection terms = new ArrayList();
    terms.add(term);
    return terms;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TermQuery)) return false;
    final SpanRegexQuery that = (SpanRegexQuery) o;
    return term.equals(that.term) && getBoost() == that.getBoost();
  }

  public int hashCode() {
    return term.hashCode() ^ Float.floatToRawIntBits(getBoost()) ^ 0x4BCEF3A9;
  }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("spanRegexQuery(");
    buffer.append(term);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
}
