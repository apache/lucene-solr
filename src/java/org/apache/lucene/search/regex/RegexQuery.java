package org.apache.lucene.search.regex;

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

public class RegexQuery extends MultiTermQuery {
  public RegexQuery(Term term) {
    super(term);
  }

  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    Term term = new Term(getTerm().field(), getTerm().text());
    return new RegexTermEnum(reader, term);
  }

  public boolean equals(Object o) {
    if (o instanceof RegexQuery)
      return super.equals(o);

    return false;
  }
}
