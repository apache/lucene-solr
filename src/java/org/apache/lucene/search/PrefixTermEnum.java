package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified prefix filter term.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 *
 */
public class PrefixTermEnum extends FilteredTermEnum {

  private Term prefix;
  private boolean endEnum = false;

  public PrefixTermEnum(IndexReader reader, Term prefix) throws IOException {
    this.prefix = prefix;

    setEnum(reader.terms(new Term(prefix.field(), prefix.text())));
  }

  public float difference() {
    return 1.0f;
  }

  protected boolean endEnum() {
    return endEnum;
  }

  protected boolean termCompare(Term term) {
    if (term.field() == prefix.field() && term.text().startsWith(prefix.text())) {                                                                              
      return true;
    }
    endEnum = true;
    return false;
  }
}
