package org.apache.lucene.search.regex;

import org.apache.lucene.search.FilteredTermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.util.regex.Pattern;
import java.io.IOException;

public class RegexTermEnum extends FilteredTermEnum {
  private String field = "";
  private String pre = "";
  boolean endEnum = false;
  private Pattern pattern;

  public RegexTermEnum(IndexReader reader, Term term) throws IOException {
    super();
    field = term.field();
    String text = term.text();

    pattern = Pattern.compile(text);

    // Find the first regex character position, to find the
    // maximum prefix to use for term enumeration
    int index = 0;
    while (index < text.length()) {
      char c = text.charAt(index);

      // TODO: improve the logic here.  There are other types of patterns
      // that could break this, such as "\d*" and "\*abc"
      if (c == '*' || c == '[' || c == '?' || c == '.') break;

      index++;
    }

    pre = text.substring(0, index);

    setEnum(reader.terms(new Term(term.field(), pre)));
  }

  protected final boolean termCompare(Term term) {
    if (field == term.field()) {
      String searchText = term.text();
      if (searchText.startsWith(pre)) {
        return pattern.matcher(searchText).matches();
      }
    }
    endEnum = true;
    return false;
  }

  public final float difference() {
// TODO: adjust difference based on distance of searchTerm.text() and term().text()
    return 1.0f;
  }

  public final boolean endEnum() {
    return endEnum;
  }

  public void close() throws IOException {
    super.close();
    field = null;
  }
}
