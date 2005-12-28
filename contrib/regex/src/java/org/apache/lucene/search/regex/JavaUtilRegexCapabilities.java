package org.apache.lucene.search.regex;

import java.util.regex.Pattern;

public class JavaUtilRegexCapabilities implements RegexCapabilities {
  private Pattern pattern;

  public void compile(String pattern) {
    this.pattern = Pattern.compile(pattern);
  }

  public boolean match(String string) {
    return pattern.matcher(string).lookingAt();
  }

  public String prefix() {
    return null;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final JavaUtilRegexCapabilities that = (JavaUtilRegexCapabilities) o;

    if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) return false;

    return true;
  }

  public int hashCode() {
    return (pattern != null ? pattern.hashCode() : 0);
  }
}
