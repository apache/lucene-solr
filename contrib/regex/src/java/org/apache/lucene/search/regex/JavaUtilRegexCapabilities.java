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
}
