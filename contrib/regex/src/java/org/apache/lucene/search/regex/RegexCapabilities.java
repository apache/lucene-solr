package org.apache.lucene.search.regex;

public interface RegexCapabilities {
  void compile(String pattern);
  boolean match(String string);
  String prefix();
}
