package org.apache.lucene.search.regex;

public interface RegexQueryCapable {
  void setRegexImplementation(RegexCapabilities impl);
  RegexCapabilities getRegexImplementation();
}
