package org.apache.lucene.analysis;

import java.util.Map;

/**
 * Fake token filter factory for testing
 */
public class FakeTokenFilterFactory extends TokenFilterFactory {

  public static final String NAME = "fake";

  /** Create a FakeTokenFilterFactory */
  public FakeTokenFilterFactory(Map<String, String> args) {
    super(args);
  }

  /** Default ctor for compatibility with SPI */
  public FakeTokenFilterFactory() {
    defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return input;
  }
}
