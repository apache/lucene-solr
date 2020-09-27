package org.apache.lucene.analysis;

import java.io.Reader;
import java.util.Map;

/**
 * Fake char filter factory for testing
 */
public class FakeCharFilterFactory extends CharFilterFactory {

  public static final String NAME = "fake";

  /** Create a FakeCharFilterFactory */
  public FakeCharFilterFactory(Map<String, String> args) {
    super(args);
  }

  /** Default ctor for compatibility with SPI */
  public FakeCharFilterFactory() {
    defaultCtorException();
  }

  @Override
  public Reader create(Reader input) {
    return input;
  }
}
