package org.apache.lucene.util.junitcompat;

/**
 * A pointcut-like definition where we should trigger
 * an assumption or error.
 */
public enum SorePoint {
  // STATIC_INITIALIZER, // I assume this will result in JUnit failure to load a suite.
  BEFORE_CLASS,
  INITIALIZER,
  RULE,
  BEFORE,
  TEST,
  AFTER,
  AFTER_CLASS
}