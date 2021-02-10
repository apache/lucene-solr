package org.apache.solr;

import org.apache.lucene.util.LuceneTestCase;

public class SolrTestCaseUtil {
  /**
   * Checks a specific exception class is thrown by the given runnable, and returns it.
   */
  public static <T extends Throwable> T expectThrows(Class<T> expectedType, LuceneTestCase.ThrowingRunnable runnable) {
    return expectThrows(expectedType, "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown", runnable);
  }

  /**
   * Checks a specific exception class is thrown by the given runnable, and returns it.
   */
  public static <T extends Throwable> T expectThrows(Class<T> expectedType, String noExceptionMessage, LuceneTestCase.ThrowingRunnable runnable) {
    return LuceneTestCase.expectThrows(expectedType, noExceptionMessage, runnable);
  }

  public static <T> T pickRandom(T... options) {
    return options[SolrTestCase.random().nextInt(options.length)];
  }
}