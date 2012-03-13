package org.apache.lucene.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class StoreClassNameRule implements TestRule {
  private volatile Description description;

  @Override
  public Statement apply(final Statement s, final Description d) {
    if (!d.isSuite()) {
      throw new IllegalArgumentException("This is a @ClassRule (applies to suites only).");
    }

    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          description = d; 
          s.evaluate();
        } finally {
          description = null;
        }
      }
    };
  }
  
  /**
   * Returns the test class currently executing in this rule.
   */
  public Class<?> getTestClass() {
    Description localDescription = description;
    if (localDescription == null) {
      throw new RuntimeException("The rule is not currently executing.");
    }
    return localDescription.getTestClass();
  }
}
