package org.apache.lucene.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This rule will cause the suite to be assumption-ignored if 
 * the test class implements a given marker interface and a special
 * property is not set.
 * 
 * <p>This is a workaround for problems with certain JUnit containers (IntelliJ)
 * which automatically discover test suites and attempt to run nested classes
 * that we use for testing the test framework itself.
 */
public final class TestRuleIgnoreTestSuites implements TestRule {
  /** 
   * Marker interface for nested suites that should be ignored
   * if executed in stand-alone mode.
   */
  public static interface NestedTestSuite {}
  
  /**
   * A boolean system property indicating nested suites should be executed
   * normally.
   */
  public final static String PROPERTY_RUN_NESTED = "tests.runnested"; 
  
  @Override
  public Statement apply(final Statement s, final Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (NestedTestSuite.class.isAssignableFrom(d.getTestClass())) {
          LuceneTestCase.assumeTrue("Nested suite class ignored (started as stand-along).",
              isRunningNested());
        }
        s.evaluate();
      }
    };
  }

  /**
   * Check if a suite class is running as a nested test.
   */
  public static boolean isRunningNested() {
    return Boolean.getBoolean(PROPERTY_RUN_NESTED);
  }
}
