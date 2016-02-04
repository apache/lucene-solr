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
package org.apache.lucene.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Stores the suite name so you can retrieve it
 * from {@link #getTestClass()}
 */
public class TestRuleStoreClassName implements TestRule {
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
