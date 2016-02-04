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
 * Require assertions for Lucene/Solr packages.
 */
public class TestRuleAssertionsRequired implements TestRule {
  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          // Make sure -ea matches -Dtests.asserts, to catch accidental mis-use:
          if (LuceneTestCase.assertsAreEnabled != LuceneTestCase.TEST_ASSERTS_ENABLED) {
            String msg = "Assertions mismatch: ";
            if (LuceneTestCase.assertsAreEnabled) {
              msg += "-ea was specified";
            } else {
              msg += "-ea was not specified";
            }
            if (LuceneTestCase.TEST_ASSERTS_ENABLED) {
              msg += " but -Dtests.asserts=true";
            } else  {
              msg += " but -Dtests.asserts=false";
            }
            System.err.println(msg);
            throw new Exception(msg);
          }
        } catch (AssertionError e) {
          // Ok, enabled.
        }

        base.evaluate();
      }
    };
  }
}
