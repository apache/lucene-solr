package org.apache.lucene.util;

import org.junit.Assert;
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
 * Make sure {@link LuceneTestCase#setUp()} and {@link LuceneTestCase#tearDown()} were invoked even if they
 * have been overriden. We assume nobody will call these out of non-overriden
 * methods (they have to be public by contract, unfortunately). The top-level
 * methods just set a flag that is checked upon successful execution of each test
 * case.
 */
class TestRuleSetupTeardownChained implements TestRule {
  /**
   * @see TestRuleSetupTeardownChained  
   */
  public boolean setupCalled;

  /**
   * @see TestRuleSetupTeardownChained
   */
  public boolean teardownCalled;

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        setupCalled = false;
        teardownCalled = false;
        base.evaluate();

        // I assume we don't want to check teardown chaining if something happens in the
        // test because this would obscure the original exception?
        if (!setupCalled) { 
          Assert.fail("One of the overrides of setUp does not propagate the call.");
        }
        if (!teardownCalled) { 
          Assert.fail("One of the overrides of tearDown does not propagate the call.");
        }
      }
    };
  }
}