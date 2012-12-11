package org.apache.lucene.util;

import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
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
 * A {@link TestRule} that guarantees the execution of {@link #after} even
 * if an exception has been thrown from delegate {@link Statement}. This is much
 * like {@link AfterClass} or {@link After} annotations but can be used with
 * {@link RuleChain} to guarantee the order of execution.
 */
abstract class AbstractBeforeAfterRule implements TestRule {
  @Override
  public Statement apply(final Statement s, final Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        final ArrayList<Throwable> errors = new ArrayList<Throwable>();

        try {
          before();
          s.evaluate();
        } catch (Throwable t) {
          errors.add(t);
        }
        
        try {
          after();
        } catch (Throwable t) {
          errors.add(t);
        }

        MultipleFailureException.assertEmpty(errors);
      }
    };
  }

  protected void before() throws Exception {}
  protected void after() throws Exception {}
}
