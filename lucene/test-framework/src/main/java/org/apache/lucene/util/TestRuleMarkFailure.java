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

import java.util.ArrayList;
import java.util.List;

import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A rule for marking failed tests and suites.
 */
public final class TestRuleMarkFailure implements TestRule {
  private final TestRuleMarkFailure [] chained;
  private volatile boolean failures;

  public TestRuleMarkFailure(TestRuleMarkFailure... chained) {
    this.chained = chained;
  }
  
  @Override
  public Statement apply(final Statement s, Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        // Clear status at start.
        failures = false;

        try {
          s.evaluate();
        } catch (Throwable t) {
          if (!isAssumption(t)) {
            markFailed();
          }
          throw t;
        }
      }
    };
  }

  /**
   * Is a given exception (or a MultipleFailureException) an 
   * {@link AssumptionViolatedException}?
   */
  public static boolean isAssumption(Throwable t) {
    for (Throwable t2 : expandFromMultiple(t)) {
      if (!(t2 instanceof AssumptionViolatedException)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Expand from multi-exception wrappers.
   */
  private static List<Throwable> expandFromMultiple(Throwable t) {
    return expandFromMultiple(t, new ArrayList<Throwable>());
  }

  /** Internal recursive routine. */
  private static List<Throwable> expandFromMultiple(Throwable t, List<Throwable> list) {
    if (t instanceof org.junit.runners.model.MultipleFailureException) {
      for (Throwable sub : ((org.junit.runners.model.MultipleFailureException) t).getFailures()) {
        expandFromMultiple(sub, list);
      }
    } else {
      list.add(t);
    }

    return list;
  }

  /**
   * Taints this object and any chained as having failures.
   */
  public void markFailed() {
    failures = true;
    for (TestRuleMarkFailure next : chained) {
      next.markFailed();
    }
  }

  /**
   * Check if this object had any marked failures.
   */
  public boolean hadFailures() {
    return failures;
  }

  /**
   * Check if this object was successful (the opposite of {@link #hadFailures()}). 
   */
  public boolean wasSuccessful() {
    return !hadFailures();
  }
}
