package org.apache.lucene.util;

import java.util.concurrent.atomic.AtomicReference;

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
 * A {@link TestRule} that delegates to another {@link TestRule} via a delegate
 * contained in a an {@link AtomicReference}.
 */
final class TestRuleDelegate<T extends TestRule> implements TestRule {
  private AtomicReference<T> delegate;

  private TestRuleDelegate(AtomicReference<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Statement apply(Statement s, Description d) {
    return delegate.get().apply(s, d);
  }

  static <T extends TestRule> TestRuleDelegate<T> of(AtomicReference<T> delegate) {
    return new TestRuleDelegate<>(delegate);
  }
}
