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
package org.apache.lucene.gradle;

import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;

// Duplicates the input suite N times.
public class DuplicateSuiteTestClassProcessor implements TestClassProcessor {
  private final TestClassProcessor delegate;
  private final int dups;
  private final CustomProgressLogger logger;

  public DuplicateSuiteTestClassProcessor(CustomProgressLogger logger, int dups, TestClassProcessor delegate) {
    this.delegate = delegate;
    this.dups = dups;
    this.logger = logger;
  }

  @Override
  public void startProcessing(TestResultProcessor testResultProcessor) {
    delegate.startProcessing(testResultProcessor);
  }

  @Override
  public void processTestClass(TestClassRunInfo testClassRunInfo) {
    for (int i = 0; i < dups; i++) {
      delegate.processTestClass(testClassRunInfo);
    }
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public void stopNow() {
    delegate.stopNow();
  }
}
