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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Enforce test naming convention. */
public class VerifyTestClassNamingConvention extends AbstractBeforeAfterRule {
  private final String packagePrefix;
  private final Pattern namingConvention;

  public VerifyTestClassNamingConvention(String packagePrefix, Pattern namingConvention) {
    this.packagePrefix = packagePrefix;
    this.namingConvention = namingConvention;
  }

  @Override
  protected void before() throws Exception {
    if (TestRuleIgnoreTestSuites.isRunningNested()) {
      // Ignore nested test suites that test the test framework itself.
      return;
    }

    String suiteName = RandomizedContext.current().getTargetClass().getName();

    Matcher matcher = namingConvention.matcher(suiteName);
    if (suiteName.startsWith(packagePrefix) && !matcher.matches()) {
      throw new AssertionError(
          packagePrefix
              + " suite must follow "
              + namingConvention
              + " naming convention: "
              + suiteName);
    }
  }
}
