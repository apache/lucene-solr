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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Assume;

/** Enforce test naming convention. */
public class VerifyTestClassNamingConvention extends AbstractBeforeAfterRule {
  public static final Pattern ALLOWED_CONVENTION = Pattern.compile("(.+\\.)(Test)([^.]+)");

  private static Set<String> exceptions;

  static {
    try {
      exceptions = new HashSet<>();
      try (BufferedReader is =
          new BufferedReader(
              new InputStreamReader(
                  VerifyTestClassNamingConvention.class.getResourceAsStream(
                      "test-naming-exceptions.txt"),
                  StandardCharsets.UTF_8))) {
        is.lines().forEach(exceptions::add);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void before() throws Exception {
    if (TestRuleIgnoreTestSuites.isRunningNested()) {
      // Ignore nested test suites that test the test framework itself.
      return;
    }

    String suiteName = RandomizedContext.current().getTargetClass().getName();

    // You can use this helper method to dump all suite names to a file.
    // Run gradle with one worker so that it doesn't try to append to the same
    // file from multiple processes:
    //
    // gradlew  test --max-workers 1 -Dtests.useSecurityManager=false
    //
    // dumpSuiteNamesOnly(suiteName);

    Matcher matcher = ALLOWED_CONVENTION.matcher(suiteName);
    if (!matcher.matches()) {
      // if this class exists on the exception list, leave it.
      if (!exceptions.contains("!" + suiteName)) {
        throw new AssertionError("Suite must follow Test*.java naming convention: " + suiteName);
      }
    } else if (matcher.groupCount() == 3) {
      // conventional suite name: package test class
      // alternative  suite name: package class test
      String alternativeSuiteName = matcher.group(1) + matcher.group(3) + matcher.group(2);
      // if FooBarTest.java exists on the exception list, disallow TestFooBar.java addition
      if (exceptions.contains("!" + alternativeSuiteName)) {
        throw new AssertionError(
            "Please remove the '"
                + alternativeSuiteName
                + "' test-naming-exceptions.txt before adding a new suite that follows the Test*.java naming convention: "
                + suiteName);
      }
    }
  }

  private void dumpSuiteNamesOnly(String suiteName) throws IOException {
    // Has to be a global unique path (not a temp file because temp files
    // are different for each JVM).
    Path temporaryFile = Paths.get("c:\\_tmp\\test-naming-exceptions.txt");
    try (Writer w =
        Files.newBufferedWriter(
            temporaryFile, StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
      if (!ALLOWED_CONVENTION.matcher(suiteName).matches()) {
        w.append("!");
      }
      w.append(suiteName);
      w.append("\n");
    }
    Assume.assumeFalse(true);
  }
}
