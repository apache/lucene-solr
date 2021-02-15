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
package org.apache.lucene.analysis.hunspell;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Same as {@link TestSpellChecking}, but checks all Hunspell's test data. The path to the checked
 * out Hunspell repository should be in {@code hunspell.repo.path} system property.
 */
@RunWith(Parameterized.class)
public class TestHunspellRepositoryTestCases {
  private static final Set<String> EXPECTED_FAILURES =
      Set.of(
          "hu", // Hungarian is hard: a lot of its rules are hardcoded in Hunspell code, not aff/dic
          "morph", // we don't do morphological analysis yet
          "opentaal_keepcase", // Hunspell bug: https://github.com/hunspell/hunspell/issues/712
          "forbiddenword", // needs https://github.com/hunspell/hunspell/pull/713 PR to be merged
          "nepali", // not supported yet
          "utf8_nonbmp", // code points not supported yet
          "phone" // not supported yet, used only for suggestions in en_ZA
          );
  private final String testName;
  private final Path pathPrefix;

  public TestHunspellRepositoryTestCases(String testName, Path pathPrefix) {
    this.testName = testName;
    this.pathPrefix = pathPrefix;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    String hunspellRepo = System.getProperty("hunspell.repo.path");
    if (hunspellRepo == null) {
      throw new AssumptionViolatedException("hunspell.repo.path property not specified.");
    }

    Set<String> names = new TreeSet<>();
    Path tests = Path.of(hunspellRepo).resolve("tests");
    try (DirectoryStream<Path> files = Files.newDirectoryStream(tests)) {
      for (Path file : files) {
        String name = file.getFileName().toString();
        if (name.endsWith(".aff")) {
          names.add(name.substring(0, name.length() - 4));
        }
      }
    }

    return names.stream().map(s -> new Object[] {s, tests.resolve(s)}).collect(Collectors.toList());
  }

  @Test
  public void test() throws Throwable {
    ThrowingRunnable test = () -> TestSpellChecking.checkSpellCheckerExpectations(pathPrefix);
    if (EXPECTED_FAILURES.contains(testName)) {
      Assert.assertThrows(Throwable.class, test);
    } else {
      test.run();
    }
  }
}
