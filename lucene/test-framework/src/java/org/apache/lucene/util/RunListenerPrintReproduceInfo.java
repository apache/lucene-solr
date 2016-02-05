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

import static org.apache.lucene.util.LuceneTestCase.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedContext;

/**
 * A suite listener printing a "reproduce string". This ensures test result
 * events are always captured properly even if exceptions happen at
 * initialization or suite/ hooks level.
 */
public final class RunListenerPrintReproduceInfo extends RunListener {
  /**
   * A list of all test suite classes executed so far in this JVM (ehm, 
   * under this class's classloader).
   */
  private static List<String> testClassesRun = new ArrayList<>();

  /**
   * The currently executing scope.
   */
  private LifecycleScope scope;

  /** Current test failed. */
  private boolean testFailed;

  /** Suite-level code (initialization, rule, hook) failed. */
  private boolean suiteFailed;

  /** A marker to print full env. diagnostics after the suite. */
  private boolean printDiagnosticsAfterClass;
  
  /** true if we should skip the reproduce string (diagnostics are independent) */
  private boolean suppressReproduceLine;


  @Override
  public void testRunStarted(Description description) throws Exception {
    suiteFailed = false;
    testFailed = false;
    scope = LifecycleScope.SUITE;

    Class<?> targetClass = RandomizedContext.current().getTargetClass();
    suppressReproduceLine = targetClass.isAnnotationPresent(LuceneTestCase.SuppressReproduceLine.class);
    testClassesRun.add(targetClass.getSimpleName());
  }

  @Override
  public void testStarted(Description description) throws Exception {
    this.testFailed = false;
    this.scope = LifecycleScope.TEST;
  }

  @Override
  public void testFailure(Failure failure) throws Exception {
    if (scope == LifecycleScope.TEST) {
      testFailed = true;
    } else {
      suiteFailed = true;
    }
    printDiagnosticsAfterClass = true;
  }

  @Override
  public void testFinished(Description description) throws Exception {
    if (testFailed) {
      reportAdditionalFailureInfo(
          stripTestNameAugmentations(
              description.getMethodName()));
    }
    scope = LifecycleScope.SUITE;
    testFailed = false;
  }

  /**
   * The {@link Description} object in JUnit does not expose the actual test method,
   * instead it has the concept of a unique "name" of a test. To run the same method (tests)
   * repeatedly, randomizedtesting must make those "names" unique: it appends the current iteration
   * and seeds to the test method's name. We strip this information here.   
   */
  private String stripTestNameAugmentations(String methodName) {
    if (methodName != null) {
      methodName = methodName.replaceAll("\\s*\\{.+?\\}", "");
    }
    return methodName;
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    if (printDiagnosticsAfterClass || LuceneTestCase.VERBOSE) {
      RunListenerPrintReproduceInfo.printDebuggingInformation();
    }

    if (suiteFailed) {
      reportAdditionalFailureInfo(null);
    }
  }

  /** print some useful debugging information about the environment */
  private static void printDebuggingInformation() {
    if (classEnvRule != null) {
      System.err.println("NOTE: test params are: codec=" + classEnvRule.codec +
          ", sim=" + classEnvRule.similarity +
          ", locale=" + classEnvRule.locale.toLanguageTag() +
          ", timezone=" + (classEnvRule.timeZone == null ? "(null)" : classEnvRule.timeZone.getID()));
    }
    System.err.println("NOTE: " + System.getProperty("os.name") + " "
        + System.getProperty("os.version") + " "
        + System.getProperty("os.arch") + "/"
        + System.getProperty("java.vendor") + " "
        + System.getProperty("java.version") + " "
        + (Constants.JRE_IS_64BIT ? "(64-bit)" : "(32-bit)") + "/"
        + "cpus=" + Runtime.getRuntime().availableProcessors() + ","
        + "threads=" + Thread.activeCount() + ","
        + "free=" + Runtime.getRuntime().freeMemory() + ","
        + "total=" + Runtime.getRuntime().totalMemory());
    System.err.println("NOTE: All tests run in this JVM: " + Arrays.toString(testClassesRun.toArray()));
  }

  private void reportAdditionalFailureInfo(final String testName) {
    if (suppressReproduceLine) {
      return;
    }
    if (TEST_LINE_DOCS_FILE.endsWith(JENKINS_LARGE_LINE_DOCS_FILE)) {
      System.err.println("NOTE: download the large Jenkins line-docs file by running " +
        "'ant get-jenkins-line-docs' in the lucene directory.");
    }

    final StringBuilder b = new StringBuilder();
    b.append("NOTE: reproduce with: ant test ");

    // Test case, method, seed.
    addVmOpt(b, "testcase", RandomizedContext.current().getTargetClass().getSimpleName());
    addVmOpt(b, "tests.method", testName);
    addVmOpt(b, "tests.seed", RandomizedContext.current().getRunnerSeedAsString());

    // Test groups and multipliers.
    if (RANDOM_MULTIPLIER > 1) addVmOpt(b, "tests.multiplier", RANDOM_MULTIPLIER);
    if (TEST_NIGHTLY) addVmOpt(b, SYSPROP_NIGHTLY, TEST_NIGHTLY);
    if (TEST_WEEKLY) addVmOpt(b, SYSPROP_WEEKLY, TEST_WEEKLY);
    if (TEST_SLOW) addVmOpt(b, SYSPROP_SLOW, TEST_SLOW);
    if (TEST_AWAITSFIX) addVmOpt(b, SYSPROP_AWAITSFIX, TEST_AWAITSFIX);

    // Codec, postings, directories.
    if (!TEST_CODEC.equals("random")) addVmOpt(b, "tests.codec", TEST_CODEC);
    if (!TEST_POSTINGSFORMAT.equals("random")) addVmOpt(b, "tests.postingsformat", TEST_POSTINGSFORMAT);
    if (!TEST_DOCVALUESFORMAT.equals("random")) addVmOpt(b, "tests.docvaluesformat", TEST_DOCVALUESFORMAT);
    if (!TEST_DIRECTORY.equals("random")) addVmOpt(b, "tests.directory", TEST_DIRECTORY);

    // Environment.
    if (!TEST_LINE_DOCS_FILE.equals(DEFAULT_LINE_DOCS_FILE)) addVmOpt(b, "tests.linedocsfile", TEST_LINE_DOCS_FILE);
    if (classEnvRule != null) {
      addVmOpt(b, "tests.locale", classEnvRule.locale.toLanguageTag());
      if (classEnvRule.timeZone != null) {
        addVmOpt(b, "tests.timezone", classEnvRule.timeZone.getID());
      }
    }

    if (LuceneTestCase.assertsAreEnabled) {
      addVmOpt(b, "tests.asserts", "true");
    } else {
      addVmOpt(b, "tests.asserts", "false");
    }

    addVmOpt(b, "tests.file.encoding", System.getProperty("file.encoding"));

    System.err.println(b.toString());
  }

  /**
   * Append a VM option (-Dkey=value) to a {@link StringBuilder}. Add quotes if 
   * spaces or other funky characters are detected.
   */
  static void addVmOpt(StringBuilder b, String key, Object value) {
    if (value == null) return;

    b.append(" -D").append(key).append("=");
    String v = value.toString();
    // Add simplistic quoting. This varies a lot from system to system and between
    // shells... ANT should have some code for doing it properly.
    if (Pattern.compile("[\\s=']").matcher(v).find()) {
      v = '"' + v + '"';
    }
    b.append(v);
  }
}
