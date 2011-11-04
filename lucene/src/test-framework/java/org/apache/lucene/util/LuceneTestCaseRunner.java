package org.apache.lucene.util;

/**
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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

// please don't reorganize these into a wildcard!
import static org.apache.lucene.util.LuceneTestCase.TEST_ITER;
import static org.apache.lucene.util.LuceneTestCase.TEST_ITER_MIN;
import static org.apache.lucene.util.LuceneTestCase.TEST_METHOD;
import static org.apache.lucene.util.LuceneTestCase.TEST_SEED;
import static org.apache.lucene.util.LuceneTestCase.TEST_NIGHTLY;
import static org.apache.lucene.util.LuceneTestCase.VERBOSE;

/** optionally filters the tests to be run by TEST_METHOD */
public class LuceneTestCaseRunner extends BlockJUnit4ClassRunner {
  private List<FrameworkMethod> testMethods;
  static final long runnerSeed;
  static {
    runnerSeed = "random".equals(TEST_SEED) ? LuceneTestCase.seedRand.nextLong() : ThreeLongs.fromString(TEST_SEED).l3;
  }
  
  @Override
  protected List<FrameworkMethod> computeTestMethods() {
    if (testMethods != null)
      return testMethods;
    
    Random r = new Random(runnerSeed);
    
    LuceneTestCase.testClassesRun.add(getTestClass().getJavaClass().getSimpleName());
    testMethods = new ArrayList<FrameworkMethod>();
    for (Method m : getTestClass().getJavaClass().getMethods()) {
      // check if the current test's class has methods annotated with @Ignore
      final Ignore ignored = m.getAnnotation(Ignore.class);
      if (ignored != null && !m.getName().equals("alwaysIgnoredTestMethod")) {
        System.err.println("NOTE: Ignoring test method '" + m.getName() + "': " + ignored.value());
      }
      // add methods starting with "test"
      final int mod = m.getModifiers();
      if (m.getAnnotation(Test.class) != null ||
          (m.getName().startsWith("test") &&
              !Modifier.isAbstract(mod) &&
              m.getParameterTypes().length == 0 &&
              m.getReturnType() == Void.TYPE))
      {
        if (Modifier.isStatic(mod))
          throw new RuntimeException("Test methods must not be static.");
        testMethods.add(new FrameworkMethod(m));
      }
    }
    
    if (testMethods.isEmpty()) {
      throw new RuntimeException("No runnable methods!");
    }
    
    if (TEST_NIGHTLY == false) {
      if (getTestClass().getJavaClass().isAnnotationPresent(Nightly.class)) {
        /* the test class is annotated with nightly, remove all methods */
        String className = getTestClass().getJavaClass().getSimpleName();
        System.err.println("NOTE: Ignoring nightly-only test class '" + className + "'");
        testMethods.clear();
      } else {
        /* remove all nightly-only methods */
        for (int i = 0; i < testMethods.size(); i++) {
          final FrameworkMethod m = testMethods.get(i);
          if (m.getAnnotation(Nightly.class) != null) {
            System.err.println("NOTE: Ignoring nightly-only test method '" + m.getName() + "'");
            testMethods.remove(i--);
          }
        }
      }
      /* dodge a possible "no-runnable methods" exception by adding a fake ignored test */
      if (testMethods.isEmpty()) {
        try {
          testMethods.add(new FrameworkMethod(LuceneTestCase.class.getMethod("alwaysIgnoredTestMethod")));
        } catch (Exception e) { throw new RuntimeException(e); }
      }
    }
    // sort the test methods first before shuffling them, so that the shuffle is consistent
    // across different implementations that might order the methods different originally.
    Collections.sort(testMethods, new Comparator<FrameworkMethod>() {
      public int compare(FrameworkMethod f1, FrameworkMethod f2) {
        return f1.getName().compareTo(f2.getName());
      }
    });
    Collections.shuffle(testMethods, r);
    return testMethods;
  }
  
  @Override
  protected void runChild(FrameworkMethod arg0, RunNotifier arg1) {
    if (VERBOSE) {
      System.out.println("\nNOTE: running test " + arg0.getName());
    }
    
    // only print iteration info if the user requested more than one iterations
    final boolean verbose = VERBOSE && TEST_ITER > 1;
    
    final int currentIter[] = new int[1];
    arg1.addListener(new RunListener() {
      @Override
      public void testFailure(Failure failure) throws Exception {
        if (verbose) {
          System.out.println("\nNOTE: iteration " + currentIter[0] + " failed! ");
        }
      }
    });
    for (int i = 0; i < TEST_ITER; i++) {
      currentIter[0] = i;
      if (verbose) {
        System.out.println("\nNOTE: running iter=" + (1+i) + " of " + TEST_ITER);
      }
      super.runChild(arg0, arg1);
      if (LuceneTestCase.testsFailed) {
        if (i >= TEST_ITER_MIN - 1) { // XXX is this still off-by-one?
          break;
        }
      }
    }
  }
  
  public LuceneTestCaseRunner(Class<?> clazz) throws InitializationError {
    super(clazz);
    // evil we cannot init our random here, because super() calls computeTestMethods!!!!;
    Filter f = new Filter() {
      
      @Override
      public String describe() { return "filters according to TEST_METHOD"; }
      
      @Override
      public boolean shouldRun(Description d) {
        return TEST_METHOD == null || d.getMethodName().equals(TEST_METHOD);
      }
    };
    
    try {
      f.apply(this);
    } catch (NoTestsRemainException e) {
      throw new RuntimeException(e);
    }
  }
}
