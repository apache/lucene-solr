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

import java.lang.StackWalker.StackFrame;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * A {@link SecurityManager} that prevents tests calling {@link System#exit(int)}.
 * Only the test runner itself is allowed to exit the JVM.
 * All other security checks are handled by the default security policy.
 * <p>
 * Use this with {@code -Djava.security.manager=org.apache.lucene.util.TestSecurityManager}.
 */ 
public final class TestSecurityManager extends SecurityManager {
  
  private static final String JUNIT4_TEST_RUNNER_PACKAGE = "com.carrotsearch.ant.tasks.junit4.";
  private static final String ECLIPSE_TEST_RUNNER_PACKAGE = "org.eclipse.jdt.internal.junit.runner.";
  private static final String IDEA_TEST_RUNNER_PACKAGE = "com.intellij.rt.execution.junit.";
  private static final String GRADLE_TEST_RUNNER_PACKAGE = "worker.org.gradle.process.internal.worker.";

  private static final String SYSTEM_CLASS_NAME = System.class.getName();
  private static final String RUNTIME_CLASS_NAME = Runtime.class.getName();
  
  /**
   * Creates a new TestSecurityManager. This ctor is called on JVM startup,
   * when {@code -Djava.security.manager=org.apache.lucene.util.TestSecurityManager}
   * is passed to JVM.
   */
  public TestSecurityManager() {
    super();
  }
  
  /**
   * {@inheritDoc}
   * <p>This method inspects the stack trace and checks who is calling
   * {@link System#exit(int)} and similar methods
   * @throws SecurityException if the caller of this method is not the test runner itself.
   */
  @Override
  public void checkExit(final int status) {
    if (StackWalker.getInstance().walk(s -> s
        .dropWhile(Predicate.not(TestSecurityManager::isExitStackFrame)) // skip all internal stack frames
        .dropWhile(TestSecurityManager::isExitStackFrame)                // skip all exit()/halt() stack frames
        .limit(1)                                                        // only look at one more frame (caller of exit)
        .map(StackFrame::getClassName)
        .noneMatch(c -> c.startsWith(JUNIT4_TEST_RUNNER_PACKAGE) || 
            c.startsWith(ECLIPSE_TEST_RUNNER_PACKAGE) ||
            c.startsWith(IDEA_TEST_RUNNER_PACKAGE) ||
            c.startsWith(GRADLE_TEST_RUNNER_PACKAGE)))) {
      throw new SecurityException(String.format(Locale.ENGLISH,
          "System/Runtime.exit(%1$d) or halt(%1$d) calls are not allowed because they terminate the test runner's JVM.",
          status));
    }
    // we passed the stack check, delegate to super, so default policy can still deny permission:
    super.checkExit(status);
  }

  private static boolean isExitStackFrame(StackFrame f) {
    final String methodName = f.getMethodName(), className = f.getClassName();
    return ("exit".equals(methodName) || "halt".equals(methodName)) &&
        (SYSTEM_CLASS_NAME.equals(className) || RUNTIME_CLASS_NAME.equals(className));
  }
  
}
