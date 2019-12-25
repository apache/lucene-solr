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
    final String systemClassName = System.class.getName(),
        runtimeClassName = Runtime.class.getName();
    String exitMethodHit = null;
    for (final StackTraceElement se : (new Exception()).getStackTrace()) {
      final String className = se.getClassName(), methodName = se.getMethodName();
      if (
        ("exit".equals(methodName) || "halt".equals(methodName)) &&
        (systemClassName.equals(className) || runtimeClassName.equals(className))
      ) {
        exitMethodHit = className + '#' + methodName + '(' + status + ')';
        continue;
      }
      
      if (exitMethodHit != null) {
        if (className.startsWith(JUNIT4_TEST_RUNNER_PACKAGE) || 
            className.startsWith(ECLIPSE_TEST_RUNNER_PACKAGE) ||
            className.startsWith(IDEA_TEST_RUNNER_PACKAGE)) {
          // we passed the stack check, delegate to super, so default policy can still deny permission:
          super.checkExit(status);
          return;
        } else {
          // anything else in stack trace is not allowed, break and throw SecurityException below:
          break;
        }
      }
    }
    
    if (exitMethodHit == null) {
      // should never happen, only if JVM hides stack trace - replace by generic:
      exitMethodHit = "JVM exit method";
    }
    throw new SecurityException(exitMethodHit + " calls are not allowed because they terminate the test runner's JVM.");
  }

}
