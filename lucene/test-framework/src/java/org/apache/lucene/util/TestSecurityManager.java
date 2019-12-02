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

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * A {@link SecurityManager} that prevents tests calling {@link System#exit(int)}.
 * Only the test runner itself is allowed to exit the JVM.
 * All other security checks are handled by the default security policy.
 * <p>
 * Use this with {@code -Djava.security.manager=org.apache.lucene.util.TestSecurityManager}.
 */ 
public final class TestSecurityManager extends SecurityManager {
  
  static final String JUNIT4_TEST_RUNNER_PACKAGE = "com.carrotsearch.ant.tasks.junit4.";
  static final String ECLIPSE_TEST_RUNNER_PACKAGE = "org.eclipse.jdt.internal.junit.runner.";
  static final String IDEA_TEST_RUNNER_PACKAGE = "com.intellij.rt.execution.junit.";

  /**
   * Creates a new TestSecurityManager. This ctor is called on JVM startup,
   * when {@code -Djava.security.manager=org.apache.lucene.util.TestSecurityManager}
   * is passed to JVM.
   */
  public TestSecurityManager() {
    super();
  }

  // TODO: move this stuff into a Solr (non-test) SecurityManager!
  /**
   * {@inheritDoc}
   * <p>This method implements hacks to workaround hadoop's garbage Shell and FileUtil code
   */
  @Override
  public void checkExec(String cmd) {
    // NOTE: it would be tempting to just allow anything from hadoop's Shell class, but then
    // that would just give an easy vector for RCE (use hadoop Shell instead of e.g. ProcessBuilder)
    // so we whitelist actual caller impl methods instead.
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      // hadoop insists on shelling out to get the user's supplementary groups?
      if ("org.apache.hadoop.security.ShellBasedUnixGroupsMapping".equals(element.getClassName()) &&
          "getGroups".equals(element.getMethodName())) {
        return;
      }
      // hadoop insists on shelling out to parse 'df' command instead of using FileStore?
      if ("org.apache.hadoop.fs.DF".equals(element.getClassName()) &&
          "getFilesystem".equals(element.getMethodName())) {
        return;
      }
      // hadoop insists on shelling out to parse 'du' command instead of using FileStore?
      if ("org.apache.hadoop.fs.DU".equals(element.getClassName()) &&
          "refresh".equals(element.getMethodName())) {
        return;
      }
      // hadoop insists on shelling out to parse 'ls' command instead of java nio apis?
      if ("org.apache.hadoop.util.DiskChecker".equals(element.getClassName()) &&
          "checkDir".equals(element.getMethodName())) {
        return;
      }
      // hadoop insists on shelling out to parse 'stat' command instead of Files.getAttributes?
      if ("org.apache.hadoop.fs.HardLink".equals(element.getClassName()) &&
          "getLinkCount".equals(element.getMethodName())) {
        return;
      }
      // hadoop "canExecute" method doesn't handle securityexception and fails completely.
      // so, lie to it, and tell it we will happily execute, so it does not crash.
      if ("org.apache.hadoop.fs.FileUtil".equals(element.getClassName()) &&
          "canExecute".equals(element.getMethodName())) {
        return;
      }
    }
    super.checkExec(cmd);
  }

  /**
   * {@inheritDoc}
   * <p>This method implements hacks to workaround hadoop's garbage FileUtil code
   */
  @Override
  public void checkWrite(String file) {
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      // hadoop "canWrite" method doesn't handle securityexception and fails completely.
      // so, lie to it, and tell it we will happily write, so it does not crash.
      if ("org.apache.hadoop.fs.FileUtil".equals(element.getClassName()) &&
          "canWrite".equals(element.getMethodName())) {
        return;
      }
    }
    super.checkWrite(file);
  }

  /**
   * {@inheritDoc}
   * <p>This method inspects the stack trace and checks who is calling
   * {@link System#exit(int)} and similar methods
   * @throws SecurityException if the caller of this method is not the test runner itself.
   */
  @Override
  public void checkExit(final int status) {
    AccessController.doPrivileged(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        final String systemClassName = System.class.getName(),
            runtimeClassName = Runtime.class.getName();
        String exitMethodHit = null;
        for (final StackTraceElement se : Thread.currentThread().getStackTrace()) {
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
              // this exit point is allowed, we return normally from closure:
              return /*void*/ null;
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
    });
    
    // we passed the stack check, delegate to super, so default policy can still deny permission:
    super.checkExit(status);
  }

}
