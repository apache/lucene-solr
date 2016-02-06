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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * A {@link RunListener} that detects suite/ test failures. We need it because failures
 * due to thread leaks happen outside of any rule contexts.
 */
public class FailureMarker extends RunListener {
  static final AtomicInteger failures = new AtomicInteger();

  @Override
  public void testFailure(Failure failure) throws Exception {
    failures.incrementAndGet();
  }

  public static boolean hadFailures() {
    return failures.get() > 0;
  }

  static int getFailures() {
    return failures.get();
  }

  public static void resetFailures() {
    failures.set(0);
  }
}
