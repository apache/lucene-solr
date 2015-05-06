package org.apache.lucene.spatial.spatial4j;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

/**
 * A utility logger for tests in which log statements are logged following
 * test failure only.  Add this to a JUnit based test class with a {@link org.junit.Rule}
 * annotation.
 */
public class LogRule extends TestRuleAdapter {

  //TODO does this need to be threadsafe (such as via thread-local state)?
  private static ArrayList<LogEntry> logStack = new ArrayList<LogEntry>();
  private static final int MAX_LOGS = 1000;

  public static final LogRule instance = new LogRule();

  private LogRule() {}

  @Override
  protected void before() throws Throwable {
    logStack.clear();
  }

  @Override
  protected void afterAlways(List<Throwable> errors) throws Throwable {
    if (!errors.isEmpty())
      logThenClear();
  }

  private void logThenClear() {
    for (LogEntry entry : logStack) {
      //no SLF4J in Lucene... fallback to this
      if (entry.args != null && entry.args.length > 0) {
        System.out.println(entry.msg + " " + Arrays.asList(entry.args) + "(no slf4j subst; sorry)");
      } else {
        System.out.println(entry.msg);
      }
    }
    logStack.clear();
  }

  public static void clear() {
    logStack.clear();
  }

  /**
   * Enqueues a log message with substitution arguments ala SLF4J (i.e. {} syntax).
   * If the test fails then it'll be logged then, otherwise it'll be forgotten.
   */
  public static void log(String msg, Object... args) {
    if (logStack.size() > MAX_LOGS) {
      throw new RuntimeException("Too many log statements: "+logStack.size() + " > "+MAX_LOGS);
    }
    LogEntry entry = new LogEntry();
    entry.msg = msg;
    entry.args = args;
    logStack.add(entry);
  }

  private static class LogEntry { String msg; Object[] args; }
}
