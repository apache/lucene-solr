package org.apache.lucene.util;

import com.carrotsearch.randomizedtesting.ThreadFilter;

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

/**
 * Last minute patches.
 * TODO: remove when integrated in system filters in rr.
 */
public class QuickPatchThreadsFilter implements ThreadFilter {
  static final boolean isJ9;
  
  static {
    isJ9 = System.getProperty("java.vm.info", "<?>").contains("IBM J9");
  }

  @Override
  public boolean reject(Thread t) {
    if (isJ9) {
      StackTraceElement [] stack = t.getStackTrace();
      if (stack.length > 0 && stack[stack.length - 1].getClassName().equals("java.util.Timer$TimerImpl")) {
        return true; // LUCENE-4736
      }
    }
    return false;
  }
}
