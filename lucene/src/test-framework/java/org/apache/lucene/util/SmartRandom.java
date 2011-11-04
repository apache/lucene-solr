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

import java.util.Random;

/**
 * A random that tracks if its been initialized properly,
 * and throws an exception if it hasn't.
 */
public class SmartRandom extends Random {
  boolean initialized;
  
  SmartRandom(long seed) {
    super(seed);
  }
  
  @Override
  protected int next(int bits) {
    if (!initialized) {
      System.err.println("!!! WARNING: test is using random from static initializer !!!");
      Thread.dumpStack();
      // I wish, but it causes JRE crashes
      // throw new IllegalStateException("you cannot use this random from a static initializer in your test");
    }
    return super.next(bits);
  }
}
