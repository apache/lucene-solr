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

import static com.carrotsearch.randomizedtesting.MethodCollector.flatten;
import static com.carrotsearch.randomizedtesting.MethodCollector.mutableCopy1;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import com.carrotsearch.randomizedtesting.TestMethodProvider;

/**
 * Backwards compatible test* method provider (public, non-static).
 */
public final class LuceneJUnit3MethodProvider implements TestMethodProvider {
  @Override
  public Collection<Method> getTestMethods(Class<?> suiteClass, List<List<Method>> methods) {
    // We will return all methods starting with test* and rely on further validation to weed
    // out static or otherwise invalid test methods.
    List<Method> copy = mutableCopy1(flatten(methods));
    Iterator<Method> i =copy.iterator();
    while (i.hasNext()) {
      Method m= i.next();
      if (!m.getName().startsWith("test") ||
          !Modifier.isPublic(m.getModifiers()) ||
           Modifier.isStatic(m.getModifiers()) ||
           m.getParameterTypes().length != 0) {
        i.remove();
      }
    }
    return copy;
  }
}
