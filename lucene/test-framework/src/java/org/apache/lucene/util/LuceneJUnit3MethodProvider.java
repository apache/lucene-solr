package org.apache.lucene.util;

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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.carrotsearch.randomizedtesting.ClassModel;
import com.carrotsearch.randomizedtesting.ClassModel.MethodModel;
import com.carrotsearch.randomizedtesting.TestMethodProvider;

/**
 * Backwards compatible test* method provider (public, non-static).
 */
public final class LuceneJUnit3MethodProvider implements TestMethodProvider {
  @Override
  public Collection<Method> getTestMethods(Class<?> suiteClass, ClassModel classModel) {
    Map<Method,MethodModel> methods = classModel.getMethods();
    ArrayList<Method> result = new ArrayList<>();
    for (MethodModel mm : methods.values()) {
      // Skip any methods that have overrieds/ shadows.
      if (mm.getDown() != null) continue;

      Method m = mm.element;
      if (m.getName().startsWith("test") &&
          Modifier.isPublic(m.getModifiers()) &&
          !Modifier.isStatic(m.getModifiers()) &&
          m.getParameterTypes().length == 0) {
        result.add(m);
      }
    }
    return result;
  }
}
