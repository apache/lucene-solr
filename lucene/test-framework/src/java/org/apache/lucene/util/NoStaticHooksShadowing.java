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

import static com.carrotsearch.randomizedtesting.MethodCollector.allDeclaredMethods;
import static com.carrotsearch.randomizedtesting.MethodCollector.annotatedWith;
import static com.carrotsearch.randomizedtesting.MethodCollector.flatten;
import static com.carrotsearch.randomizedtesting.MethodCollector.removeShadowed;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.ClassValidator;

public class NoStaticHooksShadowing implements ClassValidator {
  @Override
  public void validate(Class<?> clazz) throws Throwable {
    List<List<Method>> all = allDeclaredMethods(clazz);

    checkNoShadows(clazz, all, BeforeClass.class);
    checkNoShadows(clazz, all, AfterClass.class);
  }

  private void checkNoShadows(Class<?> clazz, List<List<Method>> all, Class<? extends Annotation> ann) {
    List<List<Method>> methodHierarchy = annotatedWith(all, ann);
    List<List<Method>> noShadows = removeShadowed(methodHierarchy);
    if (!noShadows.equals(methodHierarchy)) {
      Set<Method> shadowed = new HashSet<Method>(flatten(methodHierarchy));
      shadowed.removeAll(flatten(noShadows));

      StringBuilder b = new StringBuilder();
      for (Method m : shadowed) {
        String sig = signature(m);
        for (Method other : flatten(methodHierarchy)) {
          if (other != m && sig.equals(signature(other))) {
            b.append("Method: " + m.toString()
                + "#" + sig + " possibly shadowed by " + 
                other.toString() + "#" + signature(other) + "\n");
          }
        }
      }

      throw new RuntimeException("There are shadowed methods annotated with "
          + ann.getName() + ". These methods would not be executed by JUnit and need to manually chain themselves which can lead to" +
              " maintenance problems.\n" + b.toString().trim());
    }
  }

  private String signature(Method m) {
    return m.getName() + Arrays.toString(m.getParameterTypes());
  }
}

