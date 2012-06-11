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

import static com.carrotsearch.randomizedtesting.MethodCollector.allDeclaredMethods;
import static com.carrotsearch.randomizedtesting.MethodCollector.annotatedWith;
import static com.carrotsearch.randomizedtesting.MethodCollector.flatten;
import static com.carrotsearch.randomizedtesting.MethodCollector.removeOverrides;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.carrotsearch.randomizedtesting.ClassValidator;

/**
 * Don't allow {@link Before} and {@link After} hook overrides as it is most
 * likely a user error and will result in superclass methods not being called
 * (requires manual chaining). 
 */
public class TestRuleNoInstanceHooksOverrides implements TestRule, ClassValidator {
  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        validate(description.getTestClass());
        base.evaluate();
      }
    };
  }

  @Override
  public void validate(Class<?> clazz) throws Throwable {
    List<List<Method>> all = allDeclaredMethods(clazz);

    checkNoShadows(clazz, all, Before.class);
    checkNoShadows(clazz, all, After.class);
  }

  private void checkNoShadows(Class<?> clazz, List<List<Method>> all, Class<? extends Annotation> ann) {
    List<List<Method>> methodHierarchy = filterIgnored(annotatedWith(all, ann));
    List<List<Method>> noOverrides = removeOverrides(methodHierarchy);
    if (!noOverrides.equals(methodHierarchy)) {
      Set<Method> shadowed = new HashSet<Method>(flatten(methodHierarchy));
      shadowed.removeAll(flatten(noOverrides));

      StringBuilder b = new StringBuilder();
      for (Method m : shadowed) {
        String sig = signature(m);
        for (Method other : flatten(methodHierarchy)) {
          if (other != m && sig.equals(signature(other))) {
            b.append("Method: " + m.toString()
                + "#" + sig + " possibly overriden by " + 
                other.toString() + "#" + signature(other) + "\n");
          }
        }
      }

      throw new RuntimeException("There are overridden methods annotated with "
          + ann.getName() + ". These methods would not be executed by JUnit and need to manually chain themselves which can lead to" +
              " maintenance problems. Consider using different method names or make hook methods private.\n" + b.toString().trim());
    }
  }

  private List<List<Method>> filterIgnored(List<List<Method>> methods) {
    Set<String> ignored = new HashSet<String>(Arrays.asList("setUp", "tearDown"));
    List<List<Method>> copy = new ArrayList<List<Method>>();
    for (List<Method> m : methods) {
      if (!ignored.contains(m.get(0).getName())) {
        copy.add(m);
      }
    }
    return copy;
  }

  private String signature(Method m) {
    return m.getName() + Arrays.toString(m.getParameterTypes());
  }
}

