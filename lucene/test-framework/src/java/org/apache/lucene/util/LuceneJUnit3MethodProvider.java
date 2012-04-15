package org.apache.lucene.util;

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
