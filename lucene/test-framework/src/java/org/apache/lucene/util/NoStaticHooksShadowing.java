package org.apache.lucene.util;

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

