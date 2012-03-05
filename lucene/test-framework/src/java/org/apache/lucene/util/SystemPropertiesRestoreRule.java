package org.apache.lucene.util;

import java.util.Map;
import java.util.*;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Restore system properties from before the nested {@link Statement}.
 */
public class SystemPropertiesRestoreRule implements TestRule {
  @Override
  public Statement apply(final Statement s, Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        TreeMap<String,String> before = cloneAsMap(System.getProperties());
        try {
          s.evaluate();
        } finally {
          TreeMap<String,String> after = cloneAsMap(System.getProperties());
          if (!after.equals(before)) {
            // Restore original properties.
            restore(before, after);
          }
        }
      }
    };
  }
  
  static TreeMap<String,String> cloneAsMap(Properties properties) {
    TreeMap<String,String> result = new TreeMap<String,String>();
    for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements();) {
      String key = (String) e.nextElement();
      result.put(key, (String) properties.get(key));
    }
    return result;
  }

  static void restore(
      TreeMap<String,String> before,
      TreeMap<String,String> after) {
    after.keySet().removeAll(before.keySet());
    for (String key : after.keySet()) {
      System.clearProperty(key);
    }
    for (Map.Entry<String,String> e : before.entrySet()) {
      if (e.getValue() == null) {
        System.clearProperty(e.getKey()); // Can this happen?
      } else {
        System.setProperty(e.getKey(), e.getValue());
      }
    }
  }  
}