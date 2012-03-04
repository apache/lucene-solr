package org.apache.lucene.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

public class SystemPropertiesInvariantRule implements TestRule {
  @Override
  public Statement apply(final Statement s, Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        TreeMap<String,String> before = SystemPropertiesRestoreRule.cloneAsMap(System.getProperties());
        ArrayList<Throwable> errors = new ArrayList<Throwable>();
        try {
          s.evaluate();
        } catch (Throwable t) {
          errors.add(t);
        } finally {
          TreeMap<String,String> after = SystemPropertiesRestoreRule.cloneAsMap(System.getProperties());
          if (!after.equals(before)) {
            errors.add(
                new AssertionError("System properties invariant violated.\n" + 
                    collectErrorMessage(before, after)));
          }

          // Restore original properties.
          SystemPropertiesRestoreRule.restore(before, after);
        }

        MultipleFailureException.assertEmpty(errors);
      }

      private StringBuilder collectErrorMessage(
          TreeMap<String,String> before, TreeMap<String,String> after) {
        TreeSet<String> newKeys = new TreeSet<String>(after.keySet());
        newKeys.removeAll(before.keySet());
        
        TreeSet<String> missingKeys = new TreeSet<String>(before.keySet());
        missingKeys.removeAll(after.keySet());
        
        TreeSet<String> differentKeyValues = new TreeSet<String>(before.keySet());
        differentKeyValues.retainAll(after.keySet());
        for (Iterator<String> i = differentKeyValues.iterator(); i.hasNext();) {
          String key = i.next();
          String valueBefore = before.get(key);
          String valueAfter = after.get(key);
          if ((valueBefore == null && valueAfter == null) ||
              (valueBefore.equals(valueAfter))) {
            i.remove();
          }
        }

        final StringBuilder b = new StringBuilder();
        if (!missingKeys.isEmpty()) {
          b.append("Missing keys:\n");
          for (String key : missingKeys) {
            b.append("  ").append(key)
              .append("=")
              .append(before.get(key))            
              .append("\n");
          }
        }
        if (!newKeys.isEmpty()) {
          b.append("New keys:\n");
          for (String key : newKeys) {
            b.append("  ").append(key)
              .append("=")
              .append(after.get(key))
              .append("\n");
          }
        }
        if (!differentKeyValues.isEmpty()) {
          b.append("Different values:\n");
          for (String key : differentKeyValues) {
            b.append("  [old]").append(key)
              .append("=")
              .append(before.get(key)).append("\n");
            b.append("  [new]").append(key)
              .append("=")
              .append(after.get(key)).append("\n");
          }
        }
        return b;
      }
    };
  }
}