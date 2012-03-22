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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

public class SystemPropertiesInvariantRule implements TestRule {
  /**
   * Ignored property keys.
   */
  private final HashSet<String> ignoredProperties;

  /**
   * Cares about all properties. 
   */
  public SystemPropertiesInvariantRule() {
    this(Collections.<String>emptySet());
  }

  /**
   * Don't care about the given set of properties. 
   */
  public SystemPropertiesInvariantRule(String... ignoredProperties) {
    this.ignoredProperties = new HashSet<String>(Arrays.asList(ignoredProperties));
  }

  /**
   * Don't care about the given set of properties. 
   */
  public SystemPropertiesInvariantRule(Set<String> ignoredProperties) {
    this.ignoredProperties = new HashSet<String>(ignoredProperties);
  }

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
          final TreeMap<String,String> after = SystemPropertiesRestoreRule.cloneAsMap(System.getProperties());

          // Remove ignored if they exist.
          before.keySet().removeAll(ignoredProperties);
          after.keySet().removeAll(ignoredProperties);

          if (!after.equals(before)) {
            errors.add(
                new AssertionError("System properties invariant violated.\n" + 
                    collectErrorMessage(before, after)));
          }

          // Restore original properties.
          SystemPropertiesRestoreRule.restore(before, after, ignoredProperties);
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