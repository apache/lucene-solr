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
package org.apache.lucene.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

/**
 * Restore a given set of system properties to a snapshot taken at the beginning
 * of the rule.
 * 
 * This is semantically similar to {@link SystemPropertiesRestoreRule} but
 * the list of properties to restore must be provided explicitly (because the security
 * manager prevents us from accessing the whole set of properties).
 * 
 * All properties to be restored must have r/w property permission.
 */
public class TestRuleRestoreSystemProperties extends TestRuleAdapter {  
  private final String[] propertyNames;
  private final Map<String, String> restore = new HashMap<String, String>();

  public TestRuleRestoreSystemProperties(String... propertyNames) {
    this.propertyNames = propertyNames;
    
    if (propertyNames.length == 0) {
      throw new IllegalArgumentException("No properties to restore? Odd.");
    }
  }

  @Override
  protected void before() throws Throwable {
    super.before();

    assert restore.isEmpty();
    for (String key : propertyNames) {
      restore.put(key, System.getProperty(key));
    }
  }
  
  @Override
  protected void afterAlways(List<Throwable> errors) throws Throwable {
    for (String key : propertyNames) {
      try {
        String value = restore.get(key);
        if (value == null) {
          System.clearProperty(key);
        } else {
          System.setProperty(key, value);
        }
      } catch (SecurityException e) {
        // We should have permission to write but if we don't, record the error
        errors.add(e);
      }
    }
    restore.clear();

    super.afterAlways(errors);
  }
}
