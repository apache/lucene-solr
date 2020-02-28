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

package org.apache.solr;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.ExternalPaths;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.junit.Assert.fail;

public class SourceHomeNullifier extends TestWatcher {

  private String oldSourceHome;
  private Field sourceHomeField;

  @Override
  protected void starting(Description description) {
    // see https://stackoverflow.com/a/2591122
    // this test is only meant to run under java < 9.0
    if (System.getProperty("java.version").startsWith("1.")) {
      boolean nullify = RandomizedContext.current().getRandom().nextBoolean();
      if (nullify) {
        oldSourceHome = ExternalPaths.SOURCE_HOME;
        try {
          sourceHomeField = ExternalPaths.class.getDeclaredField("SOURCE_HOME");
          setFinalStatic(sourceHomeField, null);
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }

  @SuppressForbidden(reason = "Need to tweak the value of a static final field on a per test basis")
  static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    field.set(null, newValue);
  }

  @Override
  protected void finished(Description description) {
    // see https://stackoverflow.com/a/2591122
    // this test is only meant to run under java < 9.0
    if (System.getProperty("java.version").startsWith("1.")) {
      if (sourceHomeField != null) {
        try {
          sourceHomeField.set(null, oldSourceHome);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    }
  }
}
