package org.apache.lucene.util;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

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

final class TestRuleIcuHack implements TestRule {
  /** Globally only check hack once. */
  private static volatile AtomicBoolean icuTested = new AtomicBoolean(false);

  @Override
  public Statement apply(final Statement s, Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        // START hack to init ICU safely before we randomize locales.
        // ICU fails during classloading when a special Java7-only locale is the default
        // see: http://bugs.icu-project.org/trac/ticket/8734
        if (!icuTested.getAndSet(true)) {
          Locale previous = Locale.getDefault();
          try {
            Locale.setDefault(Locale.US);
            Class.forName("com.ibm.icu.util.ULocale");
          } catch (ClassNotFoundException cnfe) {
            // ignore if no ICU is in classpath
          } finally {
            Locale.setDefault(previous);
          }
        }

        s.evaluate();
      }
    };
  }
}
