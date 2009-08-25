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

import java.util.Locale;
import java.util.Set;

/**
 * Base test class for Lucene test classes that test Locale-sensitive behavior.
 * <p>
 * This class will run tests under the default Locale, but then will also run
 * tests under all available JVM locales. This is helpful to ensure tests will
 * not fail under a different environment.
 * </p>
 */
public abstract class LocalizedTestCase extends LuceneTestCase {
  /**
   * Before changing the default Locale, save the default Locale here so that it
   * can be restored.
   */
  private final Locale defaultLocale = Locale.getDefault();

  /**
   * The locale being used as the system default Locale
   */
  private Locale locale;

  /**
   * An optional limited set of testcases that will run under different Locales.
   */
  private final Set testWithDifferentLocales;

  public LocalizedTestCase() {
    super();
    testWithDifferentLocales = null;
  }

  public LocalizedTestCase(String name) {
    super(name);
    testWithDifferentLocales = null;
  }

  public LocalizedTestCase(Set testWithDifferentLocales) {
    super();
    this.testWithDifferentLocales = testWithDifferentLocales;
  }

  public LocalizedTestCase(String name, Set testWithDifferentLocales) {
    super(name);
    this.testWithDifferentLocales = testWithDifferentLocales;
  }

  // @Override
  protected void setUp() throws Exception {
    super.setUp();
    Locale.setDefault(locale);
  }

  // @Override
  protected void tearDown() throws Exception {
    Locale.setDefault(defaultLocale);
    super.tearDown();
  }
  
  // @Override
  public void runBare() throws Throwable {
    // Do the test with the default Locale (default)
    try {
      locale = defaultLocale;
      super.runBare();
    } catch (Throwable e) {
      System.out.println("Test failure of '" + getName()
          + "' occurred with the default Locale " + locale);
      throw e;
    }

    if (testWithDifferentLocales == null
        || testWithDifferentLocales.contains(getName())) {
      // Do the test again under different Locales
      Locale systemLocales[] = Locale.getAvailableLocales();
      for (int i = 0; i < systemLocales.length; i++) {
        try {
          locale = systemLocales[i];
          super.runBare();
        } catch (Throwable e) {
          System.out.println("Test failure of '" + getName()
              + "' occurred under a different Locale " + locale);
          throw e;
        }
      }
    }
  }
}
