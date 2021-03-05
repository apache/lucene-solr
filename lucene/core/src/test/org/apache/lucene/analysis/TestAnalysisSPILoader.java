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
package org.apache.lucene.analysis;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

public class TestAnalysisSPILoader extends LuceneTestCase {

  private Map<String, String> versionArgOnly() {
    return new HashMap<String, String>() {
      {
        put("luceneMatchVersion", Version.LATEST.toString());
      }
    };
  }

  public void testLookupTokenizer() {
    assertSame(
        StandardTokenizerFactory.class,
        TokenizerFactory.forName("Standard", versionArgOnly()).getClass());
    assertSame(
        StandardTokenizerFactory.class,
        TokenizerFactory.forName("STANDARD", versionArgOnly()).getClass());
    assertSame(
        StandardTokenizerFactory.class,
        TokenizerFactory.forName("standard", versionArgOnly()).getClass());
  }

  public void testBogusLookupTokenizer() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenizerFactory.forName("sdfsdfsdfdsfsdfsdf", new HashMap<String, String>());
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenizerFactory.forName("!(**#$U*#$*", new HashMap<String, String>());
        });
  }

  public void testLookupTokenizerClass() {
    assertSame(StandardTokenizerFactory.class, TokenizerFactory.lookupClass("Standard"));
    assertSame(StandardTokenizerFactory.class, TokenizerFactory.lookupClass("STANDARD"));
    assertSame(StandardTokenizerFactory.class, TokenizerFactory.lookupClass("standard"));
  }

  public void testBogusLookupTokenizerClass() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenizerFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenizerFactory.lookupClass("!(**#$U*#$*");
        });
  }

  public void testAvailableTokenizers() {
    assertTrue(TokenizerFactory.availableTokenizers().contains("standard"));
  }

  public void testLookupTokenFilter() {
    assertSame(
        FakeTokenFilterFactory.class,
        TokenFilterFactory.forName("Fake", versionArgOnly()).getClass());
    assertSame(
        FakeTokenFilterFactory.class,
        TokenFilterFactory.forName("FAKE", versionArgOnly()).getClass());
    assertSame(
        FakeTokenFilterFactory.class,
        TokenFilterFactory.forName("fake", versionArgOnly()).getClass());
  }

  public void testBogusLookupTokenFilter() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenFilterFactory.forName("sdfsdfsdfdsfsdfsdf", new HashMap<String, String>());
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenFilterFactory.forName("!(**#$U*#$*", new HashMap<String, String>());
        });
  }

  public void testLookupTokenFilterClass() {
    assertSame(FakeTokenFilterFactory.class, TokenFilterFactory.lookupClass("Fake"));
    assertSame(FakeTokenFilterFactory.class, TokenFilterFactory.lookupClass("FAKE"));
    assertSame(FakeTokenFilterFactory.class, TokenFilterFactory.lookupClass("fake"));
  }

  public void testBogusLookupTokenFilterClass() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenFilterFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          TokenFilterFactory.lookupClass("!(**#$U*#$*");
        });
  }

  public void testAvailableTokenFilters() {
    assertTrue(TokenFilterFactory.availableTokenFilters().contains("fake"));
  }

  public void testLookupCharFilter() {
    assertSame(
        FakeCharFilterFactory.class,
        CharFilterFactory.forName("Fake", versionArgOnly()).getClass());
    assertSame(
        FakeCharFilterFactory.class,
        CharFilterFactory.forName("FAKE", versionArgOnly()).getClass());
    assertSame(
        FakeCharFilterFactory.class,
        CharFilterFactory.forName("fake", versionArgOnly()).getClass());
  }

  public void testBogusLookupCharFilter() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CharFilterFactory.forName("sdfsdfsdfdsfsdfsdf", new HashMap<String, String>());
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CharFilterFactory.forName("!(**#$U*#$*", new HashMap<String, String>());
        });
  }

  public void testLookupCharFilterClass() {
    assertSame(FakeCharFilterFactory.class, CharFilterFactory.lookupClass("Fake"));
    assertSame(FakeCharFilterFactory.class, CharFilterFactory.lookupClass("FAKE"));
    assertSame(FakeCharFilterFactory.class, CharFilterFactory.lookupClass("fake"));
  }

  public void testBogusLookupCharFilterClass() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CharFilterFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CharFilterFactory.lookupClass("!(**#$U*#$*");
        });
  }

  public void testAvailableCharFilters() {
    assertTrue(CharFilterFactory.availableCharFilters().contains("fake"));
  }
}
