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


import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.ParseException;
import java.util.Locale;
import java.util.Random;

public class TestVersion extends LuceneTestCase {

  public void testOnOrAfter() throws Exception {
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        Version v = (Version)field.get(Version.class);
        assertTrue("LATEST must be always onOrAfter("+v+")", Version.LATEST.onOrAfter(v));
      }
    }
    assertTrue(Version.LUCENE_6_0_0.onOrAfter(Version.LUCENE_5_0_0));;
  }

  public void testToString() {
    assertEquals("5.0.0", Version.LUCENE_5_0_0.toString());
    assertEquals("6.0.0", Version.LUCENE_6_0_0.toString());
  }

  public void testParseLeniently() throws Exception {
    assertEquals(Version.LUCENE_5_0_0, Version.parseLeniently("5.0"));
    assertEquals(Version.LUCENE_5_0_0, Version.parseLeniently("5.0.0"));
    assertEquals(Version.LUCENE_5_0_0, Version.parseLeniently("LUCENE_50"));
    assertEquals(Version.LUCENE_5_0_0, Version.parseLeniently("LUCENE_5_0"));
    assertEquals(Version.LUCENE_5_0_0, Version.parseLeniently("LUCENE_5_0_0"));
    assertEquals(Version.LUCENE_6_0_0, Version.parseLeniently("6.0"));
    assertEquals(Version.LUCENE_6_0_0, Version.parseLeniently("6.0.0"));
    assertEquals(Version.LUCENE_6_0_0, Version.parseLeniently("LUCENE_60"));
    assertEquals(Version.LUCENE_6_0_0, Version.parseLeniently("LUCENE_6_0"));
    assertEquals(Version.LUCENE_6_0_0, Version.parseLeniently("LUCENE_6_0_0"));
    assertEquals(Version.LATEST, Version.parseLeniently("LATEST"));
    assertEquals(Version.LATEST, Version.parseLeniently("latest"));
    assertEquals(Version.LATEST, Version.parseLeniently("LUCENE_CURRENT"));
    assertEquals(Version.LATEST, Version.parseLeniently("lucene_current"));
  }
  
  public void testParseLenientlyExceptions() {
    ParseException expected = expectThrows(ParseException.class, () -> {
      Version.parseLeniently("LUCENE");
    });
    assertTrue(expected.getMessage().contains("LUCENE"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parseLeniently("LUCENE_610");
    });
    assertTrue(expected.getMessage().contains("LUCENE_610"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parseLeniently("LUCENE61");
    });
    assertTrue(expected.getMessage().contains("LUCENE61"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parseLeniently("LUCENE_6.0.0");
    });
    assertTrue(expected.getMessage().contains("LUCENE_6.0.0"));
  }

  public void testParseLenientlyOnAllConstants() throws Exception {
    boolean atLeastOne = false;
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        atLeastOne = true;
        Version v = (Version)field.get(Version.class);
        assertEquals(v, Version.parseLeniently(v.toString()));
        assertEquals(v, Version.parseLeniently(field.getName()));
        assertEquals(v, Version.parseLeniently(field.getName().toLowerCase(Locale.ROOT)));
      }
    }
    assertTrue(atLeastOne);
  }

  public void testParse() throws Exception {
    assertEquals(Version.LUCENE_6_0_0, Version.parse("6.0.0"));
    assertEquals(Version.LUCENE_5_0_0, Version.parse("5.0.0"));
    
    // Version does not pass judgement on the major version:
    assertEquals(1, Version.parse("1.0").major);
    assertEquals(7, Version.parse("7.0.0").major);
  }

  public void testForwardsCompatibility() throws Exception {
    assertTrue(Version.parse("5.10.20").onOrAfter(Version.LUCENE_5_0_0));
  }

  public void testParseExceptions() {
    ParseException expected = expectThrows(ParseException.class, () -> {
      Version.parse("LUCENE_6_0_0");
    });
    assertTrue(expected.getMessage().contains("LUCENE_6_0_0"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.256");
    });
    assertTrue(expected.getMessage().contains("6.256"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.-1");
    });
    assertTrue(expected.getMessage().contains("6.-1"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.1.256");
    });
    assertTrue(expected.getMessage().contains("6.1.256"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.1.-1");
    });
    assertTrue(expected.getMessage().contains("6.1.-1"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.1.1.3");
    });
    assertTrue(expected.getMessage().contains("6.1.1.3"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.1.1.-1");
    });
    assertTrue(expected.getMessage().contains("6.1.1.-1"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.1.1.1");
    });
    assertTrue(expected.getMessage().contains("6.1.1.1"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.1.1.2");
    });
    assertTrue(expected.getMessage().contains("6.1.1.2"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.0.0.0");
    });
    assertTrue(expected.getMessage().contains("6.0.0.0"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6.0.0.1.42");
    });
    assertTrue(expected.getMessage().contains("6.0.0.1.42"));

    expected = expectThrows(ParseException.class, () -> {
      Version.parse("6..0.1");
    });
    assertTrue(expected.getMessage().contains("6..0.1"));
  }
  
  public void testDeprecations() throws Exception {
    // all but the latest version should be deprecated
    boolean atLeastOne = false;
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        atLeastOne = true;
        Version v = (Version)field.get(Version.class);
        final boolean dep = field.isAnnotationPresent(Deprecated.class);
        if (v.equals(Version.LATEST) && field.getName().equals("LUCENE_CURRENT") == false) {
          assertFalse(field.getName() + " should not be deprecated", dep);
        } else {
          assertTrue(field.getName() + " should be deprecated", dep);
        }
      }
    }
    assertTrue(atLeastOne);
  }

  public void testLatestVersionCommonBuild() {
    // common-build.xml sets 'tests.LUCENE_VERSION', if not, we skip this test!
    String commonBuildVersion = System.getProperty("tests.LUCENE_VERSION");
    assumeTrue("Null 'tests.LUCENE_VERSION' test property. You should run the tests with the official Lucene build file",
        commonBuildVersion != null);
    assertEquals("Version.LATEST does not match the one given in common-build.xml",
        Version.LATEST.toString(), commonBuildVersion);
  }

  public void testEqualsHashCode() throws Exception {
    Random random = random();
    String version = "" + (4 + random.nextInt(1)) + "."  + random.nextInt(10) + "." + random.nextInt(10);
    Version v1 = Version.parseLeniently(version);
    Version v2 = Version.parseLeniently(version);
    assertEquals(v1.hashCode(), v2.hashCode());
    assertEquals(v1, v2);
    final int iters = 10 + random.nextInt(20);
    for (int i = 0; i < iters; i++) {
      String v = "" + (4 + random.nextInt(1)) + "."  + random.nextInt(10) + "." + random.nextInt(10);
      if (v.equals(version)) {
        assertEquals(Version.parseLeniently(v).hashCode(), v1.hashCode());
        assertEquals(Version.parseLeniently(v), v1);
      } else {
        assertFalse(Version.parseLeniently(v).equals(v1));
      }
    }
  }
}
