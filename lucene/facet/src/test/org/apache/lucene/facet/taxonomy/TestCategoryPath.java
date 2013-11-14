package org.apache.lucene.facet.taxonomy;

import java.util.Arrays;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

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

public class TestCategoryPath extends FacetTestCase {
  
  @Test 
  public void testBasic() {
    assertEquals(0, FacetLabel.EMPTY.length);
    assertEquals(1, new FacetLabel("hello").length);
    assertEquals(2, new FacetLabel("hello", "world").length);
  }
  
  @Test 
  public void testToString() {
    // When the category is empty, we expect an empty string
    assertEquals("", FacetLabel.EMPTY.toString('/'));
    // one category (so no delimiter needed)
    assertEquals("hello", new FacetLabel("hello").toString('/'));
    // more than one category (so no delimiter needed)
    assertEquals("hello/world", new FacetLabel("hello", "world").toString('/'));
  }

  @Test 
  public void testGetComponent() {
    String[] components = new String[atLeast(10)];
    for (int i = 0; i < components.length; i++) {
      components[i] = Integer.toString(i);
    }
    FacetLabel cp = new FacetLabel(components);
    for (int i = 0; i < components.length; i++) {
      assertEquals(i, Integer.parseInt(cp.components[i]));
    }
  }

  @Test
  public void testDelimiterConstructor() {
    FacetLabel p = new FacetLabel("", '/');
    assertEquals(0, p.length);
    p = new FacetLabel("hello", '/');
    assertEquals(p.length, 1);
    assertEquals(p.toString('@'), "hello");
    p = new FacetLabel("hi/there", '/');
    assertEquals(p.length, 2);
    assertEquals(p.toString('@'), "hi@there");
    p = new FacetLabel("how/are/you/doing?", '/');
    assertEquals(p.length, 4);
    assertEquals(p.toString('@'), "how@are@you@doing?");
  }
  
  @Test
  public void testDefaultConstructor() {
    // test that the default constructor (no parameters) currently
    // defaults to creating an object with a 0 initial capacity.
    // If we change this default later, we also need to change this
    // test.
    FacetLabel p = FacetLabel.EMPTY;
    assertEquals(0, p.length);
    assertEquals("", p.toString('/'));
  }
  
  @Test 
  public void testSubPath() {
    final FacetLabel p = new FacetLabel("hi", "there", "man");
    assertEquals(p.length, 3);
    
    FacetLabel p1 = p.subpath(2);
    assertEquals(2, p1.length);
    assertEquals("hi/there", p1.toString('/'));

    p1 = p.subpath(1);
    assertEquals(1, p1.length);
    assertEquals("hi", p1.toString('/'));

    p1 = p.subpath(0);
    assertEquals(0, p1.length);
    assertEquals("", p1.toString('/'));

    // with all the following lengths, the prefix should be the whole path 
    int[] lengths = { 3, -1, 4 };
    for (int i = 0; i < lengths.length; i++) {
      p1 = p.subpath(lengths[i]);
      assertEquals(3, p1.length);
      assertEquals("hi/there/man", p1.toString('/'));
      assertEquals(p, p1);
    }
  }

  @Test 
  public void testEquals() {
    assertEquals(FacetLabel.EMPTY, FacetLabel.EMPTY);
    assertFalse(FacetLabel.EMPTY.equals(new FacetLabel("hi")));
    assertFalse(FacetLabel.EMPTY.equals(Integer.valueOf(3)));
    assertEquals(new FacetLabel("hello", "world"), new FacetLabel("hello", "world"));    
  }
  
  @Test 
  public void testHashCode() {
    assertEquals(FacetLabel.EMPTY.hashCode(), FacetLabel.EMPTY.hashCode());
    assertFalse(FacetLabel.EMPTY.hashCode() == new FacetLabel("hi").hashCode());
    assertEquals(new FacetLabel("hello", "world").hashCode(), new FacetLabel("hello", "world").hashCode());
  }
  
  @Test 
  public void testLongHashCode() {
    assertEquals(FacetLabel.EMPTY.longHashCode(), FacetLabel.EMPTY.longHashCode());
    assertFalse(FacetLabel.EMPTY.longHashCode() == new FacetLabel("hi").longHashCode());
    assertEquals(new FacetLabel("hello", "world").longHashCode(), new FacetLabel("hello", "world").longHashCode());
  }
  
  @Test 
  public void testArrayConstructor() {
    FacetLabel p = new FacetLabel("hello", "world", "yo");
    assertEquals(3, p.length);
    assertEquals("hello/world/yo", p.toString('/'));
  }
  
  @Test 
  public void testCharsNeededForFullPath() {
    assertEquals(0, FacetLabel.EMPTY.fullPathLength());
    String[] components = { "hello", "world", "yo" };
    FacetLabel cp = new FacetLabel(components);
    int expectedCharsNeeded = 0;
    for (String comp : components) {
      expectedCharsNeeded += comp.length();
    }
    expectedCharsNeeded += cp.length - 1; // delimiter chars
    assertEquals(expectedCharsNeeded, cp.fullPathLength());
  }
  
  @Test 
  public void testCopyToCharArray() {
    FacetLabel p = new FacetLabel("hello", "world", "yo");
    char[] charArray = new char[p.fullPathLength()];
    int numCharsCopied = p.copyFullPath(charArray, 0, '.');
    assertEquals(p.fullPathLength(), numCharsCopied);
    assertEquals("hello.world.yo", new String(charArray, 0, numCharsCopied));
  }
  
  @Test 
  public void testCompareTo() {
    FacetLabel p = new FacetLabel("a/b/c/d", '/');
    FacetLabel pother = new FacetLabel("a/b/c/d", '/');
    assertEquals(0, pother.compareTo(p));
    assertEquals(0, p.compareTo(pother));
    pother = new FacetLabel("", '/');
    assertTrue(pother.compareTo(p) < 0);
    assertTrue(p.compareTo(pother) > 0);
    pother = new FacetLabel("a/b_/c/d", '/');
    assertTrue(pother.compareTo(p) > 0);
    assertTrue(p.compareTo(pother) < 0);
    pother = new FacetLabel("a/b/c", '/');
    assertTrue(pother.compareTo(p) < 0);
    assertTrue(p.compareTo(pother) > 0);
    pother = new FacetLabel("a/b/c/e", '/');
    assertTrue(pother.compareTo(p) > 0);
    assertTrue(p.compareTo(pother) < 0);
  }

  @Test
  public void testEmptyNullComponents() throws Exception {
    // LUCENE-4724: CategoryPath should not allow empty or null components
    String[][] components_tests = new String[][] {
      new String[] { "", "test" }, // empty in the beginning
      new String[] { "test", "" }, // empty in the end
      new String[] { "test", "", "foo" }, // empty in the middle
      new String[] { null, "test" }, // null at the beginning
      new String[] { "test", null }, // null in the end
      new String[] { "test", null, "foo" }, // null in the middle
    };

    for (String[] components : components_tests) {
      try {
        assertNotNull(new FacetLabel(components));
        fail("empty or null components should not be allowed: " + Arrays.toString(components));
      } catch (IllegalArgumentException e) {
        // ok
      }
    }
    
    String[] path_tests = new String[] {
        "/test", // empty in the beginning
        "test//foo", // empty in the middle
    };
    
    for (String path : path_tests) {
      try {
        assertNotNull(new FacetLabel(path, '/'));
        fail("empty or null components should not be allowed: " + path);
      } catch (IllegalArgumentException e) {
        // ok
      }
    }

    // a trailing path separator is produces only one component
    assertNotNull(new FacetLabel("test/", '/'));
    
  }

  @Test
  public void testInvalidDelimChar() throws Exception {
    // Make sure CategoryPath doesn't silently corrupt:
    char[] buf = new char[100];
    FacetLabel cp = new FacetLabel("foo/bar");
    try {
      cp.toString();
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      cp.copyFullPath(buf, 0, '/');
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    cp = new FacetLabel("abc", "foo/bar");
    try {
      cp.toString();
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      cp.copyFullPath(buf, 0, '/');
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    cp = new FacetLabel("foo:bar");
    try {
      cp.toString(':');
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      cp.copyFullPath(buf, 0, ':');
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    cp = new FacetLabel("abc", "foo:bar");
    try {
      cp.toString(':');
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      cp.copyFullPath(buf, 0, ':');
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testLongPath() throws Exception {
    String bigComp = null;
    while (true) {
      int len = FacetLabel.MAX_CATEGORY_PATH_LENGTH;
      bigComp = _TestUtil.randomSimpleString(random(), len, len);
      if (bigComp.indexOf('\u001f') != -1) {
        continue;
      }
      break;
    }

    try {
      assertNotNull(new FacetLabel("dim", bigComp));
      fail("long paths should not be allowed; len=" + bigComp.length());
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      assertNotNull(new FacetLabel("dim\u001f" + bigComp, '\u001f'));
      fail("long paths should not be allowed; len=" + bigComp.length());
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
}
