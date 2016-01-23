package org.apache.lucene.facet.taxonomy;

import java.util.Arrays;

import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
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

public class TestFacetLabel extends FacetTestCase {
  
  @Test 
  public void testBasic() {
    assertEquals(0, new FacetLabel().length);
    assertEquals(1, new FacetLabel("hello").length);
    assertEquals(2, new FacetLabel("hello", "world").length);
  }
  
  @Test 
  public void testToString() {
    // When the category is empty, we expect an empty string
    assertEquals("FacetLabel: []", new FacetLabel().toString());
    // one category
    assertEquals("FacetLabel: [hello]", new FacetLabel("hello").toString());
    // more than one category
    assertEquals("FacetLabel: [hello, world]", new FacetLabel("hello", "world").toString());
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
  public void testDefaultConstructor() {
    // test that the default constructor (no parameters) currently
    // defaults to creating an object with a 0 initial capacity.
    // If we change this default later, we also need to change this
    // test.
    FacetLabel p = new FacetLabel();
    assertEquals(0, p.length);
    assertEquals("FacetLabel: []", p.toString());
  }
  
  @Test 
  public void testSubPath() {
    final FacetLabel p = new FacetLabel("hi", "there", "man");
    assertEquals(p.length, 3);
    
    FacetLabel p1 = p.subpath(2);
    assertEquals(2, p1.length);
    assertEquals("FacetLabel: [hi, there]", p1.toString());

    p1 = p.subpath(1);
    assertEquals(1, p1.length);
    assertEquals("FacetLabel: [hi]", p1.toString());

    p1 = p.subpath(0);
    assertEquals(0, p1.length);
    assertEquals("FacetLabel: []", p1.toString());

    // with all the following lengths, the prefix should be the whole path 
    int[] lengths = { 3, -1, 4 };
    for (int i = 0; i < lengths.length; i++) {
      p1 = p.subpath(lengths[i]);
      assertEquals(3, p1.length);
      assertEquals("FacetLabel: [hi, there, man]", p1.toString());
      assertEquals(p, p1);
    }
  }

  @Test 
  public void testEquals() {
    assertEquals(new FacetLabel(), new FacetLabel());
    assertFalse(new FacetLabel().equals(new FacetLabel("hi")));
    assertFalse(new FacetLabel().equals(Integer.valueOf(3)));
    assertEquals(new FacetLabel("hello", "world"), new FacetLabel("hello", "world"));    
  }
  
  @Test 
  public void testHashCode() {
    assertEquals(new FacetLabel().hashCode(), new FacetLabel().hashCode());
    assertFalse(new FacetLabel().hashCode() == new FacetLabel("hi").hashCode());
    assertEquals(new FacetLabel("hello", "world").hashCode(), new FacetLabel("hello", "world").hashCode());
  }
  
  @Test 
  public void testLongHashCode() {
    assertEquals(new FacetLabel().longHashCode(), new FacetLabel().longHashCode());
    assertFalse(new FacetLabel().longHashCode() == new FacetLabel("hi").longHashCode());
    assertEquals(new FacetLabel("hello", "world").longHashCode(), new FacetLabel("hello", "world").longHashCode());
  }
  
  @Test 
  public void testArrayConstructor() {
    FacetLabel p = new FacetLabel("hello", "world", "yo");
    assertEquals(3, p.length);
    assertEquals("FacetLabel: [hello, world, yo]", p.toString());
  }
  
  @Test 
  public void testCompareTo() {
    FacetLabel p = new FacetLabel("a", "b", "c", "d");
    FacetLabel pother = new FacetLabel("a", "b", "c", "d");
    assertEquals(0, pother.compareTo(p));
    assertEquals(0, p.compareTo(pother));
    pother = new FacetLabel();
    assertTrue(pother.compareTo(p) < 0);
    assertTrue(p.compareTo(pother) > 0);
    pother = new FacetLabel("a", "b_", "c", "d");
    assertTrue(pother.compareTo(p) > 0);
    assertTrue(p.compareTo(pother) < 0);
    pother = new FacetLabel("a", "b", "c");
    assertTrue(pother.compareTo(p) < 0);
    assertTrue(p.compareTo(pother) > 0);
    pother = new FacetLabel("a", "b", "c", "e");
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
        // expected
      }
      try {
        new FacetField("dim", components);
        fail("empty or null components should not be allowed: " + Arrays.toString(components));
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        new AssociationFacetField(new BytesRef(), "dim", components);
        fail("empty or null components should not be allowed: " + Arrays.toString(components));
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        new IntAssociationFacetField(17, "dim", components);
        fail("empty or null components should not be allowed: " + Arrays.toString(components));
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        new FloatAssociationFacetField(17.0f, "dim", components);
        fail("empty or null components should not be allowed: " + Arrays.toString(components));
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
    try {
      new FacetField(null, new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new FacetField("", new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new IntAssociationFacetField(17, null, new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new IntAssociationFacetField(17, "", new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new FloatAssociationFacetField(17.0f, null, new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new FloatAssociationFacetField(17.0f, "", new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new AssociationFacetField(new BytesRef(), null, new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new AssociationFacetField(new BytesRef(), "", new String[] {"abc"});
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SortedSetDocValuesFacetField(null, "abc");
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SortedSetDocValuesFacetField("", "abc");
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SortedSetDocValuesFacetField("dim", null);
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SortedSetDocValuesFacetField("dim", "");
      fail("empty or null components should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testLongPath() throws Exception {
    String bigComp = null;
    while (true) {
      int len = FacetLabel.MAX_CATEGORY_PATH_LENGTH;
      bigComp = TestUtil.randomSimpleString(random(), len, len);
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
  }
}
