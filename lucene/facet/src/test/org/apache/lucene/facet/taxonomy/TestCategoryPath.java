package org.apache.lucene.facet.taxonomy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

public class TestCategoryPath extends LuceneTestCase {
  
  @Test 
  public void testBasic() {
    CategoryPath p = new CategoryPath(0,0);
    assertEquals(0, p.length());
    for (int i=0; i<1000; i++) {
      p.add("hello");
      assertEquals(i+1, p.length());
    }
  }
  
  @Test 
  public void testConstructorCapacity() {
    CategoryPath p = new CategoryPath(0,0);
    assertEquals(0, p.capacityChars());
    assertEquals(0, p.capacityComponents());
    assertEquals(0, p.length());
    p = new CategoryPath(5,18);
    assertEquals(5, p.capacityChars());
    assertEquals(18, p.capacityComponents());
    assertEquals(0, p.length());
    p = new CategoryPath(27,13);
    assertEquals(27, p.capacityChars());
    assertEquals(13, p.capacityComponents());
    assertEquals(0, p.length());
  }
  
  @Test 
  public void testClear() {
    CategoryPath p = new CategoryPath(0,0);
    p.add("hi");
    p.add("there");
    assertEquals(2, p.length());
    p.clear();
    assertEquals(0, p.length());
    p.add("yo!");
    assertEquals(1, p.length());
  }

  @Test 
  public void testTrim() {
    CategoryPath p = new CategoryPath(0,0);
    p.add("this");
    p.add("message");
    p.add("will");
    p.add("self");
    p.add("destruct");
    p.add("in");
    p.add("five");
    p.add("seconds");
    assertEquals(8, p.length());
    p.trim(3);
    assertEquals(5, p.length());
    p.trim(0); // no-op
    assertEquals(5, p.length());
    p.trim(-3);  // no-op
    assertEquals(5, p.length());
    p.trim(1);
    assertEquals(4, p.length());
    p.trim(8); // clear
    assertEquals(0, p.length());
    p.add("yo!");
    assertEquals(1, p.length());
    p.trim(1); // clear
    assertEquals(0, p.length());
  }

  @Test 
  public void testComponentsLimit() {
    // Test that we can add up to 2^15-1 components
    CategoryPath p = new CategoryPath(0,0);
    for (int i=0; i<32767; i++) {
      p.add("");
      assertEquals(i+1, p.length());
    }
    // Also see that in the current implementation, this is actually
    // the limit: if we add one more component, things break (because
    // we used a short to hold ncomponents). See that it breaks in the
    // way we expect it to:
    p.add(""); // this still works, but...
    assertEquals(-32768, p.length()); // now the length is wrong and negative
  }
  
  @Test 
  public void testCharsLimit() {
    // Test that we can add up to 2^15-1 characters
    CategoryPath p = new CategoryPath(0,0);
    for (int i=0; i<8192; i++) {
      p.add("aaaa");
    }
    // Also see that in the current implementation, this is actually the
    // limit: If we add one more character, things break (because ends[]
    // is an array of shorts), and we actually get an exception.
    try {
      p.add("a");
      fail("Should have thrown an exception");
    } catch (ArrayIndexOutOfBoundsException e) {
      // good.
    }
  }
  
  @Test 
  public void testToString() {
    CategoryPath p = new CategoryPath(0,0);
    // When the category is empty, we expect an empty string
    assertEquals("", p.toString('/'));
    // This is (deliberately, in our implementation) indistinguishable
    // from the case of a single empty component:
    p.add("");
    assertEquals("", p.toString('/'));
    // Check just one category (so no delimiter needed):
    p.clear();
    p.add("hello");
    assertEquals("hello", p.toString('/'));
    // Now for two categories:
    p.clear();
    p.add("hello");
    p.add("world");
    assertEquals("hello/world", p.toString('/'));
    // And for a thousand...
    p.clear();
    p.add("0");
    StringBuilder expected = new StringBuilder("0");
    for (int i=1; i<1000; i++) {
      String num = Integer.toString(i);
      p.add(num);
      expected.append('/');
      expected.append(num);
    }
    assertEquals(expected.toString(), p.toString('/'));
    // Check that toString() without a parameter just defaults to '/':
    assertEquals(expected.toString(), p.toString());
  }

  // testing toString() and its variants already test most of the appendTo()
  // code, but not all of it (the "eclemma" code-coverage tool discovered
  // this for us). Here we complete the coverage of the appendTo() methods:
  @Test 
  public void testAppendTo() throws IOException {
    CategoryPath p = new CategoryPath(0,0);
    StringBuilder sb = new StringBuilder();
    p.appendTo(sb, '/');
    assertEquals(0, sb.length());
    p.appendTo(sb, '/', -1);
    assertEquals(0, sb.length());
    p.appendTo(sb, '/', 1);
    assertEquals(0, sb.length());
    p.appendTo(sb, '/', -1, 1);
    assertEquals(0, sb.length());
  }
  
  @Test 
  public void testLastComponent() {
    CategoryPath p = new CategoryPath(1000,1000);
    // When the category is empty, we expect a null
    assertNull(p.lastComponent());
    for (int i=0; i<=100; i++) {
      String num = Integer.toString(i);
      p.add(num);
      assertEquals(num, p.lastComponent());
    }
  }
  
  @Test 
  public void testGetComponent() {
    CategoryPath p = new CategoryPath(1000,1000);
    // When the category is empty, we expect a null
    assertNull(p.getComponent(0));
    assertNull(p.getComponent(1));
    assertNull(p.getComponent(-1));
    for (int i=0; i<=100; i++) {
      p.add(Integer.toString(i));
      for (int j=0; j<=i; j++) {
        assertEquals(j, Integer.parseInt(p.getComponent(j)));
      }
      assertNull(p.getComponent(-1));
      assertNull(p.getComponent(i+1));
    }
  }

  @Test 
  public void testToStringPrefix() {
    CategoryPath p = new CategoryPath(0,0);
    p.add("hi");
    p.add("there");
    p.add("man");
    assertEquals("hi/there/man", p.toString('/'));
    assertEquals("", p.toString('/', 0));
    assertEquals("hi", p.toString('/', 1));
    assertEquals("hi/there", p.toString('/', 2));
    assertEquals("hi/there/man", p.toString('/', 3));
    assertEquals("hi/there/man", p.toString('/', 4));
    assertEquals("hi/there/man", p.toString('/', -1));
  }

  @Test 
  public void testToStringSubpath() {
    CategoryPath p = new CategoryPath(0,0);
    assertEquals("", p.toString('/', 0, 0));
    p.add("hi");
    p.add("there");
    p.add("man");
    assertEquals("", p.toString('/', 0, 0));
    assertEquals("hi", p.toString('/', 0, 1));
    assertEquals("hi/there", p.toString('/', 0, 2));
    assertEquals("hi/there/man", p.toString('/', 0, 3));
    assertEquals("hi/there/man", p.toString('/', 0, 4));
    assertEquals("hi/there/man", p.toString('/', 0, -1));
    assertEquals("hi/there/man", p.toString('/', -1, -1));
    assertEquals("there/man", p.toString('/', 1, -1));
    assertEquals("man", p.toString('/', 2, -1));
    assertEquals("", p.toString('/', 3, -1));
    assertEquals("there/man", p.toString('/', 1, 3));
    assertEquals("there", p.toString('/', 1, 2));
    assertEquals("", p.toString('/', 1, 1));
  }

  @Test 
  public void testDelimiterConstructor() {
    // Test that the constructor that takes a string and a delimiter
    // works correctly. Also check that it allocates exactly the needed
    // needed size for the array - not more.
    CategoryPath p = new CategoryPath("", '/');
    assertEquals(p.length(), 0);
    assertEquals(p.capacityChars(), 0);
    assertEquals(p.capacityComponents(), 0);
    p = new CategoryPath("hello", '/');
    assertEquals(p.length(), 1);
    assertEquals(p.capacityChars(), 5);
    assertEquals(p.capacityComponents(), 1);
    assertEquals(p.toString('@'), "hello");
    p = new CategoryPath("hi/there", '/');
    assertEquals(p.length(), 2);
    assertEquals(p.capacityChars(), 7);
    assertEquals(p.capacityComponents(), 2);
    assertEquals(p.toString('@'), "hi@there");
    p = new CategoryPath("how/are/you/doing?", '/');
    assertEquals(p.length(), 4);
    assertEquals(p.capacityChars(), 15);
    assertEquals(p.capacityComponents(), 4);
    assertEquals(p.toString('@'), "how@are@you@doing?");
  }
  
  @Test 
  public void testDefaultConstructor() {
    // test that the default constructor (no parameters) currently
    // defaults to creating an object with a 0 initial capacity.
    // If we change this default later, we also need to change this
    // test.
    CategoryPath p = new CategoryPath();
    assertEquals(0, p.capacityChars());
    assertEquals(0, p.capacityComponents());
    assertEquals(0, p.length());
    assertEquals("", p.toString('/'));
  }
  
  @Test 
  public void testAddEmpty() {
    // In the current implementation, p.add("") should add en empty
    // component (which is, admitingly, not a useful case. On the other
    // hand, p.add("", delimiter) should add no components at all.
    // Verify this:
    CategoryPath p = new CategoryPath(0, 0);
    p.add("");
    assertEquals(1, p.length());
    p.add("");
    assertEquals(2, p.length());
    p.add("", '/');
    assertEquals(2, p.length());
    p.clear();
    p.add("", '/');
    assertEquals(0, p.length());
  }
  
  @Test 
  public void testDelimiterAdd() {
    // Test that the add() that takes a string and a delimiter
    // works correctly. Note that unlike the constructor test above,
    // we can't expect the capacity to grow to exactly the length of
    // the given category, so we do not test this.
    CategoryPath p = new CategoryPath(0, 0);
    p.add("", '/');
    assertEquals(0, p.length());
    assertEquals("", p.toString('@'), "");
    p.clear();
    p.add("hello", '/');
    assertEquals(p.length(), 1);
    assertEquals(p.toString('@'), "hello");
    p.clear();
    p.add("hi/there", '/');
    assertEquals(p.length(), 2);
    assertEquals(p.toString('@'), "hi@there");
    p.clear();
    p.add("how/are/you/doing?", '/');
    assertEquals(p.length(), 4);
    assertEquals(p.toString('@'), "how@are@you@doing?");
    // See that this is really an add, not replace:
    p.clear();
    p.add("hi/there", '/');
    assertEquals(p.length(), 2);
    assertEquals(p.toString('@'), "hi@there");
    p.add("how/are/you/doing", '/');
    assertEquals(p.length(), 6);
    assertEquals(p.toString('@'), "hi@there@how@are@you@doing");
  }
  
  @Test 
  public void testCopyConstructor() {
    CategoryPath p = new CategoryPath(0,0);
    int expectedchars=0;
    for (int i=0; i<1000; i++) {
      CategoryPath clone = new CategoryPath(p);
      assertEquals(p.length(), clone.length());
      assertEquals(p.toString('/'), clone.toString('/'));
      // verify that the newly created clone has exactly the right
      // capacity, with no spare (while the original path p probably
      // does have spare)
      assertEquals(i, clone.capacityComponents());
      assertEquals(expectedchars, clone.capacityChars());
      // Finally, add another component to the path, for the next
      // round of this loop
      String num = Integer.toString(i);
      p.add(num);
      expectedchars+=num.length();
    }
  }

  @Test 
  public void testPrefixCopyConstructor() {
    CategoryPath p = new CategoryPath(0,0);
    p.add("hi");
    p.add("there");
    p.add("man");
    assertEquals(p.length(), 3);
    
    CategoryPath p1 = new CategoryPath(p,2);
    assertEquals(2, p1.length());
    assertEquals("hi/there", p1.toString('/'));
    // the new prefix object should only take the space it needs: 
    assertEquals(2, p1.capacityComponents());
    assertEquals(7, p1.capacityChars());

    p1 = new CategoryPath(p,1);
    assertEquals(1, p1.length());
    assertEquals("hi", p1.toString('/'));
    assertEquals(1, p1.capacityComponents());
    assertEquals(2, p1.capacityChars());

    p1 = new CategoryPath(p,0);
    assertEquals(0, p1.length());
    assertEquals("", p1.toString('/'));
    assertEquals(0, p1.capacityComponents());
    assertEquals(0, p1.capacityChars());

    // with all the following lengths, the prefix should be the whole path: 
    int[] lengths = { 3, -1, 4 };
    for (int i=0; i<lengths.length; i++) {
      p1 = new CategoryPath(p, lengths[i]);
      assertEquals(3, p1.length());
      assertEquals("hi/there/man", p1.toString('/'));
      assertEquals(p, p1);
      assertEquals(3, p1.capacityComponents());
      assertEquals(10, p1.capacityChars());
    }
  }

  @Test 
  public void testEquals() {
    // check that two empty paths are equal, even if they have different
    // capacities:
    CategoryPath p1 = new CategoryPath(0,0);
    CategoryPath p2 = new CategoryPath(1000,300);
    assertEquals(true, p1.equals(p2));
    // If we make p2 different, it is no longer equals:
    p2.add("hi");
    assertEquals(false, p1.equals(p2));
    // A categoryPath is definitely not equals to an object of some other
    // type:
    assertEquals(false, p1.equals(Integer.valueOf(3)));
    // Build two paths separately, and compare them
    p1.clear();
    p1.add("hello");
    p1.add("world");
    p2.clear();
    p2.add("hello");
    p2.add("world");
    assertEquals(true, p1.equals(p2));    
    // Check that comparison really don't look at old data which might
    // be stored in the array
    p1.clear();
    p1.add("averylongcategoryname");
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hi");
    assertEquals(true, p1.equals(p2));
    // Being of the same length is obviously not enough to be equal
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hello");
    assertEquals(false, p1.equals(p2));
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("ho");
    assertEquals(false, p1.equals(p2));
  }
  @Test 
  public void testHashCode() {
    // Note: in this test, we assume that if two paths are not equal,
    // their hash codes should come out differently. This is *not*
    // always the case, but in the examples we use below, it comes out
    // fine, and unless we have some really bad luck in changing our
    // hash function, this should also remain true in the future.
    
    // check that two empty paths are equal, even if they have different
    // capacities:
    CategoryPath p1 = new CategoryPath(0,0);
    CategoryPath p2 = new CategoryPath(1000,300);
    assertEquals(p1.hashCode(), p2.hashCode());
    // If we make p2 different, it is no longer equals:
    p2.add("hi");
    assertEquals(false, p1.hashCode()==p2.hashCode());
    // Build two paths separately, and compare them
    p1.clear();
    p1.add("hello");
    p1.add("world");
    p2.clear();
    p2.add("hello");
    p2.add("world");
    assertEquals(p1.hashCode(), p2.hashCode());
    // Check that comparison really don't look at old data which might
    // be stored in the array
    p1.clear();
    p1.add("averylongcategoryname");
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hi");
    assertEquals(p1.hashCode(), p2.hashCode());
    // Being of the same length is obviously not enough to be equal
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hello");
    assertEquals(false, p1.hashCode()==p2.hashCode());
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("ho");
    assertEquals(false, p1.hashCode()==p2.hashCode());
  }
  
  @Test 
  public void testHashCodePrefix() {
    // First, repeat the tests of testHashCode() using hashCode(-1)
    // just to make sure nothing was broken in this variant:
    CategoryPath p1 = new CategoryPath(0,0);
    CategoryPath p2 = new CategoryPath(1000,300);
    assertEquals(p1.hashCode(-1), p2.hashCode(-1));
    p2.add("hi");
    assertEquals(false, p1.hashCode(-1)==p2.hashCode(-1));
    p1.clear();
    p1.add("hello");
    p1.add("world");
    p2.clear();
    p2.add("hello");
    p2.add("world");
    assertEquals(p1.hashCode(-1), p2.hashCode(-1));
    p1.clear();
    p1.add("averylongcategoryname");
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hi");
    assertEquals(p1.hashCode(-1), p2.hashCode(-1));
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hello");
    assertEquals(false, p1.hashCode(-1)==p2.hashCode(-1));
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("ho");
    assertEquals(false, p1.hashCode(-1)==p2.hashCode(-1));
    
    // Now move to testing prefixes:
    CategoryPath p = new CategoryPath();
    p.add("this");
    p.add("is");
    p.add("a");
    p.add("test");
    assertEquals(p.hashCode(), p.hashCode(4));
    assertEquals(new CategoryPath().hashCode(), p.hashCode(0));
    assertEquals(new CategoryPath(p, 1).hashCode(), p.hashCode(1));
    assertEquals(new CategoryPath(p, 2).hashCode(), p.hashCode(2));
    assertEquals(new CategoryPath(p, 3).hashCode(), p.hashCode(3));
  }

  @Test 
  public void testLongHashCode() {
    // Note: in this test, we assume that if two paths are not equal,
    // their hash codes should come out differently. This is *not*
    // always the case, but in the examples we use below, it comes out
    // fine, and unless we have some really bad luck in changing our
    // hash function, this should also remain true in the future.
    
    // check that two empty paths are equal, even if they have different
    // capacities:
    CategoryPath p1 = new CategoryPath(0,0);
    CategoryPath p2 = new CategoryPath(1000,300);
    assertEquals(p1.longHashCode(), p2.longHashCode());
    // If we make p2 different, it is no longer equals:
    p2.add("hi");
    assertEquals(false, p1.longHashCode()==p2.longHashCode());
    // Build two paths separately, and compare them
    p1.clear();
    p1.add("hello");
    p1.add("world");
    p2.clear();
    p2.add("hello");
    p2.add("world");
    assertEquals(p1.longHashCode(), p2.longHashCode());
    // Check that comparison really don't look at old data which might
    // be stored in the array
    p1.clear();
    p1.add("averylongcategoryname");
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hi");
    assertEquals(p1.longHashCode(), p2.longHashCode());
    // Being of the same length is obviously not enough to be equal
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hello");
    assertEquals(false, p1.longHashCode()==p2.longHashCode());
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("ho");
    assertEquals(false, p1.longHashCode()==p2.longHashCode());
  }
  
  @Test 
  public void testLongHashCodePrefix() {
    // First, repeat the tests of testLongHashCode() using longHashCode(-1)
    // just to make sure nothing was broken in this variant:
    
    // check that two empty paths are equal, even if they have different
    // capacities:
    CategoryPath p1 = new CategoryPath(0,0);
    CategoryPath p2 = new CategoryPath(1000,300);
    assertEquals(p1.longHashCode(-1), p2.longHashCode(-1));
    // If we make p2 different, it is no longer equals:
    p2.add("hi");
    assertEquals(false, p1.longHashCode(-1)==p2.longHashCode(-1));
    // Build two paths separately, and compare them
    p1.clear();
    p1.add("hello");
    p1.add("world");
    p2.clear();
    p2.add("hello");
    p2.add("world");
    assertEquals(p1.longHashCode(-1), p2.longHashCode(-1));
    // Check that comparison really don't look at old data which might
    // be stored in the array
    p1.clear();
    p1.add("averylongcategoryname");
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hi");
    assertEquals(p1.longHashCode(-1), p2.longHashCode(-1));
    // Being of the same length is obviously not enough to be equal
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("hello");
    assertEquals(false, p1.longHashCode(-1)==p2.longHashCode(-1));
    p1.clear();
    p1.add("hi");
    p2.clear();
    p2.add("ho");
    assertEquals(false, p1.longHashCode(-1)==p2.longHashCode(-1));
    
    // Now move to testing prefixes:
    CategoryPath p = new CategoryPath();
    p.add("this");
    p.add("is");
    p.add("a");
    p.add("test");
    assertEquals(p.longHashCode(), p.longHashCode(4));
    assertEquals(new CategoryPath().longHashCode(), p.longHashCode(0));
    assertEquals(new CategoryPath(p, 1).longHashCode(), p.longHashCode(1));
    assertEquals(new CategoryPath(p, 2).longHashCode(), p.longHashCode(2));
    assertEquals(new CategoryPath(p, 3).longHashCode(), p.longHashCode(3));
  }
  
  @Test 
  public void testArrayConstructor() {
    CategoryPath p = new CategoryPath("hello", "world", "yo");
    assertEquals(3, p.length());
    assertEquals(12, p.capacityChars());
    assertEquals(3, p.capacityComponents());
    assertEquals("hello/world/yo", p.toString('/'));
    
    p = new CategoryPath(new String[0]);
    assertEquals(0, p.length());
    assertEquals(0, p.capacityChars());
    assertEquals(0, p.capacityComponents());
  }
  
  @Test 
  public void testCharsNeededForFullPath() {
    String[] components = { "hello", "world", "yo" };
    CategoryPath p = new CategoryPath();
    assertEquals(0, p.charsNeededForFullPath());
    int expectedCharsNeeded = 0;
    for (int i=0; i<components.length; i++) {
      p.add(components[i]);
      expectedCharsNeeded += components[i].length();
      if (i>0) {
        expectedCharsNeeded++;
      }
      assertEquals(expectedCharsNeeded, p.charsNeededForFullPath());
    }
  }
  
  @Test 
  public void testCopyToCharArray() {
    String[] components = { "hello", "world", "yo" };
    CategoryPath p = new CategoryPath(components);
    char[] charArray = new char[p.charsNeededForFullPath()];
    int numCharsCopied = 0;
    
    numCharsCopied = p.copyToCharArray(charArray, 0, 0, '.');
    assertEquals(0, numCharsCopied);
    assertEquals("", new String(charArray, 0, numCharsCopied));
    
    numCharsCopied = p.copyToCharArray(charArray, 0, 1, '.');
    assertEquals(5, numCharsCopied);
    assertEquals("hello", new String(charArray, 0, numCharsCopied));
    
    numCharsCopied = p.copyToCharArray(charArray, 0, 3, '.');
    assertEquals(14, numCharsCopied);
    assertEquals("hello.world.yo", new String(charArray, 0, numCharsCopied));
    
    numCharsCopied = p.copyToCharArray(charArray, 0, -1, '.');
    assertEquals(14, numCharsCopied);
    assertEquals("hello.world.yo", new String(charArray, 0, numCharsCopied));
    numCharsCopied = p.copyToCharArray(charArray, 0, 4, '.');
    assertEquals(14, numCharsCopied);
    assertEquals("hello.world.yo", new String(charArray, 0, numCharsCopied));
  }
  
  @Test 
  public void testCharSerialization() throws Exception {
    CategoryPath[] testCategories = {
        new CategoryPath("hi", "there", "man"),
        new CategoryPath("hello"),
        new CategoryPath("what's", "up"),
        // See that an empty category, which generates a (char)0,
        // doesn't cause any problems in the middle of the serialization:
        new CategoryPath(),
        new CategoryPath("another", "example"),
        new CategoryPath(),
        new CategoryPath()
    };
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<testCategories.length; i++) {
      testCategories[i].serializeAppendTo(sb);
    }
    
    CategoryPath tmp = new CategoryPath();
    int offset=0;
    for (int i=0; i<testCategories.length; i++) {
      // check equalsToSerialized, in a equal and non-equal case:
      assertTrue(testCategories[i].equalsToSerialized(sb, offset));
      assertFalse(new CategoryPath("Hello", "world").equalsToSerialized(sb, offset));
      assertFalse(new CategoryPath("world").equalsToSerialized(sb, offset));
      // and check hashCodeFromSerialized:
      assertEquals(testCategories[i].hashCode(), CategoryPath.hashCodeOfSerialized(sb, offset));
      // and check setFromSerialized:
      offset = tmp.setFromSerialized(sb, offset);
      assertEquals(testCategories[i], tmp);
    }
    assertEquals(offset, sb.length());
    // A similar test, for a much longer path (though not larger than the
    // 2^15-1 character limit that CategoryPath allows:
    sb = new StringBuilder();
    CategoryPath p = new CategoryPath();
    for (int i=0; i<1000; i++) {
      p.add(Integer.toString(i));
    }
    p.serializeAppendTo(sb);
    p.serializeAppendTo(sb);
    p.serializeAppendTo(sb);
    offset=0;
    assertTrue(p.equalsToSerialized(sb, offset));
    assertEquals(p.hashCode(), CategoryPath.hashCodeOfSerialized(sb, offset));
    offset = tmp.setFromSerialized(sb, offset);
    assertEquals(p, tmp);
    assertTrue(p.equalsToSerialized(sb, offset));
    assertEquals(p.hashCode(), CategoryPath.hashCodeOfSerialized(sb, offset));
    offset = tmp.setFromSerialized(sb, offset);
    assertEquals(p, tmp);
    assertTrue(p.equalsToSerialized(sb, offset));
    assertEquals(p.hashCode(), CategoryPath.hashCodeOfSerialized(sb, offset));
    offset = tmp.setFromSerialized(sb, offset);
    assertEquals(p, tmp);
    assertEquals(offset, sb.length());
    
    // Test the serializeAppendTo variant with a prefixLen
    p = new CategoryPath();
    for (int i=0; i<783; i++) {
      p.add(Integer.toString(i));
    }
    int[] prefixLengths = { 0, 574, 782, 783, 784, -1 };
    for (int prefixLen : prefixLengths) {
      sb = new StringBuilder();
      p.serializeAppendTo(prefixLen, sb);
      assertTrue(new CategoryPath(p, prefixLen).equalsToSerialized(sb, 0));
    }
    
    // Test the equalsToSerialized variant with a prefixLen
    // We use p and prefixLengths set above.
    for (int prefixLen : prefixLengths) {
      sb = new StringBuilder();
      new CategoryPath(p, prefixLen).serializeAppendTo(sb);
      assertTrue(p.equalsToSerialized(prefixLen, sb, 0));
    }
    
    // Check also the false case of equalsToSerialized with prefixLen:
    sb = new StringBuilder();
    new CategoryPath().serializeAppendTo(sb);
    assertTrue(new CategoryPath().equalsToSerialized(0, sb, 0));
    assertTrue(new CategoryPath("a", "b").equalsToSerialized(0, sb, 0));
    assertFalse(new CategoryPath("a", "b").equalsToSerialized(1, sb, 0));
    sb = new StringBuilder();
    new CategoryPath("a", "b").serializeAppendTo(sb);
    assertFalse(new CategoryPath().equalsToSerialized(0, sb, 0));
    assertFalse(new CategoryPath("a").equalsToSerialized(0, sb, 0));
    assertFalse(new CategoryPath("a").equalsToSerialized(1, sb, 0));
    assertFalse(new CategoryPath("a", "b").equalsToSerialized(0, sb, 0));
    assertFalse(new CategoryPath("a", "b").equalsToSerialized(1, sb, 0));
    assertTrue(new CategoryPath("a", "b").equalsToSerialized(2, sb, 0));
    assertTrue(new CategoryPath("a", "b", "c").equalsToSerialized(2, sb, 0));
    assertFalse(new CategoryPath("z", "b", "c").equalsToSerialized(2, sb, 0));
    assertFalse(new CategoryPath("aa", "b", "c").equalsToSerialized(2, sb, 0));
  }

  @Test 
  public void testStreamWriterSerialization() throws Exception {
    CategoryPath[] testPaths = {
        new CategoryPath("hi", "there", "man"),
        new CategoryPath("hello"),
        new CategoryPath("date", "2009", "May", "13", "14", "59", "00"),
        // See that an empty category, which generates a (char)0,
        // doesn't cause any problems in the middle of the serialization:
        new CategoryPath(),
        new CategoryPath("another", "example")
    };
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter osw = new OutputStreamWriter(baos, "UTF-8");  // UTF-8 is always supported.
    for (CategoryPath cp : testPaths) {
      cp.serializeToStreamWriter(osw);
    }
    osw.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    InputStreamReader isr = new InputStreamReader(bais, "UTF-8");
    CategoryPath[] checkPaths = {
        new CategoryPath(), new CategoryPath(), new CategoryPath(), new CategoryPath(), new CategoryPath()
    };
    for (int j = 0; j < checkPaths.length; j++) {
      checkPaths[j].deserializeFromStreamReader(isr);
      assertEquals("Paths not equal", testPaths[j], checkPaths[j]);
    }
  }

  @Test 
  public void testCharSequenceCtor() throws Exception {
    CategoryPath[] testPaths = {
        new CategoryPath(new CS("hi"), new CS("there"), new CS("man")),
        new CategoryPath(new CS("hello")),
        new CategoryPath(new CS("date"), new CS("2009"), new CS("May"), new CS("13"),
            new CS("14"), new CS("59"), new CS("00")),
        new CategoryPath(),
        new CategoryPath(new CS("another"), new CS("example"))
    };
    assertEquals("Wrong capacity", 10, testPaths[0].capacityChars());
    assertEquals("Wrong capacity", 5, testPaths[1].capacityChars());
    assertEquals("Wrong capacity", 19, testPaths[2].capacityChars());
    assertEquals("Wrong capacity", 0, testPaths[3].capacityChars());
    assertEquals("Wrong capacity", 14, testPaths[4].capacityChars());

    assertEquals("Wrong component", "hi", testPaths[0].getComponent(0));
    assertEquals("Wrong component", "there", testPaths[0].getComponent(1));
    assertEquals("Wrong component", "man", testPaths[0].getComponent(2));
    assertEquals("Wrong component", "hello", testPaths[1].getComponent(0));
    assertEquals("Wrong component", "date", testPaths[2].getComponent(0));
    assertEquals("Wrong component", "2009", testPaths[2].getComponent(1));
    assertEquals("Wrong component", "May", testPaths[2].getComponent(2));
    assertEquals("Wrong component", "13", testPaths[2].getComponent(3));
    assertEquals("Wrong component", "14", testPaths[2].getComponent(4));
    assertEquals("Wrong component", "59", testPaths[2].getComponent(5));
    assertEquals("Wrong component", "00", testPaths[2].getComponent(6));
    assertNull("Not null component", testPaths[3].getComponent(0));
    assertEquals("Wrong component", "another", testPaths[4].getComponent(0));
    assertEquals("Wrong component", "example", testPaths[4].getComponent(1));
  }

  @Test 
  public void testIsDescendantOf() throws Exception {
    CategoryPath[] testPaths = {
        new CategoryPath(new CS("hi"), new CS("there")),
        new CategoryPath(new CS("hi"), new CS("there"), new CS("man")),
        new CategoryPath(new CS("hithere"), new CS("man")),
        new CategoryPath(new CS("hi"), new CS("there"), new CS("mano")),
        new CategoryPath(),
    };
    assertTrue(testPaths[0].isDescendantOf(testPaths[0]));
    assertTrue(testPaths[0].isDescendantOf(testPaths[4]));
    assertFalse(testPaths[4].isDescendantOf(testPaths[0]));
    assertTrue(testPaths[1].isDescendantOf(testPaths[0]));
    assertTrue(testPaths[1].isDescendantOf(testPaths[1]));
    assertTrue(testPaths[3].isDescendantOf(testPaths[0]));
    assertFalse(testPaths[2].isDescendantOf(testPaths[0]));
    assertFalse(testPaths[2].isDescendantOf(testPaths[1]));
    assertFalse(testPaths[3].isDescendantOf(testPaths[1]));
  }

  @Test 
  public void testCompareTo() {
    CategoryPath p = new CategoryPath("a/b/c/d", '/');
    CategoryPath pother = new CategoryPath("a/b/c/d", '/');
    assertTrue(pother.compareTo(p) == 0);
    pother = new CategoryPath("", '/');
    assertTrue(pother.compareTo(p) < 0);
    pother = new CategoryPath("a/b_/c/d", '/');
    assertTrue(pother.compareTo(p) > 0);
    pother = new CategoryPath("a/b/c", '/');
    assertTrue(pother.compareTo(p) < 0);
    pother = new CategoryPath("a/b/c/e", '/');
    assertTrue(pother.compareTo(p) > 0);
    pother = new CategoryPath("a/b/c//e", '/');
    assertTrue(pother.compareTo(p) < 0);
  }
  
  private static class CS implements CharSequence {
    public CS(String s) {
      this.ca = new char[s.length()];
      s.getChars(0, s.length(), this.ca, 0);
    }
    public char charAt(int index) {
      return this.ca[index];
    }
    public int length() {
      return this.ca.length;
    }
    public CharSequence subSequence(int start, int end) {
      return null; // not used.
    }
    private char[] ca;
  }

}
