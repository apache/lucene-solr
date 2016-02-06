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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

/**
 * TestCase for the {@link CharacterUtils} class.
 */
public class TestCharacterUtils extends LuceneTestCase {

  @Test
  public void testCodePointAtCharSequenceInt() {
    CharacterUtils java4 = CharacterUtils.getJava4Instance();
    String cpAt3 = "Abc\ud801\udc1c";
    String highSurrogateAt3 = "Abc\ud801";
    assertEquals((int) 'A', java4.codePointAt(cpAt3, 0));
    assertEquals((int) '\ud801', java4.codePointAt(cpAt3, 3));
    assertEquals((int) '\ud801', java4.codePointAt(highSurrogateAt3, 3));
    try {
      java4.codePointAt(highSurrogateAt3, 4);
      fail("string index out of bounds");
    } catch (IndexOutOfBoundsException e) {
    }

    CharacterUtils java5 = CharacterUtils.getInstance();
    assertEquals((int) 'A', java5.codePointAt(cpAt3, 0));
    assertEquals(Character.toCodePoint('\ud801', '\udc1c'), java5.codePointAt(
        cpAt3, 3));
    assertEquals((int) '\ud801', java5.codePointAt(highSurrogateAt3, 3));
    try {
      java5.codePointAt(highSurrogateAt3, 4);
      fail("string index out of bounds");
    } catch (IndexOutOfBoundsException e) {
    }

  }

  @Test
  public void testCodePointAtCharArrayIntInt() {
    CharacterUtils java4 = CharacterUtils.getJava4Instance();
    char[] cpAt3 = "Abc\ud801\udc1c".toCharArray();
    char[] highSurrogateAt3 = "Abc\ud801".toCharArray();
    assertEquals((int) 'A', java4.codePointAt(cpAt3, 0, 2));
    assertEquals((int) '\ud801', java4.codePointAt(cpAt3, 3, 5));
    assertEquals((int) '\ud801', java4.codePointAt(highSurrogateAt3, 3, 4));

    CharacterUtils java5 = CharacterUtils.getInstance();
    assertEquals((int) 'A', java5.codePointAt(cpAt3, 0, 2));
    assertEquals(Character.toCodePoint('\ud801', '\udc1c'), java5.codePointAt(
        cpAt3, 3, 5));
    assertEquals((int) '\ud801', java5.codePointAt(highSurrogateAt3, 3, 4));
  }

  @Test
  public void testCodePointCount() {
    CharacterUtils java4 = CharacterUtils.getJava4Instance();
    CharacterUtils java5 = CharacterUtils.getInstance();
    final String s = TestUtil.randomUnicodeString(random());
    assertEquals(s.length(), java4.codePointCount(s));
    assertEquals(Character.codePointCount(s, 0, s.length()), java5.codePointCount(s));
  }

  @Test
  public void testOffsetByCodePoint() {
    CharacterUtils java4 = CharacterUtils.getJava4Instance();
    CharacterUtils java5 = CharacterUtils.getInstance();
    for (int i = 0; i < 10; ++i) {
      final char[] s = TestUtil.randomUnicodeString(random()).toCharArray();
      final int index = TestUtil.nextInt(random(), 0, s.length);
      final int offset = random().nextInt(7) - 3;
      try {
        final int o = java4.offsetByCodePoints(s, 0, s.length, index, offset);
        assertEquals(o, index + offset);
      } catch (IndexOutOfBoundsException e) {
        assertTrue((index + offset) < 0 || (index + offset) > s.length);
      }
  
      int o;
      try {
        o = java5.offsetByCodePoints(s, 0, s.length, index, offset);
      } catch (IndexOutOfBoundsException e) {
        try {
          Character.offsetByCodePoints(s, 0, s.length, index, offset);
          fail();
        } catch (IndexOutOfBoundsException e2) {
          // OK
        }
        o = -1;
      }
      if (o >= 0) {
        assertEquals(Character.offsetByCodePoints(s, 0, s.length, index, offset), o);
      }
    }
  }

  public void testConversions() {
    CharacterUtils java4 = CharacterUtils.getJava4Instance();
    CharacterUtils java5 = CharacterUtils.getInstance();
    testConversions(java4);
    testConversions(java5);
  }

  private void testConversions(CharacterUtils charUtils) {
    final char[] orig = TestUtil.randomUnicodeString(random(), 100).toCharArray();
    final int[] buf = new int[orig.length];
    final char[] restored = new char[buf.length];
    final int o1 = TestUtil.nextInt(random(), 0, Math.min(5, orig.length));
    final int o2 = TestUtil.nextInt(random(), 0, o1);
    final int o3 = TestUtil.nextInt(random(), 0, o1);
    final int codePointCount = charUtils.toCodePoints(orig, o1, orig.length - o1, buf, o2);
    final int charCount = charUtils.toChars(buf, o2, codePointCount, restored, o3);
    assertEquals(orig.length - o1, charCount);
    assertArrayEquals(Arrays.copyOfRange(orig, o1, o1 + charCount), Arrays.copyOfRange(restored, o3, o3 + charCount));
  }

  @Test
  public void testNewCharacterBuffer() {
    CharacterBuffer newCharacterBuffer = CharacterUtils.newCharacterBuffer(1024);
    assertEquals(1024, newCharacterBuffer.getBuffer().length);
    assertEquals(0, newCharacterBuffer.getOffset());
    assertEquals(0, newCharacterBuffer.getLength());

    newCharacterBuffer = CharacterUtils.newCharacterBuffer(2);
    assertEquals(2, newCharacterBuffer.getBuffer().length);
    assertEquals(0, newCharacterBuffer.getOffset());
    assertEquals(0, newCharacterBuffer.getLength());

    try {
      newCharacterBuffer = CharacterUtils.newCharacterBuffer(1);
      fail("length must be >= 2");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testFillNoHighSurrogate() throws IOException {
    CharacterUtils versions[] = new CharacterUtils[] { 
        CharacterUtils.getInstance(), 
        CharacterUtils.getJava4Instance() };
    for (CharacterUtils instance : versions) {
      Reader reader = new StringReader("helloworld");
      CharacterBuffer buffer = CharacterUtils.newCharacterBuffer(6);
      assertTrue(instance.fill(buffer,reader));
      assertEquals(0, buffer.getOffset());
      assertEquals(6, buffer.getLength());
      assertEquals("hellow", new String(buffer.getBuffer()));
      assertFalse(instance.fill(buffer,reader));
      assertEquals(4, buffer.getLength());
      assertEquals(0, buffer.getOffset());

      assertEquals("orld", new String(buffer.getBuffer(), buffer.getOffset(),
          buffer.getLength()));
      assertFalse(instance.fill(buffer,reader));
    }
  }

  @Test
  public void testFillJava15() throws IOException {
    String input = "1234\ud801\udc1c789123\ud801\ud801\udc1c\ud801";
    CharacterUtils instance = CharacterUtils.getInstance();
    Reader reader = new StringReader(input);
    CharacterBuffer buffer = CharacterUtils.newCharacterBuffer(5);
    assertTrue(instance.fill(buffer, reader));
    assertEquals(4, buffer.getLength());
    assertEquals("1234", new String(buffer.getBuffer(), buffer.getOffset(),
        buffer.getLength()));
    assertTrue(instance.fill(buffer, reader));
    assertEquals(5, buffer.getLength());
    assertEquals("\ud801\udc1c789", new String(buffer.getBuffer()));
    assertTrue(instance.fill(buffer, reader));
    assertEquals(4, buffer.getLength());
    assertEquals("123\ud801", new String(buffer.getBuffer(),
        buffer.getOffset(), buffer.getLength()));
    assertFalse(instance.fill(buffer, reader));
    assertEquals(3, buffer.getLength());
    assertEquals("\ud801\udc1c\ud801", new String(buffer.getBuffer(), buffer
        .getOffset(), buffer.getLength()));
    assertFalse(instance.fill(buffer, reader));
    assertEquals(0, buffer.getLength());
  }

  @Test
  public void testFillJava14() throws IOException {
    String input = "1234\ud801\udc1c789123\ud801\ud801\udc1c\ud801";
    CharacterUtils instance = CharacterUtils.getJava4Instance();
    Reader reader = new StringReader(input);
    CharacterBuffer buffer = CharacterUtils.newCharacterBuffer(5);
    assertTrue(instance.fill(buffer, reader));
    assertEquals(5, buffer.getLength());
    assertEquals("1234\ud801", new String(buffer.getBuffer(), buffer
        .getOffset(), buffer.getLength()));
    assertTrue(instance.fill(buffer, reader));
    assertEquals(5, buffer.getLength());
    assertEquals("\udc1c7891", new String(buffer.getBuffer()));
    buffer = CharacterUtils.newCharacterBuffer(6);
    assertTrue(instance.fill(buffer, reader));
    assertEquals(6, buffer.getLength());
    assertEquals("23\ud801\ud801\udc1c\ud801", new String(buffer.getBuffer(), buffer
        .getOffset(), buffer.getLength()));
    assertFalse(instance.fill(buffer, reader));

  }

}
