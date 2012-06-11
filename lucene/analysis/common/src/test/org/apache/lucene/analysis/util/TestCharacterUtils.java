package org.apache.lucene.analysis.util;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.junit.Test;

/**
 * TestCase for the {@link CharacterUtils} class.
 */
public class TestCharacterUtils extends LuceneTestCase {

  @Test
  public void testCodePointAtCharArrayInt() {
    CharacterUtils java4 = CharacterUtils.getInstance(Version.LUCENE_30);
    char[] cpAt3 = "Abc\ud801\udc1c".toCharArray();
    char[] highSurrogateAt3 = "Abc\ud801".toCharArray();
    assertEquals((int) 'A', java4.codePointAt(cpAt3, 0));
    assertEquals((int) '\ud801', java4.codePointAt(cpAt3, 3));
    assertEquals((int) '\ud801', java4.codePointAt(highSurrogateAt3, 3));
    try {
      java4.codePointAt(highSurrogateAt3, 4);
      fail("array index out of bounds");
    } catch (IndexOutOfBoundsException e) {
    }

    CharacterUtils java5 = CharacterUtils.getInstance(TEST_VERSION_CURRENT);
    assertEquals((int) 'A', java5.codePointAt(cpAt3, 0));
    assertEquals(Character.toCodePoint('\ud801', '\udc1c'), java5.codePointAt(
        cpAt3, 3));
    assertEquals((int) '\ud801', java5.codePointAt(highSurrogateAt3, 3));
    try {
      java5.codePointAt(highSurrogateAt3, 4);
      fail("array index out of bounds");
    } catch (IndexOutOfBoundsException e) {
    }
  }

  @Test
  public void testCodePointAtCharSequenceInt() {
    CharacterUtils java4 = CharacterUtils.getInstance(Version.LUCENE_30);
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

    CharacterUtils java5 = CharacterUtils.getInstance(TEST_VERSION_CURRENT);
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
    CharacterUtils java4 = CharacterUtils.getInstance(Version.LUCENE_30);
    char[] cpAt3 = "Abc\ud801\udc1c".toCharArray();
    char[] highSurrogateAt3 = "Abc\ud801".toCharArray();
    assertEquals((int) 'A', java4.codePointAt(cpAt3, 0, 2));
    assertEquals((int) '\ud801', java4.codePointAt(cpAt3, 3, 5));
    assertEquals((int) '\ud801', java4.codePointAt(highSurrogateAt3, 3, 4));

    CharacterUtils java5 = CharacterUtils.getInstance(TEST_VERSION_CURRENT);
    assertEquals((int) 'A', java5.codePointAt(cpAt3, 0, 2));
    assertEquals(Character.toCodePoint('\ud801', '\udc1c'), java5.codePointAt(
        cpAt3, 3, 5));
    assertEquals((int) '\ud801', java5.codePointAt(highSurrogateAt3, 3, 4));

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
    Version[] versions = new Version[] { Version.LUCENE_30, TEST_VERSION_CURRENT };
    for (Version version : versions) {
      CharacterUtils instance = CharacterUtils.getInstance(version);
      Reader reader = new StringReader("helloworld");
      CharacterBuffer buffer = CharacterUtils.newCharacterBuffer(6);
      assertTrue(instance.fill(buffer,reader));
      assertEquals(0, buffer.getOffset());
      assertEquals(6, buffer.getLength());
      assertEquals("hellow", new String(buffer.getBuffer()));
      assertTrue(instance.fill(buffer,reader));
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
    CharacterUtils instance = CharacterUtils.getInstance(TEST_VERSION_CURRENT);
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
    assertTrue(instance.fill(buffer, reader));
    assertEquals(2, buffer.getLength());
    assertEquals("\ud801\udc1c", new String(buffer.getBuffer(), buffer
        .getOffset(), buffer.getLength()));
    assertTrue(instance.fill(buffer, reader));
    assertEquals(1, buffer.getLength());
    assertEquals("\ud801", new String(buffer.getBuffer(), buffer
        .getOffset(), buffer.getLength()));
    assertFalse(instance.fill(buffer, reader));
  }

  @Test
  public void testFillJava14() throws IOException {
    String input = "1234\ud801\udc1c789123\ud801\ud801\udc1c\ud801";
    CharacterUtils instance = CharacterUtils.getInstance(Version.LUCENE_30);
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
