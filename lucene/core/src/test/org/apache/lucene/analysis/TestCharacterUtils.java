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


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.lucene.analysis.CharacterUtils.CharacterBuffer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

/**
 * TestCase for the {@link CharacterUtils} class.
 */
public class TestCharacterUtils extends LuceneTestCase {

  public void testConversions() {
    final char[] orig = TestUtil.randomUnicodeString(random(), 100).toCharArray();
    final int[] buf = new int[orig.length];
    final char[] restored = new char[buf.length];
    final int o1 = TestUtil.nextInt(random(), 0, Math.min(5, orig.length));
    final int o2 = TestUtil.nextInt(random(), 0, o1);
    final int o3 = TestUtil.nextInt(random(), 0, o1);
    final int codePointCount = CharacterUtils.toCodePoints(orig, o1, orig.length - o1, buf, o2);
    final int charCount = CharacterUtils.toChars(buf, o2, codePointCount, restored, o3);
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

    // length must be >= 2
    expectThrows(IllegalArgumentException.class, () -> {
      CharacterUtils.newCharacterBuffer(1);
    });
  }

  @Test
  public void testFillNoHighSurrogate() throws IOException {
    Reader reader = new StringReader("helloworld");
    CharacterBuffer buffer = CharacterUtils.newCharacterBuffer(6);
    assertTrue(CharacterUtils.fill(buffer,reader));
    assertEquals(0, buffer.getOffset());
    assertEquals(6, buffer.getLength());
    assertEquals("hellow", new String(buffer.getBuffer()));
    assertFalse(CharacterUtils.fill(buffer,reader));
    assertEquals(4, buffer.getLength());
    assertEquals(0, buffer.getOffset());

    assertEquals("orld", new String(buffer.getBuffer(), buffer.getOffset(),
        buffer.getLength()));
    assertFalse(CharacterUtils.fill(buffer,reader));
  }

  @Test
  public void testFill() throws IOException {
    String input = "1234\ud801\udc1c789123\ud801\ud801\udc1c\ud801";
    Reader reader = new StringReader(input);
    CharacterBuffer buffer = CharacterUtils.newCharacterBuffer(5);
    assertTrue(CharacterUtils.fill(buffer, reader));
    assertEquals(4, buffer.getLength());
    assertEquals("1234", new String(buffer.getBuffer(), buffer.getOffset(),
        buffer.getLength()));
    assertTrue(CharacterUtils.fill(buffer, reader));
    assertEquals(5, buffer.getLength());
    assertEquals("\ud801\udc1c789", new String(buffer.getBuffer()));
    assertTrue(CharacterUtils.fill(buffer, reader));
    assertEquals(4, buffer.getLength());
    assertEquals("123\ud801", new String(buffer.getBuffer(),
        buffer.getOffset(), buffer.getLength()));
    assertFalse(CharacterUtils.fill(buffer, reader));
    assertEquals(3, buffer.getLength());
    assertEquals("\ud801\udc1c\ud801", new String(buffer.getBuffer(), buffer
        .getOffset(), buffer.getLength()));
    assertFalse(CharacterUtils.fill(buffer, reader));
    assertEquals(0, buffer.getLength());
  }

}
