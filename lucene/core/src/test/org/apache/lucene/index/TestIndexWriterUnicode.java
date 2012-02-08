package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;

public class TestIndexWriterUnicode extends LuceneTestCase {

  final String[] utf8Data = new String[] {
    // unpaired low surrogate
    "ab\udc17cd", "ab\ufffdcd",
    "\udc17abcd", "\ufffdabcd",
    "\udc17", "\ufffd",
    "ab\udc17\udc17cd", "ab\ufffd\ufffdcd",
    "\udc17\udc17abcd", "\ufffd\ufffdabcd",
    "\udc17\udc17", "\ufffd\ufffd",

    // unpaired high surrogate
    "ab\ud917cd", "ab\ufffdcd",
    "\ud917abcd", "\ufffdabcd",
    "\ud917", "\ufffd",
    "ab\ud917\ud917cd", "ab\ufffd\ufffdcd",
    "\ud917\ud917abcd", "\ufffd\ufffdabcd",
    "\ud917\ud917", "\ufffd\ufffd",

    // backwards surrogates
    "ab\udc17\ud917cd", "ab\ufffd\ufffdcd",
    "\udc17\ud917abcd", "\ufffd\ufffdabcd",
    "\udc17\ud917", "\ufffd\ufffd",
    "ab\udc17\ud917\udc17\ud917cd", "ab\ufffd\ud917\udc17\ufffdcd",
    "\udc17\ud917\udc17\ud917abcd", "\ufffd\ud917\udc17\ufffdabcd",
    "\udc17\ud917\udc17\ud917", "\ufffd\ud917\udc17\ufffd"
  };
  
  private int nextInt(int lim) {
    return random.nextInt(lim);
  }

  private int nextInt(int start, int end) {
    return start + nextInt(end-start);
  }
  
  private boolean fillUnicode(char[] buffer, char[] expected, int offset, int count) {
    final int len = offset + count;
    boolean hasIllegal = false;

    if (offset > 0 && buffer[offset] >= 0xdc00 && buffer[offset] < 0xe000)
      // Don't start in the middle of a valid surrogate pair
      offset--;

    for(int i=offset;i<len;i++) {
      int t = nextInt(6);
      if (0 == t && i < len-1) {
        // Make a surrogate pair
        // High surrogate
        expected[i] = buffer[i++] = (char) nextInt(0xd800, 0xdc00);
        // Low surrogate
        expected[i] = buffer[i] = (char) nextInt(0xdc00, 0xe000);
      } else if (t <= 1)
        expected[i] = buffer[i] = (char) nextInt(0x80);
      else if (2 == t)
        expected[i] = buffer[i] = (char) nextInt(0x80, 0x800);
      else if (3 == t)
        expected[i] = buffer[i] = (char) nextInt(0x800, 0xd800);
      else if (4 == t)
        expected[i] = buffer[i] = (char) nextInt(0xe000, 0xffff);
      else if (5 == t && i < len-1) {
        // Illegal unpaired surrogate
        if (nextInt(10) == 7) {
          if (random.nextBoolean())
            buffer[i] = (char) nextInt(0xd800, 0xdc00);
          else
            buffer[i] = (char) nextInt(0xdc00, 0xe000);
          expected[i++] = 0xfffd;
          expected[i] = buffer[i] = (char) nextInt(0x800, 0xd800);
          hasIllegal = true;
        } else
          expected[i] = buffer[i] = (char) nextInt(0x800, 0xd800);
      } else {
        expected[i] = buffer[i] = ' ';
      }
    }

    return hasIllegal;
  }
  
  // both start & end are inclusive
  private final int getInt(Random r, int start, int end) {
    return start + r.nextInt(1+end-start);
  }

  private final String asUnicodeChar(char c) {
    return "U+" + Integer.toHexString(c);
  }

  private final String termDesc(String s) {
    final String s0;
    assertTrue(s.length() <= 2);
    if (s.length() == 1) {
      s0 = asUnicodeChar(s.charAt(0));
    } else {
      s0 = asUnicodeChar(s.charAt(0)) + "," + asUnicodeChar(s.charAt(1));
    }
    return s0;
  }

  // LUCENE-510
  public void testRandomUnicodeStrings() throws Throwable {
    char[] buffer = new char[20];
    char[] expected = new char[20];

    UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();

    int num = atLeast(100000);
    for (int iter = 0; iter < num; iter++) {
      boolean hasIllegal = fillUnicode(buffer, expected, 0, 20);

      UnicodeUtil.UTF16toUTF8(buffer, 0, 20, utf8);
      if (!hasIllegal) {
        byte[] b = new String(buffer, 0, 20).getBytes("UTF-8");
        assertEquals(b.length, utf8.length);
        for(int i=0;i<b.length;i++)
          assertEquals(b[i], utf8.result[i]);
      }

      UnicodeUtil.UTF8toUTF16(utf8.result, 0, utf8.length, utf16);
      assertEquals(utf16.length, 20);
      for(int i=0;i<20;i++)
        assertEquals(expected[i], utf16.result[i]);
    }
  }

  // LUCENE-510
  public void testAllUnicodeChars() throws Throwable {

    UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
    char[] chars = new char[2];
    for(int ch=0;ch<0x0010FFFF;ch++) {

      if (ch == 0xd800)
        // Skip invalid code points
        ch = 0xe000;

      int len = 0;
      if (ch <= 0xffff) {
        chars[len++] = (char) ch;
      } else {
        chars[len++] = (char) (((ch-0x0010000) >> 10) + UnicodeUtil.UNI_SUR_HIGH_START);
        chars[len++] = (char) (((ch-0x0010000) & 0x3FFL) + UnicodeUtil.UNI_SUR_LOW_START);
      }

      UnicodeUtil.UTF16toUTF8(chars, 0, len, utf8);
      
      String s1 = new String(chars, 0, len);
      String s2 = new String(utf8.result, 0, utf8.length, "UTF-8");
      assertEquals("codepoint " + ch, s1, s2);

      UnicodeUtil.UTF8toUTF16(utf8.result, 0, utf8.length, utf16);
      assertEquals("codepoint " + ch, s1, new String(utf16.result, 0, utf16.length));

      byte[] b = s1.getBytes("UTF-8");
      assertEquals(utf8.length, b.length);
      for(int j=0;j<utf8.length;j++)
        assertEquals(utf8.result[j], b[j]);
    }
  }
  
  public void testEmbeddedFFFF() throws Throwable {

    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig( TEST_VERSION_CURRENT, new TestIndexWriter.StringSplitAnalyzer()));
    Document doc = new Document();
    doc.add(newField("field", "a a\uffffb", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newField("field", "a", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);
    w.close();

    d.close();
  }

  // LUCENE-510
  public void testInvalidUTF16() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new TestIndexWriter.StringSplitAnalyzer()));
    Document doc = new Document();

    final int count = utf8Data.length/2;
    for(int i=0;i<count;i++)
      doc.add(newField("f" + i, utf8Data[2*i], Field.Store.YES, Field.Index.ANALYZED));
    w.addDocument(doc);
    w.close();

    IndexReader ir = IndexReader.open(dir, true);
    Document doc2 = ir.document(0);
    for(int i=0;i<count;i++) {
      assertEquals("field " + i + " was not indexed correctly", 1, ir.docFreq(new Term("f"+i, utf8Data[2*i+1])));
      assertEquals("field " + i + " is incorrect", utf8Data[2*i+1], doc2.getField("f"+i).stringValue());
    }
    ir.close();
    dir.close();
  }
  
  // LUCENE-510
  public void testIncrementalUnicodeStrings() throws Throwable {
    char[] buffer = new char[20];
    char[] expected = new char[20];

    UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
    UnicodeUtil.UTF16Result utf16a = new UnicodeUtil.UTF16Result();

    boolean hasIllegal = false;
    byte[] last = new byte[60];

    int num = atLeast(100000);
    for (int iter = 0; iter < num; iter++) {

      final int prefix;

      if (iter == 0 || hasIllegal)
        prefix = 0;
      else
        prefix = nextInt(20);

      hasIllegal = fillUnicode(buffer, expected, prefix, 20-prefix);

      UnicodeUtil.UTF16toUTF8(buffer, 0, 20, utf8);
      if (!hasIllegal) {
        byte[] b = new String(buffer, 0, 20).getBytes("UTF-8");
        assertEquals(b.length, utf8.length);
        for(int i=0;i<b.length;i++)
          assertEquals(b[i], utf8.result[i]);
      }

      int bytePrefix = 20;
      if (iter == 0 || hasIllegal)
        bytePrefix = 0;
      else
        for(int i=0;i<20;i++)
          if (last[i] != utf8.result[i]) {
            bytePrefix = i;
            break;
          }
      System.arraycopy(utf8.result, 0, last, 0, utf8.length);

      UnicodeUtil.UTF8toUTF16(utf8.result, bytePrefix, utf8.length-bytePrefix, utf16);
      assertEquals(20, utf16.length);
      for(int i=0;i<20;i++)
        assertEquals(expected[i], utf16.result[i]);

      UnicodeUtil.UTF8toUTF16(utf8.result, 0, utf8.length, utf16a);
      assertEquals(20, utf16a.length);
      for(int i=0;i<20;i++)
        assertEquals(expected[i], utf16a.result[i]);
    }
  }
}
