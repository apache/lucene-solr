package org.apache.lucene.index.codecs.preflex;

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

import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.codecs.*;
import org.apache.lucene.util.*;

import java.util.*;
import java.io.IOException;

public class TestSurrogates extends LuceneTestCase {

  private static final boolean DEBUG = false;

  // like Term, but uses BytesRef for text
  private static class FieldAndText implements Comparable<FieldAndText> {
    String field;
    BytesRef text;

    public FieldAndText(Term t) {
      field = t.field();
      text = new BytesRef(t.text());
    }
    
    public int compareTo(FieldAndText other) {
      if (other.field == field) {
        return text.compareTo(other.text);
      } else {
        return field.compareTo(other.field);
      }
    }
  }

  // chooses from a very limited alphabet to exacerbate the
  // surrogate seeking required
  private static String makeDifficultRandomUnicodeString(Random r) {
    final int end = r.nextInt(20);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      int t = r.nextInt(5);

      if (0 == t && i < end - 1) {
        // hi
        buffer[i++] = (char) 0xd800;
        // lo
        buffer[i] = (char) 0xdc00;
      } else if (t <= 3) {
        buffer[i] = 'a';
      }  else if (4 == t) {
        buffer[i] = 0xe000;
      }
    }

    return new String(buffer, 0, end);
  }

  private SegmentInfo makePreFlexSegment(Random r, String segName, Directory dir, FieldInfos fieldInfos, Codec codec, List<FieldAndText> fieldTerms) throws IOException {

    final int numField = _TestUtil.nextInt(r, 2, 5);

    List<Term> terms = new ArrayList<Term>();

    int tc = 0;

    for(int f=0;f<numField;f++) {
      String field = "f" + f;
      Term protoTerm = new Term(field);

      fieldInfos.add(field, true, false, false, false, false, false, false);
      final int numTerms = 10000*_TestUtil.getRandomMultiplier();
      for(int i=0;i<numTerms;i++) {
        String s;
        if (r.nextInt(3) == 1) {
          s = makeDifficultRandomUnicodeString(r);
        } else {
          s = _TestUtil.randomUnicodeString(r);

          // The surrogate dance uses 0xffff to seek-to-end
          // of blocks.  Also, pre-4.0 indices are already
          // guaranteed to not contain the char 0xffff since
          // it's mapped during indexing:
          s = s.replace((char) 0xffff, (char) 0xfffe);
        }
        terms.add(protoTerm.createTerm(s + "_" + (tc++)));
      }
    }

    fieldInfos.write(dir, segName);

    // sorts in UTF16 order, just like preflex:
    Collections.sort(terms);

    TermInfosWriter w = new TermInfosWriter(dir, segName, fieldInfos, 128);
    TermInfo ti = new TermInfo();
    BytesRef utf8 = new BytesRef(10);
    String lastText = null;
    int uniqueTermCount = 0;
    if (DEBUG) {
      System.out.println("TEST: utf16 order:");
    }
    for(Term t : terms) {
      FieldInfo fi = fieldInfos.fieldInfo(t.field());

      String text = t.text();
      if (lastText != null && lastText.equals(text)) {
        continue;
      }
      fieldTerms.add(new FieldAndText(t));
      uniqueTermCount++;
      lastText = text;
      UnicodeUtil.UTF16toUTF8(text, 0, text.length(), utf8);

      if (DEBUG) {
        System.out.println("  " + toHexString(t));
      }
      w.add(fi.number, utf8.bytes, utf8.length, ti);
    }
    w.close();

    Collections.sort(fieldTerms);
    if (DEBUG) {
      System.out.println("\nTEST: codepoint order");
      for(FieldAndText t: fieldTerms) {
        System.out.println("  " + t.field + ":" + UnicodeUtil.toHexString(t.text.utf8ToString()));
      }
    }

    dir.createOutput(segName + ".prx").close();
    dir.createOutput(segName + ".frq").close();

    // !!hack alert!! stuffing uniqueTermCount in as docCount
    return new SegmentInfo(segName, uniqueTermCount, dir, false, -1, null, false, true, codec);
  }

  private String toHexString(Term t) {
    return t.field() + ":" + UnicodeUtil.toHexString(t.text());
  }

  public void testSurrogatesOrder() throws Exception {
    Directory dir = new MockRAMDirectory();

    Codec codec = new PreFlexCodec();

    Random r = newRandom();
    FieldInfos fieldInfos = new FieldInfos();
    List<FieldAndText> fieldTerms = new ArrayList<FieldAndText>();
    SegmentInfo si = makePreFlexSegment(r, "_0", dir, fieldInfos, codec, fieldTerms);

    // hack alert!!
    int uniqueTermCount = si.docCount;

    FieldsProducer fields = codec.fieldsProducer(new SegmentReadState(dir, si, fieldInfos, 1024, 1));
    assertNotNull(fields);

    if (DEBUG) {
      System.out.println("\nTEST: now enum");
    }
    FieldsEnum fieldsEnum = fields.iterator();
    String field;
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();

    int termCount = 0;
    while((field = fieldsEnum.next()) != null) {
      TermsEnum termsEnum = fieldsEnum.terms();
      BytesRef text;
      BytesRef lastText = null;
      while((text = termsEnum.next()) != null) {
        UnicodeUtil.UTF8toUTF16(text.bytes, text.offset, text.length, utf16);
        if (DEBUG) {
          System.out.println("got term=" + field + ":" + UnicodeUtil.toHexString(new String(utf16.result, 0, utf16.length)));
          System.out.println();
        }
        if (lastText == null) {
          lastText = new BytesRef(text);
        } else {
          assertTrue(lastText.compareTo(text) < 0);
          lastText.copy(text);
        }
        assertEquals(fieldTerms.get(termCount).field, field);
        assertEquals(fieldTerms.get(termCount).text, text);
        termCount++;
      }
      if (DEBUG) {
        System.out.println("  no more terms for field=" + field);
      }
    }
    assertEquals(uniqueTermCount, termCount);

    fields.close();
  }
}
