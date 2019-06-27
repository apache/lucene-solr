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
package org.apache.solr.common.util;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Test for FastInputStream.
 *
 *
 * @see org.apache.solr.common.util.FastInputStream
 */
public class TestFastInputStream extends SolrTestCase {
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testgzip() throws Exception {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    FastOutputStream fos = new FastOutputStream(b);
    GZIPOutputStream gzos = new GZIPOutputStream(fos);
    String ss = "Helloooooooooooooooooooo";
    writeChars(gzos, ss, 0, ss.length());
    gzos.close();
    JavaBinCodec.writeVInt(10, fos);
    fos.flushBuffer();
    GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(b.toByteArray(), 0, b.size()));
    char[] cbuf = new char[ss.length()];
    readChars(gzis, cbuf, 0, ss.length());
    assertEquals(new String(cbuf), ss);
    // System.out.println("passes w/o FastInputStream");

    ByteArrayInputStream bis = new ByteArrayInputStream(b.toByteArray(), 0, b.size());
    gzis = new GZIPInputStream(new FastInputStream(bis));
    cbuf = new char[ss.length()];
    readChars(gzis, cbuf, 0, ss.length());
    assertEquals(new String(cbuf), ss);
    // System.out.println("passes w FastInputStream");
  }

  //code copied from NamedListCodec#readChars
  public static void readChars(InputStream in, char[] buffer, int start, int length)
          throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      int b = in.read();
      if ((b & 0x80) == 0)
        buffer[i] = (char) b;
      else if ((b & 0xE0) != 0xE0) {
        buffer[i] = (char) (((b & 0x1F) << 6)
                | (in.read() & 0x3F));
      } else
        buffer[i] = (char) (((b & 0x0F) << 12)
                | ((in.read() & 0x3F) << 6)
                | (in.read() & 0x3F));
    }
  }

  // code copied rfrom NamedlistCode#writechars
  public static void writeChars(OutputStream os, String s, int start, int length) throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      final int code = (int) s.charAt(i);
      if (code >= 0x01 && code <= 0x7F)
        os.write(code);
      else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
        os.write(0xC0 | (code >> 6));
        os.write(0x80 | (code & 0x3F));
      } else {
        os.write(0xE0 | (code >>> 12));
        os.write(0x80 | ((code >> 6) & 0x3F));
        os.write(0x80 | (code & 0x3F));
      }
    }
  }
}
