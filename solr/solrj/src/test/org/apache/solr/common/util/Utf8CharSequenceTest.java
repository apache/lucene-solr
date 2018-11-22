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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;

public class Utf8CharSequenceTest extends SolrTestCaseJ4 {

  public void testLargeString() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("Hello World!");
    }
    ByteArrayUtf8CharSequence utf8 = new ByteArrayUtf8CharSequence(sb.toString());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[256];
    FastOutputStream fos = new FastOutputStream(baos, buf, 0);
    fos.writeUtf8CharSeq(utf8);
    fos.flush();
    byte[] result = baos.toByteArray();
    ByteArrayUtf8CharSequence utf81 = new ByteArrayUtf8CharSequence(result, 0, result.length);
    assertTrue(utf81.equals(utf8));
    baos.reset();
    utf8.write(baos);
    result = baos.toByteArray();
    utf81 = new ByteArrayUtf8CharSequence(result, 0, result.length);
    assertTrue(utf81.equals(utf8));

    Map m0 = new HashMap();
    m0.put("str", utf8);
    baos.reset();
    new JavaBinCodec().marshal(m0, baos);
    result = baos.toByteArray();
    Map m1 = (Map) new JavaBinCodec()
        .setReadStringAsCharSeq(true)
        .unmarshal(new ByteArrayInputStream(result));
    utf81 = (ByteArrayUtf8CharSequence) m1.get("str");
    assertTrue(utf81.equals(utf8));
  }
}
