package org.apache.solr.common.util;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestJavaBinCodec extends LuceneTestCase {
  
 public void testStrings() throws Exception {
    JavaBinCodec javabin = new JavaBinCodec();
    for (int i = 0; i < 10000*RANDOM_MULTIPLIER; i++) {
      String s = _TestUtil.randomUnicodeString(random);
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      javabin.marshal(s, os);
      ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
      Object o = javabin.unmarshal(is);
      assertEquals(s, o);
    }
  }
}
