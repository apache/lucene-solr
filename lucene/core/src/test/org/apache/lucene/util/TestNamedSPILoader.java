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
package org.apache.lucene.util;

import java.util.Set;

import org.apache.lucene.codecs.Codec;


// TODO: maybe we should test this with mocks, but it's easy
// enough to test the basics via Codec
public class TestNamedSPILoader extends LuceneTestCase {
  
  public void testLookup() {
    String currentName = TestUtil.getDefaultCodec().getName();
    Codec codec = Codec.forName(currentName);
    assertEquals(currentName, codec.getName());
  }
  
  // we want an exception if it's not found.
  public void testBogusLookup() {
    try {
      Codec.forName("dskfdskfsdfksdfdsf");
      fail();
    } catch (IllegalArgumentException expected) {}
  }
  
  public void testAvailableServices() {
    Set<String> codecs = Codec.availableCodecs();
    assertTrue(codecs.contains(TestUtil.getDefaultCodec().getName()));
  }
}
