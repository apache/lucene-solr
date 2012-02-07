package org.apache.lucene.util;

import java.util.Set;

import org.apache.lucene.codecs.Codec;

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

// TODO: maybe we should test this with mocks, but its easy
// enough to test the basics via Codec
public class TestNamedSPILoader extends LuceneTestCase {
  public void testLookup() {
    Codec codec = Codec.forName("Lucene40");
    assertEquals("Lucene40", codec.getName());
  }
  
  // we want an exception if its not found.
  public void testBogusLookup() {
    try {
      Codec codec = Codec.forName("dskfdskfsdfksdfdsf");
      fail();
    } catch (IllegalArgumentException expected) {}
  }
  
  public void testAvailableServices() {
    Set<String> codecs = Codec.availableCodecs();
    assertTrue(codecs.contains("Lucene40"));
  }
}
