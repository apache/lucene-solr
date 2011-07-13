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

package org.apache.solr.common.util;

import org.apache.lucene.util.LuceneTestCase;

public class NamedListTest extends LuceneTestCase {
  public void testRemove() {
    NamedList<String> nl = new NamedList<String>();
    nl.add("key1", "value1");
    nl.add("key2", "value2");
    assertEquals(2, nl.size());
    String value = nl.remove(0);
    assertEquals("value1", value);
    assertEquals(1, nl.size());
  }
}
