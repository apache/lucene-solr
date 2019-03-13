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
package org.apache.solr.request;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class SubstringBytesRefFilterTest extends SolrTestCase {
    
  @Test
  public void testSubstringBytesRefFilter() {
    final List<String> substrings = new ArrayList<>(4);
    substrings.add("foo");
    substrings.add("ooba");
    substrings.add("bar");
    substrings.add("foobar");

    final String contains = substrings.get(random().nextInt(substrings.size()));
    final boolean ignoreCase = random().nextBoolean();
    final SubstringBytesRefFilter filter = new SubstringBytesRefFilter(contains, ignoreCase);

    assertTrue(filter.test(new BytesRef("foobar")));

    if (ignoreCase) {
      assertTrue(filter.test(new BytesRef("FooBar")));
    } else {
      assertFalse(filter.test(new BytesRef("FooBar")));
    }

    assertFalse(filter.test(new BytesRef("qux")));
  }

} 
