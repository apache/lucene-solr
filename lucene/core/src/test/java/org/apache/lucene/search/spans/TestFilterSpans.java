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
package org.apache.lucene.search.spans;

import java.lang.reflect.Method;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestFilterSpans extends LuceneTestCase {

  @Test
  public void testOverrides() throws Exception {
    // verify that all methods of Spans are overridden by FilterSpans,
    for (Method m : FilterSpans.class.getMethods()) {
      if (m.getDeclaringClass() == Spans.class) {
        fail("method " + m.getName() + " not overridden!");
      }
    }
  }
  
}
