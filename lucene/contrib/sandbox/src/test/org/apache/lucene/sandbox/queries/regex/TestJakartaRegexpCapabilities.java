package org.apache.lucene.sandbox.queries.regex;

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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Testcase for {@link JakartaRegexpCapabilities}
 */
public class TestJakartaRegexpCapabilities extends LuceneTestCase {

  public void testGetPrefix(){
    JakartaRegexpCapabilities cap = new JakartaRegexpCapabilities();
    RegexCapabilities.RegexMatcher matcher = cap.compile("luc[e]?");
    assertTrue(matcher.match(new BytesRef("luce")));
    assertEquals("luc", matcher.prefix());
    
    matcher = cap.compile("lucene");
    assertTrue(matcher.match(new BytesRef("lucene")));
    assertEquals("lucene", matcher.prefix());
  }
  
  public void testShakyPrefix(){
    JakartaRegexpCapabilities cap = new JakartaRegexpCapabilities();
    RegexCapabilities.RegexMatcher matcher = cap.compile("(ab|ac)");
    assertTrue(matcher.match(new BytesRef("ab")));
    assertTrue(matcher.match(new BytesRef("ac")));
    // why is it not a???
    assertNull(matcher.prefix());
  }
}
