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
package org.apache.lucene.expressions.js;


import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.expressions.js.VariableContext.Type.MEMBER;
import static org.apache.lucene.expressions.js.VariableContext.Type.STR_INDEX;
import static org.apache.lucene.expressions.js.VariableContext.Type.INT_INDEX;
import static org.apache.lucene.expressions.js.VariableContext.Type.METHOD;

public class TestVariableContext extends LuceneTestCase {

  public void testSimpleVar() {
    VariableContext[] x = VariableContext.parse("foo");
    assertEquals(1, x.length);
    assertEquals(x[0].type, MEMBER);
    assertEquals(x[0].text, "foo");
  }

  public void testEmptyString() {
    VariableContext[] x = VariableContext.parse("foo['']");
    assertEquals(2, x.length);
    assertEquals(x[1].type, STR_INDEX);
    assertEquals(x[1].text, "");
  }

  public void testUnescapeString() {
    VariableContext[] x = VariableContext.parse("foo['\\'\\\\']");
    assertEquals(2, x.length);
    assertEquals(x[1].type, STR_INDEX);
    assertEquals(x[1].text, "'\\");
  }

  public void testMember() {
    VariableContext[] x = VariableContext.parse("foo.bar");
    assertEquals(2, x.length);
    assertEquals(x[1].type, MEMBER);
    assertEquals(x[1].text, "bar");
  }

  public void testMemberFollowedByMember() {
    VariableContext[] x = VariableContext.parse("foo.bar.baz");
    assertEquals(3, x.length);
    assertEquals(x[2].type, MEMBER);
    assertEquals(x[2].text, "baz");
  }

  public void testMemberFollowedByIntArray() {
    VariableContext[] x = VariableContext.parse("foo.bar[1]");
    assertEquals(3, x.length);
    assertEquals(x[2].type, INT_INDEX);
    assertEquals(x[2].integer, 1);
  }

  public void testMethodWithMember() {
    VariableContext[] x = VariableContext.parse("m.m()");
    assertEquals(2, x.length);
    assertEquals(x[1].type, METHOD);
    assertEquals(x[1].text, "m");
  }

  public void testMethodWithStrIndex() {
    VariableContext[] x = VariableContext.parse("member['blah'].getMethod()");
    assertEquals(3, x.length);
    assertEquals(x[2].type, METHOD);
    assertEquals(x[2].text, "getMethod");
  }

  public void testMethodWithNumericalIndex() {
    VariableContext[] x = VariableContext.parse("member[0].getMethod()");
    assertEquals(3, x.length);
    assertEquals(x[2].type, METHOD);
    assertEquals(x[2].text, "getMethod");
  }
}
