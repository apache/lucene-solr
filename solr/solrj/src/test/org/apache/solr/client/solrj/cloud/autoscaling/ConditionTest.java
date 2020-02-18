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

package org.apache.solr.client.solrj.cloud.autoscaling;

import static org.junit.Assert.*;

import org.junit.Test;

public class ConditionTest {
  @Test
  public void testEqualsHashCode() {
    assertHashMatchesEquals("equals should match hash (names are equal)",
        new Condition("node", null, null, null, null),
        new Condition("node", null, null, null, null));
    assertHashMatchesEquals("equals should match hash (names aren't equal)",
        new Condition("node", null, null, null, null),
        new Condition("host", null, null, null, null));
    assertHashMatchesEquals("equals should match hash (values are equal)",
        new Condition("node", "localhost", null, null, null),
        new Condition("node", "localhost", null, null, null));
    assertHashMatchesEquals("equals should match hash (values aren't equal)",
        new Condition("node", "localhost", null, null, null),
        new Condition("node", "lucene.apache.org", null, null, null));
    assertHashMatchesEquals("equals should match hash (operands are equal)",
        new Condition("node", null, Operand.EQUAL, null, null),
        new Condition("node", null, Operand.EQUAL, null, null));
    assertHashMatchesEquals("equals should match hash (operands aren't equal)",
        new Condition("node", null, Operand.EQUAL, null, null),
        new Condition("node", null, Operand.NOT_EQUAL, null, null));

    Condition condition = new Condition("host", "localhost", Operand.EQUAL, null, null);
    assertHashMatchesEquals("equals should match hash when compared to self", condition, condition);
    assertTrue("equals should be true when compared to self", condition.equals(condition));
  }

  @Test
  public void testEqualsInvertible() {
    assertEqualsInvertible("equals should be invertible (names are equal)",
        new Condition("node", null, null, null, null),
        new Condition("node", null, null, null, null));
    assertEqualsInvertible("equals should be invertible (names aren't equal)",
        new Condition("node", null, null, null, null),
        new Condition("host", null, null, null, null));
    assertEqualsInvertible("equals should be invertible (values are equal)",
        new Condition("node", "localhost", null, null, null),
        new Condition("node", "localhost", null, null, null));
    assertEqualsInvertible("equals should be invertible (values aren't equal)",
        new Condition("node", "localhost", null, null, null),
        new Condition("node", "lucene.apache.org", null, null, null));
    assertEqualsInvertible("equals should be invertible (operands are equal)",
        new Condition("node", null, Operand.EQUAL, null, null),
        new Condition("node", null, Operand.EQUAL, null, null));
    assertEqualsInvertible("equals should be invertible (operands aren't equal)",
        new Condition("node", null, Operand.EQUAL, null, null),
        new Condition("node", null, Operand.NOT_EQUAL, null, null));
  }

  private void assertEqualsInvertible(String message, Condition a, Condition b) {
    assertEquals(message, a != null && a.equals(b), b != null && b.equals(a));
  }

  private void assertHashMatchesEquals(String message, Condition a, Condition b) {
    assertTrue(message, (a.hashCode() == b.hashCode()) || (!a.equals(b) && !b.equals(a)));
  }
}
