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
package org.apache.solr.common;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class HitCountRelationTest extends SolrTestCaseJ4 {

  @Test
  public void testOrdinalsDontChange() {
    assertEquals("HitCountRelation ordinals are used for serialization, don't change them without considering compatibility",
        HitCountRelation.EQUAL_TO, HitCountRelation.values()[0]);
    assertEquals("HitCountRelation ordinals are used for serialization, don't change them without considering compatibility",
        HitCountRelation.GREATER_THAN_OR_EQUAL_TO, HitCountRelation.values()[1]);
  }
  
  @Test
  public void testForOrdinal() {
    expectThrows(IllegalArgumentException.class, () -> HitCountRelation.forOrdinal(-1));
    expectThrows(IllegalArgumentException.class, () -> HitCountRelation.forOrdinal(2));
    assertEquals(HitCountRelation.EQUAL_TO, HitCountRelation.forOrdinal(0));
    assertEquals(HitCountRelation.GREATER_THAN_OR_EQUAL_TO, HitCountRelation.forOrdinal(1));
  }

}
