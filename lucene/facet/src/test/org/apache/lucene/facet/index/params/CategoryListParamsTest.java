package org.apache.lucene.facet.index.params;

import org.apache.lucene.index.Term;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.params.CategoryListParams;

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

public class CategoryListParamsTest extends LuceneTestCase {

  @Test
  public void testDefaultSettings() {
    CategoryListParams clp = new CategoryListParams();
    assertEquals("wrong default term", new Term("$facets", "$fulltree$"), clp.getTerm());
    assertEquals("unexpected default encoder", "Sorting (Unique (DGap (VInt8)))", clp.createEncoder().toString());
    assertEquals("unexpected default decoder", "DGap (VInt8)", clp.createEncoder().createMatchingDecoder().toString());
  }
  
  /**
   * Test that the {@link CategoryListParams#hashCode()} and
   * {@link CategoryListParams#equals(Object)} are consistent.
   */
  @Test
  public void testIdentity() {
    CategoryListParams clParams1 = new CategoryListParams();
    // Assert identity is correct - a CategoryListParams equals itself.
    assertEquals("A CategoryListParams object does not equal itself.",
        clParams1, clParams1);
    // For completeness, the object's hashcode equals itself
    assertEquals("A CategoryListParams object's hashCode does not equal itself.",
        clParams1.hashCode(), clParams1.hashCode());
  }

  /**
   * Test that CategoryListParams behave correctly when compared against each
   * other.
   */
  @Test
  public void testIdentityConsistency() {
    // Test 2 CategoryListParams with the default parameter
    CategoryListParams clParams1 = new CategoryListParams();
    CategoryListParams clParams2 = new CategoryListParams();
    assertEquals(
        "2 CategoryListParams with the same default term should equal each other.",
        clParams1, clParams2);
    assertEquals("2 CategoryListParams with the same default term should have the same hashcode",
        clParams1.hashCode(), clParams2.hashCode());

    // Test 2 CategoryListParams with the same specified Term
    clParams1 = new CategoryListParams(new Term("test"));
    clParams2 = new CategoryListParams(new Term("test"));
    assertEquals(
        "2 CategoryListParams with the same term should equal each other.",
        clParams1, clParams2);
    assertEquals("2 CategoryListParams with the same term should have the same hashcode",
        clParams1.hashCode(), clParams2.hashCode());
    
    // Test 2 CategoryListParams with DIFFERENT terms
    clParams1 = new CategoryListParams(new Term("test1"));
    clParams2 = new CategoryListParams(new Term("test2"));
    assertFalse(
        "2 CategoryListParams with the different terms should NOT equal each other.",
        clParams1.equals(clParams2));
    assertFalse(
        "2 CategoryListParams with the different terms should NOT have the same hashcode.",
        clParams1.hashCode() == clParams2.hashCode());
  }

}
