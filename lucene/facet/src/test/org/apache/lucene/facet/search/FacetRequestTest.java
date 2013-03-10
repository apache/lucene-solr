package org.apache.lucene.facet.search;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.junit.Test;

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

public class FacetRequestTest extends FacetTestCase {

  @Test(expected=IllegalArgumentException.class)
  public void testIllegalNumResults() throws Exception {
    assertNotNull(new CountFacetRequest(new CategoryPath("a", "b"), 0));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testIllegalCategoryPath() throws Exception {
    assertNotNull(new CountFacetRequest(null, 1));
  }

  @Test
  public void testHashAndEquals() {
    CountFacetRequest fr1 = new CountFacetRequest(new CategoryPath("a"), 8);
    CountFacetRequest fr2 = new CountFacetRequest(new CategoryPath("a"), 8);
    assertEquals("hashCode() should agree on both objects", fr1.hashCode(), fr2.hashCode());
    assertTrue("equals() should return true", fr1.equals(fr2));
    fr1.setDepth(10);
    assertFalse("equals() should return false as fr1.depth != fr2.depth", fr1.equals(fr2));
  }
  
}
