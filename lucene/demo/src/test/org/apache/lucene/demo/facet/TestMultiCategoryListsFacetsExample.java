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
package org.apache.lucene.demo.facet;


import java.util.List;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestMultiCategoryListsFacetsExample extends LuceneTestCase {

  @Test
  public void testExample() throws Exception {
    List<FacetResult> results = new MultiCategoryListsFacetsExample().runSearch();
    assertEquals(2, results.size());
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n", results.get(0).toString());
    assertEquals("dim=Publish Date path=[] value=5 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n", results.get(1).toString());
  }
}
