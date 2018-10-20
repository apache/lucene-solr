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



import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.LuceneTestCase;

public class TestDistanceFacetsExample extends LuceneTestCase {

  public void testSimple() throws Exception {
    DistanceFacetsExample example = new DistanceFacetsExample();
    example.index();
    FacetResult result = example.search();
    assertEquals("dim=field path=[] value=3 childCount=4\n  < 1 km (1)\n  < 2 km (2)\n  < 5 km (2)\n  < 10 km (3)\n", result.toString());
    example.close();
  }

  public void testDrillDown() throws Exception {
    DistanceFacetsExample example = new DistanceFacetsExample();
    example.index();
    TopDocs hits = example.drillDown(example.FIVE_KM);
    assertEquals(2, hits.totalHits.value);
    example.close();
  }
}
