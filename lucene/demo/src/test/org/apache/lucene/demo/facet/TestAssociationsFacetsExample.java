package org.apache.lucene.demo.facet;

import java.util.List;

import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.util.LuceneTestCase;
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

public class TestAssociationsFacetsExample extends LuceneTestCase {
  
  private static final double[] EXPECTED_INT_SUM_RESULTS = { 4, 2};
  private static final double[] EXPECTED_FLOAT_SUM_RESULTS = { 1.62, 0.34};

  @Test
  public void testExamples() throws Exception {
    assertExampleResult(new AssociationsFacetsExample().runSumIntAssociations(), EXPECTED_INT_SUM_RESULTS);
    assertExampleResult(new AssociationsFacetsExample().runSumFloatAssociations(), EXPECTED_FLOAT_SUM_RESULTS);
  }

  private void assertExampleResult(List<FacetResult> res, double[] expectedResults) {
    assertNotNull("Null result!", res);
    assertEquals("Wrong number of results!", 1, res.size());
    assertEquals("Wrong number of facets!", 2, res.get(0).getNumValidDescendants());
    
    Iterable<? extends FacetResultNode> it = res.get(0).getFacetResultNode().subResults;
    int i = 0;
    for (FacetResultNode fResNode : it) {
      assertEquals("Wrong result for facet "+fResNode.label, expectedResults[i++], fResNode.value, 1E-5);
    }
  }
  
}
