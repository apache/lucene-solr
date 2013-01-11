package org.apache.lucene.facet.example;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.example.ExampleResult;
import org.apache.lucene.facet.example.association.CategoryAssociationsMain;
import org.apache.lucene.facet.search.results.FacetResultNode;

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

/**
 * Test that the association example works as expected. This test helps to
 * verify that examples code is alive!
 */
public class TestAssociationExample extends LuceneTestCase {
  
  private static final double[] EXPECTED_INT_SUM_RESULTS = { 4, 2};
  private static final double[] EXPECTED_FLOAT_SUM_RESULTS = { 1.62, 0.34};

  @Test
  public void testAssociationExamples() throws Exception {
    assertExampleResult(new CategoryAssociationsMain().runSumIntAssociationSample(), EXPECTED_INT_SUM_RESULTS);
    assertExampleResult(new CategoryAssociationsMain().runSumFloatAssociationSample(), EXPECTED_FLOAT_SUM_RESULTS);
  }

  private void assertExampleResult(ExampleResult res, double[] expectedResults) {
    assertNotNull("Null result!", res);
    assertNotNull("Null facet result!", res.getFacetResults());
    assertEquals("Wrong number of results!", 1, res.getFacetResults().size());
    assertEquals("Wrong number of facets!", 2, res.getFacetResults().get(0).getNumValidDescendants());
    
    Iterable<? extends FacetResultNode> it = res.getFacetResults().get(0).getFacetResultNode().getSubResults();
    int i = 0;
    for (FacetResultNode fResNode : it) {
      assertEquals("Wrong result for facet "+fResNode.getLabel(), expectedResults[i++], fResNode.getValue(), 1E-5);
    }
  }
  
}
