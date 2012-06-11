package org.apache.lucene.facet.example;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.example.ExampleResult;
import org.apache.lucene.facet.example.multiCL.MultiCLMain;
import org.apache.lucene.facet.search.results.FacetResult;
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
 * Test that the multi-category list example works as expected. This test helps
 * to verify that examples code is alive!
 */
public class TestMultiCLExample extends LuceneTestCase {

  @Test
  public void testMulti() throws Exception {
    ExampleResult res = new MultiCLMain().runSample();
    assertCorrectMultiResults(res);
  }

  public static void assertCorrectMultiResults(ExampleResult exampleResults)
      throws Exception {
    List<FacetResult> results = exampleResults.getFacetResults();
    FacetResult result = results.get(0);
    assertNotNull("Result should not be null", result);
    assertEquals("Invalid label", "5", result.getFacetResultNode()
        .getLabel().toString());
    assertEquals("Invalid value", 2.0, result.getFacetResultNode()
        .getValue(), 0.0);
    assertEquals("Invalid # of subresults", 3, result.getFacetResultNode()
        .getNumSubResults());

    Iterator<? extends FacetResultNode> subResults = result
        .getFacetResultNode().getSubResults().iterator();
    FacetResultNode sub = subResults.next();
    assertEquals("Invalid subresult value", 1.0, sub.getValue(), 0.0);
    assertEquals("Invalid subresult label", "5/2", sub.getLabel()
        .toString());
    sub = subResults.next();
    assertEquals("Invalid subresult value", 1.0, sub.getValue(), 0.0);
    assertEquals("Invalid subresult label", "5/7", sub.getLabel()
        .toString());
    sub = subResults.next();
    assertEquals("Invalid subresult value", 1.0, sub.getValue(), 0.0);
    assertEquals("Invalid subresult label", "5/5", sub.getLabel()
        .toString());

    result = results.get(1);
    assertNotNull("Result should not be null", result);
    assertEquals("Invalid label", "5/5", result.getFacetResultNode()
        .getLabel().toString());
    assertEquals("Invalid value", 1,
        result.getFacetResultNode().getValue(), 0.0);
    assertEquals("Invalid number of subresults", 0, result
        .getFacetResultNode().getNumSubResults());

    result = results.get(2);
    assertNotNull("Result should not be null", result);
    assertEquals("Invalid label", "6/2", result.getFacetResultNode()
        .getLabel().toString());
    assertEquals("Invalid value", 1,
        result.getFacetResultNode().getValue(), 0.0);
    assertEquals("Invalid number of subresults", 0, result
        .getFacetResultNode().getNumSubResults());

  }

}
