package org.apache.lucene.demo.facet;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.demo.facet.ExampleResult;
import org.apache.lucene.demo.facet.multiCL.MultiCLMain;
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

  public static void assertCorrectMultiResults(ExampleResult exampleResults) {
    List<FacetResult> results = exampleResults.getFacetResults();
    FacetResult result = results.get(0);
    assertNotNull("Result should not be null", result);
    FacetResultNode node = result.getFacetResultNode();
    assertEquals("Invalid label", "5", node.label.toString());
    assertEquals("Invalid # of subresults", 3, node.subResults.size());

    Iterator<? extends FacetResultNode> subResults = node.subResults.iterator();
    FacetResultNode sub = subResults.next();
    assertEquals("Invalid subresult value", 1.0, sub.value, 0.0);
    assertEquals("Invalid subresult label", "5/2", sub.label.toString());
    sub = subResults.next();
    assertEquals("Invalid subresult value", 1.0, sub.value, 0.0);
    assertEquals("Invalid subresult label", "5/7", sub.label.toString());
    sub = subResults.next();
    assertEquals("Invalid subresult value", 1.0, sub.value, 0.0);
    assertEquals("Invalid subresult label", "5/5", sub.label.toString());

    result = results.get(1);
    node = result.getFacetResultNode();
    assertNotNull("Result should not be null", result);
    assertEquals("Invalid label", "5/5", node.label.toString());
    assertEquals("Invalid value", 1, node.value, 0.0);
    assertEquals("Invalid number of subresults", 0, node.subResults.size());

    result = results.get(2);
    node = result.getFacetResultNode();
    assertNotNull("Result should not be null", result);
    assertEquals("Invalid label", "6/2", node.label.toString());
    assertEquals("Invalid value", 1, node.value, 0.0);
    assertEquals("Invalid number of subresults", 0, node.subResults.size());

  }

}
