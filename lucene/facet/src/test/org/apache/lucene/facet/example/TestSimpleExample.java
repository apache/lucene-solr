package org.apache.lucene.facet.example;

import java.util.Iterator;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.example.ExampleResult;
import org.apache.lucene.facet.example.simple.SimpleMain;
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
 * Test that the simple example works as expected. This test helps to verify
 * that examples code is alive!
 */
public class TestSimpleExample extends LuceneTestCase {

  @Test
  public void testSimple() throws Exception {
    ExampleResult res = new SimpleMain().runSimple();
    assertNotNull("Null result!", res);
    assertNotNull("Null facet result!", res.getFacetResults());
    assertEquals("Wrong number of results!",1, res.getFacetResults().size());
    assertEquals("Wrong number of facets!",3, res.getFacetResults().get(0).getNumValidDescendants());
  }

  /**
   * In drill down test we are drilling down to a facet that appears in a single document.
   * As result, facets that without drill down got count of 2 will now get a count of 1. 
   */
  @Test
  public void testDrillDown() throws Exception {
    ExampleResult res = new SimpleMain().runDrillDown();
    assertNotNull("Null result!", res);
    assertNotNull("Null facet result!", res.getFacetResults());
    assertEquals("Wrong number of results!",1, res.getFacetResults().size());
    
    // drill down facet appears in only 1 doc, and that doc has only 2 facets  
    FacetResult facetResult = res.getFacetResults().get(0);
    assertEquals("Wrong number of facets!",2, facetResult.getNumValidDescendants());
    
    Iterator<? extends FacetResultNode> resIterator = facetResult.getFacetResultNode().getSubResults().iterator();
    assertTrue("Too few results", resIterator.hasNext());
    assertEquals("wrong count for first result out of 2", 1, (int)resIterator.next().getValue());
    assertTrue("Too few results", resIterator.hasNext());
    assertEquals("wrong count for second result out of 2", 1, (int)resIterator.next().getValue());
    assertFalse("Too many results!", resIterator.hasNext());
  }
}
