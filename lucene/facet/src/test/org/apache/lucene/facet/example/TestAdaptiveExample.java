package org.apache.lucene.facet.example;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.example.ExampleResult;
import org.apache.lucene.facet.example.adaptive.AdaptiveMain;

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
 * Test that the adaptive example works as expected. This test helps to verify
 * that examples code is alive!
 */
public class TestAdaptiveExample extends LuceneTestCase {
  
  @Test
  public void testAdaptive () throws Exception {
    ExampleResult res = new AdaptiveMain().runSample();
    assertNotNull("Null result!", res);
    assertNotNull("Null facet result!", res.getFacetResults());
    assertEquals("Wrong number of results!",1, res.getFacetResults().size());
    assertEquals("Wrong number of facets!",3, res.getFacetResults().get(0).getNumValidDescendants());
  }
}
