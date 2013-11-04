package org.apache.lucene.demo.facet;

import java.util.List;
import java.util.Locale;

import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
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

@SuppressCodecs("Lucene3x")
public class TestExpressionAggregationFacetsExample extends LuceneTestCase {

  private static String toSimpleString(FacetResult fr) {
    StringBuilder sb = new StringBuilder();
    toSimpleString(fr.getFacetRequest().categoryPath.length, 0, sb, fr.getFacetResultNode(), "");
    return sb.toString();
  }
  
  private static void toSimpleString(int startLength, int depth, StringBuilder sb, FacetResultNode node, String indent) {
    sb.append(String.format(Locale.ROOT, "%s%s (%.3f)\n", indent, node.label.components[startLength + depth - 1], node.value));
    for (FacetResultNode childNode : node.subResults) {
      toSimpleString(startLength, depth + 1, sb, childNode, indent + "  ");
    }
  }

  @Test
  public void testSimple() throws Exception {
    List<FacetResult> facetResults = new ExpressionAggregationFacetsExample().runSearch();
    assertEquals("A (0.000)\n  B (2.236)\n  C (1.732)\n", toSimpleString(facetResults.get(0)));
  }
  
}
