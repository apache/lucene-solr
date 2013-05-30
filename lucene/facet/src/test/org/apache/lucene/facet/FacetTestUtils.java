package org.apache.lucene.facet;

import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;

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

public class FacetTestUtils {

  public static String toSimpleString(FacetResult fr) {
    StringBuilder sb = new StringBuilder();
    toSimpleString(fr.getFacetRequest().categoryPath.length, 0, sb, fr.getFacetResultNode(), "");
    return sb.toString();
  }
  
  private static void toSimpleString(int startLength, int depth, StringBuilder sb, FacetResultNode node, String indent) {
    sb.append(indent + node.label.components[startLength+depth-1] + " (" + (int) node.value + ")\n");
    for (FacetResultNode childNode : node.subResults) {
      toSimpleString(startLength, depth + 1, sb, childNode, indent + "  ");
    }
  }

}
