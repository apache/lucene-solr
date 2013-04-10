package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.PriorityQueue;

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
 * A {@link DepthOneFacetResultsHandler} which fills the categories values from
 * {@link FacetArrays#getIntArray()}.
 * 
 * @lucene.experimental
 */
public final class IntFacetResultsHandler extends DepthOneFacetResultsHandler {
  
  private final int[] values;
  
  public IntFacetResultsHandler(TaxonomyReader taxonomyReader, FacetRequest facetRequest, FacetArrays facetArrays) {
    super(taxonomyReader, facetRequest, facetArrays);
    this.values = facetArrays.getIntArray();
  }
  
  @Override
  protected final double valueOf(int ordinal) {
    return values[ordinal];
  }
  
  @Override
  protected final int addSiblings(int ordinal, int[] siblings, PriorityQueue<FacetResultNode> pq) {
    FacetResultNode top = pq.top();
    int numResults = 0;
    while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      int value = values[ordinal];
      if (value > 0) {
        ++numResults;
        if (value > top.value) {
          top.value = value;
          top.ordinal = ordinal;
          top = pq.updateTop();
        }
      }
      ordinal = siblings[ordinal];
    }
    return numResults;
  }
  
  @Override
  protected final void addSiblings(int ordinal, int[] siblings, ArrayList<FacetResultNode> nodes) throws IOException {
    while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      int value = values[ordinal];
      if (value > 0) {
        FacetResultNode node = new FacetResultNode(ordinal, value);
        node.label = taxonomyReader.getPath(ordinal);
        nodes.add(node);
      }
      ordinal = siblings[ordinal];
    }
  }
  
}
