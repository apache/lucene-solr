package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.facet.index.CategoryContainer;
import org.apache.lucene.facet.taxonomy.CategoryPath;

/**
 * Simple implementation of a random facet source
 * <p>
 * Supports the following parameters:
 * <ul>
 * <li><b>rand.seed</b> - defines the seed to initialize Random with (default: <b>13</b>).
 * <li><b>max.doc.facets</b> - maximal #facets per doc (default: <b>10</b>).
 *    Actual number of facets in a certain doc would be anything between 1 and that number.
 * <li><b>max.facet.depth</b> - maximal #components in a facet (default: <b>3</b>).
 *    Actual number of components in a certain facet would be anything between 1 and that number.
 * </ul>
 */
public class RandomFacetSource extends FacetSource {

  Random random;
  
  private int maxDocFacets = 10;
  private int maxFacetDepth = 3;
  private int maxValue = maxDocFacets * maxFacetDepth;
  
  @Override
  public CategoryContainer getNextFacets(CategoryContainer facets) throws NoMoreDataException, IOException {
    if (facets == null) {
      facets = new CategoryContainer();
    } else {
      facets.clear();
    }
    int numFacets = 1 + random.nextInt(maxDocFacets-1); // at least one facet to each doc
    for (int i=0; i<numFacets; i++) {
      CategoryPath cp = new CategoryPath();
      int depth = 1 + random.nextInt(maxFacetDepth-1); // depth 0 is not useful
      for (int k=0; k<depth; k++) {
        cp.add(Integer.toString(random.nextInt(maxValue)));
        addItem();
      }
      facets.addCategory(cp);
      addBytes(cp.toString().length()); // very rough approximation
    }
    return facets;
  }

  @Override
  public void close() throws IOException {
    // nothing to do here
  }

  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    random = new Random(config.get("rand.seed", 13));
    maxDocFacets = config.get("max.doc.facets", 200);
    maxFacetDepth = config.get("max.facet.depth", 10);
    maxValue = maxDocFacets * maxFacetDepth;
  }
}
