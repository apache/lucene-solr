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
package org.apache.lucene.benchmark.byTask.feeds;


import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;

/**
 * Simple implementation of a random facet source
 * <p>
 * Supports the following parameters:
 * <ul>
 * <li><b>rand.seed</b> - defines the seed to initialize {@link Random} with
 * (default: <b>13</b>).
 * <li><b>max.doc.facet.dims</b> - Max number of random dimensions to
 * create (default: <b>5</b>); actual number of dimensions
 * would be anything between 1 and that number.
 * <li><b>max.doc.facets</b> - maximal #facets per doc (default: <b>10</b>).
 * Actual number of facets in a certain doc would be anything between 1 and that
 * number.
 * <li><b>max.facet.depth</b> - maximal #components in a facet (default:
 * <b>3</b>). Actual number of components in a certain facet would be anything
 * between 1 and that number.
 * </ul>
 */
public class RandomFacetSource extends FacetSource {

  private Random random;
  private int maxDocFacets;
  private int maxFacetDepth;
  private int maxDims;
  private int maxValue = maxDocFacets * maxFacetDepth;
  
  @Override
  public void getNextFacets(List<FacetField> facets) throws NoMoreDataException, IOException {
    facets.clear();
    int numFacets = 1 + random.nextInt(maxDocFacets); // at least one facet to each doc
    for (int i = 0; i < numFacets; i++) {
      int depth;
      if (maxFacetDepth == 2) {
        depth = 2;
      } else {
        depth = 2 + random.nextInt(maxFacetDepth-2); // depth < 2 is not useful
      }

      String dim = Integer.toString(random.nextInt(maxDims));
      String[] components = new String[depth-1];
      for (int k = 0; k < depth-1; k++) {
        components[k] = Integer.toString(random.nextInt(maxValue));
        addItem();
      }
      FacetField ff = new FacetField(dim, components);
      facets.add(ff);
      addBytes(ff.toString().length()); // very rough approximation
    }
  }

  @Override
  public void configure(FacetsConfig config) {
    for(int i=0;i<maxDims;i++) {
      config.setHierarchical(Integer.toString(i), true);
      config.setMultiValued(Integer.toString(i), true);
    }
  }

  @Override
  public void close() throws IOException {
    // nothing to do here
  }

  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    random = new Random(config.get("rand.seed", 13));
    maxDocFacets = config.get("max.doc.facets", 10);
    maxDims = config.get("max.doc.facets.dims", 5);
    maxFacetDepth = config.get("max.facet.depth", 3);
    if (maxFacetDepth < 2) {
      throw new IllegalArgumentException("max.facet.depth must be at least 2; got: " + maxFacetDepth);
    }
    maxValue = maxDocFacets * maxFacetDepth;
  }
}
