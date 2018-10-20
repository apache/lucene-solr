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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Maps specified dims to provided Facets impls; else, uses
 *  the default Facets impl. */
public class MultiFacets extends Facets {
  private final Map<String,Facets> dimToFacets;
  private final Facets defaultFacets;

  /** Create this, with no default {@link Facets}. */
  public MultiFacets(Map<String,Facets> dimToFacets) {
    this(dimToFacets, null);
  }

  /** Create this, with the specified default {@link Facets}
   *  for fields not included in {@code dimToFacets}. */
  public MultiFacets(Map<String,Facets> dimToFacets, Facets defaultFacets) {
    this.dimToFacets = dimToFacets;
    this.defaultFacets = defaultFacets;
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    Facets facets = dimToFacets.get(dim);
    if (facets == null) {
      if (defaultFacets == null) {
        throw new IllegalArgumentException("invalid dim \"" + dim + "\"");
      }
      facets = defaultFacets;
    }
    return facets.getTopChildren(topN, dim, path);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    Facets facets = dimToFacets.get(dim);
    if (facets == null) {
      if (defaultFacets == null) {
        throw new IllegalArgumentException("invalid dim \"" + dim + "\"");
      }
      facets = defaultFacets;
    }
    return facets.getSpecificValue(dim, path);
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {

    List<FacetResult> results = new ArrayList<FacetResult>();

    // First add the specific dim's facets:
    for(Map.Entry<String,Facets> ent : dimToFacets.entrySet()) {
      results.add(ent.getValue().getTopChildren(topN, ent.getKey()));
    }

    if (defaultFacets != null) {

      // Then add all default facets as long as we didn't
      // already add that dim:
      for(FacetResult result : defaultFacets.getAllDims(topN)) {
        if (dimToFacets.containsKey(result.dim) == false) {
          results.add(result);
        }
      }
    }

    return results;
  }
}
