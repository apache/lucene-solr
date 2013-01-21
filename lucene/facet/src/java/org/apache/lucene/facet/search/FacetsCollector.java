package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;

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
 * A {@link Collector} which executes faceted search and computes the weight of
 * requested facets. To get the facet results you should call
 * {@link #getFacetResults()}.
 * {@link #create(FacetSearchParams, IndexReader, TaxonomyReader)} returns the
 * most optimized {@link FacetsCollector} for the given parameters.
 * 
 * @lucene.experimental
 */
public abstract class FacetsCollector extends Collector {
  
  /**
   * Returns the most optimized {@link FacetsCollector} for the given search
   * parameters. The returned {@link FacetsCollector} is guaranteed to satisfy
   * the requested parameters.
   */
  public static FacetsCollector create(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader) {
    if (CountingFacetsCollector.assertParams(fsp) == null) {
      return new CountingFacetsCollector(fsp, taxoReader);
    }
    
    return new StandardFacetsCollector(fsp, indexReader, taxoReader);
  }
  
  /**
   * Returns a {@link FacetResult} per {@link FacetRequest} set in
   * {@link FacetSearchParams}. Note that if one of the {@link FacetRequest
   * requests} is for a {@link CategoryPath} that does not exist in the taxonomy,
   * no matching {@link FacetResult} will be returned.
   */
  public abstract List<FacetResult> getFacetResults() throws IOException;

}
