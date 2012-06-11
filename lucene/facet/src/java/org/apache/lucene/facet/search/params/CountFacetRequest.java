package org.apache.lucene.facet.search.params;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.aggregator.Aggregator;
import org.apache.lucene.facet.search.aggregator.ComplementCountingAggregator;
import org.apache.lucene.facet.search.aggregator.CountingAggregator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * Facet request for counting facets.
 * 
 * @lucene.experimental
 */
public class CountFacetRequest extends FacetRequest {

  /**
   * Create a count facet request for a given node in the taxonomy.
   * 
   * @param path category path of the category of interest.
   * @param num number of child categories for which count info is requeted.
   *        reqiested. Default implementation will find <b>top</b> categories, -
   *        this behavior can be overridden by overriding
   *        {@link #createFacetResultsHandler(TaxonomyReader)}.
   */
  public CountFacetRequest(CategoryPath path, int num) {
    super(path, num);
  }

  @Override
  public Aggregator createAggregator(boolean useComplements,
                                      FacetArrays arrays, IndexReader reader,
                                      TaxonomyReader taxonomy) {
    // we rely on that, if needed, result is cleared by arrays!
    int[] a = arrays.getIntArray();
    if (useComplements) {
      return new ComplementCountingAggregator(a);
    }
    return new CountingAggregator(a);
  }

  @Override
  public double getValueOf(FacetArrays arrays, int ordinal) {
    return arrays.getIntArray()[ordinal];
  }

  @Override
  public boolean supportsComplements() {
    return true;
  }
  
  @Override
  public boolean requireDocumentScore() {
    return false;
  }
}
