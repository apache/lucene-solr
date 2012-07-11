package org.apache.lucene.facet.search.params.association;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.aggregator.Aggregator;
import org.apache.lucene.facet.search.aggregator.association.AssociationFloatSumAggregator;
import org.apache.lucene.facet.search.params.FacetRequest;
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
 * Facet request for weighting facets according to their float association by
 * summing the association values.
 * 
 * @lucene.experimental
 */
public class AssociationFloatSumFacetRequest extends FacetRequest {

  /**
   * Create a float association facet request for a given node in the
   * taxonomy.
   */
  public AssociationFloatSumFacetRequest(CategoryPath path, int num) {
    super(path, num);
  }

  @Override
  public Aggregator createAggregator(boolean useComplements,
                                      FacetArrays arrays, IndexReader reader,
                                      TaxonomyReader taxonomy) throws IOException {
    assert !useComplements : "complements are not supported by this FacetRequest";
    return new AssociationFloatSumAggregator(reader, arrays.getFloatArray());
  }

  @Override
  public double getValueOf(FacetArrays arrays, int ordinal) {
    return arrays.getFloatArray()[ordinal];
  }

  @Override
  public boolean supportsComplements() {
    return false;
  }
  
  @Override
  public boolean requireDocumentScore() {
    return false;
  }
  
}
