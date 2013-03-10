package org.apache.lucene.facet.search;

import java.io.IOException;

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
 * Computes the top categories for a given {@link FacetRequest}. 
 * 
 * @lucene.experimental
 */
public abstract class FacetResultsHandler {

  public final TaxonomyReader taxonomyReader;

  public final FacetRequest facetRequest;
  
  protected final FacetArrays facetArrays;

  public FacetResultsHandler(TaxonomyReader taxonomyReader, FacetRequest facetRequest, FacetArrays facetArrays) {
    this.taxonomyReader = taxonomyReader;
    this.facetRequest = facetRequest;
    this.facetArrays = facetArrays;
  }

  /** Computes the {@link FacetResult} for the given {@link FacetArrays}. */
  public abstract FacetResult compute() throws IOException;
  
}
