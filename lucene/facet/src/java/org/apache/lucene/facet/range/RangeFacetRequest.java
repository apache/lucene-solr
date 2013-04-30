package org.apache.lucene.facet.range;

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

import java.util.List;

import org.apache.lucene.facet.search.Aggregator;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

/**
 * Facet request for dynamic ranges based on a
 * NumericDocValues field.  This does not use the taxonomy
 * index nor any indexed facet values.
 * 
 * @lucene.experimental
 */
public class RangeFacetRequest<T extends Range> extends FacetRequest {

  public final Range[] ranges;

  public RangeFacetRequest(String field, T...ranges) {
    super(new CategoryPath(field), 1);
    this.ranges = ranges;
  }

  @SuppressWarnings("unchecked")
  public RangeFacetRequest(String field, List<T> ranges) {
    this(field, (T[]) ranges.toArray(new Range[ranges.size()]));
  }

  @Override
  public Aggregator createAggregator(boolean useComplements, FacetArrays arrays, TaxonomyReader taxonomy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getValueOf(FacetArrays arrays, int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FacetArraysSource getFacetArraysSource() {
    throw new UnsupportedOperationException();
  }
  
}
