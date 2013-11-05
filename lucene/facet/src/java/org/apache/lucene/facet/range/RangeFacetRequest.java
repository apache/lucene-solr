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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;

/**
 * A {@link FacetRequest} for dynamic ranges based on a {@link NumericDocValues}
 * field or {@link ValueSource}. This does not use the taxonomy index nor any
 * indexed facet values.
 * 
 * @lucene.experimental
 */
public class RangeFacetRequest<T extends Range> extends FacetRequest {

  public final Range[] ranges;
  public final String label;
  
  private final ValueSource valueSource;
  
  /**
   * Create a request for the given ranges over the specified
   * {@link NumericDocValues} field. The field will be used to as the root's
   * {@link FacetResultNode} label.
   */
  @SuppressWarnings("unchecked")
  public RangeFacetRequest(String field, T...ranges) {
    this(field, new LongFieldSource(field), ranges);
  }

  /**
   * Create a request for the given ranges over the specified
   * {@link NumericDocValues} field. The field will be used to as the root's
   * {@link FacetResultNode} label.
   */
  @SuppressWarnings("unchecked")
  public RangeFacetRequest(String field, List<T> ranges) {
    this(field, (T[]) ranges.toArray(new Range[ranges.size()]));
  }
  
  /**
   * Create a request for the given ranges over the specified
   * {@link ValueSource}. The label will be used to as the root's
   * {@link FacetResultNode} label.
   */
  @SuppressWarnings("unchecked")
  public RangeFacetRequest(String label, ValueSource valueSource, T...ranges) {
    super(new CategoryPath(label), 1);
    this.ranges = ranges;
    this.valueSource = valueSource;
    this.label = label;
  }
  
  /**
   * Create a request for the given ranges over the specified
   * {@link ValueSource}. The label will be used to as the root's
   * {@link FacetResultNode} label.
   */
  @SuppressWarnings("unchecked")
  public RangeFacetRequest(String label, ValueSource valueSource, List<T> ranges) {
    this(label, valueSource, (T[]) ranges.toArray(new Range[ranges.size()]));
  }

  /**
   * Returns the {@link FunctionValues} for the given
   * {@link AtomicReaderContext}. If the request was created over a
   * {@link NumericDocValues} field, the respective {@link NumericDocValues} is
   * returned.
   */
  public FunctionValues getValues(AtomicReaderContext context) throws IOException {
    return valueSource.getValues(Collections.emptyMap(), context);
  }
  
  @Override
  public FacetsAggregator createFacetsAggregator(FacetIndexingParams fip) {
    throw new UnsupportedOperationException("this FacetRequest does not support categories aggregation and only works with RangeAccumulator");
  }
  
}
