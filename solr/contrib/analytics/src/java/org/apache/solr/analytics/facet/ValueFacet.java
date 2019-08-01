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
package org.apache.solr.analytics.facet;

import java.util.function.Consumer;

import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.value.StringValueStream;

/**
 * A facet that breaks up data by the values of a mapping expression or field.
 * The mapping expression must be castable to a {@link StringValueStream}.
 */
public class ValueFacet extends SortableFacet implements StreamingFacet, Consumer<String> {
  private StringValueStream expression;

  public ValueFacet(String name, StringValueStream expression) {
    super(name);
    this.expression = expression;
  }

  @Override
  public void addFacetValueCollectionTargets() {
    expression.streamStrings(this);
  }

  @Override
  public void accept(String t) {
    ReductionDataCollection collection = reductionData.get(t);
    if (collection == null) {
      collection = collectionManager.newDataCollectionTarget();
      reductionData.put(t, collection);
    } else {
      collectionManager.addCollectTarget(collection);
    }
  }

  /**
   * Get the expression used to create the facet values.
   *
   * @return a string mapping expression
   */
  public StringValueStream getExpression() {
    return expression;
  }
}