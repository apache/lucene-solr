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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.analytics.function.ExpressionCalculator;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.common.util.NamedList;

/**
 * A facet that takes in multiple ValueFacet expressions and does analytics calculations over each dimension given.
 */
public class PivotFacet extends AnalyticsFacet implements StreamingFacet {
  private final PivotHead<?> pivotHead;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public PivotFacet(String name, PivotNode<?> topPivot) {
    super(name);
    this.pivotHead = new PivotHead(topPivot);
  }

  @Override
  public void setReductionCollectionManager(ReductionCollectionManager collectionManager) {
    pivotHead.setReductionCollectionManager(collectionManager);
  }

  @Override
  public void setExpressionCalculator(ExpressionCalculator expressionCalculator) {
    pivotHead.setExpressionCalculator(expressionCalculator);
  }

  @Override
  public void addFacetValueCollectionTargets() {
    pivotHead.addFacetValueCollectionTargets();
  }

  @Override
  public void importShardData(DataInput input) throws IOException {
    pivotHead.importShardData(input);
  }

  @Override
  public void exportShardData(DataOutput output) throws IOException {
    pivotHead.exportShardData(output);
  }

  @Override
  public NamedList<Object> createOldResponse() {
    return new NamedList<>();
  }

  @Override
  public Iterable<Map<String,Object>> createResponse() {
    return pivotHead.createResponse();
  }
}
/**
 * Typed Pivot class that stores the overall Pivot data and head of the Pivot node chain.
 *
 * This class exists so that the {@link PivotFacet} class doesn't have to be typed ( {@code <T>} ).
 */
class PivotHead<T> implements StreamingFacet {
  private final PivotNode<T> topPivot;
  private final Map<String, T> pivotValues;

  public PivotHead(PivotNode<T> topPivot) {
    this.topPivot = topPivot;
    this.pivotValues = new HashMap<>();
  }

  public void setReductionCollectionManager(ReductionCollectionManager collectionManager) {
    topPivot.setReductionCollectionManager(collectionManager);
  }

  public void setExpressionCalculator(ExpressionCalculator expressionCalculator) {
    topPivot.setExpressionCalculator(expressionCalculator);
  }

  @Override
  public void addFacetValueCollectionTargets() {
    topPivot.addFacetValueCollectionTargets(pivotValues);
  }

  public void importShardData(DataInput input) throws IOException {
    topPivot.importPivot(input, pivotValues);
  }

  public void exportShardData(DataOutput output) throws IOException {
    topPivot.exportPivot(output, pivotValues);
  }

  public Iterable<Map<String,Object>> createResponse() {
    return topPivot.getPivotedResponse(pivotValues);
  }
}