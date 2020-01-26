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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.solr.analytics.AnalyticsDriver;
import org.apache.solr.analytics.function.ExpressionCalculator;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.function.reduction.data.ReductionData;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.analytics.value.StringValueStream;

/**
 * Representation of one layer of a Pivot Facet. A PivotFacet node is individually sortable,
 * and is collected during the streaming phase of the {@link AnalyticsDriver}.
 */
public abstract class PivotNode<T> extends SortableFacet implements Consumer<String> {
  private StringValueStream expression;
  protected Map<String,T> currentPivot;

  public PivotNode(String name, StringValueStream expression) {
    super(name);
    this.expression = expression;
  }

  /**
   * Determine which facet values match the current document. Add the {@link ReductionDataCollection}s of the relevant facet values
   * to the targets of the streaming {@link ReductionCollectionManager} so that they are updated with the current document's data.
   */
  public void addFacetValueCollectionTargets(Map<String,T> pivot) {
    currentPivot = pivot;
    expression.streamStrings(this);
  }

  /**
   * Import the shard data from a bit-stream for the given pivot, exported by the {@link #exportPivot} method
   * in the each of the collection's shards.
   *
   * @param input The bit-stream to import the data from
   * @param pivot the values for this pivot node and the pivot children (if they exist)
   * @throws IOException if an exception occurs while reading from the {@link DataInput}
   */
  public void importPivot(DataInput input, Map<String,T> pivot) throws IOException {
    int size = input.readInt();
    currentPivot = pivot;
    for (int i = 0; i < size; ++i) {
      importPivotValue(input, input.readUTF());
    }
  }
  /**
   * Import the next pivot value's set of {@link ReductionData} and children's {@link ReductionData} if they exist.
   *
   * @param input the bit-stream to import the reduction data from
   * @param pivotValue the next pivot value
   * @throws IOException if an exception occurs while reading from the input
   */
  protected abstract void importPivotValue(DataInput input, String pivotValue) throws IOException;

  /**
   * Export the shard data through a bit-stream for the given pivot,
   * to be imported by the {@link #importPivot} method in the originating shard.
   *
   * @param output The bit-stream to output the data through
   * @param pivot the values for this pivot node and the pivot children (if they exist)
   * @throws IOException if an exception occurs while writing to the {@link DataOutput}
   */
  public void exportPivot(DataOutput output, Map<String,T> pivot) throws IOException {
    output.writeInt(pivot.size());
    for (Map.Entry<String, T> entry : pivot.entrySet()) {
      output.writeUTF(entry.getKey());
      exportPivotValue(output, entry.getValue());
    }
  }
  /**
   * Export the given pivot data, containing {@link ReductionData} and pivot children if they exist.
   *
   * @param output the bit-stream to output the reduction data to
   * @param pivotData the next pivot value data
   * @throws IOException if an exception occurs while reading from the input
   */
  protected abstract void exportPivotValue(DataOutput output, T pivotData) throws IOException;

  /**
   * Create the response of the facet to be returned in the overall analytics response.
   *
   * @param pivot the pivot to create a response for
   * @return the response of the facet
   */
  public abstract Iterable<Map<String,Object>> getPivotedResponse(Map<String,T> pivot);

  /**
   * A pivot node that has no pivot children.
   */
  public static class PivotLeaf extends PivotNode<ReductionDataCollection> {

    public PivotLeaf(String name, StringValueStream expression) {
      super(name, expression);
    }

    @Override
    public void accept(String pivotValue) {
      ReductionDataCollection collection = currentPivot.get(pivotValue);
      if (collection == null) {
        collection = collectionManager.newDataCollectionTarget();
        currentPivot.put(pivotValue, collection);
      } else {
        collectionManager.addCollectTarget(collection);
      }
    }

    @Override
    protected void importPivotValue(DataInput input, String pivotValue) throws IOException {
      ReductionDataCollection dataCollection = currentPivot.get(pivotValue);
      if (dataCollection == null) {
        currentPivot.put(pivotValue, collectionManager.newDataCollectionIO());
      } else {
        collectionManager.prepareReductionDataIO(dataCollection);
      }
      collectionManager.mergeData();
    }

    @Override
    protected void exportPivotValue(DataOutput output, ReductionDataCollection pivotData) throws IOException {
      collectionManager.prepareReductionDataIO(pivotData);
      collectionManager.exportData();
    }

    @Override
    public Iterable<Map<String,Object>> getPivotedResponse(Map<String,ReductionDataCollection> pivot) {
      final List<FacetBucket> facetResults = new ArrayList<>();
      pivot.forEach((facetVal, dataCol) -> {
        collectionManager.setData(dataCol);
        facetResults.add(new FacetBucket(facetVal,expressionCalculator.getResults()));
      });

      Iterable<FacetBucket> facetResultsIter = applyOptions(facetResults);
      final LinkedList<Map<String,Object>> results = new LinkedList<>();
      // Export each expression in the bucket.
      for (FacetBucket bucket : facetResultsIter) {
        Map<String, Object> bucketMap = new HashMap<>();
        bucketMap.put(AnalyticsResponseHeadings.PIVOT_NAME, name);
        bucketMap.put(AnalyticsResponseHeadings.FACET_VALUE, bucket.getFacetValue());
        bucketMap.put(AnalyticsResponseHeadings.RESULTS, bucket.getResults());
        results.add(bucketMap);
      }
      return results;
    }
  }

  /**
   * A pivot node that has pivot children.
   */
  public static class PivotBranch<T> extends PivotNode<PivotBranch.PivotDataPair<T>> {
    private final PivotNode<T> childPivot;
    public PivotBranch(String name, StringValueStream expression, PivotNode<T> childPivot) {
      super(name, expression);
      this.childPivot = childPivot;
    }

    @Override
    public void setReductionCollectionManager(ReductionCollectionManager collectionManager) {
      super.setReductionCollectionManager(collectionManager);
      childPivot.setReductionCollectionManager(collectionManager);
    }

    @Override
    public void setExpressionCalculator(ExpressionCalculator expressionCalculator) {
      super.setExpressionCalculator(expressionCalculator);
      childPivot.setExpressionCalculator(expressionCalculator);
    }

    @Override
    public void accept(String pivotValue) {
      PivotDataPair<T> pivotData = currentPivot.get(pivotValue);
      if (pivotData == null) {
        pivotData = new PivotDataPair<>();
        pivotData.childPivots = new HashMap<>();
        pivotData.pivotReduction = collectionManager.newDataCollectionTarget();
        currentPivot.put(pivotValue, pivotData);
      } else {
        collectionManager.addCollectTarget(pivotData.pivotReduction);
      }
      childPivot.addFacetValueCollectionTargets(pivotData.childPivots);
    }

    @Override
    protected void importPivotValue(DataInput input, String pivotValue) throws IOException {
      PivotDataPair<T> pivotData = currentPivot.get(pivotValue);
      if (pivotData == null) {
        pivotData = new PivotDataPair<>();
        pivotData.childPivots = new HashMap<>();
        pivotData.pivotReduction = collectionManager.newDataCollectionIO();
        currentPivot.put(pivotValue, pivotData);
      } else {
        collectionManager.prepareReductionDataIO(pivotData.pivotReduction);
      }
      collectionManager.mergeData();
      childPivot.importPivot(input, pivotData.childPivots);
    }

    @Override
    protected void exportPivotValue(DataOutput output, PivotDataPair<T> pivotData) throws IOException {
      collectionManager.prepareReductionDataIO(pivotData.pivotReduction);
      collectionManager.exportData();

      childPivot.exportPivot(output, pivotData.childPivots);
    }

    @Override
    public Iterable<Map<String,Object>> getPivotedResponse(Map<String,PivotDataPair<T>> pivot) {
      final List<FacetBucket> facetResults = new ArrayList<>();
      pivot.forEach((facetVal, dataPair) -> {
        collectionManager.setData(dataPair.pivotReduction);
        facetResults.add(new FacetBucket(facetVal,expressionCalculator.getResults()));
      });

      Iterable<FacetBucket> facetResultsIter = applyOptions(facetResults);
      final LinkedList<Map<String,Object>> results = new LinkedList<>();
      // Export each expression in the bucket.
      for (FacetBucket bucket : facetResultsIter) {
        Map<String, Object> bucketMap = new HashMap<>();
        bucketMap.put(AnalyticsResponseHeadings.PIVOT_NAME, name);
        bucketMap.put(AnalyticsResponseHeadings.FACET_VALUE, bucket.getFacetValue());
        bucketMap.put(AnalyticsResponseHeadings.RESULTS, bucket.getResults());
        bucketMap.put(AnalyticsResponseHeadings.PIVOT_CHILDREN, childPivot.getPivotedResponse(pivot.get(bucket.getFacetValue()).childPivots));
        results.add(bucketMap);
      }
      return results;
    }

    /**
     * Contains pivot data for {@link PivotNode.PivotBranch} classes.
     */
    protected static class PivotDataPair<T> {
      ReductionDataCollection pivotReduction;
      Map<String,T> childPivots;
    }
  }
}
