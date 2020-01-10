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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.solr.analytics.function.ExpressionCalculator;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.function.reduction.data.ReductionData;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.common.util.NamedList;

/**
 * An abstract Facet to break up Analytics data over.
 */
public abstract class AnalyticsFacet {
  protected final Map<String,ReductionDataCollection> reductionData;
  protected ReductionCollectionManager collectionManager;
  protected ExpressionCalculator expressionCalculator;

  protected final String name;

  public AnalyticsFacet(String name) {
    this.reductionData = new LinkedHashMap<>();
    this.name = name;
  }

  /**
   * Set the {@link ReductionCollectionManager} that manages the collection of the expressions
   * calculated with this facet.
   *
   * @param collectionManager The manager for relevant expressions
   */
  public void setReductionCollectionManager(ReductionCollectionManager collectionManager) {
    this.collectionManager = collectionManager;
  }

  /**
   * Set the {@link ExpressionCalculator} that calculates the collection of the expressions
   * requested for this facet.
   *
   * @param expressionCalculator The calculator for relevant expressions
   */
  public void setExpressionCalculator(ExpressionCalculator expressionCalculator) {
    this.expressionCalculator = expressionCalculator;
  }

  /**
   * Import the shard data from a bit-stream, exported by the {@link #exportShardData} method
   * in the each of the collection's shards.
   *
   * @param input The bit-stream to import the data from
   * @throws IOException if an exception occurs while reading from the {@link DataInput}
   */
  public void importShardData(DataInput input) throws IOException {
    int size = input.readInt();
    for (int i = 0; i < size; ++i) {
      importFacetValue(input, input.readUTF());
    }
  }
  /**
   * Import the next facet value's set of {@link ReductionData}.
   *
   * @param input the bit-stream to import the reduction data from
   * @param facetValue the next facet value
   * @throws IOException if an exception occurs while reading from the input
   */
  protected void importFacetValue(DataInput input, String facetValue) throws IOException {
    ReductionDataCollection dataCollection = reductionData.get(facetValue);
    if (dataCollection == null) {
      reductionData.put(facetValue, collectionManager.newDataCollectionIO());
    } else {
      collectionManager.prepareReductionDataIO(dataCollection);
    }

    collectionManager.mergeData();
  }

  /**
   * Export the shard data through a bit-stream, to be imported by the {@link #importShardData} method
   * in the originating shard.
   *
   * @param output The bit-stream to output the data through
   * @throws IOException if an exception occurs while writing to the {@link DataOutput}
   */
  public void exportShardData(DataOutput output) throws IOException {
    output.writeInt(reductionData.size());
    for (String facetValue : reductionData.keySet()) {
      exportFacetValue(output, facetValue);
    }
  }
  /**
   * Export the next facet value's set of {@link ReductionData}.
   *
   * @param output the bit-stream to output the reduction data to
   * @param facetValue the next facet value
   * @throws IOException if an exception occurs while reading from the input
   */
  protected void exportFacetValue(DataOutput output, String facetValue) throws IOException {
    output.writeUTF(facetValue);

    collectionManager.prepareReductionDataIO(reductionData.get(facetValue));
    collectionManager.exportData();
  }

  /**
   * Create the old olap-style response of the facet to be returned in the overall analytics response.
   *
   * @return the response of the facet
   */
  public NamedList<Object> createOldResponse() {
    NamedList<Object> nl = new NamedList<>();
    reductionData.forEach((facetVal, dataCol) -> {
      collectionManager.setData(dataCol);
      nl.add(facetVal, new NamedList<>(expressionCalculator.getResults()));
    });
    return nl;
  }

  /**
   * Create the response of the facet to be returned in the overall analytics response.
   *
   * @return the response of the facet
   */
  public Iterable<Map<String,Object>> createResponse() {
    LinkedList<Map<String,Object>> list = new LinkedList<>();
    reductionData.forEach((facetVal, dataCol) -> {
      Map<String, Object> bucket = new HashMap<>();
      bucket.put(AnalyticsResponseHeadings.FACET_VALUE, facetVal);
      collectionManager.setData(dataCol);
      expressionCalculator.addResults(bucket);
      list.add(bucket);
    });
    return list;
  }

  /**
   * Get the name of the Facet. This is unique for the grouping.
   *
   * @return The name of the Facet
   */
  public String getName() {
    return name;
  }
}