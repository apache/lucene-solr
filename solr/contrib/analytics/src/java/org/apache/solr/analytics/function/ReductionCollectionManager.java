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
package org.apache.solr.analytics.function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.analytics.function.field.AnalyticsField;
import org.apache.solr.analytics.function.reduction.data.ReductionData;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.stream.reservation.read.ReductionDataReader;
import org.apache.solr.analytics.stream.reservation.write.ReductionDataWriter;
import org.apache.solr.analytics.value.AnalyticsValue;

/**
 * The manager of reduction collection.
 * Contains a group of {@link ReductionDataCollector}s which will be updated together.
 * <p>
 * The manager assumes a non-distributed request. {@link MergingReductionCollectionManager} is used for distributed requests.
 */
public class ReductionCollectionManager {
  protected final ReductionDataCollector<?>[] reductionDataCollectors;
  private final List<ReductionDataReservation<?,?>> reservations;

  private final List<ReductionDataReader<?>> readers;
  private final List<ReductionDataWriter<?>> writers;

  private final Iterable<AnalyticsField> fields;

  public ReductionCollectionManager() {
    this(new ReductionDataCollector<?>[0], new ArrayList<>(0));
  }

  /**
   * Create a Manager to oversee the given {@link ReductionDataCollector}s.
   *
   * @param reductionDataCollectors array of collectors that are collecting over the same set of data
   * @param fields all fields used by the given collectors
   */
  public ReductionCollectionManager(final ReductionDataCollector<?>[] reductionDataCollectors, final Iterable<AnalyticsField> fields) {
    this.reductionDataCollectors = reductionDataCollectors;
    Arrays.sort(reductionDataCollectors, (a,b) -> a.getExpressionStr().compareTo(b.getExpressionStr()));

    reservations = new LinkedList<>();
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].submitReservations(reservation -> reservations.add(reservation));
    }

    this.fields = fields;

    this.readers = new ArrayList<>();
    this.writers = new ArrayList<>();
  }

  /**
   * Return whether or not the manager needs collection done, which is false if no collectors are
   * being managed and true if at least one is.
   *
   * @return true if at least one collector is being managed
   */
  public boolean needsCollection() {
    return reductionDataCollectors.length > 0;
  }

  /**
   * Merge this collection manager with others.
   *
   * @param reductionManagers the collection managers to merge with
   * @return a collection manager that manages the union of data collectors from this class and the given managers
   */
  public ReductionCollectionManager merge(Iterable<ReductionCollectionManager> reductionManagers) {
    HashMap<String,ReductionDataCollector<?>> mergedCollectors = new HashMap<>();
    HashMap<String,AnalyticsField> mergedFields = new HashMap<>();

    for (ReductionDataCollector<?> collector : reductionDataCollectors) {
      mergedCollectors.put(collector.getExpressionStr(), collector);
    }
    fields.forEach( field -> mergedFields.put(field.getExpressionStr(), field) );

    reductionManagers.forEach( manager -> {
      for (ReductionDataCollector<?> collector : manager.reductionDataCollectors) {
        mergedCollectors.put(collector.getExpressionStr(), collector);
      }
      manager.fields.forEach( field -> mergedFields.put(field.getExpressionStr(), field) );
    });
    ReductionDataCollector<?>[] collectors = new ReductionDataCollector<?>[mergedCollectors.size()];
    mergedCollectors.values().toArray(collectors);
    return createNewManager(collectors, mergedFields.values());
  }

  /**
   * Create an {@link ReductionCollectionManager} to manage the given collectors and fields.
   *
   * @param reductionDataCollectors Reduction collectors
   * @param fields fields used by the reductions
   * @return a collection manager
   */
  protected ReductionCollectionManager createNewManager(final ReductionDataCollector<?>[] reductionDataCollectors, final Iterable<AnalyticsField> fields) {
    return new ReductionCollectionManager(reductionDataCollectors,fields);
  }

  /**
   * Get the {@link AnalyticsField}s used in the managed expressions.
   *
   * @return the fields used
   */
  public Iterable<AnalyticsField> getUsedFields() {
    return fields;
  }

  /**
   * Set the context of the readers of the used {@link AnalyticsField}s.
   *
   * @param context the reader context
   * @throws IOException if an error occurs while setting the fields' context
   */
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    for (AnalyticsField field : fields) {
      field.doSetNextReader(context);
    }
  }

  /**
   * Collect the values from the used {@link AnalyticsField}s.
   *
   * @param doc the document to collect values for
   * @throws IOException if an error occurs during field collection
   */
  public void collect(int doc) throws IOException {
    for (AnalyticsField field : fields) {
      field.collect(doc);
    }
  }

  /**
   * Add a {@link ReductionDataCollection} to target while collecting documents.
   * This target is valid until the lasting targets are cleared.
   *
   * @param target data collection to add document data too
   */
  public void addLastingCollectTarget(ReductionDataCollection target) {
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].addLastingCollectTarget(target.dataArr[i]);
    }
  }
  /**
   * Clear lasting collection targets.
   */
  public void clearLastingCollectTargets() {
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].clearLastingCollectTargets();
    }
  }

  /**
   * Add a new {@link ReductionDataCollection} to target while collecting the next document.
   * This target is only valid for the next {@link #apply()} call.
   *
   * @return the new data collection being targeted
   */
  public ReductionDataCollection newDataCollectionTarget() {
    ReductionDataCollection newCol = new ReductionDataCollection();
    newCol.dataArr = new ReductionData[reductionDataCollectors.length];
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      newCol.dataArr[i] = reductionDataCollectors[i].newDataTarget();
    }
    return newCol;
  }
  /**
   * Add a {@link ReductionDataCollection} to target while collecting the next document.
   * This target is only valid for the next {@link #apply()} call.
   *
   * @param target data collection to add document data too
   */
  public void addCollectTarget(ReductionDataCollection target) {
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].addCollectTarget(target.dataArr[i]);
    }
  }

  /**
   * Apply the values of the collected fields through the expressions' logic to the managed data collectors.
   * This is called after {@link #collect(int)} has been called and the collection targets have been added.
   */
  public void apply() {
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].collectAndApply();;
    }
  }

  /**
   * Finalize the reductions with the collected data stored in the parameter.
   * Once the data is finalized, the {@link ReductionFunction}s that use these
   * {@link ReductionDataCollector}s act like regular {@link AnalyticsValue} classes that
   * can be accessed through their {@code get<value-type>} methods.
   *
   * @param dataCollection the collection of reduction data to compute results for
   */
  public void setData(ReductionDataCollection dataCollection) {
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].setData(dataCollection.dataArr[i]);
    }
  }

  /**
   * Construct a new data collection holding data for all managed data collectors.
   *
   * @return a new data collection
   */
  public ReductionDataCollection newDataCollection() {
    ReductionDataCollection newCol = new ReductionDataCollection();
    newCol.dataArr = new ReductionData[reductionDataCollectors.length];
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      newCol.dataArr[i] = reductionDataCollectors[i].newData();
    }
    return newCol;
  }

  /**
   * Sets the stream of shard data to merge with.
   *
   * @param input the stream of shard data
   */
  public void setShardInput(DataInput input) {
    readers.clear();
    reservations.forEach( resv -> readers.add(resv.createReadStream(input)));
  }
  /**
   * Merge the data from the given shard input stream into the set IO data collectors.
   * Should always be called after {@link #setShardInput(DataInput)} and either {@link #prepareReductionDataIO(ReductionDataCollection)}
   * or {@link #newDataCollectionIO()} have been called.
   *
   * @throws IOException if an error occurs while reading the shard data
   */
  public void mergeData() throws IOException {
    for (ReductionDataReader<?> reader : readers) {
      reader.read();
    }
  }

  /**
   * Sets the stream to export shard data to.
   *
   * @param output the stream of shard data
   */
  public void setShardOutput(DataOutput output) {
    writers.clear();
    reservations.forEach( resv -> writers.add(resv.createWriteStream(output)));
  }
  /**
   * Export the data from the set IO data collectors to the given shard output stream.
   * Should always be called after {@link #setShardOutput(DataOutput)} and {@link #prepareReductionDataIO(ReductionDataCollection)}.
   *
   * @throws IOException if an error occurs while writing the shard data
   */
  public void exportData() throws IOException {
    for (ReductionDataWriter<?> writer : writers) {
      writer.write();
    }
  }

  /**
   * Set the given data collection to be used for either merging or exporting
   *
   * @param col collection to export from or merge to
   */
  public void prepareReductionDataIO(ReductionDataCollection col) {
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      reductionDataCollectors[i].dataIO(col.dataArr[i]);
    }
  }

  /**
   * Create a new {@link ReductionDataCollection} to merge to or export from.
   * Mainly used for creating facet value collectors when merging shard data.
   *
   * @return the new data collection created
   */
  public ReductionDataCollection newDataCollectionIO() {
    ReductionDataCollection newCol = new ReductionDataCollection();
    newCol.dataArr = new ReductionData[reductionDataCollectors.length];
    for (int i = 0; i < reductionDataCollectors.length; i++) {
      newCol.dataArr[i] = reductionDataCollectors[i].newDataIO();
    }
    return newCol;
  }

  /**
   * Holds the collection of {@link ReductionData} that will be updated together.
   *
   * For example each grouping will have a separate {@link ReductionDataCollection}, and
   * ungrouped expressions will have their own as well.
   */
  public static class ReductionDataCollection{
    public ReductionData[] dataArr;
  }
}

