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
package org.apache.solr.analytics.function.reduction.data;

import java.util.ArrayList;
import java.util.function.Consumer;

import org.apache.solr.analytics.function.ReductionFunction;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.value.AnalyticsValue;

/**
 * Manager of a specific instance of {@link ReductionData} collection.
 *
 * @param <T> the type of reduction data being collected
 */
public abstract class ReductionDataCollector<T extends ReductionData> {

  protected ArrayList<T> lastingTargets;
  protected ArrayList<T> collectionTargets;
  protected T ioData;

  protected ReductionDataCollector() {
    lastingTargets = new ArrayList<>();
    collectionTargets = new ArrayList<>();
  }

  /**
   * Submits the data reservations needed for this data collector.
   *
   * @param consumer the consumer which the reservations are submitted to
   */
  public abstract void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer);

  /**
   * A clean slate to start a new reduction.
   *
   * @return the new reduction data
   */
  public abstract T newData();

  /**
   * Add a reduction data to target during collection.
   * The given target is valid until the lasting targets are cleared.
   *
   * @param data the data to target
   */
  @SuppressWarnings("unchecked")
  public void addLastingCollectTarget(ReductionData data) {
    lastingTargets.add((T) data);
  }

  /**
   * Clear the lasting collection targets. After this is called the current lasting
   * targets will not be affected by future {@link #collectAndApply()} calls.
   */
  public void clearLastingCollectTargets() {
    lastingTargets.clear();
  }

  /**
   * Create a new reduction data to target during collection.
   * The given target is only valid for one call to {@link #collectAndApply()}.
   *
   * @return the reduction data created
   */
  public T newDataTarget() {
    T data = newData();
    collectionTargets.add(data);
    return data;
  }

  /**
   * Add a reduction data to target during collection.
   * The given target is only valid for one call to {@link #collectAndApply()}.
   *
   * @param data the data to target
   */
  @SuppressWarnings("unchecked")
  public void addCollectTarget(ReductionData data) {
    collectionTargets.add((T)data);
  }

  /**
   * Collect the info for the current Solr Document and apply the results to the
   * given collection targets.
   *
   * After application, all non-lasting targets are removed.
   */
  public void collectAndApply() {
    collect();
    lastingTargets.forEach( target -> apply(target) );
    collectionTargets.forEach( target -> apply(target) );
    collectionTargets.clear();
  }

  /**
   * Collect the information from current Solr Document.
   */
  protected void collect() { }

  /**
   * Apply the collected info to the given reduction data.
   * Should always be called after a {@link #collect()} call.
   *
   * @param data reduction data to apply collected info to
   */
  protected abstract void apply(T data);

  /**
   * Create a new reduction data to use in exporting and merging.
   *
   * @return the created reduction data
   */
  public T newDataIO() {
    ioData = newData();
    return ioData;
  }

  /**
   * Set the reduction data to use in exporting and merging.
   *
   * @param data the data to use
   */
  @SuppressWarnings("unchecked")
  public void dataIO(ReductionData data) {
    ioData = (T)data;
  }

  /**
   * Finalize the reduction with the merged data stored in the parameter.
   * Once the reduction is finalized, the {@link ReductionFunction}s that use this
   * data collector act like regular {@link AnalyticsValue} classes that
   * can be accessed through their {@code get<value-type>} methods.
   *
   * (FOR CLOUD)
   *
   * @param data the merged data to compute a reduction for
   */
  public abstract void setMergedData(ReductionData data);

  /**
   * Finalize the reduction with the collected data stored in the parameter.
   * Once the reduction is finalized, the {@link ReductionFunction}s that use this
   * data collector act like regular {@link AnalyticsValue} classes that
   * can be accessed through their {@code get<value-type>} methods.
   *
   * (FOR SINGLE-SHARD)
   *
   * @param data the collected data to compute a reduction for
   */
  public abstract void setData(ReductionData data);

  /**
   * Get the name of the reduction data collector. This is the same across all instances of the data collector.
   *
   * @return the name
   */
  public abstract String getName();

  /**
   * The unique expression string of the reduction data collector, given all inputs and parameters.
   * Used during {@link ReductionDataCollector} syncing. Since the string should be unique,
   * only one of expression is kept.
   *
   * @return the expression string
   */
  public abstract String getExpressionStr();
}
